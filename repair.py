# -*- coding: utf-8 -*-
"""损坏视频：只读体检 + 三级修复（app 内建）。

三级（由便宜到贵）
  ① 索引表重建   ISO-BMFF(MP4/MOV) 的 stco 被截 -> 用完好那条轨推算缺失 chunk 偏移，
                **纯追加**（原地、秒级、回滚 = truncate 回原长度）
  ② 小洞插帧     洞 <= GAP_SEC（默认 5s）-> minterpolate 合成过渡帧（是"合成"，不是"恢复"），原地
  ③ 大洞重编码   容错解码 + CFR 重编码 -> 独立目录产出，完成后自动替换原文件

原则
  * 体检（probe/plan_*）**只读**，不写任何字节
  * 永不覆盖原文件已有字节；① 只追加、② 先产临时再替换（替换前后都留回滚点）
  * 洞里的真实画面永久丢失——插帧/重编码只保证"连续、能播"，不恢复内容
  * 引擎算法来自已验证的 mp4idx_repair（CLUB-580 2.21GB / FC2PPV 4088845A 735MB 两次成功）

环境变量
  MS_REPAIR_GAP_SEC   小洞阈值（秒），默认 5
  MS_REPAIR_OUT       重编码独立输出目录，默认 F:\\MediaScraper\\repair_out
"""

import io
import json
import os
import random
import re
import shutil
import struct
import subprocess
import sys
import time

FFMPEG = os.environ.get('FFMPEG', 'ffmpeg')
FFPROBE = os.environ.get('FFPROBE', 'ffprobe')
GAP_SEC = float(os.environ.get('MS_REPAIR_GAP_SEC', '5') or 5)
OUT_DIR = os.environ.get('MS_REPAIR_OUT') or r'F:\MediaScraper\repair_out'
HOLE_MIN = 64 * 1024                      # 洞的最小长度（技能口径：>=64KB 才算真洞）
VIDEO_EXT = {'.mp4', '.mkv', '.avi', '.wmv', '.mov', '.ts', '.m4v', '.flv',
             '.rmvb', '.rm', '.mpg', '.mpeg', '.webm', '.m2ts', '.mts'}
MP4_EXT = {'.mp4', '.mov', '.m4v'}


# ----------------------------------------------------------------- 基础
def _run(cmd, timeout=300):
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout)
        return r.returncode, (r.stdout or b'').decode('utf-8', 'replace'), \
            (r.stderr or b'').decode('utf-8', 'replace')
    except Exception as e:
        return -1, '', type(e).__name__ + ': ' + str(e)[:120]


def _rd(src, off, n):
    """从文件对象或 bytes 里读一段。"""
    if hasattr(src, 'seek'):
        src.seek(off)
        return src.read(n)
    return src[off:off + n]


def _boxes(fp, start, end, limit=256):
    """走同级 box；返回 [(type, off, size, hlen)]。fp 可以是文件对象，也可以是 bytes。"""
    out, off = [], start
    while off + 8 <= end and len(out) < limit:
        h = _rd(fp, off, 16)
        if len(h) < 8:
            break
        l = struct.unpack('>I', h[:4])[0]
        t = h[4:8].decode('latin1', 'replace')
        hl = 8
        if l == 1 and len(h) >= 16:
            l = struct.unpack('>Q', h[8:16])[0]
            hl = 16
        elif l == 0:
            l = end - off
        if l < hl:
            break
        out.append((t, off, l, hl))
        off += l
    return out


def _path_box(fp, s, e, path):
    for t, o, l, hl in _boxes(fp, s, e):
        if t == path[0]:
            if len(path) == 1:
                return (o, l, hl)
            r = _path_box(fp, o + hl, o + l, path[1:])
            if r:
                return r
    return None


def _track_tables(buf, trak, moov_off):
    t, to, tl, thl = trak
    sb = _path_box(buf, to + thl, to + tl, [b'mdia'.decode(), 'minf', 'stbl'])
    if not sb:
        return None
    so, sl, shl = sb
    ch = {c[0]: c for c in _boxes(buf, so + shl, so + sl)}
    if not all(k in ch for k in ('stsc', 'stsz', 'stco')):
        return None
    _, o, l, hl = ch['stsc']
    n = struct.unpack('>I', buf[o + hl + 4:o + hl + 8])[0]
    a = o + hl + 8
    stsc = [struct.unpack('>III', buf[a + i * 12:a + i * 12 + 12]) for i in range(n)]
    _, o, l, hl = ch['stsz']
    ss = struct.unpack('>I', buf[o + hl + 4:o + hl + 8])[0]
    ns = struct.unpack('>I', buf[o + hl + 8:o + hl + 12])[0]
    a = o + hl + 12
    sizes = [ss] * ns if ss else list(struct.unpack('>%dI' % ns, buf[a:a + ns * 4]))
    _, o, l, hl = ch['stco']
    nco = struct.unpack('>I', buf[o + hl + 4:o + hl + 8])[0]
    a = o + hl + 8
    have = max(0, (len(buf) - a) // 4)
    P = min(nco, have)
    co = list(struct.unpack('>%dI' % P, buf[a:a + P * 4]))
    spc = [0] * (nco + 1)
    for i, (fc, sp, sdi) in enumerate(stsc):
        nxt = stsc[i + 1][0] if i + 1 < len(stsc) else nco + 1
        for c in range(fc, min(nxt, nco + 1)):
            spc[c] = sp
    cb = [0] * (nco + 1)
    si = 0
    for c in range(1, nco + 1):
        cnt = spc[c]
        cb[c] = sum(sizes[si:si + cnt])
        si += cnt
    return dict(nco=nco, off=co, cb=cb, data_abs=moov_off + a, avail=len(buf) - a,
                present=P, nsamp=ns, consumed=si)


# ----------------------------------------------------------------- ① 索引重建
def plan_index(path):
    """只读：走 box 链定位被截的 stco，算缺失字节与预期新尺寸；不改文件。"""
    out = {'ok': False, 'kind': 'index', 'path': path}
    try:
        sz = os.path.getsize(path)
    except Exception as e:
        out['error'] = 'stat 失败: %s' % e
        return out
    out['size'] = sz
    try:
        with open(path, 'rb') as fp:
            tops = {}
            for t, o, l, hl in _boxes(fp, 0, sz, limit=64):
                tops.setdefault(t, (o, l))
            if 'moov' not in tops:
                out['error'] = '没有 moov（不是"被截"场景）'
                return out
            if 'mdat' not in tops:
                out['error'] = '没有 mdat'
                return out
            moov_off, moov_len = tops['moov']
            mdat_off, mdat_len = tops['mdat']
            mdat_end = mdat_off + mdat_len
            out.update(moov_off=moov_off, moov_declared=moov_len, mdat_end=mdat_end)
            avail = sz - moov_off
            short = moov_len - avail
            out['moov_short'] = short
            if short <= 0:
                out['error'] = 'moov 完整（不是被截场景）'
                return out
            fp.seek(moov_off)
            buf = fp.read(avail)
            traks = [b for b in _boxes(_MemFile(buf), 8, len(buf)) if b[0] == 'trak']
            if len(traks) < 2:
                out['error'] = 'trak 不足（%d）' % len(traks)
                return out
            V = _track_tables(buf, traks[0], moov_off)
            A = _track_tables(buf, traks[1], moov_off)
            if not V or not A:
                out['error'] = '表解析失败（stsc/stsz/stco 缺失）'
                return out
            N, P = A['nco'], A['present']
            out.update(video_chunks=V['nco'], audio_declared=N, audio_present=P,
                       avail_bytes=A['avail'])
            bad = 0
            for i in range(1, P + 1):
                if A['off'][i - 1] != V['off'][i - 1] + V['cb'][i]:
                    bad += 1
            out['pair_mismatch'] = bad
            out['pair_checked'] = P
            if bad:
                out['error'] = '交错规则不成立（%d/%d 不符）—— 不做猜测' % (bad, P)
                return out
            if P >= N:
                out['error'] = 'stco 未缺（present=%d declared=%d）' % (P, N)
                return out
            full = struct.pack('>%dI' % N, *[V['off'][i - 1] + V['cb'][i] for i in range(1, N + 1)])
            append = full[A['avail']:]
            last = struct.unpack('>I', append[-4:])[0]
            ends = last + A['cb'][N]
            free_bytes = 54 + 97                      # 两次实测的 stbl/moov 父尾
            new_size = sz + len(append) + free_bytes
            target = moov_off + moov_len
            out.update(append_bytes=len(append), free_bytes=free_bytes,
                       new_size=new_size, target=target,
                       selfcheck_last_ends_at=ends, selfcheck_mdat_end=mdat_end,
                       selfcheck_ok=(ends == mdat_end),
                       size_ok=(new_size == target))
            out['ok'] = bool(out['selfcheck_ok'] and out['size_ok'])
            if not out['ok']:
                out['error'] = '自检未过（末 chunk 落点=%s mdat末=%s / 新尺寸=%s 目标=%s）' % (
                    ends, mdat_end, new_size, target)
            else:
                out['plan'] = '追加 %d 字节（%s）' % (len(append) + free_bytes, '原地可回滚')
            return out
    except Exception as e:
        out['error'] = type(e).__name__ + ': ' + str(e)[:120]
        return out


class _MemFile(object):
    """把 bytes 当只读文件对象喂给 _boxes()。"""

    def __init__(self, b):
        self.b = b

    def seek(self, o):
        self.o = o

    def read(self, n):
        r = self.b[self.o:self.o + n]
        self.o += n
        return r


def apply_index(path, log=None):
    """原地纯追加修复（只改文件尾部）。返回 undo 信息。"""
    say = log or (lambda *a: None)
    p = plan_index(path)
    if not p.get('ok'):
        return {'ok': False, 'error': p.get('error') or '计划未通过'}
    sz = p['size']
    with open(path, 'rb') as fp:
        fp.seek(p['moov_off'])
        buf = fp.read(sz - p['moov_off'])
    traks = [b for b in _boxes(_MemFile(buf), 8, len(buf)) if b[0] == 'trak']
    V = _track_tables(buf, traks[0], p['moov_off'])
    A = _track_tables(buf, traks[1], p['moov_off'])
    N = A['nco']
    full = struct.pack('>%dI' % N, *[V['off'][i - 1] + V['cb'][i] for i in range(1, N + 1)])
    lead = struct.pack('>I', 54) + b'free' + b'\0' * 46
    lead2 = struct.pack('>I', 97) + b'free' + b'\0' * 89
    say('  追加 %d + %d 字节' % (len(full[A['avail']:]), len(lead) + len(lead2)))
    with open(path, 'ab') as o:
        o.write(full[A['avail']:])
        o.write(lead)
        o.write(lead2)
    now = os.path.getsize(path)
    ok = (now == p['target'])
    return {'ok': ok, 'kind': 'index', 'path': path, 'orig_size': sz, 'new_size': now,
            'undo': {'action': 'truncate', 'path': path, 'to': sz},
            'error': '' if ok else '尺寸未达预期（%d != %d）' % (now, p['target'])}


def undo_index(path, orig_size):
    """回滚 ①：truncate 回原长度。"""
    try:
        with open(path, 'r+b') as fp:
            fp.truncate(int(orig_size))
        return {'ok': True, 'size': os.path.getsize(path)}
    except Exception as e:
        return {'ok': False, 'error': str(e)[:120]}


# ----------------------------------------------------------------- 体检
def probe(path):
    """只读体检：能否打开 / 容器结构 / 属于哪一类。"""
    r = {'path': path, 'cls': '', 'reason': ''}
    try:
        r['size'] = os.path.getsize(path)
    except Exception as e:
        r['cls'] = 'dead'; r['reason'] = 'stat 失败'
        return r
    rc, so, se = _run([FFPROBE, '-v', 'error', '-show_format', '-show_streams', path], 120)
    r['ffprobe_rc'] = rc
    r['ffprobe_err'] = (se or '').strip().splitlines()[:2]
    ext = os.path.splitext(path)[1].lower()
    if r['size'] == 0:
        r['cls'] = 'dead'; r['reason'] = '0 字节（下载失败）'
        return r
    if rc == 0:
        dur = 0.0
        m = re.search(r'duration=([\d.]+)', so)
        if m:
            dur = float(m.group(1))
        r['duration'] = dur
        # 头部自洽：moov 声明长度 vs 实际（只有 MP4 系能做）
        if ext in MP4_EXT:
            try:
                with open(path, 'rb') as fp:
                    tops = {}
                    for t, o, l, hl in _boxes(fp, 0, r['size'], limit=64):
                        tops.setdefault(t, (o, l))
                if 'moov' in tops:
                    mo, ml = tops['moov']
                    if mo + ml > r['size']:
                        r['cls'] = 'index-cut'
                        r['reason'] = 'moov 尾部被截 %d 字节' % (mo + ml - r['size'])
                        return r
            except Exception:
                pass
        r['cls'] = 'ok'; r['reason'] = '可打开、头部自洽'
        return r
    low = (se or '').lower()
    if 'moov atom not found' in low or 'moov' in low and 'not found' in low:
        if r['size'] < 1024 * 1024:
            r['cls'] = 'dead'; r['reason'] = 'moov 缺失且体积很小（未下载完）'
        else:
            r['cls'] = 'moov-gone'; r['reason'] = 'moov 整体缺失（非"被截"，需同源参考或重下）'
        return r
    if 'contradict' in low or 'error reading header' in low or 'invalid' in low:
        r['cls'] = 'index-cut'; r['reason'] = (se or '').strip().splitlines()[0][:90]
        return r
    r['cls'] = 'other'; r['reason'] = (se or '').strip().splitlines()[0][:90] if se else 'ffprobe 失败'
    return r


def plan_repair(path, log=None):
    """汇总一个文件该走哪级 + 代价。只读。"""
    pr = probe(path)
    out = dict(pr)
    out['tier'] = None
    out['plan'] = []
    out['gap_sec'] = GAP_SEC
    if pr['cls'] == 'dead':
        out['tier'] = 'none'; out['reason'] = pr['reason'] + ' -> 不可修'
        return out
    if pr['cls'] == 'moov-gone':
        out['tier'] = 'none'; out['reason'] = pr['reason']
        return out
    if pr['cls'] == 'index-cut':
        p = plan_index(path)
        out['index'] = p
        if p.get('ok'):
            out['tier'] = 'index'
            out['plan'] = ['① 索引重建：追加 %d 字节（原地、秒级、可回滚）'
                           % (p['append_bytes'] + p['free_bytes'])]
        else:
            out['tier'] = 'none'
            out['reason'] = '索引重建条件不满足：' + (p.get('error') or '')
        return out
    if pr['cls'] == 'ok':
        out['tier'] = 'ok'; out['reason'] = '无需处理'
        return out
    out['tier'] = 'reencode'
    out['plan'] = ['③ 大洞/码流损坏：容错重编码（独立目录 -> 完成后自动替换）']
    return out


# ----------------------------------------------------------------- ③ 容错重编码（带进度）
def reencode_cmd(path, out):
    return [FFMPEG, '-hide_banner', '-err_detect', 'ignore_err', '-i', path,
            '-fps_mode', 'cfr', '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '20',
            '-c:a', 'aac', '-y', out]


def tolerant_reencode(path, out_dir=None, log=None, tick=None):
    """③ 独立目录产出，完成后**自动替换**原文件（原文件先移到回滚点）。"""
    say = log or (lambda *a: None)
    out_dir = out_dir or OUT_DIR
    os.makedirs(out_dir, exist_ok=True)
    stage = os.path.join(out_dir, os.path.splitext(os.path.basename(path))[0] + '.mp4')
    dur = 0.0
    try:
        pr = probe(path)
        dur = float(pr.get('duration') or 0)
    except Exception:
        pass
    say('  重编码 -> %s（预计 %s）' % (stage, ('%d 分钟' % round(dur / 5.1 / 60)) if dur else '未知'))
    cmd = reencode_cmd(path, stage)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         universal_newlines=True, bufsize=1)
    done = False
    t0 = time.time()
    for line in p.stdout:
        m = re.search(r'time=(\d+):(\d+):([\d.]+)', line)
        if m and dur and tick:
            secs = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
            tick(min(secs / dur, 0.995), '重编码 %.1f%%' % (100 * secs / dur))
        if 'Error' in line and 'ignore_err' not in line:
            say('  ffmpeg: ' + line.strip()[:110])
    p.wait()
    done = (p.returncode == 0 and os.path.isfile(stage) and os.path.getsize(stage) > 1024)
    if not done:
        return {'ok': False, 'error': '重编码失败（rc=%s）' % p.returncode, 'stage': stage}
    say('  重编码完成 %.0fs，替换原文件' % (time.time() - t0))
    orig = path + '.orig'
    k = 1
    while os.path.exists(orig):
        orig = '%s.orig%d' % (path, k); k += 1
    try:
        os.replace(path, orig)             # 原文件 -> 回滚点（同卷 rename，不占额外空间）
        shutil.copy2(stage, path)          # 新文件就位（跨卷，按实际速度）
    except Exception as e:
        return {'ok': False, 'error': '替换失败: %s' % str(e)[:120], 'stage': stage, 'orig': orig}
    try:
        os.remove(stage)
    except Exception:
        pass
    return {'ok': True, 'kind': 'reencode', 'path': path, 'orig_backup': orig,
            'undo': {'action': 'restore', 'path': path, 'backup': orig},
            'secs': round(time.time() - t0, 1)}


# ----------------------------------------------------------------- CLI（手工验证用）
def _main():
    args = sys.argv[1:]
    if not args:
        print('用法: repair.py probe <file|dir> | index <file> | apply-index <file> | undo-index <file> <orig_size>')
        return
    cmd, target = args[0], (args[1] if len(args) > 1 else '')
    if cmd == 'probe':
        if os.path.isdir(target):
            n, bad = 0, {}
            for root, dirs, files in os.walk(target):
                for f in files:
                    if os.path.splitext(f)[1].lower() not in VIDEO_EXT:
                        continue
                    n += 1
                    r = probe(os.path.join(root, f))
                    bad.setdefault(r['cls'], []).append(r)
            print('  视频 %d 个' % n)
            for k, v in sorted(bad.items(), key=lambda x: -len(x[1])):
                print('   %-11s %d 个' % (k, len(v)))
                for r in v[:5]:
                    print('      %s  %s' % (os.path.basename(r['path'])[:52], r['reason'][:56]))
        else:
            print(json.dumps(probe(target), ensure_ascii=False, indent=1)[:1200])
    elif cmd == 'index':
        print(json.dumps(plan_index(target), ensure_ascii=False, indent=1)[:1600])
    elif cmd == 'apply-index':
        print(json.dumps(apply_index(target, log=print), ensure_ascii=False, indent=1)[:600])
    elif cmd == 'undo-index':
        print(json.dumps(undo_index(target, args[2]), ensure_ascii=False))
    else:
        print('未知命令', cmd)


if __name__ == '__main__':
    _main()

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

# app 可设置停止钩子：repair.STOP_HOOK = lambda: bool
STOP_HOOK = None


def _stop_req():
    """是否已请求停止（由 app 通过 STOP_HOOK 注入；默认不停止）。"""
    try:
        return bool(STOP_HOOK and STOP_HOOK())
    except Exception:
        return False


FFMPEG = os.environ.get('FFMPEG', 'ffmpeg')
FFPROBE = os.environ.get('FFPROBE', 'ffprobe')
GAP_SEC = float(os.environ.get('MS_REPAIR_GAP_SEC', '5') or 5)
OUT_DIR = os.environ.get('MS_REPAIR_OUT') or r'F:\MediaScraper\repair_out'
HOLE_MIN = 64 * 1024                      # 洞的最小长度（技能口径：>=64KB 才算真洞）
THREADS = int(os.environ.get('MS_REPAIR_THREADS', '0') or 0)   # 0 = 按核数自动
DEEP_WORKERS = int(os.environ.get('MS_DEEP_WORKERS', '0') or 0)  # 0 = 自动（见 _deep_workers）
VIDEO_EXT = {'.mp4', '.mkv', '.avi', '.wmv', '.mov', '.ts', '.m4v', '.flv',
             '.rmvb', '.rm', '.mpg', '.mpeg', '.webm', '.m2ts', '.mts'}
MP4_EXT = {'.mp4', '.mov', '.m4v'}


# ----------------------------------------------------------------- 基础
def _cores():
    """逻辑核数（失败时退回 2）。"""
    try:
        return max(1, int(os.cpu_count() or 2))
    except Exception:
        return 2


def _threads():
    """编码线程数。实测本机 56 逻辑核，但 x264 帧级线程超过 ~16 收益递减，
    所以封顶 16；MS_REPAIR_THREADS 可覆盖。"""
    return THREADS or max(1, min(16, _cores()))


def _fthreads():
    """滤波器线程（minterpolate/blend/scale）：可多给，封顶 32。"""
    return THREADS or max(1, min(32, _cores()))


def _deep_workers():
    """深检并发流数。**实测结论：并发无收益** —— NAS 总带宽约 8MB/s：
    单流 7.0~7.5MB/s，4 流并发时每流降到约 3.9MB/s（合计不变）。
    瓶颈在存储/网络而不是 CPU，所以默认 1（顺序扫）；换存储后再用 MS_DEEP_WORKERS 实测。
    （注：曾误测到“2 流 96MB/s”，复查发现是系统缓存假象。）"""
    return max(1, min(8, DEEP_WORKERS)) if DEEP_WORKERS else 1


def _lines(p):
    """逐行产出子进程输出（按 UTF-8 解、坏字节替换）。

    ffmpeg 的错误信息里会带非 GBK 字符（日文文件名等），用系统默认编码解会
    UnicodeDecodeError 直接崩掉整个修复任务。
    """
    try:
        for raw in iter(p.stdout.readline, b''):
            yield raw.decode('utf-8', 'replace') if isinstance(raw, bytes) else raw
    except Exception:
        return


def _run(cmd, timeout=300):
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=timeout,
                           encoding='utf-8', errors='replace')
        return r.returncode, (r.stdout or ''), (r.stderr or '')
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


DEEP_CHUNK = 96 * 1024 * 1024          # 深检每轮扫描的字节数（可被“停止”打断，下次续）


def mdat_range(path):
    """返回 (mdat_off, mdat_end)；非 MP4 或无 mdat 返回 (0, 0)。"""
    try:
        sz = os.path.getsize(path)
        with open(path, 'rb') as fp:
            for t, o, l, hl in _boxes(fp, 0, sz, limit=64):
                if t == 'mdat':
                    return (o, o + l)
    except Exception:
        pass
    return (0, 0)


def merge_holes(holes):
    """按区间合并（相交或相接的洞并成一个）；跨段重复/截断的片段这样就能拼回完整长度。"""
    iv = sorted([dict(h) for h in (holes or [])], key=lambda x: x['start'])
    out = []
    for h in iv:
        if out and h['start'] <= out[-1]['end']:
            out[-1]['end'] = max(out[-1]['end'], h['end'])
            out[-1]['size'] = out[-1]['end'] - out[-1]['start']
        else:
            out.append(h)
    return out


def scan_holes_chunk(path, start=0, limit=DEEP_CHUNK, min_hole=None, block=4 * 1024 * 1024):
    """断点续扫：只扫 mdat 的 [start, start+limit) 这一段。

    返回 dict(holes=[{start,end,size}], next_off, done, scanned, mdat=(m0,m1), error?)。
    每段与上一段重叠 min_hole-1 字节，保证跨块的洞不丢；重叠区产生的重复按 start 去重。
    """
    import re
    m0, m1 = mdat_range(path)
    if not m1:
        return {'holes': [], 'next_off': 0, 'done': True, 'scanned': 0, 'error': '无 mdat（非 MP4 或无数据段）'}
    min_hole = int(min_hole or HOLE_MIN)
    start = max(m0, int(start or m0))
    if start >= m1:
        return {'holes': [], 'next_off': m1, 'done': True, 'scanned': 0, 'mdat': (m0, m1)}
    end = min(m1, start + max(1, int(limit)))
    # 回退 min_hole-1 字节重读：保证跨段的长洞完整可见（结果用 merge_holes 合并去重）
    read_from = max(m0, start - (min_hole - 1)) if start > m0 else start
    holes = []
    rx = re.compile(rb'\x00{%d,}' % min_hole)
    try:
        with open(path, 'rb') as fp:
            fp.seek(read_from)
            pos = read_from
            while pos < end:
                b = fp.read(min(block, end - pos))
                if not b:
                    break
                buf = b
                for m in rx.finditer(buf):
                    s, e = pos + m.start(), pos + m.end()
                    if e > m0 and s < m1:
                        holes.append({'start': s, 'end': e, 'size': e - s})
                pos += len(b)
            return {'holes': merge_holes(holes), 'next_off': end, 'done': end >= m1,
                    'scanned': end - start, 'mdat': (m0, m1)}
    except Exception as e:
        return {'holes': [], 'next_off': start, 'done': False, 'scanned': 0,
                'mdat': (m0, m1), 'error': type(e).__name__ + ': ' + str(e)[:80]}


def _track_tables(buf, trak, moov_off):
    t, to, tl, thl = trak
    sb = _path_box(buf, to + thl, to + tl, [b'mdia'.decode(), 'minf', 'stbl'])
    if not sb:
        return None
    so, sl, shl = sb
    ch = {c[0]: c for c in _boxes(buf, so + shl, so + sl)}
    if 'stsc' not in ch or 'stsz' not in ch or ('stco' not in ch and 'co64' not in ch):
        return None
    _co64 = ('stco' not in ch)          # 64 位偏移表（大文件 / 某些 muxer）
    _, o, l, hl = ch['stsc']
    n = struct.unpack('>I', buf[o + hl + 4:o + hl + 8])[0]
    a = o + hl + 8
    stsc = [struct.unpack('>III', buf[a + i * 12:a + i * 12 + 12]) for i in range(n)]
    _, o, l, hl = ch['stsz']
    ss = struct.unpack('>I', buf[o + hl + 4:o + hl + 8])[0]
    ns = struct.unpack('>I', buf[o + hl + 8:o + hl + 12])[0]
    a = o + hl + 12
    sizes = [ss] * ns if ss else list(struct.unpack('>%dI' % ns, buf[a:a + ns * 4]))
    _, o, l, hl = ch['co64' if _co64 else 'stco']
    nco = struct.unpack('>I', buf[o + hl + 4:o + hl + 8])[0]
    a = o + hl + 8
    esz = 8 if _co64 else 4
    have = max(0, (len(buf) - a) // esz)
    P = min(nco, have)
    co = list(struct.unpack(('>%dQ' if _co64 else '>%dI') % P, buf[a:a + P * esz]))
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
                present=P, nsamp=ns, consumed=si, spc=spc, co64=_co64)


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
            fmt = '>%dQ' % N if A.get('co64') else '>%dI' % N
            full = struct.pack(fmt, *[V['off'][i - 1] + V['cb'][i] for i in range(1, N + 1)])
            append = full[A['avail']:]
            last = struct.unpack('>Q' if A.get('co64') else '>I',
                                 append[(-8 if A.get('co64') else -4):])[0]
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


TS_EXT = {'.ts', '.m2ts', '.mts'}
MKV_EXT = {'.mkv', '.webm'}


def _check_ts(path):
    """TS/M2TS：包同步头 + 长度对齐 + 连续计数（只读抽样，很快）。

    自动识别 188（TS）与 192（M2TS：4 字节时间戳 + 188）包长。
    """
    issues = []
    try:
        sz = os.path.getsize(path)
        if sz < 188 * 4:
            return (False, ['文件太小（不足 4 个包）'])
        with open(path, 'rb') as fp:
            head = fp.read(2048)
        found = None
        for psz in (188, 192):
            for off in range(psz):
                if off + 4 * psz > len(head):
                    break
                if all(head[off + i * psz:off + i * psz + 1] == b'\x47' for i in range(4)):
                    found = (psz, off)
                    break
            if found:
                break
        if not found:
            return (False, ['找不到 TS 包同步头（0x47）：既不是 188 也不是 192 包长'])
        psz, off = found
        if (sz - off) % psz:
            issues.append('文件长度不是包长 %d 的整数倍（尾部残 %d 字节，可能被截）'
                          % (psz, (sz - off) % psz))
        tail_start = max(0, sz - psz * 60)
        with open(path, 'rb') as fp:
            fp.seek(tail_start)
            tail = fp.read()
        base = (-(tail_start - off)) % psz
        bad = sum(1 for p in range(base, len(tail), psz) if tail[p:p + 1] != b'\x47')
        if bad:
            issues.append('尾部 %d 处包同步头不符（可能被截/损坏）' % bad)
        cc = {}
        i = off
        while i + psz <= len(head) and len(cc) < 40:
            s = i + 4 if psz == 192 else i
            p = head[s:s + 188]
            if len(p) >= 188 and p[0] == 0x47:
                pid = ((p[1] & 0x1F) << 8) | p[2]
                if pid != 0x1FFF:
                    c = p[3] & 0x0F
                    if pid in cc and (cc[pid] + 1) % 16 != c:
                        issues.append('PID %d 连续计数跳变（丢包/损坏）' % pid)
                        break
                    cc[pid] = c
            i += psz
    except Exception as e:
        return (False, [type(e).__name__ + ': ' + str(e)[:80]])
    return (not issues, issues)


def _check_mkv(path):
    """MKV/WebM：EBML 头 + Segment 声明长度 vs 文件大小（截断检测）。"""
    issues = []

    def rd_vint(b, i):
        if i >= len(b) or b[i] == 0:
            return None, i + 1
        first = b[i]
        length, mask = 1, 0x80
        while not (first & mask):
            mask >>= 1
            length += 1
            if length > 8:
                return None, i + 1
        val = first & (mask - 1)
        for k in range(1, length):
            if i + k >= len(b):
                return None, i + length
            val = (val << 8) | b[i + k]
        return val, i + length

    try:
        sz = os.path.getsize(path)
        with open(path, 'rb') as fp:
            head = fp.read(min(4 * 1024 * 1024, sz))
        if head[:4] != b'\x1a\x45\xdf\xa3':
            return (False, ['不是 EBML/MKV（magic 不符）'])
        j = head.find(b'\x18\x53\x80\x67')
        if j < 0:
            issues.append('找不到 Segment 元素（EBML 头后缺少主体）')
            return (False, issues)
        val, after = rd_vint(head, j + 4)
        if val is None:
            issues.append('Segment 长度字段无法解析')
            return (False, issues)
        if val < (1 << 56) - 1:
            decl_end = after + val
            if decl_end > sz + 8:
                issues.append('Segment 声明到 %d 字节，超出现有文件 %d 字节（被截）'
                              % (decl_end, decl_end - sz))
    except Exception as e:
        return (False, [type(e).__name__ + ': ' + str(e)[:80]])
    return (not issues, issues)


def check_index(path):
    """按容器分派的结构级自检。返回 (ok, issues)：
    True=无问题；False=有问题；None=该格式不做结构级检查（如 rmvb，只能靠解码级）。"""
    ext = os.path.splitext(path)[1].lower()
    if ext in MP4_EXT:
        return _check_mp4(path)
    if ext in TS_EXT:
        return _check_ts(path)
    if ext in MKV_EXT:
        return _check_mkv(path)
    return (None, ['%s：该容器暂只做解码级检查（未做结构级索引检查）' % (ext or '?')])


def _check_mp4(path):
    """索引结构自检（只读头部，很快）——比 ffprobe 细：

    1) 顶层 / moov 内每个 box 的声明长度是否越界（被截）
    2) 两轨表自洽：stco 声明/实际条数、stsc 消耗样本 vs stsz、chunk 偏移是否落在 mdat 内且单调、
       末 chunk 落点是否越过 mdat 末
    返回 (ok, issues)；非 MP4/MOV 返回 (None, [...])。
    用途：抓“ffprobe 能打开但表已坏”的文件（这类不会在头部长度检查里暴露）。
    """
    ext = os.path.splitext(path)[1].lower()
    if ext not in MP4_EXT:
        return (None, ['非 MP4/MOV 容器，本自检不覆盖'])
    issues = []
    try:
        sz = os.path.getsize(path)
    except Exception:
        return (False, ['stat 失败'])
    if sz == 0:
        return (False, ['0 字节'])
    try:
        with open(path, 'rb') as fp:
            tops = {}
            for t, o, l, hl in _boxes(fp, 0, sz, limit=64):
                if o + l > sz:
                    issues.append('顶层 box %s 越界 %d 字节（被截）' % (t, o + l - sz))
                tops.setdefault(t, (o, l))
            if 'moov' not in tops:
                return (False, issues + ['没有 moov'])
            if 'mdat' not in tops:
                return (False, issues + ['没有 mdat'])
            mo, ml = tops['moov']
            md0, mdl = tops['mdat']
            mde = md0 + mdl
            fp.seek(mo)
            buf = fp.read(sz - mo)
            for t, o, l, hl in _boxes(_MemFile(buf), 8, min(ml, len(buf)), limit=512):
                if o + l > len(buf):
                    issues.append('moov 内 box %s 越界 %d 字节' % (t, o + l - len(buf)))
            traks = [b for b in _boxes(_MemFile(buf), 8, len(buf)) if b[0] == 'trak']
            if not traks:
                issues.append('没有 trak')
            for ti, tr in enumerate(traks[:2]):
                V = _track_tables(buf, tr, mo)
                if not V:
                    issues.append('trak%d 的 stsc/stsz/stco 缺失（表被截）' % ti)
                    continue
                declared, present = V['nco'], len(V['off'])
                _esz = 8 if V.get('co64') else 4
                _tab = 'co64' if V.get('co64') else 'stco'
                if declared and not present:
                    issues.append('trak%d %s 声明 %d 条但一条都读不到（表损坏/长度错）'
                                  % (ti, _tab, declared))
                elif present < declared:
                    issues.append('trak%d %s 声明 %d 条、只剩 %d 条（缺 %d 字节）'
                                  % (ti, _tab, declared, present, (declared - present) * _esz))
                if V.get('nsamp') and V.get('consumed', 0) > V['nsamp']:
                    issues.append('trak%d stsc 消耗样本 %d > stsz 样本 %d'
                                  % (ti, V['consumed'], V['nsamp']))
                offs = V['off']
                if offs:
                    bad = sum(1 for o in offs if o < md0 or o >= mde)
                    if bad:
                        issues.append('trak%d 有 %d 个 chunk 偏移落在 mdat 之外' % (ti, bad))
                    nz = sum(1 for i in range(1, len(offs)) if offs[i] < offs[i - 1])
                    if nz:
                        issues.append('trak%d 有 %d 处 chunk 偏移非单调（索引损坏）' % (ti, nz))
                    last = offs[-1] + V['cb'][min(present, len(V['cb']) - 1)]
                    if last > mde + 8:
                        issues.append('trak%d 末 chunk 落点 %d 超出 mdat 末 %d（超 %d）'
                                      % (ti, last, mde, last - mde))
    except Exception as e:
        return (False, issues + [type(e).__name__ + ': ' + str(e)[:80]])
    return (not issues, issues)


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
    fmt = '>%dQ' % N if A.get('co64') else '>%dI' % N
    full = struct.pack(fmt, *[V['off'][i - 1] + V['cb'][i] for i in range(1, N + 1)])
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
        if ext in MP4_EXT or ext in TS_EXT or ext in MKV_EXT:
            # 索引/容器自洽检查（只读头部/抽样，很便宜）：抓“能打开但表已坏/被截”的文件
            okc, iss = check_index(path)
            if okc is False and iss:
                r['cls'] = 'index-bad'
                r['reason'] = '索引/容器结构异常：' + '；'.join(iss[:2])
                r['issues'] = iss
                return r
            if okc is None and iss:
                r['note'] = iss[0]
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


# ----------------------------------------------------------------- ② 小洞：定位 + 映射到时间轴
NUL_RX = None


def find_holes(path, min_hole=None, block=4 * 1024 * 1024):
    """数据级：在 mdat 里找 >= min_hole（默认 64KB）的连续 0x00 区段。

    这是“下载预分配 / 没写满”留下的真洞。**注意代价**：要把整个 mdat 读一遍
    （NAS 实测 ~11MB/s，5GB 约 8 分钟），所以只用于按需深检，不当全库例行体检。
    也注意：索引区里零字节是正常的（表里都是小整数），所以只扫 mdat。
    """
    import re
    global NUL_RX
    min_hole = int(min_hole or HOLE_MIN)
    out = []
    try:
        sz = os.path.getsize(path)
        with open(path, 'rb') as fp:
            tops = {t: (o, l) for t, o, l, hl in _boxes(fp, 0, sz, limit=64)}
            md = tops.get('mdat')
            if not md:
                return out
            m0, m1 = md[0], md[0] + md[1]
            rx = re.compile(rb'\x00{%d,}' % min_hole)
            fp.seek(m0)
            pos, carry = m0, b''
            while pos < m1:
                b = fp.read(min(block, m1 - pos))
                if not b:
                    break
                buf = carry + b
                base = pos - len(carry)
                for m in rx.finditer(buf):
                    out.append({'start': base + m.start(), 'end': base + m.end(),
                                'size': m.end() - m.start()})
                carry = buf[-min(min_hole - 1, len(buf)):] if min_hole > 1 else b''
                pos += len(b)
    except Exception:
        pass
    return out


def _mdhd_timescale(buf, trak):
    md = _path_box(buf, trak[1] + trak[3], trak[1] + trak[2], ['mdia', 'mdhd'])
    if not md:
        return 0
    o, l, hl = md
    try:
        ver = buf[o + hl]
        if ver == 1:
            return struct.unpack('>I', buf[o + hl + 20:o + hl + 24])[0]
        return struct.unpack('>I', buf[o + hl + 12:o + hl + 16])[0]
    except Exception:
        return 0


def _stts(buf, trak):
    sb = _path_box(buf, trak[1] + trak[3], trak[1] + trak[2], ['mdia', 'minf', 'stbl'])
    if not sb:
        return []
    ch = {c[0]: c for c in _boxes(buf, sb[0] + sb[2], sb[0] + sb[1])}
    if 'stts' not in ch:
        return []
    _, o, l, hl = ch['stts']
    try:
        n = struct.unpack('>I', buf[o + hl + 4:o + hl + 8])[0]
        a = o + hl + 8
        return [struct.unpack('>II', buf[a + i * 8:a + i * 8 + 8]) for i in range(n)]
    except Exception:
        return []


def holes_to_time(path, holes):
    """把字节区间映射到时间轴：用 stco/stsz/stsc + stts 算，不靠解码。"""
    if not holes:
        return []
    try:
        with open(path, 'rb') as fp:
            sz = os.path.getsize(path)
            tops = {t: (o, l) for t, o, l, hl in _boxes(fp, 0, sz, limit=64)}
            mo, ml = tops['moov'][0], tops['moov'][1]
            fp.seek(mo)
            buf = fp.read(ml)   # 只读 moov；faststart（moov 在前）时原写法会把整个文件读进来
    except Exception:
        return []
    traks = [b for b in _boxes(_MemFile(buf), 8, len(buf)) if b[0] == 'trak']
    if not traks:
        return []
    V = _track_tables(buf, traks[0], mo)
    if not V or not V.get('off'):
        return []
    scale = _mdhd_timescale(buf, traks[0]) or 1000
    stts = _stts(buf, traks[0])
    samp_t = []
    t = 0
    for cnt, delta in stts:
        for _ in range(cnt):
            samp_t.append(t)
            t += delta
    offs, spc = V['off'], V['spc']
    cs, acc = [], 0
    for c in range(1, len(offs) + 1):
        cs.append(acc)
        acc += spc[c] if c < len(spc) else 0

    def _find_chunk(byte):
        lo, hi = 0, len(offs) - 1
        best = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            if offs[mid] <= byte:
                best, lo = mid, mid + 1
            else:
                hi = mid - 1
        return best

    out = []
    for h in holes:
        ci = _find_chunk(h['start'])
        cj = _find_chunk(h['end'])
        si = cs[ci] if ci < len(cs) else 0
        sj = cs[cj] if cj < len(cs) else len(samp_t) - 1
        t1 = (samp_t[min(si, len(samp_t) - 1)] / scale) if samp_t else 0.0
        t2 = (samp_t[min(sj, len(samp_t) - 1)] / scale) if samp_t else 0.0
        out.append(dict(h, t1=round(t1, 2), t2=round(t2, 2), dur=round(max(0.0, t2 - t1), 2)))
    return out


def find_gaps(path, min_hole=None):
    """数据级找洞并映射到时间：返回 [{start,end,size,t1,t2,dur}]。"""
    return holes_to_time(path, find_holes(path, min_hole))


def probe_windows(path, n=6, sec=10, log=None):
    """低成本数据级体检：抽 n 个窗口做容错解码，数解码器级错误。"""
    bad = []
    dur = 0.0
    try:
        rc, so, se = _run([FFPROBE, '-v', 'error', '-show_entries', 'format=duration',
                           '-of', 'default=nw=1:nk=1', path], 90)
        dur = float((so or '0').strip() or 0)
    except Exception:
        dur = 0.0
    if dur <= 0:
        return {'dur': 0, 'bad': [], 'windows': 0, 'sec': sec, 'note': '读不到时长'}
    for k in range(n):
        t = dur * (k + 0.5) / n
        rc, so, se = _run([FFMPEG, '-hide_banner', '-v', 'error', '-err_detect', 'ignore_err',
                           '-ss', '%.2f' % max(0, t - sec / 2), '-t', '%.0f' % sec,
                           '-i', path, '-f', 'null', '-'], 180)
        marks = [x for x in (se or '').splitlines()
                 if ('concealing' in x.lower() or 'error while decoding' in x.lower()
                     or 'Invalid NAL' in x or 'corrupt' in x.lower())]
        if marks:
            bad.append({'t': round(t, 1), 'n': len(marks), 'sample': marks[0][:90]})
            if log:
                log('  窗口 %.0fs 有 %d 条解码错误：%s' % (t, len(marks), marks[0][:70]))
    return {'dur': round(dur, 1), 'bad': bad, 'windows': n, 'sec': sec}


def plan_repair(path, deep=False, log=None):
    """汇总该文件走哪级 + 代价。

    deep=False：只看头部（快，0.7 秒/文件）——**洞在这个层面看不见**。
    deep=True ：额外做数据级检查（字节扫洞 + 抽窗解码），慢，但能看到"能打开的坏文件"。
    """
    pr = probe(path)
    out = dict(pr)
    out['tier'] = None
    out['plan'] = []
    out['gap_sec'] = GAP_SEC
    out['deep'] = bool(deep)

    if pr['cls'] == 'dead':
        out['tier'] = 'none'; out['reason'] = pr['reason'] + ' -> 不可修'
        return out
    if pr['cls'] == 'moov-gone':
        out['tier'] = 'none'; out['reason'] = pr['reason']
        return out
    if pr['cls'] == 'index-bad':
        out['tier'] = 'remux'
        i = pr.get('issues') or []
        out['plan'] = ['② 无损重封装（-c copy 重建索引；不重编码、原画质）',
                       '注：会先比对时长，不符则拒绝替换（坏索引上 -c copy 不可靠）']
        out['reason'] = (pr.get('reason') or '')[:160]
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

    # cls == 'ok' 或 'other' —— 是否有数据洞，只有深检知道
    if pr['cls'] == 'ok' and not deep:
        out['tier'] = 'ok'
        out['reason'] = '头部级通过（是否有数据洞需深检）'
        return out

    gaps = []
    try:
        gaps = find_gaps(path)
    except Exception:
        gaps = []
    if gaps:
        small = [g for g in gaps if g['dur'] <= GAP_SEC]
        big = [g for g in gaps if g['dur'] > GAP_SEC]
        out['gaps'] = gaps
        if big:
            out['tier'] = 'reencode'
            out['plan'] = ['③ 大洞 %d 处（> %.0fs，最长 %.1fs）：容错重编码'
                           % (len(big), GAP_SEC, max(g['dur'] for g in big))]
        else:
            out['tier'] = 'gap'
            out['plan'] = ['② 小洞插帧：%d 处（合计 %.1fs，均 <= %.0fs）'
                           % (len(small), sum(g['dur'] for g in small), GAP_SEC)]
        return out

    if pr['cls'] == 'ok':
        w = None
        try:
            w = probe_windows(path, n=4, sec=6, log=None)
        except Exception:
            w = None
        if w and w.get('bad'):
            out['tier'] = 'reencode'
            out['plan'] = ['③ 无 >=64KB 数据洞，但抽窗解码有错误（%d 个窗口）：容错重编码'
                           % len(w['bad'])]
            out['windows'] = w
            return out
        out['tier'] = 'ok'
        out['reason'] = '深检通过（无 >=64KB 数据洞，抽窗无解码错误）'
        out['windows'] = w
        return out

    out['tier'] = 'reencode'
    out['plan'] = ['③ 码流损坏：容错重编码（独立目录 -> 完成后自动替换）']
    return out


# ----------------------------------------------------------------- ③ 容错重编码（带进度）


def reencode_cmd(path, out):
    th, ft = _threads(), _fthreads()
    return [FFMPEG, '-hide_banner', '-filter_threads', str(ft),
            '-err_detect', 'ignore_err', '-i', path,
            '-fps_mode', 'cfr', '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '20',
            '-threads', str(th), '-c:a', 'aac', '-y', out]


def _ffprobe_wh(path):
    """读分辨率与帧率（给插帧用，保证尺寸/帧率一致）。"""
    w = h = 0
    fps = 25.0
    rc, so, se = _run([FFPROBE, '-v', 'error', '-select_streams', 'v:0', '-show_entries',
                       'stream=width,height,r_frame_rate', '-of', 'default=nw=1', path], 90)
    for line in (so or '').splitlines():
        if line.startswith('width='):
            w = int(line.split('=')[1] or 0)
        elif line.startswith('height='):
            h = int(line.split('=')[1] or 0)
        elif line.startswith('r_frame_rate='):
            v = line.split('=')[1]
            if '/' in v:
                a, b = v.split('/')
                try:
                    fps = float(a) / max(1.0, float(b))
                except Exception:
                    pass
    return w, h, (fps or 25.0)


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
    say('  重编码 -> %s（线程 编码 %d / 滤波 %d；逻辑核 %d；预计 %s）'
        % (stage, _threads(), _fthreads(), _cores(),
           ('%d 分钟' % round(dur / 5.1 / 60)) if dur else '未知'))
    cmd = reencode_cmd(path, stage)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=False)
    done = False
    t0 = time.time()
    for line in _lines(p):
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
    # 替换前校验：可播 + 时长一致（坏产物绝不覆盖原文件）
    pr2 = probe(stage)
    if pr2['cls'] != 'ok':
        return {'ok': False, 'stage': stage,
                'error': '产物不可播（%s）—— 未替换原文件' % (pr2.get('reason') or pr2['cls'])}
    d2 = float(pr2.get('duration') or 0)
    if dur and d2 and abs(d2 - dur) > max(1.0, dur * 0.01):
        return {'ok': False, 'stage': stage,
                'error': '产物时长不符（%.1fs vs %.1fs）—— 未替换原文件' % (d2, dur)}
    say('  重编码完成 %.0fs，替换原文件' % (time.time() - t0))
    orig = path + '.orig'
    k = 1
    while os.path.exists(orig):
        orig = '%s.orig%d' % (path, k); k += 1
    try:
        os.replace(path, orig)             # 原文件 -> 回滚点（同卷 rename，不占额外空间）
        _copy_progress(stage, path, log=log)          # 新文件就位（跨卷，按实际速度）
    except Exception as e:
        return {'ok': False, 'error': '替换失败: %s' % str(e)[:120], 'stage': stage, 'orig': orig}
    try:
        os.remove(stage)
    except Exception:
        pass
    return {'ok': True, 'kind': 'reencode', 'path': path, 'orig_backup': orig,
            'undo': {'action': 'restore', 'path': path, 'backup': orig},
            'secs': round(time.time() - t0, 1)}


# ----------------------------------------------------------------- ② 执行：小洞插帧（原地替换）
def _grab_png(src, t, out):
    """取 t 秒处一帧存成 png。

    注意：损坏流上 ffmpeg 常常返回非 0（实测 rc=69）**但仍然写出可用帧**，
    所以判据是“png 写出来了且体积合理”，不是 rc。
    """
    cmd = [FFMPEG, '-hide_banner', '-v', 'error', '-err_detect', 'ignore_err',
           '-ss', '%.3f' % max(0.0, t), '-i', src, '-frames:v', '1', '-y', out]
    _run(cmd, 240)
    return os.path.isfile(out) and os.path.getsize(out) > 1000


def remux_copy(path, log=None, tick=None):
    """无损重封装（-c copy）：不重编码、重建容器索引。用于“能读但索引坏”的文件。

    产物先校验（可播 + 索引自检无 issue）才原地替换，原文件留 .orig 回滚点。
    """
    say = log or (lambda *a: None)
    t0 = time.time()
    os.makedirs(OUT_DIR, exist_ok=True)
    base = os.path.splitext(os.path.basename(path))[0]
    stage = os.path.join(OUT_DIR, base + '.remux.mp4')
    say('  无损重封装 -> %s（不重编码）' % os.path.basename(stage))
    cmd = [FFMPEG, '-hide_banner', '-v', 'error', '-err_detect', 'ignore_err',
           '-i', path, '-c', 'copy', '-movflags', '+faststart', '-y', stage]
    rc, so, se = _run(cmd, 7200)
    if rc != 0 or not os.path.isfile(stage) or os.path.getsize(stage) < 2048:
        err = ' | '.join([l for l in (se or '').strip().splitlines() if l.strip()][-2:])[:220]
        return {'ok': False, 'error': '重封装失败 rc=%s: %s' % (rc, err), 'stage': stage}
    pr = probe(stage)
    if pr['cls'] != 'ok':
        return {'ok': False, 'error': '重封装后索引仍异常：%s' % (pr.get('reason') or pr['cls']), 'stage': stage}
    src_dur = 0.0
    try:
        src_dur = float(probe(path).get('duration') or 0)
    except Exception:
        src_dur = 0.0
    d2 = float(pr.get('duration') or 0)
    if src_dur and d2 and abs(d2 - src_dur) > max(1.0, src_dur * 0.01):
        return {'ok': False, 'stage': stage,
                'error': '重封装后时长不符（%.1fs vs %.1fs）—— 索引已损坏时 -c copy 会跟着坏表复制、丢内容；'
                         '请改走 ① 索引重建或 ③ 容错重编码。原文件未改动'
                         % (d2, src_dur)}
    orig = path + '.orig'
    k = 1
    while os.path.exists(orig):
        orig = '%s.orig%d' % (path, k); k += 1
    try:
        os.replace(path, orig)
        _copy_progress(stage, path, log=log)
    except Exception as e:
        return {'ok': False, 'error': '替换失败: %s' % str(e)[:110], 'stage': stage, 'orig': orig}
    try:
        os.remove(stage)
    except Exception:
        pass
    say('  已原地替换（回滚点 %s，耗时 %.0fs）' % (os.path.basename(orig), time.time() - t0))
    return {'ok': True, 'kind': 'remux', 'path': path, 'orig_backup': orig,
            'undo': {'action': 'restore', 'path': path, 'backup': orig},
            'secs': round(time.time() - t0, 1)}


def _copy_progress(src, dst, log=None, every=6.0, block=4 * 1024 * 1024):
    """跨卷拷贝并在日志里报进度。

    大文件拷回 NAS 可能要几分钟；原先用 shutil.copy2 全程无输出，
    看起来就像"卡住了"。这里每 every 秒报一次（MB 进度 + 速度 + 预估剩余）。
    """
    total = os.path.getsize(src)
    done_bytes = 0
    t0 = t_last = time.time()
    with open(src, 'rb') as fi, open(dst, 'wb') as fo:
        while True:
            b = fi.read(block)
            if not b:
                break
            fo.write(b)
            done_bytes += len(b)
            now = time.time()
            if log and (now - t_last >= every):
                t_last = now
                sp = done_bytes / max(1e-6, now - t0) / 1048576.0
                left = (total - done_bytes) / max(1e-6, sp * 1048576) / 60.0
                log('    替换中… %d/%d MB（%.0f MB/s，剩约 %.1f 分钟）'
                    % (done_bytes // 1048576, total // 1048576, sp, left))
    try:
        shutil.copystat(src, dst)
    except Exception:
        pass
    return done_bytes


def _stss(buf, trak):
    """同步样本（关键帧）表 -> 1-based 样本号；没有 stss 表示全部是关键帧。"""
    try:
        sb = _path_box(buf, trak[1] + trak[3], trak[1] + trak[2],
                       ['mdia', 'minf', 'stbl'])
        if not sb:
            return None
        so, sl, shl = sb
        ch = {c[0]: c for c in _boxes(buf, so + shl, so + sl)}
        if 'stss' not in ch:
            return None
        _, o, l, hl = ch['stss']
        n = struct.unpack('>I', buf[o + hl + 4:o + hl + 8])[0]
        a = o + hl + 8
        return list(struct.unpack('>%dI' % n, buf[a:a + n * 4]))
    except Exception:
        return None


def kf_times(path):
    """视频轨关键帧时间点（秒）。返回 [] 表示无需/无法吸附。"""
    try:
        sz = os.path.getsize(path)
        with open(path, 'rb') as fp:
            tops = {t: (o, l) for t, o, l, hl in _boxes(fp, 0, sz, limit=64)}
            mo, ml = tops['moov']
            fp.seek(mo)
            buf = fp.read(ml)
    except Exception:
        return []
    traks = [b for b in _boxes(_MemFile(buf), 8, len(buf)) if b[0] == 'trak']
    if not traks:
        return []
    st = _stss(buf, traks[0])
    if not st:
        return []                       # 没有 stss（全关键帧）或读不到 -> 不吸附
    scale = _mdhd_timescale(buf, traks[0]) or 1000
    ts = []
    t = 0
    for cnt, delta in _stts(buf, traks[0]):
        for _ in range(cnt):
            ts.append(t)
            t += delta
    out = []
    for si in st:
        if 1 <= si <= len(ts):
            out.append(ts[si - 1] / float(scale))
    return out


def slice_repair_placeholder():
    pass


def _ts_seg(path, start, end, out, mode='copy', log=None):
    """切一段到 MPEG-TS。mode='copy' 原样搬运（不重编码）；'tol' 容错重编码。"""
    cmd = [FFMPEG, '-hide_banner', '-nostdin', '-loglevel', 'error',
           '-ss', '%.3f' % max(0.0, start), '-to', '%.3f' % end, '-i', path]
    if mode == 'copy':
        cmd += ['-c', 'copy', '-avoid_negative_ts', 'make_zero']
    else:
        cmd += ['-err_detect', 'ignore_err', '-fps_mode', 'cfr',
                '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '20',
                '-threads', str(_threads()), '-c:a', 'aac']
    cmd += ['-f', 'mpegts', '-y', out]
    rc, so, se = _run(cmd, 7200)
    ok = (rc == 0 and os.path.isfile(out) and os.path.getsize(out) > 4096)
    if not ok and log:
        log('    切段失败 [%.1f-%.1f] %s: %s' % (start, end, mode, (se or '')[-160:]))
    return ok


def _probe_at(path, t, sec=2.0):
    """在 t 处解码 sec 秒，返回 (是否有错, 错误数)。用于校验拼接点。"""
    try:
        rc, so, se = _run([FFMPEG, '-hide_banner', '-v', 'error', '-nostdin',
                           '-ss', '%.2f' % max(0.0, t), '-i', path, '-t', '%.1f' % sec,
                           '-f', 'null', '-'], 300)
        txt = (se or '')
        bad = sum(1 for l in txt.splitlines() if l.strip() and 'error' not in l.lower()[:0] and
                  any(k in l.lower() for k in ('error', 'invalid', 'corrupt', 'missing')))
        return (rc != 0 or bad > 0), bad
    except Exception:
        return True, -1


def splice_repair(path, windows, log=None, tick=None, dry=False):
    """分段修复：未损坏段 -c copy（**零损失**），只重编码损坏窗口，再拼接。

    windows: [{'t1','t2', ...}] 需重编码(容错)的时间区间（一般来自深检的洞）
    返回 {'ok','stage','secs','windows','kept_ratio'} 或 {'ok':False,'error'}
    """
    import tempfile
    say = log or (lambda *a: None)
    pr = probe(path)
    dur = float(pr.get('duration') or 0)
    if dur <= 0:
        return {'ok': False, 'error': '拿不到时长，放弃分段方案'}
    # 合并/扩边窗口（前后各留 0.3s，避免正好卡在损坏边界）
    ws = []
    for x in (windows or []):
        try:
            a, b = float(x.get('t1') or 0), float(x.get('t2') or 0)
        except Exception:
            continue
        if b <= a:
            continue
        ws.append((max(0.0, a - 0.3), min(dur, b + 0.3)))
    ws.sort()
    merged = []
    for a, b in ws:
        if merged and a <= merged[-1][1] + 0.05:
            merged[-1] = (merged[-1][0], max(merged[-1][1], b))
        else:
            merged.append((a, b))
    if not merged:
        return {'ok': False, 'error': '没有需要重编码的窗口'}
    # 关键帧吸附：copy 段必须从关键帧起、到关键帧止，否则会从损坏中途开始拷
    kfs = kf_times(path)
    if kfs:
        snapped = []
        for a, b in merged:
            k1 = max([k for k in kfs if k <= a + 1e-3] or [a])
            later = [k for k in kfs if k >= b - 1e-3]
            k2 = min(later) if later else b
            if k2 <= k1:
                later = [k for k in kfs if k > k1 + 1e-3]
                k2 = min(later) if later else b
            snapped.append((max(0.0, k1), min(dur, k2)))
        merged = snapped
        say('  按关键帧吸附：%s' % (['%.1f-%.1f' % x for x in merged],))
    # 段列表：copy / tol 交替
    segs, cur = [], 0.0
    for a, b in merged:
        if a - cur > 0.15:
            segs.append((cur, a, 'copy'))
        segs.append((a, b, 'tol'))
        cur = b
    if dur - cur > 0.15:
        segs.append((cur, dur, 'copy'))
    total_tol = sum(b - a for a, b, m in segs if m == 'tol')
    kept = max(0.0, dur - total_tol)
    say('  分段修复：%d 段（原样搬运 %d 段 / 重编码 %d 段）；'
        '零损失覆盖 %.1f%%（重编码仅 %.1fs）'
        % (len(segs), sum(1 for x in segs if x[2] == 'copy'),
           sum(1 for x in segs if x[2] == 'tol'), 100.0 * kept / max(1e-6, dur), total_tol))
    if dry:
        return {'ok': True, 'dry': True, 'segs': segs}
    tmp = tempfile.mkdtemp(prefix='splice_', dir=os.path.dirname(path) if os.path.isdir(os.path.dirname(path)) else None)
    stage = os.path.join(os.path.dirname(path) or '.', os.path.splitext(os.path.basename(path))[0] + '.splice.mp4')
    t0 = time.time()
    try:
        names = []
        for i, (a, b, m) in enumerate(segs):
            out = os.path.join(tmp, 'seg%03d.ts' % i)
            if not _ts_seg(path, a, b, out, m, log=say):
                return {'ok': False, 'error': '切段失败（第 %d 段 %.1f-%.1f %s）' % (i + 1, a, b, m)}
            names.append(out)
            if tick:
                tick(0.7 * (i + 1) / max(1, len(segs)), '切段 %d/%d' % (i + 1, len(segs)))
        lst = os.path.join(tmp, 'list.txt')
        with io.open(lst, 'w', encoding='utf-8', newline='\n') as f:
            for n in names:
                f.write("file '%s'\n" % n.replace('\\', '/'))
        say('  拼接中…（%d 段）' % len(names))
        rc, so, se = _run([FFMPEG, '-hide_banner', '-v', 'error', '-nostdin', '-f', 'concat',
                           '-safe', '0', '-i', lst, '-c', 'copy', '-bsf:a', 'aac_adtstoasc',
                           '-movflags', '+faststart', '-y', stage], 7200)
        if rc != 0 or not os.path.isfile(stage) or os.path.getsize(stage) < 10240:
            return {'ok': False, 'error': '拼接失败: %s' % (se or '')[-160:]}
        if tick:
            tick(0.9, '校验')
        p2 = probe(stage)
        d2 = float(p2.get('duration') or 0)
        if p2['cls'] != 'ok':
            return {'ok': False, 'error': '产物不可播（%s）' % (p2.get('reason') or p2['cls'])}
        if d2 < dur - total_tol - 5.0 or d2 > dur + 5.0:
            return {'ok': False, 'error': '产物时长异常（%.1fs vs 源 %.1fs，损坏段共 %.1fs）'
                    % (d2, dur, total_tol)}
        bads = 0
        for a, b in merged[:6]:
            for t in (max(0.0, a - 1.0), min(max(0.0, d2 - 2.5), b + 1.0)):
                bad, nb = _probe_at(stage, t, 2.0)
                if bad:
                    bads += 1
        if bads:
            return {'ok': False, 'error': '拼接点抽窗仍有 %d 处报错' % bads}
        if _stop_req():
            return {'ok': False, 'error': '已请求停止（未替换）', 'stage': stage}
        orig = path + '.orig'
        k = 1
        while os.path.exists(orig):
            orig = '%s.orig%d' % (path, k)
            k += 1
        try:
            os.replace(path, orig)
            _copy_progress(stage, path, log=log)
        except Exception as e:
            return {'ok': False, 'error': '替换失败: %s' % str(e)[:120], 'orig': orig, 'stage': stage}
        try:
            os.remove(stage)
        except Exception:
            pass
        return {'ok': True, 'kind': 'splice', 'path': path, 'orig_backup': orig,
                'windows': merged, 'kept_ratio': kept / max(1e-6, dur),
                'undo': {'action': 'restore', 'path': path, 'backup': orig},
                'secs': round(time.time() - t0, 1)}
    finally:
        try:
            import shutil as _sh
            _sh.rmtree(tmp, ignore_errors=True)
        except Exception:
            pass


def undo_restore(path, backup):
    """把 .orig 备份还原回原路径（② 与 ③ 共用）。"""
    try:
        if not backup or not os.path.isfile(backup):
            return {'ok': False, 'error': '备份不存在'}
        shutil.copy2(backup, path)
        return {'ok': True, 'path': path, 'from': backup}
    except Exception as e:
        return {'ok': False, 'error': str(e)[:120]}


def interp_gap(path, gaps=None, log=None, tick=None, mi_mode='mci', verify=True, dry=False):
    """② 小洞插帧：用洞两侧的帧插出过渡帧，重编码为**时间轴不变**的新文件，
    校验通过后**原地替换**原文件（原文件先改名为 .orig 作为回滚点）。

    代价：minterpolate 实测 ~0.034× 实时（720p），所以 5 秒的洞约 2.5 分钟。
    """
    say = log or (lambda *a: None)
    t_start = time.time()
    try:
        gs = gaps if gaps is not None else find_gaps(path)
    except Exception:
        gs = []
    gs = sorted([g for g in gs if g['dur'] <= GAP_SEC], key=lambda g: g['t1'])
    if not gs:
        return {'ok': False, 'error': '没有 <= %.0fs 的小洞（大洞应走重编码）' % GAP_SEC}
    W, H, fps = _ffprobe_wh(path)
    if not W or not H:
        return {'ok': False, 'error': '读不到视频参数'}
    os.makedirs(OUT_DIR, exist_ok=True)
    base = os.path.splitext(os.path.basename(path))[0]
    stage = os.path.join(OUT_DIR, base + '.gapfix.mp4')
    pad = 0.2
    say('  洞 %d 处：%s' % (len(gs), ', '.join('%.1f-%.1fs(%.1fs)' % (g['t1'], g['t2'], g['dur']) for g in gs)))
    inputs, segs = ['-i', path], []
    for i, g in enumerate(gs):
        # 两侧都多偏移重试：洞附近的数据可能坏很长一段，就近取不到就往外找
        pick = None
        for d in (0.2, 0.5, 1.0, 2.0):
            xt = max(0.0, g['t1'] - d)
            xf = os.path.join(OUT_DIR, '_gap%d_x.png' % i)
            if _grab_png(path, xt, xf):
                pick = [xt, xf]; break
        after = None
        for d in (0.2, 0.5, 1.0, 2.0):
            yt = g['t2'] + d
            yf = os.path.join(OUT_DIR, '_gap%d_y.png' % i)
            if _grab_png(path, yt, yf):
                after = [yt, yf]; break
        if not pick or not after:
            return {'ok': False, 'error': '取边界帧失败 @ %.1fs（两侧 ±2s 都取不到，建议走 ③ 重编码）'
                    % g['t1']}
        a, xf = pick
        b, yf = after
        dur = max(0.2, b - a)
        if d_used := max(g['t1'] - a, b - g['t2']):
            say('  洞 %d：边界帧 %.2fs / %.2fs（洞 %.2f-%.2f，向外让 %.1fs）'
                % (i + 1, a, b, g['t1'], g['t2'], d_used))
        fr = max(1.0, fps or 25.0)
        keep = dur + 0.5
        inputs += ['-loop', '1', '-framerate', '%.4f' % fr, '-t', '%.3f' % keep, '-i', xf,
                   '-loop', '1', '-framerate', '%.4f' % fr, '-t', '%.3f' % keep, '-i', yf]
        segs.append((a, b, 1 + i * 2, 2 + i * 2, dur))

    fc, labs, n = [], [], 1
    prev = 0.0
    for k, (a, b, xi, yi, dur) in enumerate(segs):
        if a > prev + 0.01:
            fc.append('[0:v]trim=%.3f:%.3f,setpts=PTS-STARTPTS[v%d]' % (prev, a, n))
            labs.append('[v%d]' % n); n += 1
        fc.append('[%d:v]scale=%d:%d,format=yuv420p,setpts=PTS-STARTPTS,fps=%.4f,'
                  'trim=duration=%.3f[x%d];'
                  '[%d:v]scale=%d:%d,format=yuv420p,setpts=PTS-STARTPTS,fps=%.4f,'
                  'trim=duration=%.3f[y%d];'
                  "[x%d][y%d]blend=all_expr='A*(1-T/%.3f)+B*(T/%.3f)',format=yuv420p,"
                  'setpts=PTS-STARTPTS[v%d]'
                  % (xi, W, H, fr, dur, n, yi, W, H, fr, dur, n,
                     n, n, dur, dur, n + 1))
        labs.append('[v%d]' % (n + 1)); n += 2
        prev = b
    fc.append('[0:v]trim=start=%.3f,setpts=PTS-STARTPTS[v%d]' % (prev, n))
    labs.append('[v%d]' % n); n += 1
    fc.append('%sconcat=n=%d:v=1:a=0[v]' % (''.join(labs), len(labs)))
    graph = ';'.join(fc)
    say('  minterpolate(%s) 插帧 -> %s（预计 %.1f 分钟）'
        % (mi_mode, os.path.basename(stage), sum(s[4] for s in segs) / 0.034 / 60.0))
    _th, _ft = _threads(), _fthreads()
    say('  线程：编码 %d / 滤波 %d（逻辑核 %d）' % (_th, _ft, _cores()))
    cmd = [FFMPEG, '-hide_banner', '-filter_threads', str(_ft),
           '-err_detect', 'ignore_err'] + inputs + \
          ['-filter_complex', graph, '-map', '[v]', '-map', '0:a?',
           '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '20', '-threads', str(_th),
           '-c:a', 'aac', '-y', stage]
    if dry:
        return {'ok': False, 'dry': True, 'cmd': cmd, 'graph': graph, 'gaps': len(gs),
                'segs': [[round(s[0], 2), round(s[1], 2)] for s in segs]}
    src_dur = 0.0
    try:
        rc, so, se = _run([FFPROBE, '-v', 'error', '-show_entries', 'format=duration',
                           '-of', 'default=nw=1:nk=1', path], 90)
        src_dur = float((so or '0').strip() or 0)
    except Exception:
        pass
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=False)
    tail = []
    for line in _lines(p):
        tail.append(line.rstrip())
        if len(tail) > 14:
            tail.pop(0)
        m = re.search(r'time=(\d+):(\d+):([\d.]+)', line)
        if m and src_dur and tick:
            secs = int(m.group(1)) * 3600 + int(m.group(2)) * 60 + float(m.group(3))
            tick(min(secs / src_dur, 0.995), '插帧 %.0f%%' % (100 * secs / src_dur))
    p.wait()
    for i in range(len(segs)):
        for s in ('_gap%d_x.png' % i, '_gap%d_y.png' % i):
            try:
                os.remove(os.path.join(OUT_DIR, s))
            except Exception:
                pass
    if p.returncode != 0 or not os.path.isfile(stage) or os.path.getsize(stage) < 2048:
        err = ' | '.join([t for t in tail[-4:] if t.strip()])[:320]
        say('  ffmpeg 末尾输出: ' + err)
        return {'ok': False, 'error': 'ffmpeg 失败 rc=%s: %s' % (p.returncode, err), 'stage': stage,
                'cmd': ' '.join(cmd)}

    if verify:
        pr = probe(stage)
        if pr['cls'] != 'ok':
            return {'ok': False, 'error': '产物不可播（%s）' % (pr.get('reason') or pr['cls']), 'stage': stage}
        d2 = float(pr.get('duration') or 0)
        if src_dur and d2 and abs(d2 - src_dur) > max(1.0, src_dur * 0.01):
            return {'ok': False, 'error': '时长不符（%.1f vs %.1f）' % (d2, src_dur), 'stage': stage}
        w = probe_windows(stage, n=4, sec=6)
        say('  产物校验：可播=%s 时长=%.1fs 抽窗错误窗口=%d' % (pr['cls'] == 'ok', d2, len(w.get('bad') or [])))

    orig = path + '.orig'
    k = 1
    while os.path.exists(orig):
        orig = '%s.orig%d' % (path, k); k += 1
    try:
        os.replace(path, orig)                 # 原文件 -> 回滚点（同卷改名，不占空间）
        _copy_progress(stage, path, log=log)
    except Exception as e:
        return {'ok': False, 'error': '替换失败: %s' % str(e)[:110], 'stage': stage, 'orig': orig}
    try:
        os.remove(stage)
    except Exception:
        pass
    say('  已原地替换（回滚点 %s，耗时 %.0fs）' % (os.path.basename(orig), time.time() - t_start))
    return {'ok': True, 'kind': 'gap', 'path': path, 'orig_backup': orig, 'gaps': len(gs),
            'undo': {'action': 'restore', 'path': path, 'backup': orig},
            'secs': round(time.time() - t_start, 1)}


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

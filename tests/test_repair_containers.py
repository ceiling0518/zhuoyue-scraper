# -*- coding: utf-8 -*-
"""容器级检查自测：co64（64 位偏移）+ MKV/TS 结构检查。

  a) MP4：健康片 check_index 干净
  b) 把 stco 改造成 co64（字节级改造，mdat 在 moov 前时 chunk 偏移仍有效）：
     偏移读数必须与原 stco 完全一致、check_index 干净、ffprobe 仍可读
  c) MKV：健康片干净；截断 30% 后应报「Segment 被截」
  d) TS：健康片干净；截成非 188 整数倍后应报残包/不同步
"""
import os
import shutil
import struct as st
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import repair as RP                                                     # noqa: E402

T = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_fixture_ctn')
shutil.rmtree(T, ignore_errors=True)
os.makedirs(T, exist_ok=True)
fails = []


def mk_clip(path, sec=12, extra=None):
    cmd = [RP.FFMPEG, '-hide_banner', '-v', 'error', '-f', 'lavfi',
           '-i', 'testsrc2=size=320x240:rate=25', '-f', 'lavfi', '-i', 'sine=frequency=440',
           '-t', str(sec), '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
           '-c:a', 'aac']
    if extra:
        cmd += extra
    cmd += ['-shortest', '-y', path]
    rc, so, se = RP._run(cmd, 300)
    return rc == 0


def boxes(b, s, e):
    out, o = [], s
    while o + 8 <= e:
        l = st.unpack('>I', b[o:o + 4])[0]
        t = b[o + 4:o + 8]
        hl = 8
        if l == 1:
            l = st.unpack('>Q', b[o + 8:o + 16])[0]
            hl = 16
        elif l == 0:
            l = e - o
        if l < hl:
            break
        out.append((t, o, l, hl))
        o += l
    return out


def mk_co64(src, dst):
    """把 moov 里各轨 stco 换成 co64，并修正各级父 box 长度。"""
    data = open(src, 'rb').read()
    tops = {}
    for (t, o, l, hl) in boxes(data, 0, len(data)):
        tops.setdefault(t, (o, l, hl))
    if b'moov' not in tops or b'mdat' not in tops:
        return None
    mo, ml, mhl = tops[b'moov']
    if tops[b'mdat'][0] > mo:
        return None                       # mdat 必须在 moov 之前，改造后偏移才仍有效

    def fix(b, s, e, in_stbl):
        parts = []
        for (t, o, l, hl) in boxes(b, s, e):
            if t == b'stco' and in_stbl:
                n = st.unpack('>I', b[o + hl + 4:o + hl + 8])[0]
                co = list(st.unpack('>%dI' % n, b[o + hl + 8:o + hl + 8 + n * 4]))
                body = b[o + hl:o + hl + 8] + st.pack('>%dQ' % n, *co)
                parts.append(st.pack('>I', 8 + len(body)) + b'co64' + body)
            elif t in (b'trak', b'mdia', b'minf', b'stbl'):
                inner = fix(b, o + hl, o + l, in_stbl or t == b'stbl')
                parts.append(st.pack('>I', hl + len(inner)) + t + inner)
            else:
                parts.append(b[o:o + l])
        return b''.join(parts)

    body = fix(data, mo + mhl, mo + ml, False)
    newmoov = st.pack('>I', 8 + len(body)) + b'moov' + body
    with open(dst, 'wb') as f:
        f.write(data[:mo] + newmoov + data[mo + ml:])
    return dst


src = os.path.join(T, 'a.mp4')
if not mk_clip(src):
    print("  夹具失败"); sys.exit(1)
print("  夹具 %.2fMB" % (os.path.getsize(src) / 1048576))

print("\n  [a] 健康 MP4")
ok, iss = RP.check_index(src)
print("      check_index ok=%s issues=%s" % (ok, iss))
if ok is not True:
    fails.append('健康 mp4 被误报: %s' % iss)

print("\n  [b] stco -> co64 改造")
co64f = mk_co64(src, os.path.join(T, 'co64.mp4'))
if not co64f:
    print("      跳过（夹具布局不适配）")
else:
    def offs(p):
        with open(p, 'rb') as fp:
            sz = os.path.getsize(p)
            tops = {t: (o, l) for t, o, l, hl in RP._boxes(fp, 0, sz, limit=64)}
            mo = tops['moov'][0]
            fp.seek(mo)
            buf = fp.read()
        tr = [b for b in RP._boxes(RP._MemFile(buf), 8, len(buf)) if b[0] == 'trak']
        return RP._track_tables(buf, tr[0], mo)
    v0, v1 = offs(src), offs(co64f)
    same = v0['off'][:50] == v1['off'][:50] and v0['cb'][:50] == v1['cb'][:50]
    print("      改造后 co64=%s  偏移与 stco 一致=%s（各 %d / %d 条）"
          % (v1.get('co64'), same, len(v0['off']), len(v1['off'])))
    if not v1.get('co64'):
        fails.append('改造后未识别为 co64')
    if not same:
        fails.append('co64 偏移读数与 stco 不一致')
    ok2, iss2 = RP.check_index(co64f)
    pr2 = RP.probe(co64f)
    print("      check_index ok=%s issues=%s | probe=%s | 尺寸 %.2fMB"
          % (ok2, iss2, pr2['cls'], os.path.getsize(co64f) / 1048576))
    if ok2 is not True:
        fails.append('co64 文件被误报: %s' % iss2)
    if pr2['cls'] != 'ok':
        fails.append('co64 文件 probe 应为 ok，实际 %s' % pr2['cls'])

print("\n  [c] MKV")
m0 = os.path.join(T, 'a.mkv')
mk_clip(m0, 12, ['-f', 'matroska'])
okm, ism = RP.check_index(m0)
print("      健康 mkv: ok=%s issues=%s" % (okm, ism))
if okm is not True:
    fails.append('健康 mkv 被误报: %s' % ism)
m1 = os.path.join(T, 'cut.mkv')
shutil.copy2(m0, m1)
szm = os.path.getsize(m1)
with open(m1, 'r+b') as f:
    f.truncate(int(szm * 0.7))
okm2, ism2 = RP.check_index(m1)
print("      截 30%% 后: ok=%s issues=%s" % (okm2, ism2[:2]))
if okm2 is not False or not ism2:
    fails.append('截断 mkv 未被检出')
prm = RP.probe(m1)
if prm['cls'] not in ('index-bad', 'ok', 'other'):
    fails.append('截断 mkv probe 异常: %s' % prm['cls'])
print("      截断 mkv probe=%s（备注 %s）" % (prm['cls'], (prm.get('reason') or '')[:48]))

print("\n  [d] TS")
t0 = os.path.join(T, 'a.ts')
mk_clip(t0, 12, ['-f', 'mpegts'])
okt, ist = RP.check_index(t0)
print("      健康 ts: ok=%s issues=%s" % (okt, ist[:2]))
if okt is not True:
    fails.append('健康 ts 被误报: %s' % ist)
t1 = os.path.join(T, 'cut.ts')
shutil.copy2(t0, t1)
szt = os.path.getsize(t1)
with open(t1, 'r+b') as f:
    f.truncate(szt - 500)          # 非 188 整数倍
okt2, ist2 = RP.check_index(t1)
print("      截 500 字节后: ok=%s issues=%s" % (okt2, ist2[:2]))
if okt2 is not False or not ist2:
    fails.append('截断 ts 未被检出')

shutil.rmtree(T, ignore_errors=True)
print("\n  >>> %s" % ("全部通过 ✅" if not fails else "未通过: %s" % fails))
sys.exit(1 if fails else 0)

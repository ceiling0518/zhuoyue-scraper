# -*- coding: utf-8 -*-
"""分批插帧（interp_splice_repair）自测。

夹具：30 秒、1 秒 GOP，均匀挖 20 个小洞。
验证：
  a) ok=True，产物可播
  b) 未损坏区间逐帧哈希与修复前完全一致（-c copy 零损失）
  c) 产物里**不再有 >=64KB 数据洞**（洞已被过渡段替换）
"""
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import repair as RP                                                     # noqa: E402

T = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_fixture_isplice')
shutil.rmtree(T, ignore_errors=True)
os.makedirs(T, exist_ok=True)
fails = []
PTS = (0.5, 29.0)


def framemd5(path, a, sec=1.5):
    rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error', '-nostdin',
                          '-ss', '%.2f' % a, '-i', path, '-t', '%.1f' % sec,
                          '-an', '-f', 'framemd5', '-'], 300)
    out = []
    for l in (so or '').splitlines():
        if l.startswith('#'):
            continue
        p = l.split(',')
        if len(p) >= 6:
            out.append(p[-1].strip())
    return out


def common_run(x, y):
    best = 0
    pos = {}
    for i, v in enumerate(y):
        pos.setdefault(v, i)
    i = 0
    while i < len(x):
        j = pos.get(x[i])
        if j is None:
            i += 1
            continue
        k = 0
        while i + k < len(x) and j + k < len(y) and x[i + k] == y[j + k]:
            k += 1
        best = max(best, k)
        i += max(1, k)
    return best


src = os.path.join(T, 'many.mp4')
rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error', '-f', 'lavfi',
                      '-i', 'testsrc2=size=640x480:rate=25', '-f', 'lavfi', '-i', 'sine=frequency=440',
                      '-t', '36', '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '20', '-g', '25',
                      '-c:a', 'aac', '-shortest', '-y', src], 300)
if rc:
    print("  夹具失败"); sys.exit(1)
m0, m1 = RP.mdat_range(src)
N = 8
step = (m1 - m0) // (N + 1)
with open(src, 'r+b') as fp:
    for i in range(1, N + 1):
        fp.seek(m0 + i * step)
        fp.write(b'\x00' * 120000)
holes = RP.find_gaps(src)
gaps = RP.holes_to_time(src, holes)
dur = float(RP.probe(src).get('duration') or 0)
print("  夹具 %.2fMB 时长 %.1fs；挖了 %d 个洞，检出 %d 个（合计 %.1fs）"
      % (os.path.getsize(src) / 1048576.0, dur, N, len(holes), sum(g['dur'] for g in gaps)))
if len(gaps) < 8:
    print("  !! 洞太少，测试意义不足"); sys.exit(1)

PRE = {t: framemd5(src, t) for t in PTS}
print("  修复前取样：%s" % {k: len(v) for k, v in PRE.items()})
res = RP.interp_splice_repair(src, gaps, log=lambda s: print("     " + s))
print("\n  interp_splice_repair -> ok=%s %s" % (res.get('ok'), (res.get('error') or '')[:130]))
if not res.get('ok'):
    fails.append('未通过: %s' % res.get('error'))
else:
    p = RP.probe(src)
    print("  产物: cls=%s 时长=%.1fs（源 %.1fs）大小=%.2fMB"
          % (p['cls'], float(p.get('duration') or 0), dur, os.path.getsize(src) / 1048576.0))
    if p['cls'] != 'ok':
        fails.append('产物不可播: %s' % p['cls'])
    chk = RP.scan_holes_chunk(src, 0, 1 << 62)
    hs = chk.get('holes') or []
    print("  产物里的数据洞：%d 处%s" % (len(hs), ('（最大 %.0fKB）' % (max(h['size'] for h in hs) / 1024.0)) if hs else ''))
    if hs:
        fails.append('产物仍有洞 %d 处' % len(hs))
    print("\n  逐帧哈希对比（产物 vs 修复前，未损坏区间应一致）：")
    for t in PTS:
        post = framemd5(src, t)
        run = common_run(PRE[t], post)
        print("     t=%.1fs  修复前 %d 帧 / 产物 %d 帧；公共连续帧 %d" % (t, len(PRE[t]), len(post), run))
        if len(PRE[t]) >= 20 and run < len(PRE[t]) - 2:
            fails.append('t=%.1fs 未损坏区不一致（%d/%d）' % (t, run, len(PRE[t])))

shutil.rmtree(T, ignore_errors=True)
print("\n  >>> %s" % ("全部通过 ✅（零损失 + 洞已填）" if not fails else "未通过: %s" % fails))
sys.exit(1 if fails else 0)

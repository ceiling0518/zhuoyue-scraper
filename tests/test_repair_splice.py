# -*- coding: utf-8 -*-
"""分段修复（splice）自测 —— 重点证明「未损坏部分零损失」。

夹具：16 秒 640x480，中途抹掉一块（造洞）。
流程：深检式找洞 -> 映射时间窗口 -> 取**修复前**未损坏区间的逐帧哈希
      -> splice_repair -> 再取同一区间的逐帧哈希 -> 必须逐帧一致。
（若被重编码，绝大多数帧哈希都会变；-c copy 则完全一致。）
"""
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import repair as RP                                                     # noqa: E402

T = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_fixture_splice')
shutil.rmtree(T, ignore_errors=True)
os.makedirs(T, exist_ok=True)
fails = []
PTS = (1.0, 14.0)                      # 头尾，远离损坏窗口（吸附后会打印实际窗口再校验）


def framemd5(path, a, sec=3.0):
    rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error', '-nostdin',
                          '-ss', '%.2f' % a, '-i', path, '-t', '%.1f' % sec,
                          '-an', '-f', 'framemd5', '-'], 300)
    out = []
    for l in (so or '').splitlines():
        if l.startswith('#'):
            continue
        parts = l.split(',')
        if len(parts) >= 6:
            out.append(parts[-1].strip())
    return out


def longest_common_run(x, y):
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


src = os.path.join(T, 'sp.mp4')
rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error', '-f', 'lavfi',
                      '-i', 'testsrc2=size=640x480:rate=25', '-f', 'lavfi', '-i', 'sine=frequency=440',
                      '-t', '16', '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '20', '-g', '25',
                      '-c:a', 'aac', '-shortest', '-y', src], 300)
if rc:
    print("  夹具失败"); sys.exit(1)
m0, m1 = RP.mdat_range(src)
with open(src, 'r+b') as fp:
    fp.seek(m0 + (m1 - m0) // 2)
    fp.write(b'\x00' * (200 * 1024))
holes = RP.find_gaps(src)
wins = RP.holes_to_time(src, [h for h in holes])
dur = float(RP.probe(src).get('duration') or 0)
print("  夹具 %.2fMB 时长 %.1fs；洞 %d 处 -> 窗口 %s"
      % (os.path.getsize(src) / 1048576.0, dur, len(holes),
         [(w['t1'], w['t2']) for w in wins]))
if not wins:
    print("  !! 没造出洞"); sys.exit(1)

print("\n  取样修复前未损坏区间的逐帧哈希…")
PRE = {t: framemd5(src, t, 3.0) for t in PTS}
print("     %s" % {k: len(v) for k, v in PRE.items()})

res = RP.splice_repair(src, wins, log=lambda s: print("     " + s))
print("\n  splice_repair -> ok=%s %s" % (res.get('ok'), (res.get('error') or '')[:120]))
if not res.get('ok'):
    fails.append('splice 未通过: %s' % res.get('error'))
else:
    p = RP.probe(src)
    d2 = float(p.get('duration') or 0)
    print("  产物: cls=%s 时长=%.1fs（源 %.1fs）大小=%.2fMB 零损失覆盖=%.1f%%"
          % (p['cls'], d2, dur, os.path.getsize(src) / 1048576.0,
             100.0 * (res.get('kept_ratio') or 0)))
    if p['cls'] != 'ok':
        fails.append('产物不可播: %s' % p['cls'])
    if abs(d2 - dur) > max(3.0, 0.06 * dur):
        fails.append('时长偏差过大 %.1f vs %.1f' % (d2, dur))
    wins_out = res.get('windows') or []
    print("\n  实际窗口（按关键帧吸附后）：%s" % wins_out)
    print("\n  逐帧哈希对比（产物 vs 修复前；只比对窗口之外的区间）：")
    for t in PTS:
        if any(a - 1.0 <= t <= b + 1.0 for a, b, *_ in wins_out):
            print("     t=%.1fs 落在窗口内（会被重编码），跳过比对" % t)
            continue
        post = framemd5(src, t, 3.0)
        run = longest_common_run(PRE[t], post)
        print("     t=%.1fs  修复前 %d 帧 / 产物 %d 帧；公共连续帧 %d"
              % (t, len(PRE[t]), len(post), run))
        if len(PRE[t]) < 30:
            fails.append('t=%.1fs 取样帧太少(%d)' % (t, len(PRE[t])))
        elif run < len(PRE[t]) - 2:
            fails.append('t=%.1fs 未损坏区不一致（公共 %d / %d）—— 说明被重编码了'
                         % (t, run, len(PRE[t])))
    print("\n  回滚点: %s" % ('在' if os.path.isfile(src + '.orig') or os.path.isfile(src + '.orig1') else '无'))

shutil.rmtree(T, ignore_errors=True)
print("\n  >>> %s" % ("全部通过 ✅（未损坏部分零损失已证明）" if not fails else "未通过: %s" % fails))
sys.exit(1 if fails else 0)

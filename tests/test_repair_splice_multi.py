# -*- coding: utf-8 -*-
"""多窗口分段修复自测（守住两个真实 bug：吸附后窗口重叠 / 时间轴回退）。

夹具：36 秒、1 秒 GOP，挖 10 个洞 -> 直接调 splice_repair（纯窗口，无 clip）。
验证：
  a) ok=True、产物可播、时长基本一致
  b) 返回的窗口单调且互不重叠（重叠是曾经的 bug，会让段数膨胀 + 时间轴回退）
  c) **零损失**：在「搬运区间」内取样，比较**帧序列**（允许时间戳偏移；
     TS 拼接会带来每边界约一帧的漂移，逐点单帧比对必然误判）
"""
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import repair as RP                                                     # noqa: E402

T = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_fixture_splicemulti')
shutil.rmtree(T, ignore_errors=True)
os.makedirs(T, exist_ok=True)
fails = []
LINES = []


def seq(path, t, sec=1.0):
    rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error', '-nostdin',
                          '-ss', '%.3f' % t, '-i', path, '-t', '%.2f' % sec,
                          '-an', '-f', 'framemd5', '-'], 180)
    out = []
    for l in (so or '').splitlines():
        if l.startswith('#'):
            continue
        p = l.split(',')
        if len(p) >= 6:
            out.append(p[-1].strip())
    return out


def longest(x, y):
    """x 的帧序列在 y 里最长的「连续相同」段；返回 (长度, x 起点, y 起点)。

    y 起点 - x 起点 = 该点的时间轴漂移（帧）。
    """
    best, pos = (0, -1, -1), {}
    for i, v in enumerate(y):
        pos.setdefault(v, []).append(i)
    for i, v in enumerate(x):
        for j in pos.get(v, []):
            k = 0
            while i + k < len(x) and j + k < len(y) and x[i + k] == y[j + k]:
                k += 1
            if k > best[0]:
                best = (k, i, j)
    return best


src = os.path.join(T, 'many.mp4')
rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error', '-f', 'lavfi',
                      '-i', 'testsrc2=size=640x480:rate=25', '-f', 'lavfi', '-i', 'sine=frequency=440',
                      '-t', '36', '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '20', '-g', '25',
                      '-c:a', 'aac', '-shortest', '-y', src], 300)
if rc:
    print("  夹具失败"); sys.exit(1)
m0, m1 = RP.mdat_range(src)
N = 7
step = (m1 - m0) // (N + 1)
with open(src, 'r+b') as fp:
    for i in range(1, N + 1):
        fp.seek(m0 + i * step)
        fp.write(b'\x00' * 120000)
holes = RP.find_gaps(src)
gaps = RP.holes_to_time(src, holes)
dur = float(RP.probe(src).get('duration') or 0)
print("  夹具 %.2fMB 时长 %.1fs；挖 %d 个洞，检出 %d 个（合计 %.1fs）"
      % (os.path.getsize(src) / 1048576.0, dur, N, len(holes), sum(g['dur'] for g in gaps)))
if len(gaps) < 6:
    print("  !! 洞太少"); sys.exit(1)

PTS = [round(0.3 + 1.0 * i, 2) for i in range(0, 34)]
PTS = [t for t in PTS if t + 1.0 < dur]
print("  修复前取样 %d 点（每点 1.2s 帧序列）…" % len(PTS))
PRE = {t: seq(src, t) for t in PTS}

res = RP.splice_repair(src, gaps, log=lambda s: (LINES.append(s), print("     " + s)))
print("\n  splice_repair -> ok=%s %s" % (res.get('ok'), (res.get('error') or '')[:140]))
if not res.get('ok'):
    fails.append('未通过: %s' % res.get('error'))
else:
    txt = "\n".join(LINES)
    wins_out = res.get('windows') or []
    last = None
    for w in wins_out:
        a = float(w[0])
        b = float(w[1]) if len(w) > 1 else a
        if last and a < last[1] - 1e-6:
            fails.append('窗口重叠/回退: %.1f-%.1f 与上一窗口 %.1f-%.1f' % (a, b, last[0], last[1]))
        last = (a, b)
    if '合并重叠窗口' in txt:
        print("  （本次吸附产生了重叠，已自动合并 ✅）")
    p = RP.probe(src)
    pdur = float(p.get('duration') or 0)
    print("  产物: cls=%s 时长=%.1fs（源 %.1fs）大小=%.2fMB 窗口 %d 个"
          % (p['cls'], pdur, dur, os.path.getsize(src) / 1048576.0, len(wins_out)))
    if p['cls'] != 'ok':
        fails.append('产物不可播: %s' % p['cls'])
    if abs(pdur - dur) > 1.0:
        fails.append('时长差过大（%+.2fs）' % (pdur - dur))
    W = [(float(w[0]), float(w[1])) for w in wins_out]
    safe = [t for t in PTS if all(t + 1.0 < a - 0.2 or t > b + 0.2 for a, b in W)]
    print("  搬运区间内的安全采样点：%d 个" % len(safe))
    bad, drifts = [], []
    # POST 取样窗放宽（前后各留 0.6s）：TS 分段每段会多出 ~1 个 AAC 帧
    # （1024/44100 = 0.023s）造成累积漂移 —— 逐帧内容仍一致，只是时间轴后移，
    # 所以比对必须允许这个整体偏移，不能用同宽的窗口硬比。
    for t in safe:
        post = seq(src, max(0.0, t - 0.6), sec=2.2)
        n, ix, iy = longest(PRE[t], post)
        drifts.append(iy - ix)
        if not PRE[t] or n < len(PRE[t]):
            bad.append((t, n, len(PRE[t])))
    if drifts:
        print("  时间轴漂移（帧）: %s" % ' '.join('%+d' % d for d in drifts[:12]))
    print("  序列比对：通过 %d / 不一致 %d %s"
          % (len(safe) - len(bad), len(bad), ('（例：%s）' % bad[:4]) if bad else ''))
    if len(safe) < 6:
        fails.append('安全采样点太少（%d）' % len(safe))
    if bad:
        fails.append('搬运区间帧序列不一致 %d 处' % len(bad))

shutil.rmtree(T, ignore_errors=True)
print("\n  >>> %s" % ("全部通过 ✅（零损失 + 窗口无重叠）" if not fails else "未通过: %s" % fails))
sys.exit(1 if fails else 0)

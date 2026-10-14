# -*- coding: utf-8 -*-
"""深检断点续扫 自测（合成夹具）。

  a) mdat_range 合理
  b) 分多轮小段扫（模拟大文件）-> 能扫出洞、最终 done=True
  c) 「中断后续扫」：第 2 轮后停手，保存 off；从该 off 续扫到底
     -> 结果必须与一次性扫到底**完全一致**，且不重复
"""
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import repair as RP                                                     # noqa: E402

T = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_fixture_deep')
shutil.rmtree(T, ignore_errors=True)
os.makedirs(T, exist_ok=True)
fails = []

src = os.path.join(T, 'deep.mp4')
rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error', '-f', 'lavfi',
                      '-i', 'testsrc2=size=640x480:rate=25', '-f', 'lavfi', '-i', 'sine=frequency=440',
                      '-t', '30', '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '20',
                      '-c:a', 'aac', '-shortest', '-y', src], 300)
if rc:
    print("  夹具失败:", se[:80]); sys.exit(1)
sz = os.path.getsize(src)
m0, m1 = RP.mdat_range(src)
print("  夹具 %.2fMB  mdat %d..%d（%.2fMB）" % (sz / 1048576, m0, m1, (m1 - m0) / 1048576))
if not (m0 > 0 and m1 > m0):
    fails.append('mdat_range 不合理')

# 在 mdat 中间抹两个洞
holes_at = [(m0 + (m1 - m0) // 3, 160 * 1024), (m0 + 2 * (m1 - m0) // 3, 120 * 1024)]
with open(src, 'r+b') as fp:
    for at, n in holes_at:
        fp.seek(at)
        fp.write(b'\x00' * n)
print("  已抹 %d 个洞（%s）" % (len(holes_at), '、'.join('%dKB' % (n // 1024) for _, n in holes_at)))

print("\n  [b] 分轮小段扫（每轮 512KB，模拟大文件逐轮推进）")
CHUNK = 512 * 1024
off, holes, rounds = 0, [], 0
while True:
    r = RP.scan_holes_chunk(src, off, CHUNK)
    if r.get('error'):
        fails.append('续扫报错: %s' % r['error']); break
    holes += [h for h in r['holes']]
    off = r['next_off']
    rounds += 1
    if r['done']:
        break
    if rounds > 400:
        fails.append('轮次过多（未收敛）'); break
uniq = RP.merge_holes(holes)
print("      共 %d 轮；扫出洞 %d 处：%s" % (rounds, len(uniq),
                                          [(h['size'], h['start']) for h in uniq]))
if len(uniq) != len(holes_at):
    fails.append('洞数量不对：期望 %d 实际 %d' % (len(holes_at), len(uniq)))
for k, h in enumerate(uniq):
    if abs(h['size'] - holes_at[k][1]) > 8 * 1024:
        fails.append('第 %d 个洞大小偏差过大 %d vs %d' % (k + 1, h['size'], holes_at[k][1]))

print("\n  [c] 中断 -> 续扫（保存 off 再接着扫）")
off2, acc, n_round = 0, [], 0
while True:
    r = RP.scan_holes_chunk(src, off2, CHUNK)
    acc += r['holes']
    off2 = r['next_off']
    n_round += 1
    if n_round == 2:
        print("      第 2 轮后模拟中断：已扫到 %d 字节（mdat 的 %.0f%%）"
              % (off2, 100.0 * (off2 - m0) / max(1, m1 - m0)))
        break
if off2 <= m0:
    fails.append('中断点没有推进（%d）' % off2)
resumed_rounds = 0
while True:
    r = RP.scan_holes_chunk(src, off2, CHUNK)
    if r.get('error'):
        fails.append('续扫报错: %s' % r['error']); break
    acc += r['holes']
    off2 = r['next_off']
    resumed_rounds += 1
    if r['done']:
        break
    if resumed_rounds > 400:
        break
uniq2 = RP.merge_holes(acc)
same = [(h['start'], h['size']) for h in uniq2] == [(h['start'], h['size']) for h in uniq]
print("      续扫 %d 轮后：洞 %d 处；与一次性扫到底的结果一致=%s"
      % (resumed_rounds, len(uniq2), same))
print("      续扫结果: %s" % [(h['size'], h['start']) for h in uniq2])
if not same:
    fails.append('续扫结果与一次性扫描不一致（重复或漏）')

shutil.rmtree(T, ignore_errors=True)
print("\n  >>> %s" % ("全部通过 ✅" if not fails else "未通过: %s" % fails))
sys.exit(1 if fails else 0)

# -*- coding: utf-8 -*-
"""索引自检 + 无损重封装 自测（合成夹具）。

  a) 健康片：check_index 干净、probe=ok、tier=ok
  b) 故意改坏一个 chunk 偏移：check_index 检出、probe=index-bad、tier=remux
  c) 对坏索引片做重封装 -> **必须被拒绝**（时长守门），且**原文件一个字节都不能动**
  d) 对健康片做重封装 -> 成功、时长保持、索引干净、可回滚
"""
import os
import shutil
import struct
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import repair as RP                                                     # noqa: E402

T = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_fixture_idx')
shutil.rmtree(T, ignore_errors=True)
os.makedirs(T, exist_ok=True)
fails = []


def mk(path, sec=20):
    rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error',
                          '-f', 'lavfi', '-i', 'testsrc2=size=320x240:rate=25',
                          '-f', 'lavfi', '-i', 'sine=frequency=440', '-t', str(sec),
                          '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
                          '-c:a', 'aac', '-shortest', path], 300)
    return rc == 0


src = os.path.join(T, 'idx.mp4')
good = os.path.join(T, 'good.mp4')
if not mk(src) or not mk(good, 8):
    print("  夹具生成失败"); sys.exit(1)
print("  夹具 idx.mp4=%.2fMB  good.mp4=%.2fMB"
      % (os.path.getsize(src) / 1048576, os.path.getsize(good) / 1048576))

print("\n  [a] 健康片")
ok, iss = RP.check_index(src)
print("      check_index ok=%s issues=%s" % (ok, iss))
if ok is not True or iss:
    fails.append('健康片被误报: %s' % iss)
if RP.probe(src)['cls'] != 'ok':
    fails.append('健康片 probe 应 ok')
if RP.plan_repair(src).get('tier') != 'ok':
    fails.append('健康片 tier 应 ok')

print("\n  [b] 改坏一个 chunk 偏移")
with open(src, 'rb') as fp:
    sz = os.path.getsize(src)
    tops = {t: (o, l) for t, o, l, hl in RP._boxes(fp, 0, sz, limit=64)}
    mo, ml = tops['moov']
    fp.seek(mo)
    buf = fp.read()
traks = [b for b in RP._boxes(RP._MemFile(buf), 8, len(buf)) if b[0] == 'trak']
sb = RP._path_box(buf, traks[0][1] + traks[0][3], traks[0][1] + traks[0][2], ['mdia', 'minf', 'stbl'])
ch = {c[0]: c for c in RP._boxes(RP._MemFile(buf), sb[0] + sb[2], sb[0] + sb[1])}
_, so_, sl_, shl = ch['stco']
nco = struct.unpack('>I', buf[so_ + shl + 4:so_ + shl + 8])[0]
idx = min(5, nco - 1)
ent = mo + so_ + shl + 8 + idx * 4
old = struct.unpack('>I', buf[so_ + shl + 8 + idx * 4:so_ + shl + 12 + idx * 4])[0]
with open(src, 'r+b') as fp:
    fp.seek(ent)
    fp.write(struct.pack('>I', 0xFFFFFF00))
print("      第 %d 个 chunk 偏移 %d -> 4278190080（stco %d 条）" % (idx, old, nco))
ok2, iss2 = RP.check_index(src)
print("      check_index ok=%s issues=%s" % (ok2, iss2[:2]))
if ok2 is not False or not iss2:
    fails.append('改坏后应检出')
pb = RP.probe(src)
print("      probe=%s" % pb['cls'])
if pb['cls'] != 'index-bad':
    fails.append('改坏后 probe 应 index-bad（实际 %s）' % pb['cls'])
zb = RP.plan_repair(src)
print("      tier=%s" % zb.get('tier'))
if zb.get('tier') != 'remux':
    fails.append('改坏后 tier 应 remux（实际 %s）' % zb.get('tier'))

print("\n  [c] 坏索引片做重封装 —— 必须被拒且原文件不动")
size_before = os.path.getsize(src)
dur_before = float(RP.probe(src).get('duration') or 0)
r = RP.remux_copy(src, log=lambda s: print("      " + s))
print("      remux_copy ok=%s  %s" % (r.get('ok'), (r.get('error') or '')[:120]))
if r.get('ok'):
    fails.append('坏索引片不该判为重封装成功')
if os.path.getsize(src) != size_before:
    fails.append('被拒时原文件体积变了（危险！）')
if abs(float(RP.probe(src).get('duration') or 0) - dur_before) > 0.5:
    fails.append('被拒时原文件时长变了（危险！）')
if not r.get('orig_backup'):
    print("      （未产生回滚点，符合预期：没有替换）")

print("\n  [d] 健康片重封装 —— 应成功且时长保持")
d0 = float(RP.probe(good).get('duration') or 0)
r2 = RP.remux_copy(good, log=lambda s: print("      " + s))
print("      remux_copy ok=%s %s" % (r2.get('ok'), (r2.get('error') or '')[:90]))
if not r2.get('ok'):
    fails.append('健康片重封装失败: %s' % r2.get('error'))
else:
    okg, issg = RP.check_index(good)
    d1 = float(RP.probe(good).get('duration') or 0)
    print("      修复后 probe=%s 时长=%.2fs（原 %.2fs） check_index ok=%s"
          % (RP.probe(good)['cls'], d1, d0, okg))
    if okg is not True:
        fails.append('健康片重封装后索引自检异常: %s' % issg)
    if abs(d1 - d0) > max(1.0, d0 * 0.01):
        fails.append('健康片重封装后时长偏差 %.2f vs %.2f' % (d1, d0))
    u = RP.undo_restore(good, r2['orig_backup'])
    print("      回滚 -> ok=%s" % u.get('ok'))
    if not u.get('ok'):
        fails.append('回滚失败')

shutil.rmtree(T, ignore_errors=True)
print("\n  >>> %s" % ("全部通过 ✅" if not fails else "未通过: %s" % fails))
sys.exit(1 if fails else 0)

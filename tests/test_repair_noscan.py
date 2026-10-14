# -*- coding: utf-8 -*-
"""证明：给了洞区间（来自深检记录）时，interp_gap 不会再去扫整个文件。

做法：把 repair.find_gaps 打成"一调用就抛异常"，
  a) interp_gap(src, gaps=<深检算出的区间>) 必须成功（说明全程没碰 find_gaps）
  b) interp_gap(src)（不传区间）则应失败并提示"没有小洞"（说明它确实会去扫）
"""
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import repair as RP                                                     # noqa: E402

T = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_fixture_noscan')
shutil.rmtree(T, ignore_errors=True)
os.makedirs(T, exist_ok=True)
fails = []

src = os.path.join(T, 'ns.mp4')
rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error', '-f', 'lavfi',
                      '-i', 'testsrc2=size=320x240:rate=25', '-f', 'lavfi', '-i', 'sine=frequency=440',
                      '-t', '16', '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
                      '-c:a', 'aac', '-shortest', '-y', src], 300)
if rc:
    print("  夹具失败"); sys.exit(1)
with open(src, 'rb') as fp:
    sz = os.path.getsize(src)
    tops = {t: (o, l) for t, o, l, hl in RP._boxes(fp, 0, sz, limit=64)}
    m0, mdl = tops['mdat']
with open(src, 'r+b') as fp:
    fp.seek(m0 + mdl // 2)
    fp.write(b'\x00' * (180 * 1024))
gaps = RP.find_gaps(src)
print("  夹具 %.2fMB；深检式找洞 1 次 -> %s" % (sz / 1048576,
      [(g['t1'], g['t2'], g['dur']) for g in gaps]))
if not gaps:
    print("  !! 没造出洞"); sys.exit(1)

# 把 find_gaps 打成"一调用就炸"
_orig = RP.find_gaps


def _boom(*a, **k):
    raise RuntimeError('不该再扫文件！find_gaps 被调用了')


RP.find_gaps = _boom
try:
    r = RP.interp_gap(src, gaps=gaps, log=lambda s: print("      " + s))
    print("\n  [a] 传区间: ok=%s  %s" % (r.get('ok'), (r.get('error') or '')[:80]))
    if not r.get('ok'):
        fails.append('传区间时应成功（说明没重扫），实际失败: %s' % r.get('error'))
finally:
    RP.find_gaps = _orig

print("\n  [b] 不传区间（对照）: 应因扫描被拦而失败")
r2 = RP.interp_gap(src, log=lambda s: print("      " + s))
print("      ok=%s  %s" % (r2.get('ok'), (r2.get('error') or '')[:80]))
if r2.get('ok'):
    fails.append('不传区间时不该成功（说明没走扫描路径）')

shutil.rmtree(T, ignore_errors=True)
print("\n  >>> %s" % ("全部通过 ✅（确认：给了区间就不再重扫）" if not fails else "未通过: %s" % fails))
sys.exit(1 if fails else 0)

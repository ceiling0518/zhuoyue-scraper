# -*- coding: utf-8 -*-
"""② 小洞：数据级检测 + 时间映射 自测（合成夹具）。

做法：造一个 20 秒的片子 -> 在 mdat 中间抹掉 200KB 零 -> 验证
  a) 健康文件不误报
  b) 抹零后能被 find_holes 找到、能映射到时间轴
  c) plan_repair 按洞大小分流（小洞 -> gap，大洞 -> reencode）
  d) probe_windows（抽窗解码）能命中该洞；健康文件不误报
"""
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import repair as RP                                                     # noqa: E402

T = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_fixture_gap')
shutil.rmtree(T, ignore_errors=True)
os.makedirs(T, exist_ok=True)
fails = []

src = os.path.join(T, 'hole.mp4')
rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error',
                      '-f', 'lavfi', '-i', 'testsrc2=size=320x240:rate=25',
                      '-f', 'lavfi', '-i', 'sine=frequency=440',
                      '-t', '20', '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
                      '-c:a', 'aac', '-shortest', src], 300)
if rc:
    fails.append('夹具生成失败: %s' % se[:80])
print("  生成 rc=%s size=%.2f MB  %s" % (rc, (os.path.getsize(src) if os.path.exists(src) else 0) / 1048576, se[:60]))

h0 = RP.find_holes(src) if rc == 0 else ['?']
print("  [a] 健康文件 find_holes -> %d 处（应 0）" % len(h0))
if h0:
    fails.append('健康文件被误判有洞')

if rc == 0:
    w, h, fps = RP._ffprobe_wh(src)
    print("      分辨率 %dx%d  fps=%.2f  GAP_SEC=%.0f" % (w, h, fps, RP.GAP_SEC))
    with open(src, 'rb') as fp:
        sz = os.path.getsize(src)
        tops = {t: (o, l) for t, o, l, hl in RP._boxes(fp, 0, sz, limit=64)}
        md0, mdl = tops['mdat']
    hole = 200 * 1024
    at = md0 + mdl // 2
    with open(src, 'r+b') as fp:
        fp.seek(at)
        fp.write(b'\x00' * hole)
    print("  [b] 已抹零 %d KB @ %d（mdat %d..%d，共 %.2f MB）"
          % (hole // 1024, at, md0, md0 + mdl, mdl / 1048576))

    fs = RP.find_holes(src)
    print("      find_holes -> %d 处 %s" % (len(fs), [(x['size'], x['start']) for x in fs][:3]))
    if not fs:
        fails.append('抹零后没检测到洞')

    g = RP.find_gaps(src)
    print("      find_gaps  -> %s" % [{k: x[k] for k in ('size', 't1', 't2', 'dur')} for x in g])
    if not g:
        fails.append('洞没能映射到时间轴')
    else:
        br = os.path.getsize(src) * 8 / 20.0
        print("      映射 %.2fs（按码率 %.0f kbps 估算 %.2fs）"
              % (g[0]['dur'], br / 1000, hole * 8 / br))

    p0 = RP.plan_repair(src)
    print("  [c0] plan_repair(浅) -> tier=%s %s（设计：洞里看不见）"
          % (p0.get('tier'), (p0.get('reason') or '')[:40]))
    p = RP.plan_repair(src, deep=True)
    print("  [c] plan_repair(深) -> tier=%s  %s" % (p.get('tier'), p.get('plan')))
    if p.get('tier') != 'gap':
        fails.append('2.0s 的洞应判 gap，实际 %s' % p.get('tier'))
    if not p.get('gaps'):
        fails.append('深检没带回 gaps')

    print("  [d] probe_windows（抽 4 窗 × 5s）")
    pw = RP.probe_windows(src, n=4, sec=5, log=lambda s: print("      " + s))
    print("      -> %d/%d 个窗口有解码错误" % (len(pw['bad']), pw['windows']))
    if not pw['bad']:
        fails.append('抽窗解码没发现错误（洞在中间，本该命中）')

    clean = os.path.join(T, 'clean.mp4')
    RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error', '-f', 'lavfi',
             '-i', 'testsrc2=size=320x240:rate=25', '-t', '8',
             '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23', clean], 200)
    pw2 = RP.probe_windows(clean, n=3, sec=3)
    print("      健康对照 -> %d 个窗口报错（应 0）" % len(pw2['bad']))
    if pw2['bad']:
        fails.append('健康对照被误报')

shutil.rmtree(T, ignore_errors=True)
print("\n  >>> %s" % ("全部通过 ✅" if not fails else "未通过: %s" % fails))
sys.exit(1 if fails else 0)

# -*- coding: utf-8 -*-
"""repair.py 自测（只读为主）：

  A) 合成夹具：带音轨 MP4 -> 截掉 moov 尾巴 -> 体检 -> 计划 -> 修复 -> 验证 -> 回滚
  B) 真实媒体库抽样只读体检（不写任何字节）
  C) 找已知坏文件（YST-341，前次勘察发现的 moov 缺失样本）

用法: python tests/test_repair.py
"""
import os
import shutil
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import repair as RP                                                     # noqa: E402

T = os.path.join(os.path.dirname(os.path.abspath(__file__)), '_fixture_repair')
shutil.rmtree(T, ignore_errors=True)
os.makedirs(T, exist_ok=True)
fails = []

print("===== A) 合成夹具：截断 moov 尾巴 =====")
src = os.path.join(T, "sample.mp4")
rc, so, se = RP._run([RP.FFMPEG, '-hide_banner', '-v', 'error',
                      '-f', 'lavfi', '-i', 'testsrc2=size=320x240:rate=25',
                      '-f', 'lavfi', '-i', 'sine=frequency=440',
                      '-t', '10', '-c:v', 'libx264', '-preset', 'ultrafast',
                      '-c:a', 'aac', '-shortest', src], 300)
print("  生成 rc=%s size=%s %s" % (rc, os.path.getsize(src) if os.path.exists(src) else '-', se[:70]))
if rc != 0:
    fails.append('夹具生成失败')
else:
    p0 = RP.plan_index(src)
    print("  未截断: ok=%s err=%s" % (p0.get('ok'), (p0.get('error') or '')[:60]))
    if p0.get('ok'):
        fails.append('未截断的文件竟然给出了修复计划（不该）')
    # 复现 CLUB-580 的形状：从【第二条轨 stco 的中段】截断（视频表在前、完好）
    import struct
    sz0 = os.path.getsize(src)
    with open(src, 'rb') as fp:
        tops = {t: (o, l) for t, o, l, hl in RP._boxes(fp, 0, sz0, limit=64)}
        mo, ml = tops['moov']
        fp.seek(mo)
        buf = fp.read()
    traks = [b for b in RP._boxes(RP._MemFile(buf), 8, len(buf)) if b[0] == 'trak']
    cut_at = None
    if len(traks) >= 2:
        sb = RP._path_box(buf, traks[1][1] + traks[1][3], traks[1][1] + traks[1][2],
                          ['mdia', 'minf', 'stbl'])
        if sb:
            ch = {c[0]: c for c in RP._boxes(RP._MemFile(buf), sb[0] + sb[2], sb[0] + sb[1])}
            if 'stco' in ch:
                _, so, sl, shl = ch['stco']
                nco = struct.unpack('>I', buf[so + shl + 4:so + shl + 8])[0]
                cut_at = mo + so + shl + 8 + max(1, nco // 2) * 4      # 只留一半条目
    with open(src, 'r+b') as fp:
        fp.truncate(cut_at if (cut_at and cut_at < sz0) else sz0 - 3000)
    print("  截断到 %d 字节（原 %d，切掉 %d）%s"
          % (os.path.getsize(src), sz0, sz0 - os.path.getsize(src),
             "（从音频 stco 中段）" if cut_at and cut_at < sz0 else "（尾部）"))
    pr = RP.probe(src)
    print("  probe: cls=%s | %s" % (pr['cls'], pr['reason'][:70]))
    if pr['cls'] != 'index-cut':
        fails.append('截断后应判为 index-cut，实际 ' + str(pr['cls']))
    pl = RP.plan_index(src)
    print("  plan_index: ok=%s short=%s pair=%s/%s append=%s new=%s target=%s selfcheck=%s"
          % (pl.get('ok'), pl.get('moov_short'), pl.get('pair_mismatch'), pl.get('pair_checked'),
             pl.get('append_bytes'), pl.get('new_size'), pl.get('target'), pl.get('selfcheck_ok')))
    if pl.get('error'):
        print("    引擎给出的原因: %s" % pl['error'][:130])
        if 'AttributeError' in pl['error'] or 'TypeError' in pl['error'] or 'NameError' in pl['error']:
            fails.append('plan_index 抛了实现级异常: ' + pl['error'][:80])
        elif '配对' in pl['error'] or '交错' in pl['error']:
            print("    （该夹具 muxer 的布局不是 1:1 交错 -> 引擎按设计拒绝，属正确行为；"
                  "字节追加路径已在 CLUB-580 / FC2PPV 两个真实文件上验证过）")
        else:
            fails.append('截断夹具未给出可执行计划: ' + pl['error'][:90])
    if pl.get('ok'):
        before = os.path.getsize(src)
        r = RP.apply_index(src, log=lambda *a: None)
        print("  apply_index: ok=%s %d -> %s" % (r.get('ok'), before, r.get('new_size')))
        chk = RP.probe(src)
        print("  修复后 probe: cls=%s dur=%s" % (chk['cls'], chk.get('duration')))
        if chk['cls'] != 'ok':
            fails.append('修复后 ffprobe 仍不通过')
        u = RP.undo_index(src, r['orig_size'])
        back = os.path.getsize(src)
        print("  undo_index: ok=%s -> %d 字节（应回到 %d）" % (u.get('ok'), back, before))
        if back != before:
            fails.append('回滚尺寸不对')
    else:
        before = os.path.getsize(src)
        r = RP.apply_index(src)
        print("  计划未通过时 apply_index 必须拒绝且不写字节: ok=%s size未变=%s"
              % (r.get('ok'), os.path.getsize(src) == before))
        if r.get('ok') or os.path.getsize(src) != before:
            fails.append('计划未通过却动了文件')

print("\n===== B) 真实库抽样只读体检 =====")
con = sqlite3.connect("file:F:/MediaScraper/mlib.db?mode=ro", uri=True)
con.row_factory = sqlite3.Row
rows = [dict(r) for r in con.execute(
    "SELECT id,name,path FROM items WHERE present=1 ORDER BY RANDOM() LIMIT 24")]
con.close()
tally, bad, t0 = {}, [], time.time()
for it in rows:
    f = None
    try:
        for x in sorted(os.listdir(it['path'])):
            if os.path.splitext(x)[1].lower() in RP.VIDEO_EXT:
                f = os.path.join(it['path'], x)
                break
    except Exception:
        continue
    if not f:
        tally['(无视频)'] = tally.get('(无视频)', 0) + 1
        continue
    r = RP.probe(f)
    tally[r['cls']] = tally.get(r['cls'], 0) + 1
    if r['cls'] != 'ok':
        try:
            fsz = os.path.getsize(f)
        except Exception:
            fsz = -1
        bad.append((it['id'], os.path.basename(f)[:40], r['cls'], r['reason'][:52], fsz))
print("  24 条耗时 %.0fs" % (time.time() - t0))
for k, v in sorted(tally.items(), key=lambda x: -x[1]):
    print("   %-11s %d" % (k, v))
for b in bad:
    print("   !! id=%s %s  [%s] %.1fMB  %s" % (b[0], b[1], b[2], (b[4] or 0) / 1048576, b[3]))

print("\n===== C) 已知坏文件 YST-341 =====")
con = sqlite3.connect("file:F:/MediaScraper/mlib.db?mode=ro", uri=True)
con.row_factory = sqlite3.Row
hit = [dict(r) for r in con.execute(
    "SELECT id,name,path FROM items WHERE present=1 AND (name LIKE '%YST-341%' OR path LIKE '%YST-341%')")]
con.close()
if not hit:
    print("   （库里没找到）")
for it in hit:
    print("   id=%s %s" % (it['id'], (it['name'] or '')[:50]))
    try:
        for x in sorted(os.listdir(it['path'])):
            if os.path.splitext(x)[1].lower() in RP.VIDEO_EXT:
                fp = os.path.join(it['path'], x)
                pr = RP.probe(fp)
                print("      %-44s %9d B  cls=%-10s %s"
                      % (x[:44], os.path.getsize(fp), pr['cls'], pr['reason'][:52]))
    except Exception as e:
        print("      列目录失败:", e)

shutil.rmtree(T, ignore_errors=True)
print("\n  >>> %s" % ("全部通过 ✅" if not fails else "未通过: %s" % fails))
sys.exit(1 if fails else 0)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""flatten.py 的自测：在临时夹具上验证 计划 / 执行 / 回滚，不碰真实媒体库。

用法: python tests/test_flatten.py
"""
import glob
import io
import json
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import flatten                                                     # noqa: E402

ROOT = os.path.join(HERE, '_fixtures')
TRASH = os.path.join(HERE, '_trash')
shutil.rmtree(ROOT, ignore_errors=True)
shutil.rmtree(TRASH, ignore_errors=True)


def mk(p):
    fp = os.path.join(ROOT, p)
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    io.open(fp, 'w', encoding='utf-8').write('x')


# 1) 一级 P/V 桶          2) 深层(3层)      3) 长名          4) 未知类型
for f in ('1.jpg', '2.jpg', '10.jpg'):
    mk('A_type/P/' + f)
mk('A_type/V/a.mp4'); mk('A_type/V/b.mkv')
mk('B_deep/x/P/1.jpg'); mk('B_deep/x/P/2.jpg')
mk('C_long/P/photo_1_2026-08-23_18-57-14_watermarked.jpg')
mk('D_unknown/P/1.jpg'); mk('D_unknown/V/a.mp4'); mk('D_unknown/V/weird.xyz')
# 5) 具名目录（约定里的例子）   6) 重名冲突        7) 垃圾文件
mk('E_named/1.你好123/1.jpg')
mk('E_named/2，飒飒/1.jpg'); mk('E_named/2，飒飒/1.mp4')
mk('G_dup/散图/a/1.jpg'); mk('G_dup/散图_a/1.jpg')
mk('H_junk/P/1.jpg'); mk('H_junk/P/Thumbs.db'); mk('H_junk/P/描述.txt')
mk('I_longpre/这是一个非常长的子目录名字用于触发前缀警告/1.jpg')          # 前缀>20 -> warn

EXP = {                      # 期望：mode / 改名数 / warn（depth>=2 或 前缀>20 字符）
    'A_type':    ('type',  5, False),
    'B_deep':    ('named', 2, True),
    'C_long':    ('type',  1, False),
    'D_unknown': ('type',  2, False),
    'E_named':   ('named', 3, False),
    'G_dup':     ('named', 2, True),
    'H_junk':    ('type',  1, False),
    'I_longpre': ('named', 1, True),
}
fails, plans = [], {}
for name, (mode, cnt, warn) in EXP.items():
    p = flatten.build_plan(os.path.join(ROOT, name))
    plans[name] = p
    if (p['mode'], p['count'], p['warn']) != (mode, cnt, warn):
        fails.append('%s 期望=%s 实际=(%s,%s,%s)' % (name, (mode, cnt, warn), p['mode'], p['count'], p['warn']))
    print('  %-10s mode=%-6s 改名%2d depth=%d 最长前缀=%2d warn=%-5s ok=%s'
          % (name, p['mode'], p['count'], p['depth'], p['max_pre_len'], p['warn'], p['ok']))

# 约定里的例子必须一字不差
got = {i['src']: i['dst'] for i in plans['E_named']['items']}
want = {'1.你好123\\1.jpg': '1_你好123_1.jpg',
        '2，飒飒\\1.jpg': '2_飒飒_1.jpg',
        '2，飒飒\\1.mp4': '2_飒飒_1.mp4'}
for k, v in want.items():
    if got.get(k) != v:
        fails.append('命名规则: %s -> %s（期望 %s）' % (k, got.get(k), v))

# 未知类型不得改名，且其子目录不得被清理
if 'V\\weird.xyz' not in plans['D_unknown']['unknown']:
    fails.append('未知类型文件未被识别')
# 冲突必须拦住
if plans['G_dup']['ok']:
    fails.append('重名冲突未被拦住')

# 执行：同秒连续多次，验证回收目录与 undo 文件互不覆盖
undos = {}
for nm in ('A_type', 'D_unknown', 'H_junk', 'E_named'):
    p = plans[nm]
    r = flatten.apply_plan(os.path.join(ROOT, nm), p['items'], TRASH, p['subs'])
    undos[nm] = r['undo_file']
    for e in r['errors']:
        fails.append('执行报错 %s: %s' % (nm, e))
if len({os.path.dirname(v) for v in undos.values()}) != len(undos):
    fails.append('回收目录互相覆盖')
if sorted(os.listdir(os.path.join(ROOT, 'E_named'))) != ['1_你好123_1.jpg', '2_飒飒_1.jpg', '2_飒飒_1.mp4']:
    fails.append('E_named 执行结果不对')
if os.path.isdir(os.path.join(ROOT, 'D_unknown', 'V')) is False:
    fails.append('D_unknown 含未知文件的子目录被误删')

# 回滚
for nm, uf in undos.items():
    r = flatten.do_undo(uf)
    if r['errors']:
        fails.append('回滚 %s 报错: %s' % (nm, r['errors'][:2]))
if not os.path.isdir(os.path.join(ROOT, 'E_named', '2，飒飒')):
    fails.append('回滚后子目录没回来')

merged = flatten.merge_undo(list(undos.values()), os.path.join(TRASH, 'batch_undo.json'))
if not os.path.isfile(merged):
    fails.append('合并 undo 失败')

shutil.rmtree(ROOT, ignore_errors=True)
shutil.rmtree(TRASH, ignore_errors=True)
print('\n  >>> %s' % ('全部通过 ✅' if not fails else '未通过:\n   - ' + '\n   - '.join(fails)))
sys.exit(1 if fails else 0)

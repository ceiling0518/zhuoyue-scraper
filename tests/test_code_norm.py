# -*- coding: utf-8 -*-
"""番号归一化自测：剥版本后缀 / coded 剥前导数字 / find_code 不抓站点前缀。

规则依据（2026-10-15 用 801 条真实站点命中样本反推）：
  · 版本后缀 C/X/J/HD/SD/4K/U 不属于番号
  · coded 的前导 2~4 位数字不属于番号：480FRIN-052 -> FRIN-052
  · uncensored（10musume / 1pondo …）整串保留 —— 数字是站点名的一部分
  · 站点前缀 kss55.cc@ / dxxdom.com@ 不是番号
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import javscrape as JS                                                  # noqa: E402
import mlib                                                             # noqa: E402

fails = []


def ck(expr, got, want):
    ok = (got == want)
    print("  %-44s -> %-20s %s" % (expr, got, 'OK' if ok else 'FAIL (want %s)' % want))
    if not ok:
        fails.append('%s: got %r want %r' % (expr, got, want))


print("== split_code：剥版本后缀 ==")
for v, want in (('SVMGM-052C', 'SVMGM-052'), ('ABP-123-HD', 'ABP-123'),
                ('480FRIN-052C', '480FRIN-052'), ('MIAA-123', 'MIAA-123'),
                ('259LUXU-1696', '259LUXU-1696'), ('FC2-PPV-1234567', 'FC2-PPV-1234567')):
    ck('split_code(%s)' % v, mlib.split_code(v)[0], want)

print()
print("== strip_code_prefix：coded 剥前导数字 ==")
for v, want in (('480FRIN-052', 'FRIN-052'), ('483SGK-052', 'SGK-052'),
                ('529STCV-036', 'STCV-036'), ('200GANA-2566', 'GANA-2566'),
                ('261ARA-573', 'ARA-573'), ('042CLT-068', 'CLT-068'),
                ('SSIS-352', 'SSIS-352'), ('FRIN-052', 'FRIN-052')):
    ck('strip_code_prefix(%s)' % v, mlib.strip_code_prefix(v), want)
# 反面示范：uncensored 若调用它会被剥坏 -> 所以语义/扫描路径都加了 is_uncensored 保护
ck('strip_code_prefix(10musume-011526_01) [反面示范]',
   mlib.strip_code_prefix('10musume-011526_01'), 'MUSUME-011526_01')

print()
print("== find_code：不抓站点前缀 + 剥版本后缀 ==")
for v, want in (('kss55.cc@SVMGM-052C', 'SVMGM-052'),
                ('bydda.cc@737SRC-009', '737SRC-009'),
                ('dxxdom.com@261ARA-247', '261ARA-247'),
                ('kss55.cc@200GANA-3443J', '200GANA-3443'),
                ('kss55.cc@DLDSS-558C', 'DLDSS-558'),
                ('480FRIN-052C', '480FRIN-052')):
    ck('find_code(%s)' % v, JS.find_code(v), want)

print()
print("== semantic_batch：批量路径（曾因插入位置错误崩掉）==")
import json as _json                                                  # noqa: E402

_HERE = os.path.dirname(os.path.abspath(__file__))


def _fake_chat(*a, **k):
    return _json.dumps([{'kind': 'coded', 'code': '480FRIN-052C'},
                        {'kind': 'coded', 'code': '483SGK-052C'},
                        {'kind': 'uncensored', 'code': '10musume-011526_01'}])


_orig = getattr(mlib, '_chat', None)
mlib._chat = _fake_chat
try:
    items = [{'name': 'a', 'path': _HERE}, {'name': 'b', 'path': _HERE},
             {'name': 'c', 'path': _HERE}]
    out = mlib.semantic_batch(items, model='fake')
finally:
    mlib._chat = _orig
ck('semantic_batch -> code 列表', [o['code'] for o in (out or [])],
   ['FRIN-052', 'SGK-052', '10musume-011526_01'])
ck('semantic_batch -> kind 列表', [o['kind'] for o in (out or [])],
   ['coded', 'coded', 'uncensored'])

print()
print("  >>> %s" % ('全部通过' if not fails else '未通过: %s' % fails))
sys.exit(1 if fails else 0)

# -*- coding: utf-8 -*-
"""把「多重嵌套」的媒体目录整理成 app 能自动刮削的平坦结构。

命名规则（与使用者约定一致）
  1) 纯视图/视频桶（子目录名是 P / V / T / 图 / 图片 / 照片 / 视图 / 视频 / 自拍图 …）
     图片 -> `P_原名`，视频 -> `V_原名`
  2) 具名内容目录（`1.你好123` / `1-聊天记录` / `WANIMAL图片`）
     -> `序号_名字_原名`（无序号则 `名字_原名`）
  3) 更深一层：把中间目录名也串进前缀，保证唯一（`散图_x_1.jpg`）

安全约束
  * 只重命名「媒体文件」；任何**未知类型**文件一律不碰，并且**该子目录不会被清理**，
    只把它列为 unknown 交回给使用者（避免把 .webm 之类误判成垃圾丢掉）。
  * 子目录整体**移入 trash**（可还原），绝不永久删除。
  * 重名 / 撞名 -> 拒绝执行，返回冲突清单。

确认门（UI 弹框用）
  warn = (子目录层数 >= WARN_DEPTH) 或 (任一改后文件名长度 > WARN_NAME_LEN)
"""

import json
import os
import re
import shutil
import time

# ---- 扩展名（视频取 app 的 VIDEO_EXT 并补常见项，避免漏判成"未知"） ----------
def _video_ext():
    base = set()
    try:
        import cover                                  # app 自带的定义
        base |= {str(e).lower() for e in getattr(cover, 'VIDEO_EXT', set())}
    except Exception:
        pass
    base |= {'.mp4', '.mkv', '.avi', '.wmv', '.mov', '.ts', '.m2ts', '.mts',
             '.m4v', '.flv', '.rmvb', '.rm', '.mpg', '.mpeg', '.vob', '.webm',
             '.3gp', '.ogv', '.asf', '.divx', '.qt', '.f4v', '.iso'}
    return base


VIDEO_EXT = _video_ext()
IMG_EXT = {'.jpg', '.jpeg', '.jpe', '.png', '.webp', '.gif', '.bmp', '.tif',
           '.tiff', '.heic', '.avif', '.jfif'}

# 明确的垃圾（这些才允许随子目录一起进 trash）
JUNK_NAMES = {'thumbs.db', 'desktop.ini', '.ds_store'}
JUNK_EXT = {'.db', '.txt', '.ini', '.url', '.lnk', '.log', '.sfv', '.nfo.bak'}

# 类型桶名（纯视图 / 纯视频）
TYPE_BUCKETS = {'p', 'v', 't', '图', '图片', '照片', '视图', '视频', '自拍图',
                'pics', 'vids', 'photos', 'videos', 'img', 'imgs'}

WARN_DEPTH = int(os.environ.get('MS_FLATTEN_WARN_DEPTH', '2'))      # 子目录层数 >= 此值 -> 建议人工
WARN_NAME_LEN = int(os.environ.get('MS_FLATTEN_WARN_LEN', '10'))    # 改后文件名 > 此长度 -> 建议人工

_SEQ = re.compile(r'^\s*(\d+)\s*[\.、,，\-_:：\s]*(.*)$')


def is_type_bucket(name):
    return (name or '').strip().lower() in TYPE_BUCKETS


def is_junk(name):
    n = (name or '').lower()
    return n in JUNK_NAMES or os.path.splitext(n)[1] in JUNK_EXT


def _kind_of(name):
    e = os.path.splitext(name)[1].lower()
    if e in VIDEO_EXT:
        return 'video'
    if e in IMG_EXT:
        return 'img'
    return 'unknown'


def _head(top_name, mode, kind):
    """一层目录贡献的前缀。"""
    if mode == 'type':
        return 'P_' if kind == 'img' else 'V_'
    m = _SEQ.match(top_name)
    num, nm = (m.group(1), m.group(2).strip()) if m else ('', top_name.strip())
    return (num + '_' if num else '') + (nm or top_name.strip()) + '_'


def build_plan(folder):
    """扫描一个 item 目录，产出改名计划（不改任何文件）。"""
    out = {'ok': False, 'folder': folder, 'mode': '', 'items': [], 'subs': [],
           'count': 0, 'depth': 0, 'max_name_len': 0, 'warn': False,
           'reasons': [], 'dups': [], 'clash': [], 'unknown': [], 'junk': 0}
    if not os.path.isdir(folder):
        out['error'] = '目录不可访问'
        return out

    # 1) 收集所有子目录里的文件 + 目录深度
    rels, depth = [], 0
    for root, dirs, files in os.walk(folder):
        if os.path.abspath(root) == os.path.abspath(folder):
            continue
        d = os.path.relpath(root, folder).count(os.sep) + 1
        depth = max(depth, d)
        for f in files:
            rels.append(os.path.relpath(os.path.join(root, f), folder))
    out['depth'] = depth

    tops = {r.split(os.sep)[0] for r in rels if os.sep in r}
    withmedia = set()
    for r in rels:
        if _kind_of(r) in ('video', 'img'):
            withmedia.add(r.split(os.sep)[0])
    mode = 'type' if (withmedia and all(is_type_bucket(t) for t in withmedia)) else 'named'
    out['mode'] = mode

    # 2) 生成新名字
    for rel in sorted(rels):
        parts = rel.split(os.sep)
        if len(parts) < 2:
            continue                                   # 已经在根目录，不动
        kind = _kind_of(rel)
        top, deep = parts[0], parts[1:-1]
        base = parts[-1]
        if kind == 'unknown':
            out['unknown'].append(rel)
            continue
        pre = _head(top, mode, kind) + ('_'.join(deep) + '_' if deep else '')
        out['items'].append({'src': rel, 'dst': pre + base, 'kind': kind})

    out['count'] = len(out['items'])
    out['max_name_len'] = max([len(os.path.basename(i['dst'])) for i in out['items']] or [0])

    seen = {}
    for i in out['items']:
        seen.setdefault(i['dst'], []).append(i['src'])
    out['dups'] = [{'dst': k, 'src': v} for k, v in seen.items() if len(v) > 1]
    out['clash'] = [i['dst'] for i in out['items'] if os.path.exists(os.path.join(folder, i['dst']))]

    # 3) 子目录清单（决定哪些可以清掉）
    for s in sorted(os.listdir(folder)):
        p = os.path.join(folder, s)
        if os.path.isdir(p):
            out['subs'].append(s)
    for s in out['subs']:
        for root, dirs, files in os.walk(os.path.join(folder, s)):
            for f in files:
                if _kind_of(f) == 'unknown' and not is_junk(f):
                    pass
                if is_junk(f):
                    out['junk'] += 1

    # 4) 确认门
    names = [os.path.basename(i['dst']) for i in out['items']]
    long_ones = sorted({n for n in names if len(n) > WARN_NAME_LEN})
    if out['depth'] >= WARN_DEPTH:
        out['reasons'].append('子目录层数 %d >= %d' % (out['depth'], WARN_DEPTH))
    if long_ones:
        out['reasons'].append('有 %d 个改后文件名超过 %d 字符（最长 %d：%s）'
                              % (len(long_ones), WARN_NAME_LEN, max(len(n) for n in long_ones), long_ones[0]))
    out['warn'] = bool(out['reasons'])
    out['ok'] = (not out['dups']) and (not out['clash'])
    return out


def _clearable(folder, sub):
    """子目录是否可安全清理：不含任何未识别文件，也不该残留媒体文件。"""
    for root, dirs, files in os.walk(os.path.join(folder, sub)):
        for f in files:
            if not is_junk(f) and _kind_of(f) == 'unknown':
                return False, os.path.relpath(os.path.join(root, f), folder)
            if _kind_of(f) in ('img', 'video'):
                return False, os.path.relpath(os.path.join(root, f), folder)
    return True, ''


def _safe(s, n=40):
    return re.sub(r'[^\w\u4e00-\u9fff.-]+', '_', str(s or 'x'))[:n] or 'x'


def apply_plan(folder, items, trash_root, subdirs, tag=None, log=None):
    """执行改名 + 把可安全清理的子目录移入 trash。返回结果与 undo 文件路径。

    tag 由调用方按「一批」传入，同批共用一个回收目录；每个 item 各自一份 undo 文件，
    并存不互相覆盖（同秒并发也不会撞）。
    """
    say = log or (lambda *a: None)
    ts = time.strftime('%Y%m%d_%H%M%S')
    base = 'flatten_%s%s' % (ts, ('_' + _safe(tag, 20)) if tag else '')
    tdir, k = os.path.join(trash_root, base), 0
    while True:
        try:
            os.makedirs(tdir, exist_ok=False)
            break
        except FileExistsError:
            k += 1
            tdir = os.path.join(trash_root, '%s_%d' % (base, k))

    undo, err = [], []
    for it in items:
        src = os.path.join(folder, it['src'])
        dst = os.path.join(folder, it['dst'])
        try:
            if not os.path.exists(src):
                err.append(['源缺失', it['src']]); continue
            if os.path.exists(dst):
                err.append(['目标已存在', it['dst']]); continue
            os.replace(src, dst)
            undo.append({'src': src, 'dst': dst})
        except Exception as e:
            err.append([type(e).__name__ + ': ' + str(e)[:80], it['src']])

    moved, skipped = [], []
    for s in subdirs:
        p = os.path.join(folder, s)
        if not os.path.isdir(p):
            continue
        ok, bad = _clearable(folder, s)
        if not ok:
            skipped.append([s, bad]); continue
        dest = os.path.join(tdir, os.path.basename(folder)[:40] + '__' + s)
        try:
            if os.path.exists(dest):
                dest += '.dup'
            shutil.move(p, dest)
            moved.append({'src': p, 'dst': dest})
        except Exception as e:
            err.append(['move dir: ' + str(e)[:60], s])

    # undo 文件（改名映射 + 被移走的子目录）—— 按 item 命名，互不覆盖
    undo_file = os.path.join(tdir, 'undo_%s.json' % _safe(os.path.basename(folder), 50))
    with open(undo_file, 'w', encoding='utf-8') as fh:
        json.dump({'folder': folder, 'renamed': undo, 'moved_dirs': moved,
                   'skipped_dirs': skipped}, fh, ensure_ascii=False, indent=1)
    return {'renamed': len(undo), 'errors': err, 'moved_dirs': moved,
            'skipped_dirs': skipped, 'undo_file': undo_file, 'trash': tdir}


def do_undo(undo_file, log=None):
    """按 undo.json 还原（先搬回子目录，再把文件名改回去）。"""
    say = log or (lambda *a: None)
    with open(undo_file, encoding='utf-8') as fh:
        u = json.load(fh)
    if isinstance(u, dict) and 'jobs' in u:          # 合并文件：逐个还原
        tot, errs = 0, []
        for j in u['jobs']:
            r = _undo_one(j)
            tot += r['restored']; errs += r['errors']
        return {'restored': tot, 'errors': errs}
    return _undo_one(u)


def _undo_one(u):
    ok, err = 0, []
    for d in u.get('moved_dirs', []):
        src, dst = (d['src'], d.get('dst')) if isinstance(d, dict) else (d, None)
        cur = dst if (dst and os.path.isdir(dst)) else src
        if not os.path.isdir(cur):
            err.append(['回收里已找不到', str(d)]); continue
        parent = os.path.dirname(src)
        try:
            if os.path.exists(src):
                err.append(['原位置已被占用', src]); continue
            if parent and not os.path.isdir(parent):
                os.makedirs(parent, exist_ok=True)
            shutil.move(cur, src); ok += 1
        except Exception as e:
            err.append([type(e).__name__ + ': ' + str(e)[:60], str(d)])
    for r in reversed(u.get('renamed', [])):
        try:
            if os.path.exists(r['src']):
                err.append(['原名已被占用', r['src']]); continue
            os.replace(r['dst'], r['src']); ok += 1
        except Exception as e:
            err.append([type(e).__name__ + ': ' + str(e)[:60], r['dst']])
    return {'restored': ok, 'errors': err}


def merge_undo(paths, out_file, folder=''):
    """把一批 undo_*.json 合并成一个可一键回滚的文件。"""
    jobs = []
    for p in paths:
        try:
            with open(p, encoding='utf-8') as fh:
                jobs.append(json.load(fh))
        except Exception:
            pass
    with open(out_file, 'w', encoding='utf-8') as fh:
        json.dump({'folder': folder, 'jobs': jobs}, fh, ensure_ascii=False, indent=1)
    return out_file

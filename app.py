#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MediaScraper -- standalone local scraping service.

Self-contained: a stdlib HTTP server + SQLite (driven by mlib.py in this folder).
No OpenClaw / Control UI dependency.  Start with run.cmd, then open the printed URL.

质量 quality : 有(完整,跳过) / 无(=没有,正常刮) / 有误(留人工复核)
来源 source  : code(javbus/javdb) / Uncen(missav) / AI(模型)
"""
import threading
import io, json, os, subprocess, sys, threading, time, traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import mlib                                   # noqa: E402

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding='utf-8')
    except Exception:
        pass

HOST = os.environ.get('MS_HOST', '127.0.0.1')
PORT = int(os.environ.get('MS_PORT', '8800'))
LOGMAX = 800
THUMB_DIR = os.path.join(HERE, 'thumbs')

_LOCK = threading.Lock()
_DLOK = threading.Lock()
STATE = {'running': False, 'name': '', 'done': 0, 'total': 0,
         'stop': False, 'started': 0, 'log': []}
QUEUE = []                                    # [(item_id, mode)]  -- persisted in `queue`
_LOGDB = None
_LOGBUF = []


def _logdb():
    """Shared log connection (check_same_thread=False: 两条线都会写)。"""
    global _LOGDB
    if _LOGDB is None:
        import sqlite3
        _LOGDB = sqlite3.connect(mlib.DB_PATH, timeout=30, check_same_thread=False)
        _LOGDB.execute('PRAGMA journal_mode=WAL')
        _LOGDB.execute('PRAGMA busy_timeout=30000')
    return _LOGDB


def _flush_log():
    """Write buffered log lines to joblog in one go (every 5 lines)."""
    with _LOCK:
        if not _LOGBUF:
            return
        rows = list(_LOGBUF)
        _LOGBUF[:] = []
    try:
        with _DLOK:
            c = _logdb()
            c.executemany('INSERT INTO joblog(ts,line) VALUES(?,?)',
                          [(mlib.now(), x) for x in rows])
            c.execute('DELETE FROM joblog WHERE id NOT IN '
                      '(SELECT id FROM joblog ORDER BY id DESC LIMIT 3000)')
            c.commit()
    except Exception:
        pass


def q_load():
    try:
        QUEUE[:] = [(r['item_id'], r['mode'] or 'auto') for r in
                    mlib.db().execute('SELECT item_id, mode FROM queue ORDER BY added_at, item_id')]
    except Exception:
        pass


def q_add(iid, mode='auto'):
    with _LOCK:
        if any(q[0] == iid for q in QUEUE):
            return len(QUEUE)
        QUEUE.append((iid, mode))
    try:
        c = mlib.db()
        c.execute('INSERT OR REPLACE INTO queue(item_id, mode, added_at) VALUES(?,?,?)',
                  (iid, mode, mlib.now()))
        c.commit()
    except Exception:
        pass
    return len(QUEUE)


def q_pop():
    with _LOCK:
        if not QUEUE:
            return None
        iid, mode = QUEUE.pop(0)
    try:
        c = mlib.db()
        c.execute('DELETE FROM queue WHERE item_id=?', (iid,))
        c.commit()
    except Exception:
        pass
    return iid, mode


def q_clear():
    with _LOCK:
        QUEUE[:] = []
    try:
        c = mlib.db()
        c.execute('DELETE FROM queue')
        c.commit()
    except Exception:
        pass


def _log(msg):
    line = '%s  %s' % (time.strftime('%H:%M:%S'), msg)
    with _LOCK:
        STATE['log'].append(line)
        if len(STATE['log']) > LOGMAX:
            del STATE['log'][:LOGMAX // 2]
        _LOGBUF.append(line)
        full = len(_LOGBUF) >= 5          # 5 条写一次库，减少抢锁
    if full:
        _flush_log()


def _begin(name, total=0):
    with _LOCK:
        STATE.update(running=True, name=name, done=0, total=total, stop=False,
                     started=time.time())
    _log('===== %s 开始 (total=%d) =====' % (name, total))


def _end(msg):
    with _LOCK:
        STATE.update(running=False)
    _log('===== 结束: %s =====' % msg)
    _flush_log()


def _stop_req():
    with _LOCK:
        return STATE['stop']


def _tick(n, total=None):
    with _LOCK:
        STATE['done'] = n
        if total:
            STATE['total'] = total


# ---------------------------------------------------------------- passes
def _scan_pass(mode):
    r = mlib.scan(mode=mode)
    _log('scan(%s): %s' % (mode, json.dumps(r, ensure_ascii=False)))
    return r


def _sem_pass(limit, force=False, group=None):
    """limit=0 -> 全部;  group=0 -> 全部一批（内部单批上限 60 自动切）。"""
    dbc = mlib.db()
    limit = int(limit or 0)
    group = int(group or 0)
    q = "SELECT id,name,path FROM items WHERE present=1"
    if not force:
        q += " AND (sem_kind IS NULL OR sem_kind='')"
    q += ' AND COALESCE(sem_manual,0)=0 ORDER BY id'
    args = ()
    if limit > 0:
        q += ' LIMIT ?'
        args = (limit,)
    rows = dbc.execute(q, args).fetchall()
    if group <= 0:
        group = len(rows) or 1
    if group > 60:
        _log('  单批上限 60（自动切），请求组=%d' % group)
        group = 60
    _tick(0, len(rows))
    _log('语义 %d 条，%d 条一批（一次喂给模型）' % (len(rows), group))
    ok = 0
    for i in range(0, len(rows), group):
        if _stop_req():
            _log('! 已请求停止'); break
        chunk = [dict(r) for r in rows[i:i + group]]
        res = mlib.semantic_batch(chunk)
        if res is None:
            _log('  批 %d 解析失败 -> 逐条兜底' % (i // group + 1))
        for k, r in enumerate(chunk):
            s = res[k] if res else mlib.semantic_code(r)
            kind = mlib.kind_by_rule(r['name'])          # 标签按系列名粗分，不必很准
            code = (s or {}).get('code') or ''
            dbc.execute('UPDATE items SET sem_kind=?, sem_code=?, last_update=? WHERE id=?',
                        (kind, code, mlib.now(), r['id']))
            if (s or {}).get('nested'):
                dbc.execute("UPDATE items SET quality='有误' WHERE id=?", (r['id'],))
            dbc.commit()
            ok += 1
            _log('[%d/%d] id=%d  %s %s' % (i + k + 1, len(rows), r['id'], kind, code))
            _tick(i + k + 1)
    return ok, len(rows)


def _one(dbc, iid, mode, force=False, ai_result=None):
    try:
        o, m = mlib.process(dbc, iid, mode, force=force, ai_result=ai_result)
        if mode == 'website' and 'website:-' in m:
            # 显式只跑网站线而且没抓到 -> 有误（与批量抓取保持一致）
            row = dbc.execute('SELECT site FROM items WHERE id=?', (iid,)).fetchone()
            if not ((row['site'] if row else '') or ''):
                dbc.execute("UPDATE items SET quality='有误', last_update=? WHERE id=?",
                            (mlib.now(), iid))
                dbc.commit()
                m += ' -> 有误'
        _log('id=%d %s' % (iid, m))
        return o
    except Exception as e:
        _log('id=%d ERR %s' % (iid, str(e)[:90]))
        return False


def _fetch_pass(mode, limit, ai_group=None):
    """两条线【并行】：
      网站线 = sem_kind coded/uncensored（javbus/javdb/MissAV），失败 -> 质量=有误
      AI 线  = sem_kind none（批量喂模型）
    limit=0 -> 全部; ai_group=0 -> 全部一批（单批上限 60 自动切）。
    """
    limit = int(limit or 0)
    ai_group = int(ai_group or 0)
    lim_sql = ' LIMIT ?' if limit > 0 else ''
    lim_arg = (limit,) if limit > 0 else ()
    c0 = mlib.db()
    rows = c0.execute("SELECT id,name,code,sem_code,sem_kind FROM items WHERE present=1 "
                      "AND COALESCE(quality,'') IN ('','无') AND COALESCE(kind,'')<>'nested' "
                      'ORDER BY id' + lim_sql, lim_arg).fetchall()
    web, ai = [], []
    for r in rows:
        # 标签不参与决策；系列名不在识别表的，默认按 code 走网站线
        k = mlib.kind_by_rule(r['name'] or '')
        if (r['sem_code'] or r['code'] or '').strip() or k in ('coded', 'uncensored'):
            web.append(dict(r))
        else:
            ai.append(dict(r))
    total = len(rows)
    _tick(0, total)
    _log('两条线并行：网站线 %d 条 / AI 线 %d 条 (mode=%s)' % (len(web), len(ai), mode))
    cnt = {'n': 0}
    clk = threading.Lock()

    def bump():
        with clk:
            cnt['n'] += 1
            _tick(cnt['n'])

    def web_worker():
        if mode not in ('website', 'auto'):
            return
        d = mlib.db()
        for r in web:
            if _stop_req():
                _log('! [站] 已请求停止'); return
            try:
                ok, m = mlib.process(d, r['id'], 'website')
            except Exception as e:
                ok, m = False, 'ERR %s' % str(e)[:80]
            failed = ('website:-' in m) or ('site:skip' in m)
            if failed:                            # 网站线一条数据都没抓到
                d.execute("UPDATE items SET quality='有误', last_update=? WHERE id=?",
                          (mlib.now(), r['id']))
                d.commit()
                m += ' -> 有误'
            _log('[站] id=%-5d %s' % (r['id'], m))
            bump()

    def ai_worker():
        if mode not in ('ai', 'auto'):
            return
        d = mlib.db()
        batch = ai_group if ai_group > 0 else (len(ai) or 1)
        if batch > 60:
            _log('  AI 单批上限 60（自动切），请求组=%d' % batch)
            batch = 60
        nb = (len(ai) + batch - 1) // batch if ai else 0
        for i in range(0, len(ai), batch):
            if _stop_req():
                _log('! [AI] 已请求停止'); return
            chunk = ai[i:i + batch]
            res = mlib.ai_batch(chunk)
            if res is None:
                _log('  [AI] 批 %d 解析失败 -> 逐条兜底' % (i // batch + 1))
            for k, r in enumerate(chunk):
                if _stop_req():
                    return
                try:
                    mlib.process(d, r['id'], 'ai', ai_result=(res[k] if res else None))
                except Exception as e:
                    _log('[AI] id=%d ERR %s' % (r['id'], str(e)[:80]))
                bump()
            _log('[AI] 批 %d/%d 完成' % (i // batch + 1, nb))

    th = []
    for fn in (web_worker, ai_worker):
        t = threading.Thread(target=fn, daemon=True)
        t.start()
        th.append(t)
    for t in th:
        t.join()
    return cnt['n'], total


# ---------------------------------------------------------------- job bodies
def _job_scan(mode):
    _begin('scan:' + mode)
    try:
        _scan_pass(mode); _end('scan 完成')
    except Exception:
        _log('ERR ' + traceback.format_exc()[:500]); _end('scan 失败')


def _job_sem(limit, force, group=None):
    _begin('sem')
    try:
        ok, tot = _sem_pass(limit, force, group); mlib.unload_ai(); _end('sem %d/%d' % (ok, tot))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:500]); _end('sem 失败')


def _job_fetch(mode, limit, ai_group=None):
    _begin('fetch')
    try:
        ok, tot = _fetch_pass(mode, limit, ai_group); mlib.unload_ai(); _end('fetch %d/%d' % (ok, tot))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:500]); _end('fetch 失败')


def _nested_mark(dbc, r):
    """内嵌目录含视频 -> 标 有误 并返回 True（该条不处理）。"""
    try:
        if mlib.has_nested_videos(r.get('path')):
            dbc.execute("UPDATE items SET quality='有误' WHERE id=?", (r['id'],))
            dbc.commit()
            _log('  id=%d 内嵌目录含视频 -> 有误（跳过）' % r['id'])
            return True
    except Exception:
        pass
    return False


def _fill_pass(limit, ai_group=None, ids=None):
    """AI 补全：给【有简介但没标签】的条目按**简介**补 tag（**不看文件名**）。"""
    dbc = mlib.db()
    limit = int(limit or 0)
    ai_group = int(ai_group or 0)
    q = ("SELECT id,name,title,plot,code,sem_code,path FROM items WHERE present=1 "
         "AND COALESCE(plot,'')<>''")
    args = []
    if ids:
        # 选中项：按简介重补（允许覆盖旧标签，也不看 kind）
        q += ' AND id IN (%s)' % ','.join('?' * len(ids))
        args += list(ids)
    else:
        # 未选中：只补没标签、且不是嵌套目录的
        q += " AND COALESCE(kind,'')<>'nested' AND COALESCE(tags,'')=''"
    q += ' ORDER BY id'
    if limit > 0:
        q += ' LIMIT ?'
        args.append(limit)
    rows = [dict(r) for r in dbc.execute(q, tuple(args)).fetchall()]
    if not rows:
        _log('AI 补全: 没有【有简介、无标签】的条目，跳过')
        return 0, 0
    batch = ai_group if ai_group > 0 else (len(rows) or 1)
    if batch > 60:
        _log('  单批上限 60（自动切），请求组=%d' % batch)
        batch = 60
    _tick(0, len(rows))
    _log('AI 补全 %d 条，%d 条一批（只看简介，不看文件名）' % (len(rows), batch))
    ok = 0
    for i in range(0, len(rows), batch):
        if _stop_req():
            _log('! 已请求停止'); break
        chunk = rows[i:i + batch]
        chunk = [r for r in chunk if not _nested_mark(dbc, r)]   # 内嵌目录先剔掉
        if not chunk:
            continue
        res = mlib.ai_tags_batch(chunk)
        if res is None:
            _log('  批 %d 解析失败 -> 逐条兜底' % (i // batch + 1))
        for k, r in enumerate(chunk):
            if _stop_req():
                break
            tags = res[k] if res else None
            if not tags:
                one = mlib.ai_tags_batch([r])
                tags = one[0] if one else None
            if tags:
                dbc.execute("UPDATE items SET tags=?, quality='有', last_update=? WHERE id=?",
                            (','.join(tags), mlib.now(), r['id']))
                dbc.commit()
                try:
                    it = dict(dbc.execute('SELECT * FROM items WHERE id=?',
                                          (r['id'],)).fetchone())
                    mlib.write_nfo(it)          # 标签落盘到 nfo
                except Exception:
                    pass
                ok += 1
                _log('[%d/%d] id=%d 补 %d 个标签: %s'
                     % (i + k + 1, len(rows), r['id'], len(tags), ','.join(tags)[:70]))
            else:
                _log('[%d/%d] id=%d 补全失败（保持 缺）' % (i + k + 1, len(rows), r['id']))
            _tick(i + k + 1)
    return ok, len(rows)


def _job_fill(limit, ai_group=None, ids=None):
    _begin('fill')
    try:
        ok, tot = _fill_pass(limit, ai_group, ids)
        mlib.unload_ai()
        _end('fill %d/%d' % (ok, tot))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:500]); _end('fill 失败')


def _vscan_pass(limit, ids=None):
    """AI 看片：抽帧 -> 视觉模型 -> 标题/简介/标签。

    目标：文件名语义分析不出来的条目（quality 为空/无/有误）。
    """
    dbc = mlib.db()
    limit = int(limit or 0)
    if ids:
        # 选中项：不受 quality 限制（用户明确指定了）
        q = ("SELECT id,name,path,quality,title FROM items WHERE present=1 "
             "AND id IN (%s)" % ','.join('?' * len(ids)))
        args = list(ids)
    else:
        q = ("SELECT id,name,path,quality,title FROM items WHERE present=1 "
             "AND COALESCE(quality,'') IN ('','无','有误')")
        args = []
    q += ' ORDER BY id'
    if limit > 0:
        q += ' LIMIT ?'
        args.append(limit)
    rows = [dict(r) for r in dbc.execute(q, tuple(args)).fetchall()]
    if not rows:
        _log('AI 看片: 没有【无/有误】的条目，跳过')
        return 0, 0
    _tick(0, len(rows))
    _log('AI 看片 %d 条（抽帧看视频，不看文件名）' % len(rows))
    ok = 0
    for i, r in enumerate(rows):
        if _stop_req():
            _log('! 已请求停止'); break
        if _nested_mark(dbc, r):          # 内嵌目录含视频 -> 有误，不看片
            _tick(i + 1)
            continue
        d = mlib.ai_video_scan(r)
        if d:
            tags = ','.join(d['tags'])
            qual = '有' if tags else '缺'
            dbc.execute('UPDATE items SET title=?, plot=?, tags=?, quality=?, source=?, '
                        'site=?, last_update=? WHERE id=?',
                        (d['title'], d['plot'], tags, qual, 'AI', 'ai', mlib.now(), r['id']))
            dbc.commit()
            try:
                it = dict(dbc.execute('SELECT * FROM items WHERE id=?', (r['id'],)).fetchone())
                mlib.write_nfo(it)
            except Exception:
                pass
            ok += 1
            _log('[%d/%d] id=%d OK: %s | %s'
                 % (i + 1, len(rows), r['id'], d['title'][:28], tags[:46]))
        else:
            _log('[%d/%d] id=%d 看片失败' % (i + 1, len(rows), r['id']))
        _tick(i + 1)
    return ok, len(rows)


def _job_vscan(limit, ids=None):
    _begin('vscan')
    try:
        ok, tot = _vscan_pass(limit, ids)
        mlib.unload_ai()
        _end('vscan %d/%d' % (ok, tot))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:500]); _end('vscan 失败')


def _job_pipeline(limit, sgroup=None, agroup=None):
    _begin('pipeline')
    try:
        _scan_pass('incr'); _sem_pass(limit, False, sgroup); _fetch_pass('auto', limit, agroup)
        mlib.unload_ai(); _end('pipeline 完成')
    except Exception:
        _log('ERR ' + traceback.format_exc()[:500]); _end('pipeline 失败')


def _job_one(iid, mode, force):
    _begin('run #%d' % iid, 1)
    try:
        _one(mlib.db(), iid, mode, force); _tick(1); _end('run 完成')
    except Exception:
        _log('ERR ' + traceback.format_exc()[:500]); _end('run 失败')


def _job_queue():
    _begin('queue', len(QUEUE))
    dbc = mlib.db()
    n, ok = 0, 0
    while True:
        ent = q_pop()
        if not ent:
            break
        if _stop_req():
            _log('! 已请求停止')
            q_add(ent[0], ent[1])          # 没跑的放回队列
            break
        iid, mode = ent
        n += 1
        ok += 1 if _one(dbc, iid, mode, force=True) else 0
        _tick(n)
    mlib.unload_ai()
    _end('queue %d/%d' % (ok, n))


def start_job(fn, *a):
    with _LOCK:
        if STATE['running']:
            return False
        STATE['running'] = True

    def runner():
        try:
            fn(*a)
        except Exception:
            _log('ERR ' + traceback.format_exc()[:500])
            with _LOCK:
                STATE['running'] = False
    threading.Thread(target=runner, daemon=True).start()
    return True


# ---------------------------------------------------------------- read helpers
def _ollama():
    try:
        import requests
        r = requests.get(mlib.OLLAMA + '/api/tags', timeout=4)
        names = [m['name'] for m in r.json().get('models', [])]
        cur = mlib.ai_model()
        return {'up': True, 'model': cur,
                'model_ok': any(n.split(':')[0] == cur.split(':')[0] for n in names),
                'models': names}
    except Exception as e:
        return {'up': False, 'model': mlib.ai_model(), 'model_ok': False, 'error': str(e)[:90]}


def stats():
    dbc = mlib.db()
    out = {'total': dbc.execute('SELECT COUNT(*) c FROM items WHERE present=1').fetchone()['c'],
           'by_quality': {}, 'by_source': {}, 'sem': {}, 'ollama': _ollama(),
           'archive': dbc.execute('SELECT COUNT(*) c FROM archive').fetchone()['c'],
           'root': mlib.scan_root(), 'model': mlib.ai_model(), 'db': mlib.DB_PATH,
           'cookie': bool(mlib.javdb_cookie()),
           'queue': len(QUEUE)}
    for r in dbc.execute("SELECT COALESCE(NULLIF(quality,''),'无') q, COUNT(*) c "
                         'FROM items WHERE present=1 GROUP BY q'):
        out['by_quality'][r['q']] = r['c']
    for r in dbc.execute("SELECT COALESCE(NULLIF(source,''),'—') s, COUNT(*) c "
                         'FROM items WHERE present=1 GROUP BY s'):
        out['by_source'][r['s']] = r['c']
    for r in dbc.execute("SELECT COALESCE(NULLIF(sem_kind,''),'(未跑)') k, COUNT(*) c "
                         'FROM items WHERE present=1 GROUP BY k'):
        out['sem'][r['k']] = r['c']
    return out


def items(params):
    dbc = mlib.db()
    where, args = ['present=1'], []
    qy, sc, kd, q = (params.get('quality'), params.get('source'),
                     params.get('kind'), params.get('q'))
    if qy == '删除':
        where.append("quality='删除'")
    elif qy == '无':
        where.append("COALESCE(quality,'') IN ('','无')")
    elif qy:
        where.append('quality=?'); args.append(qy)
    else:
        where.append("COALESCE(quality,'')<>'删除'")     # 默认不含已删除
    if sc:
        where.append('source=?'); args.append(sc)
    if kd == '(未跑)':
        where.append("(sem_kind IS NULL OR sem_kind='')")
    elif kd:
        where.append('sem_kind=?'); args.append(kd)
    if q:
        where.append('(name LIKE ? OR title LIKE ? OR code LIKE ? OR sem_code LIKE ?)')
        args += ['%' + q + '%'] * 4
    ws = ' AND '.join(where)
    total = dbc.execute('SELECT COUNT(*) c FROM items WHERE ' + ws, args).fetchone()['c']
    limit = min(int(params.get('limit') or 50), 300)
    offset = int(params.get('offset') or 0)
    rows = dbc.execute('SELECT id,name,code,sem_kind,sem_code,sem_manual,status,site,'
                       'quality,source,title,plot,tags,actors,year,studio,cover,last_update '
                       'FROM items WHERE ' + ws + ' ORDER BY id LIMIT ? OFFSET ?',
                       args + [limit, offset]).fetchall()
    return {'total': total, 'limit': limit, 'offset': offset, 'rows': [dict(r) for r in rows]}


def item_detail(iid):
    dbc = mlib.db()
    r = dbc.execute('SELECT * FROM items WHERE id=?', (iid,)).fetchone()
    if not r:
        return None
    d = dict(r)
    try:
        d['nfo'] = io.open(os.path.join(d['path'], 'movie.nfo'), encoding='utf-8').read()[:4000]
    except Exception:
        d['nfo'] = ''
    try:
        d['files'] = sorted(os.listdir(d['path']))[:80]
    except Exception:
        d['files'] = []
    return d


def _img_files(d):
    out = []
    try:
        for n in os.listdir(d):
            if os.path.splitext(n)[1].lower() in ('.jpg', '.jpeg', '.png', '.webp'):
                out.append(os.path.join(d, n))
    except Exception:
        pass
    return out


def big_image(iid):
    """Path of the biggest image in the item folder (None if none)."""
    dbc = mlib.db()
    r = dbc.execute('SELECT path FROM items WHERE id=?', (iid,)).fetchone()
    if not r:
        return None
    files = _img_files(r['path'])
    if not files:
        return None
    files.sort(key=lambda p: os.path.getsize(p), reverse=True)
    return files[0]


def open_image(iid):
    """Open the item's big image with the HOST's viewer (local file, not a served copy)."""
    p = big_image(iid)
    if not p:
        return False
    try:
        os.startfile(p)
        return True
    except Exception as e:
        sys.stderr.write('openimg err %s\n' % str(e)[:80])
        return False


def make_poster(iid):
    """没封面就造一张：优先目录里已有的图，否则从视频里截一帧，写成 poster.jpg。"""
    dbc = mlib.db()
    r = dbc.execute('SELECT path FROM items WHERE id=?', (iid,)).fetchone()
    if not r or not r['path'] or not os.path.isdir(r['path']):
        return None
    p = r['path']
    dst = os.path.join(p, 'poster.jpg')
    if os.path.exists(dst):
        return dst
    # 1) 目录里已有图 -> 选最大的那张当海报
    src = None
    imgs = _img_files(p)
    if imgs:
        imgs.sort(key=lambda x: os.path.getsize(x), reverse=True)
        src = imgs[0]
    # 2) 没有图 -> 从视频里截一帧（时间轴中点）
    if not src:
        v = mlib.biggest_video(p)
        if v:
            frames = mlib.video_frames(os.path.join(p, v), n=1)
            if frames:
                try:
                    io.open(dst, 'wb').write(frames[0])
                    return dst
                except Exception:
                    return None
    if src:
        try:
            if os.path.abspath(src) != os.path.abspath(dst):
                io.open(dst, 'wb').write(io.open(src, 'rb').read())
            return dst
        except Exception:
            return src
    return None


def drop_poster_files(iid):
    """删掉条目的封面：目录里的 poster/fanart 等 + 应用里的缓存副本，并清空 DB cover。

    注意：**不动缩略图缓存**。
    """
    dbc = mlib.db()
    r = dbc.execute('SELECT path, cover FROM items WHERE id=?', (iid,)).fetchone()
    if not r:
        return 0
    n = 0
    names = ['poster.jpg', 'poster.png', 'fanart.jpg', 'fanart.png',
             'folder.jpg', 'cover.jpg']
    if r['cover']:
        names.append(os.path.basename(r['cover']))
    for nm in dict.fromkeys(names):
        try:
            fp = os.path.join(r['path'] or '', nm)
            if nm and os.path.exists(fp):
                os.remove(fp)
                n += 1
        except Exception:
            pass
    try:
        cp = os.path.join(mlib.CACHE_DIR, '%d.jpg' % iid)
        if os.path.exists(cp):
            os.remove(cp)
            n += 1
    except Exception:
        pass
    try:
        dbc.execute("UPDATE items SET cover='' WHERE id=?", (iid,))
        dbc.commit()
    except Exception:
        pass
    return n


def drop_covers(ids):
    """删封面图 + 缩略图（两个一起删）。"""
    n = 0
    for iid in ids:
        try:
            n += drop_poster_files(iid)
        except Exception:
            pass
        try:
            p = os.path.join(THUMB_DIR, '%d.jpg' % iid)
            if os.path.exists(p):
                os.remove(p)
                n += 1
        except Exception:
            pass
    return n


def refresh_thumbs(ids=None, make=False):
    """删掉缩略图缓存（下次显示时按需重建）。

    ids 为空 -> 全删；make=True -> 重建前先确保有封面（没封面就造一张）。
    """
    n = 0
    if ids:
        for iid in ids:
            if make:
                try:
                    make_poster(iid)
                except Exception:
                    pass
            p = os.path.join(THUMB_DIR, '%d.jpg' % iid)
            try:
                if os.path.exists(p):
                    os.remove(p)
                    n += 1
            except Exception:
                pass
        return n
    try:
        for f in os.listdir(THUMB_DIR):
            if f.lower().endswith('.jpg'):
                try:
                    os.remove(os.path.join(THUMB_DIR, f))
                    n += 1
                except Exception:
                    pass
    except Exception:
        pass
    return n


def cover_image(iid):
    """缩略图用的源图：**优先封面（cover / poster.jpg / fanart）**，
    没有才退回目录里最大的图。
    """
    dbc = mlib.db()
    r = dbc.execute('SELECT path, cover FROM items WHERE id=?', (iid,)).fetchone()
    if not r:
        return None
    p = r['path'] or ''
    names = []
    if r['cover']:
        names.append(os.path.basename(r['cover']))
    names += ['poster.jpg', 'poster.png', 'fanart.jpg', 'folder.jpg', 'cover.jpg']
    for n in names:
        if not n:
            continue
        fp = os.path.join(p, n)
        if os.path.exists(fp):
            return fp
    return big_image(iid)


def thumb(iid):
    """SMALL cached thumbnail (width 160) -> jpeg bytes.

    若条目在缩略图生成之后又被重跑过（last_update 更新），则重建——
    否则重跑完网格里还是旧图。
    """
    cp = os.path.join(THUMB_DIR, '%d.jpg' % iid)
    if os.path.exists(cp):
        try:
            row = mlib.db().execute('SELECT last_update FROM items WHERE id=?',
                                    (iid,)).fetchone()
            lu = (row['last_update'] or '') if row else ''
            stamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(cp)))
            if not lu or stamp >= lu:
                return io.open(cp, 'rb').read()
        except Exception:
            pass
    src = cover_image(iid)
    if not src:
        return None
    try:
        import cv2
        import numpy as np
        img = cv2.imdecode(np.fromfile(src, dtype=np.uint8), cv2.IMREAD_COLOR)
        if img is None:
            return None
        h, w = img.shape[:2]
        if w > 160:
            img = cv2.resize(img, (160, max(1, int(round(h * 160.0 / w)))),
                             interpolation=cv2.INTER_AREA)
        ok, buf = cv2.imencode('.jpg', img, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
        if not ok:
            return None
        try:
            os.makedirs(THUMB_DIR, exist_ok=True)
            buf.tofile(cp)
        except Exception:
            pass
        return buf.tobytes()
    except Exception:
        return None


def image(iid):
    """Big image bytes (kept for in-browser view)."""
    p = big_image(iid)
    if not p:
        return None, None
    ext = os.path.splitext(p)[1].lower()
    ctype = {'.png': 'image/png', '.webp': 'image/webp'}.get(ext, 'image/jpeg')
    try:
        return io.open(p, 'rb').read(), ctype
    except Exception:
        return None, None


def archive_rows(limit=200):
    dbc = mlib.db()
    return [dict(r) for r in dbc.execute(
        'SELECT ref,id,name,code,title,quality,source,archived_at,reason FROM archive '
        'ORDER BY ref DESC LIMIT ?', (limit,))]


def set_cell(iid, field, value):
    """Inline table edit: quality / source / sem_kind / sem_code.

    sem_* edits also set sem_manual=1 so the semantic pass will not overwrite them.
    """
    if field not in ('quality', 'source', 'sem_kind', 'sem_code'):
        return False
    c = mlib.db()
    if field in ('sem_kind', 'sem_code'):
        c.execute('UPDATE items SET %s=?, sem_manual=1, last_update=? WHERE id=?' % field,
                  (value, mlib.now(), iid))
    else:
        c.execute('UPDATE items SET %s=?, last_update=? WHERE id=?' % field,
                  (value, mlib.now(), iid))
    c.commit()
    return True


MODAL_FIELDS = ('quality', 'source', 'sem_kind', 'sem_code', 'title', 'plot',
                'tags', 'actors')


def save_item(iid, kv):
    """弹窗里一次保存多个字段；改了语义/番号则置 sem_manual=1，并重写 nfo。"""
    c = mlib.db()
    sets, vals = [], []
    for f in MODAL_FIELDS:
        if f in kv:
            sets.append('%s=?' % f)
            vals.append(kv[f])
    if not sets:
        return False
    if ('sem_kind' in kv) or ('sem_code' in kv):
        sets.append('sem_manual=1')
    sets.append('last_update=?')
    vals.append(mlib.now())
    vals.append(iid)
    c.execute('UPDATE items SET %s WHERE id=?' % ','.join(sets), tuple(vals))
    c.commit()
    try:
        it = dict(c.execute('SELECT * FROM items WHERE id=?', (iid,)).fetchone())
        mlib.write_nfo(it)
    except Exception:
        pass
    return True


def sites_info():
    on = set(mlib.sites_enabled())
    return [{'k': k, 'n': n, 't': t, 'u': u, 'has': h, 'on': k in on}
            for (k, n, t, u, h) in mlib.SITES]


def set_cookie(txt):
    """Write (or clear) javdb_cookie.txt. Accepts a raw cookie or a 'Cookie: ...' header."""
    txt = (txt or '').strip()
    if txt.lower().startswith('cookie:'):
        txt = txt.split(':', 1)[1].strip()
    try:
        if txt:
            io.open(mlib.COOKIE_FILE, 'w', encoding='utf-8').write(txt)
        elif os.path.exists(mlib.COOKIE_FILE):
            os.remove(mlib.COOKIE_FILE)
        return True
    except Exception as e:
        sys.stderr.write('cookie err %s\n' % str(e)[:80])
        return False


def open_item(iid):
    """Open the item's folder in Explorer and FORCE the window to the foreground."""
    dbc = mlib.db()
    r = dbc.execute('SELECT path FROM items WHERE id=?', (iid,)).fetchone()
    if not r:
        return False
    d = os.path.normpath(r['path'])
    try:
        subprocess.Popen(['explorer.exe', d])
    except Exception:
        try:
            os.startfile(d)
        except Exception as e2:
            sys.stderr.write('open err %s\n' % str(e2)[:80])
            return False
    time.sleep(1.2)
    # Explorer reuses an existing window and can stay behind -> activate by title
    nm = os.path.basename(d)
    ps = ("$w=New-Object -ComObject WScript.Shell; $w.AppActivate('%s') | Out-Null"
          % nm.replace("'", "''"))
    try:
        subprocess.run(['powershell', '-NoProfile', '-NonInteractive', '-Command', ps],
                       capture_output=True, timeout=30)
    except Exception:
        pass
    return True


def delete_item(iid):
    """Delete the item folder (video + 目录) to the Windows Recycle Bin, then archive the row.

    Uses Microsoft.VisualBasic FileSystem.DeleteDirectory(...,'SendToRecycleBin').
    NOTE: on a UNC/network path (NAS) Windows cannot recycle -- it deletes for good.
    """
    dbc = mlib.db()
    r = dbc.execute('SELECT path,name FROM items WHERE id=?', (iid,)).fetchone()
    if not r:
        return False, 'no such item'
    p = r['path']
    if not os.path.isdir(p):
        # 目录已经不在了（外部删除/改名）：仍然把记录归档，别让用户卡着删不掉
        _archive_row(dbc, iid, 'deleted(missing-folder)')
        return True, '目录已不存在 —— 已从库里移除'
    ps = ("Add-Type -AssemblyName Microsoft.VisualBasic; $ErrorActionPreference='Stop'; "
          "[Microsoft.VisualBasic.FileIO.FileSystem]::DeleteDirectory('%s','OnlyErrorDialogs','SendToRecycleBin')"
          % p.replace("'", "''"))
    try:
        rc = subprocess.run(['powershell', '-NoProfile', '-NonInteractive', '-Command', ps],
                            capture_output=True, timeout=1800)
    except Exception as e:
        return False, 'ps err %s' % str(e)[:100]
    if rc.returncode != 0 or os.path.isdir(p):
        msg = (rc.stderr or b'').decode('utf-8', 'replace') or 'delete failed'
        return False, msg.strip()[:200]
    _archive_row(dbc, iid, 'deleted')
    return True, '已删除到回收站'


def _archive_row(dbc, iid, reason):
    """把条目录入 archive，并标记为已删除（quality='删除'）。"""
    dbc.execute("INSERT INTO archive(id,path,name,code,title,quality,source,archived_at,reason) "
                "SELECT id,path,name,code,title,quality,source,?,? FROM items WHERE id=?",
                (mlib.now(), reason, iid))
    dbc.execute("UPDATE items SET quality='删除', present=1, last_scan=? WHERE id=?",
                (mlib.now(), iid))
    dbc.commit()


DOCS = {'readme': 'README.md', 'prompt': '有误处理提示词.md'}


def doc_page(name):
    fn = DOCS.get(name, name)
    try:
        body = io.open(os.path.join(HERE, fn), encoding='utf-8').read()
    except Exception:
        body = '(缺文件: %s)' % fn
    b = body.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    return ('<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>%s</title>'
            '<style>body{background:#12151b;color:#e6e9ef;font:14px/1.7 system-ui,"Segoe UI",'
            'sans-serif;padding:26px;max-width:1000px;margin:auto}pre{white-space:pre-wrap;'
            'word-break:break-word}a{color:#4c9aff}</style></head><body>'
            '<p><a href="/">← 返回</a> · <a href="/doc/readme">README</a> · '
            '<a href="/doc/prompt">有误处理提示词</a></p><pre>%s</pre></body></html>'
            % (fn, b))


# ---------------------------------------------------------------- http
class H(BaseHTTPRequestHandler):
    server_version = 'ZhuoYue-Scraper/1.0'

    def _send(self, code, body, ctype='application/json; charset=utf-8'):
        b = body.encode('utf-8') if isinstance(body, str) else body
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(b)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        try:
            self.wfile.write(b)
        except Exception:
            pass

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj, ensure_ascii=False))

    def _body(self):
        try:
            n = int(self.headers.get('Content-Length') or 0)
            return self.rfile.read(n).decode('utf-8', 'replace') if n else ''
        except Exception:
            return ''

    def log_message(self, *a):
        pass

    def do_GET(self):
        u = urlparse(self.path)
        p, q = u.path, {k: v[0] for k, v in parse_qs(u.query).items()}
        try:
            if p in ('/', '/index.html'):
                return self._send(200, PAGE, 'text/html; charset=utf-8')
            if p.startswith('/doc/'):
                return self._send(200, doc_page(p.rsplit('/', 1)[1]), 'text/html; charset=utf-8')
            if p == '/api/sites':
                return self._json({'sites': sites_info(), 'on': mlib.sites_enabled()})
            if p == '/api/net':
                return self._json(net_state())
            if p == '/api/lang':
                return self._json({'lang': mlib.out_lang()})
            if p == '/api/stats':
                return self._json(stats())
            if p == '/api/items':
                return self._json(items(q))
            if p.startswith('/api/image/'):
                b, ct = image(int(p.rsplit('/', 1)[1]))
                return self._send(200, b, ct) if b else self._send(404, b'', 'image/jpeg')
            if p.startswith('/api/thumb/'):
                b = thumb(int(p.rsplit('/', 1)[1]))
                return self._send(200, b, 'image/jpeg') if b else self._send(404, b'', 'image/jpeg')
            if p.startswith('/api/item/'):
                d = item_detail(int(p.rsplit('/', 1)[1]))
                return self._json(d or {'error': 'not found'}, 200 if d else 404)
            if p == '/api/archive':
                return self._json({'rows': archive_rows(int(q.get('limit') or 200))})
            if p == '/api/job':
                with _LOCK:
                    return self._json(dict(STATE, queue=len(QUEUE)))
            if p == '/api/ollama':
                return self._json(_ollama())
            return self._json({'error': 'bad path'}, 404)
        except Exception as e:
            return self._json({'error': str(e)[:300]}, 500)

    def do_POST(self):
        u = urlparse(self.path)
        p, q = u.path, {k: v[0] for k, v in parse_qs(u.query).items()}
        lim = int(q.get('limit') or 100000)
        force = q.get('force') in ('1', 'true', 'yes')
        mode = q.get('mode') or 'auto'
        try:
            if p == '/api/config':
                if q.get('root'):
                    mlib.cfg_set('root', q['root'])
                if q.get('model'):
                    mlib.cfg_set('model', q['model'])
                return self._json({'ok': True, 'root': mlib.scan_root(), 'model': mlib.ai_model()})
            if p == '/api/scan':
                ok = start_job(_job_scan, q.get('mode') or 'incr')
            elif p == '/api/sem':
                ok = start_job(_job_sem, lim, force, int(q.get('group') or 0))
            elif p == '/api/fetch':
                ok = start_job(_job_fetch, mode, lim, int(q.get('ai_group') or 0))
            elif p == '/api/fill':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                ok = start_job(_job_fill, lim, int(q.get('ai_group') or 0), ids)
            elif p == '/api/vscan':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                ok = start_job(_job_vscan, lim, ids)
            elif p == '/api/pipeline':
                ok = start_job(_job_pipeline, lim, int(q.get('group') or 0),
                               int(q.get('ai_group') or 0))
            elif p == '/api/queue/run':
                ok = start_job(_job_queue)
            elif p == '/api/queue/clear':
                q_clear()
                return self._json({'ok': True, 'queue': 0})
            elif p.startswith('/api/queue/'):
                n = q_add(int(p.rsplit('/', 1)[1]), mode)
                return self._json({'ok': True, 'queue': n})
            elif p.startswith('/api/run/'):
                ok = start_job(_job_one, int(p.rsplit('/', 1)[1]), mode, force)
            elif p.startswith('/api/manual/sem/'):
                iid = int(p.rsplit('/', 1)[1])
                c = mlib.db()
                c.execute('UPDATE items SET sem_kind=?,sem_code=?,sem_manual=1,last_update=? '
                          'WHERE id=?', (q.get('kind') or 'none', q.get('code') or '',
                                         mlib.now(), iid))
                c.commit()
                return self._json({'ok': True})
            elif p.startswith('/api/manual/state/'):
                iid = int(p.rsplit('/', 1)[1])
                c = mlib.db()
                c.execute('UPDATE items SET quality=?,source=?,sem_manual=COALESCE(sem_manual,0),'
                          'last_update=? WHERE id=?',
                          (q.get('quality') or '', q.get('source') or '', mlib.now(), iid))
                c.commit()
                return self._json({'ok': True})
            elif p.startswith('/api/delete/'):
                ok, msg = delete_item(int(p.rsplit('/', 1)[1]))
                return self._json({'ok': ok, 'msg': msg})
            elif p.startswith('/api/cell/'):
                iid = int(p.rsplit('/', 1)[1])
                return self._json({'ok': set_cell(iid, q.get('field') or '',
                                                  q.get('value') or '')})
            elif p.startswith('/api/save/'):
                iid2 = int(p.rsplit('/', 1)[1])
                kv2 = {k: v[0] for k, v in parse_qs(self._body() or '').items()}
                return self._json({'ok': save_item(iid2, kv2)})
            elif p == '/api/thumbs/refresh':
                tids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                return self._json({'ok': True, 'n': refresh_thumbs(tids or None, make=True)})
            elif p == '/api/covers/drop':
                tids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                return self._json({'ok': True, 'n': drop_covers(tids) if tids else 0})
            elif p == '/api/sites':
                mlib.set_sites([x for x in (q.get('on') or '').split(',') if x])
                return self._json({'ok': True, 'on': mlib.sites_enabled()})
            elif p == '/api/net':
                try:
                    _p = int(q.get('port') or 0) or 8800
                except Exception:
                    _p = 8800
                _lan = (q.get('lan') or '') in ('1', 'true', 'on')
                mlib.cfg_set('port', str(_p))
                mlib.cfg_set('lan', '1' if _lan else '0')
                _ok, _msg = rebind('0.0.0.0' if _lan else '127.0.0.1', _p)
                _st = net_state()
                return self._json({'ok': _ok, 'msg': _msg, 'port': _st['port'],
                                   'lan': _st['lan'], 'urls': _st['urls']})
            elif p == '/api/lang':
                _lg = (q.get('lang') or 'zh').strip().lower()
                mlib.cfg_set('lang', 'en' if _lg.startswith('en') else 'zh')
                return self._json({'ok': True, 'lang': mlib.out_lang()})
            elif p == '/api/sites/check':
                res = {}
                for (k, n, t, u, h) in mlib.SITES:
                    res[k] = mlib.site_check(k)
                return self._json({'ok': True, 'res': res})
            elif p == '/api/cookie':
                if q.get('clear'):
                    set_cookie('')
                    return self._json({'ok': True, 'set': False})
                ok = set_cookie(self._body() or q.get('text', ''))
                cur = mlib.javdb_cookie()
                return self._json({'ok': ok, 'set': bool(cur), 'len': len(cur or '')})
            elif p.startswith('/api/openimg/'):
                return self._json({'ok': open_image(int(p.rsplit('/', 1)[1]))})
            elif p.startswith('/api/open/'):
                return self._json({'ok': open_item(int(p.rsplit('/', 1)[1]))})
            elif p == '/api/stop':
                with _LOCK:
                    STATE['stop'] = True
                return self._json({'ok': True})
            else:
                return self._json({'error': 'bad path'}, 404)
            return self._json({'ok': ok, 'busy': not ok})
        except Exception as e:
            return self._json({'error': str(e)[:300]}, 500)


PAGE = r"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>卓"悦"刮削器 — 给你和你的小姐姐</title>
<style>
:root{--bg:#12151b;--card:#1b202a;--fg:#e6e9ef;--dim:#8b93a7;--acc:#4c9aff;--ok:#3fbf6f;--warn:#e8b23a;--bad:#e05c5c;--pur:#c08cff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:13px/1.5 system-ui,"Segoe UI",sans-serif}
header{padding:10px 16px;background:var(--card);border-bottom:1px solid #2a3140;display:flex;gap:12px;align-items:center;flex-wrap:wrap;position:sticky;top:0;z-index:11}
.stickytop{position:sticky;z-index:10;background:var(--bg);padding-top:8px;box-shadow:0 8px 14px -10px #000}
h1{font-size:15px;margin:0;font-weight:600}
.wrap{padding:14px 16px;max-width:1600px;margin:0 auto}
.cards{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
.card{background:var(--card);border:1px solid #2a3140;border-radius:9px;padding:8px 12px;min-width:86px}
.card b{display:block;font-size:19px;line-height:1.2}.card span{color:var(--dim);font-size:11px}
.bar{display:flex;gap:7px;flex-wrap:wrap;align-items:center;background:var(--card);border:1px solid #2a3140;border-radius:9px;padding:9px 11px;margin-bottom:10px}
button{background:#26303f;color:var(--fg);border:1px solid #39455a;border-radius:7px;padding:6px 11px;cursor:pointer;font-size:12px}
button:hover{background:#2f3b4d}button:disabled{opacity:.45;cursor:not-allowed}
button.primary{background:var(--acc);border-color:var(--acc);color:#08121f;font-weight:600}
button.danger{background:#3a2226;border-color:#5a3238}
input,select,textarea{background:#0e1117;color:var(--fg);border:1px solid #39455a;border-radius:7px;padding:5px 7px;font-size:12px}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:9px;overflow:hidden}
th,td{padding:6px 8px;border-bottom:1px solid #232a36;text-align:left;vertical-align:top;font-size:12px}
th{color:var(--dim);background:#171c25;font-weight:600}
tr:hover td{background:#1f2531}
.q-有{color:var(--ok);font-weight:600}.q-有误{color:var(--bad);font-weight:600}.q-无{color:var(--dim)}
.s-code{color:var(--ok)}.s-Uncen{color:var(--pur)}.s-AI{color:var(--warn)}
.thumb{width:66px;height:38px;object-fit:cover;border-radius:5px;background:#0b0e13}
tr.sel td{background:#22314a}
.selbtn{background:#26303f;color:var(--fg);border:1px solid #39455a;border-radius:6px;padding:3px 8px;cursor:pointer;font-size:11px}
tr.sel .selbtn{background:var(--acc);border-color:var(--acc);color:#08121f;font-weight:600}
.meta{color:var(--dim);font-size:11px;margin-top:2px}
.tags{color:#9fb0d0}
#log{background:#0b0e13;border:1px solid #2a3140;border-radius:9px;padding:9px;height:170px;overflow:auto;font:12px/1.45 ui-monospace,Consolas,monospace;white-space:pre-wrap;margin-bottom:10px}
#prog{height:5px;background:#0b0e13;border-radius:4px;overflow:hidden;margin-bottom:9px}
#prog>i{display:block;height:100%;background:var(--acc);width:0}
.muted{color:var(--dim)}.right{margin-left:auto}
.modal{position:fixed;inset:0;background:#0009;display:none;align-items:center;justify-content:center;padding:22px;z-index:1000}
.modal.on{display:flex}
.modal .box{background:var(--card);border:1px solid #2a3140;border-radius:11px;max-width:920px;width:100%;max-height:86vh;overflow:auto;padding:15px}
.modal .box>.bar:first-child{position:sticky;top:-15px;margin:-15px -15px 10px;padding:12px 15px;background:var(--card);border:0;border-bottom:1px solid #2a3140;border-radius:11px 11px 0 0;z-index:6}
pre{background:#0b0e13;border:1px solid #2a3140;border-radius:7px;padding:9px;overflow:auto;font-size:11px}
</style></head><body>
<header>
  <h1>卓"悦"刮削器</h1> <span class="muted">给你和你的小姐姐</span>
  <a href="/doc/readme" target="_blank"><button>说明</button></a>
  <button id="b-cookie">javdb Cookie</button>
  <button id="b-sites">站点</button>
  <button id="b-lang">EN</button>
  <span class="muted">媒体库</span><input id="cfg-root" style="width:330px">
  <span class="muted">模型</span><select id="cfg-model"></select>
  <span class="muted">端口</span><input id="cfg-port" type="number" style="width:80px"> <label class="muted"><input type="checkbox" id="cfg-lan"> 局域网</label>
  <button id="cfg-save">保存设置</button>
  <span id="ollama" class="muted right">ollama …</span>
</header>
<div class="wrap">
  <div class="stickytop" id="stickytop">
  <div class="cards" id="cards"></div>
  <div class="bar">
    <button class="primary" id="b-pipeline">① 全流程</button>
    <button class="warn-btn" id="b-rebuild">建库</button>
    <button id="b-incr">增量扫描</button>
    <button id="b-sem">语义识别</button>
    <button id="b-fetch">抓取</button>
    <button id="b-qrun">跑队列</button>
    <button id="b-qclear">清空队列</button>
    <button class="danger" id="b-stop">停止</button>
    <span class="muted">上限(0=全部)</span><input id="limit" type="number" value="0" style="width:104px">
    <span class="muted">模式</span>
    <select id="mode"><option value="auto">auto</option><option value="ai">ai</option>
      <option value="website">website</option></select>
    <span class="muted">语义组(0=全部)</span><input id="semg" type="number" value="20" style="width:76px">
    <span class="muted">AI组(0=全部)</span><input id="aig" type="number" value="5" style="width:74px">
    <label class="muted"><input type="checkbox" id="force"> force</label>
    <span class="muted right">队列 <b id="qcount">0</b></span>
  </div>
  <div id="prog"><i></i></div>
  <div id="log"></div>
  <div class="bar">
    <span class="muted">筛选</span>
    <select id="f-quality"><option value="">质量:全部</option><option value="有">有</option><option value="缺">缺</option><option value="有误">有误</option><option value="无">无</option><option value="删除">删除</option></select>
    <select id="f-source"><option value="">来源:全部</option><option value="code">code</option><option value="Uncen">Uncen</option><option value="AI">AI</option></select>
    <select id="f-kind"><option value="">语义:全部</option><option>coded</option><option>uncensored</option><option>none</option><option>(未跑)</option></select>
    <input id="f-q" placeholder="搜索 名字/标题/番号" style="width:200px">
    <button id="b-refresh">刷新</button>
    <button id="b-archive">档案</button>
    <span class="muted right" id="count"></span>
  </div>
  <div class="bar" id="actbar" style="background:#171c25">
    <span class="muted">已选</span><b id="seln">0</b><span class="muted">条</span>
    <button class="primary" id="a-web">重跑·网站</button>
    <button id="a-ai">重跑·AI</button>
    <button id="a-fill">AI 补全</button>
    <button id="a-vscan">AI 看片</button>
    <button id="a-auto">重跑·全流程</button>
    <select id="qmode" style="min-width:86px"><option value="auto">auto</option><option value="website">website</option><option value="ai">ai</option></select>
    <button id="a-queue">重跑队列</button>
    <button id="a-open">打开目录</button>
    <button id="a-thumbs">刷新缩略图</button>
    <button id="a-coverdel">删除封面图</button>
    <button class="danger" id="a-del">删除</button>
    <span class="muted">|</span>
    <button id="a-selall">全选本页</button>
    <button id="a-selnone">取消选择</button>
  </div>
  </div>
  <table><thead><tr>
    <th style="width:74px">缩略图</th><th style="width:48px">id</th>
    <th style="width:58px">质量</th><th style="width:66px">来源</th>
    <th style="width:96px">番号</th><th style="width:92px">语义</th>
    <th>标题 / 元信息</th><th style="width:70px">选择</th>
  </tr></thead><tbody id="rows"></tbody></table>
  <div class="bar"><button id="b-prev">‹ 上一页</button><select id="pjump" style="min-width:132px"></select><span id="page" class="muted"></span><button id="b-next">下一页 ›</button><button id="b-reload" class="right">⟳ 刷新列表</button></div>
</div>
<div class="modal" id="modal"><div class="box">
  <div class="bar"><b id="md-title">item</b><button class="right" id="md-close">关闭</button></div>
  <div id="md-body"></div></div></div>
<div class="modal" id="modal3"><div class="box">
  <div class="bar"><b>JavDB Cookie</b><span class="muted" id="ck-state"></span><button class="right" id="md3-close">关闭</button></div>
  <p class="muted">粘贴 Cookie 字符串（可含 “Cookie:” 前缀），或选文件上传。保存后写入 javdb_cookie.txt，JavDB 默认识别即生效。</p>
  <textarea id="ck-text" style="width:100%;height:130px" placeholder="vd=...; over18=1; ..."></textarea>
  <div class="bar"><input type="file" id="ck-file"><button class="primary" id="ck-save">保存</button><button id="ck-clear">清空</button></div>
</div></div>
<div class="modal" id="modal2"><div class="box">
  <div class="bar"><b id="md2-title">档案</b><button class="right" id="md2-close">关闭</button></div>
  <div id="md2-body"></div></div></div>
<div class="modal" id="modal4"><div class="box">
  <div class="bar"><b>站点设置</b><span class="muted" id="st-info"></span><button class="right" id="md4-close">关闭</button></div>
  <p class="muted">勾选要使用的站点（抓取时按此顺序尝试）。javbus / javdb / MissAV 已有专属解析器，其余为待接入站点。</p>
  <table id="st-tbl"></table>
  <div class="bar"><button id="st-check">检测连通性</button><button class="primary" id="st-save">保存</button></div>
</div></div>
<script>
const $=s=>document.querySelector(s);let off=0,lim=50,THUMBV=0;
const esc=t=>String(t==null?'':t).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
async function post(p){const r=await fetch(p,{method:'POST'});const j=await r.json();
  if(j.busy)alert('已有任务在跑');return j;}
function tagsOf(s){try{const a=JSON.parse(s||'[]');return (a||[]).join(' / ');}catch(e){return s||'';}}
async function cfg(){const s=await(await fetch('/api/stats')).json();
  if(!$('#cfg-root').value)$('#cfg-root').value=s.root;
  const sel=$('#cfg-model');const ms=(s.ollama&&s.ollama.models)||[];
  if(sel.options.length!==ms.length+1){sel.innerHTML='<option value="'+esc(s.model)+'">'+esc(s.model)+'</option>'
    +ms.map(m=>'<option>'+esc(m)+'</option>').join('');}
  sel.value=s.model;
  const o=s.ollama||{};$('#ollama').innerHTML=o.up?('ollama ✅ '+(o.model_ok?'':'<span style="color:var(--bad)">模型缺失</span>')):'<span style="color:var(--bad)">ollama ✗ '+(o.error||'')+'</span>';
  const q=s.by_quality||{},sr=s.by_source||{},sm=s.sem||{};
  $('#cards').innerHTML=[['总数',s.total],['质量·有',q['有']||0],['质量·缺',q['缺']||0],['质量·有误',q['有误']||0],['质量·无',q['无']||0],['质量·删除',q['删除']||0],
     ['来源·code',sr.code||0],['来源·Uncen',sr.Uncen||0],['来源·AI',sr.AI||0],
     ['语义未跑',sm['(未跑)']||0],['档案',s.archive]]
    .map(([k,v])=>'<div class="card"><b>'+v+'</b><span>'+k+'</span></div>').join('');
  $('#qcount').textContent=s.queue||0;}
async function job(){const j=await(await fetch('/api/job')).json();
  $('#log').textContent=(j.log||[]).join('\n');
  const L=$('#log');L.scrollTop=L.scrollHeight;   // 永远默认停在最后一行
  const p=j.total?Math.round(j.done*100/j.total):0;
  $('#prog').firstElementChild.style.width=(j.running?Math.max(p,3):(j.done?'100':'0'))+'%';
  const busy=j.running;['b-pipeline','b-rebuild','b-incr','b-sem','b-fetch','b-qrun'].forEach(i=>$('#'+i).disabled=busy);
  $('#b-stop').disabled=!busy;$('#qcount').textContent=j.queue||0;}
function saveState(){try{localStorage.setItem('ms_state',JSON.stringify({off:off,
  quality:$('#f-quality').value,source:$('#f-source').value,kind:$('#f-kind').value,q:$('#f-q').value}));}catch(e){}}
function loadState(){try{const s=JSON.parse(localStorage.getItem('ms_state')||'{}');
  if(typeof s.off==='number'&&s.off>=0)off=s.off;
  if(s.quality!==undefined)$('#f-quality').value=s.quality;
  if(s.source!==undefined)$('#f-source').value=s.source;
  if(s.kind!==undefined)$('#f-kind').value=s.kind;
  if(s.q!==undefined)$('#f-q').value=s.q;}catch(e){}}
async function list(){const qs=new URLSearchParams({limit:lim,offset:off,
  quality:$('#f-quality').value,source:$('#f-source').value,kind:$('#f-kind').value,q:$('#f-q').value});
  const d=await(await fetch('/api/items?'+qs)).json();
  $('#rows').innerHTML=d.rows.map(r=>{
    const q=r.quality||'';
    return '<tr id="r'+r.id+'"><td><a href="#" class="imgopen" data-id="'+r.id+'" title="本地打开大图"><img class="thumb" loading="lazy" src="/api/thumb/'+r.id+'?v='+THUMBV+'" onerror="this.style.opacity=.15"></a></td>'
    +'<td>'+r.id+'</td>'
    +'<td class="q-'+q+' ed" data-f="quality" data-id="'+r.id+'" data-v="'+(q||'')+'" title="双击修改">'+(q||'无')+'</td>'
    +'<td class="s-'+(r.source||'')+' ed" data-f="source" data-id="'+r.id+'" data-v="'+(r.source||'')+'" title="双击修改">'+(r.source||'—')+'</td>'
    +'<td class="ed" data-f="sem_code" data-id="'+r.id+'" data-v="'+esc(r.sem_code||r.code||'')+'" title="双击修改">'+esc(r.sem_code||r.code||'')+'</td>'
    +'<td class="ed" data-f="sem_kind" data-id="'+r.id+'" data-v="'+(r.sem_kind||'')+'" title="双击修改">'+(r.sem_kind||'—')+(r.sem_manual?' <span class="muted">(人工)</span>':'')+'</td>'
    +'<td><a href="#" data-id="'+r.id+'" class="lk" style="color:var(--fg)">'+esc(r.title||r.name)+'</a>'
    +'<div class="meta">简介: '+esc((r.plot||'').slice(0,70))+'</div>'
    +'<div class="meta">标签: <span class="tags">'+esc(tagsOf(r.tags))+'</span></div>'
    +'<div class="meta">作者: '+esc(tagsOf(r.actors))+' ｜ 年份: '+esc(r.year||'')+' ｜ 制作: '+esc(r.studio||'')+'</div>'
    +'</td>'
    +'<td><button class="selbtn" data-id="'+r.id+'">选择</button></td></tr>';}).join('');
  $('#count').textContent='共 '+d.total+' 条';
  document.querySelectorAll('.selbtn').forEach(b=>{if(SEL.has(b.dataset.id))selSet(b,true);});
  const pages=Math.max(1,Math.ceil(d.total/lim)),cur=Math.floor(off/lim)+1;
  const pj=$('#pjump');pj.innerHTML='';
  for(let i=1;i<=pages;i++){const o=document.createElement('option');
    o.value=(i-1)*lim;o.textContent='第 '+i+' / '+pages+' 页';if(i===cur)o.selected=true;pj.appendChild(o);}
  $('#page').textContent=(off+1)+'–'+Math.min(off+lim,d.total);
  saveState();}
const SEL=new Set();
function selinfo(){$('#seln').textContent=SEL.size;}
function selSet(b,on){const id=b.dataset.id,tr=b.closest('tr');
  if(on){SEL.add(id);tr.classList.add('sel');b.textContent='✓ 已选';}
  else{SEL.delete(id);tr.classList.remove('sel');b.textContent='选择';}}
$('#a-selall').onclick=()=>{document.querySelectorAll('.selbtn').forEach(b=>selSet(b,true));selinfo();};
$('#a-selnone').onclick=()=>{SEL.clear();document.querySelectorAll('.selbtn').forEach(b=>selSet(b,false));selinfo();};
function batchRun(mode){return async()=>{if(!SEL.size)return alert('先选条目');let n=0;
  for(const id of SEL){const r=await post('/api/queue/'+id+'?mode='+mode);n=r.queue;}
  $('#qcount').textContent=n;await post('/api/queue/run');};}
$('#a-web').onclick=batchRun('website');
$('#a-ai').onclick=batchRun('ai');
$('#a-fill').onclick=async()=>{
  const ids=[...SEL].join(',');
  if(!SEL.size&&!confirm('未选中条目 —— 将对【全部 有简介但没标签】的条目按简介补标签，继续？'))return;
  await post('/api/fill?limit='+$('#limit').value+'&ai_group='+$('#aig').value+(ids?'&ids='+ids:''));};
$('#a-vscan').onclick=async()=>{
  const ids=[...SEL].join(',');
  if(!SEL.size&&!confirm('未选中条目 —— 将对【全部 无/有误】的条目抽帧看视频（较慢），继续？'))return;
  await post('/api/vscan?limit='+$('#limit').value+(ids?'&ids='+ids:''));};
$('#a-auto').onclick=batchRun('auto');
$('#a-queue').onclick=async()=>{if(!SEL.size)return alert('先选条目');let n=0;
  for(const id of SEL){const r=await post('/api/queue/'+id+'?mode='+$('#qmode').value);n=r.queue;}
  $('#qcount').textContent=n;};
$('#a-open').onclick=async()=>{if(!SEL.size)return alert('先选条目');
  for(const id of SEL)await post('/api/open/'+id);};
$('#a-del').onclick=async()=>{if(!SEL.size)return alert('先选条目');
  if(!confirm('【删除】共 '+SEL.size+' 个目录（含视频）→ Windows 回收站\n\n⚠ NAS 网络路径，实际是【永久删除】，不可恢复。\n\n继续？'))return;
  if(!confirm('再次确认：永久删除这 '+SEL.size+' 个？'))return;
  for(const id of SEL)await post('/api/delete/'+id);
  SEL.clear();selinfo();list();};
document.addEventListener('dblclick',e=>{
  const td=e.target.closest?e.target.closest('td.ed'):null;
  if(!td)return;
  const f=td.dataset.f,id=td.dataset.id,cur=td.dataset.v||'';
  const OPTS={quality:['有','缺','有误','无','删除'],source:['code','Uncen','AI'],
              sem_kind:['coded','uncensored','none']};
  let el;
  if(OPTS[f]){el=document.createElement('select');
    el.innerHTML='<option value=""></option>'+OPTS[f].map(o=>'<option>'+o+'</option>').join('');
    el.value=cur;}
  else{el=document.createElement('input');el.value=cur;el.style.width='110px';}
  td.innerHTML='';td.appendChild(el);el.focus();if(el.select)el.select();
  const save=async()=>{const v=el.value;
    await fetch('/api/cell/'+id+'?field='+f+'&value='+encodeURIComponent(v),{method:'POST'});list();};
  el.onchange=save;
  el.onkeydown=ev=>{if(ev.key==='Enter')save();else if(ev.key==='Escape')list();};
  if(!OPTS[f])el.onblur=save;
});
document.addEventListener('click',async e=>{
  const sb=e.target.closest?e.target.closest('.selbtn'):null;
  if(sb){selSet(sb,!SEL.has(sb.dataset.id));selinfo();return;}
  const ia=e.target.closest?e.target.closest('.imgopen'):null;
  if(ia){e.preventDefault();await post('/api/openimg/'+ia.dataset.id);return;}
  const t=e.target;if(!t.classList.contains('lk'))return;e.preventDefault();
  const d=await(await fetch('/api/item/'+t.dataset.id)).json();
  $('#md-title').textContent='#'+d.id+'  '+d.name;
  const sel=(v,arr)=>arr.map(o=>'<option'+(o===v?' selected':'')+'>'+o+'</option>').join('');
  $('#md-body').innerHTML=
    '<div class="bar">'
    +'<span class="muted">质量</span><select id="ed-quality">'+sel(d.quality||'',['有','缺','有误','无','删除'])+'</select>'
    +'<span class="muted">来源</span><select id="ed-source">'+sel(d.source||'',['code','Uncen','AI'])+'</select>'
    +'<span class="muted">语义</span><select id="ed-sem_kind">'+sel(d.sem_kind||'',['coded','uncensored','none'])+'</select>'
    +'<span class="muted">番号</span><input id="ed-sem_code" value="'+esc(d.sem_code||d.code||'')+'" style="width:180px">'
    +'<button class="primary" id="ed-save" data-id="'+d.id+'">保存</button>'
    +'<span class="muted right">status='+esc(d.status||'')+'</span></div>'
    +'<div class="bar"><span class="muted">标题</span><input id="ed-title" value="'+esc(d.title||'')+'" style="flex:1;min-width:300px"></div>'
    +'<div class="bar"><span class="muted">简介</span><textarea id="ed-plot" style="flex:1;min-width:300px;height:80px">'+esc(d.plot||'')+'</textarea></div>'
    +'<div class="bar"><span class="muted">标签</span><input id="ed-tags" value="'+esc(rawList(d.tags))+'"><span class="muted">逗号分隔</span></div>'
    +'<div class="bar"><span class="muted">作者</span><input id="ed-actors" value="'+esc(rawList(d.actors))+'" style="flex:1"></div>'
    +'<div class="meta">文件: '+esc((d.files||[]).join(' | '))+'</div><pre>'+esc(d.nfo||'(无 movie.nfo)')+'</pre>';
  $('#modal').classList.add('on');
  const _box=$('#modal').querySelector('.box');if(_box)_box.scrollTop=0;});
$('#md-close').onclick=()=>$('#modal').classList.remove('on');
$('#md-body').addEventListener('click',async e=>{
  if(e.target.id!=='ed-save')return;
  const id=e.target.dataset.id;
  const kv={quality:$('#ed-quality').value,source:$('#ed-source').value,
    sem_kind:$('#ed-sem_kind').value,sem_code:$('#ed-sem_code').value,
    title:$('#ed-title').value,plot:$('#ed-plot').value,
    tags:$('#ed-tags').value,actors:$('#ed-actors').value};
  await fetch('/api/save/'+id,{method:'POST',
    headers:{'Content-Type':'application/x-www-form-urlencoded'},
    body:new URLSearchParams(kv).toString()});
  e.target.textContent='已保存 ✓';setTimeout(()=>e.target.textContent='保存',1200);
  list();});
$('#md2-close').onclick=()=>$('#modal2').classList.remove('on');
$('#cfg-save').onclick=async()=>{await fetch('/api/config?root='+encodeURIComponent($('#cfg-root').value)
  +'&model='+encodeURIComponent($('#cfg-model').value),{method:'POST'});
  const n=await(await fetch('/api/net?port='+($('#cfg-port').value||0)+'&lan='+($('#cfg-lan').checked?1:0),{method:'POST'})).json();
  if(n.msg)console.log(n.msg); if(n.urls&&n.urls.length)console.log('访问地址: '+n.urls.join('  '));
  cfg();};
(async()=>{try{const n=await(await fetch('/api/net')).json();
  $('#cfg-port').value=n.port||8800; $('#cfg-lan').checked=!!n.lan;
  if(n.lan)console.log('局域网已开启: '+(n.urls||[]).join('  '));}catch(e){}})();
$('#b-pipeline').onclick=()=>post('/api/pipeline?limit='+$('#limit').value+'&group='+$('#semg').value+'&ai_group='+$('#aig').value);
$('#b-rebuild').onclick=()=>{ if(!confirm('【建库】会清空并重建全部档案！\n当前所有行的状态/语义/元信息都会被重置，旧记录先进入“档案”。\n\n确定继续？'))return;
  if(!confirm('请再次确认：真的要【建库】吗？'))return; post('/api/scan?mode=rebuild'); };
$('#b-incr').onclick=()=>post('/api/scan?mode=incr');
$('#b-sem').onclick=()=>post('/api/sem?limit='+$('#limit').value+'&group='+$('#semg').value+($('#force').checked?'&force=1':''));
$('#b-fetch').onclick=()=>post('/api/fetch?mode='+$('#mode').value+'&limit='+$('#limit').value+'&ai_group='+$('#aig').value);
$('#b-cookie').onclick=async()=>{const s=await(await fetch('/api/stats')).json();
  $('#ck-state').textContent=s.cookie?'当前：已设置':'当前：未设置';$('#modal3').classList.add('on');};
$('#md3-close').onclick=()=>$('#modal3').classList.remove('on');
$('#ck-file').onchange=e=>{const f=e.target.files[0];if(!f)return;const rd=new FileReader();
  rd.onload=()=>{$('#ck-text').value=rd.result;};rd.readAsText(f);};
$('#ck-save').onclick=async()=>{const r=await fetch('/api/cookie',{method:'POST',body:$('#ck-text').value});
  const j=await r.json();alert(j.set?('已保存 ('+j.len+' 字符)'):'已清空');$('#modal3').classList.remove('on');};
$('#ck-clear').onclick=async()=>{await fetch('/api/cookie?clear=1',{method:'POST'});alert('已清空');$('#modal3').classList.remove('on');};
const I18N={'总数':'Total','质量有':'OK','质量缺':'Partial','质量有误':'Bad','质量无':'None',
'质量删除':'Deleted','来源 code':'Src:code','来源 Uncen':'Src:Uncen','来源 AI':'Src:AI',
'语义未跑':'Sem:none','档案':'Archive','建库':'Rebuild','增量扫描':'Scan','语义识别':'Classify',
'抓取':'Fetch','跑队列':'Run queue','清空队列':'Clear queue','停止':'Stop','上限(0=全部)':'Limit (0=all)',
'模式':'Mode','语义组(0=全部)':'Sem group (0=all)','AI组(0=全部)':'AI group (0=all)','队列':'Queue',
'筛选':'Filter','质量全部':'Quality: all','来源全部':'Source: all','语义全部':'Semantic: all',
'搜索名字/标题/番号':'Search name/title/code','刷新':'Refresh','保存设置':'Save','说明':'Docs',
'站点':'Sites','媒体库':'Library','模型':'Model','端口':'Port','局域网':'LAN','缩略图':'Thumb',
'质量':'Quality','来源':'Source','番号':'Code','语义':'Semantic','标题/元信息':'Title / metadata',
'选择':'Select','操作':'Actions','重跑·网站':'Re-run · sites','重跑·AI':'Re-run · AI',
'重跑·全流程':'Re-run · full','重跑队列':'Queue re-run','打开目录':'Open folder','删除':'Delete',
'删除封面图':'Delete cover','刷新缩略图':'Refresh thumbs','AI 补全':'AI tag fill','AI 看片':'AI watch',
'全选本页':'Select page','取消选择':'Clear','上一页':'Prev','下一页':'Next','加载中':'Loading',
'作者':'Author','标签':'Tags','简介':'Plot','标题':'Title','番号':'Code','全流程':'Full pipeline'};
// 正则规则：处理带前缀 / 带数字 / 带数据的文本
const I18NR=[
 [/^①\s*全流程$/, '① Full pipeline'],
 [/^共\s*(\d+)\s*条$/, 'Total $1'],
 [/^已选\s*(\d+)\s*条$/, 'Selected $1'],
 [/^第\s*(\d+)\s*\/\s*(\d+)\s*页$/, 'Page $1 / $2'],
 [/^质量[\s:：]*有$/, 'OK'], [/^质量[\s:：]*缺$/, 'Partial'],
 [/^质量[\s:：]*有误$/, 'Bad'], [/^质量[\s:：]*无$/, 'None'],
 [/^质量[\s:：]*删除$/, 'Deleted'],
 [/^来源[\s:：]*(code|Uncen|AI)$/, 'Src:$1'],
 [/^作者[:：]\s*(.*)$/, 'Author: $1'],
 [/^标签[:：]\s*(.*)$/, 'Tags: $1'],
 [/^简介[:：]\s*(.*)$/, 'Plot: $1'],
 [/^标题[:：]\s*(.*)$/, 'Title: $1'],
 [/^番号[:：]\s*(.*)$/, 'Code: $1'],
];
let LANG='zh';
function applyLang(){
  if(LANG!=='en')return;
  document.querySelectorAll('button,span,div,td,th,option,label,a,li,p,b,strong,small').forEach(function(el){
    if(el.children.length)return;                 // 只动叶子节点，别拆父结构
    const s=(el.textContent||'').trim();
    if(!s||s.length>46)return;
    let t=I18N[s];
    if(!t){for(let k=0;k<I18NR.length;k++){const re=I18NR[k][0];if(re.test(s)){t=s.replace(re,I18NR[k][1]);break;}}}
    if(t&&t!==s)el.textContent=el.textContent.replace(s,t);
  });
  document.querySelectorAll('input[placeholder]').forEach(function(el){
    const s=(el.placeholder||'').trim(); if(I18N[s])el.placeholder=I18N[s];});
  const lb=document.getElementById('b-lang'); if(lb)lb.textContent='中文';
}
// 统计卡 / 表格 / 日志是异步渲染的：渲染完再套一次
let _i18t=null;
try{new MutationObserver(function(){
  if(LANG!=='en')return; clearTimeout(_i18t);
  _i18t=setTimeout(applyLang,150);
}).observe(document.body,{childList:true,subtree:true});}catch(e){}
if(b)b.textContent='中文';
}
(async()=>{try{const r=await(await fetch('/api/lang')).json();LANG=r.lang||'zh';applyLang();}catch(e){}})();
$('#b-lang').onclick=async()=>{const n=(LANG==='en')?'zh':'en';await fetch('/api/lang?lang='+n,{method:'POST'});location.reload();};
$('#b-sites').onclick=async()=>{await stLoad();$('#st-info').textContent='';$('#modal4').classList.add('on');};
$('#md4-close').onclick=()=>$('#modal4').classList.remove('on');
async function stLoad(){
  const d=await(await fetch('/api/sites')).json();
  $('#st-tbl').innerHTML='<thead><tr><th style="width:44px">启用</th><th>站点</th><th style="width:56px">类型</th><th>入口</th><th style="width:160px">状态</th></tr></thead><tbody>'
   +d.sites.map(s=>'<tr><td><input type="checkbox" class="ston" data-k="'+s.k+'"'+(s.on?' checked':'')+'></td>'
     +'<td>'+esc(s.n)+(s.has?'':' <span class="muted">(待接入)</span>')+'</td><td>'+esc(s.t)+'</td>'
     +'<td class="muted" style="font-size:11px">'+esc(s.u)+'</td>'
     +'<td class="st-'+s.k+'">—</td></tr>').join('')+'</tbody>';}
$('#st-check').onclick=async()=>{
  $('#st-info').textContent='检测中…（逐个真实请求）';
  const d=await(await fetch('/api/sites/check',{method:'POST'})).json();
  for(const k in d.res){const r=d.res[k];const td=document.querySelector('.st-'+k);
    if(!td)continue;
    td.innerHTML=r.ok?('<span style="color:#39d98a">通 '+r.status+' · '+r.ms+'ms</span>')
                     :('<span style="color:#ff6b6b">不通 '+(r.err||r.status)+'</span>');}
  $('#st-info').textContent='检测完成';};
$('#st-save').onclick=async()=>{
  const on=[...document.querySelectorAll('.ston')].filter(c=>c.checked).map(c=>c.dataset.k).join(',');
  if(!on)return alert('至少勾一个站点');
  const d=await(await fetch('/api/sites?on='+encodeURIComponent(on),{method:'POST'})).json();
  $('#st-info').textContent='已保存：'+d.on.join(', ');};
$('#b-qrun').onclick=()=>post('/api/queue/run');
$('#b-qclear').onclick=async()=>{await post('/api/queue/clear');$('#qcount').textContent=0;};
$('#b-stop').onclick=()=>post('/api/stop');
$('#b-refresh').onclick=()=>list();
$('#b-reload').onclick=()=>list();   // 只重拉列表，不动整页
$('#a-thumbs').onclick=async()=>{if(!SEL.size)return alert('先选条目');
  const r=await post('/api/thumbs/refresh?ids='+[...SEL].join(','));
  THUMBV++;await list();console.log('已重建 %s 张缩略图',r.n);};
$('#a-coverdel').onclick=async()=>{if(!SEL.size)return alert('先选条目');
  if(!confirm('将删除选中条目的封面图（poster.jpg 等）+ 缩略图缓存，继续？'))return;
  const r=await post('/api/covers/drop?ids='+[...SEL].join(','));
  THUMBV++;await list();console.log('已删除封面/缩略图 %s 个文件',r.n);};
$('#b-archive').onclick=async()=>{const d=await(await fetch('/api/archive?limit=300')).json();
  $('#md2-body').innerHTML='<table><thead><tr><th>时间</th><th>原因</th><th>id</th><th>名字</th><th>质量</th><th>来源</th></tr></thead><tbody>'
   +d.rows.map(r=>'<tr><td>'+esc(r.archived_at)+'</td><td>'+esc(r.reason)+'</td><td>'+r.id+'</td><td>'+esc(r.name)+'</td><td>'+esc(r.quality)+'</td><td>'+esc(r.source)+'</td></tr>').join('')
   +'</tbody></table>';$('#modal2').classList.add('on');};
$('#b-prev').onclick=()=>{off=Math.max(0,off-lim);list();};
$('#b-next').onclick=()=>{off+=lim;list();};
$('#pjump').onchange=e=>{off=parseInt(e.target.value,10)||0;list();};
['f-quality','f-source','f-kind'].forEach(i=>$('#'+i).onchange=()=>{off=0;list();});
$('#f-q').onkeydown=e=>{if(e.key==='Enter'){off=0;list();}};
const rawList=v=>{if(v==null)return '';let a=v;try{a=JSON.parse(v)}catch(e){}
  if(Array.isArray(a))return a.join(',');return String(v);};
setInterval(()=>{job();cfg();},2000);
function fixSticky(){
  const h=document.querySelector('header'),s=document.getElementById('stickytop');
  if(h&&s)s.style.top=h.offsetHeight+'px';}
window.addEventListener('resize',fixSticky);
fixSticky();
loadState();
job();cfg();list();
</script></body></html>
"""


SERVER = None


def _local_ips():
    """本机局域网 IP（用来显示访问地址）。"""
    ips = []
    try:
        import socket as _s
        for ip in _s.gethostbyname_ex(_s.gethostname())[2]:
            if ip and not ip.startswith('127.') and ip not in ips:
                ips.append(ip)
    except Exception:
        pass
    try:
        import socket as _s
        so = _s.socket(_s.AF_INET, _s.SOCK_DGRAM)
        so.connect(('8.8.8.8', 80))
        ip = so.getsockname()[0]
        so.close()
        if ip and not ip.startswith('127.') and ip not in ips:
            ips.insert(0, ip)
    except Exception:
        pass
    return ips


def _cur_host_port():
    try:
        return SERVER.server_address[0], SERVER.server_address[1]
    except Exception:
        return HOST, PORT


def net_state():
    """当前监听状态 + 可访问地址。"""
    h, p = _cur_host_port()
    lan = str(h or '').startswith('0.0.0.0') or mlib.cfg_get('lan') in ('1', 'true', 'on')
    urls = ['http://127.0.0.1:%d/' % p]
    if lan:
        urls += ['http://%s:%d/' % (ip, p) for ip in _local_ips()]
    return {'host': h, 'port': p, 'lan': lan, 'urls': urls}


def rebind(host, port):
    """热切换监听地址/端口；切换不了就让重启生效。"""
    global SERVER
    try:
        new = ThreadingHTTPServer((host, port), H)
    except Exception as e:
        return False, '切换失败（重启后生效）：%s' % str(e)[:90]
    new.daemon_threads = True
    old = SERVER
    SERVER = new
    threading.Thread(target=new.serve_forever, daemon=True).start()
    if old is not None:
        def _late_close(_o=old):
            # 只停循环；**不要** server_close —— 会把还在 select() 的线程打断
            time.sleep(1.0)
            try:
                _o.shutdown()
            except Exception:
                pass
        threading.Thread(target=_late_close, daemon=True).start()
    return True, '已切换到 %s:%d' % (host, port)


def main():
    print('  卓"悦"刮削器 — 给你和你的小姐姐')
    print('    python : %s' % sys.executable)
    print('    root   : %s' % mlib.scan_root())
    print('    db     : %s' % mlib.DB_PATH)
    print('    ai     : %s' % mlib.ai_model())
    o = _ollama()
    print('    ollama : %s' % ('OK' if o.get('up') else 'DOWN  -> ' + str(o.get('error'))))
    q_load()
    print('    queue  : %d (\u5df2\u6301\u4e45\u5316)' % len(QUEUE))
    try:
        rows = mlib.db().execute('SELECT line FROM joblog ORDER BY id DESC LIMIT 200').fetchall()
        STATE['log'] = [r['line'] for r in reversed(rows)]
    except Exception:
        pass
    global SERVER, HOST, PORT
    HOST = (os.environ.get('MS_HOST')
            or ('0.0.0.0' if mlib.cfg_get('lan') in ('1', 'true', 'on') else '127.0.0.1'))
    PORT = int(os.environ.get('MS_PORT') or mlib.cfg_get('port') or 8800)
    print('    open   : http://127.0.0.1:%d/' % PORT)
    if HOST == '0.0.0.0':
        for _ip in _local_ips():
            print('    lan    : http://%s:%d/' % (_ip, PORT))
    srv = ThreadingHTTPServer((HOST, PORT), H)
    SERVER = srv
    try:
        while True:
            cur = SERVER
            cur.serve_forever()      # 热切换后旧的会返回 -> 继续用新的
            try:
                if SERVER is not cur:
                    cur.server_close()   # 旧循环已退出，这时关才安全
            except Exception:
                pass
    except KeyboardInterrupt:
        pass
    finally:
        try:
            (SERVER or srv).server_close()
        except Exception:
            pass


if __name__ == '__main__':
    main()

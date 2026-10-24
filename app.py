#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MediaScraper -- standalone local scraping service.

Self-contained: a stdlib HTTP server + SQLite (driven by mlib.py in this folder).
No OpenClaw / Control UI dependency.  Start with run.cmd, then open the printed URL.

质量 quality : 有(完整,跳过) / 无(=没有,正常刮) / 有误(留人工复核)
来源 source  : code(javbus/javdb) / Uncen(missav) / AI(模型)
"""
import threading
import io, json, os, re, shutil, subprocess, sys, threading, time, traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import mlib                                   # noqa: E402

import flatten                                  # 整理嵌套目录（平铺 + P_/V_ 命名 + 可回滚）
import repair                                   # 损坏视频体检 + 三级修复（索引重建/插帧/容错重编码）


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
# 两条车道：heavy=吃 NAS/CPU 的重活（体检/深检/扫描/vscan/修复），light=外网/轻量（抓取/AI/语义/填标签）
# STATE 就是 heavy 车道的状态对象（保持向后兼容）；light 单独一份。
LANES = {'heavy': STATE,
         'light': {'running': False, 'name': '', 'done': 0, 'total': 0,
                   'stop': False, 'started': 0}}
_LANE = threading.local()


def _cur():
    """当前线程所属车道的状态对象（不在作业线程里时返回 heavy/STATE）。"""
    n = getattr(_LANE, 'name', None)
    return LANES.get(n, STATE)
QUEUE = []                                    # [(item_id, mode)]  -- persisted in `queue`
_LOGDB = None
_LOGBUF = []

LAST_OP = {}                                   # 最近一次体检/修复的汇总（供完成弹窗）


def _logdb():
    """Shared log connection (check_same_thread=False: 两条线都会写)。"""
    global _LOGDB
    if _LOGDB is None:
        import sqlite3
        _LOGDB = sqlite3.connect(mlib.DB_PATH, timeout=30, check_same_thread=False)
        _LOGDB.execute('PRAGMA journal_mode=WAL')
        _LOGDB.execute('PRAGMA busy_timeout=800')      # 撞锁快速失败，
        # 绝不能在作业线程上死等（作业握着未提交的写事务时，30s 超时会把作业拖死）
    return _LOGDB


_LOGFLUSH_N = [0]


def _flush_log():
    """Write buffered log lines to joblog in one go (every 5 lines).

    错误行特殊处理：库里也只保留一条（删掉旧的 ERR 行再写当前这条）。
    """
    with _LOCK:
        rows = list(_LOGBUF)
        _LOGBUF[:] = []
    err = STATE.get('errline')
    new_err = err if (err and err != STATE.get('err_db')) else None
    if not rows and not new_err:
        return
    try:
        with _DLOK:
            c = _logdb()
            if rows:
                c.executemany('INSERT INTO joblog(ts,line) VALUES(?,?)',
                              [(mlib.now(), x) for x in rows])
            if new_err:
                c.execute('DELETE FROM joblog WHERE line LIKE ?', ('%' + _ERRTAG + '%',))
                c.execute('INSERT INTO joblog(ts,line) VALUES(?,?)', (mlib.now(), new_err))
            _LOGFLUSH_N[0] += 1
            if _LOGFLUSH_N[0] % 200 == 0:      # 别每次都全表扫（每次 flush 都扫会抢写锁）
                c.execute('DELETE FROM joblog WHERE id NOT IN '
                          '(SELECT id FROM joblog ORDER BY id DESC LIMIT 3000)')
            c.commit()
            if new_err:
                STATE['err_db'] = new_err     # 提交成功后才记，失败下次重写
    except Exception:
        # 被作业自己的写事务锁住：把这些行放回缓冲，下次再写（无损、不阻塞）
        if rows:
            with _LOCK:
                _LOGBUF[:0] = rows
                if len(_LOGBUF) > 4000:
                    del _LOGBUF[:len(_LOGBUF) - 4000]


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


_ERRTAG = '  ERR '
_ERRRE = re.compile(r'(^|\s)ERR(\s|:|$)')
_ERRPAT = re.compile(r'ERR|ffmpeg:|ffprobe|Invalid NAL|Error submitting|missing picture|'
                     r'Invalid data|Terminating thread|corrupt', re.I)


def _log_purge():
    """删掉日志里的错误行（内存 + joblog 库），返回删除条数。"""
    with _LOCK:
        lg = STATE['log']
        keep = [x for x in lg if not _ERRPAT.search(x)]
        n = len(lg) - len(keep)
        lg[:] = keep
        STATE.pop('errline', None)
        STATE.pop('err_db', None)
    try:
        with _DLOK:
            c = _logdb()
            rows = c.execute('SELECT id, line FROM joblog').fetchall()
            ids = [r[0] for r in rows if _ERRPAT.search(r[1] or '')]
            for i in range(0, len(ids), 400):
                ch = ids[i:i + 400]
                c.execute('DELETE FROM joblog WHERE id IN (%s)'
                          % ','.join('?' * len(ch)), ch)
            c.commit()
    except Exception:
        pass
    return n


def _log(msg):
    """普通流水一行一条；**错误只保留一条**（新的替换旧的，最多每秒更新一次）。"""
    msg = str(msg)
    if _ERRRE.search(msg):
        body = msg[4:] if msg.startswith('ERR ') else msg
        flat = '%s%s%s' % (time.strftime('%H:%M:%S'), _ERRTAG, ' '.join(body.split()))
        with _LOCK:
            now = time.time()
            if STATE.get('errline') and now - STATE.get('err_t', 0.0) < 1.0:
                STATE['err_drop'] = STATE.get('err_drop', 0) + 1     # 同一秒内的后续错误丢弃
                return
            STATE['err_t'] = now
            STATE['errline'] = flat
            lg = STATE['log']
            for k in range(len(lg) - 1, -1, -1):
                if _ERRTAG in lg[k]:
                    lg[k] = flat                                    # 就地替换上一条错误
                    break
            else:
                lg.append(flat)
                if len(lg) > LOGMAX:
                    del lg[:LOGMAX // 2]
        _flush_log()
        return
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
    _s = _cur()
    with _LOCK:
        _s.update(running=True, name=name, done=0, total=total, stop=False,
                  started=time.time())
    _log('===== %s 开始 (total=%d) =====' % (name, total))


def _end(msg):
    with _LOCK:
        _cur().update(running=False)
    _log('===== 结束: %s =====' % msg)
    _flush_log()


def _stop_req():
    with _LOCK:
        return _cur()['stop']


def _tick(n, total=None):
    _s = _cur()
    with _LOCK:
        _s['done'] = n
        if total:
            _s['total'] = total


# 让 repair.py 里的长任务也能感知「停止」
try:
    repair.STOP_HOOK = _stop_req
except Exception:
    pass


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
def _job_scan(mode, sem=False):
    _begin('scan:' + mode + ('+sem' if sem else ''))
    try:
        r = _scan_pass(mode)
        if not sem:
            _end('scan 完成')
            return
        _log('  扫描完成（新增 %s / 变更 %s / 跳过未变 %s），接着跑语义'
             % (r.get('added'), r.get('changed'), r.get('skipped')))
        ok, tot = _sem_pass(0, False, 0)      # 只处理还没语义的（增量）
        mlib.unload_ai()
        _end('scan+sem 完成：新增 %s 变更 %s 跳过 %s / 语义 %d/%d'
             % (r.get('added'), r.get('changed'), r.get('skipped'), ok, tot))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1600]); _end('scan 失败')


def _job_sem(limit, force, group=None):
    _begin('sem')
    try:
        ok, tot = _sem_pass(limit, force, group); mlib.unload_ai(); _end('sem %d/%d' % (ok, tot))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1600]); _end('sem 失败')


def _job_fetch(mode, limit, ai_group=None):
    _begin('fetch')
    try:
        ok, tot = _fetch_pass(mode, limit, ai_group); mlib.unload_ai(); _end('fetch %d/%d' % (ok, tot))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1600]); _end('fetch 失败')


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
        _log('ERR ' + traceback.format_exc()[:1600]); _end('fill 失败')


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
        _log('ERR ' + traceback.format_exc()[:1600]); _end('vscan 失败')


def _job_pipeline(limit, sgroup=None, agroup=None):
    _begin('pipeline')
    try:
        _r = _scan_pass('incr')
        if isinstance(_r, dict) and _r.get('error'):
            _log('! 扫描失败：%s' % _r['error'])
            _log('! 流水线中止：不跑语义/抓取（避免拿旧库白跑或把状态改乱）')
            _end('pipeline 中止')
            return
        _sem_pass(limit, False, sgroup); _fetch_pass('auto', limit, agroup)
        mlib.unload_ai(); _end('pipeline 完成')
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1600]); _end('pipeline 失败')


def _job_one(iid, mode, force):
    _begin('run #%d' % iid, 1)
    try:
        _one(mlib.db(), iid, mode, force); _tick(1); _end('run 完成')
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1600]); _end('run 失败')


def _job_runset(ids, mode='auto'):
    """只跑指定的一批 id（不经过队列）—— 队列里有什么都不影响这一批。"""
    _begin('runset', len(ids))
    dbc = mlib.db()
    n, ok = 0, 0
    for iid in ids:
        if _stop_req():
            _log('! 已请求停止，剩余 %d 个未跑' % (len(ids) - n))
            break
        n += 1
        ok += 1 if _one(dbc, iid, mode, force=True) else 0
        _tick(n)
    mlib.unload_ai()
    _end('runset %d/%d' % (ok, n))
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


def _flatten_targets(ids=None):
    """要整理的条目：给了 ids 用 ids；否则全部 kind='nested'。"""
    c = mlib.db()
    if ids:
        q = ('SELECT id,name,path,kind,quality FROM items WHERE present=1 AND id IN (%s)'
             % ','.join('?' * len(ids)))
        return [dict(r) for r in c.execute(q, ids)]
    return [dict(r) for r in c.execute(
        "SELECT id,name,path,kind,quality FROM items WHERE present=1 "
        "AND COALESCE(kind,'')='nested' ORDER BY id")]


def _flatten_plan(ids=None, limit=None):
    """产出整理计划（只读，不动任何文件）。"""
    items = _flatten_targets(ids)
    if limit:
        items = items[:limit]
    out, warn_n = [], 0
    for it in items:
        try:
            p = flatten.build_plan(it['path'])
        except Exception as e:
            p = {'ok': False, 'error': type(e).__name__ + ': ' + str(e)[:70], 'items': [], 'subs': [],
                 'count': 0, 'depth': 0, 'max_name_len': 0, 'warn': False, 'reasons': [], 'dups': [], 'clash': []}
        p['id'] = it['id']
        p['name'] = it['name']
        if not os.path.isdir(it['path']):
            p['ok'] = False
            p.setdefault('error', '目录不可访问')
        if p.get('warn'):
            warn_n += 1
        out.append(p)
    return {'items': out, 'total': len(out), 'files': sum(x.get('count', 0) for x in out),
            'warn': warn_n, 'bad': [x['id'] for x in out if not x.get('ok')],
            'warn_depth': flatten.WARN_DEPTH, 'warn_prefix': flatten.WARN_PREFIX_LEN}


def _job_flatten(ids=None):
    _begin('flatten', 1)
    tdir = os.path.join(HERE, 'trash')
    done, undos = [], []
    try:
        items = _flatten_targets(ids)
        _tick(0, len(items))
        n = 0
        for it in items:
            if _stop_req():
                _log('! 已请求停止')
                break
            n += 1
            _tick(n)
            try:
                p = flatten.build_plan(it['path'])
            except Exception as e:
                _log('  id=%s 跳过（%s）' % (it['id'], type(e).__name__))
                continue
            if not p.get('ok'):
                _log('  id=%s 跳过（重名%d / 撞名%d / %s）' % (it['id'], len(p['dups']),
                                                              len(p['clash']), p.get('error') or ''))
                continue
            if p.get('warn'):
                _log('  id=%s 提醒：%s' % (it['id'], ' / '.join(p['reasons'])))
            if not p['items'] and not p['subs']:
                _log('  id=%s 已是平坦结构，跳过' % it['id'])
                continue
            r = flatten.apply_plan(it['path'], p['items'], tdir, p['subs'], tag=n, log=_log)
            undos.append(r['undo_file'])
            done.append((it['id'], r['renamed'], len(r['moved_dirs'])))
            _log('  id=%s 改名 %d，清子目录 %d%s%s' % (
                it['id'], r['renamed'], len(r['moved_dirs']),
                ('，跳过子目录 %d（含未识别文件）' % len(r['skipped_dirs'])) if r['skipped_dirs'] else '',
                ('，错误 %d' % len(r['errors'])) if r['errors'] else ''))
        if undos:
            mf = flatten.merge_undo(undos, os.path.join(tdir, 'flatten_last_undo.json'))
            _log('  回滚文件：%s' % mf)
        _end('flatten %d 个目录 / 共改名 %d' % (len(done), sum(d[1] for d in done)))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1600])
        _end('flatten 失败')


HEALTH_SQL = ("CREATE TABLE IF NOT EXISTS health("
              "item_id INTEGER, file TEXT, cls TEXT, reason TEXT, tier TEXT, "
              "size INTEGER, checked_at TEXT, PRIMARY KEY(item_id,file))")


def _video_files(dirpath):
    out = []
    try:
        for f in sorted(os.listdir(dirpath)):
            if os.path.splitext(f)[1].lower() in repair.VIDEO_EXT:
                out.append(os.path.join(dirpath, f))
    except Exception:
        pass
    return out


def _items_by_ids(ids=None):
    dbc = mlib.db()
    if ids:
        q = ('SELECT id,name,path,kind,quality FROM items WHERE present=1 AND id IN (%s) ORDER BY id'
             % ','.join('?' * len(ids)))
        return [dict(r) for r in dbc.execute(q, ids)]
    return [dict(r) for r in dbc.execute(
        'SELECT id,name,path,kind,quality FROM items WHERE present=1 ORDER BY id')]


def _ensure_media(dbc):
    dbc.execute("CREATE TABLE IF NOT EXISTS media("
                "item_id INTEGER PRIMARY KEY, file TEXT, size INTEGER, dur REAL, "
                "w INTEGER, h INTEGER, fps REAL, vbr INTEGER, ts TEXT, ads TEXT, ads_ts TEXT)")
    for _c in ('ads', 'ads_ts'):
        try:
            dbc.execute('ALTER TABLE media ADD COLUMN %s TEXT' % _c)
        except Exception:
            pass
    dbc.commit()


def _media_probe(path):
    """只读头部：时长/宽高/帧率/视频码率（不读 mdat，快）。"""
    try:
        rc, so, se = repair._run(
            [repair.FFPROBE, '-v', 'error', '-select_streams', 'v:0',
             '-show_entries', 'stream=width,height,avg_frame_rate,bit_rate:format=duration,bit_rate',
             '-of', 'json', path], 90)
        d = json.loads(so or '{}')
    except Exception:
        return None
    st = (d.get('streams') or [{}])[0]
    fm = d.get('format') or {}
    try:
        vbr = int(st.get('bit_rate') or fm.get('bit_rate') or 0)
    except Exception:
        vbr = 0
    try:
        dur = float(fm.get('duration') or 0)
    except Exception:
        dur = 0.0
    fps = 0.0
    try:
        afr = str(st.get('avg_frame_rate') or '')
        if '/' in afr:
            a, b = afr.split('/', 1)
            fps = float(a) / max(1e-6, float(b))
        elif afr:
            fps = float(afr)
    except Exception:
        fps = 0.0
    return {'dur': dur, 'w': int(st.get('width') or 0), 'h': int(st.get('height') or 0),
            'fps': fps, 'vbr': vbr}


def _job_media(ids=None, force=False):
    """媒体信息扫描：主视频的时长/宽高/帧率/码率 -> media 表（heavy 车道）。"""
    _begin('media', 1)
    dbc = mlib.db()
    try:
        _ensure_media(dbc)
        its = _items_by_ids(ids)
        _tick(0, max(1, len(its)))
        n_new = n_skip = n_err = 0
        _vid_ext = ('.mp4', '.m4v', '.mov')
        for i, it in enumerate(its, 1):
            if _stop_req():
                _log('! 已请求停止')
                break
            try:
                vids = [v for v in _video_files(it['path'])
                        if os.path.splitext(v)[1].lower() in _vid_ext]
                if not vids:
                    _tick(i)
                    continue
                f = max(vids, key=lambda x: os.path.getsize(x))
                base = os.path.basename(f)
                sz = os.path.getsize(f)
            except Exception:
                n_err += 1
                _tick(i)
                continue
            _r = dbc.execute('SELECT size FROM media WHERE item_id=?', (it['id'],)).fetchone()
            if _r and int(_r['size'] or 0) == sz and not force:
                n_skip += 1
                _tick(i)
                continue
            m = _media_probe(f)
            if not m or not m.get('vbr'):
                n_err += 1
                _log('  id=%s %s 码率读取失败' % (it['id'], base[:36]))
                _tick(i)
                continue
            dbc.execute('INSERT OR REPLACE INTO media VALUES(?,?,?,?,?,?,?,?,?)',
                        (it['id'], base, sz, m['dur'], m['w'], m['h'], m['fps'],
                         m['vbr'], mlib.now()))
            n_new += 1
            if n_new % 10 == 0:
                dbc.commit()
            _tick(i)
        dbc.commit()
        _log('媒体信息：新增/更新 %d，跳过 %d，读取失败 %d' % (n_new, n_skip, n_err))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1200])
    finally:
        dbc.close()
        _end('media 完成')


def _ensure_health(dbc):
    dbc.execute("CREATE TABLE IF NOT EXISTS health(item_id INTEGER, file TEXT, cls TEXT, reason TEXT,"
                " tier TEXT, size INTEGER, checked_at TEXT, deep INTEGER DEFAULT 0,"
                " PRIMARY KEY(item_id,file))")
    try:
        dbc.execute("ALTER TABLE health ADD COLUMN deep INTEGER DEFAULT 0")   # 旧库补列
    except Exception:
        pass
    dbc.execute("CREATE TABLE IF NOT EXISTS deepscan(item_id INTEGER, file TEXT, size INTEGER,"
                " off INTEGER, done INTEGER, holes TEXT, updated_at TEXT,"
                " PRIMARY KEY(item_id,file))")
    dbc.commit()


def _deep_state(dbc, item_id, base, sz):
    """取深检续扫状态；文件体积变了就重头扫。"""
    r = dbc.execute("SELECT size,off,done,holes FROM deepscan WHERE item_id=? AND file=?",
                    (item_id, base)).fetchone()
    if r and int(r['size'] or 0) == sz:
        try:
            holes = json.loads(r['holes'] or '[]')
        except Exception:
            holes = []
        return {'off': int(r['off'] or 0), 'done': int(r['done'] or 0), 'holes': holes}
    return {'off': 0, 'done': 0, 'holes': []}


def _deep_save(dbc, item_id, base, sz, st):
    try:
        dbc.execute("INSERT OR REPLACE INTO deepscan VALUES(?,?,?,?,?,?,?)",
                    (item_id, base, sz, int(st.get('off') or 0), 1 if st.get('done') else 0,
                     json.dumps(st.get('holes') or []), mlib.now()))
        dbc.commit()
    except Exception:
        pass


def _deep_scan_file(dbc, item_id, f, base, sz, log=None, chunk=None, progress=None):
    """断点续扫：每轮扫 chunk 字节并落库；被停止/中断后下次从 off 继续。"""
    st = _deep_state(dbc, item_id, base, sz)
    if st['done']:
        return st
    chunk = chunk or repair.DEEP_CHUNK
    guard = 0
    _last10 = -1
    if progress:
        try:
            progress(int(st['off'] or 0), sz)
        except Exception:
            pass
    while not st['done'] and guard < 500:
        guard += 1
        r = repair.scan_holes_chunk(f, st['off'], chunk)
        if r.get('error'):
            if log:
                log('  id=%s %s 深检失败：%s' % (item_id, base[:38], r['error']))
            st['error'] = r['error']
            return st
        if r['holes']:
            st['holes'] = repair.merge_holes(list(st['holes']) + list(r['holes']))
        st['off'] = r['next_off']
        st['done'] = bool(r['done'])
        _deep_save(dbc, item_id, base, sz, st)
        if progress:
            try:
                progress(int(st['off'] or 0), sz)
            except Exception:
                pass
        if log and sz >= 200 * 1024 * 1024:
            _pct = int(100.0 * (st['off'] or 0) / max(1, sz))
            if _pct // 10 != _last10 // 10:
                _last10 = _pct
                log('  深检 %s：%.2f / %.2f GB（%d%%）'
                    % (base[:34], (st['off'] or 0) / 1073741824.0,
                       sz / 1073741824.0, _pct))
        if _stop_req():
            if log:
                log('  ! 已请求停止：id=%s %s（深检进度已存，下次从这里续扫）'
                    % (item_id, base[:34]))
            return st
        if r['scanned'] == 0:
            break
    return st


def _deep_plan(dbc, item_id, f, base, log=None, progress=None):
    """深检结论：洞（映射到时间）-> gap/reencode；无洞 -> 抽窗解码 -> reencode/ok。"""
    try:
        sz = os.path.getsize(f)
    except Exception:
        return {'cls': 'error', 'reason': 'stat 失败', 'tier': 'none'}
    st = _deep_scan_file(dbc, item_id, f, base, sz, log=log, progress=progress)
    if st.get('error'):
        return {'cls': 'error', 'reason': st['error'], 'tier': 'none'}
    if not st['done']:
        return {'cls': 'deep-partial', 'tier': 'none', 'partial': True,
                'reason': '深检未完成（已扫 %d 字节，可续扫）' % int(st.get('off') or 0)}
    gaps = repair.holes_to_time(f, st['holes']) if st['holes'] else []
    if gaps:
        small = [g for g in gaps if g['dur'] <= repair.GAP_SEC]
        big = [g for g in gaps if g['dur'] > repair.GAP_SEC]
        if big:
            plan = ['③ 大洞 %d 处（> %.0fs，最长 %.1fs）：容错重编码'
                    % (len(big), repair.GAP_SEC, max(g['dur'] for g in big))]
        else:
            plan = ['② 小洞过渡填充：%d 处（合计 %.1fs）'
                    % (len(small), sum(g['dur'] for g in small))]
        return {'cls': 'hole', 'tier': ('reencode' if big else 'gap'), 'plan': plan, 'gaps': gaps,
                'reason': '深检：洞 %d 处（合计 %.1fs）' % (len(gaps), sum(g['dur'] for g in gaps))}
    w = None
    try:
        w = repair.probe_windows(f, n=4, sec=6)
    except Exception:
        w = None
    if w and w.get('bad'):
        return {'cls': 'bitstream', 'tier': 'reencode', 'windows': w,
                'reason': '无 >=64KB 数据洞，但抽窗解码有错误（%d 个窗口）' % len(w['bad']),
                'plan': ['③ 容错重编码（码流错误）']}
    return {'cls': 'ok', 'tier': 'ok', 'reason': '深检通过（无 >=64KB 数据洞，抽窗无错误）'}


def _prune_health(dbc, item_id, dirpath, log=None):
    # 删掉 health/deepscan 里【文件已不存在】的僵尸行。
    # 目录列不出来（NAS 掉线等）时一律不动，避免误删。
    try:
        have = set(os.listdir(dirpath))
    except Exception:
        return 0
    if not have:
        return 0
    try:
        rows = dbc.execute('SELECT file FROM health WHERE item_id=?', (item_id,)).fetchall()
    except Exception:
        return 0
    dead = [r['file'] for r in rows if r['file'] not in have]
    if not dead:
        return 0
    try:
        for f in dead:
            dbc.execute('DELETE FROM health WHERE item_id=? AND file=?', (item_id, f))
            dbc.execute('DELETE FROM deepscan WHERE item_id=? AND file=?', (item_id, f))
        dbc.commit()
    except Exception:
        return 0
    if log:
        log('  id=%s 清理失效记录 %d 条（文件已不在）：%s'
            % (item_id, len(dead), '、'.join(x[:26] for x in dead[:4])))
    return len(dead)


def _job_adtect(ids=None):
    """广告检测：30s 粗扫 + 10/5/1s 精修 -> media.ads（[[t0,t1],...]）。"""
    _begin('adtect', 1)
    dbc = mlib.db()
    try:
        _ensure_media(dbc)
        its = _items_by_ids(ids)
        _tick(0, max(1, len(its)))
        ok = 0
        for i, it in enumerate(its):
            if _stop_req():
                _log('! 已请求停止'); break
            v = mlib.biggest_video(it['path'] or '')
            if not v:
                _log('[%d/%d] id=%s 没有视频' % (i + 1, len(its), it['id']))
                _tick(i + 1)
                continue
            vp = os.path.join(it['path'], v)
            try:
                dur = mlib.video_duration(vp)
                _log('[%d/%d] id=%s 检测 %s (%.0fs)' % (i + 1, len(its), it['id'], v, dur or 0))
                segs = mlib.detect_ads(vp, dur, log=lambda m: _log(m))
                dbc.execute('INSERT INTO media(item_id,file,ads,ads_ts) VALUES(?,?,?,?) '
                            'ON CONFLICT(item_id) DO UPDATE SET file=excluded.file, '
                            'ads=excluded.ads, ads_ts=excluded.ads_ts',
                            (it['id'], v, json.dumps(segs), mlib.now()))
                dbc.commit()
                _log('    -> %d 段广告 %s' % (len(segs), segs))
                ok += 1
            except Exception as e:
                _log('    ERR %s' % str(e)[:180])
            _tick(i + 1)
        _end('adtect %d/%d' % (ok, len(its)))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1200]); _end('adtect 失败')


def _job_addel(ids=None):
    """删除广告：按 media.ads 切掉广告区间，校验后原子替换原片（回滚点 .orig，临时文件清理）。"""
    _begin('addel', 1)
    dbc = mlib.db()
    try:
        _ensure_media(dbc)
        its = _items_by_ids(ids)
        _tick(0, max(1, len(its)))
        ok = 0
        for i, it in enumerate(its):
            if _stop_req():
                _log('! 已请求停止'); break
            row = dbc.execute('SELECT ads FROM media WHERE item_id=?', (it['id'],)).fetchone()
            segs = []
            if row and row['ads']:
                try:
                    segs = json.loads(row['ads'])
                except Exception:
                    segs = []
            if not segs:
                _log('[%d/%d] id=%s 没有广告区间（先跑广告检测）' % (i + 1, len(its), it['id']))
                _tick(i + 1)
                continue
            v = mlib.biggest_video(it['path'] or '')
            if not v:
                _tick(i + 1)
                continue
            vp = os.path.join(it['path'], v)
            _log('[%d/%d] id=%s 删除 %d 段广告' % (i + 1, len(its), it['id'], len(segs)))
            r = mlib.cut_ads(vp, segs, log=lambda m: _log(m))
            if r.get('ok'):
                _log('    完成: 时长 %.1f -> %.1f 秒，体积 %.1f -> %.1f MB（回滚点 %s）'
                     % (r.get('old_dur') or 0, r.get('new_dur') or 0,
                        (r.get('old_size') or 0) / 1048576.0,
                        (r.get('new_size') or 0) / 1048576.0,
                        os.path.basename(r.get('orig_backup') or '')))
                try:
                    dbc.execute('UPDATE media SET ads=?, dur=?, size=? WHERE item_id=?',
                                ('[]', r.get('new_dur') or 0,
                                 r.get('new_size') or 0, it['id']))
                    dbc.execute('DELETE FROM health WHERE item_id=? AND file=?',
                                (it['id'], v))
                    dbc.execute('DELETE FROM deepscan WHERE item_id=? AND file=?',
                                (it['id'], v))
                    dbc.commit()
                except Exception as _e:
                    _log('    库更新失败: %s' % str(_e)[:90])

                # ---- 深检成片（数据级）：有洞/码流错就回滚，绝不留坏文件 ----
                _cls, _rsn, _good = 'error', '', False
                try:
                    _log('    深检成片（扫 mdat 找洞 + 抽窗解码，NAS 上较慢）...')
                    _dp = _deep_plan(dbc, it['id'], vp, v, log=_log)
                    _cls = _dp.get('cls') or 'error'
                    _rsn = (_dp.get('reason') or '')[:200]
                    _good = (_cls == 'ok')
                    _log('    深检结论: %s — %s' % (_cls, _rsn))
                    dbc.execute('INSERT OR REPLACE INTO health VALUES(?,?,?,?,?,?,?,?)',
                                (it['id'], v, _cls, _rsn, _dp.get('tier') or 'none',
                                 os.path.getsize(vp), mlib.now(), 1))
                    dbc.commit()
                except Exception as _e:
                    _log('    深检异常: %s' % str(_e)[:120])

                if _good:
                    ok += 1
                else:
                    _bk = r.get('orig_backup')
                    if _bk and os.path.exists(_bk):
                        _rr = repair.undo_restore(vp, _bk)
                        if _rr.get('ok'):
                            _log('    ! 深检不合格 -> 已回滚原片（%s）' % _rsn[:90])
                        else:
                            _log('    ! 回滚失败: %s' % str(_rr.get('error') or '')[:110])
                    else:
                        _log('    ! 深检不合格，且无回滚点（保留 0 天），请人工检查')
                    try:
                        dbc.execute("UPDATE items SET quality='有误', last_update=? WHERE id=?",
                                    (mlib.now(), it['id']))
                        dbc.execute('UPDATE media SET dur=?, size=? WHERE item_id=?',
                                    (r.get('old_dur') or 0, r.get('old_size') or 0, it['id']))
                        dbc.commit()
                    except Exception:
                        pass
            else:
                _log('    未替换: %s' % str((r or {}).get('error') or '未知')[:120])
            _tick(i + 1)
        _end('addel %d/%d' % (ok, len(its)))
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1200]); _end('addel 失败')


def _job_health(ids=None, force=False, deep=False):
    """只读体检，结果写入 health 表（下次默认跳过）。

    deep=False：只看头部（快）。
    deep=True ：数据级深检（字节扫洞 + 抽窗解码），慢 —— 能发现"能打开的坏文件"。
    已体检过（体积未变）的默认跳过；deep 会把浅检结果视为"未测"从而重测。
    """
    _begin('deephealth' if deep else 'health', 1)
    dbc = mlib.db()
    try:
        _ensure_health(dbc)
        items = _items_by_ids(ids)
        bad, files, checked, skipped = {}, 0, 0, 0
        fixable, heavy, dead = set(), set(), set()
        lists = {'index': [], 'gap': [], 'reencode': [], 'dead': [], 'novideo': [], 'partial': []}
        _bscan, _bskip = {}, [0]          # 深检进度：当前文件已扫字节 / 已跳过字节
        if deep:
            _log('深检：逐文件扫 mdat 找洞（NAS ~11MB/s，大文件较慢）')
            # 进度按【字节】走：先汇总待扫总量，大文件也能看到进度在动
            _btot = 0
            for _it0 in items:
                try:
                    for _f0 in _video_files(_it0['path']):
                        try:
                            _btot += os.path.getsize(_f0)
                        except Exception:
                            pass
                except Exception:
                    pass
            _tick(0, max(1, _btot))
        else:
            _tick(0, len(items))

        def _bprog(_b, _off):
            """深检单文件进度（多线程回调）：累加已扫字节 -> 推进进度条。"""
            with _LOCK:
                _bscan[_b] = int(_off or 0)
                _cur = _bskip[0] + sum(_bscan.values())
            _tick(_cur)
        for n, it in enumerate(items, 1):
            if _stop_req():
                _log('! 已请求停止')
                break
            vids = _video_files(it['path'])
            if vids:
                _prune_health(dbc, it['id'], it['path'], log=_log)
            if not vids:
                dbc.execute('INSERT OR REPLACE INTO health VALUES(?,?,?,?,?,?,?,?)',
                            (it['id'], '', 'novideo', '目录里没有视频', 'none', 0, mlib.now(), 1 if deep else 0))
                continue
            known = {}
            if not force:
                for r in dbc.execute('SELECT file,cls,tier,size,deep FROM health WHERE item_id=?', (it['id'],)):
                    known[r['file']] = dict(r)
            # ---- 先分拣：可跳过 / 待检测 ----
            to_scan = []
            for _fi, f in enumerate(vids, 1):
                base = os.path.basename(f)
                files += 1
                try:
                    sz = os.path.getsize(f)
                except Exception:
                    sz = 0
                k = known.get(base)
                if deep:
                    # 深检：只有「已深检且合格(ok)」的才算已测并跳过；
                    # 没测过 / 没测完 / 有问题的都自动重扫，不需要 force
                    usable = (k and (k.get('cls') or '') == 'ok'
                              and int(k.get('size') or 0) == sz
                              and int(k.get('deep') or 0) >= 1)
                else:
                    usable = (k and k.get('cls') and int(k.get('size') or 0) == sz
                              and int(k.get('deep') or 0) >= 0)
                if usable and not force:
                    cls = k['cls']
                    tier = k.get('tier') or ''
                    skipped += 1
                    if deep:                       # 跳过的文件也算已完成进度
                        _bskip[0] += sz
                        _tick(_bskip[0] + sum(_bscan.values()))
                    if cls not in ('ok',):
                        _log('  id=%s %s [%s]（跳过已测，沿用记录）' % (it['id'], base[:42], cls))
                    if cls not in ('ok',):
                        bad.setdefault(cls, []).append(it['id'])
                    _tag = 'id=%d %s' % (it['id'], base)
                    if tier == 'index':
                        fixable.add(it['id']); lists['index'].append(_tag)
                    elif tier == 'gap':
                        fixable.add(it['id']); lists['gap'].append(_tag)
                    elif tier == 'reencode':
                        heavy.add(it['id']); lists['reencode'].append(_tag)
                    elif cls == 'deep-partial':
                        lists['partial'].append(_tag + '（深检未完成，可续扫）')
                    elif cls not in ('ok',):
                        dead.add(it['id']); lists['dead'].append(_tag + ' [%s]' % cls)
                else:
                    to_scan.append((_fi, f, base, sz))

            # ---- 深检：多文件并发（NAS 实测 2 流 ~1.7x；每流独立 DB 连接）----
            _pp = {}
            if deep and len(to_scan) > 1:
                _w = repair._deep_workers()
                # 小文件是 CPU/固定开销密集（解码占大头），可以多开；
                # 大文件是 NAS 读带宽受限，2 流封顶
                try:
                    _avg = sum(_t[3] for _t in to_scan) / max(1, len(to_scan))
                except Exception:
                    _avg = 0.0
                if _avg and _avg < 60 * 1024 * 1024:
                    _w = max(_w, min(4, os.cpu_count() or 4))
                if _w > 1:
                    from concurrent.futures import ThreadPoolExecutor
                    _log('  深检并发 %d 流（本条目待检 %d 个文件）' % (_w, len(to_scan)))

                    def _one(_t):
                        _i2, _f2, _b2, _s2 = _t
                        _d2 = mlib.db()
                        try:
                            _r2 = _deep_plan(_d2, it['id'], _f2, _b2, log=_log,
                                             progress=_bprog)
                        except Exception as e:
                            _r2 = {'cls': 'error', 'tier': 'none',
                                   'reason': '深检异常 %s: %s'
                                             % (type(e).__name__, str(e)[:60])}
                        finally:
                            try:
                                _d2.close()
                            except Exception:
                                pass
                        return _b2, _r2

                    with ThreadPoolExecutor(max_workers=_w) as _ex:
                        for _b3, _r3 in _ex.map(_one, to_scan):
                            _pp[_b3] = _r3

            # ---- 逐个文件落库 / 记日志 ----
            for _fi, f, base, sz in to_scan:
                if deep:
                    p = _pp.get(base)
                    if p is None:
                        try:
                            p = _deep_plan(dbc, it['id'], f, base, log=_log,
                                           progress=_bprog)
                        except Exception as e:
                            p = {'cls': 'error', 'tier': 'none',
                                 'reason': '深检异常 %s: %s'
                                           % (type(e).__name__, str(e)[:60])}
                else:
                    try:
                        p = repair.plan_repair(f)
                    except Exception as e:
                        p = {'cls': 'error', 'reason': type(e).__name__, 'tier': 'none'}
                cls, tier = p.get('cls'), p.get('tier') or ''
                _old = known.get(base) or {}
                _same = int(_old.get('size') or 0) == sz
                if (not deep) and int(_old.get('deep') or 0) and _same:
                    skipped += 1
                    _log('  id=%s %s 保留深检记录（浅检不覆盖；文件未变）' % (it['id'], base[:40]))
                else:
                    dbc.execute('INSERT OR REPLACE INTO health VALUES(?,?,?,?,?,?,?,?)',
                                (it['id'], base, cls,
                                 (p.get('reason') or ' / '.join(p.get('plan') or []))[:170],
                                 tier, sz, mlib.now(), 1 if deep else 0))
                checked += 1
                if cls not in ('ok',):
                    _log('  id=%s %s [%s] %s' % (it['id'], base[:42], cls, (p.get('reason') or '')[:58]))
                elif len(vids) > 1:
                    # 多视频条目：逐个文件都报一行，避免看起来「只检了第一个」
                    _log('  id=%s %s [ok] %s %d/%d'
                         % (it['id'], base[:42], '深检' if deep else '体检',
                            _fi, len(vids)))
                if cls not in ('ok',):
                    bad.setdefault(cls, []).append(it['id'])
                _tag = 'id=%d %s' % (it['id'], base)
                if tier == 'index':
                    fixable.add(it['id']); lists['index'].append(_tag)
                elif tier == 'gap':
                    fixable.add(it['id']); lists['gap'].append(_tag)
                elif tier == 'reencode':
                    heavy.add(it['id']); lists['reencode'].append(_tag)
                elif cls == 'deep-partial':
                    lists['partial'].append(_tag + '（深检未完成，可续扫）')
                elif cls not in ('ok',):
                    dead.add(it['id']); lists['dead'].append(_tag + ' [%s]' % cls)
            if n % 20 == 0:
                dbc.commit()
            if not deep:
                _tick(n)                     # 深检按字节推进，别用条目数覆盖
        dbc.commit()
        _log('%s完成：%d 条 / %d 个视频（新测 %d、跳过已测 %d）；可修 %d / 需重编码 %d / 不可修 %d'
             % ('深检' if deep else '体检', len(items), files, checked, skipped,
                len(fixable), len(heavy), len(dead)))
        for k2, v2 in sorted(bad.items(), key=lambda x: -len(x[1])):
            _log('  %-11s %d 个文件' % (k2, len(v2)))
        for _key, _lab in (('index', '可修·索引重建'), ('gap', '可修·小洞插帧'),
                           ('reencode', '需重编码'), ('dead', '不可修'),
                           ('partial', '深检未完成（可续扫）'), ('novideo', '无视频')):
            _v = lists.get(_key) or []
            if not _v:
                continue
            _log('  【%s】%d 个：' % (_lab, len(_v)))
            for _x in _v[:40]:
                _log('      ' + _x)
            if len(_v) > 40:
                _log('      …还有 %d 个' % (len(_v) - 40))
        LAST_OP.clear()
        LAST_OP.update({'kind': 'health', 'deep': bool(deep), 'files': files,
                        'bad': sum(len(v) for v in bad.values()), 'checked': checked,
                        'skipped': skipped, 'fixable': len(fixable), 'heavy': len(heavy),
                        'dead': len(dead), 'ids': sorted(fixable),
                        'lists': {k: v[:200] for k, v in lists.items()}})
        _end(('deephealth' if deep else 'health') + ' 完成')
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1600])
        _end('health 失败')



def _health_of(item_id):
    """读 health 表里某个条目的记录（file 名 -> dict）。"""
    dbc = mlib.db()
    try:
        return {r['file']: dict(r) for r in dbc.execute(
            'SELECT file,cls,reason,tier,size,checked_at FROM health WHERE item_id=?', (item_id,))}
    except Exception:
        return {}



def _repair_plan(ids=None, cap=80, refresh=False):
    """给 UI 的预览：**优先用 health 表里的体检记录**；未测过的才现场探测（refresh=1 全部重测）。"""
    out = []
    for it in _items_by_ids(ids)[:cap]:
        recs = []
        known = {} if refresh else _health_of(it['id'])
        for f in _video_files(it['path']):
            base = os.path.basename(f)
            k = known.get(base)
            if k and k.get('cls'):
                cls, tier = k['cls'], k.get('tier') or 'none'
                reason = k.get('reason') or ''
                plan = (['① 索引重建（追加·原地·可回滚）'] if tier == 'index'
                        else (['③ 容错重编码（独立目录→自动替换）'] if tier == 'reencode' else []))
            else:
                try:
                    p = repair.plan_repair(f)
                except Exception as e:
                    p = {'cls': 'error', 'reason': type(e).__name__, 'tier': 'none'}
                cls, tier = p.get('cls'), p.get('tier') or 'none'
                reason = p.get('reason') or ''
                plan = p.get('plan') or []
            if cls != 'ok':
                recs.append({'file': base, 'cls': cls, 'reason': reason[:110], 'tier': tier,
                             'plan': plan, 'size': int((k or {}).get('size') or 0)})
        if recs:
            out.append({'id': it['id'], 'name': it['name'], 'files': recs})
    return {'items': out, 'n_items': len(out), 'n_files': sum(len(x['files']) for x in out),
            'gap_sec': repair.GAP_SEC}



def _sweep_temp(log=None):
    """清掉上次异常退出留下的中间产物（只在 OUT_DIR，绝不碰媒体库）。"""
    n = 0
    try:
        for f in os.listdir(repair.OUT_DIR):
            if (f.startswith('_gap') and f.endswith('.png')) or \
                    f.endswith(('.gapfix.mp4', '.remux.mp4', '.splice.mp4')):
                try:
                    os.remove(os.path.join(repair.OUT_DIR, f))
                    n += 1
                except Exception:
                    pass
    except Exception:
        pass
    if n and log:
        log('  清理上次遗留的中间产物 %d 个' % n)
    return n


def _mark_repaired(dbc, item_id, base, path, kind, backup, log=None):
    """修复后的收尾：抽窗校验 -> 写回体检记录 -> 清理回滚点。

    - 产物是全新写入的完整字节流（物理上不可能有数据洞），所以不做全文件字节扫描
      （那只是空转），改做**抽窗解码校验**：通过即记 deep=1（徽标 ✓✓）。
    - 校验不过则只记 ok（仍 ✓），并在 reason 里点名"建议 force 深检"。
    - .orig 只是执行过程中的暂存：完成即删，不留回滚点（media 库保持干净）。
    """
    try:
        sz = os.path.getsize(path)
    except Exception:
        sz = 0
    deep, note = 0, ''
    try:
        w = repair.probe_windows(path, n=4, sec=6)
        if w and not (w.get('bad') or []):
            deep, note = 1, '；抽窗解码校验通过'
        elif w:
            note = '；⚠ 抽窗仍有 %d 个窗口报错，建议 force 深检' % len(w.get('bad') or [])
    except Exception as e:
        note = '；校验异常 %s' % type(e).__name__
    try:
        dbc.execute("INSERT OR REPLACE INTO health VALUES(?,?,?,?,?,?,?,?)",
                    (item_id, base, 'ok',
                     ('已修复（%s）%s' % (kind, note))[:170], 'ok', sz, mlib.now(), deep))
        dbc.execute("DELETE FROM deepscan WHERE item_id=? AND file=?", (item_id, base))
        dbc.commit()
    except Exception:
        pass
    # .orig 只是执行过程中的暂存：完成后一律删除，不留回滚点。
    if backup and os.path.exists(backup):
        try:
            os.remove(backup)
            if log:
                log('  已清理暂存 %s' % os.path.basename(backup))
        except Exception as e:
            if log:
                log('  !! 暂存删除失败：%s' % str(e)[:60])
    # 媒体目录保持干净：清掉本文件相关的任何暂存残留
    _k = 0
    try:
        d = os.path.dirname(path)
        b = os.path.basename(path)
        st = os.path.splitext(b)[0]
        for n in os.listdir(d):
            if (n.startswith(b + '.orig') or n == st + '.splice.mp4'
                    or n == st + '.gapfix.mp4' or n == st + '.remux.mp4'):
                try:
                    os.remove(os.path.join(d, n))
                    _k += 1
                except Exception:
                    pass
    except Exception:
        pass
    if _k and log:
        log('  已清理媒体目录残留 %d 个' % _k)
    if log:
        log('  id=%s %s 收尾完成：%s，体积 %d'
            % (item_id, base[:34],
               '深检合格 ✓✓' if deep else '仅体检 ✓（建议 force 深检）', sz))


def _job_repair(ids=None, mode='index', go=False):
    """按体检结果修复。mode=index 只做索引重建（快/原地/可回滚）；mode=all 也做容错重编码。"""
    _begin('repair', 1)
    _sweep_temp(log=_log)
    dbc = mlib.db()          # 本函数要用：写回体检记录 / 读深检记录的洞区间
    n_idx, n_remux, n_gap, n_re, n_skip, n_fail, backs = 0, 0, 0, 0, 0, 0, []
    lists = {'index': [], 'remux': [], 'gap': [], 'recode': [], 'fail': [], 'skip': []}
    try:
        items = _items_by_ids(ids)
        _tick(0, len(items))
        for n, it in enumerate(items, 1):
            if _stop_req():
                _log('! 已请求停止')
                break
            _hmap = _health_of(it['id'])          # 体检/深检的记录：优先采信
            for f in _video_files(it['path']):
                base = os.path.basename(f)[:40]
                _h = _hmap.get(os.path.basename(f)) or {}
                if _h.get('cls') and _h.get('tier'):
                    p = {'cls': _h['cls'], 'tier': _h['tier'], 'reason': _h.get('reason') or '',
                         'plan': [], 'from': 'record'}
                else:
                    try:
                        p = repair.plan_repair(f)
                    except Exception as e:
                        p = {'cls': 'error', 'reason': type(e).__name__, 'tier': 'none'}
                t = p.get('tier')
                if t == 'index' and go and mode in ('index', 'safe', 'all'):
                    r = repair.apply_index(f, log=_log)
                    if r.get('ok'):
                        n_idx += 1
                        lists['index'].append('id=%s %s' % (it['id'], base))
                        _mark_repaired(dbc, it['id'], os.path.basename(f), f, '索引重建',
                                       r.get('orig_backup'), log=_log)
                        _log('  id=%s %s 索引重建 OK（+%d 字节；回滚=truncate 到 %d）'
                             % (it['id'], base, r['new_size'] - r['orig_size'], r['orig_size']))
                    else:
                        n_fail += 1
                        _log('  id=%s %s 索引重建失败：%s' % (it['id'], base, r.get('error')))
                elif t == 'remux' and go and mode in ('safe', 'all'):
                    r = repair.remux_copy(f, log=_log)
                    if r.get('ok'):
                        n_remux += 1
                        lists['remux'].append('id=%s %s' % (it['id'], base))
                        _mark_repaired(dbc, it['id'], os.path.basename(f), f, '无损重封装',
                                       r.get('orig_backup'), log=_log)
                        _bk = r.get('orig_backup') or ''
                        if _bk and os.path.exists(_bk):
                            backs.append(_bk)
                        _log('  id=%s %s 无损重封装 OK（%.0fs；回滚点 %s）'
                             % (it['id'], base, r.get('secs', 0),
                                os.path.basename(r.get('orig_backup') or '')))
                    else:
                        n_fail += 1
                        lists['fail'].append('id=%s %s -> %s'
                                             % (it['id'], base, (r.get('error') or '')[:60]))
                        _log('  id=%s %s 重封装未采用：%s' % (it['id'], base, (r.get('error') or '')[:90]))
                elif t == 'gap' and go and mode in ('safe', 'all'):
                    _last2 = [0.0]

                    def _pk2(frac, lab, _id=it['id'], _L=_last2):
                        if frac - _L[0] >= 0.1:
                            _L[0] = frac
                            _log('    id=%s %s' % (_id, lab))
                    # 用深检已记录的洞直接定位：避免为“找洞”把整个文件再扫一遍
                    _gaps = None
                    try:
                        _row = dbc.execute("SELECT holes FROM deepscan WHERE item_id=? AND file=?",
                                           (it['id'], os.path.basename(f))).fetchone()
                        if _row is not None:
                            _hj = json.loads((_row['holes'] or '') or '[]')
                            _gaps = repair.holes_to_time(f, _hj) if _hj else []
                            if _gaps:
                                _log('  id=%s %s 用深检记录定位 %d 处洞（不重新扫描）'
                                     % (it['id'], base[:34], len(_gaps)))
                            else:
                                _log('  id=%s %s 深检记录里无洞（不扫描）' % (it['id'], base[:34]))
                    except Exception:
                        _gaps = None
                    if _gaps is None:
                        _log('  id=%s %s 无深检记录 -> 现场扫描定位洞（较慢）'
                             % (it['id'], base[:34]))
                    if _gaps and len(_gaps) > 12:
                        # 洞太多：逐洞插帧不但慢（minterpolate≈0.034x 实时），
                        # 而且会把 67+ 个洞塞进一条 filter_complex，命令行超长直接崩。
                        _log('  id=%s %s 小洞 %d 处（合计 %.1fs）—— 洞太多，改走分段修复'
                             % (it['id'], base[:34], len(_gaps), sum(g['dur'] for g in _gaps)))
                        try:
                            r = repair.splice_repair(f, _gaps, log=_log, tick=_pk2)
                        except Exception as e:
                            r = {'ok': False, 'error': 'splice 异常: %s: %s'
                                 % (type(e).__name__, str(e)[:60])}
                        if not r.get('ok'):
                            if r.get('locked'):
                                _log('  id=%s %s 分段成品已生成，但原文件被占用；'
                                     '关闭播放器/预览后重试（成品：%s）'
                                     % (it['id'], base[:34], r.get('stage')))
                                lists['fail'].append('id=%s %s -> 文件被占用（成品已保留）'
                                                     % (it['id'], base))
                                n_fail += 1
                            else:
                                _log('  id=%s %s 分段修复未采用（%s）-> 改走整段容错重编码'
                                     % (it['id'], base[:34], (r.get('error') or '')[:70]))
                                r = None
                    else:
                        r = repair.interp_gap(f, gaps=_gaps, log=_log, tick=_pk2)
                        if not r.get('ok'):
                            _log('  id=%s %s 小洞过渡未采用（%s）-> 改走整段容错重编码'
                                 % (it['id'], base[:34], (r.get('error') or '')[:70]))
                            r = None
                    if r is None:
                        _log('  id=%s %s 整段容错重编码…' % (it['id'], base[:34]))
                        r = repair.tolerant_reencode(f, log=_log, tick=_pk2)
                    if r.get('ok'):
                        n_gap += 1
                        lists['gap'].append('id=%s %s' % (it['id'], base))
                        _mark_repaired(dbc, it['id'], os.path.basename(f), f, '小洞过渡',
                                       r.get('orig_backup'), log=_log)
                        _bk = r.get('orig_backup') or ''
                        if _bk and os.path.exists(_bk):
                            backs.append(_bk)
                        _log('  id=%s %s 小洞过渡填充 OK（%.0fs；回滚点 %s）'
                             % (it['id'], base, r.get('secs', 0), os.path.basename(r.get('orig_backup') or '')))
                    else:
                        n_fail += 1
                        lists['fail'].append('id=%s %s -> %s'
                                             % (it['id'], base, (r.get('error') or '')[:60]))
                        _log('  id=%s %s 过渡填充失败：%s' % (it['id'], base, (r.get('error') or '')[:80]))
                elif t == 'reencode' and go and mode == 'all':
                    last = [0.0]

                    def _pk(frac, lab, _id=it['id'], _last=last):
                        if frac - _last[0] >= 0.1:
                            _last[0] = frac
                            _log('    id=%s %s' % (_id, lab))
                    # 先试「分段修复」：未损坏段 -c copy（零损失），只重编码损坏窗口
                    _wins = None
                    try:
                        _row = dbc.execute("SELECT holes FROM deepscan WHERE item_id=? AND file=?",
                                           (it['id'], os.path.basename(f))).fetchone()
                        _hj = json.loads((_row['holes'] if _row else '') or '[]')
                        if _hj:
                            _wins = repair.holes_to_time(f, _hj)
                    except Exception:
                        _wins = None
                    r = None
                    if _wins:
                        _log('  id=%s %s 分段修复：%d 处损坏窗口（其余原样搬运，零损失）'
                             % (it['id'], base[:34], len(_wins)))
                        try:
                            r = repair.splice_repair(f, _wins, log=_log, tick=_pk)
                        except Exception as e:
                            r = {'ok': False, 'error': 'splice 异常: %s: %s'
                                 % (type(e).__name__, str(e)[:60])}
                        if not r.get('ok'):
                            if r.get('locked'):
                                _log('  id=%s %s 分段成品已生成，但原文件被占用；'
                                     '关闭播放器/预览后重试（成品：%s）'
                                     % (it['id'], base[:34], r.get('stage')))
                                lists['fail'].append('id=%s %s -> 文件被占用（成品已保留）'
                                                     % (it['id'], base))
                                n_fail += 1
                            else:
                                _log('  id=%s %s 分段修复未采用（%s）-> 回退整段重编码'
                                     % (it['id'], base[:34], (r.get('error') or '')[:70]))
                                r = None
                    if r is None:
                        r = repair.tolerant_reencode(f, log=_log, tick=_pk)
                    if r.get('ok'):
                        n_re += 1
                        lists['recode'].append('id=%s %s' % (it['id'], base))
                        _mark_repaired(dbc, it['id'], os.path.basename(f), f, '容错重编码',
                                       r.get('orig_backup'), log=_log)
                        _bk = r.get('orig_backup') or ''
                        if _bk and os.path.exists(_bk):
                            backs.append(_bk)
                        _log('  id=%s %s 重编码完成（%.0fs；回滚点 %s）'
                             % (it['id'], base, r.get('secs', 0), r.get('orig_backup')))
                    else:
                        n_fail += 1
                        lists['fail'].append('id=%s %s -> %s'
                                             % (it['id'], base, (r.get('error') or '')[:60]))
                        _log('  id=%s %s 重编码失败：%s' % (it['id'], base, r.get('error')))
                else:
                    n_skip += 1
                    lists['skip'].append('id=%s %s [%s]' % (it['id'], base, p.get('cls')))
                    why = (p.get('reason') or '')
                    plan = ' / '.join(p.get('plan') or [])
                    hint = ''
                    if t == 'reencode' and mode != 'all':
                        hint = '（大洞/码流损坏：需重编码，当前模式不含 —— 请点「全部（含重编码）」）'
                    elif t in ('gap', 'remux') and mode == 'index':
                        hint = '（可修：需「修索引+小洞」或「全部」模式）'
                    elif t in ('none', None, '') or p.get('cls') in ('dead', 'moov-gone'):
                        hint = '（不可修：需重下或同源参考）'
                    _log('  id=%s %s [%s] %s%s' % (it['id'], base, p.get('cls'), (plan or why)[:60], hint))
            _tick(n)
        _log('修复结束：索引 %d / 重封装 %d / 小洞过渡 %d / 重编码 %d / 跳过 %d / 失败 %d'
             % (n_idx, n_remux, n_gap, n_re, n_skip, n_fail))
        for _key, _lab in (('index', '已重建索引'), ('remux', '已重封装'),
                           ('gap', '已填小洞'), ('recode', '已重编码'),
                           ('fail', '失败'), ('skip', '跳过/未处理')):
            _v = lists.get(_key) or []
            if not _v:
                continue
            _log('  【%s】%d 个：' % (_lab, len(_v)))
            for _x in _v[:40]:
                _log('      ' + _x)
        LAST_OP.clear()
        LAST_OP.update({'kind': 'repair', 'index': n_idx, 'remux': n_remux, 'gap': n_gap, 'recode': n_re,
                        'skip': n_skip, 'fail': n_fail, 'ok': n_idx + n_remux + n_gap + n_re,
                        'backups': [b for b in backs if b],
                        'lists': {k: v[:200] for k, v in lists.items()}})
        _end('repair 完成')
    except Exception:
        _log('ERR ' + traceback.format_exc()[:1600])
        _end('repair 失败')


def _purge_backups(paths):
    """删除本次修复产生的回滚备份（只删它自己刚建的那几个 .orig）。"""
    n, err = 0, []
    for p in paths or []:
        try:
            if os.path.isfile(p):
                os.remove(p)
                n += 1
                _log('  已删除回滚备份 %s' % os.path.basename(p)[:60])
        except Exception as e:
            err.append(str(e)[:60])
    return {'ok': True, 'removed': n, 'errors': err}


def start_job(fn, *a, lane='light', exclusive=False):
    """启动作业。lane: 'heavy'（吃 NAS/CPU，互斥）| 'light'（外网，可与 heavy 并行）。
    exclusive=True（修复）：把 light 车道也挡住，避免读到替换中的半成品。"""
    _s = LANES[lane]
    with _LOCK:
        if lane == 'heavy':
            if STATE['running']:
                return False
            if exclusive and LANES['light']['running']:
                return False          # 修复：等 light 车道收尾（不能读替换中的半成品）
        else:
            if LANES['light']['running']:
                return False
            if STATE['running'] and STATE.get('exclusive'):
                return False
        _s['exclusive'] = bool(exclusive)
        _s['running'] = True
        _s['stop'] = False

    def runner():
        _LANE.name = lane
        try:
            fn(*a)
        except Exception:
            _log('ERR ' + traceback.format_exc()[:1600])
        finally:
            with _LOCK:
                _s['running'] = False
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
           'by_quality': {}, 'by_source': {}, 'sem': {}, 'ollama': _ollama(), 'os': ('win' if mlib.IS_WIN else 'linux'),
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
    _sort = (params.get('sort') or 'id').strip()
    _desc = (params.get('order') or '').lower() in ('desc', 'descending', 'down', '-1')
    if _sort == 'bitrate':
        order_by = 'COALESCE(m.vbr,0) %s, items.id %s' % (
            'DESC' if _desc else 'ASC', 'DESC' if _desc else 'ASC')
    else:
        if _sort not in ('id', 'first_seen', 'last_update'):
            _sort = 'id'                          # 白名单，防注入
        order_by = '%s %s, id %s' % (_sort, 'DESC' if _desc else 'ASC',
                                     'DESC' if _desc else 'ASC')
    total = dbc.execute('SELECT COUNT(*) c FROM items WHERE ' + ws, args).fetchone()['c']
    limit = min(int(params.get('limit') or 50), 300)
    offset = int(params.get('offset') or 0)
    rows = dbc.execute('SELECT items.id AS id,name,code,sem_kind,sem_code,sem_manual,status,site,path,'
                       'm.vbr AS vbr, m.w AS mw, m.h AS mh, m.fps AS mfps, m.dur AS mdur, m.ads AS ads, m.ads_ts AS ads_ts,'
                       'quality,source,title,plot,tags,actors,year,studio,cover,last_update,first_seen, '
                       "(SELECT CASE WHEN COUNT(*)=0 THEN 0 "
                       "WHEN SUM(CASE WHEN cls<>'ok' THEN 1 ELSE 0 END)>0 THEN 2 "
                       "WHEN MIN(deep)=1 THEN 3 ELSE 1 END "
                       "FROM health h WHERE h.item_id=items.id) AS hstate "
                       'FROM items LEFT JOIN media m ON m.item_id=items.id WHERE ' + ws +
                       ' ORDER BY ' + order_by + ' LIMIT ? OFFSET ?',
                       args + [limit, offset]).fetchall()
    out = [dict(r) for r in rows]
    for o in out:
        v = int(o.get('vbr') or 0)
        o['kbps'] = round(v / 1000.0, 1) if v else 0
        try:
            w = int(o.get('mw') or 0)
            h = int(o.get('mh') or 0)
            fps = float(o.get('mfps') or 0)
            o['bpp'] = round(v / (w * h * fps), 4) if (v and w and h and fps) else 0
        except Exception:
            o['bpp'] = 0
    # 记录过期检查：文件体积变了（例如刚修过）就不能再算「已检」-> hstate=4
    for d in out:
        if not d.get('hstate'):
            continue
        try:
            recs = dbc.execute('SELECT file,size,cls,deep FROM health WHERE item_id=?',
                               (d['id'],)).fetchall()
        except Exception:
            recs = []
        if not recs:
            d['hstate'] = 0
            continue
        p = d.get('path') or ''
        stale, dead = 0, []
        for r in recs:
            try:
                cur = os.path.getsize(os.path.join(p, r['file']))
            except Exception:
                cur = -1
            if cur < 0:
                dead.append(r['file'])              # 文件已不在 -> 僵尸行
            elif int(r['size'] or 0) != cur:
                stale += 1
        if dead:                                    # 删掉僵尸行，否则条目永远卡在「需重测」
            try:
                for f in dead:
                    dbc.execute('DELETE FROM health WHERE item_id=? AND file=?', (d['id'], f))
                    dbc.execute('DELETE FROM deepscan WHERE item_id=? AND file=?', (d['id'], f))
                dbc.commit()
            except Exception:
                pass
            recs = [r for r in recs if r['file'] not in dead]
        if not recs:
            d['hstate'] = 0
            continue
        bad = sum(1 for r in recs if (r['cls'] or '') not in ('ok',))
        deep_ok = sum(1 for r in recs if int(r['deep'] or 0) >= 1)
        d['hstate'] = 4 if stale else (2 if bad else (3 if deep_ok == len(recs) else 1))
    return {'total': total, 'limit': limit, 'offset': offset, 'rows': out}


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
    if not mlib.IS_WIN:
        return False            # 容器/Linux 无本机看图器
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
    # 2) 没有图 -> 从视频里随机取一帧（多视频随机挑；长视频在 [2min, 片尾] 内随机起点）
    if not src:
        fr = mlib.cover_frame(p)          # 随机视频 + 随机取帧（见 mlib.cover_frame）
        if fr:
            try:
                io.open(dst, 'wb').write(fr)
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
    if not mlib.IS_WIN:
        return False            # 容器/Linux 无资源管理器
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
    if not mlib.IS_WIN:
        # 容器/Linux：没有回收站，直接永久删除（界面上已有红色警告）
        try:
            shutil.rmtree(p)
        except Exception as e:
            return False, 'rm err %s' % str(e)[:120]
        if os.path.isdir(p):
            return False, '删除后目录仍存在'
        _archive_row(dbc, iid, 'deleted(permanent/linux)')
        return True, '已永久删除（容器内无回收站）'
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
    server_version = 'ZhuoYue-Scraper/1.16.1'

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
        p, q = u.path, {k: v[0] for k, v in parse_qs(u.query, keep_blank_values=True).items()}
        try:
            if p in ('/', '/index.html'):
                return self._send(200, PAGE, 'text/html; charset=utf-8')
            if p.startswith('/doc/'):
                return self._send(200, doc_page(p.rsplit('/', 1)[1]), 'text/html; charset=utf-8')
            if p == '/api/sites':
                return self._json({'sites': sites_info(), 'on': mlib.sites_enabled()})
            if p == '/api/net':
                _n = dict(net_state())
                _n['bak_days'] = repair.BAK_DAYS
                _n['bak_dir'] = repair.BAK_DIR
                return self._json(_n)
            if p == '/api/lang':
                return self._json({'lang': mlib.ui_lang(), 'out_lang': mlib.out_lang()})
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
            if p == '/api/log/purge':
                return self._json({'removed': _log_purge()})
            if p == '/api/job':
                with _LOCK:
                    _act = STATE if STATE['running'] else LANES['light']
                    _snap = dict(_act)
                    _snap['log'] = STATE['log']
                    _snap['queue'] = len(QUEUE)
                    _snap['lanes'] = {'heavy': bool(STATE['running']),
                                      'light': bool(LANES['light']['running'])}
                    return self._json(_snap)
            if p == '/api/ollama':
                return self._json(_ollama())
            return self._json({'error': 'bad path'}, 404)
        except Exception as e:
            return self._json({'error': str(e)[:300]}, 500)

    def do_POST(self):
        # 记录请求来源：浏览器点按钮会带 Referer 和浏览器 UA；脚本（urllib/curl）不带
        try:
            _ua = (self.headers.get('User-Agent') or '')[:52]
            _ref = (self.headers.get('Referer') or '')[:52]
            _p = (self.path or '')
            if '/api/health' in _p or '/api/repair' in _p:
                _log('HTTP POST %s | UA=%s | Referer=%s' % (_p[:86], _ua, _ref))
        except Exception:
            pass
        u = urlparse(self.path)
        p, q = u.path, {k: v[0] for k, v in parse_qs(u.query, keep_blank_values=True).items()}
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
                ok = start_job(_job_scan, q.get('mode') or 'incr',
                               (q.get('sem') or '') in ('1', 'true', 'yes'),
                               lane='heavy')
            elif p == '/api/sem':
                ok = start_job(_job_sem, lim, force, int(q.get('group') or 0))
            elif p == '/api/fetch':
                ok = start_job(_job_fetch, mode, lim, int(q.get('ai_group') or 0))
            elif p == '/api/fill':
                _rid = q.get('ids') or ''
                if '?' in _rid:          # 兼容旧前端误拼：?ids=2005?deep=1
                    _rid, _tail = _rid.split('?', 1)
                    for _kv in _tail.split('&'):
                        if '=' in _kv:
                            _k2, _v2 = _kv.split('=', 1)
                            q.setdefault(_k2, _v2)
                ids = [int(x) for x in _rid.split(',') if x.strip().isdigit()]
                ok = start_job(_job_fill, lim, int(q.get('ai_group') or 0), ids)
            elif p == '/api/vscan':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                ok = start_job(_job_vscan, lim, ids, lane='heavy')
            elif p == '/api/pipeline':
                ok = start_job(_job_pipeline, lim, int(q.get('group') or 0),
                               int(q.get('ai_group') or 0))
            elif p == '/api/media':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                ok = start_job(_job_media, ids, (q.get('force') or '') in ('1', 'true', 'yes'),
                               lane='heavy')
            elif p == '/api/adtect':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                ok = start_job(_job_adtect, ids, lane='heavy')
            elif p == '/api/addel':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                ok = start_job(_job_addel, ids, lane='heavy')
            elif p == '/api/health':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                ok = start_job(_job_health, ids, (q.get('force') or '') in ('1', 'true', 'yes'),
                                (q.get('deep') or '') in ('1', 'true', 'yes'),
                                lane='heavy')
            elif p == '/api/repair/plan':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                return self._json(_repair_plan(ids, refresh=(q.get('refresh') or '') in ('1', 'true', 'yes')))
            elif p == '/api/repair/last':
                return self._json(dict(LAST_OP))
            elif p == '/api/repair/purge':
                return self._json(_purge_backups(LAST_OP.get('backups') or []))
            elif p == '/api/repair':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                if not ids:
                    return self._json({'ok': False, 'error': 'ids 为空'}, 400)
                ok = start_job(_job_repair, ids, q.get('mode') or 'index',
                               (q.get('go') or '') in ('1', 'true', 'yes'),
                               lane='heavy')
            elif p == '/api/runset':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                if not ids:
                    return self._json({'ok': False, 'error': 'ids 为空'}, 400)
                ok = start_job(_job_runset, ids, mode)
            elif p == '/api/queue/run':
                ok = start_job(_job_queue, lane='heavy')
            elif p == '/api/queue/clear':
                q_clear()
                return self._json({'ok': True, 'queue': 0})
            elif p.startswith('/api/queue/'):
                n = q_add(int(p.rsplit('/', 1)[1]), mode)
                return self._json({'ok': True, 'queue': n})
            elif p == '/api/flatten/preview':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                return self._json(_flatten_plan(ids, int(q.get('limit') or 0) or None))
            elif p == '/api/flatten/run':
                ids = [int(x) for x in (q.get('ids') or '').split(',') if x.strip().isdigit()]
                ok = start_job(_job_flatten, ids, lane='heavy')
            elif p == '/api/flatten/undo':
                f = q.get('file') or ''
                if not f and q.get('last'):
                    cand = []
                    for root2, _d, files2 in os.walk(os.path.join(HERE, 'trash')):
                        for fn2 in files2:
                            if fn2.endswith('.json') and 'undo' in fn2:
                                cand.append(os.path.join(root2, fn2))
                    f = max(cand, key=os.path.getmtime) if cand else ''
                if not f or not os.path.isfile(f):
                    return self._json({'ok': False, 'error': '回滚文件不存在'}, 400)
                r = flatten.do_undo(f, _log)
                return self._json(dict(r, ok=True, file=f))
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
                kv2 = {k: v[0] for k, v in parse_qs(self._body() or '', keep_blank_values=True).items()}
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
                _bd = q.get('bak_days')
                if _bd is not None and str(_bd) != '':
                    try:
                        _bd = max(0, int(_bd))
                    except Exception:
                        _bd = 7
                    mlib.cfg_set('bak_keep_days', str(_bd))
                    repair.set_backup_policy(days=_bd)
                mlib.cfg_set('port', str(_p))
                mlib.cfg_set('lan', '1' if _lan else '0')
                _ok, _msg = rebind('0.0.0.0' if _lan else '127.0.0.1', _p)
                _st = net_state()
                return self._json({'ok': _ok, 'msg': _msg, 'port': _st['port'],
                                   'lan': _st['lan'], 'urls': _st['urls'],
                                   'bak_days': repair.BAK_DAYS})
            elif p == '/api/lang':
                if q.get('lang') is not None:
                    mlib.cfg_set('lang', mlib.norm_lang(q.get('lang')))
                if q.get('out_lang') is not None:
                    _ol = (q.get('out_lang') or '').strip()
                    mlib.cfg_set('out_lang', mlib.norm_lang(_ol) if _ol else '')
                return self._json({'ok': True, 'lang': mlib.ui_lang(),
                                   'out_lang': mlib.out_lang()})
            elif p == '/api/sites/check':
                res = {}
                for (k, n, t, u, h) in mlib.SITES:
                    if h:                      # 只查已启用站点，免被禁用站点拖时间
                        res[k] = mlib.site_check(k)
                return self._json({'ok': True, 'res': res})
            elif p == '/api/sites/deep':
                res = {}
                for (k, n, t, u, h) in mlib.SITES:
                    if h:
                        res[k] = mlib.site_deep(k)
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
                    LANES['light']['stop'] = True
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
.s-Web{color:var(--ok)}.s-AI{color:var(--warn)}
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
  <select id="b-lang" title="界面语言 / UI language" style="min-width:96px"><option value="zh">中文</option><option value="en">English</option><option value="ja">日本語</option><option value="es">Español</option><option value="zh-Hant">繁體中文</option></select>
  <span class="muted">媒体库</span><input id="cfg-root" style="width:330px">
  <span class="muted">模型</span><select id="cfg-model"></select><span class="muted">输出语言</span><select id="cfg-outlang" title="元数据输出语言" style="min-width:104px"><option value="">跟随界面</option><option value="zh">中文</option><option value="en">English</option><option value="ja">日本語</option><option value="es">Español</option><option value="zh-Hant">繁體中文</option></select>
  <span class="muted">端口</span><input id="cfg-port" type="number" style="width:80px"> <label class="muted"><input type="checkbox" id="cfg-lan"> 局域网</label>
  <span class="muted">回滚点保留</span><input id="cfg-bakdays" type="number" style="width:54px" title="替换/修复后的 .orig 备份保留天数；备份统一放在 app 目录下的 backup；0 = 不保留"> <span class="muted">天</span>
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
<button id="b-incrsem">增量+语义</button>
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
    <select id="f-source"><option value="">来源:全部</option><option value="Web">Web</option><option value="AI">AI</option></select>
    <select id="f-kind"><option value="">语义:全部</option><option>coded</option><option>uncensored</option><option>none</option><option>(未跑)</option></select>
    <input id="f-q" placeholder="搜索 名字/标题/番号" style="width:200px">
    <select id="f-sort" title="排序字段"><option value="id">排序:编号</option><option value="first_seen">排序:入库时间</option><option value="last_update">排序:最后编辑</option><option value="bitrate">排序:码率</option></select>
    <select id="f-order" title="正序 / 倒序"><option value="asc">正序 ↑</option><option value="desc">倒序 ↓</option></select>
    <button id="b-refresh">刷新</button>
    <button id="b-archive">档案</button>
    <span class="muted right" id="count"></span>
  </div>
  <div class="bar" id="actbar" style="background:#171c25">
    <span class="muted">已选</span><b id="seln">0</b><span class="muted">条</span>
    <button class="primary" id="a-web" title="只对【选中的条目】跑网站线（不经过队列，队列里有别的也不会被跑）">重跑·网站</button>
    <button id="a-ai" title="只对【选中的条目】跑 AI 线（不经过队列）">重跑·AI</button>
    <button id="a-fill">AI 补全</button>
    <button id="a-vscan">AI 看片</button>
    <button id="a-auto" title="只对【选中的条目】跑全流程（不经过队列）">重跑·全流程</button>
    <select id="qmode" style="min-width:86px"><option value="auto">auto</option><option value="website">website</option><option value="ai">ai</option></select>
    <button id="a-queue" title="把选中条目加入队列（用右侧 qmode 的模式），之后点「跑队列」才执行">加入队列</button>
    <button id="a-open">打开目录</button>
<span id="hb-legend" style="margin-left:10px;font-size:12px;color:var(--muted)">徽标：<b style="color:#2e7d32">✓✓</b> 深检 · <b style="color:#1565c0">✓</b> 浅检 · <b style="color:var(--bad)">⚠</b> 有问题 · <b style="color:var(--muted)">↻</b> 需重测　｜　属性：<b style="color:var(--ok)">●</b>有 <b style="color:var(--warn)">◐</b>缺 <b style="color:var(--muted)">○</b>无 <b style="color:var(--bad)">✕</b>有误 <b style="color:#777">–</b>删除　<b>🌐</b>网站 <b>🤖</b>AI　<b style="color:var(--warn)">📢n</b>广告段数</span>
    <button id="a-flatten" title="把嵌套子目录平铺成 app 可自动刮削的结构：图片 P_、视频 V_，具名目录 序号_名字_；子目录移入 trash 可回滚">整理嵌套目录</button>
    <button class="danger" id="a-del">删除</button>
    <span class="muted">|</span>
    <button id="a-selall">全选本页</button>
    <button id="a-selnone">取消选择</button>
  </div>
  <div class="bar" id="actbar2" style="background:#171c25">
    <button id="a-thumbs">刷新缩略图</button>
    <button id="a-coverdel">删除封面图</button>
    <button id="a-health" title="只读体检：逐个视频探测能否打开/容器是否被截；不写任何媒体文件">体检</button>
    <button id="a-deep" title="数据级深检：逐文件扫 mdat 找洞 + 抽窗解码。慢（NAS ~11MB/s），用于查“能打开但花屏/跳帧”的文件">深检</button>
     <button id="a-media" title="媒体信息：读取每个条目主视频的平均码率/分辨率/帧率（只读头部，很快）">媒体信息</button>
     <button id="a-adtect" title="抽帧判断广告/片头片尾：先随机 30s + 末尾 30s 预检，有广告再按 30s 粗扫，最后 10/5/1s 精修边界；结果显示在列表「广告」列">广告检测</button>
     <button id="a-addel" title="按检测结果切掉广告区间（帧级精确重编码），【替换原片】；原文件留 .orig 回滚点，临时文件自动清理">删除广告</button>
    <button id="a-repair" title="按体检结果修复：索引重建（快·原地·可回滚）、容错重编码（慢·独立目录后自动替换）">修复</button>
  </div>
  </div>
  <table><thead><tr>
    <th style="width:104px">缩略图</th><th style="width:62px" title="id + 体检标识（不可编辑）">id</th>
    <th style="width:92px" title="质量 ●有 ◐缺 ○无 ✕有误 –删除 ｜ 来源 🌐网站 🤖AI ｜ 广告 📢段数">属性</th>
    <th style="width:80px">番号</th><th style="width:74px">语义</th>
    <th style="width:116px" title="分辨率 · 平均码率 · bpp（<0.04 红=信息量不足）">画质</th><th>标题 / 元信息</th><th style="width:70px">选择</th>
  </tr></thead><tbody id="rows"></tbody></table>
  <div class="bar"><button id="b-prev">‹ 上一页</button><select id="pjump" style="min-width:132px"></select><span id="page" class="muted"></span><button id="b-next">下一页 ›</button><button id="b-reload" class="right">⟳ 刷新列表</button></div>
</div>
<div class="modal" id="modal6"><div class="box">
  <div class="bar"><b id="rp-title">修复</b><span class="muted" id="rp-sum"></span><button class="right" id="md6-close">关闭</button></div>
  <div id="rp-body"></div>
  <div class="bar" id="rp-btns"></div>
</div></div>
<div class="modal" id="modal5"><div class="box">
  <div class="bar"><b>整理嵌套目录</b><span class="muted" id="fl-sum"></span><button class="right" id="md5-close">关闭</button></div>
  <div id="fl-body"></div>
  <div class="bar"><button class="primary" id="fl-go">继续整理</button><button id="fl-undo">回滚上一次整理</button><button id="fl-cancel">取消</button></div>
</div></div>
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
  <div class="bar"><button id="st-check">检测连通性</button><button id="st-deep">深度自检</button><button class="primary" id="st-save">保存</button></div>
</div></div>
<script>
const $=s=>document.querySelector(s);let off=0,lim=50,THUMBV=0;
const esc=t=>String(t==null?'':t).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
async function post(p){const r=await fetch(p,{method:'POST'});const j=await r.json();
  if(j.busy)alert(T('已有任务在跑'));return j;}
let PLAT='win';
function tagsOf(s){try{const a=JSON.parse(s||'[]');return (a||[]).join(' / ');}catch(e){return s||'';}}
async function cfg(){const s=await(await fetch('/api/stats')).json();
  PLAT=s.os||'win';                     // 容器/Linux：隐藏 Windows 专属按钮
  const _ao=document.getElementById('a-open'); if(_ao)_ao.style.display=(PLAT==='win'?'':'none');
  if(!$('#cfg-root').value)$('#cfg-root').value=s.root;
  const sel=$('#cfg-model');const ms=(s.ollama&&s.ollama.models)||[];
  if(sel.options.length!==ms.length+1){sel.innerHTML='<option value="'+esc(s.model)+'">'+esc(s.model)+'</option>'
    +ms.map(m=>'<option>'+esc(m)+'</option>').join('');}
  sel.value=s.model;
  const o=s.ollama||{};$('#ollama').innerHTML=o.up?('ollama ✅ '+(o.model_ok?'':'<span style="color:var(--bad)">模型缺失</span>')):'<span style="color:var(--bad)">ollama ✗ '+(o.error||'')+'</span>';
  const q=s.by_quality||{},sr=s.by_source||{},sm=s.sem||{};
  $('#cards').innerHTML=[['总数',s.total],['质量·有',q['有']||0],['质量·缺',q['缺']||0],['质量·有误',q['有误']||0],['质量·无',q['无']||0],['质量·删除',q['删除']||0],
     ['来源·Web',(sr.Web||0)+(sr.code||0)+(sr.Uncen||0)],['来源·AI',sr.AI||0],
     ['语义未跑',sm['(未跑)']||0],['档案',s.archive]]
    .map(([k,v])=>'<div class="card"><b>'+v+'</b><span>'+k+'</span></div>').join('');
  $('#qcount').textContent=s.queue||0;}
async function job(){const j=await(await fetch('/api/job')).json();
  if(!j.running&&_jbWas){_jbWas=false;if(/^(health|repair)/.test(j.name||''))_afterOp();}
  if(j.running)_jbWas=true;
  $('#log').textContent=(j.log||[]).join('\n');
  const L=$('#log');L.scrollTop=L.scrollHeight;   // 永远默认停在最后一行
  const p=j.total?Math.round(j.done*100/j.total):0;
  $('#prog').firstElementChild.style.width=(j.running?Math.max(p,3):(j.done?'100':'0'))+'%';
  const busy=j.running;['b-pipeline','b-rebuild','b-incr','b-incrsem','b-sem','b-fetch','b-qrun'].forEach(i=>$('#'+i).disabled=busy);
  $('#b-stop').disabled=!busy;$('#qcount').textContent=j.queue||0;}
function saveState(){try{localStorage.setItem('ms_state',JSON.stringify({off:off,
  quality:$('#f-quality').value,source:$('#f-source').value,kind:$('#f-kind').value,q:$('#f-q').value,
  sort:$('#f-sort').value,order:$('#f-order').value}));}catch(e){}}
function loadState(){try{const s=JSON.parse(localStorage.getItem('ms_state')||'{}');
  if(typeof s.off==='number'&&s.off>=0)off=s.off;
  if(s.quality!==undefined)$('#f-quality').value=s.quality;
  if(s.source!==undefined)$('#f-source').value=s.source;
  if(s.kind!==undefined)$('#f-kind').value=s.kind;
  if(s.q!==undefined)$('#f-q').value=s.q;
  if(s.sort!==undefined)$('#f-sort').value=s.sort;
  if(s.order!==undefined)$('#f-order').value=s.order;}catch(e){}}
const QICON={'有':['●','var(--ok)','质量：有（齐全）'],
  '缺':['◐','var(--warn)','质量：缺（有标题、缺标签）'],
  '无':['○','var(--muted)','质量：无（没抓到信息）'],
  '有误':['✕','var(--bad)','质量：有误（抓取失败 / 体检不合格）'],
  '删除':['–','#777','已标记删除']};
function qIcon(q){const m=QICON[q]||['·','var(--muted)','质量：未定'];
  return '<span style="color:'+m[1]+';font-weight:700" title="'+T(m[2])+'">'+m[0]+'</span>';}
function srcIcon(s){return (s==='AI')
  ? '<span title="'+T('来源：AI 线')+'">🤖</span>'
  : '<span title="'+T('来源：网站线')+'">🌐</span>';}
function fmtT(t){t=Math.max(0,Math.round(t||0));const m=Math.floor(t/60);
  return m+':'+String(t%60).padStart(2,'0');}
function kindLabel(k){const M={coded:'Code',uncensored:'Uncen',none:'unknown'};
  return M[k]||(k||'—');}
function adIcon(r,short){try{const a=JSON.parse(r.ads||'[]');
  if(a&&a.length){let t='';for(const x of a){t+=(t?' · ':'')+fmtT(x[0])+'-'+fmtT(x[1]);}
    return '<span style="color:var(--warn)" title="'+T('广告 ')+a.length+T(' 段：')+t+'">📢'+a.length+'</span>';}
  if(r.ads_ts){return '<span style="color:var(--muted)" title="'+T('检测过：无广告')+'">📢</span>';}
  return '';}catch(e){return '';}}
function resLabel(w,h){const m=Math.min(w,h),M=Math.max(w,h);
  return (m>=2000)?'4K':(m>=1400?'1440p':(m>=1000?'1080p':(m>=700?'720p':M+'×'+m)));}
async function list(){const qs=new URLSearchParams({limit:lim,offset:off,
  quality:$('#f-quality').value,source:$('#f-source').value,kind:$('#f-kind').value,q:$('#f-q').value,
  sort:$('#f-sort').value,order:$('#f-order').value});
  const d=await(await fetch('/api/items?'+qs)).json();
  $('#rows').innerHTML=d.rows.map(r=>{
    const q=r.quality||'';
    return '<tr id="r'+r.id+'"><td><a href="#" class="imgopen" data-id="'+r.id+'" title="本地打开大图"><img class="thumb" loading="lazy" src="/api/thumb/'+r.id+'?v='+THUMBV+'" onerror="this.style.opacity=.15"></a></td>'
    +'<td title="入库 '+esc(r.first_seen||'')+'\uff0f最后编辑 '+esc(r.last_update||'')+'">'+r.id+hbadge(r)+'</td>'
    +'<td style="white-space:nowrap">'+qIcon(q)+' '+srcIcon(r.source)+adIcon(r,1)+'</td>'
    +'<td class="ed" data-f="sem_code" data-id="'+r.id+'" data-v="'+esc(r.sem_code||r.code||'')+'" title="双击修改">'+esc(r.sem_code||r.code||'')+'</td>'
    +'<td class="ed" data-f="sem_kind" data-id="'+r.id+'" data-v="'+(r.sem_kind||'')+'" title="双击修改">'+kindLabel(r.sem_kind)+(r.sem_manual?' <span class="muted">(人工)</span>':'')+'</td>'
    +'<td class="muted">'+(r.mw&&r.mh
      ?resLabel(r.mw,r.mh)+' <span style="color:var(--fg)">'
       +(r.kbps?(r.kbps>=1000?(r.kbps/1000).toFixed(1)+'Mb':r.kbps+'k'):'')+'</span>'
      +'<div class="meta" style="color:'+((r.bpp&&r.bpp<0.04)?'var(--bad)':(r.bpp&&r.bpp<0.08)?'var(--warn)':'var(--ok)')     +'">'+(r.bpp?'bpp '+r.bpp.toFixed(3):'')+'</div>'
      +'<div class="meta">'+r.mw+'×'+r.mh+' @'+(Math.round((r.mfps||0)*10)/10)+'</div>'
      :'—')+'</td>'
    +'<td><a href="#" data-id="'+r.id+'" class="lk" style="color:var(--fg)">'+esc(r.title||r.name)+'</a>'
    +'<div class="meta">简介: '+esc((r.plot||'').slice(0,70))+'</div>'
    +'<div class="meta">标签: <span class="tags">'+esc(tagsOf(r.tags))+'</span></div>'
    +'<div class="meta">作者: '+esc(tagsOf(r.actors))+' ｜ 年份: '+esc(r.year||'')+' ｜ 制作: '+esc(r.studio||'')+'</div>'
    +'</td>'
    +'<td><button class="selbtn" data-id="'+r.id+'">选择</button></td></tr>';}).join('');
  $('#count').textContent='共 '+d.total+' 条';
  stripWinOnly();
  document.querySelectorAll('.selbtn').forEach(b=>{if(SEL.has(b.dataset.id))selSet(b,true);});
  const pages=Math.max(1,Math.ceil(d.total/lim)),cur=Math.floor(off/lim)+1;
  const pj=$('#pjump');pj.innerHTML='';
  for(let i=1;i<=pages;i++){const o=document.createElement('option');
    o.value=(i-1)*lim;o.textContent='第 '+i+' / '+pages+' 页';if(i===cur)o.selected=true;pj.appendChild(o);}
  $('#page').textContent=(off+1)+'–'+Math.min(off+lim,d.total);
  saveState();}
function adsText(s){try{const a=JSON.parse(s||'[]');if(!a||!a.length)return '';
  const f=t=>{t=Math.max(0,Math.round(t));const m=Math.floor(t/60),ss=t%60;
    return m+':'+String(ss).padStart(2,'0');};
  return a.map(x=>f(x[0])+'-'+f(x[1])).join(' ');}catch(e){return '';}}
const SEL=new Set();
function selinfo(){$('#seln').textContent=SEL.size;}
function selSet(b,on){const id=b.dataset.id,tr=b.closest('tr');
  if(on){SEL.add(id);tr.classList.add('sel');b.textContent='✓ 已选';}
  else{SEL.delete(id);tr.classList.remove('sel');b.textContent='选择';}}
$('#a-selall').onclick=()=>{document.querySelectorAll('.selbtn').forEach(b=>selSet(b,true));selinfo();};
$('#a-selnone').onclick=()=>{SEL.clear();document.querySelectorAll('.selbtn').forEach(b=>selSet(b,false));selinfo();};
function batchRun(mode){return async()=>{if(!SEL.size)return alert(T('先选条目'));
  await post('/api/runset?mode='+mode+'&ids='+[...SEL].join(','));};}
$('#a-web').onclick=batchRun('website');
$('#a-ai').onclick=batchRun('ai');
$('#a-fill').onclick=async()=>{
  const ids=[...SEL].join(',');
  if(!SEL.size&&!confirm(T('未选中条目 —— 将对【全部 有简介但没标签】的条目按简介补标签，继续？')))return;
  await post('/api/fill?limit='+$('#limit').value+'&ai_group='+$('#aig').value+(ids?'&ids='+ids:''));};
$('#a-vscan').onclick=async()=>{
  const ids=[...SEL].join(',');
  if(!SEL.size&&!confirm(T('未选中条目 —— 将对【全部 无/有误】的条目抽帧看视频（较慢），继续？')))return;
  await post('/api/vscan?limit='+$('#limit').value+(ids?'&ids='+ids:''));};
$('#a-auto').onclick=batchRun('auto');
$('#a-queue').onclick=async()=>{if(!SEL.size)return alert(T('先选条目'));let n=0;
  for(const id of SEL){const r=await post('/api/queue/'+id+'?mode='+$('#qmode').value);n=r.queue;}
  $('#qcount').textContent=n;};
$('#a-open').onclick=async()=>{if(!SEL.size)return alert(T('先选条目'));
  for(const id of SEL)await post('/api/open/'+id);};
$('#a-del').onclick=async()=>{if(!SEL.size)return alert(T('先选条目'));
  if(!confirm(T('【删除】共 '+SEL.size+' 个目录（含视频）→ Windows 回收站\n\n⚠ NAS 网络路径，实际是【永久删除】，不可恢复。\n\n继续？')))return;
  if(!confirm(T('再次确认：永久删除这 '+SEL.size+' 个？')))return;
  for(const id of SEL)await post('/api/delete/'+id);
  SEL.clear();selinfo();list();};
let FL_IDS=[];
function flRow(x){
  const st=(x.ok?'':'<span style="color:var(--bad)">冲突</span> ')+(x.warn?'<span style="color:var(--warn,#e0a458)">需确认</span>':'');
  return '<tr><td>'+x.id+'</td><td>'+esc(String(x.name||'').slice(0,42))+'</td><td>'+(x.mode==='type'?'P/V 桶':'具名')+'</td><td>'+x.count
    +'</td><td>'+x.depth+'</td><td>'+x.max_pre_len+'</td><td>'+st+'</td></tr>';}
$('#a-flatten').onclick=async()=>{
  const ids=[...SEL].join(',');
  const r=await(await fetch('/api/flatten/preview'+(ids?'?ids='+ids:''),{method:'POST'})).json();
  if(!r.total)return alert(T('没有需要整理的嵌套目录（kind=nested 为空）'));
  const warn=(r.items||[]).filter(x=>x.warn), bad=r.bad||[];
  let h='<p>将整理 <b>'+r.total+'</b> 个目录、共改名 <b>'+r.files+'</b> 个文件。子目录会移入 trash（可用【回滚】还原）；随子目录一起移走的是 Thumbs.db / 描述.txt 之类的非媒体文件，未识别类型的文件不会被动、其子目录也不清理。</p>';
  if(warn.length)h+='<p style="color:var(--bad)"><b>⚠️ 有 '+warn.length+' 个目录建议先手动整理：</b>子目录层数 ≥ '+r.warn_depth+'，或加的前缀超过 '+r.warn_prefix+' 字符。</p>';
  if(bad.length)h+='<p style="color:var(--bad)">⚠️ '+bad.length+' 个目录有重名/撞名，将整条跳过：id '+bad.join(', ')+'</p>';
  h+='<table><thead><tr><th>id</th><th>目录</th><th>模式</th><th>改名</th><th>层数</th><th>最长前缀</th><th>提示</th></tr></thead><tbody>'
    +(r.items||[]).map(flRow).join('')+'</tbody></table>';
  const first=(r.items||[]).find(x=>x.ok&&x.items&&x.items.length);
  if(first)h+='<p class="muted">示例（id '+first.id+'）：</p><pre style="white-space:pre-wrap;max-height:190px;overflow:auto">'
    +first.items.slice(0,10).map(i=>esc(i.src)+'  ->  '+esc(i.dst)).join('\n')+'</pre>';
  $('#fl-body').innerHTML=h;$('#fl-sum').textContent=r.files+' 个文件';
  FL_IDS=(r.items||[]).filter(x=>x.ok&&((x.count||0)>0||(x.subs||[]).length>0)).map(x=>x.id);
  $('#modal5').classList.add('on');};
$('#md5-close').onclick=$('#fl-cancel').onclick=()=>$('#modal5').classList.remove('on');
$('#fl-go').onclick=async()=>{
  if(!FL_IDS.length)return alert(T('没有可整理的目录'));
  if(!confirm(T('开始整理 '+FL_IDS.length+' 个目录？')+'\n\n'+T('子目录会移入 trash，可用【回滚上一次整理】还原。')))return;
  await post('/api/flatten/run?ids='+FL_IDS.join(','));
  $('#modal5').classList.remove('on');list();};
$('#fl-undo').onclick=async()=>{
  if(!confirm(T('回滚上一次整理？（把子目录搬回、文件名改回）')))return;
  const j=await(await fetch('/api/flatten/undo?last=1',{method:'POST'})).json();
  alert(j.ok?(T('回滚完成')+'：restored '+j.restored+((j.errors&&j.errors.length)?('，异常 '+j.errors.length):'')):(T('回滚失败')+'：'+(j.error||'')));
  await fetch('/api/scan?mode=incr',{method:'POST'});list();};
let _jbWas=false;
function hbadge(r){const s=+r.hstate||0;if(!s)return '';const M={4:['var(--muted)','记录已过期（文件有变动，如刚修复）—— 需重测','↻'],2:['var(--bad)','发现问题：体检/深检不合格（点开看详情）','⚠'],3:['#2e7d32','体检 + 深检 均通过（深）','✓✓'],1:['#1565c0','仅体检通过（浅）：未做数据级深检，未知是否有数据洞','✓']};const m=M[s]||M[1];return ' <span style="color:'+m[0]+';font-weight:600" title="'+T(m[1])+'">'+m[2]+'</span>'}
function rpShow(title,sum,html,btns){
  $('#rp-title').textContent=title;$('#rp-sum').textContent=sum||'';$('#rp-body').innerHTML=html||'';
  $('#rp-btns').innerHTML='';(btns||[]).forEach(b=>{const el=document.createElement('button');
    if(b.primary)el.className='primary';el.textContent=b.label;el.onclick=b.fn;$('#rp-btns').appendChild(el);});
  $('#modal6').classList.add('on');}
$('#md6-close').onclick=()=>$('#modal6').classList.remove('on');
$('#a-deep').onclick=async()=>{
  const ids=[...SEL].join(',');
  if(!SEL.size&&!confirm(T('未选中条目 —— 将对【全部】条目做数据级深检（要扫整个文件，很慢；不写媒体文件），继续？')))return;
  if(!confirm(T('深检会逐个读取视频文件找数据洞（NAS 上约 11MB/s）。继续？')))return;
  const fq=((document.getElementById('force')||{}).checked)?((ids?'&':'?')+'force=1'):'';
  await post('/api/health'+(ids?'?ids='+ids:'')+fq+((fq||ids)?'&':'?')+'deep=1');};
$('#a-adtect').onclick=async()=>{
  const ids=[...SEL].join(',');
  if(!SEL.size&&!confirm(T('未选中条目 —— 将对【全部】条目做广告检测（逐条抽帧调视觉模型，很慢），继续？')))return;
  await post('/api/adtect'+(ids?'?ids='+ids:''));};
$('#a-addel').onclick=async()=>{
  const ids=[...SEL].join(',');
  if(!SEL.size)return alert(T('先选条目'));
  if(!confirm(T('删除选中条目的广告区间？会【替换原片】：先校验时长/抽帧，再原子换名，原文件备份为 .orig 回滚点，临时文件自动清理')))return;
  await post('/api/addel?ids='+ids);};
$('#a-media').onclick=async()=>{
  const ids=[...SEL].join(',');
  const fq=((document.getElementById('force')||{}).checked)?((ids?'&':'?')+'force=1'):'';
  await post('/api/media'+(ids?'?ids='+ids:'')+fq);};
$('#a-health').onclick=async()=>{
  const ids=[...SEL].join(',');
  if(!SEL.size&&!confirm(T('未选中条目 —— 将对【全部】条目做只读体检（逐文件探测，较慢；不写任何文件），继续？')))return;
  const fq=((document.getElementById('force')||{}).checked)?((ids?'&':'?')+'force=1'):'';
  await post('/api/health'+(ids?'?ids='+ids:'')+fq);};
$('#a-repair').onclick=async()=>{
  const ids=[...SEL].join(',');
  if(!SEL.size)return alert(T('先选条目'));
  const rfq=((document.getElementById('force')||{}).checked)?'&refresh=1':'';
  const r=await(await fetch('/api/repair/plan'+(ids?'?ids='+ids:'')+rfq,{method:'POST'})).json();
  const closeBtn={label:T('关闭'),fn:()=>$('#modal6').classList.remove('on')};
  if(!r.n_files){rpShow(T('修复'),'', '<p>'+T('选中的条目没有发现需要修复的文件。')+'</p>',[closeBtn]);return;}
  const rows=(r.items||[]).map(x=>'<tr><td>'+x.id+'</td><td>'+esc(String(x.name||'').slice(0,30))+'</td><td>'
    +x.files.map(f=>'<div class="meta">'+esc(String(f.file||'').slice(0,40))+' <b>['+esc(f.cls||'')+']</b> '
      +esc((f.plan&&f.plan.length)?f.plan.join(' / '):(f.reason||''))+'</div>').join('')+'</td></tr>').join('');
  const cnt=t=>(r.items||[]).reduce((a,x)=>a+x.files.filter(f=>f.tier===t).length,0);
  const idx=cnt('index')+cnt('gap')+cnt('remux'),re=cnt('reencode');
  rpShow(T('修复'), idx+' '+T('可修（索引/小洞）')+' / '+re+' '+T('需重编码')+' / '+Math.max(0,r.n_files-idx-re)+' '+T('不可修'),
    '<p>'+T('小洞阈值')+' '+r.gap_sec+T(' 秒（插帧待接入）')+'</p><table><thead><tr><th>id</th><th>'+T('条目')+'</th><th>'+T('文件与处理')+'</th></tr></thead><tbody>'+rows+'</tbody></table>',
    [{label:T('修索引+小洞（原地·可回滚）'),primary:true,fn:async()=>{await post('/api/repair?ids='+ids+'&mode=safe&go=1');$('#modal6').classList.remove('on');list();}},
     {label:T('全部（含重编码·慢·会替换）'),fn:async()=>{if(!confirm(T('重编码会产出新文件并【自动替换】原文件（原文件保留为 .orig 回滚点）。继续？')))return;await post('/api/repair?ids='+ids+'&mode=all&go=1');$('#modal6').classList.remove('on');list();}},
     closeBtn]);
};
async function _afterOp(){
  let r={};try{r=await(await fetch('/api/repair/last',{method:'POST'})).json();}catch(e){}
  if(!r||!r.kind)return;
  const _sec=(k,t)=>{const v=(r.lists&&r.lists[k])||[];if(!v.length)return '';
    return '<p><b>'+t+'（'+v.length+'）</b></p><pre style="white-space:pre-wrap;max-height:140px;overflow:auto;margin:0 0 8px">'
      +esc(v.slice(0,40).join('\n'))+(v.length>40?'\n…还有 '+(v.length-40)+' 个':'')+'</pre>';};
  if(r.kind==='health'){
    rpShow(T('体检完成'), T('异常')+' '+r.bad+' / '+T('文件')+' '+r.files,
      '<p>'+T('可修')+' '+(r.fixable+r.heavy)+' · '+T('不可修')+' '+r.dead+'</p>'
      +_sec('index',T('可修·索引重建'))+_sec('gap',T('可修·小洞插帧'))
      +_sec('reencode',T('需重编码'))+_sec('dead',T('不可修'))+_sec('partial',T('深检未完成（可续扫）'))+_sec('novideo',T('无视频')),
      (r.fixable?[{label:T('是 — 立即修复可修的'),primary:true,fn:async()=>{await post('/api/repair?ids='+(r.ids||[]).join(',')+'&mode=safe&go=1');$('#modal6').classList.remove('on');}},
                  {label:T('否 — 稍后再说'),fn:()=>$('#modal6').classList.remove('on')}]
                :[{label:T('好'),fn:()=>$('#modal6').classList.remove('on')}]));
  }else if(r.kind==='repair'){
    const hasB=(r.backups||[]).length;
    rpShow(T('修复完成'), T('成功')+' '+r.ok+' / '+T('失败')+' '+r.fail,
      '<p>'+T('索引重建')+' '+r.index+' · '+T('重编码')+' '+r.recode+' · '+T('跳过')+' '+r.skip+'</p>'
      +_sec('index',T('已重建索引'))+_sec('remux',T('已重封装'))+_sec('recode',T('已重编码'))+_sec('fail',T('失败'))+_sec('skip',T('跳过/未处理'))
      +(hasB?('<p class="muted">'+T('回滚备份')+' '+hasB+' '+T('个（重编码前原文件）')+'</p>'):
             '<p class="muted">'+T('无回滚备份（索引重建是纯追加，回滚=truncate 回原长度）')+'</p>'),
      (hasB?[{label:T('是 — 删除回滚备份'),fn:async()=>{if(!confirm(T('删除 '+hasB+' 个 .orig 备份？（不可恢复）')))return;const r2=await post('/api/repair/purge');alert(T('已删除')+' '+r2.removed);$('#modal6').classList.remove('on');}},
             {label:T('否 — 保留备份'),primary:true,fn:()=>$('#modal6').classList.remove('on')}]
           :[{label:T('好'),fn:()=>$('#modal6').classList.remove('on')}]));
  }
}
document.addEventListener('dblclick',e=>{
  const td=e.target.closest?e.target.closest('td.ed'):null;
  if(!td)return;
  const f=td.dataset.f,id=td.dataset.id,cur=td.dataset.v||'';
  const OPTS={quality:['有','缺','有误','无','删除'],source:['Web','AI'],
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
    +'<span class="muted">来源</span><select id="ed-source">'+sel(d.source||'',['Web','AI'])+'</select>'
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
  const n=await(await fetch('/api/net?port='+($('#cfg-port').value||0)+'&lan='+($('#cfg-lan').checked?1:0)
    +'&bak_days='+($('#cfg-bakdays').value||0),{method:'POST'})).json();
  if(n.msg)console.log(n.msg); if(n.urls&&n.urls.length)console.log(T('访问地址: ')+n.urls.join('  '));
  cfg();};
(async()=>{try{const n=await(await fetch('/api/net')).json();
  $('#cfg-port').value=n.port||8800; $('#cfg-lan').checked=!!n.lan;
  if(n.bak_days!==undefined)$('#cfg-bakdays').value=n.bak_days;
  if(n.lan)console.log(T('局域网已开启: ')+(n.urls||[]).join('  '));}catch(e){}})();
$('#b-pipeline').onclick=()=>post('/api/pipeline?limit='+$('#limit').value+'&group='+$('#semg').value+'&ai_group='+$('#aig').value);
$('#b-rebuild').onclick=()=>{ if(!confirm(T('【建库】会清空并重建全部档案！\n当前所有行的状态/语义/元信息都会被重置，旧记录先进入“档案”。\n\n确定继续？')))return;
  if(!confirm(T('请再次确认：真的要【建库】吗？')))return; post('/api/scan?mode=rebuild'); };
$('#b-incr').onclick=()=>post('/api/scan?mode=incr');
$('#b-incrsem').onclick=()=>post('/api/scan?mode=incr&sem=1');
$('#b-sem').onclick=()=>post('/api/sem?limit='+$('#limit').value+'&group='+$('#semg').value+($('#force').checked?'&force=1':''));
$('#b-fetch').onclick=()=>post('/api/fetch?mode='+$('#mode').value+'&limit='+$('#limit').value+'&ai_group='+$('#aig').value);
$('#b-cookie').onclick=async()=>{const s=await(await fetch('/api/stats')).json();
  $('#ck-state').textContent=s.cookie?'当前：已设置':'当前：未设置';$('#modal3').classList.add('on');};
$('#md3-close').onclick=()=>$('#modal3').classList.remove('on');
$('#ck-file').onchange=e=>{const f=e.target.files[0];if(!f)return;const rd=new FileReader();
  rd.onload=()=>{$('#ck-text').value=rd.result;};rd.readAsText(f);};
$('#ck-save').onclick=async()=>{const r=await fetch('/api/cookie',{method:'POST',body:$('#ck-text').value});
  const j=await r.json();alert(j.set?T('已保存 (%d 字符)').replace('%d',j.len):T('已清空'));$('#modal3').classList.remove('on');};
$('#ck-clear').onclick=async()=>{await fetch('/api/cookie?clear=1',{method:'POST'});alert(T('已清空'));$('#modal3').classList.remove('on');};
const UI_LANGS=['zh','en','ja','es','zh-Hant'];
const LANG_LABEL={'zh':'中文','en':'English','ja':'日本語','es':'Español','zh-Hant':'繁體中文'};
const DICT={"en": {"关闭": "Close", "删除": "Delete", "档案": "Archive", "保存": "Save", "说明": "Docs", "站点": "Sites", "媒体库": "Library", "模型": "Model", "端口": "Port", "局域网": "LAN", "保存设置": "Save", "① 全流程": "① Full pipeline", "建库": "Rebuild", "增量扫描": "Scan", "语义识别": "Classify", "抓取": "Fetch", "跑队列": "Run queue", "清空队列": "Clear queue", "停止": "Stop", "上限(0=全部)": "Limit (0=all)", "模式": "Mode", "语义组(0=全部)": "Sem group (0=all)", "AI组(0=全部)": "AI group (0=all)", "队列": "Queue", "筛选": "Filter", "质量:全部": "Quality: all", "来源:全部": "Source: all", "语义:全部": "Semantic: all", "(未跑)": "(none)", "刷新": "Refresh", "已选": "Selected", "条": "items", "重跑·网站": "Re-run · sites", "重跑·AI": "Re-run · AI", "AI 补全": "AI tag fill", "AI 看片": "AI watch", "重跑·全流程": "Re-run · full", "重跑队列": "Queue re-run", "打开目录": "Open folder", "刷新缩略图": "Refresh thumbs", "删除封面图": "Delete cover", "全选本页": "Select page", "取消选择": "Clear", "缩略图": "Thumb", "质量": "Quality", "来源": "Source", "番号": "Code", "语义": "Semantic", "标题 / 元信息": "Title / metadata", "选择": "Select", "← 上一页": "← Prev", "下一页 →": "Next →", "⟳ 刷新列表": "⟳ Refresh list", "清空": "Clear", "站点设置": "Sites", "检测连通性": "Check connectivity", "深度自检": "Deep check", "搜索 名字/标题/番号": "Search name / title / code", "总数": "Total", "质量·有": "Q·OK", "质量·缺": "Q·Partial", "质量·有误": "Q·Check", "质量·无": "Q·None", "质量·删除": "Q·Deleted", "来源·code": "Src·code", "来源·Uncen": "Src·Uncen", "来源·AI": "Src·AI", "语义未跑": "Sem:none", "简介": "Plot", "标签": "Tags", "作者": "Author", "年份": "Year", "制作": "Studio", "双击修改": "Double-click to edit", "本地打开大图": "Open full image", "(待接入)": "(pending)", "检测中…（逐个真实请求）": "Checking… (real request each)", "检测完成": "Check done", "至少勾一个站点": "Select at least one site", "先选条目": "Select items first", "已有任务在跑": "A job is already running", "已清空": "Cleared", "当前：已设置": "Current: set", "当前：未设置": "Current: not set", "已保存": "Saved", "保存成功": "Saved", "文件": "File", "逗号分隔": "comma separated", "确认": "Confirm", "是": "Yes", "否": "No", "有": "OK", "缺": "Partial", "有误": "Check", "无": "None", "启用": "On", "类型": "Type", "入口": "Entry", "状态": "Status", "时间": "Time", "原因": "Reason", "名字": "Name", "模型缺失": "model missing", "未选中条目": "Nothing selected", "【删除】共 {n} 个目录（含视频）→ Windows 回收站\n\n⚠ NAS 网络路径，实际是【永久删除】，不可恢复。\n\n继续？": "[Delete] {n} folders (with videos) → Recycle Bin\n\n⚠ On a NAS network path this is a PERMANENT delete, unrecoverable.\n\nContinue?", "再次确认：永久删除这 {n} 个？": "Confirm again: permanently delete these {n}?", "【建库】会清空并重建全部档案！\n当前所有行的状态/语义/元信息都会被重置，旧记录先进入“档案”。\n\n确定继续？": "[Rebuild] wipes and rebuilds all entries!\nStatus/semantic/metadata of every row will be reset; old records move to \"Archive\".\n\nContinue?", "请再次确认：真的要【建库】吗？": "Confirm again: really rebuild?", "将对【全部/有简介但没标签】的条目按简介补标签，继续？": "Will add tags from plot for [all / has plot but no tags]. Continue?", "将对【全部/无/有误】的条目抽帧看视频（较慢），继续？": "Will extract frames and watch videos for [all / none / bad] (slow). Continue?", "删除选中条目的封面图（poster.jpg 等）+ 缩略图缓存，继续？": "Delete cover images (poster.jpg etc.) + thumbnail cache of selected items. Continue?", "已保存 (%d 字符)": "Saved (%d chars)", "已保存：": "Saved: ", "已删除封面/缩略图 %s 个文件": "Deleted %s cover/thumb files", "已重建 %s 张缩略图": "Rebuilt %s thumbnails", "切换失败（重启后生效）：": "Switch failed (takes effect after restart): ", "已切换到": "Switched to ", "访问地址: ": "URL: ", "局域网已开启: ": "LAN enabled: ", "通过": "OK", "不通过": "FAIL"}, "ja": {"关闭": "閉じる", "删除": "削除", "档案": "アーカイブ", "保存": "保存", "说明": "説明", "站点": "サイト", "媒体库": "ライブラリ", "模型": "モデル", "端口": "ポート", "局域网": "LAN", "保存设置": "設定を保存", "① 全流程": "① 全処理", "建库": "再構築", "增量扫描": "差分スキャン", "语义识别": "分類", "抓取": "取得", "跑队列": "キュー実行", "清空队列": "キューを空に", "停止": "停止", "上限(0=全部)": "上限 (0=すべて)", "模式": "モード", "语义组(0=全部)": "分類組 (0=すべて)", "AI组(0=全部)": "AI組 (0=すべて)", "队列": "キュー", "筛选": "絞り込み", "质量:全部": "品質: すべて", "来源:全部": "取得元: すべて", "语义:全部": "分類: すべて", "(未跑)": "(未実行)", "刷新": "更新", "已选": "選択中", "条": "件", "重跑·网站": "再取得 · サイト", "重跑·AI": "再取得 · AI", "AI 补全": "AI タグ補完", "AI 看片": "AI 映像解析", "重跑·全流程": "再実行 · 全処理", "重跑队列": "キューに再実行", "打开目录": "フォルダを開く", "刷新缩略图": "サムネ更新", "删除封面图": "カバー削除", "全选本页": "このページを全選択", "取消选择": "選択解除", "缩略图": "サムネ", "质量": "品質", "来源": "取得元", "番号": "品番", "语义": "分類", "标题 / 元信息": "タイトル / メタ情報", "选择": "選択", "← 上一页": "← 前へ", "下一页 →": "次へ →", "⟳ 刷新列表": "⟳ 再読込", "清空": "クリア", "站点设置": "サイト設定", "检测连通性": "接続確認", "深度自检": "詳細チェック", "搜索 名字/标题/番号": "名前 / タイトル / 品番を検索", "总数": "総数", "质量·有": "品質·完全", "质量·缺": "品質·不足", "质量·有误": "品質·要確認", "质量·无": "品質·なし", "质量·删除": "品質·削除済", "来源·code": "取得元·code", "来源·Uncen": "取得元·Uncen", "来源·AI": "取得元·AI", "语义未跑": "分類未実行", "简介": "あらすじ", "标签": "タグ", "作者": "出演", "年份": "年", "制作": "メーカー", "双击修改": "ダブルクリックで編集", "本地打开大图": "原寸画像を開く", "(待接入)": "(未対応)", "检测中…（逐个真实请求）": "確認中…（1件ずつ実リクエスト）", "检测完成": "確認完了", "至少勾一个站点": "サイトを1つ以上選択してください", "先选条目": "先に項目を選択してください", "已有任务在跑": "既に実行中のタスクがあります", "已清空": "クリアしました", "当前：已设置": "現在: 設定済み", "当前：未设置": "現在: 未設定", "已保存": "保存しました", "保存成功": "保存しました", "文件": "ファイル", "逗号分隔": "カンマ区切り", "确认": "確認", "是": "はい", "否": "いいえ", "有": "完全", "缺": "不足", "有误": "要確認", "无": "なし", "启用": "有効", "类型": "種別", "入口": "入口", "状态": "状態", "时间": "日時", "原因": "理由", "名字": "名前", "模型缺失": "モデル未検出", "未选中条目": "未選択", "【删除】共 {n} 个目录（含视频）→ Windows 回收站\n\n⚠ NAS 网络路径，实际是【永久删除】，不可恢复。\n\n继续？": "【削除】{n} 件のフォルダ（動画含む）→ Windows ゴミ箱\n\n⚠ NAS ネットワークパスでは【完全削除】となり復元できません。\n\n続行しますか？", "再次确认：永久删除这 {n} 个？": "再確認：この {n} 件を完全削除しますか？", "【建库】会清空并重建全部档案！\n当前所有行的状态/语义/元信息都会被重置，旧记录先进入“档案”。\n\n确定继续？": "【再構築】は全アーカイブを消去して作り直します！\n全行の状態/分類/メタ情報はリセットされ、旧記録は「アーカイブ」へ移動します。\n\n続行しますか？", "请再次确认：真的要【建库】吗？": "再確認：本当に【再構築】しますか？", "将对【全部/有简介但没标签】的条目按简介补标签，继续？": "【全件 / あらすじ有りタグ無し】の項目にあらすじからタグを補完します。続行しますか？", "将对【全部/无/有误】的条目抽帧看视频（较慢），继续？": "【全件 / なし / 要確認】の項目をフレーム抽出して解析します（低速）。続行しますか？", "删除选中条目的封面图（poster.jpg 等）+ 缩略图缓存，继续？": "選択項目のカバー画像（poster.jpg 等）とサムネイルキャッシュを削除します。続行しますか？", "已保存 (%d 字符)": "保存しました (%d 文字)", "已保存：": "保存しました: ", "已删除封面/缩略图 %s 个文件": "カバー/サムネ %s 件を削除しました", "已重建 %s 张缩略图": "サムネ %s 件を再生成しました", "切换失败（重启后生效）：": "切替失敗（再起動後に反映）: ", "已切换到": "切替完了: ", "访问地址: ": "URL: ", "局域网已开启: ": "LAN 有効: ", "通过": "OK", "不通过": "NG"}, "es": {"关闭": "Cerrar", "删除": "Eliminar", "档案": "Archivo", "保存": "Guardar", "说明": "Ayuda", "站点": "Sitios", "媒体库": "Biblioteca", "模型": "Modelo", "端口": "Puerto", "局域网": "LAN", "保存设置": "Guardar", "① 全流程": "① Flujo completo", "建库": "Reconstruir", "增量扫描": "Escanear", "语义识别": "Clasificar", "抓取": "Obtener", "跑队列": "Ejecutar cola", "清空队列": "Vaciar cola", "停止": "Detener", "上限(0=全部)": "Límite (0=todo)", "模式": "Modo", "语义组(0=全部)": "Grupo sem. (0=todo)", "AI组(0=全部)": "Grupo IA (0=todo)", "队列": "Cola", "筛选": "Filtro", "质量:全部": "Calidad: todo", "来源:全部": "Fuente: todo", "语义:全部": "Semántica: todo", "(未跑)": "(sin ejecutar)", "刷新": "Actualizar", "已选": "Seleccionados", "条": "ítems", "重跑·网站": "Re-ejecutar · sitios", "重跑·AI": "Re-ejecutar · IA", "AI 补全": "Relleno IA", "AI 看片": "Ver con IA", "重跑·全流程": "Re-ejecutar · completo", "重跑队列": "Re-ejecutar en cola", "打开目录": "Abrir carpeta", "刷新缩略图": "Actualizar miniaturas", "删除封面图": "Borrar portada", "全选本页": "Seleccionar página", "取消选择": "Quitar selección", "缩略图": "Miniatura", "质量": "Calidad", "来源": "Fuente", "番号": "Código", "语义": "Semántica", "标题 / 元信息": "Título / metadatos", "选择": "Seleccionar", "← 上一页": "← Anterior", "下一页 →": "Siguiente →", "⟳ 刷新列表": "⟳ Actualizar lista", "清空": "Vaciar", "站点设置": "Ajustes de sitios", "检测连通性": "Comprobar conexión", "深度自检": "Comprobación real", "搜索 名字/标题/番号": "Buscar nombre / título / código", "总数": "Total", "质量·有": "Cal·OK", "质量·缺": "Cal·Parcial", "质量·有误": "Cal·Revisar", "质量·无": "Cal·Ninguno", "质量·删除": "Cal·Borrado", "来源·code": "Fuente·code", "来源·Uncen": "Fuente·Uncen", "来源·AI": "Fuente·IA", "语义未跑": "Sem:sin ejecutar", "简介": "Sinopsis", "标签": "Etiquetas", "作者": "Autor", "年份": "Año", "制作": "Estudio", "双击修改": "Doble clic para editar", "本地打开大图": "Abrir imagen completa", "(待接入)": "(pendiente)", "检测中…（逐个真实请求）": "Comprobando… (una petición real cada uno)", "检测完成": "Comprobación lista", "至少勾一个站点": "Marca al menos un sitio", "先选条目": "Selecciona elementos primero", "已有任务在跑": "Ya hay una tarea en ejecución", "已清空": "Vaciado", "当前：已设置": "Actual: definido", "当前：未设置": "Actual: sin definir", "已保存": "Guardado", "保存成功": "Guardado", "文件": "Archivo", "逗号分隔": "separado por comas", "确认": "Confirmar", "是": "Sí", "否": "No", "有": "OK", "缺": "Parcial", "有误": "Revisar", "无": "Ninguno", "启用": "Activo", "类型": "Tipo", "入口": "Entrada", "状态": "Estado", "时间": "Hora", "原因": "Motivo", "名字": "Nombre", "模型缺失": "modelo ausente", "未选中条目": "Nada seleccionado", "【删除】共 {n} 个目录（含视频）→ Windows 回收站\n\n⚠ NAS 网络路径，实际是【永久删除】，不可恢复。\n\n继续？": "[Eliminar] {n} carpetas (con vídeos) → Papelera de Windows\n\n⚠ En una ruta de red NAS esto es un borrado PERMANENTE, irrecuperable.\n\n¿Continuar?", "再次确认：永久删除这 {n} 个？": "Confirma de nuevo: ¿borrar permanentemente estos {n}?", "【建库】会清空并重建全部档案！\n当前所有行的状态/语义/元信息都会被重置，旧记录先进入“档案”。\n\n确定继续？": "¡[Reconstruir] borra y rehace todos los registros!\nSe reinician estado/semántica/metadatos de todas las filas; los antiguos pasan a \"Archivo\".\n\n¿Continuar?", "请再次确认：真的要【建库】吗？": "Confirma otra vez: ¿realmente reconstruir?", "将对【全部/有简介但没标签】的条目按简介补标签，继续？": "Se añadirán etiquetas desde la sinopsis para [todo / con sinopsis sin etiquetas]. ¿Continuar?", "将对【全部/无/有误】的条目抽帧看视频（较慢），继续？": "Se extraerán fotogramas y se analizarán vídeos para [todo / ninguno / revisar] (lento). ¿Continuar?", "删除选中条目的封面图（poster.jpg 等）+ 缩略图缓存，继续？": "Se borrarán las portadas (poster.jpg etc.) y la caché de miniaturas de lo seleccionado. ¿Continuar?", "已保存 (%d 字符)": "Guardado (%d caracteres)", "已保存：": "Guardado: ", "已删除封面/缩略图 %s 个文件": "Eliminados %s archivos de portada/miniatura", "已重建 %s 张缩略图": "Regeneradas %s miniaturas", "切换失败（重启后生效）：": "Fallo al cambiar (tras reiniciar): ", "已切换到": "Cambiado a ", "访问地址: ": "URL: ", "局域网已开启: ": "LAN activada: ", "通过": "OK", "不通过": "FALLO"}, "zh-Hant": {"(待接入)": "(待接入)", "(未跑)": "(未跑)", "AI 看片": "AI 看片", "AI 补全": "AI 補全", "AI组(0=全部)": "AI組(0=全部)", "← 上一页": "← 上一頁", "① 全流程": "① 全流程", "⟳ 刷新列表": "⟳ 刷新列表", "上限(0=全部)": "上限(0=全部)", "下一页 →": "下一頁 →", "作者": "作者", "保存": "保存", "保存成功": "保存成功", "保存设置": "保存設置", "停止": "停止", "先选条目": "先選條目", "全选本页": "全選本頁", "关闭": "關閉", "删除": "刪除", "删除封面图": "刪除封面圖", "制作": "製作", "刷新": "刷新", "刷新缩略图": "刷新縮略圖", "双击修改": "雙擊修改", "取消选择": "取消選擇", "否": "否", "增量扫描": "增量掃描", "媒体库": "媒體庫", "局域网": "局域網", "已保存": "已保存", "已有任务在跑": "已有任務在跑", "已清空": "已清空", "已选": "已選", "年份": "年份", "建库": "建庫", "当前：已设置": "當前：已設置", "当前：未设置": "當前：未設置", "总数": "總數", "打开目录": "打開目錄", "抓取": "抓取", "搜索 名字/标题/番号": "搜索 名字/標題/番號", "文件": "文件", "无": "無", "是": "是", "有": "有", "有误": "有誤", "本地打开大图": "本地打開大圖", "条": "條", "来源": "來源", "来源:全部": "來源:全部", "来源·AI": "來源·AI", "来源·Uncen": "來源·Uncen", "来源·code": "來源·code", "标签": "標籤", "标题 / 元信息": "標題 / 元信息", "档案": "檔案", "检测中…（逐个真实请求）": "檢測中…（逐個真實請求）", "检测完成": "檢測完成", "检测连通性": "檢測連通性", "深度自检": "深度自檢", "模型": "模型", "模式": "模式", "清空": "清空", "清空队列": "清空隊列", "番号": "番號", "确认": "確認", "站点": "站點", "站点设置": "站點設置", "端口": "端口", "筛选": "篩選", "简介": "簡介", "缩略图": "縮略圖", "缺": "缺", "至少勾一个站点": "至少勾一個站點", "语义": "語義", "语义:全部": "語義:全部", "语义未跑": "語義未跑", "语义组(0=全部)": "語義組(0=全部)", "语义识别": "語義識別", "说明": "說明", "质量": "質量", "质量:全部": "質量:全部", "质量·删除": "質量·刪除", "质量·无": "質量·無", "质量·有": "質量·有", "质量·有误": "質量·有誤", "质量·缺": "質量·缺", "跑队列": "跑隊列", "选择": "選擇", "逗号分隔": "逗號分隔", "重跑·AI": "重跑·AI", "重跑·全流程": "重跑·全流程", "重跑·网站": "重跑·網站", "重跑队列": "重跑隊列", "队列": "隊列", "启用": "啓用", "类型": "類型", "入口": "入口", "状态": "狀態", "时间": "時間", "原因": "原因", "名字": "名字", "模型缺失": "模型缺失", "未选中条目": "未選中條目", "【删除】共 {n} 个目录（含视频）→ Windows 回收站\n\n⚠ NAS 网络路径，实际是【永久删除】，不可恢复。\n\n继续？": "【刪除】共 {n} 個目錄（含視頻）→ Windows 回收站\n\n⚠ NAS 網絡路徑，實際是【永久刪除】，不可恢復。\n\n繼續？", "再次确认：永久删除这 {n} 个？": "再次確認：永久刪除這 {n} 個？", "【建库】会清空并重建全部档案！\n当前所有行的状态/语义/元信息都会被重置，旧记录先进入“档案”。\n\n确定继续？": "【建庫】會清空並重建全部檔案！\n當前所有行的狀態/語義/元信息都會被重置，舊記錄先進入「檔案」。\n\n確定繼續？", "请再次确认：真的要【建库】吗？": "請再次確認：真的要【建庫】嗎？", "将对【全部/有简介但没标签】的条目按简介补标签，继续？": "將對【全部/有簡介但沒標籤】的條目按簡介補標籤，繼續？", "将对【全部/无/有误】的条目抽帧看视频（较慢），继续？": "將對【全部/無/有誤】的條目抽幀看視頻（較慢），繼續？", "删除选中条目的封面图（poster.jpg 等）+ 缩略图缓存，继续？": "刪除選中條目的封面圖（poster.jpg 等）+ 縮略圖緩存，繼續？", "已保存 (%d 字符)": "已保存 (%d 字符)", "已保存：": "已保存：", "已删除封面/缩略图 %s 个文件": "已刪除封面/縮略圖 %s 個文件", "已重建 %s 张缩略图": "已重建 %s 張縮略圖", "切换失败（重启后生效）：": "切換失敗（重啓後生效）：", "已切换到": "已切換到", "访问地址: ": "訪問地址: ", "局域网已开启: ": "局域網已開啓: ", "通过": "通過", "不通过": "不通過"}};
// 动态文案：整串模式的按语言替换（就这几条，写死在表里）
DICT.en['整理嵌套目录']='Flatten nested folders';
DICT.en['排序:编号']='Sort: ID';
DICT.en['排序:入库时间']='Sort: added';
DICT.en['排序:最后编辑']='Sort: edited';
DICT.en['正序 ↑']='Asc ↑';
DICT.en['倒序 ↓']='Desc ↓';
DICT.en['继续整理']='Continue';
DICT.en['回滚上一次整理']='Undo last flatten';
DICT.en['取消']='Cancel';
const PATS={
 'en':[[/^共\s*(\d+)\s*条$/, 'Total $1'], [/^第\s*(\d+)\s*\/\s*(\d+)\s*页$/, 'Page $1 / $2'], [/^通过\s*/, 'OK '], [/^不通过\s*/, 'FAIL ']],
 'ja':[[/^共\s*(\d+)\s*条$/, '全 $1 件'], [/^第\s*(\d+)\s*\/\s*(\d+)\s*页$/, '$1 / $2 ページ'], [/^通过\s*/, 'OK '], [/^不通过\s*/, 'NG ']],
 'es':[[/^共\s*(\d+)\s*条$/, 'Total $1'], [/^第\s*(\d+)\s*\/\s*(\d+)\s*页$/, 'Página $1 / $2'], [/^通过\s*/, 'OK '], [/^不通过\s*/, 'FALLO ']],
 'zh-Hant':[[/^共\s*(\d+)\s*条$/, '共 $1 筆'], [/^第\s*(\d+)\s*\/\s*(\d+)\s*页$/, '第 $1 / $2 頁'], [/^通过\s*/, '通過 '], [/^不通过\s*/, '不通過 ']]
};
// 一行里含多个标签（作者: x ｜ 年份: y）→ 子串替换
const SUBS={
 'en':{'简介:':'Plot: ','标签:':'Tags: ','作者:':'Author: ','年份:':'Year: ','制作:':'Studio: ',' ｜ ':' | '},
 'ja':{'简介:':'あらすじ: ','标签:':'タグ: ','作者:':'出演: ','年份:':'年: ','制作:':'メーカー: ',' ｜ ':' | '},
 'es':{'简介:':'Sinopsis: ','标签:':'Etiquetas: ','作者:':'Autor: ','年份:':'Año: ','制作:':'Estudio: ',' ｜ ':' | '},
 'zh-Hant':{'简介:':'簡介:','标签:':'標籤:','作者:':'作者:','年份:':'年份:','制作:':'製作:',' ｜ ':' | '}
};
let LANG='zh';
function T(s){ if(LANG==='zh'||!s)return s; const d=DICT[LANG]||{}; return (s in d)?d[s]:s; }
// 带 {n} 占位符的句子
function TN(s,n){ const t=T(s); return t.replace('{n}',n); }
function F(s){
  if(LANG==='zh'||!s)return s;
  const d=DICT[LANG]||{};
  if(s in d)return d[s];
  const ps=PATS[LANG]||[];
  for(let i=0;i<ps.length;i++){ if(ps[i][0].test(s))return s.replace(ps[i][0],ps[i][1]); }
  const sb=SUBS[LANG]; let out=s,hit=false;
  for(const k in sb){ if(out.indexOf(k)>=0){ out=out.split(k).join(sb[k]); hit=true; } }
  return hit?out:s;
}
function applyLang(){
  if(LANG==='zh')return;
  const M='button,span,div,td,th,option,label,a,li,p,b,strong,small';
  document.querySelectorAll(M).forEach(function(el){
    if(el.children.length)return;                 // 只动叶子节点
    if(el.id==='b-lang'||el.tagName==='OPTION'&&el.parentNode&&el.parentNode.id==='b-lang')return;
    const s=(el.textContent||'').trim();
    if(!s||s.length>140)return;
    const t=F(s);
    if(t!==s)el.textContent=el.textContent.replace(s,t);
  });
  document.querySelectorAll('input[placeholder]').forEach(function(el){
    const s=(el.placeholder||'').trim(); if(s){const t=F(s); if(t!==s)el.placeholder=t;}});
  document.documentElement.lang=(LANG==='zh-Hant')?'zh-Hant':LANG;
}
let _i18t=null;
try{new MutationObserver(function(){
  if(LANG==='zh')return; clearTimeout(_i18t);
  _i18t=setTimeout(applyLang,150);
}).observe(document.body,{childList:true,subtree:true});}catch(e){}
let OUT_LANG='zh';
async function setUILang(v){await fetch('/api/lang?lang='+encodeURIComponent(v),{method:'POST'});location.reload();}
(async()=>{try{const r=await(await fetch('/api/lang')).json();
  LANG=r.lang||'zh'; OUT_LANG=r.out_lang||LANG;
  const b=document.getElementById('b-lang'); if(b)b.value=LANG;
  const o=document.getElementById('cfg-outlang'); if(o)o.value=r.out_lang||'';
  applyLang();}catch(e){}})();
const _bl=document.getElementById('b-lang'); if(_bl)_bl.onchange=()=>setUILang(_bl.value);
const _co=document.getElementById('cfg-outlang');
if(_co)_co.onchange=async()=>{await fetch('/api/lang?out_lang='+encodeURIComponent(_co.value),{method:'POST'});};
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
    td.innerHTML=r.ok?('<span style="color:#39d98a">通过 '+r.status+' · '+r.ms+'ms'
        +(r.reason&&r.reason!=='ok'?' <span class="muted">('+r.reason+')</span>':'')+'</span>')
                     :('<span style="color:#ff6b6b">不通过 '+(r.err||r.reason||r.status)+' · '+r.ms+'ms</span>');}
  $('#st-info').textContent='检测完成';};
$('#st-deep').onclick=async()=>{
  $('#st-info').textContent=T('检测中…（逐个真实请求）');
  const d=await(await fetch('/api/sites/deep',{method:'POST'})).json();
  for(const k in d.res){const r=d.res[k];const td=document.querySelector('.st-'+k);
    if(!td)continue;
    td.innerHTML=r.ok?('<span style="color:#39d98a">命中 '+esc(r.code)+' · '+r.fields+' 字段 · '+r.ms+'ms</span>')
                     :('<span style="color:#ff6b6b">未命中 '+(r.reason||'')+' · '+r.ms+'ms</span>');}
  $('#st-info').textContent='检测完成';};
$('#st-save').onclick=async()=>{
  const on=[...document.querySelectorAll('.ston')].filter(c=>c.checked).map(c=>c.dataset.k).join(',');
  if(!on)return alert(T('至少勾一个站点'));
  const d=await(await fetch('/api/sites?on='+encodeURIComponent(on),{method:'POST'})).json();
  $('#st-info').textContent='已保存：'+d.on.join(', ');};
$('#b-qrun').onclick=()=>post('/api/queue/run');
$('#b-qclear').onclick=async()=>{await post('/api/queue/clear');$('#qcount').textContent=0;};
$('#b-stop').onclick=()=>post('/api/stop');
$('#b-refresh').onclick=()=>list();
$('#b-reload').onclick=()=>list();   // 只重拉列表，不动整页
$('#a-thumbs').onclick=async()=>{if(!SEL.size)return alert(T('先选条目'));
  const r=await post('/api/thumbs/refresh?ids='+[...SEL].join(','));
  THUMBV++;await list();console.log('已重建 %s 张缩略图',r.n);};
$('#a-coverdel').onclick=async()=>{if(!SEL.size)return alert(T('先选条目'));
  if(!confirm(T('将删除选中条目的封面图（poster.jpg 等）+ 缩略图缓存，继续？')))return;
  const r=await post('/api/covers/drop?ids='+[...SEL].join(','));
  THUMBV++;await list();console.log('已删除封面/缩略图 %s 个文件',r.n);};
$('#b-archive').onclick=async()=>{const d=await(await fetch('/api/archive?limit=300')).json();
  $('#md2-body').innerHTML='<table><thead><tr><th>时间</th><th>原因</th><th>id</th><th>名字</th><th>质量</th><th>来源</th></tr></thead><tbody>'
   +d.rows.map(r=>'<tr><td>'+esc(r.archived_at)+'</td><td>'+esc(r.reason)+'</td><td>'+r.id+'</td><td>'+esc(r.name)+'</td><td>'+esc(r.quality)+'</td><td>'+esc(r.source)+'</td></tr>').join('')
   +'</tbody></table>';$('#modal2').classList.add('on');};
$('#b-prev').onclick=()=>{off=Math.max(0,off-lim);list();};
$('#b-next').onclick=()=>{off+=lim;list();};
$('#pjump').onchange=e=>{off=parseInt(e.target.value,10)||0;list();};
['f-quality','f-source','f-kind','f-sort','f-order'].forEach(i=>$('#'+i).onchange=()=>{off=0;list();});
$('#f-q').onkeydown=e=>{if(e.key==='Enter'){off=0;list();}};
const rawList=v=>{if(v==null)return '';let a=v;try{a=JSON.parse(v)}catch(e){}
  if(Array.isArray(a))return a.join(',');return String(v);};
setInterval(()=>{job();cfg();},2000);
function stripWinOnly(){                 // 非 Windows：缩略图不再包「本机打开大图」链接
  if(PLAT==='win')return;
  document.querySelectorAll('a.imgopen').forEach(function(a){
    const im=a.querySelector('img'); if(im){a.replaceWith(im);} });
}
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
    # 启动即迁移：补齐 media.ads / ads_ts（否则 /api/items 会报 no such column）
    try:
        _ensure_media(mlib.db())
    except Exception as _e:
        print('    migrate: %s' % str(_e)[:120])
    try:
        _bd = mlib.cfg_get('bak_keep_days')
        _bd = 7 if _bd in (None, '') else int(_bd)
        _st = repair.set_backup_policy(mlib.cfg_get('bak_dir') or None, _bd)
        repair.purge_backups(lambda m: print('    ' + m))
        print('    backup : %s  (%d 天)' % (_st['dir'], _st['days']))
    except Exception as _e:
        print('    backup : %s' % str(_e)[:120])
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

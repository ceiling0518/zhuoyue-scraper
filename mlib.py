import os
# -*- coding: utf-8 -*-
"""
mlib.py -- media library core: snapshot / diff / scrape(website|ai) / nfo write.

DB: media/mlib.db (sqlite)
  items(id, path, name, code, status, site, title, plot, tags, actors,
        cover, images, nfo, first_seen, last_scan, last_update, note, present)
    status: 'website' | 'ai' | 'empty'
  runs(id, ts, kind, added, removed, total, note)

CLI:
  python mlib.py scan  [--root R]
  python mlib.py list  [--status empty|website|ai] [--limit N]
  python mlib.py run   <id> [--mode website|ai|auto] [--force]
  python mlib.py auto  [--limit N] [--mode website|ai|auto] [--workers N]
  python mlib.py stats
"""
import argparse, base64, io, json, os, re, shutil, sqlite3, subprocess, sys, time
import requests
try:
    import zhconv                     # pure-python 繁->简 converter
except Exception:
    zhconv = None

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import javscrape as JS            # noqa: E402
import cover as COVER             # noqa: E402

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding='utf-8')
    except Exception:
        pass

IS_WIN = (os.name == 'nt')        # 平台判定：容器/Linux 下自动降级
# 运行数据目录：MS_DB（容器里挂 /data）；不设则跟随脚本目录（本地行为不变）
DB_DIR = os.environ.get('MS_DB') or HERE
DB_PATH = os.path.join(DB_DIR, 'mlib.db')
CACHE_DIR = os.path.join(DB_DIR, 'cache')
NFO_DIR = os.path.join(DB_DIR, 'nfo_out')
COOKIE_FILE = os.path.join(DB_DIR, 'javdb_cookie.txt')
# Ollama：容器里指向宿主 host.docker.internal
OLLAMA = (os.environ.get('MS_OLLAMA') or 'http://127.0.0.1:11434').rstrip('/')
FFMPEG = os.environ.get('FFMPEG', 'ffmpeg')
FFPROBE = os.environ.get('FFPROBE', 'ffprobe')
AI_MODEL = 'qwen38-27b-uncen'
# User chose 30m: keep the uncensored 27B resident between items so we don't pay
# ~40s reload per call. Uses `unload_ai()` to release the card at the end of a run.
AI_KEEP_ALIVE = '30m'
ROOT_DEFAULT = os.environ.get('MS_ROOT') or r'\\CeilingNAS\usbshare1\movie'
VIDEO_EXT = COVER.VIDEO_EXT

for d in (DB_DIR, CACHE_DIR, NFO_DIR):
    os.makedirs(d, exist_ok=True)


# ------------------------------------------------------------------ db
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    # WAL + busy_timeout so two waves can write concurrently without lock errors
    try:
        c.execute('PRAGMA journal_mode=WAL')
        c.execute('PRAGMA busy_timeout=30000')
        c.execute('PRAGMA synchronous=NORMAL')
    except Exception:
        pass
    c.execute("""CREATE TABLE IF NOT EXISTS items(
        id INTEGER PRIMARY KEY,
        path TEXT UNIQUE, name TEXT, code TEXT,
        status TEXT DEFAULT 'empty', site TEXT,
        title TEXT, plot TEXT, tags TEXT, actors TEXT,
        cover TEXT, images INTEGER DEFAULT 0, nfo INTEGER DEFAULT 0,
        studio TEXT,
        first_seen TEXT, last_scan TEXT, last_update TEXT,
        note TEXT, present INTEGER DEFAULT 1)""")
    c.execute("""CREATE TABLE IF NOT EXISTS runs(
        id INTEGER PRIMARY KEY, ts TEXT, kind TEXT,
        added INTEGER, removed INTEGER, total INTEGER, note TEXT)""")
    cols = {r[1] for r in c.execute('PRAGMA table_info(items)')}
    for col, typ in (('studio', 'TEXT'), ('sem_kind', 'TEXT'), ('sem_code', 'TEXT'),
                     ('quality', 'TEXT'), ('source', 'TEXT'), ('year', 'TEXT'), ('kind', 'TEXT'),
                     ('sem_manual', 'INTEGER DEFAULT 0')):
        if col not in cols:
            c.execute('ALTER TABLE items ADD COLUMN %s %s' % (col, typ))
    c.execute("""CREATE TABLE IF NOT EXISTS archive(
        ref INTEGER PRIMARY KEY AUTOINCREMENT,
        id INTEGER, path TEXT, name TEXT, code TEXT, title TEXT,
        quality TEXT, source TEXT, archived_at TEXT, reason TEXT)""")
    c.execute('CREATE TABLE IF NOT EXISTS config(k TEXT PRIMARY KEY, v TEXT)')
    c.execute('CREATE TABLE IF NOT EXISTS health(item_id INTEGER, file TEXT, cls TEXT, reason TEXT, tier TEXT, size INTEGER, checked_at TEXT, PRIMARY KEY(item_id,file))')
    c.execute('''CREATE TABLE IF NOT EXISTS queue(
        item_id INTEGER PRIMARY KEY, mode TEXT, added_at TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS joblog(
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, line TEXT)''')
    c.commit()
    return c


def cfg_get(k, default=None):
    r = db().execute('SELECT v FROM config WHERE k=?', (k,)).fetchone()
    return r['v'] if r else default


def cfg_set(k, v):
    c = db()
    c.execute('INSERT INTO config(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v',
              (k, str(v)))
    c.commit()


def scan_root():
    return cfg_get('root', ROOT_DEFAULT)


# ---------------------------------------------------------------- 语言
# 两套独立设置：
#   config.lang     = 界面语言（前端 i18n 用）
#   config.out_lang = 元数据输出语言（模型 prompt 用）；留空 = 跟随界面语言
LANGS = ('zh', 'en', 'ja', 'es', 'zh-Hant')


def norm_lang(v, default='zh'):
    """把任意输入规整为 LANGS 中的规范代码。"""
    s = str(v or '').strip().lower().replace('_', '-')
    if not s:
        return default
    if s.startswith('en'):
        return 'en'
    if s.startswith('ja') or s.startswith('jp'):
        return 'ja'
    if s.startswith('es'):
        return 'es'
    if s in ('zh-hant', 'zh-tw', 'zh-hk', 'tw', 'hk', 'hant', 'zhhant'):
        return 'zh-Hant'
    if s.startswith('zh'):
        return 'zh'
    return default


def ui_lang():
    """界面语言（config.lang）。"""
    return norm_lang(cfg_get('lang'), 'zh')


LANG_RULE = {
    'zh': ('语言规则：一律输出 **简体中文**；若原文是**日文则保留日文原文**（不要翻译）；'
           '**繁体中文转成简体**；其它语言**翻译成简体中文**。'),
    'en': ('Language rule: write EVERYTHING in **English** - title, plot and tags. '
           'If the source text is Japanese, **translate it into English** '
           '(do not keep the Japanese original).'),
    'ja': ('言語ルール：**日本語**で出力する（タイトル・あらすじ・タグのすべて）。'
           '原文が中国語の場合は**日本語に翻訳**する（中国語のまま残さない）。'),
    'es': ('Regla de idioma: escribe TODO en **español** (título, sinopsis y etiquetas). '
           'Si el texto original está en japonés o chino, **tradúcelo al español**.'),
    'zh-Hant': ('語言規則：一律輸出 **繁體中文**；若原文是**日文則保留日文原文**（不要翻譯）；'
                '**簡體中文轉成繁體**；其它語言**翻譯成繁體中文**。'),
}
LANG_TAGS = {
    'zh': '5-8 个中文标签',
    'en': '5-8 English tags',
    'ja': '5-8 個の日本語タグ',
    'es': '5-8 etiquetas en español',
    'zh-Hant': '5-8 個繁體中文標籤',
}


JAVDB_DEFAULT = 'https://javdb.com'


def javdb_domain():
    """javdb 入口域名：环境变量 MS_JAVDB > config.javdb_domain > 默认。"""
    v = os.environ.get('MS_JAVDB') or cfg_get('javdb_domain') or JAVDB_DEFAULT
    return str(v).strip().rstrip('/')


def out_lang():
    """模型输出语言：config.out_lang 优先；留空则跟随界面语言。"""
    v = cfg_get('out_lang')
    if v is None or not str(v).strip():
        v = cfg_get('lang')
    return norm_lang(v, 'zh')


def lang_rule():
    return LANG_RULE.get(out_lang(), LANG_RULE['zh'])


def lang_tags():
    return LANG_TAGS.get(out_lang(), LANG_TAGS['zh'])


def ai_model():
    return cfg_get('model', AI_MODEL)


def now():
    return time.strftime('%Y-%m-%d %H:%M:%S')


# ------------------------------------------------------------------ scan
def read_nfo(folder):
    """Parse movie.nfo in a folder -> dict, or None if absent."""
    p = None
    for _n in ('movie.nfo', 'tvshow.nfo'):
        if os.path.exists(os.path.join(folder, _n)):
            p = os.path.join(folder, _n)
            break
    if not p:
        return None
    try:
        x = io.open(p, encoding='utf-8', errors='replace').read()
    except Exception:
        return None
    d = {}
    for tag in ('title', 'plot', 'year', 'studio', 'num'):
        m = re.search(r'<%s>(.*?)</%s>' % (tag, tag), x, re.S)
        d[tag] = JS._clean(m.group(1)) if m else ''
    d['tags'] = [JS._clean(m) for m in re.findall(r'<tag>(.*?)</tag>', x, re.S)]
    d['actors'] = [JS._clean(m) for m in re.findall(r'<actor>\s*<name>(.*?)</name>', x, re.S)]
    m = re.search(r'<source>([^<]+)</source>', x)
    if m:
        d['source'] = m.group(1).strip()
    else:
        m = re.search(r'<uniqueid type="([^"]*)"', x)
        t = (m.group(1) if m else '').lower()
        d['source'] = {'javbus': 'code', 'javdb': 'code', 'missav': 'Uncen'}.get(t, 'AI')
    return d


def _clean_tags(tags):
    """Drop junk tag tokens like the literal [] / empty strings."""
    out = []
    for t in (tags or []):
        t = str(t).strip().strip('[]"\'')
        if not t or t in ('[]', 'null', 'None'):
            continue
        out.append(t)
    return out


def nfo_state(nfo, raw_name=''):
    """-> (quality, source).

    quality: 有   = 真标签 + 标题已清洗（不再等于原文件夹名）
             无   = 没有真标签 / 标题就是原文件夹名（=狗屁不通）-> 当作没刮
             有误 = nfo 存在但缺标题
    """
    if not nfo:
        return '', ''
    title = (nfo.get('title') or '').strip()
    plot = (nfo.get('plot') or '').strip()
    tags = _clean_tags(nfo.get('tags'))
    src = nfo.get('source') or 'AI'

    def sq(s):
        return re.sub(r'[^0-9A-Za-z\u3040-\u30ff\u4e00-\u9fff]+', '', s or '')

    rn = sq(raw_name)
    not_cleaned = bool(rn) and (sq(title) == rn or sq(plot) == rn)
    if not title:
        return '有误', src
    if not_cleaned:
        return '无', src
    if tags:
        return '有', src
    # 没 tag：网站线(code/Uncen)抓到了 -> 缺（默认完成，后期人工/AI 补 tag）
    return ('缺' if src in ('code', 'Uncen') else '无'), src


def folder_kind(path):
    """-> (kind, n_top_videos, subdirs_with_video).

    single   = 1 个视频
    series   = 多个视频（剧-集，平铺）
    nested   = 子目录里还有视频（多重嵌套 -> 识别不了，交人工）
    novideo  = 目录里没视频
    """
    try:
        names = os.listdir(path)
    except Exception:
        return 'novideo', 0, []
    vids = [n for n in names if os.path.splitext(n)[1].lower() in VIDEO_EXT]
    subs = []
    for n in names:
        p = os.path.join(path, n)
        if os.path.isdir(p):
            try:
                if any(os.path.splitext(x)[1].lower() in VIDEO_EXT for x in os.listdir(p)):
                    subs.append(n)
            except Exception:
                pass
    if subs:
        return 'nested', len(vids), subs
    if len(vids) > 1:
        return 'series', len(vids), []
    if len(vids) == 1:
        return 'single', 1, []
    return 'novideo', 0, []


def name_is_useless(name):
    """文件夹名提供不了信息（纯数字 / 太短）-> 只能靠 AI 看视频。"""
    s = re.sub(r'^[^@]*@', '', (name or '')).strip()
    core = re.sub(r'[^0-9A-Za-z\u4e00-\u9fff]', '', s)
    if len(core) <= 3:
        return True
    return bool(re.fullmatch(r'\d+', core))


def has_nested_videos(dirpath, depth=2):
    """目录结构异常（-> 有误）：① 内嵌目录里有视频；② 内嵌目录 ≥2 个。"""
    if depth <= 0 or not dirpath or not os.path.isdir(dirpath):
        return False
    try:
        names = os.listdir(dirpath)
    except Exception:
        return False
    subs = [n for n in names if os.path.isdir(os.path.join(dirpath, n))]
    if len(subs) >= 2:
        return True
    for n in subs:
        p = os.path.join(dirpath, n)
        try:
            if biggest_video(p):
                return True
        except Exception:
            pass
        if has_nested_videos(p, depth - 1):
            return True
    return False


def scan(root=None, mode='incr', dbc=None):
    """Snapshot the source tree + read each folder's movie.nfo.

    mode 'rebuild': archive ALL rows, then rebuild from scratch.
    mode 'incr'   : add/update changed; items gone from disk -> archive (present=0).
    Sets 质量 quality (有/有误/'') and 来源 source (code/Uncen/AI) from the nfo.
    """
    dbc = dbc or db()
    root = root or scan_root()
    ts = now()
    if mode == 'rebuild':
        dbc.execute("INSERT INTO archive(id,path,name,code,title,quality,source,archived_at,reason) "
                    "SELECT id,path,name,code,title,quality,source,?, 'rebuild' FROM items", (ts,))
        dbc.execute('DELETE FROM items')
        dbc.commit()
        known = {}
    else:
        known = {r['path']: r['id'] for r in dbc.execute('SELECT id, path FROM items')}
    seen, added, changed = set(), 0, 0
    if not os.path.isdir(root):
        return {'error': 'root unreachable: %s' % root}
    for name in sorted(os.listdir(root)):
        p = os.path.join(root, name)
        if not os.path.isdir(p):
            continue
        seen.add(p)
        code = JS.find_code(name) or ''
        nfo = read_nfo(p)
        q, src = nfo_state(nfo, name)
        if q == '有误':
            q = '无'                      # 建库：有误 全部还原成 无
        kk, _nv, _subs = folder_kind(p)
        if kk == 'nested':
            q = '有误'                    # 多重嵌套目录 -> 建档就判有误（人工调目录后重跑）
        nf = {}
        if nfo:
            nf = {'title': nfo.get('title', ''), 'plot': nfo.get('plot', ''),
                  'tags': json.dumps(nfo.get('tags') or [], ensure_ascii=False),
                  'actors': json.dumps(nfo.get('actors') or [], ensure_ascii=False),
                  'studio': nfo.get('studio', ''), 'year': nfo.get('year', '')}
        if p not in known:
            dbc.execute("""INSERT INTO items(path,name,code,status,quality,source,kind,title,plot,
                                             tags,actors,studio,year,first_seen,last_scan,present)
                           VALUES(?,?,?,'empty',?,?,?,?,?,?,?,?,?,?,?,1)""",
                        (p, name, code, q, src, kk, nf.get('title', ''), nf.get('plot', ''),
                         nf.get('tags', ''), nf.get('actors', ''), nf.get('studio', ''),
                         nf.get('year', ''), ts, ts))
            added += 1
        else:
            iid = known[p]
            r = dbc.execute('SELECT code,quality,source,kind,present FROM items WHERE id=?',
                            (iid,)).fetchone()
            if (r['code'] != code or (r['quality'] or '') != q or (r['source'] or '') != src
                    or (r['kind'] or '') != kk or r['present'] != 1):
                dbc.execute('UPDATE items SET code=?,quality=?,source=?,kind=?,present=1,last_scan=? '
                            'WHERE id=?', (code, q, src, kk, ts, iid))
                changed += 1
            else:
                dbc.execute('UPDATE items SET last_scan=? WHERE id=?', (ts, iid))
            if nfo:      # mirror the on-disk nfo into the row (list needs no NAS read)
                dbc.execute('UPDATE items SET title=?,plot=?,tags=?,actors=?,studio=?,year=? '
                            'WHERE id=?',
                            (nf['title'], nf['plot'], nf['tags'], nf['actors'],
                             nf['studio'], nf['year'], iid))
    removed = 0
    for p, i in known.items():
        if p not in seen:
            cur = dbc.execute('SELECT quality FROM items WHERE id=?', (i,)).fetchone()
            if (cur['quality'] or '') != '删除':
                dbc.execute("INSERT INTO archive(id,path,name,code,title,quality,source,archived_at,reason) "
                            "SELECT id,path,name,code,title,quality,source,?, 'removed' FROM items WHERE id=?",
                            (ts, i))
                removed += 1
            # 文件已不存在 -> 标 删除（留在列表里，可单独筛选查看）
            dbc.execute("UPDATE items SET quality='删除', present=1, last_scan=? WHERE id=?", (ts, i))
    dbc.execute('INSERT INTO runs(ts,kind,added,removed,total,note) VALUES(?,?,?,?,?,?)',
                (ts, 'scan:' + mode, added, removed, len(seen), root))
    dbc.commit()
    return {'mode': mode, 'added': added, 'removed': removed, 'changed': changed,
            'total': len(seen), 'root': root}


# ------------------------------------------------------------------ website scrape
def javdb_cookie():
    if os.path.exists(COOKIE_FILE):
        try:
            t = io.open(COOKIE_FILE, encoding='utf-8').read().strip()
            return t or None
        except Exception:
            return None
    return None


def parse_javdb_detail(html):
    # A login / Cloudflare-challenge page has none of the movie panel markup.
    # Never let those through -- we would otherwise store the login page title.
    if 'panel-block' not in html or 'movie-panel-info' not in html:
        return {}
    d = {}
    # 新版在 </strong> 与 <span class="value"> 之间多了 &nbsp; ——旧正则 \s* 匹配不到，整段都解析不出来
    for m in re.finditer(r'<div class="panel-block[^"]*">\s*<strong>([^<]+)</strong>'
                         r'\s*(?:&nbsp;)?\s*<span class="value">(.*?)</span>',
                         html, re.S):
        key, val = (m.group(1).strip().strip(':：'),
                    JS._clean(m.group(2).replace('&nbsp;', ' ').replace('\xa0', ' ')))
        d[key] = val
    m = re.search(r'<img[^>]+class="video-cover"[^>]+src="([^"]+)"', html)
    if not m:
        m = re.search(r'<img[^>]+src="(https?://[^"]*jdbstatic[^"]+)"', html)
    cov = m.group(1) if m else ''
    out = {'source': 'javdb', 'cover': cov}
    keymap = {'番號': 'code', '日期': 'date', '時長': 'runtime', '導演': 'director',
              '片商': 'studio', '發行': 'label', '系列': 'series', '類別': 'genres',
              '演員': 'actors', '評分': 'rating'}
    for k, v in d.items():
        out[keymap.get(k, k)] = v
    m = re.search(r'<h2 class="title is-4"[^>]*>([^<]+)</h2>', html) or re.search(r'<title>([^<]+)</title>', html)
    out['title'] = JS._clean(m.group(1)) if m else ''
    out['title'] = re.sub(r'\s*\|\s*JavDB.*$', '', out['title']).strip()
    # javdb 改版后 panel-block 里的番号被标签拆开、正则抠不到 -> 从标题/正文兜底
    if not out.get('code'):
        mm = (re.match(r'^\s*([A-Za-z]{2,8}[-_ ]?\d{2,6})\b', out['title'])
              or re.search(r'\b([A-Z]{2,8}-\d{2,6})\b', html))
        if mm:
            out['code'] = re.sub(r'[_ ]', '-', mm.group(1)).upper()
    return out


def fetch_javdb(code, cookie=None):
    cookie = cookie or javdb_cookie()
    if not cookie:
        return None
    h = dict(JS.HDRS)
    h['Cookie'] = cookie
    s = JS.new_session()
    try:
        r = s.get('%s/search?q=%s&f=all' % (javdb_domain(), code), proxies=JS.PROXIES, timeout=40, headers=h)
        # 站点改版后：外层 <a title="片名"> 不再带番号，番号移到内层元素里。
        # 外层 title + 内层文本 一起收进来匹配，避免漏掉。
        items = []
        for _href, _attrs, _inner in re.findall(
                r'<a href="(/v/[A-Za-z0-9]+)"([^>]*)>(.*?)</a>', r.text, re.S):
            _mt = re.search(r'title="([^"]*)"', _attrs)
            _ttl = _mt.group(1) if _mt else ''
            _txt = re.sub(r'<[^>]+>', ' ', _inner)
            items.append((_href, (_ttl + ' ' + _txt).strip()))
        if not items:
            return None
        # Require a CONFIDENT match -- attaching the wrong film's metadata is
        # worse than attaching none. Compare the flattened code and, for FC2 and
        # similar, the numeric id.
        flat = code.upper().replace('-', '').replace('_', '').replace(' ', '')
        mnum = re.search(r'(\d{5,})', code)
        numid = mnum.group(1) if mnum else ''

        def norm(x):
            return x.upper().replace('-', '').replace('_', '').replace(' ', '')

        pick = None
        for href, title in items:
            if flat in norm(title):
                pick = (href, title)
                break
        if pick is None and numid:
            for href, title in items:
                if numid in title:
                    pick = (href, title)
                    break
        if pick is None:
            sys.stderr.write('  javdb: no confident match for %s (hits=%d)\n' % (code, len(items)))
            return None
        time.sleep(1)
        r2 = s.get('%s' % javdb_domain() + pick[0], proxies=JS.PROXIES, timeout=40, headers=h)
        d = parse_javdb_detail(r2.text)
        if not d.get('code'):
            sys.stderr.write('  javdb: detail page was not a movie page (login/challenge) for %s\n' % code)
            return None
        if norm(d['code']) != norm(code):
            sys.stderr.write('  javdb: detail code %s != %s\n' % (d.get('code'), code))
            return None
        d['source'] = 'javdb'
        d['url'] = 'https://javdb.com' + pick[0]
        return d
    except Exception as e:
        sys.stderr.write('  javdb err %s\n' % str(e)[:80])
        return None


# ------------------------------------------------------------------ 站点管理
# (key, 名称, 类型, 入口, 是否已有专属解析器)
SITES = [
    ('javbus',      'JavBus',      '有码', 'https://www.javbus.com',          1),
    ('javdb',       'JavDB',       '有码', 'https://javdb.com',               1),
    ('jav321',      'JAV321',      '有码', 'https://www.jav321.com',          0),   # 源站 522（CF 源站超时），站点不可用，暂不接入
    ('missav',      'MissAV',      '无码', 'https://missav123.com',           1),
    ('avsox',       'avsox 系',    '无码', 'https://avsox.click',             1),
    ('123av',       '123AV',       '无码', 'https://123av.com',               1),
    ('njav',        'NJAV',        '无码', 'https://njav.tv',                 1),
    ('netflav',     'Netflav',     '聚合', 'https://netflav.com',             1),
    ('javtrailers', 'JAVTrailers', '资料', 'https://javtrailers.com',         1),
    ('fc2',         'FC2 官方',    'FC2',  'https://adult.contents.fc2.com',  0),
]
DEFAULT_SITES = 'javbus,javdb,missav,fc2'


def sites_enabled():
    v = cfg_get('sites') or DEFAULT_SITES
    return [x.strip() for x in v.split(',') if x.strip()]


def set_sites(keys):
    cfg_set('sites', ','.join([k for k in (keys or []) if k]))


# 浅查里算「真失败」的原因；cloudflare?/spa-shell 只作提示，以深度自检为准
FAIL_REASONS = ('geo-block', 'origin-down', 'not-found', 'no-response', 'unknown')


def _classify(html, status):
    """把一次响应归类成可读原因（给界面显示用）。"""
    t = html or ''
    if 'prohibited in the country' in t or 'Due to copyright restrictions' in t:
        return 'geo-block'          # 代理落地地区被封锁
    if 'Just a moment' in t or 'cf-challenge' in t or 'cf-turnstile' in t:
        return 'cloudflare?'
    if status == 0:
        return 'no-response'
    if status == 404:
        return 'not-found'
    if status >= 500:
        return 'origin-down'        # 源站挂（如 522）
    if status >= 400:
        return 'http-%d' % status
    if len(t) < 2000:
        return 'spa-shell'        # 200 但正文极小 = 被拦/需 JS
    return 'ok'


def site_url(key):
    """站点入口；javdb 允许用配置覆盖域名（换镜像/换区）。"""
    if key == 'javdb':
        return javdb_domain()
    return dict((s[0], s[3]) for s in SITES).get(key, '')


def site_check(key, timeout=14):
    """真实请求入口 URL，返回 ok/status/reason/ms。"""
    url = site_url(key)
    if not url:
        return {'ok': False, 'err': 'unknown'}
    t = time.time()
    try:
        r = JS.get(url, timeout=timeout)
    except Exception as e:
        return {'ok': False, 'reason': 'timeout', 'err': type(e).__name__,
                'ms': int((time.time() - t) * 1000)}
    sc = getattr(r, 'status_code', 0)
    reason = _classify(getattr(r, 'text', ''), sc)
    return {'ok': sc < 400 and reason not in FAIL_REASONS, 'status': sc, 'reason': reason,
            'ms': int((time.time() - t) * 1000)}


DEEP_CODE = {'有码': 'SSIS-001', '无码': 'HEYZO-3957', '聚合': 'SSIS-001',
             '资料': 'SSIS-001', 'FC2': 'FC2-PPV-4405704'}


def site_deep(key, timeout=40):
    """深度自检：用固定试号真跑一次该站抓取，回报命中与字段数。"""
    typ = dict((s[0], s[2]) for s in SITES).get(key, '')
    code = DEEP_CODE.get(typ, 'SSIS-001')
    item = {'code': code, 'sem_code': '', 'name': code, 'path': '', 'title': '', 'plot': ''}
    t = time.time()
    try:
        if key == 'javbus':
            d = JS.fetch(code)
            if d:
                d.setdefault('source', 'javbus')
        elif key == 'javdb':
            d = fetch_javdb(code)
        elif key == 'missav':
            d = _missav_title(item)
        elif key == 'fc2':
            d = fetch_fc2(item)
        else:
            fn = globals().get('fetch_%s' % key)
            d = fn(item) if fn else None
    except Exception as e:
        return {'ok': False, 'code': code, 'reason': type(e).__name__,
                'err': str(e)[:80], 'ms': int((time.time() - t) * 1000)}
    ms = int((time.time() - t) * 1000)
    if not d:
        return {'ok': False, 'code': code, 'reason': 'miss', 'ms': ms}
    return {'ok': True, 'code': code, 'ms': ms, 'fields': len(d),
            'title': str(d.get('title') or '')[:60], 'source': d.get('source') or key}

def fc2_num(item):
    """从 code / sem_code / name 里抠出 FC2 商品号（5-8 位）。"""
    for s in (item.get('code'), item.get('sem_code'), item.get('name')):
        m = re.search(r'fc2[-_ ]?ppv[-_ ]?(\d{5,8})', (s or ''), re.I)
        if m:
            return m.group(1)
    return ''


def fetch_fc2(item):
    """FC2 官方商品页：官方标题 + 样张封面 + 卖家 + 评分/价格。

    页面里的 JSON-LD(Product) 最全；查不到（未上架/已下架）返回 None。
    """
    if 'fc2' not in sites_enabled():
        return None
    num = fc2_num(item)
    if not num:
        return None
    u = 'https://adult.contents.fc2.com/article/%s/' % num
    try:
        r = JS.get(u, timeout=30)
    except Exception:
        return None
    if r is None or r.status_code != 200:
        return None
    t = r.text or ''
    m = re.search(r'<title[^>]*>(.*?)</title>', t, re.S | re.I)
    if m and '未找到' in m.group(1):
        return None                     # 官网没有这条（已下架）
    title, cover, seller, rating, price = '', '', '', '', ''
    for s in re.findall(r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', t, re.S | re.I):
        try:
            j = json.loads(s.strip())
        except Exception:
            continue
        if not isinstance(j, dict) or j.get('@type') != 'Product':
            continue
        title = (j.get('name') or '').strip()
        im = j.get('image') or {}
        cover = (im.get('url') if isinstance(im, dict) else '') or ''
        br = j.get('brand') or {}
        if isinstance(br, dict):
            seller = (br.get('name') or '').strip()
            if not seller:
                mm = re.search(r'/users/([^/]+)', br.get('url') or '')
                seller = mm.group(1) if mm else ''
        ar = j.get('aggregateRating') or {}
        if isinstance(ar, dict) and ar.get('ratingValue'):
            rating = '%s/%s(%s评)' % (ar.get('ratingValue'), ar.get('bestRating') or 5,
                                      ar.get('reviewCount') or 0)
        of = j.get('offers') or {}
        if isinstance(of, dict) and of.get('price'):
            price = '¥%s' % of.get('price')
        break
    if not title:
        mm = re.search(r'og:title"[^>]*content="([^"]+)"', t, re.I)
        if mm:
            title = re.sub(r'(?i)^\s*fc2[-_ ]?ppv[-_ ]?\d+\s*', '', mm.group(1)).strip()
    if not cover:
        mm = re.search(r'og:image"[^>]*content="([^"]+)"', t, re.I)
        if mm:
            cover = mm.group(1)
    if not title:
        return None
    samples = []
    for iu in re.findall(r'https?://storage\d*\.contents\.fc2\.com/[^"\']+?\.(?:jpg|jpeg|png)',
                         t, re.I):
        if iu != cover and iu not in samples:
            samples.append(iu)
    return {'source': 'fc2', 'code': 'FC2-PPV-%s' % num, 'title': title,
            'plot': title, 'studio': seller, 'cover': cover, 'samples': samples[:3],
            'url': u, 'genres': [], 'actors': [],
            'note': ' / '.join([x for x in (rating, price) if x])}


# ---------------------------------------------------------------- 番号后缀规则
# <番号>-<编号>[-<版本后缀>]：C=中文字幕 / J,X=破解 / HD=高画质 / SD=低画质 / 4K
CODE_SUFFIX_RE = re.compile(r'^(.+?\d)[-_ ](HD|SD|4K|C|J|X|U)$', re.I)


def split_code(code):
    """拆出 (基础番号, 版本后缀)。ABP-123-HD -> ('ABP-123', 'HD')。

    后缀只描述这一份拷贝的版本，不参与匹配 —— 搜站前要剥掉。
    """
    c = re.sub(r'\s+', ' ', (code or '').strip())
    m = CODE_SUFFIX_RE.match(c)
    if m:
        return m.group(1).upper(), m.group(2).upper()
    return c.upper(), ''


def fetch_123av(item):
    """123AV（无码）：详情页 HTML 里就有封面/时长/演员/制作商/类型。"""
    if '123av' not in sites_enabled():
        return None
    raw = (item.get('code') or item.get('sem_code') or '').strip()
    if not raw:
        return None
    slug = raw.lower()
    u = 'https://123av.com/cn/v/%s' % slug
    try:
        r = JS.get(u, timeout=30)
    except Exception:
        return None
    if r is None or r.status_code != 200:
        return None
    b = r.text or ''
    m = re.search(r'og:title"[^>]*content="([^"]+)"', b) or \
        re.search(r'<title[^>]*>(.*?)</title>', b, re.S | re.I)
    title = JS._clean(m.group(1)) if m else ''
    title = re.sub(r'\s*[-—|]\s*123AV.*$', '', title).strip()
    if len(title) < 2:
        return None
    m = re.search(r'(https://icdn\.123av\.me/img2/s\d+/[0-9a-f]+/%s/cover\.jpg[^"\'\s]*)'
                  % re.escape(slug), b)
    cover = m.group(1) if m else ''
    txt = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', b))
    dur = ''
    m = re.search(r'(?:時長|时长|時間)\s*([0-9]{1,2}:[0-9]{2}:[0-9]{2})', txt)
    if m:
        dur = m.group(1)
    rt = ''
    m = re.search(r'(\d{1,2}):(\d{2}):(\d{2})', dur)
    if m:
        rt = '%d分鐘' % (int(m.group(1)) * 60 + int(m.group(2)))
    actors = []
    m = re.search(r'(?:演員|演员)\s*(.{0,60}?)\s*(?:制作商|製作商)', txt)
    if m:
        actors = [x.strip() for x in re.split(r'[,、]', m.group(1)) if 1 < len(x.strip()) < 20]
    studio = ''
    m = re.search(r'(?:制作商|製作商)\s*(.{0,40}?)\s*(?:系列|類別|类别)', txt)
    if m:
        studio = m.group(1).strip()
    genres = []
    for g in re.findall(r'href="/cn/genres/[^"]*"[^>]*>\s*([^<]{1,24}?)\s*<', b):
        g = JS._clean(g)
        if g and g not in genres:
            genres.append(g)
    return {'source': '123av', 'code': raw.upper(), 'title': title, 'plot': '',
            'studio': studio, 'genres': genres[:12], 'actors': actors[:6],
            'runtime': rt, 'cover': cover, 'samples': [], 'url': u}


# ---------------------------------------------------- 剩余站点专属解析器
# 2026-10-12 实测（均走本地代理 JAV_PROXY）：
#   netflav     —— Next.js SPA，搜索结果的静态 HTML 里没有内容；前端调 /api98/video/advanceSearchVideo
#   javtrailers —— Nuxt，但搜索页是 SSR，卡片里直接有 /video/<slug> + 标题 + 封面 + 时长 + 日期
#   avsox       —— avsox.com 已成占位跳转页，真正站是 avsox.click（javu 家族），前端 POST /javu/data/api/*
#   njav        —— njav.tv 整体迁到 123av.com（页面只剩“We have moved to 123av.com”），故走 123AV 详情解析
#   jav321      —— 源站 522（Cloudflare 源站超时），站点自身不可用，暂不接入
NETFLAV = 'https://netflav.com'
NETFLAV_API = NETFLAV + '/api98'
JT = 'https://javtrailers.com'
AVSOX = 'https://avsox.click'
AVSOX_API = AVSOX + '/javu/data/api/'


def _flat(s):
    """番号归一化比较用：只留 A-Z0-9。"""
    return re.sub(r'[^0-9A-Za-z]', '', (s or '')).upper()


def _hms_to_min(s):
    """'2:27:00' -> '147'（分钟）。"""
    m = re.findall(r'\d+', s or '')
    if len(m) >= 3:
        return str(int(m[0]) * 60 + int(m[1]))
    if len(m) == 2:
        return str(int(m[0]))
    return ''


def _nf_parse(s):
    """netflav 详情的 'k|v|k|v' 串；空值会被省略，tags / previewImages 的值本身含 '|'。

    空格在值里被写成 '+'，故除 URL 外的值做 unquote_plus。
    """
    from urllib.parse import unquote_plus
    keys = ('_id', 'actors', 'code', 'description', 'duration', 'preview_hp',
            'previewImages', 'previewVideo', 'producer', 'premiumUrl', 'tags')
    toks = (s or '').split('|')
    d, i = {}, 0
    while i < len(toks):
        k = toks[i]
        if k in ('tags', 'previewImages'):
            vals, j = [], i + 1
            while j < len(toks) and toks[j] not in keys:
                vals.append(toks[j]); j += 1
            d[k] = [unquote_plus(v) for v in vals] if k == 'tags' else vals
            i = j
        elif k in keys:
            nxt = toks[i + 1] if i + 1 < len(toks) else None
            if nxt is not None and nxt in keys:
                d[k] = ''            # 空值=被省略，下一个 token 已是新 key
                i += 1
            else:
                v = nxt or ''
                if not v.startswith('http'):
                    v = unquote_plus(v)
                d[k] = v
                i += 2
        else:
            i += 1
    return d


def fetch_netflav(item):
    """Netflav（聚合）：调它自己的 JSON API（前端 fetch 用的那个）。"""
    if 'netflav' not in sites_enabled():
        return None
    code = (item.get('code') or item.get('sem_code') or '').strip()
    if not code:
        return None
    try:
        r = JS.get('%s/video/advanceSearchVideo?type=title&page=1&keyword=%s'
                   % (NETFLAV_API, requests.utils.quote(code)), timeout=30)
    except Exception:
        return None
    if r is None or r.status_code != 200:
        return None
    try:
        docs = ((r.json().get('result') or {}).get('docs')) or []
    except Exception:
        return None
    flat = _flat(code)
    hit = None
    for doc in docs:
        if _flat(doc.get('code') or doc.get('uCode')) == flat:
            hit = doc
            break
    if hit is None:
        return None                      # 不确认就不返回，避免挂错片
    title = (hit.get('title') or hit.get('title_zh') or hit.get('title_en') or '').strip()
    title = re.sub(r'^\s*' + re.escape(code) + r'\s*[-—:：]?\s*', '', title, flags=re.I).strip() or title
    actors = []
    for a in (hit.get('actors') or []):
        a = re.sub(r'^(jp|zh|en):', '', str(a)).strip()
        if a and a not in actors:
            actors.append(a)
    d = {'source': 'netflav', 'code': (hit.get('code') or code).upper(),
         'title': title, 'plot': '', 'studio': '', 'genres': [], 'actors': actors[:8],
         'runtime': '', 'date': (hit.get('sourceDate') or '')[:10],
         'cover': hit.get('preview') or '', 'samples': [],
         'url': '%s/video/%s' % (NETFLAV, hit.get('videoId') or '')}
    vid = hit.get('videoId')
    if vid:
        try:
            r2 = JS.get('%s/video/v3/retrieveVideo/%s' % (NETFLAV_API, vid), timeout=30)
            if r2 is not None and r2.status_code == 200:
                dd = _nf_parse(r2.json().get('result') or '')
                if dd.get('description'):
                    d['title'] = dd['description'].strip()
                if dd.get('producer'):
                    d['studio'] = dd['producer']
                if dd.get('duration'):
                    d['runtime'] = dd['duration']
                gs = []
                for x in (dd.get('tags') or []):
                    if ':' in x or not x or x in ('有修正', '有碼', '有码', '無修正', '无码', 'Censored'):
                        continue
                    if x not in gs:
                        gs.append(x)
                d['genres'] = gs[:12]
                if dd.get('preview_hp'):
                    d['cover'] = dd['preview_hp']
                if dd.get('previewImages'):
                    d['samples'] = dd['previewImages'][:6]
        except Exception as e:
            sys.stderr.write('  netflav detail err %s\n' % str(e)[:80])
    # 去掉标题开头的番号（详情标题也带）
    d['title'] = (re.sub(r'^\s*' + re.escape(code) + r'\s*[-—:：]?\s*', '', d['title'],
                         flags=re.I).strip() or d['title'])
    return d if d.get('title') else None


def fetch_javtrailers(item):
    """JavTrailers（资料）：搜索页 SSR，卡片即含番号/标题/封面/时长/日期。"""
    if 'javtrailers' not in sites_enabled():
        return None
    code = (item.get('code') or item.get('sem_code') or '').strip()
    if not code:
        return None
    try:
        r = JS.get('%s/search/%s' % (JT, requests.utils.quote(code)), timeout=30)
    except Exception:
        return None
    if r is None or r.status_code != 200:
        return None
    flat = _flat(code)
    for m in re.finditer(r'<a href="(/video/([a-z0-9]+))" class="video-link"(.*?)</a>',
                         r.text, re.S):
        href, body = m.group(1), m.group(3)
        mt = re.search(r'class="[^"]*vid-title[^"]*">(.*?)</p>', body, re.S)
        if not mt:
            continue
        full = (JS._clean(mt.group(1)).replace('&#39;', "'").replace('&quot;', '"')
                .replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>'))
        mc = re.match(r'^\s*([A-Za-z0-9\-_]+)\s+(.+)$', full)
        if not mc or _flat(mc.group(1)) != flat:
            continue
        mi = re.search(r'data-src="([^"]+)"', body)
        dur = re.search(r'duration-badge"[^>]*>([^<]+)<', body)
        date = re.search(r'text-muted"[^>]*>([^<]+)<', body)
        return {'source': 'javtrailers', 'code': mc.group(1).upper(),
                'title': mc.group(2).strip(), 'plot': '', 'studio': '',
                'genres': [], 'actors': [],
                'runtime': _hms_to_min(dur.group(1) if dur else ''),
                'date': (date.group(1).strip() if date else ''),
                'cover': (mi.group(1) if mi else ''), 'samples': [], 'url': JT + href}
    return None


def _avsox_post(method, body):
    """POST avsox（javu）内部 JSON 接口；返回 data 或 None。"""
    h = dict(JS.HDRS)
    h['Content-Type'] = 'application/json'
    h['X-Requested-With'] = 'XMLHttpRequest'
    try:
        r = JS.new_session().post(AVSOX_API + method, data=json.dumps(body),
                                  proxies=JS.PROXIES, timeout=30, headers=h)
    except Exception:
        return None
    if r is None or r.status_code != 200:
        return None
    try:
        j = r.json()
    except Exception:
        return None
    return j.get('data') if j.get('code') == 200 else None


def fetch_avsox(item):
    """avsox（无码，现域名 avsox.click）：getMovies 按番号搜，getMovie 取详情。"""
    if 'avsox' not in sites_enabled():
        return None
    code = (item.get('code') or item.get('sem_code') or '').strip()
    if not code:
        return None
    rows = _avsox_post('getMovies', {'query': code}) or []
    flat = _flat(code)
    hit = None
    for row in rows:
        if _flat(row.get('movieFanHao')) == flat:
            hit = row
            break
    if hit is None:
        return None
    mid = hit.get('movieId')
    det = _avsox_post('getMovie', {'movieId': mid}) if mid else None
    src = det or hit

    def pick(*keys):
        for k in keys:
            if src.get(k):
                return src[k]
        return ''

    # JAV 原始标题是日文：中/日界面优先保留日文原文；英/西优先英文标题；繁体优先台版译名
    _ol = out_lang()
    if _ol in ('en', 'es'):
        order = ('title_en', 'title', 'title_ja')
    elif _ol == 'zh-Hant':
        order = ('title_tw', 'title_cn', 'title', 'title_ja', 'title_en')
    else:
        order = ('title_ja', 'title', 'title_cn', 'title_tw', 'title_en')

    st = src.get('studio') or {}
    genres = [g.get('genreName') for g in (src.get('genre') or [])
              if g.get('genreName')] or [x.get('genreName') for x in (src.get('genre') or [])]
    genres = [g for g in genres if g]
    actors = [s.get('starName') for s in (src.get('star') or []) if s.get('starName')]
    d = {'source': 'avsox', 'code': (src.get('movieFanHao') or code).upper(),
         'title': pick(*order),
         'plot': pick('description_cn', 'description_ja', 'description_en', 'description_tw'),
         'studio': (st.get('studioName') or st.get('studioName_cn')
                    or st.get('studioName_ja') or st.get('studioName_en') or ''),
         'genres': genres[:12], 'actors': actors[:8],
         'runtime': ('%s分鐘' % src['length']) if src.get('length') else '',
         'date': src.get('releaseDate') or '',
         'cover': src.get('posterLarge') or src.get('posterSmall') or '',
         'samples': (src.get('sampleLarge') or src.get('sampleSmall') or [])[:6],
         'url': '%s/cn/movies/%s' % (AVSOX, mid or '')}
    return d if d.get('title') else None


def _123av_parse(b, slug, source='123av', url=''):
    """从 123av 详情页 HTML 抠字段（njav 已迁到 123av.com，共用这份解析）。"""
    m = re.search(r'og:title"[^>]*content="([^"]+)"', b) or \
        re.search(r'<title[^>]*>(.*?)</title>', b, re.S | re.I)
    title = JS._clean(m.group(1)) if m else ''
    title = re.sub(r'\s*[-—|]\s*123AV.*$', '', title).strip()
    if len(title) < 2:
        return None
    m = re.search(r'(https://icdn\.123av\.me/img2/s\d+/[0-9a-f]+/%s/cover\.jpg[^"\'\s]*)'
                  % re.escape(slug), b)
    cover = m.group(1) if m else ''
    txt = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', b))
    rt = ''
    m = re.search(r'(?:時長|时长|時間)\s*([0-9]{1,2}):([0-9]{2}):([0-9]{2})', txt)
    if m:
        rt = '%d分鐘' % (int(m.group(1)) * 60 + int(m.group(2)))
    actors = []
    m = re.search(r'(?:演員|演员)\s*(.{0,60}?)\s*(?:制作商|製作商)', txt)
    if m:
        actors = [x.strip() for x in re.split(r'[,、]', m.group(1)) if 1 < len(x.strip()) < 20]
    studio = ''
    m = re.search(r'(?:制作商|製作商)\s*(.{0,40}?)\s*(?:系列|類別|类别)', txt)
    if m:
        studio = m.group(1).strip()
    genres = []
    for g in re.findall(r'href="/cn/genres/[^"]*"[^>]*>\s*([^<]{1,24}?)\s*<', b):
        g = JS._clean(g)
        if g and g not in genres:
            genres.append(g)
    return {'source': source, 'code': slug.upper(), 'title': title, 'plot': '',
            'studio': studio, 'genres': genres[:12], 'actors': actors[:6],
            'runtime': rt, 'cover': cover, 'samples': [], 'url': url}


def fetch_123av(item):
    """123AV（无码）：详情页 HTML 里就有封面/时长/演员/制作商/类型。"""
    if '123av' not in sites_enabled():
        return None
    raw = (item.get('code') or item.get('sem_code') or '').strip()
    if not raw:
        return None
    slug = raw.lower()
    u = 'https://123av.com/cn/v/%s' % slug
    try:
        r = JS.get(u, timeout=30)
    except Exception:
        return None
    if r is None or r.status_code != 200:
        return None
    return _123av_parse(r.text or '', slug, '123av', u)


def fetch_njav(item):
    """NJAV（无码）：njav.tv 已整体迁到 123av.com，走同一套 123AV 详情解析。"""
    if 'njav' not in sites_enabled():
        return None
    raw = (item.get('code') or item.get('sem_code') or '').strip()
    if not raw:
        return None
    slug = raw.lower()
    u = 'https://123av.com/cn/v/%s' % slug
    try:
        r = JS.get(u, timeout=30)
    except Exception:
        return None
    if r is None or r.status_code != 200:
        return None
    return _123av_parse(r.text or '', slug, 'njav', u)


def scrape_website(item):
    """按【站点设置】里勾选的顺序尝试；javbus / javdb 有专属解析。"""
    code = item['code']
    if not code:
        return None
    on = sites_enabled()
    if 'fc2' in on and re.match(r'(?i)fc2[-_ ]?ppv', code):
        d = fetch_fc2(item)          # FC2 官方：官方标题 + 样张封面
        if d and d.get('title'):
            return d
    if 'javbus' in on:
        d = JS.fetch(code)
        if d and (d.get('code') or d.get('date')):
            d['source'] = 'javbus'
            return d
    if 'javdb' in on:
        d = fetch_javdb(code)
        if d and d.get('code'):
            return d
    if 'netflav' in on:                    # 聚合站：有码/无码都可能收录
        d = fetch_netflav(item)
        if d and d.get('title'):
            return d
    if 'javtrailers' in on:                # 资料站：片名/封面/时长
        d = fetch_javtrailers(item)
        if d and d.get('title'):
            return d
    return None


# ------------------------------------------------------------------ uncensored line
# JavBus/JavDB do not carry these series; MissAV does. Japanese first, then zh.
MISSAV = 'https://missav123.com'
UNCENSORED_TOKENS = ('1pondo', '10musume', 'caribbeancom', '1carib', 'heyzo',
                     'pacopacomama', 'tokyo-hot', 'tokyohot', 'muramura', '259luxu',
                     'luxu-', 'fc2-ppv', 'fc2ppv', '200gana', 'heydouga', 'siro-',
                     '300mium', '261ara',
                     # 系列后缀写法（053026_100-PACO / 123456_001-CARIB …）
                     '-paco', '-carib', '-1pon', '-10mu', '-mura', '-luxu',
                     '-gana', '-mium', '-ara', '-douga', '-siro', '-hey', '-hot')
_UA = (getattr(JS, 'HDRS', {}) or {}).get('User-Agent', 'Mozilla/5.0')


# 词边界匹配的系列名（避免 scute 误伤 execute 之类）
UNCENSORED_WORDS = ('scute', 'oreh', 'orer', 'gachinco', 'xxxav', 'marriedslash',
                    'madou', 'twav', 'furuke', 'maan', 'naughty4610', 'naughty0930',
                    'caribbeancompr', 'gachi', 'maan')


def guess_code(name):
    """从文件夹名里抠一个可能的番号（语义阶段没给时兜底）。"""
    s = re.sub(r'^[^@]*@', '', (name or '')).strip()
    m = re.search(r'([A-Za-z]{2,6})[-_ ]?(\d{2,5})', s)
    if m:
        return ('%s-%s' % (m.group(1), m.group(2))).upper()
    return ''


def is_uncensored(name):
    s = re.sub(r'^[^@]*@', '', (name or '')).lower()
    if any(t in s for t in UNCENSORED_TOKENS):
        return True
    for w in UNCENSORED_WORDS:
        if re.search(r'(?<![a-z])' + re.escape(w) + r'(?![a-z])', s):
            return True
    return False


def kind_by_rule(name):
    """按系列名/番号粗分标签：uncensored / coded / none。
    不必很准（只作标签用，不参与搜索路由）。"""
    if is_uncensored(name):
        return 'uncensored'
    s = re.sub(r'^[^@]*@', '', (name or '')).strip()
    if re.search(r'[A-Za-z]{2,6}[-_ ]?\d{2,5}', s):
        return 'coded'
    return 'none'


# series -> regex; \D{0,12}? skips junk like 'caribbeancom 加勒比 111425-001'
_SERIES_PATS = (
    ('1pondo', r'1pondo\D{0,12}?(\d{6}[_-]\d+)'),
    ('10musume', r'10musume\D{0,12}?(\d{6}[_-]\d+)'),
    ('pacopacomama', r'pacopacomama\D{0,12}?(\d{6}[_-]\d+)'),
    ('muramura', r'muramura\D{0,12}?(\d{6}[_-]\d+)'),
    ('caribbeancom', r'caribbeancom\D{0,12}?(\d{6}[-_]\d+)'),
    ('1carib', r'1carib\D{0,12}?(\d{6}[-_]\d+)'),
    ('heyzo', r'heyzo\D{0,12}?(\d{3,5})'),
    ('259luxu', r'259luxu\D{0,12}?(\d{3,5})'),
    ('siro', r'siro\D{0,12}?(\d{3,5})'),
    ('200gana', r'200gana\D{0,12}?(\d{3,5})'),
    ('300mium', r'300mium\D{0,12}?(\d{3,5})'),
    ('261ara', r'261ara\D{0,12}?(\d{3,5})'),
    ('heydouga', r'heydouga\D{0,12}?(\d{3,5})'),
    ('fc2', r'fc2[-_ ]?ppv[-_ ]?(\d{5,})'),
)
_DATE_SERIES = ('1pondo', '10musume', 'pacopacomama', 'muramura', 'caribbeancom', '1carib')


_SUFFIX_SERIES = {
    'paco': 'pacopacomama', 'pacopacomama': 'pacopacomama',
    'carib': 'caribbeancom', 'caribbeancom': 'caribbeancom',
    '1pon': '1pondo', '1pondo': '1pondo',
    '10mu': '10musume', '10musume': '10musume',
    'mura': 'muramura', 'muramura': 'muramura',
    'hey': 'heyzo', 'heyzo': 'heyzo',
    'luxu': '259luxu', '259luxu': '259luxu',
    'gana': '200gana', '200gana': '200gana',
    'mium': '300mium', '300mium': '300mium',
    'ara': '261ara', '261ara': '261ara',
    'siro': 'siro',
    'douga': 'heydouga', 'heydouga': 'heydouga',
    'tokyo': 'tokyo-hot', 'hot': 'tokyo-hot',
}


def uncensored_slugs(name, code=None):
    """Candidate MissAV slugs, best guess first.

    优先用模型给的干净番号（sem_code）；再看文件夹名——名字里番号
    常被中文标题挤开（如 0184-fc2ppv绝品…4550000），只能靠 code。
    """
    s = re.sub(r'^[^@]*@', '', (name or '').strip()).strip().lower()
    s = re.sub(r'-(fhd|hd|1080p|720p)$', '', s).strip()
    out = []
    c = re.sub(r'\s+', '-', (code or '').strip().lower())
    if c:
        out.append(c)                                    # 模型提取的干净番号
        m = re.match(r'fc2[-_ ]?ppv[-_ ]?(\d+)', c)
        if m:
            out.append('fc2-ppv-%s' % m.group(1))
        m = re.match(r'(\d{6}[_-]\d+)[-_ ]([a-z]{2,})$', c)
        if m and _SUFFIX_SERIES.get(m.group(2)):
            out.append('%s-%s' % (_SUFFIX_SERIES[m.group(2)], m.group(1)))
    # 形如 053026_100-PACO / 123456_001-CARIB：系列后缀 + 日期_编号
    for m in re.finditer(r'(\d{6}[_-]\d{2,})\s*[-_ ]\s*([a-z]{2,})', s):
        ser = _SUFFIX_SERIES.get(m.group(2))
        if ser:
            out.append('%s-%s' % (ser, m.group(1)))
            out.append(m.group(1))
    for series, pat in _SERIES_PATS:
        for m in re.finditer(pat, s):
            num = m.group(1)
            if series in _DATE_SERIES:
                out.append('%s-%s' % (series, num))
                out.append(num)                      # bare date_id (1pondo style)
            elif series == 'fc2':
                out.append('fc2-ppv-%s' % num)
            else:
                out.append('%s-%s' % (series, num))
    out.append(re.sub(r'\s+', '-', s))
    return [x for x in dict.fromkeys(out) if x]


def _missav_title(item):
    """MissAV 单站尝试（ja 再 zh），失败返回 None。"""
    # 不再要求 is_uncensored：系列名不在识别表时也照试（MissAV 对 slug 很宽容）
    for slug in uncensored_slugs(item['name'], item.get('code')):
        for lang in ('ja', 'zh'):
            url = '%s/%s/%s' % (MISSAV, lang, slug)
            r = None
            for attempt in (1, 2):
                try:
                    r = JS.get(url, timeout=35,
                               headers={'Accept-Language': lang, 'User-Agent': _UA})
                    break
                except Exception as e:
                    r = None
                    if attempt == 2:
                        sys.stderr.write('  missav err %s\n' % str(e)[:70])
                    else:
                        time.sleep(1.5)
            if r is None or r.status_code != 200:
                continue
            m = (re.search(r'<h1[^>]*>(.*?)</h1>', r.text, re.S | re.I)
                 or re.search(r'property="og:title"\s+content="([^"]+)"', r.text, re.I))
            if not m:
                continue
            title = JS._clean(m.group(1))
            title = re.sub(r'\s*[-|]\s*(MissAV|Watch|\u514d\u8cbb).*$', '', title, flags=re.I).strip()
            parts = title.split(None, 1)
            if (len(parts) == 2 and re.fullmatch(r'[A-Za-z0-9._\-]+', parts[0])
                    and re.search(r'\d', parts[0]) and len(parts[1].strip()) >= 2):
                title = parts[1].strip()
            if len(title) < 2:
                continue
            return {'source': 'missav', 'title': title, 'url': url,
                    'plot': '', 'tags': [], 'actors': []}
    # 兜底：用 MissAV 搜索（把 _ - 拆成空格再搜，命中率高很多）
    slug = missav_search(item['name'])
    if slug:
        for lang in ('ja', 'zh'):
            url = '%s/%s/%s' % (MISSAV, lang, slug)
            try:
                r = requests.get(url, timeout=35, proxies=getattr(JS, 'PROXIES', None),
                                 headers={'User-Agent': _UA, 'Accept-Language': lang})
            except Exception:
                continue
            if r.status_code != 200:
                continue
            m = (re.search(r'<h1[^>]*>(.*?)</h1>', r.text, re.S | re.I)
                 or re.search(r'property="og:title"\s+content="([^"]+)"', r.text, re.I))
            if not m:
                continue
            title = JS._clean(m.group(1))
            title = re.sub(r'\s*[-|]\s*(MissAV|Watch|\u7121\u6599|\u514d\u8d39).*$', '', title, flags=re.I).strip()
            parts = title.split(None, 1)
            if len(parts) == 2 and re.fullmatch(r'[A-Za-z0-9._\-]+', parts[0]) and re.search(r'\d', parts[0]):
                title = parts[1].strip()
            if len(title) >= 4:
                return {'source': 'missav', 'title': title, 'url': url,
                        'plot': '', 'tags': [], 'actors': []}
    return None


def scrape_uncensored(item):
    """无码线：MissAV 优先，其次 avsox，最后 njav（njav.tv 已迁到 123av.com）。"""
    on = sites_enabled()
    if 'missav' in on:
        d = _missav_title(item)
        if d:
            return d
    if 'avsox' in on:
        d = fetch_avsox(item)
        if d and d.get('title'):
            return d
    if 'njav' in on:
        d = fetch_njav(item)
        if d and d.get('title'):
            return d
    return None


def missav_search(query, lang='ja'):
    """MissAV 搜索兜底 —— 2026-10-12 实测已不可用。

    搜索页是纯前端渲染（结果由 XHR 异步加载，HTML 内无影片链接、也无内嵌 JSON），
    服务端拿不到结果。详情线 `_missav_title`（按 slug 直取）正常，故此处直接短路，
    不再白白等一次请求；返回 None 等同于"未找到"。"""
    return None


def split_list(v):
    if not v:
        return []
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    # stored tags/actors are json.dumps()ed -> "[]" must mean EMPTY, never ['[]']
    if s.startswith('['):
        try:
            j = json.loads(s)
            if isinstance(j, list):
                return [str(x).strip() for x in j if str(x).strip()]
        except Exception:
            pass
        s = s.strip('[]')
    if not s:
        return []
    return [x.strip() for x in re.split(r'[,，、/|]', s) if x.strip()]


# ------------------------------------------------------------------ text normalisation
_KANA = re.compile(r'[\u3040-\u30ff]')       # hiragana / katakana
_HAN = re.compile(r'[\u4e00-\u9fff]')


def normalize_text(s):
    """Unify display text: Japanese keeps its kanji+kana verbatim; Chinese is
    converted to Simplified; non-CJK text passes through (AI line translates it)."""
    if not s:
        return s
    s = str(s).strip()
    if not s:
        return s
    if _KANA.search(s):
        return s                              # Japanese -> keep as-is
    if _HAN.search(s) and zhconv is not None:
        _ol = out_lang()
        if _ol == 'zh-Hant':
            return zhconv.convert(s, 'zh-hant')   # -> 繁体
        if _ol in ('en', 'es', 'ja'):
            return s                              # 保留原样，交给模型翻译
        return zhconv.convert(s, 'zh-hans')       # 繁 -> 简    # 繁體 -> 简体
    return s


# ------------------------------------------------------------------ ai scrape
AI_PROMPT = """你是影视资料整理助手。下面是一个成人影片的文件夹名（可能很杂乱）与视频文件名。
请**只依据字面信息**生成元数据，不要编造真实姓名或作品背景。输出严格 JSON（无其它文字）：

{{"title":"干净的中文标题（保留关键卖点，去掉广告/网址/夸张符号，40字内）",
 "plot":"2-3句中文简介，客观描述内容特征",
 "tags":["{TAGS}（如 素人/自拍/FC2/制服/户外 等）"],
 "actors":[]}}

{LANG}

文件夹名: {name}
文件名: {vfile}
"""


def biggest_video(dirpath):
    best, bs = None, -1
    try:
        for f in os.listdir(dirpath):
            if os.path.splitext(f)[1].lower() in VIDEO_EXT:
                sz = os.path.getsize(os.path.join(dirpath, f))
                if sz > bs:
                    best, bs = f, sz
    except Exception:
        pass
    return best


def pick_cover_video(dirpath, top=10):
    """封面取帧要用的视频：**多视频时随机挑一个**（仍按体积取前 top 个做池子，
    避免挑到花絮/样片这类小文件）；单视频目录就是它自己。
    """
    import random
    try:
        vids = [os.path.join(dirpath, f) for f in os.listdir(dirpath)
                if os.path.splitext(f)[1].lower() in VIDEO_EXT]
    except Exception:
        return None
    if not vids:
        return None
    try:
        vids.sort(key=os.path.getsize, reverse=True)
    except Exception:
        pass
    pool = vids[:max(1, int(top))]
    if len(pool) > 1:
        random.shuffle(pool)
    return pool[0]


def video_duration(vpath):
    try:
        o = subprocess.run([FFPROBE, '-v', 'error', '-show_entries', 'format=duration',
                            '-of', 'default=nw=1:nk=1', vpath],
                           capture_output=True, timeout=90)
        return float((o.stdout or b'0').decode('utf-8', 'ignore').strip() or 0)
    except Exception:
        return 0.0


def video_frames(vpath, n=6, width=512):
    """抽 n 帧（沿时间轴均匀），返回 jpeg bytes 列表。"""
    dur = video_duration(vpath)
    if dur <= 1:
        return []
    out = []
    for i in range(n):
        t = dur * (i + 0.5) / n
        try:
            r = subprocess.run([FFMPEG, '-v', 'error', '-ss', '%.2f' % t, '-i', vpath,
                                '-frames:v', '1', '-vf', 'scale=%d:-1' % width,
                                '-f', 'image2', '-vcodec', 'mjpeg', '-'],
                               capture_output=True, timeout=180)
            if r.stdout and len(r.stdout) > 800:
                out.append(r.stdout)
        except Exception:
            pass
    return out


def cover_frame(dirpath, width=640):
    """缩略图兜底：随机挑一个视频 + 在 [MIN_START, 片尾] 内随机取一帧，返回 jpeg bytes。"""
    import random
    p = pick_cover_video(dirpath)
    if not p:
        return None
    try:
        dur = video_duration(p)
    except Exception:
        dur = 0.0
    lo = COVER.MIN_START if (dur and dur / 3.0 > COVER.MIN_START) else 0.0
    hi = max(lo, (dur or 0) - COVER.TAIL)
    t = random.uniform(lo, hi) if hi > lo else lo
    try:
        r = subprocess.run([FFMPEG, '-v', 'error', '-ss', '%.2f' % t, '-i', p,
                            '-frames:v', '1', '-vf', 'scale=%d:-1' % width,
                            '-f', 'image2', '-vcodec', 'mjpeg', '-'],
                           capture_output=True, timeout=180)
        return r.stdout if (r.stdout and len(r.stdout) > 800) else None
    except Exception:
        return None


VIDEO_PROMPT = """下面是一部影片的若干截图（按时间顺序）。文件名没有任何可用信息，只能靠画面判断。
只输出一个 JSON 对象，不要任何其它文字：
{"title":"中文标题（24字内）。**必须写出这部片子独有的辨识点**，每条都要不一样：
          点出画面里最特别的人物特征 / 服饰 / 道具 / 场景 / 动作之一（例如某件衣物、
          某个道具、某个场所、某个动作）。**不要用**「特写」「室内」「私密」「记录」
          这类放之四海皆准的词，**不要**套同一个句式。风格自由：有的偏人物、有的偏场景、
          有的偏情节、有的偏氛围。不要编造番号",
 "plot":"3-5句中文简介。要写出画面里**真实可见**的动人细节：人物神态与动作、衣着与身材、
         光线/色调与氛围、场景与陈设、镜头角度与节奏；读起来有画面感、有代入感。
         但**只写你从截图里真能看出来的**，不要编造看不到的情节、人名或对白",
 "tags":["{TAGS}"]}
{LANG}
"""


def ai_video_scan(item, model=None, n=6):
    """看视频截图 -> {title, plot, tags}；失败返回 None。"""
    model = model or ai_model()
    v = biggest_video(item.get('path') or '')
    if not v:
        return None
    frames = video_frames(os.path.join(item['path'], v), n=n)
    if not frames:
        return None
    imgs = [base64.b64encode(b).decode('ascii') for b in frames]
    try:
        r = requests.post(OLLAMA + '/api/chat', timeout=1800,
                          json={'model': model, 'stream': False,
                                'keep_alive': AI_KEEP_ALIVE, 'think': False,
                                'options': {'temperature': 0.75, 'num_predict': 900},
                                'messages': [{'role': 'user', 'content': VIDEO_PROMPT.replace('{LANG}', lang_rule()).replace('{TAGS}', lang_tags()),
                                              'images': imgs}]})
        if r.status_code != 200:
            return None
        txt = ((r.json().get('message') or {}).get('content') or '')
        j = extract_json(txt)
        if not isinstance(j, dict):
            return None
        t = str(j.get('title') or '').strip()
        p = str(j.get('plot') or '').strip()
        tg = j.get('tags') or []
        if isinstance(tg, str):
            tg = re.split(r'[,，、;；]', tg)
        tg = [str(x).strip() for x in tg if str(x).strip()][:12]
        if not (t or p):
            return None
        return {'title': t, 'plot': p, 'tags': tg}
    except Exception as e:
        sys.stderr.write('  vscan err %s\n' % str(e)[:80])
        return None


def extract_json(txt):
    """Pull the FIRST balanced {...} object out of model output (handles ```json fences)."""
    t = (txt or '').strip()
    t = re.sub(r'^```(?:json)?\s*', '', t)
    t = re.sub(r'\s*```$', '', t)
    depth = 0
    start = -1
    for i, ch in enumerate(t):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            if depth:
                depth -= 1
                if depth == 0 and start >= 0:
                    try:
                        return json.loads(t[start:i + 1])
                    except Exception:
                        start = -1
    return None


# ------------------------------------------------------------------ batch inference
# One model call for a whole group -> far fewer round-trips (20 semantic / 5 AI).
SEM_BATCH_PROMPT = """你是影片番号识别助手。下面是 {N} 个影片文件夹名，逐个判断。
只输出一个 JSON 数组，长度必须正好 {N}，顺序与输入一致，不要任何其它文字：
[{"kind":"coded|uncensored|none","code":"番号，没有就留空"}]

- coded：有明确番号（ABF-362 / SIRO-5288 / 259LUXU-1696 / 200GANA-2566）
- uncensored：无码系列或 FC2（1pondo-... / 10musume-... / caribbeancom-... / heyzo ... / fc2-ppv-...）
- none：没有可用番号（纯中文片名、探花、自拍、合集）

{ITEMS}
"""

AI_BATCH_PROMPT = """你是影片资料助手。下面是 {N} 个影片文件夹名（可能含广告/网址/夸张符号）。
只输出一个 JSON 数组，长度必须正好 {N}，顺序与输入一致，不要任何其它文字：
[{"title":"干净的中文标题（40字内，去掉广告/网址/夸张符号）",
  "plot":"2-3句中文简介，客观描述内容特征",
  "tags":["{TAGS}"]}]

{LANG}

{ITEMS}
"""


def _chat(prompt, model, num_predict=1500, temp=0.2, timeout=2400):
    r = requests.post(OLLAMA + '/api/chat', timeout=timeout,
                      json={'model': model, 'stream': False, 'keep_alive': AI_KEEP_ALIVE,
                            'think': False,
                            'options': {'temperature': temp, 'num_predict': num_predict},
                            'messages': [{'role': 'user', 'content': prompt}]})
    if r.status_code != 200:
        return None
    return (r.json().get('message') or {}).get('content', '')


def extract_json_array(text):
    """First balanced [...] in the text, parsed. None if absent/broken."""
    if not text:
        return None
    t = re.sub(r'^```[a-zA-Z]*\s*', '', text.strip())
    t = re.sub(r'```\s*$', '', t)
    i = t.find('[')
    if i < 0:
        return None
    depth = 0
    for j in range(i, len(t)):
        if t[j] == '[':
            depth += 1
        elif t[j] == ']':
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(t[i:j + 1])
                except Exception:
                    return None
    return None


def _num_lines(items, with_video=False):
    out = []
    for i, it in enumerate(items, 1):
        s = '%d. %s' % (i, it.get('name', ''))
        if with_video:
            v = biggest_video(it.get('path') or '') or ''
            if v:
                s += '  [文件: %s]' % v
        out.append(s)
    return '\n'.join(out)


def semantic_batch(items, model=None):
    """One call for N items -> list[{kind,code}] of exact length, or None (fall back to singles)."""
    model = model or ai_model()
    if not items:
        return []
    prompt = (SEM_BATCH_PROMPT.replace('{N}', str(len(items)))
              .replace('{ITEMS}', _num_lines(items, with_video=True))
              .replace('{LANG}', lang_rule()).replace('{TAGS}', lang_tags()))
    try:
        arr = extract_json_array(_chat(prompt, model, 1500))
    except Exception as e:
        sys.stderr.write('  sem batch err %s\n' % str(e)[:80])
        return None
    if not isinstance(arr, list) or len(arr) != len(items):
        return None
    out = []
    for j in arr:
        if not isinstance(j, dict):
            return None
        kind = str(j.get('kind') or '').strip().lower()
        if kind not in ('coded', 'uncensored', 'none'):
            kind = 'none'
        out.append({'kind': kind,
                    'code': re.sub(r'^[:：\s]+', '', str(j.get('code') or '').strip())})
    return out


def ai_batch(items, model=None):
    """One call for N items -> list[dict] of exact length, or None (fall back to singles)."""
    model = model or ai_model()
    if not items:
        return []
    prompt = (AI_BATCH_PROMPT.replace('{N}', str(len(items)))
              .replace('{ITEMS}', _num_lines(items, with_video=True))
              .replace('{LANG}', lang_rule()).replace('{TAGS}', lang_tags()))
    try:
        arr = extract_json_array(_chat(prompt, model, 4000, 0.3))
    except Exception as e:
        sys.stderr.write('  ai batch err %s\n' % str(e)[:80])
        return None
    if not isinstance(arr, list) or len(arr) != len(items):
        return None
    return [j if isinstance(j, dict) else {} for j in arr]


AI_TAGS_PROMPT = (
    '你是影片元数据整理助手。下面给出若干条影片的【番号、标题、简介】。\n'
    '请**只根据简介内容**，为每一条给出 {TAGS}（题材/场景/人物特征）。\n'
    '【不要看文件名】，不要编造简介里没有的内容，不要改标题。\n'
    '严格只输出一个 JSON 数组，长度必须是 {N}，每项形如 {{"tags":["标签1","标签2"]}}，'
    '不要任何解释。\n\n{ITEMS}'
)


def _fill_lines(items):
    L = []
    for i, it in enumerate(items, 1):
        L.append('%d. 番号: %s | 标题: %s | 简介: %s'
                 % (i, (it.get('code') or it.get('sem_code') or '').strip(),
                    (it.get('title') or '').strip(),
                    (it.get('plot') or '').strip()[:900]))
    return '\n'.join(L)


def ai_tags_batch(items, model=None):
    """按简介给 N 条补标签（不看文件名）。返回 [tags_list, ...] 或 None。"""
    model = model or ai_model()
    if not items:
        return []
    prompt = (AI_TAGS_PROMPT.replace('{N}', str(len(items)))
              .replace('{ITEMS}', _fill_lines(items))
              .replace('{LANG}', lang_rule()).replace('{TAGS}', lang_tags()))
    try:
        arr = extract_json_array(_chat(prompt, model, 2500, 0.3))
    except Exception as e:
        sys.stderr.write('  ai tags err %s\n' % str(e)[:80])
        return None
    if not isinstance(arr, list) or len(arr) != len(items):
        return None
    out = []
    for j in arr:
        t = j.get('tags') if isinstance(j, dict) else (j if isinstance(j, list) else [])
        if isinstance(t, str):
            t = re.split(r'[,，、;；]', t)
        out.append([str(x).strip() for x in (t or []) if str(x).strip()][:12])
    return out


def unload_ai(model=None):
    """Release the local model's VRAM now instead of squatting on the GPU."""
    model = model or ai_model()
    try:
        requests.post(OLLAMA + '/api/generate', timeout=30,
                      json={'model': model, 'keep_alive': 0})
        return True
    except Exception as e:
        sys.stderr.write('  unload err %s\n' % str(e)[:60])
        return False


def scrape_ai(item, model=None):
    """Ask the local model to clean the title + write plot/tags.

    Returns None on ANY failure (transport, non-JSON, or junk output) so the caller
    NEVER marks an un-scraped item as done.
    """
    model = model or ai_model()
    name = item['name']
    vfile = biggest_video(item['path']) or ''
    prompt = AI_PROMPT.format(name=name, vfile=vfile, LANG=lang_rule(), TAGS=lang_tags())
    try:
        # think=False is REQUIRED: qwen38-27b-uncen is a thinking model, and an
        # un-suppressed reasoning block eats the whole num_predict budget -> the
        # response's content comes back EMPTY. (ollama puts thoughts in .thinking.)
        r = requests.post(OLLAMA + '/api/chat', timeout=900,
                          json={'model': model, 'stream': False, 'keep_alive': AI_KEEP_ALIVE,
                                'think': False,
                                'options': {'temperature': 0.3, 'num_predict': 800},
                                'messages': [{'role': 'user', 'content': prompt}]})
        if r.status_code != 200:
            sys.stderr.write('  ai err http %s\n' % r.status_code)
            return None
        txt = (r.json().get('message') or {}).get('content', '')
        j = extract_json(txt)
        if not isinstance(j, dict):
            sys.stderr.write('  ai err no-json\n')
            return None
        title = str(j.get('title') or '').strip()
        plot = str(j.get('plot') or '').strip()
        tags = [str(x).strip() for x in (j.get('tags') or []) if str(x).strip()]
        actors = [str(x).strip() for x in (j.get('actors') or []) if str(x).strip()]
        if title == name and not tags and not plot:
            sys.stderr.write('  ai err junk (unchanged name, no tags/plot)\n')
            return None
        return {'source': 'ai', 'title': title or name, 'plot': plot,
                'tags': tags, 'actors': actors}
    except Exception as e:
        sys.stderr.write('  ai err %s\n' % str(e)[:80])
        return None


# ------------------------------------------------------------------ cover / nfo
# ------------------------------------------------------------------ semantic pass
# One model call per item on the FILE NAME: does it carry a code (番号), is it an
# uncensored series, or nothing? Result is stored as sem_kind / sem_code and then
# decides the fetch route: javbus -> missav -> model.
SEM_PROMPT = """你是影片番号识别助手。根据文件夹名和文件名，判断类别并提取番号。
只输出 JSON（无其它文字）：
{{"kind":"coded|uncensored|none","code":"番号，没有就留空"}}

- coded：有明确番号，如 ABF-362 / SIRO-5288 / 259LUXU-1696 / 200GANA-2566 / MIAA-123
- uncensored：无码系列或 FC2，如 1pondo-012817_473 / 10musume-050317_01 / caribbeancom-111425-001 / heyzo 3715 / fc2-ppv-4609474
- none：没有可用番号（纯中文片名、探花、自拍、合集等）

文件夹名: {name}
文件名: {vfile}
"""


def semantic_code(item, model=None):
    """One model call: classify the item and extract its code. Returns {kind,code} or None."""
    model = model or ai_model()
    if folder_kind(item['path'])[0] == 'nested':
        return {'kind': 'none', 'code': '', 'nested': True}   # 多重嵌套 -> 一开始就交人工
    name = item['name']
    vfile = biggest_video(item['path']) or ''
    prompt = SEM_PROMPT.format(name=name, vfile=vfile)
    try:
        r = requests.post(OLLAMA + '/api/chat', timeout=900,
                          json={'model': model, 'stream': False, 'keep_alive': AI_KEEP_ALIVE,
                                'think': False,
                                'options': {'temperature': 0.1, 'num_predict': 200},
                                'messages': [{'role': 'user', 'content': prompt}]})
        if r.status_code != 200:
            return None
        j = extract_json((r.json().get('message') or {}).get('content', ''))
        if not isinstance(j, dict):
            return None
        kind = str(j.get('kind') or '').strip().lower()
        if kind not in ('coded', 'uncensored', 'none'):
            kind = 'none'
        code = re.sub(r'^[:：\s]+', '', str(j.get('code') or '').strip())
        # guard against an invented code: its digits must appear in name/filename
        cd = re.sub(r'\D', '', code)
        blob = re.sub(r'\D', '', name + ' ' + (vfile or ''))
        if code and cd and cd not in blob:
            sys.stderr.write('  sem: dropped unmatched code %s\n' % code[:24])
            code = ''
        return {'kind': kind, 'code': code}
    except Exception as e:
        sys.stderr.write('  sem err %s\n' % str(e)[:80])
        return None


def make_cover(item, force=False):
    """取帧做封面。
    规则：剧集且 时长/3 > 120 -> 从 120s 之后取帧；其余情况不限定取帧时间。
    """
    dirpath = item['path']
    if not force and any(os.path.exists(os.path.join(dirpath, n))
                         for n in ('poster.jpg', 'folder.jpg', 'cover.jpg')):
        return os.path.join(dirpath, 'poster.jpg')
    out = os.path.join(dirpath, 'poster.jpg')
    series = (live_kind(item) == 'series')
    # 多视频：随机挑一个（仍按体积取前 10 个做池子，避免挑到小样片）
    pool = pick_cover_video(dirpath, top=10)
    cands = []
    if pool:
        cands = [pool]
        if series:                     # 剧集：池子里剩下的作为兜底
            try:
                rest = [os.path.join(dirpath, f) for f in os.listdir(dirpath)
                        if os.path.splitext(f)[1].lower() in VIDEO_EXT]
                rest.sort(key=os.path.getsize, reverse=True)
                cands = [pool] + [x for x in rest[:10] if x != pool]
            except Exception:
                pass
    for p in cands:
        try:
            dur = COVER.duration(p)
        except Exception:
            dur = 0
        # 长视频：起点只在 [MIN_START, 片尾] 内随机（不再只对剧集生效）
        ms = COVER.MIN_START if (dur and dur / 3.0 > COVER.MIN_START) else 0.0
        try:
            if COVER.pick(p, out, n=12, verbose=False, min_start=ms, rand=True):
                return out
        except Exception:
            continue
    return None


def folder_image(item):
    """The folder's own image, if it ships one -- then we never grab a frame."""
    d = item['path']
    try:
        names = os.listdir(d)
    except Exception:
        return None
    imgs = [x for x in names
            if os.path.splitext(x)[1].lower() in ('.jpg', '.jpeg', '.png', '.webp')]
    if not imgs:
        return None
    for pref in ('poster.jpg', 'folder.jpg', 'cover.jpg'):
        if pref in imgs:
            return os.path.join(d, pref)
    cand = [x for x in imgs if x.lower() != 'fanart.jpg'] or imgs
    cand.sort(key=lambda x: os.path.getsize(os.path.join(d, x)), reverse=True)
    return os.path.join(d, cand[0])


def cache_cover(item_id, src):
    if not src or not os.path.exists(src):
        return 0
    try:
        ext = os.path.splitext(src)[1] or '.jpg'
        dst = os.path.join(CACHE_DIR, '%d%s' % (item_id, ext))
        with open(src, 'rb') as a, open(dst, 'wb') as b:
            b.write(a.read())
        return os.path.getsize(dst)
    except Exception:
        return 0


def live_kind(item):
    """以**磁盘实际结构**为准的目录类型（DB 里的 kind 可能是扫描后目录被整理过留下的旧的）。"""
    try:
        k = folder_kind(item.get('path') or '')[0]
        if k:
            return k
    except Exception:
        pass
    return (item.get('kind') or '')


def nfo_bytes(item):
    def esc(s):
        return str(s or '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
    title = item['title'] or item['name']
    tags = split_list(item['tags'])
    actors = split_list(item['actors'])
    # a real year only -- '(\d{4})' used to grab '4716' out of an FC2 code
    m = (re.search(r'(19|20)\d{2}', item['code'] or '')
         or re.search(r'(19|20)\d{2}', item['title'] or '')
         or re.search(r'(19|20)\d{2}', item['name'] or ''))
    year = m.group(0) if m else ''
    site = item['site'] if item['site'] in ('javbus', 'javdb') else 'local'
    studio = (item['studio'] or '').strip() if item.get('studio') else ''
    src = item.get('source') or ('code' if item['site'] in ('javbus', 'javdb')
                                 else ('Uncen' if item['site'] == 'missav' else 'AI'))
    series = (live_kind(item) == 'series')
    root = 'tvshow' if series else 'movie'
    L = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>', '<%s>' % root,
         '  <source>%s</source>' % esc(src)]
    for tag, val in (('title', title), ('sorttitle', item['code'] or item['name']),
                     ('plot', item['plot'] or title), ('year', year),
                     ('studio', studio)):
        if val:
            L.append('  <%s>%s</%s>' % (tag, esc(val), tag))
    if item['code']:
        L.append('  <num>%s</num>' % esc(item['code']))
        L.append('  <uniqueid type="%s" default="true">%s</uniqueid>'
                 % (esc(site), esc(item['code'])))
    for t in tags:
        L.append('  <tag>%s</tag>' % esc(t))
        L.append('  <genre>%s</genre>' % esc(t))
    for a in actors:
        L.append('  <actor><name>%s</name></actor>' % esc(a))
    L.append('  <art><poster>poster.jpg</poster></art>')
    # image paths live inside the nfo (Jellyfin: 保存图像路径在NFO文件)
    L.append('  <thumb>poster.jpg</thumb>')
    L.append('  <fanart>fanart.jpg</fanart>')
    L.append('</%s>' % root)
    return '\n'.join(L).encode('utf-8')


def write_nfo(item):
    """single: movie.nfo + <name>.nfo.   series: ONE combined tvshow.nfo.

    以**磁盘实际结构**为准（DB 的 kind 可能过期），并顺手纠正 DB。
    """
    k = live_kind(item)
    if (item.get('kind') or '') != k:
        try:
            _d = db()
            _d.execute('UPDATE items SET kind=? WHERE id=?', (k, item['id']))
            _d.commit()
        except Exception:
            pass
        item['kind'] = k
    data = nfo_bytes(item)
    d = item['path']
    wrote = []
    single = ['movie.nfo', os.path.basename(os.path.normpath(d)) + '.nfo']
    names = (['tvshow.nfo'] if k == 'series' else single)
    # 清掉另一种写法的残留，别让 Jellyfin 读到旧的那份
    for stale in (single if k == 'series' else ['tvshow.nfo']):
        try:
            sp = os.path.join(d, stale)
            if os.path.exists(sp):
                os.remove(sp)
        except Exception:
            pass
    try:
        for nm in names:
            with open(os.path.join(d, nm), 'wb') as f:
                f.write(data)
            wrote.append(nm)
        with open(os.path.join(NFO_DIR, '%d.nfo' % item['id']), 'wb') as f:
            f.write(data)
    except Exception as e:
        sys.stderr.write('  nfo write err %s\n' % str(e)[:80])
    return wrote


# ------------------------------------------------------------------ orchestrate
def apply_result(dbc, iid, d, status):
    genres = [normalize_text(x) for x in
              split_list(d.get('genres_list') or d.get('genres') or d.get('tags'))]
    actors = [normalize_text(x) for x in
              split_list(d.get('actors_list') or d.get('actors'))]
    title = normalize_text(d.get('title') or '')
    plot = normalize_text(d.get('plot') or title)
    runtime = re.search(r'(\d+)', str(d.get('runtime') or ''))
    rt = runtime.group(1) if runtime else ''
    studio = normalize_text((d.get('studio') or '').strip())
    # 质量: 有(完整) / 有误(刮了但没给出可用标签，或标题仍是原文件夹名 -> 留人工复核)
    # 建库扫描侧则是: 没 tag/描述不通 -> 无(继续刮)
    _nm = dbc.execute('SELECT name FROM items WHERE id=?', (iid,)).fetchone()
    _nm = _nm['name'] if _nm else ''

    def _sq(s):
        return re.sub(r'[^0-9A-Za-z\u3040-\u30ff\u4e00-\u9fff]+', '', s or '')

    not_cleaned = bool(_nm) and (_sq(title) == _sq(_nm) or _sq(plot) == _sq(_nm))
    source = {'javbus': 'code', 'javdb': 'code', 'missav': 'Uncen', 'ai': 'AI'}.get(
        d.get('source'), 'AI')
    if name_is_useless(_nm):
        quality = '缺'          # 名字没信息 -> 只能等 AI 看视频
    elif not title:
        quality = '有误'        # 网站两条线都没抓到
    elif not_cleaned:
        quality = '无'
    elif genres:
        quality = '有'
    else:
        quality = '缺'          # 有 title+plot 没 tag -> 缺（网站/AI 统一）
    dbc.execute("""UPDATE items SET status=?, site=?, studio=?, title=?, plot=?, tags=?, actors=?,
                   quality=?, source=?, last_update=? WHERE id=?""",
                (status, d.get('source', ''), studio, title, plot,
                 json.dumps(genres, ensure_ascii=False),
                 json.dumps(actors, ensure_ascii=False), quality, source, now(), iid))
    dbc.commit()
    if rt:
        dbc.execute('UPDATE items SET note=? WHERE id=?',
                    ('runtime=%s min' % rt, iid))
        dbc.commit()


def download_poster(item, d):
    """Save the scraped cover (+first sample as fanart) into the item folder."""
    out = None
    url = d.get('cover')
    if url:
        try:
            r = JS.get(url, timeout=90, headers=JS.IMGHDRS)
            if r.status_code == 200 and len(r.content) > 3000:
                p = os.path.join(item['path'], 'poster.jpg')
                with open(p, 'wb') as f:
                    f.write(r.content)
                out = p
        except Exception as e:
            sys.stderr.write('  poster dl err %s\n' % str(e)[:70])
    smp = (d.get('samples') or [])[:1]
    for u in smp:
        try:
            r = JS.get(u, timeout=90, headers=JS.IMGHDRS)
            if r.status_code == 200 and len(r.content) > 3000:
                with open(os.path.join(item['path'], 'fanart.jpg'), 'wb') as f:
                    f.write(r.content)
        except Exception:
            pass
    return out


def process(dbc, iid, mode='auto', force=False, cover_fallback=True, ai_result=None):
    """mode: website | ai | auto. Returns (ok, message)."""
    it = dbc.execute('SELECT * FROM items WHERE id=?', (iid,)).fetchone()
    if not it:
        return False, 'no such item'
    it = dict(it)
    if force:
        dbc.execute("UPDATE items SET status='empty' WHERE id=?", (iid,))
        dbc.commit()
    msgs = []
    d_used = None
    q_now = (it.get('quality') or '').strip()
    if q_now in ('有', '有误', '缺') and not force:
        return True, '质量=%s -> 跳过刮削' % q_now     # 有=已完整; 有误=留人工复核
    kind = (it.get('sem_kind') or '').strip()
    if it.get('sem_code'):
        it['code'] = it['sem_code']            # semantic code beats the regex one
    # 雷：内嵌目录里还有视频 -> 不刮（重跑/AI 补全都先过这一关），直接有误交人工
    if mode in ('website', 'ai', 'auto') and has_nested_videos(it.get('path')):
        dbc.execute("UPDATE items SET quality='有误', status='nested', last_update=? WHERE id=?",
                    (now(), iid))
        dbc.commit()
        return False, 'nested(内嵌目录含视频) -> 有误'
    if mode in ('website', 'auto') and (force or it['status'] != 'website'):
        # 标签只是标签，**两条线都试**：
        # 语义阶段没给番号就从名字里抠一个，好让 javbus/javdb 也有机会
        if not (it.get('code') or '').strip():
            it['code'] = guess_code(it['name'])
        d = scrape_website(it) if it.get('code') else None       # javbus -> javdb
        if not d:
            d = scrape_uncensored(it)                            # MissAV（兜底）
            if not d:
                d = fetch_123av(it)          # 123AV：无码兜底（带封面/类型）
        if d:
            apply_result(dbc, iid, d, 'website')
            msgs.append(('uncensored:' if d.get('source') == 'missav' else 'website:')
                        + d.get('source', '?'))
            d_used = d
        else:
            msgs.append('website:-')
    if mode in ('ai', 'auto'):
        cur = dbc.execute('SELECT status FROM items WHERE id=?', (iid,)).fetchone()['status']
        if cur == 'empty' or mode == 'ai':
            a = ai_result if ai_result is not None else scrape_ai(it)
            if a:
                apply_result(dbc, iid, a, 'ai')
                msgs.append('ai:ok')
            else:
                msgs.append('ai:-')
    it = dict(dbc.execute('SELECT * FROM items WHERE id=?', (iid,)).fetchone())
    # cover: scraped poster > the folder's OWN image (never grab a frame then) > frame
    poster = None
    if d_used:
        poster = download_poster(it, d_used)
    if not poster:
        poster = folder_image(it)
    if not poster and cover_fallback:
        poster = make_cover(it, force=False)
    if poster and os.path.basename(poster).lower() != 'poster.jpg':
        try:
            shutil.copyfile(poster, os.path.join(it['path'], 'poster.jpg'))
            poster = os.path.join(it['path'], 'poster.jpg')
        except Exception:
            pass
    if poster:
        it['cover'] = os.path.basename(poster)
    # only materialise an .nfo once we actually have scraped data
    wrote = write_nfo(it) if it['status'] != 'empty' else []
    ip = 0
    for n in os.listdir(it['path']) if os.path.isdir(it['path']) else []:
        if n.lower().endswith(('.jpg', '.png', '.jpeg')):
            ip += 1
    sz = cache_cover(iid, poster) if poster else 0
    dbc.execute('UPDATE items SET cover=?, images=?, nfo=?, last_update=? WHERE id=?',
                (it.get('cover') or '', ip, len(wrote), now(), iid))
    dbc.commit()
    return True, '%s | nfo=%s img=%d cache=%dB' % (','.join(msgs), ','.join(wrote), ip, sz)


# ------------------------------------------------------------------ cli
def cmd_scan(a):
    r = scan(a.root)
    print('  ' + json.dumps(r, ensure_ascii=False))


def cmd_list(a):
    dbc = db()
    q = 'SELECT id,name,code,status,title FROM items WHERE present=1'
    p = []
    if a.status:
        q += ' AND status=?'
        p.append(a.status)
    q += ' ORDER BY id LIMIT ?'
    p.append(a.limit or 30)
    for r in dbc.execute(q, p):
        print('  %-5d %-10s %-14s %s' % (r['id'], r['status'], r['code'][:14], (r['title'] or r['name'])[:64]))


def cmd_run(a):
    dbc = db()
    ok, msg = process(dbc, a.id, a.mode, a.force)
    print('  %s %s' % (ok, msg))


def cmd_auto(a):
    dbc = db()
    q = "SELECT id FROM items WHERE present=1 AND COALESCE(quality,'') IN ('','无')"
    q += " AND COALESCE(kind,'')<>'nested'"
    if getattr(a, 'coded', False):
        q += " AND code<>''"
    if getattr(a, 'nocode', False):
        q += " AND code=''"
    q += ' ORDER BY id LIMIT ?'
    rows = dbc.execute(q, (a.limit or 100000,)).fetchall()
    print('  queue=%d mode=%s coded=%s cover=%s'
          % (len(rows), a.mode, getattr(a, 'coded', False), not getattr(a, 'no_cover', False)), flush=True)
    ok = 0
    for n, r in enumerate(rows, 1):
        try:
            o, m = process(dbc, r['id'], a.mode, cover_fallback=not getattr(a, 'no_cover', False))
            ok += 1 if o else 0
            print('  [%d/%d] id=%d %s' % (n, len(rows), r['id'], m), flush=True)
        except Exception as e:
            print('  [%d/%d] id=%d ERR %s' % (n, len(rows), r['id'], str(e)[:60]), flush=True)
    unload_ai()
    print('  done %d/%d  (9B unloaded)' % (ok, len(rows)))


def cmd_sem(a):
    """Semantic pass: model decides kind + extracts the code, once per item."""
    dbc = db()
    q = "SELECT id,name,path FROM items WHERE present=1"
    if not getattr(a, 'force', False):
        q += " AND (sem_kind IS NULL OR sem_kind='') AND COALESCE(sem_manual,0)=0"
    q += ' ORDER BY id LIMIT ?'
    rows = dbc.execute(q, (a.limit or 100000,)).fetchall()
    print('  semantic queue=%d' % len(rows), flush=True)
    ok = 0
    for n, r in enumerate(rows, 1):
        s = semantic_code(dict(r))
        if s:
            dbc.execute('UPDATE items SET sem_kind=?, sem_code=?, last_update=? WHERE id=?',
                        (s['kind'], s['code'], now(), r['id']))
            if s.get('nested'):
                dbc.execute("UPDATE items SET quality='有误' WHERE id=?", (r['id'],))
            dbc.commit()
            ok += 1
            print('  [%d/%d] id=%-5d %-10s %s' % (n, len(rows), r['id'], s['kind'], s['code']), flush=True)
        else:
            print('  [%d/%d] id=%-5d SEM-FAIL' % (n, len(rows), r['id']), flush=True)
    if getattr(a, 'unload', False):
        unload_ai()
    print('  done %d/%d' % (ok, len(rows)))


def cmd_pipeline(a):
    """1 scan -> 2 semantic pass -> 3 fetch (javbus -> missav -> model)."""
    r = scan(a.root)
    print('  [1] scan: %s' % json.dumps(r, ensure_ascii=False), flush=True)
    dbc = db()
    rows = dbc.execute("SELECT id,name,path FROM items WHERE present=1 "
                       "AND (sem_kind IS NULL OR sem_kind='') AND COALESCE(sem_manual,0)=0 ORDER BY id LIMIT ?",
                       (a.limit or 100000,)).fetchall()
    print('  [2] semantic: %d items' % len(rows), flush=True)
    for n, rr in enumerate(rows, 1):
        s = semantic_code(dict(rr))
        if s:
            dbc.execute('UPDATE items SET sem_kind=?, sem_code=?, last_update=? WHERE id=?',
                        (s['kind'], s['code'], now(), rr['id']))
            dbc.commit()
        print('  [2][%d/%d] id=%-5d %-10s %s'
              % (n, len(rows), rr['id'], (s or {}).get('kind', 'FAIL'), (s or {}).get('code', '')),
              flush=True)
    q = "SELECT id FROM items WHERE present=1 AND COALESCE(quality,'') IN ('','无') "
    q += "AND COALESCE(kind,'')<>'nested' ORDER BY id LIMIT ?"
    rows = dbc.execute(q, (a.limit or 100000,)).fetchall()
    print('  [3] fetch: %d items' % len(rows), flush=True)
    ok = 0
    for n, rr in enumerate(rows, 1):
        try:
            o, m = process(dbc, rr['id'], 'auto', cover_fallback=not getattr(a, 'no_cover', False))
            ok += 1 if o else 0
            print('  [3][%d/%d] id=%-5d %s' % (n, len(rows), rr['id'], m), flush=True)
        except Exception as e:
            print('  [3][%d/%d] id=%-5d ERR %s' % (n, len(rows), rr['id'], str(e)[:60]), flush=True)
    unload_ai()
    print('  done fetch %d/%d' % (ok, len(rows)))


def cmd_stats(a):
    dbc = db()
    print('  total(present) = %d' % dbc.execute('SELECT COUNT(*) c FROM items WHERE present=1').fetchone()['c'])
    for s, c in dbc.execute("SELECT status, COUNT(*) c FROM items WHERE present=1 GROUP BY status"):
        print('    %-9s %d' % (s, c))
    print('  removed(absent) = %d' % dbc.execute('SELECT COUNT(*) c FROM items WHERE present=0').fetchone()['c'])
    print('  javdb cookie    = %s' % ('yes' if javdb_cookie() else 'no'))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    p = sub.add_parser('scan'); p.add_argument('--root', default=ROOT_DEFAULT); p.set_defaults(func=cmd_scan)
    p = sub.add_parser('list'); p.add_argument('--status'); p.add_argument('--limit', type=int)
    p.set_defaults(func=cmd_list)
    p = sub.add_parser('run'); p.add_argument('id', type=int); p.add_argument('--mode', default='auto')
    p.add_argument('--force', action='store_true'); p.set_defaults(func=cmd_run)
    p = sub.add_parser('auto'); p.add_argument('--limit', type=int); p.add_argument('--mode', default='auto')
    p.add_argument('--workers', type=int); p.add_argument('--coded', action='store_true')
    p.add_argument('--nocode', action='store_true'); p.add_argument('--no-cover', action='store_true')
    p.set_defaults(func=cmd_auto)
    p = sub.add_parser('stats'); p.set_defaults(func=cmd_stats)
    p = sub.add_parser('sem'); p.add_argument('--limit', type=int)
    p.add_argument('--force', action='store_true'); p.add_argument('--unload', action='store_true')
    p.set_defaults(func=cmd_sem)
    p = sub.add_parser('pipeline'); p.add_argument('--limit', type=int)
    p.add_argument('--root', default=ROOT_DEFAULT); p.add_argument('--no-cover', action='store_true')
    p.set_defaults(func=cmd_pipeline)
    a = ap.parse_args()
    a.func(a)


if __name__ == '__main__':
    main()

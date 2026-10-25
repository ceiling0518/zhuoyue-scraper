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
import re
import struct
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
    c.execute('CREATE TABLE IF NOT EXISTS health(item_id INTEGER, file TEXT, cls TEXT, reason TEXT, tier TEXT, size INTEGER, checked_at TEXT, deep INTEGER DEFAULT 0, PRIMARY KEY(item_id,file))')
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
    'zh': ('语言规则：**简体中文和日文原文都允许保留**（不要互相翻译）；'
           '繁体中文转成简体；**其它语言（英/西/韩等）一律翻译成简体中文**；'
           '标签用简体中文。'),
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
# ---------------------------------------------------------------- 信息有效性 / 联网补全
_NOISE_RE = re.compile(
    r'https?://\S+|www\.\S+|\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b|'
    r'【[^】]{0,20}】|\[[^\]]{0,20}\]|\([^)]{0,12}\)|（[^）]{0,12}）')
_AD_WORDS = ('高清', '无码', '有码', '中文字幕', '字幕', '独家', '首发', '完整版',
             '未删减', '收藏', '推荐', '精品', '猎艳', '探花')


def strip_noise(t):
    """去掉网址、括号注释、多余符号；返回可读文本。"""
    x = _NOISE_RE.sub(' ', t or '')
    x = re.sub(r'[\u200b-\u200f\ufeff]', '', x)
    x = re.sub(r'[~!@#$%^&*_=+|\\/<>?"]{2,}', ' ', x)
    x = re.sub(r'\s+', ' ', x).strip(' .,-_')
    return x


def hint_ok(t, min_cjk=4):
    """判断一段标题/简介是否有实际语义（排除 227、KJSDHUIDS11 (13)、纯广告词）。"""
    x = strip_noise(t)
    if not x:
        return False
    _t = x
    for w in _AD_WORDS:
        _t = _t.replace(w, '')
    _t = _t.strip(' -_.,:;|')
    cjk = len(re.findall(r'[\u4e00-\u9fff]', _t))
    if cjk >= min_cjk:
        return True
    core = re.sub(r'[^A-Za-z0-9]', '', _t)
    if not core or core.isdigit():
        return False
    if re.match(r'^[A-Za-z]{1,6}[-_]?\d{2,6}$', core):
        return True
    if core.isalpha():
        if len(core) < 5:
            return False
        if re.search(r'[bcdfghjklmnpqrstvwxyz]{4,}', core.lower()):
            return False
        v = sum(core.lower().count(c) for c in 'aeiou')
        return v / float(len(core)) >= 0.2
    v = sum(core.lower().count(c) for c in 'aeiou')
    return len(core) >= 8 and v / float(len(core)) >= 0.22

def _http_get(url, timeout=20):
    try:
        r = requests.get(url, timeout=timeout, headers={
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/124.0 Safari/537.36'),
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.6'})
        if r.status_code == 200:
            return r.text or ''
    except Exception:
        pass
    return ''


def _blank(t):
    return strip_noise(re.sub(r'<[^>]+>', ' ', t or ''))


def _parse_yandex(html, n):
    out = []
    for m in re.finditer(r'<li[^>]+class="[^"]*serp-item[^"]*"[^>]*>(.*?)</li>', html or '', re.S):
        blk = m.group(1)
        a = re.search(r'<a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>', blk, re.S)
        if not a:
            continue
        title = _blank(a.group(2))
        snips = re.findall(r'<(?:div|span)[^>]*class="[^"]*(?:TextContainer|OrganicText|Text)[^"]*"[^>]*>(.*?)</(?:div|span)>',
                           blk, re.S)
        snip = _blank(max(snips, key=len)) if snips else ''
        if title:
            out.append({'title': title, 'snippet': snip, 'url': a.group(1)})
        if len(out) >= n:
            break
    return out


def _parse_google(html, n):
    out = []
    for m in re.finditer(r'<div[^>]+class="[^"]*\bMjjYud\b[^"]*"[^>]*>(.*?)</div>\s*</div>', html or '', re.S):
        blk = m.group(1)
        a = re.search(r'<a[^>]+href="(/url\?q=|https?://)([^"&]+)[^"]*"[^>]*>(.*?)</a>', blk, re.S)
        if not a:
            continue
        url = a.group(2) if a.group(1).startswith('http') else 'https://' + a.group(2)
        title = _blank(a.group(3))
        snip = ''
        for sm in re.finditer(r'<div[^>]*>(.*?)</div>', blk, re.S):
            txt = _blank(sm.group(1))
            if len(txt) > len(snip):
                snip = txt
        if title:
            out.append({'title': title, 'snippet': snip, 'url': url})
        if len(out) >= n:
            break
    return out


SEARXNG = (os.environ.get('MS_SEARXNG') or 'http://127.0.0.1:8888').rstrip('/')


def searxng_search(q, n=5, engines='google,yandex'):
    """本地 SearxNG（只开 google/yandex）-> [{title,snippet,url}]"""
    try:
        r = requests.get(SEARXNG + '/search',
                         params={'q': q, 'format': 'json', 'engines': engines,
                                 'language': 'zh-CN'},
                         timeout=60)
        if r.status_code != 200:
            return []
        j = r.json()
        out = []
        for x in (j.get('results') or [])[:n]:
            t = (x.get('title') or '').strip()
            if not t:
                continue
            out.append({'title': t,
                        'snippet': (x.get('content') or '').strip(),
                        'url': x.get('url') or ''})
        return out
    except Exception:
        return []


def web_search(q, n=5):
    """SearxNG(google+yandex) -> 直连 Yandex -> 直连 Google -> DDG。"""
    q = (q or '').strip()
    if not q:
        return []
    out = searxng_search(q, n)
    if out:
        return out
    from urllib.parse import quote_plus
    out = _parse_yandex(
        _http_get('https://yandex.com/search/?text=%s&lr=87' % quote_plus(q)), n)
    if out:
        return out
    out = _parse_google(
        _http_get('https://www.google.com/search?q=%s&hl=zh-CN&num=10' % quote_plus(q)), n)
    if out:
        return out
    html = _http_get('https://html.duckduckgo.com/html/?q=%s' % quote_plus(q))
    for m in re.finditer(r'<a rel="nofollow" class="result__a" href="([^"]+)">(.*?)</a>'
                         r'.*?<a class="result__snippet"[^>]*>(.*?)</a>', html or '', re.S):
        out.append({'title': _blank(m.group(2)), 'snippet': _blank(m.group(3)),
                    'url': m.group(1)})
        if len(out) >= n:
            break
    return out


WEB_FILL_PROMPT = (
    '下面是某影片的现有信息与网络搜索结果。请判断哪些信息可信，并给出补全结果。\n'
    '规则：忽略网址、广告词、无意义编号（如 227、KJSDHUIDS11）；\n'
    '拿不到或不确定的字段留空字符串/空数组，不要编造。\n'
    '{LANG}\n'
    '只输出 JSON 对象，不要解释：\n'
    '{"code":"番号（如 SSIS-001，没有就留空）",'
    '"title":"40字内中文标题","plot":"2-3句中文简介","tags":["标签1","标签2"],'
    '"actors":["演员名1","演员名2"],"studio":"制作/发行公司","year":"YYYY"}\n\n'
    '现有：番号=%s | 标题=%s | 简介=%s | 演员=%s | 公司=%s | 年份=%s\n\n搜索结果：\n%s'
)


def lang_ok(t):
    """文本是否符合设置的输出语言（粗略判断，够用即可）。"""
    L = out_lang()
    t = t or ''
    han = len(re.findall(r'[\u4e00-\u9fff]', t))
    kana = len(re.findall(r'[\u3040-\u30ff]', t))
    if L in ('zh', 'zh-Hant', 'ja'):
        return (han + kana) >= 2          # 中文/日文都允许
    return (han + kana) < 2               # en/es：出现中日文视为需要翻译


def _clean_year(y):
    mm = re.search(r'(19|20)\d{2}', str(y or ''))
    return mm.group(0) if mm else ''


def _clean_people(v):
    if isinstance(v, str):
        v = re.split(r'[,，/、|;]+', v)
    out = []
    for x in (v or []):
        x = strip_noise(str(x)).strip(' .,-')
        if 1 < len(x) <= 24 and hint_ok(x, 1):
            out.append(x)
    return list(dict.fromkeys(out))[:12]


def web_complete(item, model=None, log=None):
    """联网补全：搜索 -> 模型抽取。返回 {title,plot,tags} 或 None。"""
    def _log(m):
        if log:
            try:
                log(m)
            except Exception:
                pass
    code = (item.get('sem_code') or item.get('code') or '').strip()
    title = strip_noise(item.get('title') or item.get('name') or '')
    plot = strip_noise(item.get('plot') or '')
    qs = []
    if code and hint_ok(code, 0):
        qs.append(code + ' ' + title[:30])
    if hint_ok(title):
        qs.append(title[:60])
    if not qs:
        qs.append(title[:60] or code)
    hits = []
    for q in qs[:2]:
        hits = web_search(q, 5)
        _log('    搜索「%s」-> %d 条' % (q[:40], len(hits)))
        if hits:
            break
    if not hits:
        return None
    ctx = '\n'.join('%d. %s | %s' % (i + 1, h['title'][:80], h['snippet'][:120])
                    for i, h in enumerate(hits))
    prompt = WEB_FILL_PROMPT % (code or '-', title or '-', plot[:120] or '-',
                                item.get('actors') or '-', item.get('studio') or '-',
                                item.get('year') or '-', ctx)
    prompt = prompt.replace('{LANG}', lang_rule())
    try:
        txt = _chat(prompt, model or ai_model(), 900, 0.2)
        j = extract_json(txt)
        if not isinstance(j, dict):
            arr = extract_json_array(txt)
            if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                j = arr[0]
        if not isinstance(j, dict):
            return None
        t2 = strip_noise(j.get('title') or '')
        p2 = strip_noise(j.get('plot') or '')
        tg = [str(x).strip() for x in (j.get('tags') or []) if str(x).strip()]
        c2 = strip_noise(j.get('code') or '').strip()
        if c2 and not re.match(r'^[A-Za-z]{1,6}[-_]?\d{2,6}$', re.sub(r'[^A-Za-z0-9-]', '', c2)):
            c2 = ''
        if not hint_ok(t2, 3):
            if not c2:
                return None
            t2 = c2           # 标题无效但有合法番号 -> 用番号做标题
        return {'title': t2, 'plot': p2, 'tags': tg[:12],
                'actors': _clean_people(j.get('actors')),
                'studio': strip_noise(j.get('studio') or '')[:40],
                'year': _clean_year(j.get('year')),
                'code': c2.upper() if c2 else ''}
    except Exception as e:
        _log('    web 抽取失败: %s' % str(e)[:80])
        return None


# ---------------------------------------------------------------- 系列表
# ---------------------------------------------------------------- 系列表完善
SERIES_ENRICH_PROMPT = (
    '你是影片厂牌资料助手。根据下面的网络搜索结果，判断这个**系列/厂牌**的信息。\n'
    '系列：%s\n\n搜索结果：\n%s\n\n'
    '规则：拿不到或不确定的字段留空，不要编造；publisher 是制作/发行公司。\n'
    '只输出 JSON 对象：{"publisher":"公司","note":"一句话备注",'
    '"sem_kind":"coded|uncensored|none","has_code":0或1}\n'
)


def series_build(dbc=None, log=None):
    """从 items 的语义数据重建系列表，返回 (系列数, 条目数)。"""
    def _log(x):
        if log:
            try:
                log(x)
            except Exception:
                pass
    d = dbc or db()
    ensure_series(d)
    rows = [dict(r) for r in d.execute(
        'SELECT id,name,code,sem_code,sem_kind,studio FROM items WHERE present=1')]
    groups, spell = {}, {}
    for r in rows:
        _code = (r.get('sem_code') or r.get('code') or '').strip()
        k = series_key(_code or r.get('name') or '')
        if not k:
            continue
        # 只按【有番号值】的条目建系列（无番号的请手工在界面加）
        if not _code:
            continue
        if not series_valid(k):
            continue
        if (r.get('sem_kind') or '') == 'none':
            continue
        gk = k.lower()
        groups.setdefault(gk, []).append(r)
        spell[gk] = spell.get(gk, {})
        spell[gk][k] = spell[gk].get(k, 0) + 1
    n = 0
    # 清掉随机串/超长系列
    try:
        for _r in [dict(x) for x in d.execute('SELECT series FROM series')]:
            if not series_valid(_r['series']):
                d.execute('DELETE FROM series WHERE series=?', (_r['series'],))
        d.commit()
    except Exception:
        pass
    # 清掉历史遗留的纯数字系列
    try:
        d.execute("DELETE FROM series WHERE has_code=0 AND series GLOB '*[^ -~]*'")
        d.execute("DELETE FROM series WHERE series GLOB '[0-9]*' AND series NOT GLOB '*[A-Za-z]*'")
        d.commit()
    except Exception:
        pass
    for gk, its in groups.items():
        k = max(spell[gk], key=spell[gk].get)
        kinds = [x.get('sem_kind') or '' for x in its if (x.get('sem_kind') or '')]
        kind = max(set(kinds), key=kinds.count) if kinds else ''
        pubs = [clean_publisher(x.get('studio')) for x in its
                if clean_publisher(x.get('studio'))]
        pub = max(set(pubs), key=pubs.count) if pubs else ''
        if not pub:
            pub = studio_by_series(k)
        has_code = 1 if any((x.get('sem_code') or x.get('code') or '').strip() for x in its) else 0
        note = ''
        if not has_code and kind in ('none', ''):
            note = '无番号系列'
        series_upsert(k, has_code, kind, note, pub,
                      SERIES_INDEXWORD.get(k.upper(), ''), d)
        n += 1
    _log('系列表：重建 %d 个系列（来自 %d 个条目）' % (n, len(rows)))
    return n, len(rows)


def avfan_publisher(series):
    """avfan.com：系列页 -> 任一影片页 -> JSON-LD 面包屑第 2 项 = 制作商。"""
    code = (series or '').strip().upper()
    if not code:
        return ''
    try:
        h = _http_get('https://avfan.com/en/number_letters/%s' % code)
        m = re.search(r'href="(/en/movies/[^"]+)"', h or '')
        if not m:
            return ''
        d = _http_get('https://avfan.com' + m.group(1))
        m2 = re.search(r'\{"@type":"ListItem","position":2,"name":"([^"]+)"', d or '')
        if not m2:
            return ''
        pub = strip_noise(m2.group(1))
        return clean_publisher(pub)
    except Exception:
        return ''


def series_enrich(one, model=None, log=None, dbc=None):
    """联网查一个系列的公司/备注/语义。返回 True/False。"""
    def _log(x):
        if log:
            try:
                log(x)
            except Exception:
                pass
    one = (one or '').strip()
    if not one:
        return False
    _av = avfan_publisher(one)
    if _av:
        _cur = series_get(one, dbc) or {}
        series_upsert(one, _cur.get('has_code') or 0, _cur.get('sem_kind') or '',
                      _cur.get('note') or '', _av, dbc=dbc)
        _log('    avfan 制作商: %s' % _av)
        return True
    hits = web_search(one + ' 作品 制作公司 发行', 5)
    _log('  搜索「%s」-> %d 条' % (one, len(hits)))
    if not hits:
        return False
    ctx = '\n'.join('%d. %s | %s' % (i + 1, h['title'][:80], h['snippet'][:120])
                    for i, h in enumerate(hits))
    try:
        txt = _chat(SERIES_ENRICH_PROMPT % (one, ctx), model or ai_model(), 500, 0.2)
        j = extract_json(txt)
        if not isinstance(j, dict):
            return False
        pub = clean_publisher(j.get('publisher') or '')
        note = strip_noise(j.get('note') or '')[:60]
        kind = (j.get('sem_kind') or '').strip()
        if kind not in ('coded', 'uncensored', 'none'):
            kind = ''
        try:
            hc = int(j.get('has_code') or 0)
        except Exception:
            hc = 0
        if not (pub or note or kind):
            return False
        cur = series_get(one, dbc) or {}
        series_upsert(one, hc if hc else (cur.get('has_code') or 0),
                      kind or (cur.get('sem_kind') or ''),
                      note or (cur.get('note') or ''),
                      pub or (cur.get('publisher') or ''), dbc)
        _log('    -> 公司 %s | 备注 %s | %s' % (pub or '-', note or '-', kind or '-'))
        return True
    except Exception as e:
        _log('    ERR %s' % str(e)[:100])
        return False

# 4 列：系列 / 番号有无 / 语义 / 备注
# 语义识别与改名共用：7Boss 这类国产系列没有番号，靠 series 归类。
def ensure_series(dbc=None):
    d = dbc or db()
    d.execute("""CREATE TABLE IF NOT EXISTS series(
        series TEXT PRIMARY KEY,
        has_code INTEGER DEFAULT 0,
        sem_kind TEXT,
        note TEXT,
        publisher TEXT,
        alias TEXT,
        updated TEXT)""")
    for _col in ('publisher', 'alias'):
        try:
            d.execute('ALTER TABLE series ADD COLUMN %s TEXT' % _col)   # 旧库补列
        except Exception:
            pass
    d.commit()
    try:
        rows = [dict(x) for x in d.execute('SELECT * FROM series').fetchall()]
        for r in rows:
            k = series_key(r['series'])
            if k and k != r['series']:
                d.execute("""INSERT INTO series(series,has_code,sem_kind,note,publisher,updated)
                             VALUES(?,?,?,?,?,?)
                             ON CONFLICT(series) DO UPDATE SET
                               sem_kind=CASE WHEN excluded.sem_kind<>'' THEN excluded.sem_kind ELSE series.sem_kind END,
                               note=CASE WHEN excluded.note<>'' THEN excluded.note ELSE series.note END,
                               publisher=CASE WHEN excluded.publisher<>'' THEN excluded.publisher ELSE series.publisher END,
                               updated=excluded.updated""",
                          (k, r['has_code'], r['sem_kind'] or '', r['note'] or '',
                           r['publisher'] or '', now()))
                d.execute('DELETE FROM series WHERE series=?', (r['series'],))
        d.commit()
    except Exception:
        pass
    return d


def series_upsert(series, has_code, sem_kind='', note='', publisher='', alias='', dbc=None):
    """有则更新；空值不覆盖已有语义/备注。"""
    series = (series or '').strip()
    if not series:
        return None
    d = dbc or db()
    ensure_series(d)
    d.execute("""INSERT INTO series(series,has_code,sem_kind,note,publisher,alias,updated)
                 VALUES(?,?,?,?,?,?,?)
                 ON CONFLICT(series) DO UPDATE SET
                   has_code=excluded.has_code,
                   sem_kind=CASE WHEN excluded.sem_kind<>'' THEN excluded.sem_kind
                                 ELSE series.sem_kind END,
                   note=CASE WHEN excluded.note<>'' THEN excluded.note
                             ELSE series.note END,
                   publisher=CASE WHEN excluded.publisher<>'' THEN excluded.publisher
                                  ELSE series.publisher END,
                   alias=CASE WHEN excluded.alias<>'' THEN excluded.alias
                              ELSE series.alias END,
                   updated=excluded.updated""",
              (series, 1 if has_code else 0, sem_kind or '', note or '',
               publisher or '', alias or '', now()))
    d.commit()
    return series


def series_get(name, dbc=None):
    try:
        r = (dbc or db()).execute('SELECT * FROM series WHERE series=?',
                                  ((name or '').strip(),)).fetchone()
        return dict(r) if r else None
    except Exception:
        return None


AV_STUDIOS = {}
try:
    _ap = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'av_studios.json')
    if os.path.exists(_ap):
        AV_STUDIOS = json.load(open(_ap, encoding='utf-8'))
except Exception:
    AV_STUDIOS = {}


def studio_by_series(k):
    """按 AvNumber 映射查片商：先全大写去分隔符，再试原样。"""
    if not k:
        return ''
    for key in (k.upper().replace('-', '').replace('_', ''),
                '300' + k.upper().replace('-', '').replace('_', ''),
                k.upper()):
        info = AV_STUDIOS.get(key)
        if info and info.get('publisher'):
            return info['publisher']
    return ''


PUB_BAD = ('javbus', 'javdb', 'missav', '123av', 'njav', 'supjav', 'freejav',
           'javlibrary', 'avmoo', 'searxng', 'google', 'yandex', 'baidu', 'bing',
           'dmm.co.jp', 'dmm.com', 'javfree')


def clean_publisher(p):
    """发行商清洗：站点名/搜索引擎名不是发行商。"""
    x = strip_noise(p or '')[:60]
    low = x.lower()
    if not x:
        return ''
    if any(b in low for b in PUB_BAD):
        return ''
    return x.strip(' .-')[:40]


def normalize_code(c):
    """番号规范化：OSTP10034 -> OSTP-10034；STP36113 -> STP-36113。"""
    x = (c or '').strip().upper()
    if not x:
        return ''
    x = re.sub(r'[\s_]+', '-', x)
    x = re.sub(r'-{2,}', '-', x).strip('-')
    mm = re.match(r'^([A-Z]{2,6})(\d{2,7})$', x)
    if mm:
        return mm.group(1) + '-' + mm.group(2)
    return x


SERIES_ALIAS = {
    'FC2PPV': 'FC2', 'FC2-PPV': 'FC2', 'FC2PPV-': 'FC2',
    'CARIBBEANCOM': 'CARIB', 'CARIBBEAN': 'CARIB',
    '1PON': '1PONDO', '1PONDO': '1PONDO',
    '300MIUM': 'Mium', 'MIUM': 'Mium',
}
# 规范名 -> 索引词（没有就不写）
SERIES_INDEXWORD = {
    'FC2': 'FC2PPV',
    'CARIB': 'CARIBBEANCOM,加勒比',
    '1PONDO': '1Pon,一本道',
    'Mium': '300MIUM',
}


def series_valid(k):
    """系列名合法性：拒绝随机串（NCXJUIQW21）、纯数字、过长。"""
    k = (k or '').strip()
    if not k or len(k) > 8 or re.fullmatch(r'\d+', k):
        return False
    core = re.sub(r'[^A-Za-z0-9]', '', k)
    if not core:
        return False
    low = core.lower()
    if re.search(r'[bcdfghjklmnpqrstvwxyz]{5,}', low):
        return False
    if re.search(r'\d$', core) and len(k) > 6 and '-' not in k and '_' not in k:
        return False
    if core.isalpha():
        if len(core) >= 5:
            v = sum(low.count(c) for c in 'aeiou')
            if v / float(len(core)) < 0.2:
                return False
    return True


def series_key(raw):
    """番号 -> 系列：JUR-212 -> JUR；FC2-PPV-4544125 -> FC2。"""
    t = strip_noise(raw or '')
    t = re.sub(r'\.(mp4|mkv|avi|wmv|mov|ts|m2ts)$', '', t, flags=re.I)
    t = normalize_code(t)                             # OSTP10016 -> OSTP-10016
    t = re.sub(r'^\d{2,5}\s*[-_]\s*', '', t)      # 去掉目录序号 0005-
    head = re.split(r'[\s\-_—.:：|/\\]+', t)[0] if t else ''
    head = head.strip(' .-_')
    if not head or re.fullmatch(r'\d+', head):
        return ''
    head = head[:20]
    return SERIES_ALIAS.get(head.upper(), head)


def series_of_item(item, dbc=None):
    """从条目推断系列：番号前缀（JUR），没有番号再看名字首段。"""
    code = (item.get('sem_code') or item.get('code') or '').strip()
    if code:
        k = series_key(code)
        if k:
            return k
    name = (item.get('name') or '').strip()
    k = series_key(name)
    return k if series_valid(k) else ''

def safe_name(s, maxlen=60):
    """去掉非法字符与控制符，压缩空格，限长。"""
    s = _RENAME_BAD.sub(' ', s or '')
    s = re.sub(r'[\u200b-\u200f\ufeff]', '', s)
    s = re.sub(r'\s+', ' ', s).strip(' .')
    if len(s) > maxlen:
        s = s[:maxlen].rstrip(' .')
    return s


RENAME_PROMPT = (
    '你是媒体库命名助手。按下面规范为每条影片生成新的【目录名】和【视频文件名】：\n'
    '1) 目录名格式：番号-标题（番号优先用 sem_code，没有就用 code；都没有就只用标题）\n'
    '2) 文件名：与目录名相同 + 原扩展名；同一目录有多个视频时依次加 -1、-2\n'
    '3) 标题去掉网址、广告词、emoji、多余符号和空格，保留中文，总长不超过 60 字\n'
    '4) 不得包含 \\ / : * ? " < > | 这些字符，不要带路径\n'
    '严格只输出 JSON 数组，长度必须为 {N}，每项形如 '
    '{{"dir":"番号-标题","file":"番号-标题.mp4","why":"一句话理由"}}，不要任何解释。\n\n{ITEMS}'
)


def _rn_vids(p):
    try:
        return sorted([os.path.join(p, f) for f in os.listdir(p)
                       if os.path.splitext(f)[1].lower() in VIDEO_EXT],
                      key=os.path.getsize, reverse=True)
    except Exception:
        return []


def rename_plan(items, model=None, log=None, root=False):
    """AI 生成改名计划；失败时退回模板。返回 [{id,dir,file,why,old_dir,old_file}]"""

    def _log(m):
        if log:
            try:
                log(m)
            except Exception:
                pass

    model = model or ai_model()
    lines = []
    for i, it in enumerate(items, 1):
        p = it.get('path') or ''
        d = os.path.basename(os.path.normpath(p)) if p else ''
        code = (it.get('sem_code') or it.get('code') or '').strip()
        title = (it.get('title') or it.get('name') or '').strip()
        vids = _rn_vids(p) if p else []
        vn = ', '.join(os.path.basename(x) for x in vids[:4])
        lines.append('%d. 现目录: %s | 番号: %s | 标题: %s | 视频: %s'
                     % (i, d, code, title[:70], vn[:120]))
    prompt = (RENAME_PROMPT.replace('{N}', str(len(items)))
              .replace('{ITEMS}', '\n'.join(lines)))
    arr = None
    try:
        arr = extract_json_array(_chat(prompt, model, 4000, 0.2))
    except Exception as e:
        _log('  AI 改名失败，使用模板: %s' % str(e)[:80])
    if not isinstance(arr, list) or len(arr) != len(items):
        arr = None

    out = []
    for i, it in enumerate(items):
        p = it.get('path') or ''
        old_dir = os.path.basename(os.path.normpath(p)) if p else ''
        vids = _rn_vids(p) if p else []
        main = os.path.basename(vids[0]) if vids else ''
        ext = os.path.splitext(main)[1] or '.mp4'
        code = (it.get('sem_code') or it.get('code') or '').strip()
        title = (it.get('title') or it.get('name') or '').strip()
        base = safe_name(('%s-%s' % (code, title)) if code else title)
        if not base:
            base = safe_name(old_dir, 60) or ('item-%s' % it.get('id'))
        nd, nf, why = base, base + ext, '模板'
        if arr:
            a = arr[i] if isinstance(arr[i], dict) else {}
            nd2 = safe_name(a.get('dir') or '')
            nf2 = safe_name(a.get('file') or '')
            if nd2:
                nd = nd2
            if nf2:
                ne = os.path.splitext(nf2)[1]
                nf = nf2 if ne else (nf2 + ext)
            why = (a.get('why') or '')[:60] or 'AI'
        old_root = os.path.basename(os.path.dirname(os.path.normpath(p))) if p else ''
        new_root = ''
        if root and p:
            try:
                _sr = os.path.abspath(scan_root())
                if os.path.abspath(os.path.dirname(os.path.normpath(p))) != _sr:
                    _sr_row = series_get(series_of_item(it))
                    new_root = safe_name((_sr_row or {}).get('series') or code or old_root, 40)
            except Exception:
                new_root = ''
        out.append({'id': it.get('id'), 'old_dir': old_dir, 'new_dir': nd,
                    'old_file': main, 'new_file': nf, 'why': why,
                    'old_root': old_root, 'new_root': new_root,
                    'videos': [os.path.basename(x) for x in vids]})
    return out


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
        d['source'] = {'javbus': 'Web', 'javdb': 'Web', 'missav': 'Web'}.get(t, 'AI')
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
    return ('缺' if src in ('Web', 'code', 'Uncen') else '无'), src


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
    try:
        dbc.execute('ALTER TABLE items ADD COLUMN dir_mtime INTEGER')   # 增量判据
        dbc.commit()
    except Exception:
        pass
    if mode == 'rebuild':
        dbc.execute("INSERT INTO archive(id,path,name,code,title,quality,source,archived_at,reason) "
                    "SELECT id,path,name,code,title,quality,source,?, 'rebuild' FROM items", (ts,))
        dbc.execute('DELETE FROM items')
        dbc.commit()
        known = {}
    else:
        known = {r['path']: (r['id'], int(r['dir_mtime'] or 0))
                 for r in dbc.execute('SELECT id, path, dir_mtime FROM items')}
    seen, added, changed, skipped = set(), 0, 0, 0
    if not os.path.isdir(root):
        return {'error': 'root unreachable: %s' % root}
    for name in sorted(os.listdir(root)):
        p = os.path.join(root, name)
        if not os.path.isdir(p):
            continue
        seen.add(p)
        try:
            mt = int(os.stat(p).st_mtime)
        except Exception:
            mt = 0
        if mode != 'rebuild' and p in known and mt and known[p][1] == mt:
            # 目录 mtime 没变 -> 内容不可能变：不读 nfo、不 listdir、不查嵌套
            skipped += 1
            dbc.execute('UPDATE items SET last_scan=? WHERE id=?', (ts, known[p][0]))
            continue
        code = JS.find_code(name) or ''
        if code and not is_uncensored(name):
            code = strip_code_prefix(code)          # 剥前导数字（coded 风格目录）
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
                                             tags,actors,studio,year,first_seen,last_scan,present,dir_mtime)
                           VALUES(?,?,?,'empty',?,?,?,?,?,?,?,?,?,?,?,1,?)""",
                        (p, name, code, q, src, kk, nf.get('title', ''), nf.get('plot', ''),
                         nf.get('tags', ''), nf.get('actors', ''), nf.get('studio', ''),
                         nf.get('year', ''), ts, ts, mt))
            added += 1
        else:
            iid = known[p][0]
            r = dbc.execute('SELECT code,quality,source,kind,present FROM items WHERE id=?',
                            (iid,)).fetchone()
            if (r['code'] != code or (r['quality'] or '') != q or (r['source'] or '') != src
                    or (r['kind'] or '') != kk or r['present'] != 1):
                dbc.execute('UPDATE items SET code=?,quality=?,source=?,kind=?,present=1,'
                            'last_scan=?,dir_mtime=? WHERE id=?', (code, q, src, kk, ts, mt, iid))
                changed += 1
            else:
                dbc.execute('UPDATE items SET last_scan=?, dir_mtime=? WHERE id=?'
                            , (ts, mt, iid))
            if nfo:      # mirror the on-disk nfo into the row (list needs no NAS read)
                dbc.execute('UPDATE items SET title=?,plot=?,tags=?,actors=?,studio=?,year=? '
                            'WHERE id=?',
                            (nf['title'], nf['plot'], nf['tags'], nf['actors'],
                             nf['studio'], nf['year'], iid))
    removed = 0
    for p, _v in known.items():
        i = _v[0]
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
            'skipped': skipped, 'total': len(seen), 'root': root}


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
CODE_SUFFIX_RE = re.compile(r'^(.+?\d)[-_ ]?(HD|SD|FHD|4K|C|J|X|U)$', re.I)


def split_code(code):
    """拆出 (基础番号, 版本后缀)。ABP-123-HD -> ('ABP-123', 'HD')。

    后缀只描述这一份拷贝的版本，不参与匹配 —— 搜站前要剥掉。
    """
    c = re.sub(r'\s+', ' ', (code or '').strip())
    m = CODE_SUFFIX_RE.match(c)
    if m:
        return m.group(1).upper(), m.group(2).upper()
    return c.upper(), ''

# 前导数字不是番号：480FRIN-052 -> FRIN-052
# （coded 用；uncensored 的 10musume / 1pondo 不能剥 —— 数字是站点名的一部分）
CODE_PREFIX_RE = re.compile(r'^\d{2,4}[-_ ]?(?=[A-Za-z]{2,6}[-_ ]?\d)', re.I)


def strip_code_prefix(code):
    # 剥掉 coded 番号的前导数字；uncensored 不要调用
    c = re.sub(r'\s+', ' ', (code or '').strip())
    m = CODE_PREFIX_RE.match(c)
    return (c[m.end():] if m else c).upper()


# 无码站点系列：数字是系列名的一部分，不能剥、也不能改大小写
UNCENSORED_SERIES_RE = re.compile(
    r'^(10musume|1pondo|caribbeancom|pacopacomama|tokyo-?hot|heyzo|heydouga|'
    r'muramura|mesubuta|gachinco|xxx-?av)\b', re.I)
CODE_DASH_RE = re.compile(r'^([A-Za-z]{2,6})[-_ ]?(\d{2,5})$')


def is_uncensored_series(code):
    # 10musume / 1pondo / caribbeancom … 这类，数字就是系列名的一部分
    return bool(UNCENSORED_SERIES_RE.match((code or '').strip()))


def norm_code_for_kind(kind, code):
    # 统一归一化入口，返回 (kind, code)。
    # · 无码站点系列 -> 强制 uncensored 且原样保留（只清空白/前导标点）
    # · coded        -> 剥版本后缀 + 剥前导数字 + 字母与数字间补 '-'
    # · 其它         -> 只清空白/前导标点
    c = re.sub(r'\s+', ' ', (code or '').strip())
    c = re.sub(r'^[:：\s]+', '', c)
    if is_uncensored_series(c):
        return 'uncensored', c
    if kind == 'coded':
        c = split_code(c)[0]                     # 版本后缀（C/X/J/HD…）不属于番号
        c = strip_code_prefix(c)                 # 前导数字前缀也不属于番号
        m = CODE_DASH_RE.match(c)
        if m:
            c = '%s-%s' % (m.group(1), m.group(2))   # OYC263 -> OYC-263
        c = c.upper()
    return kind, c



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


def ai_video_scan(item, model=None, n=None):
    """看视频截图 -> {title, plot, tags}；失败返回 None。

    抽帧密度：<30 分钟固定 10 帧；>=30 分钟按每 3 分钟一帧（最多 40 帧）。
    """
    model = model or ai_model()
    v = biggest_video(item.get('path') or '')
    if not v:
        return None
    vp = os.path.join(item['path'], v)
    if n is None:
        try:
            _d = video_duration(vp)
        except Exception:
            _d = 0
        n = 10 if _d < 1800 else max(10, min(40, int(_d // 180) + 1))
    frames = video_frames(vp, n=n)
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
- 10musume / 天然素人：code 形如 10musume-093022_01，
  保留小写与下划线，不要加横杠、不要改大小写
- 版本后缀不算番号：C/X/J 是字幕/破解标记，HD/SD/4K 是画质标记，不要写进 code；例：SVMGM-052C 的番号是 SVMGM-052
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
        code = re.sub(r'^[:：\s]+', '', str(j.get('code') or '').strip())
        kind, code = norm_code_for_kind(kind, code)
        out.append({'kind': kind, 'code': code})
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
- 10musume / 天然素人：code 形如 10musume-093022_01，
  保留小写与下划线，不要加横杠、不要改大小写
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
        kind, code = norm_code_for_kind(kind, code)
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
    # title 为空时不要再拿目录名兜底：之前会把 bydda.cc@STAR395 这种目录名写进 nfo，
    # 被 Jellyfin 当成正式标题。空就干脆不写 <title>，让 Jellyfin 自己决定。
    title = (item['title'] or '').strip()
    tags = split_list(item['tags'])
    actors = split_list(item['actors'])
    # a real year only -- '(\d{4})' used to grab '4716' out of an FC2 code
    m = (re.search(r'(19|20)\d{2}', item['code'] or '')
         or re.search(r'(19|20)\d{2}', item['title'] or '')
         or re.search(r'(19|20)\d{2}', item['name'] or ''))
    year = m.group(0) if m else ''
    site = item['site'] if item['site'] in ('javbus', 'javdb') else 'local'
    studio = (item['studio'] or '').strip() if item.get('studio') else ''
    src = item.get('source') or ('Web' if item['site'] in ('javbus', 'javdb', 'missav')
                                 else 'AI')
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
    source = {'javbus': 'Web', 'javdb': 'Web', 'missav': 'Web', 'ai': 'AI'}.get(
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


# ------------------------------------------------------------------ 广告检测
# ---------------- 广告快照库 + dhash 快路 ----------------
AD_LIB_TOL = 6          # 汉明距 <= 6 视为同一张广告卡
AD_FINE = 0.25          # 边界精定位步长（秒）


def _dh9x8(buf):
    """72 字节 9x8 灰度 -> 64bit 差分哈希"""
    h = 0
    for y in range(8):
        row = buf[y * 9:y * 9 + 9]
        for x in range(8):
            h = (h << 1) | (1 if row[x] < row[x + 1] else 0)
    return h


def _ham(a, b):
    return bin((int(a) ^ int(b)) & 0xFFFFFFFFFFFFFFFF).count('1')


def _dhash_jpeg(jpg):
    """jpeg bytes -> dhash（一次微型 ffmpeg，约 30-60ms）"""
    if not jpg:
        return None
    try:
        r = subprocess.run([FFMPEG, '-hide_banner', '-v', 'error', '-i', 'pipe:0',
                            '-vf', 'scale=9:8,format=gray', '-f', 'rawvideo',
                            '-pix_fmt', 'gray', 'pipe:1'],
                           input=jpg, capture_output=True, timeout=60)
        if r.stdout and len(r.stdout) >= 72:
            return _dh9x8(r.stdout[:72])
    except Exception:
        pass
    return None


def _adlib_ready(dbc):
    dbc.execute('CREATE TABLE IF NOT EXISTS adlib(h INTEGER PRIMARY KEY,'
                ' hits INTEGER DEFAULT 1, path TEXT, seen_at TEXT)')
    dbc.commit()


def _adlib_best(dbc, h):
    """库中最接近的哈希 -> (h, 汉明距)；空库 (None, 64)"""
    if h is None:
        return None, 64
    try:
        _adlib_ready(dbc)
        rows = dbc.execute('SELECT h FROM adlib').fetchall()
    except Exception:
        return None, 64
    best, bd = None, 64
    for (x,) in rows:
        d = _ham(h, x)
        if d < bd:
            best, bd = x, d
    return best, bd


def _adlib_add(dbc, h, path=''):
    """收录一张广告快照；已存在（汉明距 <= 阈值）则只累加 hits"""
    import datetime as _dt
    if h is None:
        return False
    try:
        _adlib_ready(dbc)
        rows = dbc.execute('SELECT h FROM adlib').fetchall()
        now = _dt.datetime.now().isoformat(' ', 'seconds')
        for (x,) in rows:
            if _ham(h, x) <= AD_LIB_TOL:
                dbc.execute('UPDATE adlib SET hits=hits+1, seen_at=? WHERE h=?', (now, x))
                dbc.commit()
                return False
        dbc.execute('INSERT OR IGNORE INTO adlib(h,hits,path,seen_at) VALUES(?,1,?,?)',
                    (int(h), os.path.basename(path or ''), now))
        dbc.commit()
        return True
    except Exception:
        return False


def dhash_window(vpath, start, dur, step=AD_FINE, max_frames=8000):
    """流式解码 [start, start+dur]，按 step 采样 -> [(绝对秒, h), ...]"""
    cmd = [FFMPEG, '-hide_banner', '-v', 'error', '-nostdin']
    if start > 0.02:
        cmd += ['-ss', '%.3f' % start]
    cmd += ['-i', vpath, '-t', '%.3f' % max(0.1, dur),
            '-vf', 'fps=1/%g,scale=9:8,format=gray' % step,
            '-f', 'rawvideo', '-pix_fmt', 'gray', 'pipe:1']
    out = []
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0)
    try:
        k = 0
        while k < max_frames:
            buf = p.stdout.read(72)
            if not buf or len(buf) < 72:
                break
            out.append((round(start + k * step, 2), _dh9x8(buf)))
            k += 1
    finally:
        try:
            p.stdout.close()
        except Exception:
            pass
        try:
            p.kill()
        except Exception:
            pass
    return out


def _edge_dhash(vpath, ref, t_from, t_to, want_start, model=None, log=None):
    """在窗口内 0.25s 采样，找广告段边界（命中 ref 的首/末时刻）。
    命中不到（动态广告）则退回模型二分。"""
    lo, hi = sorted([float(t_from), float(t_to)])
    lo = max(0.0, lo)
    win = max(2.0, hi - lo)
    got = dhash_window(vpath, lo, win, AD_FINE)
    hit = [t for t, h in got if _ham(h, ref) <= AD_LIB_TOL]
    if hit:
        return (min(hit) if want_start else max(hit))
    if log:
        log('    dhash 边界未命中，退回模型二分 [%.0f, %.0f]' % (lo, hi))
    return _refine(vpath, lo if want_start else hi, hi if want_start else lo, model)


AD_PROMPT = (
    '下面是同一个视频在不同时间点截取的 {N} 张画面（按顺序编号 1..{N}）。\n'
    '请判断每张画面是否属于"广告/片头/片尾/宣传卡片/与正片无关的页面"，'
    '例如：网站宣传页、盗版站/发片站水印页、二维码、纯文字台标页、试看提示、订阅引导、'
    '片尾感谢/招募/合集宣传、黑底白字告示等。\n'
    '只要画面是正常视频内容（人物/场景/动作），就算"非广告"。\n'
    '只输出 JSON：{"ad":[是广告的编号...]}；没有广告输出 {"ad":[]}。不要输出其它文字。'
)


def frame_at(vpath, t, width=480):
    """取某一时刻的一帧（jpeg bytes），失败返回 None。"""
    try:
        r = subprocess.run([FFMPEG, '-v', 'error', '-ss', '%.2f' % max(0.0, t), '-i', vpath,
                            '-frames:v', '1', '-vf', 'scale=%d:-1' % width,
                            '-f', 'image2', '-vcodec', 'mjpeg', '-'],
                           capture_output=True, timeout=120)
        if r.stdout and len(r.stdout) > 800:
            return r.stdout
    except Exception:
        pass
    return None


def ads_batch(vpath, times, model=None, log=None):
    """一次请求多张截图，返回与 times 等长的 bool 列表。
    批大小默认 20（MS_AD_BATCH 可调）：图越多请求次数越少，整体更快。"""
    model = model or ai_model()
    out = [False] * len(times)
    try:
        chunk = max(1, min(24, int(os.environ.get('MS_AD_BATCH') or 20)))
    except Exception:
        chunk = 20
    for s in range(0, len(times), chunk):
        ts = times[s:s + chunk]
        imgs, keep = [], []
        # 抽帧是这里的真瓶颈（NAS 上每帧 ~0.3s，串行 24 帧要 7s）。
        # 多线程并发抽帧实测 8 线程 24 帧 1.0s，约 7 倍。
        # 判广告不需要大图：320 宽比 480 宽模型快 45%，识别没差别（MS_AD_WIDTH 可调）
        try:
            _fw = max(160, min(640, int(os.environ.get('MS_AD_WIDTH') or 320)))
        except Exception:
            _fw = 320
        _bs = None
        try:
            from concurrent.futures import ThreadPoolExecutor
            _w = min(8, max(1, (os.cpu_count() or 4)))
            with ThreadPoolExecutor(max_workers=_w) as _ex:
                _bs = list(_ex.map(lambda _t: frame_at(vpath, _t, width=_fw), ts))
        except Exception:
            _bs = None
        if _bs is None:
            _bs = [frame_at(vpath, t, width=_fw) for t in ts]
        for i, b in enumerate(_bs):
            if b:
                imgs.append(base64.b64encode(b).decode('ascii'))
                keep.append(i)
        if not imgs:
            continue
        _t0 = time.time()
        prompt = AD_PROMPT.replace('{N}', str(len(imgs)))
        try:
            r = requests.post(OLLAMA + '/api/chat', timeout=900,
                              json={'model': model, 'stream': False,
                                    'keep_alive': AI_KEEP_ALIVE, 'think': False,
                                    'options': {'temperature': 0.1, 'num_predict': 120},
                                    'messages': [{'role': 'user', 'content': prompt,
                                                  'images': imgs}]})
            txt = ((r.json().get('message') or {}).get('content') or '') if r.status_code == 200 else ''
            j = extract_json(txt) or {}
            ad = j.get('ad') or []
            for x in ad:
                try:
                    idx = int(x) - 1
                except Exception:
                    continue
                if 0 <= idx < len(imgs):
                    out[s + keep[idx]] = True
            if log:
                log('    批次 %d 张 -> 广告 %d 张（%.1fs）'
                    % (len(imgs), len([x for x in ad]), time.time() - _t0))
        except Exception as e:
            if log:
                log('    ads_batch err %s' % str(e)[:80])
    return out


def _is_ad(vpath, t, model=None):
    return ads_batch(vpath, [t], model=model)[0]


def _refine(vpath, t_ad, t_nonad, model=None, n=8):
    """在 [min,max] 之间一次采 n 点（1 次模型调用）找交界。

    原来是二分：每次 1 张图，30s 窗口要 5~6 次调用（每次 2-7 秒）。
    现在整窗口打包成 1 次请求，精度 (hi-lo)/n ≈ 4 秒，快 5 倍。
    """
    lo, hi = sorted([float(t_ad), float(t_nonad)])
    if hi - lo < 1.0:
        return lo
    ts = [lo + (hi - lo) * (i + 0.5) / n for i in range(n)]
    flags = ads_batch(vpath, ts, model=model)
    if (not any(flags)) or all(flags):
        # 8 点全同 -> 这批没分辨出交界（模型漏判/广告只占几秒），
        # 退回精确二分，避免边界落到窗口端点（曾把 1002s 判成 972s，多切 30s）
        is_ad_lo = _is_ad(vpath, lo, model)
        while hi - lo > 1.0:
            mid = (lo + hi) / 2.0
            if _is_ad(vpath, mid, model) == is_ad_lo:
                lo = mid
            else:
                hi = mid
        return lo if is_ad_lo else hi
    for i in range(1, n):
        if bool(flags[i]) != bool(flags[0]):
            return (ts[i - 1] + ts[i]) / 2.0
    return hi


AD_MIN_SEC = 10.0      # 短于此的"广告"直接忽略（误判/闪标，切了得不偿失）


def set_ad_min(v):
    global AD_MIN_SEC
    try:
        AD_MIN_SEC = max(0.0, min(600.0, float(v)))
    except Exception:
        pass
    return AD_MIN_SEC


def detect_ads(vpath, dur=None, model=None, log=None, mode='headtail', edge_sec=240):
    """广告检测 v2：30s 网格 + 广告快照库(dhash) + 0.25s dhash 边界精定位。
    - 网格帧先算 dhash 查快照库，命中直接判广告（0 次模型调用）
    - 未命中才送模型（8 张/批）
    - 边界用 dhash 精定位，不再逐次调模型
    返回 [[t0,t1], ...]（秒）。"""
    dur = dur or video_duration(vpath)
    if not dur or dur < 20:
        return []

    def _log(m):
        if log:
            try:
                log(m)
            except Exception:
                pass

    dbc = db()

    # 1) 预检：随机 30s 段 3 帧 + 尾部 3 帧
    import random as _rnd
    if mode == 'headtail':
        # 只关心头尾：预检也只采片头/片尾，避免中段画面误触
        probes = sorted(set(round(p, 1) for p in
                            [0.5, 2.0, 5.0, 10.0,
                             max(dur - 8, 0), max(dur - 3, 0), max(dur - 1, 0)]
                            if 0 <= p < dur))
    else:
        r0 = _rnd.uniform(0.05, 0.8) * dur
        probes = sorted(set(round(p, 1) for p in
                            [0.5, 2.0, r0, r0 + 10,
                             max(dur - 8, 0), max(dur - 3, 0), max(dur - 1, 0)]
                            if 0 <= p < dur))
    pf = ads_batch(vpath, probes, model=model, log=_log)
    _log('  预检 %d 张 -> 广告 %d 张' % (len(pf), sum(pf)))
    if not any(pf):
        return []

    # 2) 30s 网格 + 末端；每点取帧算 dhash，先查快照库
    if mode == 'headtail':
        # 只扫前 edge_sec 秒和后 edge_sec 秒，15 秒一个点
        e = min(float(edge_sec), dur / 2.0)
        times = [float(t) for t in range(0, max(1, int(e)), 15)]
        tail0 = max(0.0, dur - e)
        times += [round(tail0 + i * 15.0, 1)
                  for i in range(int((dur - tail0) // 15) + 1)]
        times = sorted(set(round(t, 1) for t in times if 0 <= t < dur))
        _log('  只查头尾各 %.0f 秒：%d 个采样点' % (e, len(times)))
    else:
        times = [float(t) for t in range(0, int(dur), 30)]
        tail = [round(dur - 1, 1)]
        for t in (dur - 120, dur - 60, dur - 30):
            if t > 1 and t < dur:
                tail.append(round(t, 1))
        times = sorted(set(times + tail))
    hmap, ask = {}, []
    for t in times:
        h = _dhash_jpeg(frame_at(vpath, t))
        hmap[t] = h
        _, d = _adlib_best(dbc, h)
        if d <= AD_LIB_TOL:
            hmap[t] = ('LIB', h)
        else:
            ask.append(t)
    lib_n = len(times) - len(ask)
    _log('  网格 %d 点：库命中 %d，需模型 %d' % (len(times), lib_n, len(ask)))

    flags = {}
    for t in times:
        v = hmap.get(t)
        flags[t] = True if (isinstance(v, tuple) and v[0] == 'LIB') else False
    if ask:
        got = ads_batch(vpath, ask, model=model, log=_log)
        for t, f in zip(ask, got):
            flags[t] = bool(f)
            if f:
                h = hmap.get(t)
                if isinstance(h, tuple):
                    h = h[1]
                _adlib_add(dbc, h, vpath)
    _log('  网格 %d 点 -> 广告 %d 点' % (len(times), sum(1 for t in times if flags[t])))
    adt = [t for t in times if flags[t]]
    if not adt:
        return []

    # 3) 连续广告点成段（间隔 <= 31s 视为同段）
    segs, cur = [], [adt[0], adt[0]]
    for t in adt[1:]:
        if t - cur[1] <= 31:
            cur[1] = t
        else:
            segs.append(cur)
            cur = [t, t]
    segs.append(cur)

    # 4) 边界：0.25s dhash 精定位（零模型调用）
    def _ref_of(t):
        for tt in (t, min(times, key=lambda x: abs(x - t))):
            v = hmap.get(tt)
            if isinstance(v, tuple):
                return v[1]
            if v is not None:
                return v
        return None

    out = []
    for a, b in segs:
        ref = _ref_of(a) or _ref_of(b)
        if ref is None:
            out.append([round(a, 1), round(b, 1)])
            continue
        if a <= 1:
            t0 = 0.0
        else:
            t0 = _edge_dhash(vpath, ref, max(0.0, a - 32), a + 1, True, model, _log)
        if b >= dur - 40:
            t1 = float(dur)
        else:
            t1 = _edge_dhash(vpath, ref, b - 1, min(dur, b + 32), False, model, _log)
        if t1 - t0 < 0.8:
            t0, t1 = float(a), float(b)
        out.append([round(t0, 1), round(t1, 1)])
        _log('  区间 粗[%.0f-%.0f] -> 精[%.1f-%.1f]' % (a, b, t0, t1))
    if mode == 'headtail':
        e = min(float(edge_sec), dur / 2.0)
        keep2 = []
        for x0, x1 in out:
            if x0 <= e or x1 >= dur - e:
                keep2.append([x0, x1])
            else:
                _log('  丢弃中段区间 [%.1f-%.1f]（头尾模式）' % (x0, x1))
        out = keep2
    if AD_MIN_SEC > 0:
        keep3 = []
        for x0, x1 in out:
            if (x1 - x0) >= AD_MIN_SEC:
                keep3.append([x0, x1])
            else:
                _log('  忽略 %.1f 秒的广告段 [%.1f-%.1f]（阈值 %.0f 秒）'
                     % (x1 - x0, x0, x1, AD_MIN_SEC))
        out = keep3
    return out

def video_bitrate(vpath):
    """源视频码率（bps）。取不到就用 体积/时长 估算（扣掉音频）。"""
    try:
        r = subprocess.run([FFPROBE, '-v', 'error', '-select_streams', 'v:0',
                            '-show_entries', 'stream=bit_rate',
                            '-of', 'default=nw=1:nk=1', vpath],
                           capture_output=True, timeout=90)
        v = (r.stdout or b'').decode('utf-8', 'ignore').strip().split('\n')[0].strip()
        if v.isdigit() and int(v) > 100000:
            return int(v)
    except Exception:
        pass
    try:
        dur = video_duration(vpath)
        sz = os.path.getsize(vpath)
        if dur > 1 and sz > 0:
            return max(200000, int(sz * 8 / dur) - 160000)
    except Exception:
        pass
    return 0


def _has_audio(path):
    """是否有音频轨。"""
    try:
        r = subprocess.run([FFPROBE, '-v', 'error', '-select_streams', 'a:0',
                            '-show_entries', 'stream=index', '-of', 'csv=p=0', path],
                           capture_output=True, timeout=90)
        return bool((r.stdout or b'').strip())
    except Exception:
        return False


def _keyframe_times(vpath, start=0.0, span=30.0):
    """列出 [start, start+span] 内的关键帧时间。只读索引，不解码整片。"""
    try:
        r = subprocess.run([FFPROBE, '-v', 'error', '-select_streams', 'v:0',
                            '-skip_frame', 'nokey',
                            '-show_entries', 'frame=pts_time', '-of', 'csv=p=0',
                            '-read_intervals',
                            '%.3f%%+%.3f' % (max(0.0, start), max(0.5, span)),
                            vpath],
                           capture_output=True, timeout=180)
        out = []
        for x in (r.stdout or b'').decode('utf-8', 'ignore').replace('\r', '\n').split('\n'):
            x = x.strip().rstrip(',')
            if not x:
                continue
            try:
                out.append(float(x))
            except ValueError:
                pass
        return sorted(out)
    except Exception:
        return []


def _try_copy_cut(vpath, t_in, t_out, tmp, log=None, max_shift=5.0):
    """头/尾广告：只重封装，不重编码（画质零损失、秒级完成）。

    起点必须落在关键帧：优先取「>= t_in」的最近关键帧（宁可少留一点正片，
    也不留广告）；差距超 max_shift 时退而取「<= t_in 且 <=0.5s」的关键帧；
    都不行返回 None，交给重编码。
    终点用 -t 在包级截断（copy 模式下可精确到帧）。
    """
    def _log(m):
        if log:
            try:
                log(m)
            except Exception:
                pass

    lo = max(0.0, t_in - 20.0)
    kfs = _keyframe_times(vpath, lo, (t_in - lo) + 40.0)
    if not kfs:
        _log('    取不到关键帧列表 -> 走重编码')
        return None

    start = None
    after = [k for k in kfs if k >= t_in - 0.02]
    if after:
        k = min(after)
        if k - t_in <= max_shift:
            start = k
            _log('    起点取关键帧 %.2fs（正片少 %.2fs）' % (k, k - t_in))
    if start is None:
        before = [k for k in kfs if k <= t_in + 0.02]
        if before:
            k = max(before)
            if t_in - k <= 0.5:
                start = k
                _log('    起点取关键帧 %.2fs（残留广告 %.2fs）' % (k, t_in - k))
    if start is None:
        _log('    附近关键帧跨度 > %.1fs -> 走重编码' % max_shift)
        return None
    if t_out - start < 1.0:
        return None

    want = t_out - start
    # copy 模式下 -ss/-t 一次只能切到 GOP 边界（实测会多出约一个 GOP，
    # 例如要 330.7s 实际出来 339.3s），所以：先粗切到本地盘（不占 NAS 写），
    # 再用第二遍 -t 精裁。两遍都是 -c copy，零重编码。
    import tempfile as _tf
    loc = os.path.join(_tf.gettempdir(),
                       'adcopy_%d_%d.mp4' % (os.getpid(), int(time.time())))
    try:
        c1 = [FFMPEG, '-hide_banner', '-v', 'error', '-y',
              '-ss', '%.3f' % start, '-i', vpath,
              '-t', '%.3f' % (want + 14.0),
              '-map', '0:v:0', '-map', '0:a:0?', '-c', 'copy',
              '-avoid_negative_ts', 'make_zero', '-dn', '-sn', loc]
        r1 = subprocess.run(c1, capture_output=True, timeout=3600)
        if r1.returncode != 0 or not os.path.exists(loc):
            _log('    第一遍重封装失败: %s'
                 % (r1.stderr or b'')[-160:].decode('utf-8', 'replace'))
            return None
        c2 = [FFMPEG, '-hide_banner', '-v', 'error', '-y', '-i', loc,
              '-t', '%.3f' % want,
              '-map', '0:v:0', '-map', '0:a:0?', '-c', 'copy',
              '-avoid_negative_ts', 'make_zero', '-dn', '-sn', tmp]
        r2 = subprocess.run(c2, capture_output=True, timeout=3600)
        if r2.returncode != 0 or not os.path.exists(tmp):
            _log('    第二遍精裁失败: %s'
                 % (r2.stderr or b'')[-160:].decode('utf-8', 'replace'))
            return None
    finally:
        try:
            if os.path.exists(loc):
                os.remove(loc)
        except Exception:
            pass

    got = video_duration(tmp)
    want = t_out - start
    if not got or abs(got - want) > max(3.0, want * 0.02):
        _log('    重封装时长异常 %.1fs（期望 %.1fs）-> 走重编码' % (got, want))
        try:
            os.remove(tmp)
        except Exception:
            pass
        return None
    _log('    ★ 只重封装（未重编码）: 起 %.2fs / 时长 %.1fs' % (start, got))
    return {'t0': start, 't1': t_out, 'dur': got}


# ---------------------------------------------------------------- 本地工作区（backup/work）
def stage_local(src, log=None):
    """NAS 源 -> backup/work 本地副本；返回本地路径。

    目的：把重活（重编码/拼接/修复）都放在本地盘，避免在 NAS 上产生
    大量小写入；同时 NAS 源在回拷成功前始终不动。
    """
    import repair as _rp
    wd = os.path.join(_rp.BAK_DIR or r'F:\MediaScraper\backup', 'work')
    os.makedirs(wd, exist_ok=True)
    stem, ext = os.path.splitext(os.path.basename(src))
    dst = os.path.join(wd, '%s.%d%s' % (stem[:90], os.getpid(), ext))
    t0 = time.time()
    sz = os.path.getsize(src)
    shutil.copy2(src, dst)
    if log:
        log('    源 -> 本地工作区 %.1f MB（%.0fs）' % (sz / 1048576.0, time.time() - t0))
    return dst


def commit_back(local, nas, log=None):
    """本地成品 -> NAS：先写同目录临时名（一次大拷贝），再原子替换。"""
    d = os.path.dirname(nas)
    stem, ext = os.path.splitext(os.path.basename(nas))
    tmp = os.path.join(d, '.%s.new.%d%s' % (stem, os.getpid(), ext))
    t0 = time.time()
    sz = os.path.getsize(local)
    try:
        shutil.copy2(local, tmp)
    except Exception as e:
        if log:
            log('    ! 回拷写入失败: %s' % str(e)[:120])
        try:
            os.remove(tmp)
        except Exception:
            pass
        return False
    try:
        os.replace(tmp, nas)
    except Exception as e:
        try:
            os.remove(tmp)
        except Exception:
            pass
        if log:
            log('    ! 回拷替换失败: %s' % str(e)[:120])
        return False
    if log:
        log('    回拷完成 %.1f MB（%.0fs）' % (sz / 1048576.0, time.time() - t0))
    return True


def stage_backup(local_orig, log=None):
    """处理成功后处理本地源副本：保留到 backup（按天数）或 0 天直接删。"""
    import repair as _rp
    if not local_orig or not os.path.exists(local_orig):
        return None
    if (_rp.BAK_DAYS or 0) <= 0:
        try:
            os.remove(local_orig)
            if log:
                log('    源副本已删除（保留 0 天）')
        except Exception:
            pass
        return None
    ts = time.strftime('%Y%m%d-%H%M%S')
    dst = os.path.join(_rp.BAK_DIR, '%s.%s.orig' % (os.path.basename(local_orig), ts))
    try:
        os.replace(local_orig, dst)
    except Exception:
        try:
            shutil.copy2(local_orig, dst)
            os.remove(local_orig)
        except Exception:
            return None
    if log:
        log('    源已保留：%s（%d 天）' % (os.path.basename(dst), _rp.BAK_DAYS))
    return dst


def drop_work(path, log=None):
    """删掉本地工作副本（成功/失败都能安全调用）。"""
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def _try_copy_cut_multi(vpath, keep, tmp, log=None, max_shift=5.0):
    """多段保留区间：每段零重编码精裁 -> concat -c copy（全程不重编码）。

    每个拼接点必须落在关键帧，所以每段末端可能少 0~max_shift 秒。
    """
    import tempfile as _tf
    _log = log or (lambda m: None)
    parts = []
    tag = '%d_%d' % (os.getpid(), int(time.time()))
    lst = os.path.join(_tf.gettempdir(), 'adcat_%s.txt' % tag)
    try:
        for i, (x0, x1) in enumerate(keep):
            pf = os.path.join(_tf.gettempdir(), 'adseg_%s_%d.mp4' % (tag, i))
            r = _try_copy_cut(vpath, x0, x1, pf, _log, max_shift=max_shift)
            if not r:
                raise RuntimeError('第 %d 段 [%.1f-%.1f] 无法重封装' % (i + 1, x0, x1))
            parts.append(pf)
        with open(lst, 'w', encoding='utf-8') as f:
            for p in parts:
                f.write("file '%s'\n" % p.replace('\\', '/'))
        cmd = [FFMPEG, '-hide_banner', '-v', 'error', '-y', '-f', 'concat',
               '-safe', '0', '-i', lst, '-map', '0:v:0', '-map', '0:a:0?',
               '-c', 'copy', '-avoid_negative_ts', 'make_zero', '-dn', '-sn', tmp]
        rr = subprocess.run(cmd, capture_output=True, timeout=3600)
        if rr.returncode != 0 or not os.path.exists(tmp):
            raise RuntimeError('concat 失败: %s'
                               % (rr.stderr or b'')[-160:].decode('utf-8', 'replace'))
        _log('    ★ 分段重封装：%d 段拼接（零重编码）' % len(parts))
        return {'parts': len(parts)}
    finally:
        for p in parts:
            try:
                os.remove(p)
            except Exception:
                pass
        try:
            os.remove(lst)
        except Exception:
            pass


# ---------------------------------------------------------------- MP4 索引裁剪
# 头：写 elst（media_time 起播点）；尾：截断 mdat + 改样本表/时长。
# 不读 mdat，不解码；只处理 moov 在文件末尾的普通 MP4。
_MP4_CONTAINERS = {b'moov', b'trak', b'mdia', b'minf', b'stbl', b'edts',
                   b'dinf', b'udta', b'mvex'}


class _Box(object):
    __slots__ = ('t', 'payload', 'children')

    def __init__(self, t, payload=b'', children=None):
        self.t, self.payload, self.children = t, payload, children

    def size(self):
        return 8 + (len(self.payload) if self.children is None
                    else sum(c.size() for c in self.children))

    def dump(self):
        body = (self.payload if self.children is None
                else b''.join(c.dump() for c in self.children))
        return struct.pack('>I4s', 8 + len(body), self.t) + body


def _mp4_parse(data, start, end):
    out, p = [], start
    while p + 8 <= end:
        size = struct.unpack('>I', data[p:p + 4])[0]
        t = data[p + 4:p + 8]
        hdr = 8
        if size == 1:
            size = struct.unpack('>Q', data[p + 8:p + 16])[0]
            hdr = 16
        elif size == 0:
            size = end - p
        if size < hdr or p + size > end:
            break
        if t in _MP4_CONTAINERS:
            out.append(_Box(t, b'', _mp4_parse(data, p + hdr, p + size)))
        else:
            out.append(_Box(t, bytes(data[p + hdr:p + size])))
        p += size
    return out


def _f1(nodes, t):
    for n in nodes:
        if n.t == t:
            return n
    return None


def _u32(b, o):
    return struct.unpack('>I', b[o:o + 4])[0]


def _u64(b, o):
    return struct.unpack('>Q', b[o:o + 8])[0]


def _p32(v):
    return struct.pack('>I', v)


def _mkbox(t, body):
    return struct.pack('>I4s', 8 + len(body), t) + body


def _parse_stsz(b):
    size, cnt = _u32(b, 4), _u32(b, 8)
    return [size] * cnt if size else [_u32(b, 12 + 4 * i) for i in range(cnt)]


def _parse_stts(b):
    n = _u32(b, 4)
    return [(_u32(b, 8 + 8 * i), _u32(b, 12 + 8 * i)) for i in range(n)]


def _parse_stsc(b):
    n = _u32(b, 4)
    return [(_u32(b, 8 + 12 * i), _u32(b, 12 + 12 * i), _u32(b, 16 + 12 * i))
            for i in range(n)]


def _parse_stco(b, is64):
    n = _u32(b, 4)
    w = 8 if is64 else 4
    return [_u64(b, 8 + w * i) if is64 else _u32(b, 8 + w * i) for i in range(n)]


def _build_stsz(sizes):
    n = len(sizes)
    return _mkbox(b'stsz', _p32(0) + _p32(0) + _p32(n) +
                  b''.join(_p32(x) for x in sizes))


def _build_stts(ents):
    return _mkbox(b'stts', _p32(0) + _p32(len(ents)) +
                  b''.join(_p32(c) + _p32(d) for c, d in ents))


def mp4_index_cut(src, dst, head_skip, tail_skip):
    """只改索引去头尾：返回 (src_bytes, trunc_at, out_bytes)。"""
    data = open(src, 'rb').read()
    total = len(data)
    top = _mp4_parse(data, 0, total)
    moov = _f1(top, b'moov')
    if not moov:
        raise RuntimeError('没有 moov')
    off, moov_off = 0, None
    for n in top:
        if n is moov:
            moov_off = off
        off += n.size()
    if moov_off is None or moov_off + moov.size() != total:
        raise RuntimeError('moov 不在文件末尾，暂不支持')

    mvhd = _f1(moov.children, b'mvhd')
    mv_ver = mvhd.payload[0]
    mv_ts = _u32(mvhd.payload, 12) if mv_ver == 0 else _u32(mvhd.payload, 20)
    traks = [n for n in moov.children if n.t == b'trak']
    if not traks:
        raise RuntimeError('没有 trak')

    trunc_at, keep_movie = 0, 0
    for tr in traks:
        mdia = _f1(tr.children, b'mdia')
        mdhd = _f1(mdia.children, b'mdhd')
        v = mdhd.payload[0]
        mts = _u32(mdhd.payload, 12) if v == 0 else _u32(mdhd.payload, 20)
        mdur = _u32(mdhd.payload, 16) if v == 0 else _u64(mdhd.payload, 24)
        keep_s = max(0.1, mdur / float(mts) - head_skip - tail_skip)
        keep_movie = max(keep_movie, int(round(keep_s * mv_ts)))

        stbl = _f1(_f1(mdia.children, b'minf').children, b'stbl')
        stsz = _f1(stbl.children, b'stsz')
        stts = _f1(stbl.children, b'stts')
        stsc = _f1(stbl.children, b'stsc')
        stco = _f1(stbl.children, b'stco') or _f1(stbl.children, b'co64')
        if not (stsz and stts and stsc and stco):
            continue
        is64 = stco.t == b'co64'
        sizes = _parse_stsz(stsz.payload)
        times = _parse_stts(stts.payload)
        sc = _parse_stsc(stsc.payload)
        offs = _parse_stco(stco.payload, is64)

        target = int(round((head_skip + keep_s) * mts))
        acc, keep_n = 0, 0
        for cnt, delta in times:
            if acc + cnt * delta <= target:
                acc += cnt * delta
                keep_n += cnt
            else:
                keep_n += max(0, int((target - acc + delta - 1) // delta))
                break
        keep_n = min(keep_n, len(sizes))
        if keep_n <= 0:
            raise RuntimeError('保留样本数为 0')
        keep_sizes = sizes[:keep_n]

        chunk_samples = []
        for ci in range(len(offs)):
            cur = 1
            for first, spc, _d in sc:
                if first - 1 <= ci:
                    cur = spc
                else:
                    break
            chunk_samples.append(cur)

        remaining, trunc_off, kept_chunks = keep_n, offs[0], 0
        for ci, cnt in enumerate(chunk_samples):
            if remaining <= 0:
                break
            take = min(cnt, remaining)
            start = keep_n - remaining
            trunc_off = offs[ci] + sum(keep_sizes[start:start + take])
            remaining -= take
            kept_chunks = ci + 1
        trunc_at = max(trunc_at, trunc_off)

        stsz.payload = _build_stsz(keep_sizes)[8:]
        left, nt = keep_n, []
        for cnt, delta in times:
            if left <= 0:
                break
            take = min(cnt, left)
            nt.append((take, delta))
            left -= take
        stts.payload = _build_stts(nt)[8:]
        stsc.payload = _mkbox(b'stsc',
                              _p32(0) + _p32(len([1 for a, _b, _c in sc if a <= kept_chunks])) +
                              b''.join(_p32(a) + _p32(bb) + _p32(cc)
                                       for a, bb, cc in sc if a <= kept_chunks))[8:]
        stco.payload = _mkbox(stco.t,
                              _p32(0) + _p32(kept_chunks) +
                              b''.join((struct.pack('>Q', x) if is64 else _p32(x))
                                       for x in offs[:kept_chunks]))[8:]
        stss = _f1(stbl.children, b'stss')
        if stss:
            sn = [_u32(stss.payload, 8 + 4 * i) for i in range(_u32(stss.payload, 4))]
            sn = [x for x in sn if x <= keep_n]
            stss.payload = _mkbox(b'stss', _p32(0) + _p32(len(sn)) +
                                  b''.join(_p32(x) for x in sn))[8:]

        mb = bytearray(mdhd.payload)
        if v == 0:
            struct.pack_into('>I', mb, 16, int(round(keep_s * mts)))
        else:
            struct.pack_into('>Q', mb, 24, int(round(keep_s * mts)))
        mdhd.payload = bytes(mb)

        tkhd = _f1(tr.children, b'tkhd')
        if tkhd:
            tb = bytearray(tkhd.payload)
            if tb[0] == 0:
                struct.pack_into('>I', tb, 20, keep_movie)
            else:
                struct.pack_into('>Q', tb, 28, keep_movie)
            tkhd.payload = bytes(tb)

        media_time = int(round(head_skip * mts))
        elst = _Box(b'elst', _p32(0) + _p32(1) +
                    struct.pack('>IIhh', keep_movie, media_time, 1, 0))
        edts = _f1(tr.children, b'edts')
        if edts:
            edts.children = [elst]
        else:
            idx = tr.children.index(tkhd) + 1 if tkhd else 0
            tr.children.insert(idx, _Box(b'edts', b'', [elst]))

    mb = bytearray(mvhd.payload)
    if mv_ver == 0:
        struct.pack_into('>I', mb, 16, keep_movie)
    else:
        struct.pack_into('>Q', mb, 24, keep_movie)
    mvhd.payload = bytes(mb)

    out = bytearray(data[:trunc_at] + moov.dump())
    off2 = 0
    for n in top:
        if n.t == b'mdat':
            if struct.unpack('>I', data[off2:off2 + 4])[0] == 1:
                struct.pack_into('>Q', out, off2 + 8, trunc_at - off2)
            else:
                struct.pack_into('>I', out, off2, trunc_at - off2)
            break
        off2 += n.size()
    open(dst, 'wb').write(out)
    return total, trunc_at, len(out)


def cut_ads_index(vpath, segs, log=None):
    """去头尾广告：只改索引 + 尾部截断；失败返回 {'ok':False,'error':...}。

    只支持「保留区间连续」（头广告 / 尾广告 / 头+尾）。中插广告请走 cut_ads。
    """
    def _log(m):
        if log:
            try:
                log(m)
            except Exception:
                pass

    dur = video_duration(vpath)
    if not dur or dur < 10:
        return {'ok': False, 'error': '读不到时长'}
    keep, cur = [], 0.0
    for a, b in sorted(segs):
        a = max(cur, min(float(a), dur))
        b = max(a, min(float(b), dur))
        if a - cur > 0.15:
            keep.append((cur, a))
        cur = max(cur, b)
    if dur - cur > 0.15:
        keep.append((cur, dur))
    if len(keep) != 1:
        return {'ok': False, 'error': '保留区间不连续（中插广告），需重封装'}
    a0, b0 = keep[0]
    head_skip, tail_skip = a0, dur - b0

    stem = os.path.splitext(os.path.basename(vpath))[0]
    tmp = os.path.join(os.path.dirname(vpath), '.%s.idx.%d.mp4' % (stem, os.getpid()))
    try:
        t_total, t_cut, t_out = mp4_index_cut(vpath, tmp, head_skip, tail_skip)
        _log('    索引裁剪：%.1f MB -> %.1f MB（头 %.1f / 尾 %.1f 秒）'
             % (t_total / 1048576.0, t_out / 1048576.0, head_skip, tail_skip))
        nd = video_duration(tmp)
        if not nd or abs(nd - (b0 - a0)) > max(1.5, (b0 - a0) * 0.005):
            raise RuntimeError('时长校验失败 %.1f != %.1f' % (nd or 0, b0 - a0))
        for t in (0.5, max(0.5, nd - 1.5)):
            if not frame_at(tmp, t, width=320):
                raise RuntimeError('抽帧失败 t=%.1f' % t)
        os.replace(tmp, vpath)
        return {'ok': True, 'new_dur': nd, 'old_dur': dur,
                'old_size': os.path.getsize(tmp) if False else t_total,
                'new_size': t_out, 'mode': 'index'}
    except Exception as e:
        try:
            os.remove(tmp)
        except Exception:
            pass
        return {'ok': False, 'error': str(e)[:160]}


def cut_ads(vpath, segs, out=None, log=None, replace=True, verify=True, stash=True):
    """按广告区间切掉、其余内容原样保留（帧级精确重编码）。

    replace=True（默认）—— 与 repair 完全同一套替换逻辑：
        编码到同目录临时文件 -> 校验时长 -> path 换名 .orig，临时文件换名回 path
                                 -> 删除临时文件（失败也删） -> 返回回滚点
    replace=False —— 老行为：只产出 <name>.adfree.mp4，原片不动。

    返回 dict：{'ok','path','orig_backup','undo','old_dur','new_dur',
                'old_size','new_size','secs'}；失败 {'ok':False,'error':...}
    """
    import time as _t

    def _log(m):
        if log:
            try:
                log(m)
            except Exception:
                pass

    if not segs:
        return {'ok': False, 'error': '没有广告区间'}

    t0 = _t.time()
    dur = video_duration(vpath)
    if not dur or dur < 1:
        return {'ok': False, 'error': '读不到源时长'}

    keep, cur = [], 0.0
    for a, b in sorted(segs):
        a = max(cur, min(float(a), dur))
        b = max(a, min(float(b), dur))
        if a - cur > 0.15:
            keep.append((cur, a))
        cur = max(cur, b)
    if dur - cur > 0.15:
        keep.append((cur, dur))
    if not keep:
        return {'ok': False, 'error': '全部时长都在广告区间内，拒绝清空'}

    gone = sum(b - a for a, b in keep)
    expect = gone
    _log('    保留 %d 段，预计 %.1f 秒（源 %.1f 秒，切掉 %.1f 秒）'
         % (len(keep), expect, dur, dur - expect))

    base = os.path.basename(vpath)
    stem, ext = os.path.splitext(base)
    d = os.path.dirname(vpath)
    if replace:
        # 临时名带 pid：即使两个作业意外并发，也不会互相覆盖同一个文件
        tmp = os.path.join(d, '.%s.adnew.%d%s' % (stem, os.getpid(), ext or '.mp4'))
    else:
        tmp = out or (os.path.splitext(vpath)[0] + '.adfree.mp4')

    # 头尾广告：先试零重编码的「只重封装」
    cop = None
    if len(keep) == 1:
        cop = _try_copy_cut(vpath, keep[0][0], keep[0][1], tmp, _log)
    else:
        # 中间广告：分段重封装 + concat，避免全片重编码
        try:
            cop = _try_copy_cut_multi(vpath, keep, tmp, _log)
        except Exception as _e:
            _log('    分段重封装不可用（%s）-> 回退重编码' % str(_e)[:120])
            cop = None

    has_a = _has_audio(vpath)
    parts, labels = [], []
    for i, (a, b) in enumerate(keep):
        parts.append('[0:v]trim=start=%.3f:end=%.3f,setpts=PTS-STARTPTS[v%d]' % (a, b, i))
        if has_a:
            parts.append('[0:a]atrim=start=%.3f:end=%.3f,asetpts=PTS-STARTPTS[a%d]' % (a, b, i))
            labels.append('[v%d][a%d]' % (i, i))
        else:
            labels.append('[v%d]' % i)
    filt = ';'.join(parts) + ';' + ''.join(labels) + \
        'concat=n=%d:v=1:a=%d[vo]%s' % (len(keep), 1 if has_a else 0, '[ao]' if has_a else '')

    cmd = [FFMPEG, '-hide_banner', '-v', 'error', '-y', '-i', vpath,
           '-filter_complex', filt, '-map', '[vo]']
    if has_a:
        cmd += ['-map', '[ao]']
    # 码率策略：按【源视频码率】封顶。否则固定 cq 会重新"提纯"，
    # 切掉广告后体积反而变大（源本身是低码率时尤其明显）。
    vbr = video_bitrate(vpath)
    if vbr:
        if cop is None:
            _log('    源视频码率 %.0f kbps -> 同码率重编码（体积随裁剪时长下降）' % (vbr / 1000.0))
        cmd += ['-c:v', 'hevc_nvenc', '-preset', 'p5', '-rc', 'vbr',
                '-b:v', str(vbr), '-maxrate', str(int(vbr * 1.15)),
                '-bufsize', str(int(vbr * 2)), '-pix_fmt', 'yuv420p']
    else:
        cmd += ['-c:v', 'hevc_nvenc', '-preset', 'p5', '-cq', '23', '-pix_fmt', 'yuv420p']
    if has_a:
        cmd += ['-c:a', 'aac', '-b:a', '128k']
    cmd += ['-movflags', '+faststart', tmp]

    ok = False
    try:
        if cop is None:
            r = subprocess.run(cmd, capture_output=True, timeout=36000)
            if r.returncode != 0 or not os.path.exists(tmp):
                _log('    编码失败: %s' % (r.stderr or b'')[-200:].decode('utf-8', 'replace'))
                return {'ok': False, 'error': '编码失败'}

        new_dur = video_duration(tmp)
        new_size = os.path.getsize(tmp)
        _log('    编码完成 %.1f 秒 / %.1f MB' % (new_dur, new_size / 1048576.0))

        if verify:
            # 重封装会吸附到关键帧，每段允许 max_shift 秒偏差
            tol = max(6.0, expect * 0.02) if cop else max(3.0, expect * 0.02)
            if not new_dur or abs(new_dur - expect) > tol:
                _log('    ! 时长校验失败：期望 %.1f±%.1f 秒，实际 %.1f 秒'
                     % (expect, tol, new_dur))
                return {'ok': False, 'error': '时长校验失败（期望 %.1f，实际 %.1f）'
                        % (expect, new_dur)}
            for _t_ in (1.0, max(1.0, new_dur - 2.0)):
                _b = frame_at(tmp, _t_, width=320)
                if not _b:
                    _log('    ! 抽帧校验失败 t=%.1f，成片可能损坏' % _t_)
                    return {'ok': False, 'error': '抽帧校验失败 t=%.1f' % _t_}
            _log('    校验通过（时长 / 抽帧）')

        old_size = os.path.getsize(vpath)
        ratio = expect / dur
        if new_size > old_size * ratio * 1.35 and ratio < 0.9:
            _log('    ! 体积反而变大：%.1f -> %.1f MB（保留 %.0f%% 时长）'
                 % (old_size / 1048576.0, new_size / 1048576.0, ratio * 100))
        if not replace:
            ok = True
            _log('    输出 %s（原片未动）' % base)
            return {'ok': True, 'path': tmp, 'orig_backup': None, 'undo': None,
                    'old_dur': dur, 'new_dur': new_dur, 'old_size': old_size,
                    'new_size': new_size, 'secs': round(_t.time() - t0, 1)}

        # ---- 与 repair 相同的原子替换：path -> path.orig, tmp -> path ----
        orig = vpath + '.orig'
        k = 1
        while os.path.exists(orig):
            orig = '%s.orig%d' % (vpath, k)
            k += 1
        err = None
        for n in range(5):
            try:
                os.replace(vpath, orig)
                os.replace(tmp, vpath)
                err = None
                break
            except Exception as e:
                err = e
                try:
                    if os.path.exists(orig) and not os.path.exists(vpath):
                        os.replace(orig, vpath)      # 别两头空
                except Exception:
                    pass
                _log('    ! 换名失败（第 %d 次，可能被播放器/同步软件占用）：%s'
                     % (n + 1, str(e)[:90]))
                _t.sleep(2)
        if err is not None:
            _log('    ! 替换失败，原片未动；成片保留在临时文件：%s' % tmp)
            return {'ok': False, 'error': '文件被占用（重试 5 次）: %s' % str(err)[:90],
                    'stage': tmp}
        ok = True
        if stash:
            try:
                import repair as _rp
                _bk = _rp.stash_orig(orig, _log)
            except Exception:
                _bk = orig
        else:
            # 交给调用方：回拷并校验通过后再保留/删除（0 天时也不能提前删）
            _bk = orig
        _log('    已替换原片（回滚点 %s）'
             % (os.path.basename(_bk) if _bk else '未保留（0 天）'))
        return {'ok': True, 'path': vpath, 'orig_backup': _bk,
                'undo': ({'action': 'restore', 'path': vpath, 'backup': _bk}
                         if _bk else None),
                'old_dur': dur, 'new_dur': new_dur, 'old_size': old_size,
                'new_size': new_size, 'secs': round(_t.time() - t0, 1)}
    finally:
        try:
            if not ok and replace and os.path.isfile(tmp):
                os.remove(tmp)          # 失败路径也要清掉临时文件
        except Exception:
            pass
        try:
            if ok and replace and os.path.isfile(tmp):
                os.remove(tmp)
        except Exception:
            pass

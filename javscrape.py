# -*- coding: utf-8 -*-
"""
JAV metadata scraper (host side, goes through the local Clash proxy)
  JavBus detail page  ->  parse fields -> Kodi/Jellyfin .nfo + poster/fanart

Usage:
  python javscrape.py probe "<html file>"                 # parse a saved page
  python javscrape.py fetch <CODE> [--dir D]              # fetch one, optionally write into D
  python javscrape.py scan  "<root>" [--limit N] [--apply]  # find 番号 folders under root
"""
import argparse, io, json, os, re, sys, time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding='utf-8')
    except Exception:
        pass

PROXY = os.environ.get('JAV_PROXY', 'http://127.0.0.1:7897')
PROXIES = {'http': PROXY, 'https': PROXY}
UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/124.0 Safari/537.36')
HDRS = {'User-Agent': UA, 'Accept-Language': 'zh-CN,zh;q=0.9,ja;q=0.8'}
IMGHDRS = dict(HDRS)
IMGHDRS['Referer'] = 'https://www.javbus.com/'

_ADAPTER = HTTPAdapter(max_retries=Retry(total=5, connect=5, read=5, backoff_factor=1.0,
                                        status_forcelist=[429, 500, 502, 503, 504],
                                        allowed_methods=['GET']))


def new_session():
    """Fresh session per request. Reusing pooled connections through the local
    Clash proxy is unreliable (the first pooled connection often dies with
    SSLEOFError), so do not share one."""
    s = requests.Session()
    s.headers.update(HDRS)
    s.mount('http://', _ADAPTER)
    s.mount('https://', _ADAPTER)
    return s

# 番号 patterns: ABC-123 / ABC-01234 / FC2-PPV-1234567 / 123ABC-456
# 版本后缀（C=中文字幕 / J,X=破解 / HD,SD,4K=画质）不是番号：匹配时容忍，返回前剥掉
CODE_SUFFIX = r'(?:[-_ ]?(?:HD|SD|FHD|4K|C|J|X|U))?'
CODE_RE = re.compile(
    r'(?<![A-Za-z0-9])('
    r'FC2[-_ ]?PPV[-_ ]?\d{4,8}'
    r'|[A-Z]{2,6}[-_ ]?\d{2,5}'
    r'|\d{3}[A-Z]{2,5}[-_ ]?\d{2,5}'
    r')' + CODE_SUFFIX + r'(?![A-Za-z0-9])', re.I)

_SUFFIX_RE = re.compile(r'[-_ ]?(?:HD|SD|FHD|4K|C|J|X|U)$', re.I)


def strip_suffix(s):
    # 剥掉版本后缀（不属于番号）
    return _SUFFIX_RE.sub('', (s or '').strip())


def norm_code(s):
    s = re.sub(r'[-_ ]+', '-', s.strip().upper())
    if s.startswith('FC2'):
        m = re.search(r'FC2-?PPV-?(\d+)', s)
        return 'FC2-PPV-%s' % m.group(1) if m else s
    return s


def find_code(name):
    # 站点前缀（kss55.cc@ / dxxdom.com@ …）必须先剥掉，
    # 否则会把站名当番号：kss55.cc@SVMGM-052C -> KSS55
    s = re.sub(r'^[^@]*@', '', (name or ''))
    hits = CODE_RE.findall(s)
    if not hits:
        return None
    # prefer the longest / most specific（返回前剥掉版本后缀）
    return norm_code(strip_suffix(max(hits, key=len)))


def get(url, timeout=40, session=None, headers=None):
    s = session if session is not None else new_session()
    return s.get(url, proxies=PROXIES, timeout=timeout, headers=headers or HDRS)


def _clean(html):
    t = re.sub(r'<br\s*/?>', '\n', html)
    t = re.sub(r'<[^>]+>', '', t)
    return re.sub(r'[ \t\u3000]+', ' ', t).strip()


LABELS = {
    'code': ['品番', '識別碼', '识别码', '番號', '番号'],
    'date': ['発売日', '發行日期', '發行日', '配信開始日'],
    'runtime': ['収録時間', '長度', '长度'],
    'director': ['監督', '導演', '导演'],
    'studio': ['メーカー', '製作商', '制作商'],
    'label': ['レーベル', '發行商', '发行商'],
    'series': ['シリーズ', '系列'],
    'genres': ['ジャンル', '類別', '类别'],
    'actors': ['出演者', '演員', '演员'],
}


def parse_javbus(html):
    d = {}
    m = re.search(r'<h3>(.*?)</h3>', html, re.S)
    full = _clean(m.group(1)) if m else ''
    if not full:
        m = re.search(r'<title>(.*?)</title>', html, re.S)
        full = _clean(m.group(1)).replace(' - JavBus', '').strip() if m else ''
    m = re.match(r'^([A-Za-z0-9][A-Za-z0-9\-_]{3,14})\s+(.+)$', full)
    if m and re.search(r'[A-Za-z]{2,6}[-_]?\d', m.group(1)):
        d['code'], d['title'] = norm_code(m.group(1)), m.group(2).strip()
    else:
        d['code'], d['title'] = '', full

    def field(labels):
        for lab in labels:
            mm = re.search(r'<span class="header">\s*' + re.escape(lab) + r'\s*:?\s*</span>\s*(.*?)(?:</p>|</div>)',
                           html, re.S)
            if mm:
                return _clean(mm.group(1))
        return ''

    d['code'] = d.get('code') or norm_code(field(['識別碼', '识别码', '品番', '番號', '番号']))
    d['date'] = field(['發行日期', '发行日期', '發売日', '配信開始日'])
    d['runtime'] = field(['長度', '长度', '収録時間'])
    d['studio'] = field(['製作商', '制作商', 'メーカー'])
    d['label'] = field(['發行商', '发行商', 'レーベル'])
    d['series'] = field(['系列', 'シリーズ'])
    d['director'] = field(['監督', '導演', '导演'])

    d['genres_list'] = re.findall(r'<span class="genre"><label><input[^>]*><a href="[^"]*/genre/[^"]*">([^<]+)</a>', html)
    actors = re.findall(r'<div class="star-name">\s*<a[^>]*>([^<]+)</a>', html)
    if not actors:
        actors = re.findall(r'<a class="avatar-box"[^>]*>.*?<span>([^<]+)</span>', html, re.S)
    d['actors_list'] = [a.strip() for a in dict.fromkeys(actors) if a.strip()]

    m = re.search(r'<a class="bigImage"\s+href="([^"]+)"', html)
    cov = m.group(1) if m else ''
    if cov.startswith('/'):
        cov = 'https://www.javbus.com' + cov
    d['cover'] = cov
    samples = re.findall(r'<a class="sample-box" href="(https?://[^"]+)"', html)
    if not samples:
        samples = ['https://www.javbus.com' + s for s in re.findall(r'<img src="(/pics/sample/[^"]+)"', html)]
    d['samples'] = samples
    return d


def fetch(code, session=None):
    """Try JavBus direct page, else JavBus search -> first hit, else JavDB search."""
    code = norm_code(code)
    url = 'https://www.javbus.com/%s' % code
    try:
        r = get(url, session=session)
        if r.status_code == 200 and 'header">' in r.text:
            d = parse_javbus(r.text)
            if d.get('code') or d.get('date'):
                d['source'] = url
                return d
    except Exception as e:
        sys.stderr.write('  javbus direct err %s\n' % e)
    # search fallback
    try:
        r = get('https://www.javbus.com/search/%s&type=1' % code, session=session)
        m = re.search(r'href="(/[A-Za-z0-9\-_]+)"[^>]*>\s*<img', r.text)
        ids = re.findall(r'<a class="movie-box" href="(https://www\.javbus\.com/[^"]+)"', r.text)
        if ids:
            r2 = get(ids[0], session=session)
            d = parse_javbus(r2.text)
            d['source'] = ids[0]
            return d
    except Exception as e:
        sys.stderr.write('  javbus search err %s\n' % e)
    # 无码入口：主站搜不到无码号，得走 /uncensored/search/
    # 日期式无码号（1pondo-012817_473 / 10musume-050317_01 / Carib-111425-001）
    # javbus 只认数字部分：012817_473 / 050317_01 / 111425-001
    cands = [code]
    m = re.search(r'(\d{6})[_-](\d{2,3})', code)
    if m:
        for sep in ('_', '-'):
            cands.append('%s%s%s' % (m.group(1), sep, m.group(2)))
    for sc in dict.fromkeys(cands):
        try:
            r = get('https://www.javbus.com/uncensored/search/%s&type=1' % sc, session=session)
            ids = re.findall(r'<a class="movie-box" href="(https://www\.javbus\.com/[^"]+)"',
                             r.text)
            if ids:
                r2 = get(ids[0], session=session)
                d = parse_javbus(r2.text)
                d['source'] = ids[0]
                return d
        except Exception as e:
            sys.stderr.write('  javbus uncensored err %s\n' % e)
    return None


def nfo_xml(d, code):
    def esc(s):
        return (str(s or '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'))
    genres = d.get('genres_list') or ([x.strip() for x in (d.get('genres') or '').split(',') if x.strip()])
    actors = d.get('actors_list') or ([x.strip() for x in (d.get('actors') or '').split(',') if x.strip()])
    year = ''
    date = (d.get('date') or '').strip()
    m = re.search(r'(\d{4})', date)
    if m:
        year = m.group(1)
    rt = 0
    m = re.search(r'(\d+)', d.get('runtime') or '')
    if m:
        rt = int(m.group(1))
    title = (d.get('title') or code).strip()
    lines = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>', '<movie>']
    for tag, val in (('title', code + ' ' + title if code not in title else title),
                     ('originaltitle', title),
                     ('sorttitle', code),
                     ('plot', (d.get('plot') or title)),
                     ('premiered', date), ('year', year),
                     ('runtime', rt or ''), ('studio', d.get('studio', '')),
                     ('label', d.get('label', '')), ('num', code),
                     ('director', d.get('director', ''))):
        if val != '' and val is not None:
            lines.append('  <%s>%s</%s>' % (tag, esc(val), tag))
    for g in genres:
        lines.append('  <genre>%s</genre>' % esc(g))
    for a in actors:
        lines.append('  <actor><name>%s</name></actor>' % esc(a))
    lines.append('  <uniqueid type="javbus" default="true">%s</uniqueid>' % esc(code))
    lines.append('  <art><poster>poster.jpg</poster><fanart>fanart.jpg</fanart></art>')
    lines.append('</movie>')
    return '\n'.join(lines), genres, actors


def write_into(d, dirpath, code):
    os.makedirs(dirpath, exist_ok=True)
    base = os.path.basename(os.path.normpath(dirpath))
    nfo, genres, actors = nfo_xml(d, code)
    # jellyfin reads: <folder>/<name>.nfo  or movie.nfo
    with io.open(os.path.join(dirpath, base + '.nfo'), 'w', encoding='utf-8') as f:
        f.write(nfo)
    with io.open(os.path.join(dirpath, 'movie.nfo'), 'w', encoding='utf-8') as f:
        f.write(nfo)
    got = []
    try:
        r = get(d['cover'], timeout=90, headers=IMGHDRS)
        if r.status_code == 200 and len(r.content) > 3000:
            for nm in ('poster.jpg', base + '-poster.jpg'):
                with open(os.path.join(dirpath, nm), 'wb') as f:
                    f.write(r.content)
            got.append('poster')
        else:
            sys.stderr.write('  cover http=%s len=%d\n' % (r.status_code, len(r.content)))
    except Exception as e:
        sys.stderr.write('  cover err %s\n' % e)
    for i, u in enumerate((d.get('samples') or [])[:1]):
        try:
            r = get(u, timeout=90, headers=IMGHDRS)
            if r.status_code == 200 and len(r.content) > 3000:
                with open(os.path.join(dirpath, 'fanart.jpg'), 'wb') as f:
                    f.write(r.content)
                got.append('fanart')
            else:
                sys.stderr.write('  fanart http=%s len=%d\n' % (r.status_code, len(r.content)))
        except Exception as e:
            sys.stderr.write('  fanart err %s\n' % e)
    return got, genres, actors


def cmd_probe(a):
    html = io.open(a.file, encoding='utf-8', errors='replace').read()
    d = parse_javbus(html)
    print(json.dumps(d, ensure_ascii=False, indent=2))


def cmd_fetch(a):
    d = fetch(a.code)
    if not d:
        print('  NOT FOUND'); return
    print(json.dumps({k: v for k, v in d.items() if k != 'samples'}, ensure_ascii=False, indent=2))
    if a.dir:
        got, g, ac = write_into(d, a.dir, norm_code(a.code))
        print('  wrote into %s :: %s' % (a.dir, ','.join(got) or '(no images)'))
        print('  genres: %s' % ', '.join(g))
        print('  actors: %s' % ', '.join(ac))


def cmd_scan(a):
    hits = []
    for name in sorted(os.listdir(a.root)):
        p = os.path.join(a.root, name)
        if not os.path.isdir(p):
            continue
        code = find_code(name)
        if code:
            hits.append((code, name, p))
    print('  dirs=%d  with 番号=%d' % (len(os.listdir(a.root)), len(hits)))
    sel = hits[:a.limit or len(hits)]
    for code, name, p in sel:
        print('    %-14s %s' % (code, name[:70]))
    if a.apply:
        for i, (code, name, p) in enumerate(sel, 1):
            print('  [%d/%d] %s' % (i, len(sel), code))
            try:
                d = fetch(code)
                if d:
                    got, g, ac = write_into(d, p, code)
                    print('      ok images=%s genres=%s' % (','.join(got), '/'.join(g[:4])))
                else:
                    print('      not found')
            except Exception as e:
                print('      ! %s' % e)
            time.sleep(2)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    p = sub.add_parser('probe'); p.add_argument('file'); p.set_defaults(func=cmd_probe)
    p = sub.add_parser('fetch'); p.add_argument('code'); p.add_argument('--dir')
    p.set_defaults(func=cmd_fetch)
    p = sub.add_parser('scan'); p.add_argument('root'); p.add_argument('--limit', type=int)
    p.add_argument('--apply', action='store_true'); p.set_defaults(func=cmd_scan)
    a = ap.parse_args()
    a.func(a)


if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""
Pick a good video frame as a cover / poster.

Samples N candidate frames across the middle of the video and scores each on
sharpness / exposure / skin-tone coverage / colourfulness (+ face presence when a
face detector is available). Keeps the best.

Works with OpenCV 4.x (cv2.CascadeClassifier) and 5.x (no Haar API -> falls back
to the non-face score mix).

Usage:
  python cover.py draw  "<video>" -o poster.jpg [--n 12] [--json]
  python cover.py batch "<dir>"    [--n 12] [--force]
"""
import argparse, glob, json, os, re, subprocess, sys, tempfile
import cv2
import numpy as np

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding='utf-8')
    except Exception:
        pass

FFMPEG = os.environ.get('FFMPEG', 'ffmpeg')
FFPROBE = os.environ.get('FFPROBE', 'ffprobe')

# 封面取帧策略（可用环境变量调）
#   起点在 [MIN_START, 片尾-TAIL] 内**随机**，每次取样的窗口为 WINDOW 秒
MIN_START = float(os.environ.get('MS_COVER_MIN_SEC', '120'))      # 最早取帧时间（秒），默认 2:00
WINDOW = float(os.environ.get('MS_COVER_WINDOW_SEC', '120'))      # 单次取样窗口（秒）
TAIL = float(os.environ.get('MS_COVER_TAIL_SEC', '5'))            # 离片尾保留的余量（秒）
import random                                                     # noqa: E402
VIDEO_EXT = {'.mp4', '.mkv', '.avi', '.wmv', '.mov', '.ts', '.m4v', '.flv', '.rmvb', '.mpg', '.mpeg'}
_COVER_NAMES = ('poster.jpg', 'folder.jpg', 'cover.jpg', 'poster.png', 'folder.png')
HERE = os.path.dirname(os.path.abspath(__file__))
YUNET = os.path.join(HERE, 'models', 'face_detection_yunet_2023mar.onnx')

_FACE = 'unset'


def face_detector():
    """Return a callable(BGR image) -> list of face boxes, or None."""
    global _FACE
    if _FACE != 'unset':
        return _FACE
    _FACE = None
    # 1) YuNet DNN detector (OpenCV >= 4.5.4, incl. 5.x)
    try:
        if os.path.exists(YUNET) and hasattr(cv2, 'FaceDetectorYN'):
            d = cv2.FaceDetectorYN.create(YUNET, '', (320, 320), 0.6, 0.3, 5000)

            def run(bgr):
                h, w = bgr.shape[:2]
                d.setInputSize((int(w), int(h)))
                _, faces = d.detect(bgr)
                return [] if faces is None else [tuple(float(v) for v in f) for f in faces]

            _FACE = run
            return _FACE
    except Exception:
        _FACE = None
    # 2) Haar cascade fallback (OpenCV 4.x)
    try:
        cls = getattr(cv2, 'CascadeClassifier', None)
        if cls is None and hasattr(cv2, 'objdetect'):
            cls = getattr(cv2.objdetect, 'CascadeClassifier', None)
        if cls is not None and hasattr(cv2, 'data'):
            p = os.path.join(cv2.data.haarcascades, 'haarcascade_frontalface_default.xml')
            if os.path.exists(p):
                c = cls(p)
                if not c.empty():
                    _FACE = lambda bgr: c.detectMultiScale(cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY),
                                                           1.1, 5, minSize=(40, 40))
    except Exception:
        _FACE = None
    return _FACE


def duration(path):
    try:
        r = subprocess.run([FFPROBE, '-v', 'error', '-show_entries', 'format=duration',
                            '-of', 'json', path], capture_output=True, timeout=120)
        return float(json.loads(r.stdout.decode('utf-8', 'replace'))['format']['duration'])
    except Exception:
        try:
            r = subprocess.run([FFMPEG, '-i', path], capture_output=True, timeout=120)
            m = re.search(r'Duration:\s*(\d+):(\d+):(\d+\.\d+)', r.stderr.decode('utf-8', 'replace'))
            if m:
                h, mi, s = m.groups()
                return int(h) * 3600 + int(mi) * 60 + float(s)
        except Exception:
            pass
    return 0.0


def grab(path, t, width=0):
    vf = ['-vf', 'scale=%d:-2' % width] if width and int(width) > 0 else []
    cmd = ([FFMPEG, '-v', 'error', '-ss', '%.3f' % t, '-i', path, '-frames:v', '1']
           + vf + ['-f', 'image2pipe', '-vcodec', 'png', '-'])
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=180)
    except Exception:
        return None
    if not r.stdout:
        return None
    return cv2.imdecode(np.frombuffer(r.stdout, np.uint8), cv2.IMREAD_COLOR)


def colourfulness(img):
    b, g, r = cv2.split(img.astype(np.float32))
    rg = np.abs(r - g)
    yb = np.abs(0.5 * (r + g) - b)
    return float(np.sqrt(rg.std() ** 2 + yb.std() ** 2) + 0.3 * np.sqrt(rg.mean() ** 2 + yb.mean() ** 2))


def score(img, det):
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    sharp = float(cv2.Laplacian(gray, cv2.CV_64F).var())
    bright = float(gray.mean())
    bscore = (1.0 - abs(bright - 115) / 145.0) if 45 <= bright <= 190 else 0.0
    ycrcb = cv2.cvtColor(img, cv2.COLOR_BGR2YCrCb)
    skin = float(cv2.inRange(ycrcb, (0, 133, 77), (255, 173, 127)).mean()) / 255.0
    colour = min(colourfulness(img) / 90.0, 1.0)
    sharp_n = min(sharp / 250.0, 1.0)
    skin_n = min(skin / 0.22, 1.0)
    faces = 0
    if det is not None:
        try:
            faces = len(det(img))
        except Exception:
            faces = 0
        fscore = min(faces, 3) / 3.0
        total = 0.34 * sharp_n + 0.24 * bscore + 0.27 * fscore + 0.15 * skin_n
    else:
        total = 0.44 * sharp_n + 0.28 * bscore + 0.20 * skin_n + 0.08 * colour
    return dict(total=total, sharp=sharp, bright=bright, faces=faces, skin=skin, colour=colour)


def grab_batch(path, times, width=0):
    """Extract many frames in ONE ffmpeg pass. Over SMB this replaces N costly
    seeks with a single open+seek, which is several times faster."""
    if not times:
        return []
    tmpdir = tempfile.mkdtemp(prefix='cvframes_')
    n = len(times)
    t0, t1 = float(times[0]), float(times[-1])
    span = max(0.001, t1 - t0)
    fps = (n - 1) / span if n > 1 else 1.0
    pat = os.path.join(tmpdir, 'f_%03d.jpg')
    cmd = [FFMPEG, '-v', 'error', '-ss', '%.3f' % t0, '-i', path, '-t', '%.3f' % (span + 1.5),
           '-vf', ('fps=%.6f,scale=%d:-2' % (fps, width)) if width and int(width) > 0
           else ('fps=%.6f' % fps), '-frames:v', str(n), '-q:v', '3', pat]
    try:
        subprocess.run(cmd, capture_output=True, timeout=900)
    except Exception:
        pass
    out = []
    for f in sorted(glob.glob(os.path.join(tmpdir, 'f_*.jpg'))):
        img = cv2.imread(f, cv2.IMREAD_COLOR)
        if img is not None:
            out.append(img)
    for f in glob.glob(os.path.join(tmpdir, '*')):
        try:
            os.remove(f)
        except Exception:
            pass
    try:
        os.rmdir(tmpdir)
    except Exception:
        pass
    return out


def pick(video, out, n=12, start_frac=0.05, end_frac=0.90, width=0, verbose=True,
         want_json=False, fast=False, min_start=None, rand=False, window=None):
    """取封面帧。rand=True 时，取样起点在 [min_start, 片尾-TAIL] 内**随机**。"""
    if min_start is None:
        min_start = MIN_START
    dur = duration(video)
    if dur <= 0:
        return None
    det = face_detector()
    # Poster rule: a frame may only be taken from MIN_START onward.
    a = max(min_start, dur * start_frac)
    b = min(dur * end_frac, max(a, dur - TAIL))
    if b <= a:
        if verbose:
            print('  dur=%.0fs < min_start=%.0fs -> no frame after min_start, skip' % (dur, min_start))
        return None
    if rand:                                   # 随机起点：在 [a, b] 里均匀取一个 t0
        w = min(float(window or WINDOW), b - a)
        a = random.uniform(a, max(a, b - w))
        b = min(b, a + w)
    times = [a + (b - a) * i / max(1, n - 1) for i in range(n)]
    imgs = grab_batch(video, times, width) if fast else []
    if len(imgs) < 2:
        imgs = [x for x in (grab(video, t, width) for t in times) if x is not None]
    best, rows = None, []
    for i, img in enumerate(imgs):
        t = times[min(i, len(times) - 1)]
        s = score(img, det)
        rows.append((t, s))
        if best is None or s['total'] > best[1]['total']:
            best = (t, s, img)
    if best is None:
        return None
    if want_json:
        for t, s in rows:
            print(json.dumps({'t': round(t, 1), **{k: round(v, 3) for k, v in s.items()}}, ensure_ascii=False))
            sys.stdout.flush()
    if verbose:
        print('  dur=%.0fs cand=%d faceDet=%s | best t=%.1fs score=%.3f sharp=%.0f bright=%.0f faces=%d skin=%.3f'
              % (dur, len(rows), 'yes' if det else 'no', best[0], best[1]['total'],
                 best[1]['sharp'], best[1]['bright'], best[1]['faces'], best[1]['skin']))
    if out:
        d = os.path.dirname(out)
        if d and not os.path.isdir(d):
            os.makedirs(d, exist_ok=True)
        # cv2.imwrite() SILENTLY FAILS on non-ASCII / UNC paths on Windows (every
        # folder here has a Chinese name). imencode + ndarray.tofile() goes through
        # the OS file APIs and actually writes.
        okenc, buf = cv2.imencode('.jpg', best[2], [int(cv2.IMWRITE_JPEG_QUALITY), 92])
        if not okenc:
            return None
        try:
            buf.tofile(out)
        except Exception:
            return None
    return best


def cmd_draw(a):
    if pick(a.video, a.out, a.n, a.start, a.end, a.width, True, a.json,
            fast=a.fast) is None:
        print('  FAILED: no frame decoded')
        sys.exit(2)


def cmd_batch(a):
    targets = []
    for dirpath, dirnames, filenames in os.walk(a.dir):
        vids = [f for f in filenames if os.path.splitext(f)[1].lower() in VIDEO_EXT]
        if not vids:
            continue
        if not a.force and any(os.path.exists(os.path.join(dirpath, n)) for n in _COVER_NAMES):
            continue
        vids.sort(key=lambda f: os.path.getsize(os.path.join(dirpath, f)), reverse=True)
        targets.append((dirpath, os.path.join(dirpath, vids[0])))
    print('  folders needing cover: %d  (faceDet=%s)' % (len(targets), 'yes' if face_detector() else 'no'))
    ok = 0
    for i, (d, v) in enumerate(targets, 1):
        out = os.path.join(d, 'poster.jpg')
        print('  [%d/%d] %s' % (i, len(targets), os.path.basename(d)[:70]))
        try:
            if pick(v, out, a.n, verbose=False) is not None:
                ok += 1
        except Exception as e:
            print('      ! %s' % e)
    print('  done: %d/%d' % (ok, len(targets)))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    p1 = sub.add_parser('draw')
    p1.add_argument('video'); p1.add_argument('-o', '--out', required=True)
    p1.add_argument('--n', type=int, default=12)
    p1.add_argument('--start', type=float, default=0.05)
    p1.add_argument('--end', type=float, default=0.90)
    p1.add_argument('--width', type=int, default=640)
    p1.add_argument('--json', action='store_true')
    p1.add_argument('--fast', action='store_true',
                    help='one ffmpeg pass (slower: decodes the whole span)')
    p1.set_defaults(func=cmd_draw)
    p2 = sub.add_parser('batch')
    p2.add_argument('dir'); p2.add_argument('--n', type=int, default=12)
    p2.add_argument('--force', action='store_true')
    p2.set_defaults(func=cmd_batch)
    a = ap.parse_args()
    a.func(a)


if __name__ == '__main__':
    main()

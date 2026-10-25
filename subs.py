# -*- coding: utf-8 -*-
"""SenseVoice(SherpaOnnx) 语音转字幕：视频 -> SRT（可选人声强化 / 翻译双语）

用法：
  python subs.py <video> <out.srt> [--model-dir DIR] [--threads N] [--lang zh]
                 [--enhance 1] [--translate zh] [--bilingual 1]

进度行写到 stderr：
  AUDIO 240.0 / PROG 12.3/240.0 / TRPROG 20/45 / DONE 45
"""
import argparse
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.request
import wave

FFMPEG = None

ENHANCE_AF = ('highpass=f=90,lowpass=f=7800,'
              'afftdn=nr=12:nf=-25,'
              'dynaudnorm=f=150:g=15')


def _find_ffmpeg():
    cands = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ffmpeg.exe'),
        r'C:\Users\Administrator\AppData\Local\Microsoft\WinGet\Links\ffmpeg.exe',
        'ffmpeg',
    ]
    for c in cands:
        if c == 'ffmpeg' or os.path.exists(c):
            try:
                subprocess.run([c, '-version'], capture_output=True, timeout=15)
                return c
            except Exception:
                continue
    return 'ffmpeg'


def extract_wav(video, wav, log, enhance=False):
    cmd = [FFMPEG, '-hide_banner', '-v', 'error', '-y', '-i', video, '-vn']
    if enhance:
        cmd += ['-af', ENHANCE_AF]
    cmd += ['-ac', '1', '-ar', '16000', '-f', 'wav', wav]
    r = subprocess.run(cmd, capture_output=True, timeout=36000)
    if r.returncode != 0 or not os.path.exists(wav):
        # 强化链失败就退回原始音频
        if enhance:
            log('ENHANCE_ERR 退回原始音频')
            return extract_wav(video, wav, log, False)
        raise RuntimeError('抽音频失败: %s'
                           % (r.stderr or b'')[-200:].decode('utf-8', 'replace'))
    with wave.open(wav, 'rb') as f:
        n = f.getnframes()
        log('AUDIO %.1f' % (n / 16000.0))
        return n


def clean_text(t):
    t = re.sub(r'<\|[^|]*\|>', '', t or '')
    t = t.replace('\u200b', '').strip()
    return t


def fmt_ts(sec):
    if sec < 0:
        sec = 0
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    return ('%02d:%02d:%06.3f' % (h, m, s)).replace('.', ',')


def _parse_json_array(txt):
    t = (txt or '').strip()
    t = re.sub(r'^```(?:json)?\s*', '', t)
    t = re.sub(r'\s*```$', '', t)
    i, j = t.find('['), t.rfind(']')
    if i < 0 or j <= i:
        return None
    try:
        v = json.loads(t[i:j + 1])
        return v if isinstance(v, list) else None
    except Exception:
        return None


def translate_lines(lines, target, model, log, host='http://127.0.0.1:11434'):
    """逐批翻译；失败时返回空串，保留原文。"""
    out = []
    B = 20
    sysmsg = ('你是字幕翻译。把用户给的 JSON 数组里的字幕逐条翻成%s，保持口语、短句，'
              '不要加解释。只返回 JSON 数组，长度和输入一致。' % ('简体中文' if target == 'zh' else target))
    for i in range(0, len(lines), B):
        chunk = lines[i:i + B]
        payload = {
            'model': model,
            'stream': False,
            'keep_alive': '30m',
            'options': {'temperature': 0.2},
            'messages': [
                {'role': 'system', 'content': sysmsg},
                {'role': 'user', 'content': json.dumps(chunk, ensure_ascii=False)},
            ],
        }
        req = urllib.request.Request(
            host + '/api/chat',
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=900) as r:
                txt = json.loads(r.read().decode('utf-8'))['message']['content']
            arr = _parse_json_array(txt)
        except Exception as e:
            log('TRANS_ERR %s' % str(e)[:100])
            arr = None
        if not arr:
            out.extend([''] * len(chunk))
        else:
            arr = [str(x).strip() for x in arr]
            if len(arr) < len(chunk):
                arr += [''] * (len(chunk) - len(arr))
            out.extend(arr[:len(chunk)])
        log('TRPROG %d/%d' % (min(i + B, len(lines)), len(lines)))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('video')
    ap.add_argument('out')
    ap.add_argument('--model-dir', default=r'F:\MediaScraper\models\asr')
    ap.add_argument('--threads', type=int, default=4)
    ap.add_argument('--lang', default='zh')
    ap.add_argument('--enhance', type=int, default=0)
    ap.add_argument('--translate', default='')
    ap.add_argument('--bilingual', type=int, default=0)
    ap.add_argument('--tr-model', default=os.environ.get('ASR_TR_MODEL', 'qwen38-9b-v2:latest'))
    ap.add_argument('--max-line', type=int, default=42)
    a = ap.parse_args()

    def log(m):
        sys.stderr.write(str(m) + '\n')
        sys.stderr.flush()

    global FFMPEG
    FFMPEG = _find_ffmpeg()

    model = os.path.join(a.model_dir, 'model.int8.onnx')
    if not os.path.exists(model):
        alt = os.path.join(a.model_dir, 'model.onnx')
        if os.path.exists(alt):
            model = alt
    tokens = os.path.join(a.model_dir, 'tokens.txt')
    vad_model = os.path.join(a.model_dir, 'silero_vad.onnx')
    for p in (model, tokens, vad_model):
        if not os.path.exists(p):
            raise SystemExit('缺少模型文件: %s' % p)

    import numpy as np
    import sherpa_onnx

    tmpdir = tempfile.mkdtemp(prefix='subs_')
    wav = os.path.join(tmpdir, 'a.wav')
    try:
        total_samples = extract_wav(a.video, wav, log, bool(a.enhance))
        rec = sherpa_onnx.OfflineRecognizer.from_sense_voice(
            model=model, tokens=tokens, num_threads=a.threads,
            use_itn=True, debug=False, language=a.lang)
        vcfg = sherpa_onnx.VadModelConfig()
        vcfg.silero_vad.model = vad_model
        vcfg.silero_vad.threshold = 0.5
        vcfg.silero_vad.min_silence_duration = 0.35
        vcfg.silero_vad.min_speech_duration = 0.25
        vcfg.silero_vad.max_speech_duration = 20
        vcfg.sample_rate = 16000
        vcfg.num_threads = 2
        vad = sherpa_onnx.VoiceActivityDetector(vcfg, buffer_size_in_seconds=60)

        win = 512
        offset = 0
        segs = []
        with wave.open(wav, 'rb') as f:
            while True:
                raw = f.readframes(win)
                if not raw:
                    break
                samples = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
                vad.accept_waveform(samples)
                while not vad.empty():
                    s = vad.front
                    st = s.start / 16000.0
                    dur = len(s.samples) / 16000.0
                    stream = rec.create_stream()
                    stream.accept_waveform(16000, s.samples)
                    rec.decode_stream(stream)
                    txt = clean_text(stream.result.text)
                    if txt:
                        segs.append((st, st + dur, txt))
                    vad.pop()
                offset += 1
                if offset % 200 == 0:
                    log('PROG %.1f/%.1f' % (offset * win / 16000.0,
                                            total_samples / 16000.0))
        vad.flush()
        while not vad.empty():
            s = vad.front
            st = s.start / 16000.0
            dur = len(s.samples) / 16000.0
            stream = rec.create_stream()
            stream.accept_waveform(16000, s.samples)
            rec.decode_stream(stream)
            txt = clean_text(stream.result.text)
            if txt:
                segs.append((st, st + dur, txt))
            vad.pop()

        total_dur = total_samples / 16000.0
        segs = [(st, min(en, total_dur), tx) for st, en, tx in segs if 0 <= st < total_dur]
        segs.sort(key=lambda x: x[0])
        merged = []
        for st, en, tx in segs:
            if merged and st - merged[-1][1] < 0.6 and len(merged[-1][2]) + len(tx) <= a.max_line:
                merged[-1] = (merged[-1][0], en, merged[-1][2] + tx)
            else:
                merged.append((st, en, tx))

        tr = []
        if a.translate and merged:
            log('TRANS_START %d' % len(merged))
            tr = translate_lines([m[2] for m in merged], a.translate, a.tr_model, log)

        with io.open(a.out, 'w', encoding='utf-8') as f:
            for i, (st, en, tx) in enumerate(merged, 1):
                body = tx
                if a.bilingual and i - 1 < len(tr) and tr[i - 1]:
                    body = tx + '\n' + tr[i - 1]
                f.write('%d\n%s --> %s\n%s\n\n' % (i, fmt_ts(st), fmt_ts(en), body))
        log('DONE %d' % len(merged))
    finally:
        try:
            os.remove(wav)
        except Exception:
            pass
        try:
            os.rmdir(tmpdir)
        except Exception:
            pass


if __name__ == '__main__':
    main()

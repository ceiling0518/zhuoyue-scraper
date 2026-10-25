# -*- coding: utf-8 -*-
"""SenseVoice(SherpaOnnx) 语音转字幕：视频 -> SRT

用法：
  python subs.py <video> <out.srt> [--model-dir DIR] [--threads N] [--lang zh]

进度行写到 stderr，形如：
  PROG 12.3/240.0
"""
import argparse
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import wave

FFMPEG = None


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


def extract_wav(video, wav, log):
    cmd = [FFMPEG, '-hide_banner', '-v', 'error', '-y', '-i', video,
           '-vn', '-ac', '1', '-ar', '16000', '-f', 'wav', wav]
    r = subprocess.run(cmd, capture_output=True, timeout=36000)
    if r.returncode != 0 or not os.path.exists(wav):
        raise RuntimeError('抽音频失败: %s' % (r.stderr or b'')[-200:].decode('utf-8', 'replace'))
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('video')
    ap.add_argument('out')
    ap.add_argument('--model-dir', default=r'F:\MediaScraper\models\asr')
    ap.add_argument('--threads', type=int, default=4)
    ap.add_argument('--lang', default='zh')
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
        total_samples = extract_wav(a.video, wav, log)
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
        segs = []          # (start, end, text)
        with wave.open(wav, 'rb') as f:
            while True:
                raw = f.readframes(win)
                if not raw:
                    break
                samples = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
                vad.accept_waveform(samples)
                while not vad.empty():
                    s = vad.front
                    st = (offset * win + s.start) / 16000.0
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
            st = (offset * win + s.start) / 16000.0
            dur = len(s.samples) / 16000.0
            stream = rec.create_stream()
            stream.accept_waveform(16000, s.samples)
            rec.decode_stream(stream)
            txt = clean_text(stream.result.text)
            if txt:
                segs.append((st, st + dur, txt))
            vad.pop()

        segs.sort(key=lambda x: x[0])
        merged = []
        for st, en, tx in segs:
            if merged and st - merged[-1][1] < 0.6 and len(merged[-1][2]) + len(tx) <= a.max_line:
                merged[-1] = (merged[-1][0], en, merged[-1][2] + tx)
            else:
                merged.append((st, en, tx))

        with io.open(a.out, 'w', encoding='utf-8') as f:
            for i, (st, en, tx) in enumerate(merged, 1):
                f.write('%d\n%s --> %s\n%s\n\n' % (i, fmt_ts(st), fmt_ts(en), tx))
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

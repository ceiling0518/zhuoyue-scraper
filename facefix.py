# -*- coding: utf-8 -*-
"""GFPGAN 面部修复（子进程，用 AI 环境的 python 3.10 跑）。

用法: python facefix.py <in.png> <out.png> [weight]
找不到脸就原图返回（exit 0），真正出错才 exit 1。
"""
import os
import sys

import numpy as np
import cv2

MODEL = r'F:\StableDiffusion\AI\models\GFPGAN\gfpganv1.4.pth'


def main():
    if len(sys.argv) < 3:
        print('usage: facefix.py in out [weight]')
        return 2
    src, dst = sys.argv[1], sys.argv[2]
    weight = float(sys.argv[3]) if len(sys.argv) > 3 else 0.6

    img = cv2.imdecode(np.fromfile(src, dtype=np.uint8), cv2.IMREAD_COLOR)
    if img is None:
        print('decode failed')
        return 1
    if not os.path.exists(MODEL):
        print('model missing')
        return 1

    from gfpgan import GFPGANer

    kwargs = dict(model_path=MODEL, upscale=1, arch='clean',
                  channel_multiplier=2, bg_upsampler=None)
    try:
        import torch
        if torch.cuda.is_available():
            kwargs['device'] = 'cuda:0'
    except Exception:
        pass

    try:
        up = GFPGANer(**kwargs)
    except TypeError:
        kwargs.pop('device', None)
        up = GFPGANer(**kwargs)

    try:
        _, _, out = up.enhance(img, has_aligned=False, only_center_face=False,
                               paste_back=True, weight=weight)
    except TypeError:
        _, _, out = up.enhance(img, has_aligned=False, only_center_face=False,
                               paste_back=True)

    if out is None:
        print('no face')
        cv2.imencode('.png', img)[1].tofile(dst)
        return 0

    cv2.imencode('.png', out)[1].tofile(dst)
    print('ok %dx%d' % (out.shape[1], out.shape[0]))
    return 0


if __name__ == '__main__':
    sys.exit(main())

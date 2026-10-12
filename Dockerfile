FROM python:3.13-slim

LABEL org.opencontainers.image.title="ZhuoYue Scraper" \
      org.opencontainers.image.description="Local Jellyfin/Emby metadata scraper: multi-site scraping + local AI (Ollama)" \
      org.opencontainers.image.url="https://github.com/ceiling0518/zhuoyue-scraper" \
      org.opencontainers.image.source="https://github.com/ceiling0518/zhuoyue-scraper" \
      org.opencontainers.image.licenses="MIT"

# 自建镜像可用 --build-arg PIP_INDEX_URL=https://your.mirror/simple 加速
ARG PIP_INDEX_URL=https://pypi.org/simple
# 国内自建可换 Debian 镜像：--build-arg DEB_MIRROR=mirrors.ustc.edu.cn
ARG DEB_MIRROR=deb.debian.org
# 容器里用 headless 版 opencv（不需要 libGL / X11）
ARG CV2PKG=opencv-python-headless

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    MS_HOST=0.0.0.0 \
    MS_PORT=8800 \
    MS_ROOT=/media \
    MS_DB=/data \
    MS_OLLAMA=http://host.docker.internal:11434 \
    PUID=0 \
    PGID=0

WORKDIR /app

# ffmpeg/ffprobe：抽帧、取时长、AI 看片都依赖；gosu：以 PUID/PGID 身份运行
RUN set -eux; \
    if [ "$DEB_MIRROR" != "deb.debian.org" ]; then \
        if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
            sed -i "s|deb.debian.org|$DEB_MIRROR|g" /etc/apt/sources.list.d/debian.sources; \
        fi; \
        if [ -f /etc/apt/sources.list ]; then \
            sed -i "s|deb.debian.org|$DEB_MIRROR|g" /etc/apt/sources.list; \
        fi; \
    fi; \
    apt-get -o Acquire::Retries=5 update; \
    apt-get -o Acquire::Retries=5 install -y --no-install-recommends ffmpeg gosu; \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
# 保留 requirements.txt 为唯一版本来源，只把 opencv 换成 headless 版
RUN grep -v '^opencv-python' /app/requirements.txt > /tmp/req.txt \
 && pip install --no-cache-dir -i "$PIP_INDEX_URL" -r /tmp/req.txt \
 && pip install --no-cache-dir -i "$PIP_INDEX_URL" "$CV2PKG" \
 && rm -f /tmp/req.txt

COPY app.py mlib.py cover.py javscrape.py docker-entrypoint.sh /app/
RUN sed -i 's/\r$//' /app/docker-entrypoint.sh \
 && chmod +x /app/docker-entrypoint.sh \
 && useradd -u 1000 -M -s /usr/sbin/nologin app \
 && mkdir -p /data /media

VOLUME ["/data"]
EXPOSE 8800

HEALTHCHECK --interval=60s --timeout=10s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request,sys;sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8800/api/stats',timeout=6).status==200 else 1)"

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["python", "app.py"]

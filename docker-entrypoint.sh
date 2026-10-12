#!/bin/sh
# 以 PUID/PGID 指定的身份运行（默认 0 = root）。
# 这样写进 /media 的 .nfo / 封面不会被 root 拥有，NAS 用户不会踩权限坑。
set -e

if [ "$(id -u)" = "0" ] && [ -n "$PUID" ] && [ "$PUID" != "0" ]; then
    PGID="${PGID:-$PUID}"
    groupmod -o -g "$PGID" app 2>/dev/null || true
    usermod  -o -u "$PUID" -g "$PGID" app 2>/dev/null || true
    chown -R "$PUID:$PGID" /data 2>/dev/null || true
    exec gosu app "$@"
fi

exec "$@"

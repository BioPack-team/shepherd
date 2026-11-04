#!/bin/sh
# Start the broker with the password from REDIS_PASSWORD.
#
# The password used to be baked into redis.conf, so setting REDIS_PASSWORD
# changed what every client and the healthcheck sent but not what the server
# expected: the default could never actually be rotated. It is passed on the
# command line instead, and the broker refuses to start without one rather
# than run with no password at all.
set -eu
conf="${1:-/usr/local/etc/redis/redis.conf}"
if [ -z "${REDIS_PASSWORD:-}" ]; then
    echo "REDIS_PASSWORD must be set to start the broker" >&2
    exit 1
fi
exec redis-server "$conf" --requirepass "$REDIS_PASSWORD"

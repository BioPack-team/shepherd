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
# Repair a damaged append-only file before Redis loads it. An unclean stop
# (Docker Desktop quit, the host sleeping or losing power) can leave the
# incremental AOF with a half-written or garbage tail; Redis then refuses to
# start ("Bad file format reading the append only file") and the container
# restart-loops until someone wipes the data. With REDIS_AOF_AUTOFIX=yes the
# AOF is checked first and, if damaged, truncated at the first bad command --
# losing only the writes after it, which the monitor's recovery fails the
# same way as a crash (workers/monitor/recovery.py). The damaged files are
# kept beside it for inspection. Off by default: the check reads the whole
# AOF on every start, and a deployment may prefer to stop and look.
aof_dir=/data/appendonlydir
manifest="$aof_dir/appendonly.aof.manifest"
if [ "${REDIS_AOF_AUTOFIX:-no}" = "yes" ] && [ -f "$manifest" ]; then
    if ! redis-check-aof "$manifest" >/dev/null 2>&1; then
        echo "AOF in $aof_dir is damaged; backing it up and repairing" >&2
        backup="$aof_dir.damaged"
        rm -rf "$backup"
        mkdir -p "$backup"
        # The base file is a full snapshot; only the manifest and the
        # incremental files can be truncated by --fix.
        cp "$manifest" "$aof_dir"/*.incr.aof "$backup"/ 2>/dev/null || true
        if ! echo y | redis-check-aof --fix "$manifest" >&2; then
            echo "Could not repair the AOF in $aof_dir. Remove the broker's" >&2
            echo "volume to start empty (docker compose down, then" >&2
            echo "docker volume rm <project>_broker_data)." >&2
            exit 1
        fi
        echo "AOF repaired; the damaged files are in $backup" >&2
    fi
fi
# redis.conf's maxmemory is sized for a local stack. A deployment gives the
# broker more by setting REDIS_MAXMEMORY (e.g. "25gb"), without having to
# ship its own copy of the config. Keep it at about half the container's
# memory limit, and the volume at about three times it (see redis.conf).
if [ -n "${REDIS_MAXMEMORY:-}" ]; then
    exec redis-server "$conf" --requirepass "$REDIS_PASSWORD" \
        --maxmemory "$REDIS_MAXMEMORY"
fi
exec redis-server "$conf" --requirepass "$REDIS_PASSWORD"

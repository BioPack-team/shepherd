"""Snapshot collector for the monitor dashboard.

Polls Redis (queue depths, pending tasks, worker heartbeats) and Postgres
(query state, callbacks) every ``MONITOR_POLL_INTERVAL_SEC`` and assembles a
single JSON snapshot. The snapshot is written into the rolling history and
broadcast to connected websocket clients.
"""

import asyncio
import json
import logging
import math
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import shepherd_utils.ars.retention as retention
from shepherd_utils.broker import broker_client
from shepherd_utils.config import settings
from shepherd_utils.db import pool as pg_pool
from shepherd_utils.heartbeat import (
    HEARTBEAT_REAP_SEC,
    HEARTBEAT_SCAN_PATTERN,
    HEARTBEAT_TTL_SEC,
    SHUTDOWN_SCAN_PATTERN,
)

from . import history, redis_instances, storage

logger = logging.getLogger("shepherd.monitor.poller")

# Persistent per-worker-type state. Lives in Redis so the dashboard remembers
# every worker it has ever seen across monitor restarts and can render a card
# for a worker type whose heartbeats have all gone away (crashed or scaled
# down).
KNOWN_WORKERS_KEY = "monitor:known_workers"
WORKER_STATE_PREFIX = "monitor:worker_state"


def _worker_state_key(stream: str) -> str:
    return f"{WORKER_STATE_PREFIX}:{stream}"


# Streams to track. Discovered dynamically from heartbeats so new ARAs are
# picked up automatically; this fallback list seeds the dashboard before any
# worker has reported in.
SEED_STREAMS = [
    "aragorn",
    "aragorn.lookup",
    "aragorn.pathfinder",
    "aragorn.omnicorp",
    "aragorn.score",
    "arax",
    "arax.rank",
    "bte",
    "bte.lookup",
    "sipr",
    "example",
    "example.lookup",
    "example.score",
    "merge_message",
    "sort_results_score",
    "filter_results_top_n",
    "filter_kgraph_orphans",
    "filter_analyses_top_n",
    "score_paths",
    "finish_query",
]

# ARA prefixes used for per-ARA volume rollups.
ARAS = ["aragorn", "arax", "bte", "sipr", "gandalf", "example"]


async def probe_broker() -> bool:
    """Cheap liveness check for the broker.

    Returns ``True`` if the broker answers a PING, ``False`` on any error
    (connection refused, timeout, auth). The poll loop calls this before
    attempting a full snapshot so a broker outage is detected as a single
    ``broker_down`` alert instead of surfacing as a full keyspace scan blowing
    up (and, on recovery, a flood of bogus worker-down alerts).
    """
    try:
        return bool(await broker_client.ping())
    except Exception:
        return False


async def _scan_keys(pattern: str) -> List[str]:
    keys: List[str] = []
    async for key in broker_client.scan_iter(match=pattern, count=200):
        keys.append(key)
    return keys


async def _collect_heartbeats() -> List[Dict[str, Any]]:
    keys = await _scan_keys(HEARTBEAT_SCAN_PATTERN)
    if not keys:
        return []
    raw_values = await broker_client.mget(keys)
    now = time.time()
    workers: List[Dict[str, Any]] = []
    # Heartbeat keys are persistent (exempt from broker eviction), so a crashed
    # worker's key never expires on its own. Delete any left unrefreshed past the
    # reap window here -- the poller already has every payload in hand, so this
    # is the cheapest place to keep dead consumers from piling up in the keyspace
    # and lingering as phantom "stale" rows on the dashboard.
    dead_keys: List[str] = []
    for key, raw in zip(keys, raw_values):
        if not raw:
            continue
        try:
            hb = json.loads(raw)
        except json.JSONDecodeError:
            continue
        age = max(0.0, now - hb.get("last_seen", now))
        if age > HEARTBEAT_REAP_SEC:
            dead_keys.append(key)
            continue
        hb["age_sec"] = age
        hb["stale"] = age > HEARTBEAT_TTL_SEC
        workers.append(hb)
    if dead_keys:
        try:
            await broker_client.delete(*dead_keys)
        except Exception as e:
            logger.debug(f"Failed to reap {len(dead_keys)} dead heartbeat(s): {e}")
    return workers


async def _collect_shutdown_markers() -> Set[Tuple[str, str]]:
    """Return ``{(stream, consumer)}`` of workers that signalled a clean exit."""
    markers: Set[Tuple[str, str]] = set()
    async for key in broker_client.scan_iter(match=SHUTDOWN_SCAN_PATTERN, count=200):
        # key format: worker:shutdown:{stream}:{consumer}
        parts = key.split(":", 3)
        if len(parts) == 4:
            markers.add((parts[2], parts[3]))
    return markers


async def _collect_streams(stream_names: List[str]) -> Dict[str, Dict[str, Any]]:
    """For each stream, gather length + consumer-group stats with a single pipeline."""
    out: Dict[str, Dict[str, Any]] = {}
    if not stream_names:
        return out
    pipe = broker_client.pipeline()
    for s in stream_names:
        pipe.xlen(s)
        # Typed xpending() returns the parsed summary dict; the previous
        # execute_command("XPENDING", ...) returned the same dict via redis-py's
        # response callback, but the parsing below expected a list and so always
        # read pending as 0.
        pipe.xpending(s, "consumer")
        pipe.execute_command("XINFO", "CONSUMERS", s, "consumer")
    # ``raise_on_error=False`` returns individual exceptions in-place so one
    # missing stream/group doesn't blow up the whole batch. Streams that don't
    # exist yet, or that have no ``consumer`` group, will surface as
    # ``NOGROUP`` / ``ERR no such key`` errors for those slots.
    try:
        results = await pipe.execute(raise_on_error=False)
    except Exception as e:
        logger.warning(f"Stream pipeline blew up entirely: {e}")
        results = [None] * (len(stream_names) * 3)

    def _ok(v):
        return v if not isinstance(v, Exception) else None

    for i, s in enumerate(stream_names):
        base = i * 3
        xlen = _ok(results[base] if base < len(results) else 0)
        xpending = _ok(results[base + 1] if base + 1 < len(results) else None)
        xinfo = _ok(results[base + 2] if base + 2 < len(results) else None)

        pending_count = 0
        if isinstance(xpending, dict):
            pending_count = int(xpending.get("pending", 0) or 0)
        elif isinstance(xpending, (list, tuple)) and len(xpending) >= 1:
            pending_count = xpending[0] or 0

        consumers = []
        max_idle = 0
        if isinstance(xinfo, list):
            for entry in xinfo:
                # XINFO CONSUMERS returns a flat array of name/value pairs per consumer
                kv: Dict[str, Any] = {}
                if isinstance(entry, list):
                    it = iter(entry)
                    for k in it:
                        try:
                            v = next(it)
                        except StopIteration:
                            break
                        kv[str(k)] = v
                if kv:
                    idle = int(kv.get("idle", 0) or 0)
                    consumers.append(
                        {
                            "name": kv.get("name"),
                            "pending": int(kv.get("pending", 0) or 0),
                            "idle_ms": idle,
                        }
                    )
                    max_idle = max(max_idle, idle)

        out[s] = {
            "xlen": int(xlen or 0),
            "pending": int(pending_count),
            "consumer_count": len(consumers),
            "consumers": consumers,
            "max_idle_ms": max_idle,
        }
    return out


# Process-local view of Postgres reachability. Used to log an outage once (on
# the healthy->down edge) and recovery once, instead of on every poll tick --
# otherwise stopping the DB spams an identical warning every few seconds.
_pg_reachable = True


def _note_pg_reachable() -> None:
    global _pg_reachable
    if not _pg_reachable:
        logger.info("Postgres reachable again")
        _pg_reachable = True


def _note_pg_unreachable(exc: Exception) -> None:
    global _pg_reachable
    if _pg_reachable:
        logger.warning(
            f"Postgres unavailable: {exc}. Suppressing repeat poll errors until "
            "it recovers."
        )
        _pg_reachable = False
    else:
        logger.debug(f"Postgres still unavailable: {exc}")


async def _collect_postgres() -> Dict[str, Any]:
    """Query state breakdown, callback backlog, ARA volume.

    This runs every poll tick (a few seconds), so everything here must stay
    cheap as ``shepherd_brain`` grows over its retention window: the recent
    windows are an index range scan (``idx_shepherd_brain_start_time``), and
    the per-state counts of *unfinished* queries ride the partial
    ``idx_shepherd_brain_unfinished``. The finished-query counts, which need
    the whole table and barely move, come from ``_collect_postgres_slow``.
    """
    snapshot: Dict[str, Any] = {
        "state_counts": {},
        "queries_last_1h": 0,
        "queries_last_24h": 0,
        "callbacks_pending": 0,
        "oldest_callback_age_sec": 0,
        "per_ara_24h": {},
        "connection_count": 0,
    }
    try:
        async with pg_pool.connection(settings.postgres_pool_timeout) as conn:
            cur = await conn.execute("""
                SELECT state, COUNT(*) FROM shepherd_brain
                WHERE state NOT IN ('COMPLETED', 'ABANDONED')
                GROUP BY state
                """)
            for state, count in await cur.fetchall():
                snapshot["state_counts"][state] = int(count)

            # One range scan for the 24h window, the 1h window inside it, and
            # the per-ARA split.
            cur = await conn.execute("""
                SELECT domain,
                       COUNT(*),
                       COUNT(*) FILTER (
                           WHERE start_time > NOW() - INTERVAL '1 hour'
                       )
                FROM shepherd_brain
                WHERE start_time > NOW() - INTERVAL '24 hours'
                GROUP BY domain
                """)
            for domain, last_24h, last_1h in await cur.fetchall():
                snapshot["queries_last_24h"] += int(last_24h or 0)
                snapshot["queries_last_1h"] += int(last_1h or 0)
                if domain:
                    snapshot["per_ara_24h"][domain] = int(last_24h or 0)

            # qid is the primary key, so the join never multiplies a callback;
            # LEFT keeps the count to every row, as COUNT(*) FROM callbacks.
            cur = await conn.execute("""
                SELECT COUNT(*),
                       COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(b.start_time))), 0)
                FROM callbacks c
                LEFT JOIN shepherd_brain b ON b.qid = c.query_id
                """)
            row = await cur.fetchone()
            snapshot["callbacks_pending"] = int(row[0] or 0)
            snapshot["oldest_callback_age_sec"] = float(row[1] or 0)

            # This database's backends, and every client backend against the
            # server's usable limit: max_connections is server-wide, and the
            # superuser-reserved slots are not the fleet's to use (#223 C11-3).
            cur = await conn.execute("""
                SELECT count(*) FILTER (WHERE datname = current_database()),
                       count(*) FILTER (WHERE backend_type = 'client backend'),
                       current_setting('max_connections')::int
                         - current_setting('superuser_reserved_connections')::int
                FROM pg_stat_activity
                """)
            row = await cur.fetchone()
            snapshot["connection_count"] = int(row[0] or 0)
            snapshot["client_connections"] = int(row[1] or 0)
            snapshot["usable_connections"] = int(row[2] or 0)
        _note_pg_reachable()
    except Exception as e:
        _note_pg_unreachable(e)
        snapshot["error"] = str(e)
    return snapshot


# The whole-table finished-query counts and the on-disk size, refreshed once
# per ``monitor_history_interval_sec`` instead of every tick: they need a scan
# of the whole retention window (or of every data file), and they move far too
# slowly for anyone to see the difference. Kept between refreshes here.
_pg_slow: Dict[str, Any] = {}
_pg_slow_at = 0.0


async def _collect_postgres_slow() -> Dict[str, Any]:
    """Finished-query counts by state, plus DB/WAL size; cached between
    refreshes. A failed refresh keeps the last values and retries next tick."""
    global _pg_slow, _pg_slow_at
    now = time.time()
    if _pg_slow and now - _pg_slow_at < settings.monitor_history_interval_sec:
        return _pg_slow
    finished: Dict[str, int] = {}
    try:
        async with pg_pool.connection(settings.postgres_pool_timeout) as conn:
            cur = await conn.execute("""
                SELECT state, COUNT(*) FROM shepherd_brain
                WHERE state IS NULL OR state IN ('COMPLETED', 'ABANDONED')
                GROUP BY state
                """)
            for state, count in await cur.fetchall():
                finished[state or "UNKNOWN"] = int(count)
    except Exception as e:
        logger.debug(f"Couldn't refresh the finished-query counts: {e}")
        return _pg_slow
    sizes = await _collect_postgres_size()
    if sizes is None:
        # Keep the last reading rather than report an empty disk: a size
        # check fails exactly when the volume is too slow to answer, and a 0
        # there would hide the database from the db_capacity alert.
        sizes = {k: _pg_slow.get(k, 0) for k in ("db_size_bytes", "wal_size_bytes")}
    _pg_slow = {"finished_state_counts": finished, **sizes}
    _pg_slow_at = now
    return _pg_slow


@dataclass
class _InstanceState:
    """What the poller remembers about one Redis between ticks. Kept per
    instance: every server has its own counters, and mixing two servers'
    readings would turn the difference between them into a bogus delta."""

    # Previous ``evicted_keys`` reading, so we can expose a per-tick delta (the
    # raw field is monotonic). ``None`` until the first sample; reset-safe
    # because a restart drops the counter to 0 and we clamp negatives.
    evicted_keys: Optional[int] = None
    # Previous ``total_net_input_bytes`` reading and when it was taken, for
    # the write rate.
    net_input_bytes: Optional[int] = None
    net_input_at: float = 0.0
    # Smoothed write rate in bytes/sec; ``None`` until two samples exist.
    write_rate: Optional[float] = None
    # Whether this instance has ever reported a connected replica, so the
    # replica count keeps being recorded in history after it drops to zero.
    had_replicas: bool = False


_instance_state: Dict[str, _InstanceState] = {}


def _write_rate(state: _InstanceState, total: int, now: float) -> Optional[float]:
    """Fold a ``total_net_input_bytes`` reading into the instance's smoothed
    write rate (bytes/sec) and return it.

    The poll interval is a few seconds, so the raw per-tick rate swings with
    every large payload. It is smoothed as an exponential moving average over
    ``monitor_redis_write_rate_window_sec``, weighted by the time each sample
    covers so an uneven tick spacing doesn't distort it. A counter that went
    backwards (the server restarted, or a failover put another server behind
    this name) is a new baseline, not a rate: the smoothed value is kept, as
    the load on the instance did not change because the server did.
    """
    previous, previous_at = state.net_input_bytes, state.net_input_at
    state.net_input_bytes, state.net_input_at = total, now
    if previous is None or total < previous or now <= previous_at:
        return state.write_rate
    elapsed = now - previous_at
    raw = (total - previous) / elapsed
    window = settings.monitor_redis_write_rate_window_sec
    if state.write_rate is None or window <= 0:
        state.write_rate = raw
    else:
        weight = 1.0 - math.exp(-elapsed / window)
        state.write_rate += weight * (raw - state.write_rate)
    return state.write_rate


def _replication(info: Dict[str, Any]) -> Dict[str, Any]:
    """The replication half of INFO: this server's role, its replication id
    (how a failover is told from a restart, see monitor.recovery), and how far
    behind each connected replica is."""
    offset = int(info.get("master_repl_offset", 0) or 0)
    replicas: List[Dict[str, Any]] = []
    # redis-py parses each ``slaveN:ip=..,port=..,state=..,offset=..,lag=..``
    # line into a dict.
    for key, entry in info.items():
        if not (key.startswith("slave") and key[5:].isdigit()):
            continue
        if not isinstance(entry, dict):
            continue
        replica_offset = int(entry.get("offset", 0) or 0)
        replicas.append(
            {
                "addr": f"{entry.get('ip')}:{entry.get('port')}",
                "state": entry.get("state"),
                # seconds since the primary last heard from it
                "lag_sec": int(entry.get("lag", 0) or 0),
                # replication stream it has not acknowledged yet: what a
                # failover to it right now would lose
                "lag_bytes": max(0, offset - replica_offset),
            }
        )
    return {
        "role": info.get("role"),
        "connected_replicas": int(info.get("connected_slaves", 0) or 0),
        "replicas": replicas,
        "replica_max_lag_sec": max((r["lag_sec"] for r in replicas), default=None),
        "replica_max_lag_bytes": max((r["lag_bytes"] for r in replicas), default=None),
        "master_replid": info.get("master_replid"),
        "master_replid2": info.get("master_replid2"),
    }


async def _collect_instance(
    instance: str, roles: List[str], client: Any
) -> Dict[str, Any]:
    """One Redis instance's numbers, from a single INFO."""
    tags: Dict[str, Any] = {"instance": instance, "roles": list(roles)}
    try:
        tags["sentinel"] = redis_instances.uses_sentinel(roles)
        info = await client.info()
    except Exception as e:
        return {**tags, "error": str(e)}
    state = _instance_state.setdefault(instance, _InstanceState())
    used = int(info.get("used_memory", 0) or 0)
    maxmem = int(info.get("maxmemory", 0) or 0)
    evicted = int(info.get("evicted_keys", 0) or 0)
    # Delta since the previous tick. Clamp to >= 0 so a counter reset (broker
    # restart) reads as "no evictions this interval" rather than a huge negative.
    evicted_delta = 0
    if state.evicted_keys is not None:
        evicted_delta = max(0, evicted - state.evicted_keys)
    state.evicted_keys = evicted
    # What the kernel counts against the container's memory limit: the
    # process's resident memory (data + allocator fragmentation + client
    # buffers) plus the pages a running snapshot / AOF-rewrite fork has copied
    # since it forked. ``used_memory`` sees none of the extra, which is how the
    # broker can be OOM-killed with the maxmemory alert never firing.
    rss = int(info.get("used_memory_rss", 0) or 0)
    footprint = rss + int(info.get("current_cow_size", 0) or 0)
    limit = redis_instances.memory_limit_bytes(roles)
    write_rate = None
    if info.get("total_net_input_bytes") is not None:
        write_rate = _write_rate(
            state, int(info.get("total_net_input_bytes") or 0), time.time()
        )
    replication = _replication(info)
    state.had_replicas = state.had_replicas or replication["connected_replicas"] > 0
    return {
        **tags,
        "used_memory_human": info.get("used_memory_human"),
        "used_memory_bytes": used,
        # ``maxmemory`` is 0 when the broker runs uncapped; downstream treats
        # that as "no cap" and skips the percentage-based memory alert.
        "maxmemory_bytes": maxmem,
        "maxmemory_pct": round(100.0 * used / maxmem, 1) if maxmem else None,
        "maxmemory_policy": info.get("maxmemory_policy"),
        "evicted_keys": evicted,
        "evicted_keys_delta": evicted_delta,
        "mem_fragmentation_ratio": info.get("mem_fragmentation_ratio"),
        "used_memory_rss_bytes": rss,
        "footprint_bytes": footprint,
        # The container's memory limit (``redis_memory_limit`` for the broker
        # role's instance, ``redis_<role>_memory_limit`` for another); 0 when
        # unset, which skips the footprint side of the memory alert.
        "memory_limit_bytes": limit,
        "memory_limit_pct": round(100.0 * footprint / limit, 1) if limit else None,
        # Persistence health. A failed snapshot or AOF write means a restart
        # would lose more than it should, and nothing else surfaces it.
        "aof_enabled": int(info.get("aof_enabled", 0) or 0),
        "aof_last_write_status": info.get("aof_last_write_status"),
        "aof_last_bgrewrite_status": info.get("aof_last_bgrewrite_status"),
        "rdb_last_bgsave_status": info.get("rdb_last_bgsave_status"),
        "rdb_last_save_time": info.get("rdb_last_save_time"),
        "fork_in_progress": int(info.get("rdb_bgsave_in_progress", 0) or 0)
        or int(info.get("aof_rewrite_in_progress", 0) or 0),
        "latest_fork_usec": info.get("latest_fork_usec"),
        "connected_clients": info.get("connected_clients"),
        "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec"),
        "uptime_in_seconds": info.get("uptime_in_seconds"),
        # New on every Redis start: how the monitor spots a restart (see
        # monitor.recovery), and whether it is still loading its dataset.
        "run_id": info.get("run_id"),
        "loading": int(info.get("loading", 0) or 0),
        # Smoothed bytes/sec arriving from clients: nearly all of it is
        # payload writes. ``None`` until a second sample exists.
        "write_bytes_per_sec": None if write_rate is None else round(write_rate, 1),
        # How long a newly written key survives once the instance is at its
        # cap and evicting oldest-first: the cap divided by the write rate.
        # ``None`` when uncapped or nothing is being written.
        "eviction_window_sec": (
            round(maxmem / write_rate, 1) if maxmem and write_rate else None
        ),
        **replication,
        "had_replicas": state.had_replicas,
    }


async def _collect_redis_info() -> Dict[str, Any]:
    """The numbers of the Redis that serves the broker role: the only Redis
    there is by default, and ``snapshot["redis"]`` either way."""
    instance, roles = redis_instances.discover()[0]
    return await _collect_instance(instance, roles, broker_client)


async def _collect_redis_instances() -> Dict[str, Dict[str, Any]]:
    """Every distinct Redis' numbers, keyed by instance, the broker role's
    first. One INFO per instance however many roles it serves, all at once;
    an instance that can't be reached reports its ``error`` in its own entry
    and leaves the others' intact."""
    found = redis_instances.discover()

    async def _extra(instance: str, roles: List[str]) -> Dict[str, Any]:
        try:
            client = redis_instances.extra_client(instance, roles)
        except Exception as e:
            return {"instance": instance, "roles": list(roles), "error": str(e)}
        return await _collect_instance(instance, roles, client)

    collected = await asyncio.gather(
        _collect_redis_info(),
        *(_extra(instance, roles) for instance, roles in found[1:]),
    )
    return {instance: stats for (instance, _), stats in zip(found, collected)}


async def _collect_payload_store() -> Dict[str, Any]:
    """The ARS payload volume's state, as the ARS watchdog last published it
    (it is the process with the volume mounted). Empty when the ARS isn't
    running, payloads are kept in Postgres, or the watchdog has gone quiet
    for long enough that the key expired."""
    try:
        state = retention.read_usage(await broker_client.get(retention.USAGE_KEY))
    except Exception:
        return {}
    if not state or not state.get("total_bytes"):
        return {}
    total = state["total_bytes"]
    state["used_pct"] = round(100.0 * state.get("used_bytes", 0) / total, 1)
    retained = state.get("retained_bytes")
    state["retained_pct"] = (
        round(100.0 * retained / total, 1) if retained is not None else None
    )
    return state


async def _collect_postgres_size() -> "Dict[str, int] | None":
    """Logical DB size + WAL size in bytes, or None if they couldn't be read.

    Used for the History infra panel and for the disk-fullness estimate that
    drives the ``db_capacity`` alert. WAL is included because a stalled archiver
    or inactive replication slot can balloon ``pg_wal`` independently of table
    growth. Note this is still only an *estimate* of on-disk usage (it excludes
    temp files and filesystem overhead), so it under-counts true volume usage.
    """
    sizes = {"db_size_bytes": 0, "wal_size_bytes": 0}
    try:
        async with pg_pool.connection(settings.postgres_pool_timeout) as conn:
            cur = await conn.execute("SELECT pg_database_size(current_database())")
            row = await cur.fetchone()
            sizes["db_size_bytes"] = int(row[0] or 0)
            try:
                cur = await conn.execute(
                    "SELECT COALESCE(SUM(size), 0) FROM pg_ls_waldir()"
                )
                row = await cur.fetchone()
                sizes["wal_size_bytes"] = int(row[0] or 0)
            except Exception:
                # pg_ls_waldir needs superuser/pg_monitor; skip if not granted.
                pass
    except Exception as e:
        logger.warning(f"Couldn't read the Postgres database size: {e}")
        return None
    return sizes


# Worker -> stream attribution. The heartbeat stream is exactly the queue the
# worker reads from, so this is just the stream name.
def _worker_type_from_stream(stream: str) -> str:
    return stream


def _rollup_workers(workers: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Group heartbeats by worker type (stream)."""
    grouped: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "alive": 0,
            "stale": 0,
            "consumers": [],
            "task_limit_total": 0,
            "in_flight_total": 0,
        }
    )
    for hb in workers:
        wtype = _worker_type_from_stream(hb.get("stream", "unknown"))
        bucket = grouped[wtype]
        if hb.get("stale"):
            bucket["stale"] += 1
        else:
            bucket["alive"] += 1
        bucket["consumers"].append(
            {
                "consumer": hb.get("consumer"),
                "started_at": hb.get("started_at"),
                "last_seen": hb.get("last_seen"),
                "task_limit": hb.get("task_limit"),
                "in_flight": hb.get("in_flight"),
                "rss_bytes": hb.get("rss_bytes"),
                "mem_limit_bytes": hb.get("mem_limit_bytes"),
                "cpu_pct": hb.get("cpu_pct"),
                "cpu_count": hb.get("cpu_count"),
                "version": hb.get("version"),
                "stale": hb.get("stale", False),
            }
        )
        bucket["task_limit_total"] += int(hb.get("task_limit", 0) or 0)
        bucket["in_flight_total"] += int(hb.get("in_flight") or 0)
    # Distinct image versions across replicas, so the dashboard can show which
    # version a worker type runs and flag a mixed fleet (e.g. mid-rollout).
    # Replicas from images that predate version reporting show as "unknown".
    for bucket in grouped.values():
        bucket["versions"] = sorted(
            {c.get("version") or "unknown" for c in bucket["consumers"]}
        )
    return dict(grouped)


async def _load_worker_state(stream: str) -> Dict[str, Any]:
    try:
        raw = await broker_client.get(_worker_state_key(stream))
    except Exception:
        return {}
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


async def _save_worker_state(stream: str, state: Dict[str, Any]) -> None:
    try:
        pipe = broker_client.pipeline()
        pipe.set(_worker_state_key(stream), json.dumps(state))
        pipe.sadd(KNOWN_WORKERS_KEY, stream)
        await pipe.execute()
    except Exception as e:
        logger.debug(f"Failed to persist worker state for {stream}: {e}")


async def _known_workers() -> Set[str]:
    try:
        return set(await broker_client.smembers(KNOWN_WORKERS_KEY))
    except Exception:
        return set()


async def _resolve_worker_states(
    workers_rollup: Dict[str, Dict[str, Any]],
    shutdown_markers: Set[Tuple[str, str]],
    now: float,
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """Annotate each worker type with persistent state and emit transition events.

    The state machine per worker type:

    * **alive**: at least one current heartbeat
    * **scaled_down**: was alive last tick, now zero alive, *and* at least one
      shutdown marker exists for this stream (a worker exited cleanly via
      SIGTERM/SIGINT)
    * **crashed**: was alive last tick, now zero alive, and no shutdown marker
      was visible at the moment of transition
    * Once in ``crashed`` or ``scaled_down`` the type stays there until alive
      heartbeats return -- so a marker expiring after 2 min doesn't flip the
      state back.
    """
    known = await _known_workers()
    all_streams = set(workers_rollup.keys()) | known

    enhanced: Dict[str, Dict[str, Any]] = {}
    events: List[Dict[str, Any]] = []

    markers_by_stream: Dict[str, int] = defaultdict(int)
    for stream, _c in shutdown_markers:
        markers_by_stream[stream] += 1

    for stream in sorted(all_streams):
        info = workers_rollup.get(
            stream,
            {
                "alive": 0,
                "stale": 0,
                "consumers": [],
                "task_limit_total": 0,
                "in_flight_total": 0,
                "versions": [],
            },
        )
        prev = await _load_worker_state(stream)
        prev_state = prev.get("state", "unknown")
        prev_alive = int(prev.get("last_alive", 0))
        prev_seen_alive = float(prev.get("last_seen_alive", 0))

        alive_now = info["alive"]

        if alive_now > 0:
            new_state = "alive"
        elif prev_state == "alive":
            # Just transitioned to zero: the decision is locked in *now* based
            # on whether any shutdown marker is currently observable.
            new_state = (
                "scaled_down" if markers_by_stream.get(stream, 0) > 0 else "crashed"
            )
        elif prev_state in ("crashed", "scaled_down"):
            new_state = prev_state
        else:
            # First time we're seeing this stream and it isn't alive. Could be a
            # seed stream that hasn't been used. Keep it as unknown until proven.
            new_state = "unknown"

        state_changed = new_state != prev_state
        state_changed_at = (
            now if state_changed else float(prev.get("state_changed_at", now))
        )

        # Emit transition events. Scale-up only matters when we move from a
        # non-alive state into alive.
        if state_changed:
            if new_state in ("crashed", "scaled_down"):
                events.append(
                    {
                        "type": "scale_down",
                        "worker": stream,
                        "from": prev_alive,
                        "to": alive_now,
                        "kind": new_state,
                    }
                )
            elif new_state == "alive" and prev_state in (
                "crashed",
                "scaled_down",
                "unknown",
            ):
                events.append(
                    {
                        "type": "scale_up",
                        "worker": stream,
                        "from": prev_alive,
                        "to": alive_now,
                        "recovered_from": prev_state,
                    }
                )
        elif new_state == "alive" and alive_now != prev_alive:
            # Steady-state scaling within the "alive" state.
            events.append(
                {
                    "type": "scale_up" if alive_now > prev_alive else "scale_down",
                    "worker": stream,
                    "from": prev_alive,
                    "to": alive_now,
                    "kind": "alive",
                }
            )

        new_record = {
            "state": new_state,
            "last_alive": alive_now if alive_now > 0 else prev_alive,
            "state_changed_at": state_changed_at,
            "last_seen_alive": now if alive_now > 0 else prev_seen_alive,
        }
        await _save_worker_state(stream, new_record)

        info["state"] = new_state
        info["state_changed_at"] = state_changed_at
        info["last_seen_alive"] = new_record["last_seen_alive"]
        info["last_alive_count"] = prev_alive if alive_now == 0 else alive_now
        enhanced[stream] = info

    return enhanced, events


def _discover_streams(
    workers: List[Dict[str, Any]], known: Set[str] | None = None
) -> List[str]:
    discovered = {hb.get("stream") for hb in workers if hb.get("stream")}
    seed = set(SEED_STREAMS) | (known or set())
    return sorted(seed | discovered)


def _attach_load_to_workers(
    workers_rollup: Dict[str, Dict[str, Any]],
    stream_stats: Dict[str, Dict[str, Any]],
) -> None:
    """Fold stream backlog/pending into each worker card.

    A worker type's stream is itself, so the lookup is direct.

    Workers ``XDEL`` an entry only once it's finished (``mark_task_as_complete``),
    so ``XLEN`` counts both tasks still waiting *and* tasks a consumer has
    already read and is working on (the group's pending entries). ``backlog`` is
    just the waiting part -- ``XLEN - pending`` -- so a single task in progress
    reads as Running 1 / Backlog 0 rather than 1 and 1. Clamped at zero because
    the two reads aren't atomic and acked-but-not-yet-deleted leftovers (which
    the janitor sweeps) can skew either side briefly.

    ``utilization`` stays total load (waiting + in progress, i.e. ``XLEN``)
    against capacity, so a worker type that's saturated with an empty queue
    still reads as busy. ``xlen`` is kept so history keeps recording the same
    total-depth series it always has.
    """
    for wtype, info in workers_rollup.items():
        stats = stream_stats.get(wtype) or {}
        xlen = int(stats.get("xlen", 0) or 0)
        pending = int(stats.get("pending", 0) or 0)
        capacity = int(info.get("task_limit_total", 0) or 0)
        info["xlen"] = xlen
        info["backlog"] = max(0, xlen - pending)
        info["pending"] = pending
        info["capacity"] = capacity
        info["utilization"] = (
            (xlen / capacity) if capacity > 0 else (1.0 if xlen > 0 else 0.0)
        )


async def collect_snapshot() -> Dict[str, Any]:
    """Build one full snapshot. Safe to call independently for /api/snapshot."""
    workers, shutdown_markers, known = await asyncio.gather(
        _collect_heartbeats(),
        _collect_shutdown_markers(),
        _known_workers(),
    )
    streams = _discover_streams(workers, known)
    stream_stats, pg_state, redis_all, pg_slow, payloads = await asyncio.gather(
        _collect_streams(streams),
        _collect_postgres(),
        _collect_redis_instances(),
        _collect_postgres_slow(),
        _collect_payload_store(),
    )
    # live unfinished counts on top of the cached finished ones
    pg_state["state_counts"] = {
        **pg_slow.get("finished_state_counts", {}),
        **pg_state["state_counts"],
    }
    db_size = pg_slow.get("db_size_bytes", 0)
    wal_size = pg_slow.get("wal_size_bytes", 0)
    capacity = settings.pg_volume_capacity_bytes
    on_disk = db_size + wal_size
    pg_state["db_size_bytes"] = db_size
    pg_state["wal_size_bytes"] = wal_size
    pg_state["disk_capacity_bytes"] = capacity
    pg_state["disk_used_bytes"] = on_disk
    pg_state["disk_used_pct"] = (
        round(100.0 * on_disk / capacity, 2) if capacity > 0 else 0.0
    )
    workers_rollup = _rollup_workers(workers)
    now = time.time()
    workers_rollup, events = await _resolve_worker_states(
        workers_rollup, shutdown_markers, now
    )
    _attach_load_to_workers(workers_rollup, stream_stats)
    snapshot = {
        "ts": now,
        "workers": workers_rollup,
        "streams": stream_stats,
        "postgres": pg_state,
        # ``redis`` is the broker role's instance (the only one by default);
        # ``redis_instances`` has every instance, that one included.
        "redis": next(iter(redis_all.values())),
        "redis_instances": redis_all,
        "ars_payload_store": payloads,
        "events": events,
        "aras": ARAS,
    }
    # Persist any events to the durable archive. Done here (not in alerts.py
    # or _resolve_worker_states) so we have a single funnel for everything
    # the History tab might want to surface.
    for ev in events:
        try:
            await storage.insert_event(
                event_type=ev.get("type", "scale_change"),
                worker=ev.get("worker"),
                severity=("critical" if ev.get("kind") == "crashed" else "info"),
                detail=(
                    f"{ev.get('worker')} {ev.get('type')} {ev.get('from')} -> "
                    f"{ev.get('to')} ({ev.get('kind', 'unknown')})"
                ),
                payload=ev,
                unix_ts=now,
            )
        except Exception:
            pass
    return snapshot


def _redis_samples(prefix: str, redis_info: Dict[str, Any]) -> Dict[str, Any]:
    """One Redis instance's history samples, named ``<prefix>:<metric>``."""
    samples: Dict[str, Any] = {}
    if redis_info.get("used_memory_bytes"):
        samples[f"{prefix}:used_memory_bytes"] = redis_info["used_memory_bytes"]
    if redis_info.get("maxmemory_bytes"):
        samples[f"{prefix}:maxmemory_bytes"] = redis_info["maxmemory_bytes"]
    if redis_info.get("evicted_keys") is not None:
        samples[f"{prefix}:evicted_keys"] = int(redis_info.get("evicted_keys") or 0)
    if redis_info.get("connected_clients") is not None:
        samples[f"{prefix}:connected_clients"] = int(
            redis_info.get("connected_clients") or 0
        )
    if redis_info.get("instantaneous_ops_per_sec") is not None:
        samples[f"{prefix}:ops_per_sec"] = int(
            redis_info.get("instantaneous_ops_per_sec") or 0
        )
    if redis_info.get("write_bytes_per_sec") is not None:
        samples[f"{prefix}:write_bytes_per_sec"] = redis_info["write_bytes_per_sec"]
    if redis_info.get("eviction_window_sec") is not None:
        samples[f"{prefix}:eviction_window_sec"] = redis_info["eviction_window_sec"]
    # Replication, only for an instance that has (or has had) replicas, so a
    # single-server deployment doesn't grow series of constant zeros.
    if redis_info.get("had_replicas"):
        samples[f"{prefix}:connected_replicas"] = int(
            redis_info.get("connected_replicas") or 0
        )
        if redis_info.get("replica_max_lag_sec") is not None:
            samples[f"{prefix}:replica_lag_sec"] = redis_info["replica_max_lag_sec"]
            samples[f"{prefix}:replica_lag_bytes"] = redis_info["replica_max_lag_bytes"]
    return samples


async def write_history(snapshot: Dict[str, Any]) -> None:
    """Persist a tick's scalars into both the Redis recent-window cache and
    the Postgres durable archive."""
    ts = snapshot["ts"]
    samples: Dict[str, Any] = {}
    for stream, stats in snapshot["streams"].items():
        samples[f"xlen:{stream}"] = stats["xlen"]
        samples[f"pending:{stream}"] = stats["pending"]
        samples[f"consumers:{stream}"] = stats["consumer_count"]
    for wtype, info in snapshot["workers"].items():
        samples[f"workers_alive:{wtype}"] = info["alive"]
        # Total stream depth (waiting + in progress): the History page plots
        # this as "Queue backlog (XLEN)" and derives its utilization heatmap
        # from it, so it deliberately isn't the card's waiting-only backlog.
        samples[f"backlog:{wtype}"] = info.get("xlen", info.get("backlog", 0))
        samples[f"capacity:{wtype}"] = info.get("capacity", 0)
    pg = snapshot["postgres"]
    samples["pg:callbacks_pending"] = pg.get("callbacks_pending", 0)
    samples["pg:queries_last_1h"] = pg.get("queries_last_1h", 0)
    samples["pg:oldest_callback_age_sec"] = pg.get("oldest_callback_age_sec", 0)
    samples["pg:connection_count"] = pg.get("connection_count", 0)
    samples["pg:db_size_bytes"] = pg.get("db_size_bytes", 0)
    samples["pg:wal_size_bytes"] = pg.get("wal_size_bytes", 0)
    if pg.get("disk_capacity_bytes"):
        samples["pg:disk_capacity_bytes"] = pg["disk_capacity_bytes"]
        samples["pg:disk_used_bytes"] = pg.get("disk_used_bytes", 0)
        samples["pg:disk_used_pct"] = pg.get("disk_used_pct", 0)
    for state, count in pg.get("state_counts", {}).items():
        samples[f"pg:state:{state}"] = count
    # The broker role's instance keeps the series names it always had
    # (``redis:...``); any other instance records the same series under
    # ``redis@<instance>:...``.
    for view in redis_instances.views(snapshot):
        prefix = "redis" if view.primary else f"redis@{view.instance}"
        samples.update(_redis_samples(prefix, view.stats))

    # Live cache: bounded Redis sorted sets used by the live dashboard.
    await history.record_many(samples, ts=ts)
    # Durable archive: 30-day retention in Postgres, used by History tab.
    await storage.insert_metrics(samples, unix_ts=ts)

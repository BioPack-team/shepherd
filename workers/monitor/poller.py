"""Snapshot collector for the monitor dashboard.

Polls Redis (queue depths, pending tasks, worker heartbeats) and Postgres
(query state, callbacks) every ``MONITOR_POLL_INTERVAL_SEC`` and assembles a
single JSON snapshot. The snapshot is written into the rolling history and
broadcast to connected websocket clients.
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

from shepherd_utils.broker import broker_client
from shepherd_utils.db import pool as pg_pool
from shepherd_utils.heartbeat import (
    HEARTBEAT_SCAN_PATTERN,
    HEARTBEAT_TTL_SEC,
    SHUTDOWN_SCAN_PATTERN,
)

from . import history

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
    "gandalf",
    "gandalf.rehydrate",
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
    for raw in raw_values:
        if not raw:
            continue
        try:
            hb = json.loads(raw)
        except json.JSONDecodeError:
            continue
        hb["age_sec"] = max(0.0, now - hb.get("last_seen", now))
        hb["stale"] = hb["age_sec"] > HEARTBEAT_TTL_SEC
        workers.append(hb)
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
        pipe.execute_command("XPENDING", s, "consumer")
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
        if isinstance(xpending, (list, tuple)) and len(xpending) >= 1:
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


async def _collect_postgres() -> Dict[str, Any]:
    """Query state breakdown, callback backlog, ARA volume."""
    snapshot: Dict[str, Any] = {
        "state_counts": {},
        "status_counts": {},
        "queries_last_1h": 0,
        "queries_last_24h": 0,
        "callbacks_pending": 0,
        "oldest_callback_age_sec": 0,
        "per_ara_24h": {},
        "connection_count": 0,
    }
    try:
        async with pg_pool.connection(10) as conn:
            cur = await conn.execute(
                "SELECT state, COUNT(*) FROM shepherd_brain GROUP BY state"
            )
            for state, count in await cur.fetchall():
                snapshot["state_counts"][state or "UNKNOWN"] = int(count)

            cur = await conn.execute(
                "SELECT status, COUNT(*) FROM shepherd_brain GROUP BY status"
            )
            for status, count in await cur.fetchall():
                snapshot["status_counts"][status or "UNKNOWN"] = int(count)

            cur = await conn.execute(
                "SELECT COUNT(*) FROM shepherd_brain WHERE start_time > NOW() - INTERVAL '1 hour'"
            )
            row = await cur.fetchone()
            snapshot["queries_last_1h"] = int(row[0] or 0)

            cur = await conn.execute(
                "SELECT COUNT(*) FROM shepherd_brain WHERE start_time > NOW() - INTERVAL '24 hours'"
            )
            row = await cur.fetchone()
            snapshot["queries_last_24h"] = int(row[0] or 0)

            cur = await conn.execute("SELECT COUNT(*) FROM callbacks")
            row = await cur.fetchone()
            snapshot["callbacks_pending"] = int(row[0] or 0)

            cur = await conn.execute(
                """
                SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(b.start_time))), 0)
                FROM callbacks c
                JOIN shepherd_brain b ON b.qid = c.query_id
                """
            )
            row = await cur.fetchone()
            snapshot["oldest_callback_age_sec"] = float(row[0] or 0)

            cur = await conn.execute(
                """
                SELECT domain, COUNT(*) FROM shepherd_brain
                WHERE start_time > NOW() - INTERVAL '24 hours'
                GROUP BY domain
                """
            )
            for domain, count in await cur.fetchall():
                if domain:
                    snapshot["per_ara_24h"][domain] = int(count)

            cur = await conn.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
            )
            row = await cur.fetchone()
            snapshot["connection_count"] = int(row[0] or 0)
    except Exception as e:
        logger.warning(f"Postgres snapshot failed: {e}")
        snapshot["error"] = str(e)
    return snapshot


async def _collect_redis_info() -> Dict[str, Any]:
    try:
        info = await broker_client.info()
    except Exception as e:
        return {"error": str(e)}
    return {
        "used_memory_human": info.get("used_memory_human"),
        "connected_clients": info.get("connected_clients"),
        "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec"),
        "uptime_in_seconds": info.get("uptime_in_seconds"),
    }


# Worker -> stream attribution. The heartbeat stream is exactly the queue the
# worker reads from, so this is just the stream name.
def _worker_type_from_stream(stream: str) -> str:
    return stream


def _rollup_workers(workers: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Group heartbeats by worker type (stream)."""
    grouped: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"alive": 0, "stale": 0, "consumers": [], "task_limit_total": 0}
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
                "stale": hb.get("stale", False),
            }
        )
        bucket["task_limit_total"] += int(hb.get("task_limit", 0) or 0)
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
            {"alive": 0, "stale": 0, "consumers": [], "task_limit_total": 0},
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
            elif new_state == "alive" and prev_state in ("crashed", "scaled_down", "unknown"):
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

    A worker type's stream is itself, so the lookup is direct. Utilization is
    clamped to a sensible range for display; the raw numbers stay accurate.
    """
    for wtype, info in workers_rollup.items():
        stats = stream_stats.get(wtype) or {}
        xlen = int(stats.get("xlen", 0) or 0)
        pending = int(stats.get("pending", 0) or 0)
        backlog = xlen
        capacity = int(info.get("task_limit_total", 0) or 0)
        info["backlog"] = backlog
        info["pending"] = pending
        info["capacity"] = capacity
        info["utilization"] = (
            (backlog / capacity) if capacity > 0 else (1.0 if backlog > 0 else 0.0)
        )


async def collect_snapshot() -> Dict[str, Any]:
    """Build one full snapshot. Safe to call independently for /api/snapshot."""
    workers, shutdown_markers, known = await asyncio.gather(
        _collect_heartbeats(),
        _collect_shutdown_markers(),
        _known_workers(),
    )
    streams = _discover_streams(workers, known)
    stream_stats, pg_state, redis_info = await asyncio.gather(
        _collect_streams(streams),
        _collect_postgres(),
        _collect_redis_info(),
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
        "redis": redis_info,
        "events": events,
        "aras": ARAS,
    }
    return snapshot


async def write_history(snapshot: Dict[str, Any]) -> None:
    """Persist a few key scalars into the rolling history."""
    ts = snapshot["ts"]
    samples: Dict[str, Any] = {}
    for stream, stats in snapshot["streams"].items():
        samples[f"xlen:{stream}"] = stats["xlen"]
        samples[f"pending:{stream}"] = stats["pending"]
        samples[f"consumers:{stream}"] = stats["consumer_count"]
    for wtype, info in snapshot["workers"].items():
        samples[f"workers_alive:{wtype}"] = info["alive"]
    pg = snapshot["postgres"]
    samples["pg:callbacks_pending"] = pg.get("callbacks_pending", 0)
    samples["pg:queries_last_1h"] = pg.get("queries_last_1h", 0)
    samples["pg:oldest_callback_age_sec"] = pg.get("oldest_callback_age_sec", 0)
    samples["pg:connection_count"] = pg.get("connection_count", 0)
    for state, count in pg.get("state_counts", {}).items():
        samples[f"pg:state:{state}"] = count
    await history.record_many(samples, ts=ts)

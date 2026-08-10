"""Reclaim orphaned Redis Streams messages from dead consumers.

Standard Redis Streams idiom: when a worker dies mid-task, its pending
messages stay assigned to it forever. Every worker periodically scans its
stream's PEL and uses ``XCLAIM`` to take over messages whose owner is no
longer alive so the work gets retried instead of dropped.

Aliveness is determined by the heartbeat keys, which are persistent (no Redis
TTL, so eviction can't shed them) and carry a ``last_seen`` timestamp refreshed
every ~5s; a consumer counts as alive only while that timestamp is fresh (see
``is_heartbeat_fresh``), so a stale leftover key doesn't shield a dead consumer.
Two independent gates protect a busy-but-alive consumer from being robbed:

1. **Heartbeat filter**: a message owned by a consumer with a live heartbeat
   is never claimed, regardless of idle time.
2. **Idle floor**: the message must have been idle for at least
   ``min_idle_sec`` seconds. The right value is per-worker -- it needs to
   exceed the worst-case legitimate processing time for that stream's tasks,
   but stay tight enough that a crashed task can be retried within the
   end-to-end query budget. Configured in :data:`PER_STREAM_MIN_IDLE_SEC`.

Multiple consumers may run reclaim concurrently; ``XCLAIM`` is atomic per
message, so at most one wins. Worst-case duplicate execution if a heartbeat
goes silent while the main loop keeps running is bounded -- the existing
``handle_task_failure`` path keeps the second execution from getting stuck.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from .broker import broker_client
from .config import settings
from .heartbeat import HEARTBEAT_PREFIX, is_heartbeat_fresh

# Per-stream idle floor in seconds. A reclaim only triggers once a pending
# message has been idle longer than this, which gates against yanking work
# from a slow-but-alive consumer whose heartbeat momentarily failed to
# refresh (e.g. the event loop stalled on a blocking call).
#
# Set each entry just above the worst-case legitimate task processing time
# for that worker. Streams not listed here use ``settings.reclaim_min_idle_sec``
# as the default (tuned for fast workers).
PER_STREAM_MIN_IDLE_SEC: Dict[str, int] = {
    # Lookup-style workers can legitimately run up to lookup_timeout (210s)
    # before timing out internally; the floor sits just above that.
    "aragorn.lookup": 240,
    "aragorn.pathfinder": 240,
    "aragorn.omnicorp": 240,
    "aragorn.score": 240,
    "bte.lookup": 240,
    "example.lookup": 240,
    # Pathfinding runs in a process pool bounded by pool_task_timeout_sec
    # (300s), so no legitimate task can outlive that; the floor sits just above.
    "arax.pathfinder": 360,
    # Medium-duration workers.
    "arax.rank": 60,
    "merge_message": 60,
    "score_paths": 60,
    "example.score": 30,
    # finish_query sends the async callback, which retries with backoff and can
    # legitimately run for minutes against a slow callback endpoint (httpx
    # timeout 120s x up to 3 attempts). At the fast default (30s) a second
    # consumer could XCLAIM the message mid-callback and deliver it twice. The
    # heartbeat filter is the primary guard; this floor is the backstop.
    "finish_query": 240,
    # Other filter / entry workers fall through to the default (fast).
}


def min_idle_sec_for(stream: str, override: Optional[int] = None) -> int:
    """Resolve the idle floor for a stream, preferring an explicit override."""
    if override is not None:
        return int(override)
    return PER_STREAM_MIN_IDLE_SEC.get(stream, settings.reclaim_min_idle_sec)


async def _alive_consumers_on_stream(stream: str) -> set:
    """Return the set of consumer names with a *fresh* heartbeat for ``stream``.

    Heartbeat keys are persistent, so key existence alone no longer proves
    liveness -- a crashed consumer's key lingers until the monitor reaps it.
    We read each payload and keep only consumers whose ``last_seen`` is fresh,
    so a dead consumer's stale key can't wrongly shield its orphaned messages
    from reclaim. This freshness check is intrinsic here (it reads last_seen
    directly), so reclaim never depends on the monitor being up to reap.
    """
    pattern = f"{HEARTBEAT_PREFIX}:{stream}:*"
    keys: List[str] = []
    async for key in broker_client.scan_iter(match=pattern, count=200):
        keys.append(key)
    if not keys:
        return set()
    values = await broker_client.mget(keys)
    alive: set = set()
    for key, raw in zip(keys, values):
        # key format: worker:heartbeat:{stream}:{consumer}
        parts = key.split(":", 3)
        if len(parts) == 4 and is_heartbeat_fresh(raw):
            alive.add(parts[3])
    return alive


async def reclaim_orphaned(
    stream: str,
    group: str,
    consumer: str,
    logger: logging.Logger,
    min_idle_sec: Optional[int] = None,
    delivery_counts: Optional[Dict[str, int]] = None,
) -> List[Tuple[str, Any]]:
    """Claim any pending messages whose owner is no longer alive.

    Returns the reclaimed messages in the same ``(id, fields_dict)`` shape as
    ``broker.get_task`` so the caller can feed them through its normal task
    pipeline.

    When ``delivery_counts`` is provided it is populated ``{msg_id:
    times_delivered}`` from the XPENDING scan for every candidate, so the caller
    can spot a message that keeps being re-delivered without completing (a poison
    pill) and dead-letter it instead of retrying forever. ``times_delivered`` is
    the count as of this scan, i.e. before the XCLAIM below bumps it.
    """
    effective_min_idle = min_idle_sec_for(stream, min_idle_sec)
    min_idle_ms = max(0, int(effective_min_idle * 1000))
    max_batch = max(1, int(settings.reclaim_max_batch))

    try:
        # XPENDING with IDLE filters at the server, but we still cross-check
        # against heartbeats below to avoid yanking work from a slow-but-alive
        # consumer whose idle just crossed the threshold.
        #
        # Use the typed ``xpending_range`` rather than
        # ``execute_command("XPENDING", ...)``: redis-py registers a response
        # callback on the XPENDING *command name* that only parses the summary
        # form (``XPENDING key group``). Invoked on this extended form
        # (IDLE/start/end/count) that callback raises ``IndexError`` -- which the
        # ``except`` below would swallow at DEBUG level, silently disabling
        # reclaim entirely. ``xpending_range`` returns a list of parsed dicts.
        detail = await broker_client.xpending_range(
            stream, group, min="-", max="+", count=max_batch, idle=min_idle_ms
        )
    except Exception as e:
        logger.debug(f"Reclaim XPENDING failed for {stream}: {e}")
        return []

    if not detail:
        return []

    alive = await _alive_consumers_on_stream(stream)

    candidates: List[str] = []
    for entry in detail:
        raw_id = entry.get("message_id")
        raw_owner = entry.get("consumer")
        if raw_id is None or raw_owner is None:
            continue
        msg_id = raw_id if isinstance(raw_id, str) else raw_id.decode()
        owner = raw_owner if isinstance(raw_owner, str) else raw_owner.decode()
        if owner == consumer:
            continue  # already ours from a previous claim
        if owner in alive:
            continue  # owner is alive -- the dual safety check
        candidates.append(msg_id)
        if delivery_counts is not None:
            # ``times_delivered`` is how many times this message has been
            # delivered/claimed without an ack -- the signal a caller uses to
            # break a poison-pill retry loop.
            delivery_counts[msg_id] = int(entry.get("times_delivered", 0) or 0)

    if not candidates:
        return []

    try:
        claimed = await broker_client.xclaim(
            stream,
            group,
            consumer,
            min_idle_ms,
            candidates,
        )
    except Exception as e:
        logger.warning(f"XCLAIM failed on {stream}: {e}")
        return []

    if claimed:
        ids = [m[0] for m in claimed if isinstance(m, (list, tuple)) and m]
        logger.info(
            f"Reclaimed {len(claimed)} orphaned message(s) on {stream} "
            f"(min_idle={effective_min_idle}s): {ids}"
        )
    return claimed

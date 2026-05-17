"""Reclaim orphaned Redis Streams messages from dead consumers.

Standard Redis Streams idiom: when a worker dies mid-task, its pending
messages stay assigned to it forever. Every worker periodically scans its
stream's PEL and claims any messages whose owner is no longer alive so the
work gets retried instead of dropped on restart.

Aliveness is determined by the heartbeat keys (TTL ~15s presence pings).
Two independent gates protect a busy-but-alive consumer from being robbed:

1. **Heartbeat filter**: a message owned by a consumer with a live heartbeat
   is never claimed.
2. **Idle floor** (``reclaim_min_idle_sec``): the message must have been
   idle for at least this long. This gives a buffer above the worst-case
   legitimate task processing time, so even a momentary heartbeat hiccup
   on an actively-working consumer can't trigger a claim.

Multiple consumers may run reclaim concurrently; ``XCLAIM`` is atomic per
message, so at most one wins. Worst-case duplicate execution if a heartbeat
goes silent while the main loop keeps running is bounded -- the existing
``handle_task_failure`` path keeps the second execution from getting stuck.
"""

import logging
from typing import Any, List, Tuple

from .broker import broker_client
from .config import settings
from .heartbeat import HEARTBEAT_PREFIX


async def _alive_consumers_on_stream(stream: str) -> set:
    """Return the set of consumer names with a live heartbeat for ``stream``."""
    pattern = f"{HEARTBEAT_PREFIX}:{stream}:*"
    alive: set = set()
    async for key in broker_client.scan_iter(match=pattern, count=200):
        # key format: worker:heartbeat:{stream}:{consumer}
        parts = key.split(":", 3)
        if len(parts) == 4:
            alive.add(parts[3])
    return alive


async def reclaim_orphaned(
    stream: str,
    group: str,
    consumer: str,
    logger: logging.Logger,
) -> List[Tuple[str, Any]]:
    """Claim any pending messages whose owner is no longer alive.

    Returns the reclaimed messages in the same ``(id, fields_dict)`` shape as
    ``broker.get_task`` so the caller can feed them through its normal task
    pipeline.
    """
    min_idle_ms = max(0, int(settings.reclaim_min_idle_sec * 1000))
    max_batch = max(1, int(settings.reclaim_max_batch))

    try:
        # XPENDING with IDLE filters at the server, but we still cross-check
        # against heartbeats below to avoid yanking work from a slow-but-alive
        # consumer whose idle just crossed the threshold.
        detail = await broker_client.execute_command(
            "XPENDING",
            stream,
            group,
            "IDLE",
            min_idle_ms,
            "-",
            "+",
            max_batch,
        )
    except Exception as e:
        logger.debug(f"Reclaim XPENDING failed for {stream}: {e}")
        return []

    if not detail or not isinstance(detail, list):
        return []

    alive = await _alive_consumers_on_stream(stream)

    candidates: List[str] = []
    for row in detail:
        if not isinstance(row, list) or len(row) < 2:
            continue
        raw_id, raw_owner = row[0], row[1]
        msg_id = raw_id if isinstance(raw_id, str) else raw_id.decode()
        owner = raw_owner if isinstance(raw_owner, str) else raw_owner.decode()
        if owner == consumer:
            continue  # already ours from a previous claim
        if owner in alive:
            continue  # owner is alive -- the dual safety check
        candidates.append(msg_id)

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
            f"Reclaimed {len(claimed)} orphaned message(s) on {stream}: {ids}"
        )
    return claimed

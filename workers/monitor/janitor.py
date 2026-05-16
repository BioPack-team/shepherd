"""Periodic cleanup tasks for the monitor.

Runs alongside the poll loop on a slower cadence. Handles two things:

* **Stream trim**: ``mark_task_as_complete`` now XDELs each message after ack,
  but legacy entries (and any best-effort XDEL failures) need to be cleaned up
  too. For each known stream we compute a *safe* MINID and call XTRIM:
    - if any messages are pending in the consumer group, MINID is the smallest
      pending id (anything older has been acked);
    - if nothing is pending, MINID is ``last-delivered-id + 1`` in the seq
      dimension. Redis-generated IDs are monotonic, so anything a producer
      adds after this call has a strictly greater id and is preserved.

* **Callback reap**: deletes rows in the ``callbacks`` table whose parent
  query is already in state ``COMPLETED`` (the per-query cleanup in
  ``finish_query`` covers new queries; the reaper sweeps up old leakage).
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from shepherd_utils.broker import broker_client
from shepherd_utils.db import reap_completed_callbacks
from shepherd_utils.heartbeat import HEARTBEAT_SCAN_PATTERN

from .poller import SEED_STREAMS

logger = logging.getLogger("shepherd.monitor.janitor")

JANITOR_INTERVAL_SEC = 300  # 5 min
CONSUMER_GROUP = "consumer"


def _next_stream_id(stream_id: str) -> str:
    """Increment a Redis stream id by one in the sequence dimension."""
    if "-" in stream_id:
        ms, seq = stream_id.split("-", 1)
        try:
            return f"{ms}-{int(seq) + 1}"
        except ValueError:
            return stream_id
    return f"{stream_id}-1"


async def _discover_streams() -> List[str]:
    """Heartbeats tell us which streams have live workers right now."""
    discovered = set(SEED_STREAMS)
    async for key in broker_client.scan_iter(match=HEARTBEAT_SCAN_PATTERN, count=200):
        # key format: worker:heartbeat:{stream}:{consumer}
        parts = key.split(":")
        if len(parts) >= 4:
            discovered.add(parts[2])
    return sorted(discovered)


async def _trim_stream(stream: str) -> Optional[Dict[str, Any]]:
    """Compute and apply a safe MINID trim for one stream.

    Returns a small dict describing what we did, or ``None`` if the stream
    doesn't exist / has no group.
    """
    try:
        xlen = await broker_client.xlen(stream)
    except Exception:
        return None
    if xlen == 0:
        return None

    try:
        xpending = await broker_client.execute_command(
            "XPENDING", stream, CONSUMER_GROUP
        )
    except Exception:
        # No consumer group yet; nothing to trim safely.
        return None

    pending_count = 0
    smallest_pending = None
    if isinstance(xpending, (list, tuple)) and len(xpending) >= 2:
        pending_count = int(xpending[0] or 0)
        smallest_pending = xpending[1]

    minid: Optional[str] = None
    if pending_count > 0 and smallest_pending:
        minid = smallest_pending if isinstance(smallest_pending, str) else smallest_pending.decode()
    else:
        try:
            groups = await broker_client.execute_command(
                "XINFO", "GROUPS", stream
            )
        except Exception:
            return None
        last_delivered: Optional[str] = None
        if isinstance(groups, list):
            for entry in groups:
                kv: Dict[str, Any] = {}
                if isinstance(entry, list):
                    it = iter(entry)
                    for k in it:
                        try:
                            v = next(it)
                        except StopIteration:
                            break
                        kv[str(k)] = v
                if str(kv.get("name")) == CONSUMER_GROUP:
                    last_delivered = kv.get("last-delivered-id")
                    break
        if last_delivered:
            ld = last_delivered if isinstance(last_delivered, str) else last_delivered.decode()
            if ld and ld != "0-0":
                minid = _next_stream_id(ld)

    if not minid:
        return {"stream": stream, "trimmed": 0, "before": xlen, "minid": None}

    try:
        trimmed = await broker_client.execute_command(
            "XTRIM", stream, "MINID", minid
        )
    except Exception as e:
        logger.debug(f"XTRIM failed for {stream} (minid={minid}): {e}")
        return None

    after = xlen - int(trimmed or 0)
    if trimmed:
        logger.info(
            f"Janitor trimmed {trimmed} entries from {stream} (xlen {xlen} -> {after}, minid={minid})"
        )
    return {"stream": stream, "trimmed": int(trimmed or 0), "before": xlen, "after": after}


async def trim_streams() -> List[Dict[str, Any]]:
    streams = await _discover_streams()
    results: List[Dict[str, Any]] = []
    for s in streams:
        out = await _trim_stream(s)
        if out is not None:
            results.append(out)
    return results


async def reap_callbacks() -> int:
    return await reap_completed_callbacks(logger)


async def run_once() -> Dict[str, Any]:
    """Single pass; safe to invoke from an admin endpoint as well."""
    trim_results, callbacks_deleted = await asyncio.gather(
        trim_streams(),
        reap_callbacks(),
        return_exceptions=False,
    )
    return {
        "streams": trim_results,
        "callbacks_deleted": callbacks_deleted,
    }


async def janitor_loop() -> None:
    while True:
        try:
            summary = await run_once()
            total_trimmed = sum(s.get("trimmed", 0) for s in summary["streams"])
            if total_trimmed or summary["callbacks_deleted"]:
                logger.info(
                    f"Janitor: trimmed {total_trimmed} stream entries, "
                    f"reaped {summary['callbacks_deleted']} orphan callbacks"
                )
        except Exception as e:
            logger.exception(f"Janitor iteration failed: {e}")
        await asyncio.sleep(JANITOR_INTERVAL_SEC)

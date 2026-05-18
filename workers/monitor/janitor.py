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

from . import storage
from .poller import SEED_STREAMS

logger = logging.getLogger("shepherd.monitor.janitor")

JANITOR_INTERVAL_SEC = 300  # 5 min
CONSUMER_GROUP = "consumer"
# A consumer with no heartbeat and no pending work is considered dead once it
# has been idle this long. The threshold guards against deleting a worker that
# just started but hasn't ticked its heartbeat yet.
STALE_CONSUMER_IDLE_MS = 60 * 60 * 1000  # 1 hour
# Retention for the Postgres historical archive.
HISTORY_RETENTION_DAYS = 30
RETENTION_SWEEP_INTERVAL_SEC = 24 * 60 * 60  # once per day
RETENTION_LAST_RUN_KEY = "monitor:janitor:last_retention_sweep"


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
        minid = (
            smallest_pending
            if isinstance(smallest_pending, str)
            else smallest_pending.decode()
        )
    else:
        try:
            groups = await broker_client.execute_command("XINFO", "GROUPS", stream)
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
            ld = (
                last_delivered
                if isinstance(last_delivered, str)
                else last_delivered.decode()
            )
            if ld and ld != "0-0":
                minid = _next_stream_id(ld)

    if not minid:
        return {"stream": stream, "trimmed": 0, "before": xlen, "minid": None}

    try:
        trimmed = await broker_client.execute_command("XTRIM", stream, "MINID", minid)
    except Exception as e:
        logger.debug(f"XTRIM failed for {stream} (minid={minid}): {e}")
        return None

    after = xlen - int(trimmed or 0)
    if trimmed:
        logger.info(
            f"Janitor trimmed {trimmed} entries from {stream} (xlen {xlen} -> {after}, minid={minid})"
        )
    return {
        "stream": stream,
        "trimmed": int(trimmed or 0),
        "before": xlen,
        "after": after,
    }


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


async def _list_alive_consumers() -> set:
    """Return ``{(stream, consumer)}`` for every worker with a live heartbeat."""
    alive = set()
    async for key in broker_client.scan_iter(match=HEARTBEAT_SCAN_PATTERN, count=200):
        # worker:heartbeat:{stream}:{consumer}
        parts = key.split(":", 3)
        if len(parts) == 4:
            alive.add((parts[2], parts[3]))
    return alive


async def _xinfo_consumers(stream: str) -> List[Dict[str, Any]]:
    try:
        raw = await broker_client.execute_command(
            "XINFO", "CONSUMERS", stream, CONSUMER_GROUP
        )
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        for entry in raw:
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
                out.append(kv)
    return out


async def cleanup_stale_consumers() -> Dict[str, Any]:
    """Drop consumer-group entries left behind by restarted workers.

    Each worker generates a fresh UUID-based consumer name on startup, so old
    entries accumulate inside the consumer group forever. We use the heartbeat
    keys as the authoritative "alive" set: any consumer that's not in that set,
    has zero pending work, and has been idle past the threshold gets removed
    via XGROUP DELCONSUMER.
    """
    alive = await _list_alive_consumers()
    streams = await _discover_streams()
    deleted = 0
    by_stream: Dict[str, int] = {}
    for stream in streams:
        consumers = await _xinfo_consumers(stream)
        if not consumers:
            continue
        for c in consumers:
            raw_name = c.get("name")
            if not raw_name:
                continue
            name = raw_name if isinstance(raw_name, str) else raw_name.decode()
            pending = int(c.get("pending", 0) or 0)
            idle = int(c.get("idle", 0) or 0)
            if (stream, name) in alive:
                continue  # heartbeat says it's live
            if pending > 0:
                continue  # don't yank in-flight work
            if idle < STALE_CONSUMER_IDLE_MS:
                continue  # may just be slow to heartbeat after startup
            try:
                await broker_client.execute_command(
                    "XGROUP", "DELCONSUMER", stream, CONSUMER_GROUP, name
                )
                deleted += 1
                by_stream[stream] = by_stream.get(stream, 0) + 1
            except Exception as e:
                logger.debug(f"DELCONSUMER {stream} {name} failed: {e}")
    if deleted:
        logger.info(
            f"Janitor deleted {deleted} stale consumers across {len(by_stream)} streams"
        )
    return {"deleted": deleted, "by_stream": by_stream}


async def reclaim_dead_consumers(
    min_idle_seconds: int = 3600,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Drop pending messages stuck on dead consumers, then remove the consumers.

    A dead consumer is one with no live heartbeat. Its pending messages get
    ACKed (so they leave the PEL) and XDELed (so they leave the stream), and
    the consumer entry is removed. This is destructive -- the messages are
    discarded, not retried -- so it lives on a separate admin endpoint instead
    of running automatically.

    Set ``dry_run=True`` to see what would be dropped without acting.
    """
    min_idle_ms = max(0, int(min_idle_seconds * 1000))
    alive = await _list_alive_consumers()
    streams = await _discover_streams()
    summary: Dict[str, Any] = {
        "dry_run": dry_run,
        "min_idle_seconds": min_idle_seconds,
        "dropped_messages": 0,
        "consumers_removed": 0,
        "by_stream": {},
    }
    for stream in streams:
        consumers = await _xinfo_consumers(stream)
        if not consumers:
            continue
        for c in consumers:
            raw_name = c.get("name")
            if not raw_name:
                continue
            name = raw_name if isinstance(raw_name, str) else raw_name.decode()
            pending = int(c.get("pending", 0) or 0)
            idle = int(c.get("idle", 0) or 0)
            if (stream, name) in alive:
                continue
            if pending == 0:
                continue  # cleanup_stale_consumers handles this case
            if idle < min_idle_ms:
                continue

            # Enumerate pending message IDs for this consumer.
            try:
                detail = await broker_client.execute_command(
                    "XPENDING",
                    stream,
                    CONSUMER_GROUP,
                    "IDLE",
                    min_idle_ms,
                    "-",
                    "+",
                    pending,
                    name,
                )
            except Exception as e:
                logger.debug(f"XPENDING detail failed for {stream}/{name}: {e}")
                continue

            msg_ids: List[str] = []
            if isinstance(detail, list):
                for row in detail:
                    if isinstance(row, list) and row:
                        raw_id = row[0]
                        msg_ids.append(
                            raw_id if isinstance(raw_id, str) else raw_id.decode()
                        )

            bucket = summary["by_stream"].setdefault(
                stream, {"messages": 0, "consumers": 0}
            )
            bucket["messages"] += len(msg_ids)
            bucket["consumers"] += 1
            summary["dropped_messages"] += len(msg_ids)
            summary["consumers_removed"] += 1

            if dry_run:
                continue

            if msg_ids:
                try:
                    await broker_client.xack(stream, CONSUMER_GROUP, *msg_ids)
                except Exception as e:
                    logger.warning(f"XACK batch failed for {stream}: {e}")
                try:
                    await broker_client.xdel(stream, *msg_ids)
                except Exception as e:
                    logger.warning(f"XDEL batch failed for {stream}: {e}")
            try:
                await broker_client.execute_command(
                    "XGROUP", "DELCONSUMER", stream, CONSUMER_GROUP, name
                )
            except Exception as e:
                logger.warning(f"DELCONSUMER {stream}/{name} failed: {e}")

    if not dry_run and summary["consumers_removed"]:
        logger.warning(
            f"Reclaimed {summary['dropped_messages']} stuck messages from "
            f"{summary['consumers_removed']} dead consumers"
        )
    return summary


async def sweep_history_retention(force: bool = False) -> Dict[str, Any]:
    """Delete Postgres history rows older than ``HISTORY_RETENTION_DAYS``.

    Rate-limited via a Redis key so the loop doesn't re-run on every 5-min
    tick; pass ``force=True`` to override (used by the admin endpoint).
    """
    import time as _time

    now = _time.time()
    if not force:
        try:
            last = await broker_client.get(RETENTION_LAST_RUN_KEY)
            if last and (now - float(last)) < RETENTION_SWEEP_INTERVAL_SEC:
                return {"skipped": True, "last_run": float(last)}
        except Exception:
            pass

    deletions = await storage.purge_older_than(HISTORY_RETENTION_DAYS)
    try:
        await broker_client.set(RETENTION_LAST_RUN_KEY, str(now))
    except Exception:
        pass
    return {"deleted": deletions, "ran_at": now}


async def run_once() -> Dict[str, Any]:
    """Single pass; safe to invoke from an admin endpoint as well."""
    (
        trim_results,
        callbacks_deleted,
        consumer_cleanup,
        retention,
    ) = await asyncio.gather(
        trim_streams(),
        reap_callbacks(),
        cleanup_stale_consumers(),
        sweep_history_retention(),
        return_exceptions=False,
    )
    return {
        "streams": trim_results,
        "callbacks_deleted": callbacks_deleted,
        "stale_consumers": consumer_cleanup,
        "history_retention": retention,
    }


async def janitor_loop() -> None:
    while True:
        try:
            summary = await run_once()
            total_trimmed = sum(s.get("trimmed", 0) for s in summary["streams"])
            consumers_deleted = summary["stale_consumers"]["deleted"]
            if total_trimmed or summary["callbacks_deleted"] or consumers_deleted:
                logger.info(
                    f"Janitor: trimmed {total_trimmed} stream entries, "
                    f"reaped {summary['callbacks_deleted']} orphan callbacks, "
                    f"deleted {consumers_deleted} stale consumers"
                )
        except Exception as e:
            logger.exception(f"Janitor iteration failed: {e}")
        await asyncio.sleep(JANITOR_INTERVAL_SEC)

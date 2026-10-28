"""Keeping the ARS's durable storage bounded.

Three passes, all run by the ARS watchdog:

* ``purge_expired_payloads`` (hourly): drop the durable payloads of trees
  that finished more than ``ars_data_retention_days`` ago. Row metadata
  stays, so reports and ``latest_pk`` keep working.
* ``evict_for_space`` (every sweep): when the payload volume
  (``ars_payload_dir``) is ``ars_payload_evict_high_pct`` full, drop the
  payloads of the oldest finished trees until it is back under
  ``ars_payload_evict_low_pct``. This is what absorbs a burst that would
  otherwise fill the volume long before the age purge gets to it.
* ``delete_expired_trees`` (hourly): delete the rows of trees older than
  ``ars_row_retention_days``, so ``ars_message`` doesn't grow forever.

Every pass works on whole trees -- a parent plus every child and merged
version under it -- and only on finished ones (every member in a terminal
status), so a tree is either fully readable or has had all its payloads
removed together; it is never left half there. Retained trees (the ARS
``retain`` endpoint) are never touched. A tree that backs a current
response-cache entry is exempt from the age-based passes; under disk
pressure it goes last, after every uncached tree, and its cache entries are
dropped with it so the cache never serves a tree that is gone.

Payloads are counted by ``ars_message.data_size``, not by reading them or
by the volume's free space, so eviction knows how much it has freed even on
a network filesystem whose free space lags behind deletes.
"""

import asyncio
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence

import shepherd_utils.db as shepherd_db
from shepherd_utils.config import settings

from . import payload_store
from .statuses import TERMINAL_STATUSES

# Trees evicted per round trip under disk pressure: small enough that each
# statement is short, large enough that freeing a few GB isn't hundreds of
# round trips.
EVICT_BATCH = 20
# Trees per round trip for the hourly passes.
PURGE_BATCH = 200
# Where the watchdog publishes the payload volume's state for the monitor
# (workers/monitor/poller.py), in the broker DB the monitor reads.
USAGE_KEY = "ars:payload_store:usage"
USAGE_TTL_SEC = 600

_TERMINAL = sorted(TERMINAL_STATUSES)

# Every tree that has a durable payload anywhere in it, with what retention
# needs to know about it. A tree's root is its parent: COALESCE(ref, id).
# Members are found through the parent's own row and through ``ref``
# (both indexed) rather than an OR join, which can't use an index.
_STORED_TREES = """
    WITH stored AS (
      SELECT DISTINCT COALESCE(ref, id) AS root FROM ars_message
      WHERE data IS NOT NULL OR data_size IS NOT NULL
    ), members AS (
      SELECT s.root, m.* FROM stored s JOIN ars_message m ON m.id = s.root
      UNION ALL
      SELECT s.root, m.* FROM stored s JOIN ars_message m ON m.ref = s.root
    ), trees AS (
      SELECT root,
             max(updated_at) AS last_update,
             bool_or(retain) AS retained,
             bool_and(status = ANY(%(terminal)s)) AS finished,
             -- bytes on the payload volume (copies still in Postgres free no
             -- space there, so they don't count toward eviction)
             COALESCE(sum(data_size) FILTER (WHERE data IS NULL), 0) AS file_bytes
      FROM members GROUP BY root
    )
"""

_LIVE_CACHE_SOURCE = """
    EXISTS (
      SELECT 1 FROM ars_response_cache c
      JOIN ars_cache_meta g ON g.generation = c.generation
      WHERE c.source_pk = t.root
    )
"""


async def _drop_payloads(roots: Sequence[uuid.UUID], logger: logging.Logger) -> int:
    """Remove every durable payload in these trees: the files first, then the
    rows' record of them (a reader in between finds no file and falls back
    to the row, which by then says nothing is stored -- the same answer it
    gets afterwards). Returns the bytes freed on the payload volume."""
    if not roots:
        return 0
    roots = list(roots)
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            SELECT id, data_size, data IS NULL FROM ars_message
            WHERE (id = ANY(%s) OR ref = ANY(%s))
              AND (data IS NOT NULL OR data_size IS NOT NULL)
            """,
            (roots, roots),
        )
        rows = await cur.fetchall()
    ids = [r[0] for r in rows]
    freed = sum(int(r[1] or 0) for r in rows if r[2])
    if payload_store.enabled():
        for pk in ids:
            try:
                await asyncio.to_thread(payload_store.delete, pk)
            except Exception as e:
                logger.error(f"Failed to delete payload file for {pk}: {e}")
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        await shepherd_db.lift_statement_timeout(conn)
        await conn.execute(
            "UPDATE ars_message SET data = NULL, data_size = NULL WHERE id = ANY(%s)",
            (ids,),
        )
        await conn.commit()
    return freed


async def purge_expired_payloads(retention_days: int, logger: logging.Logger) -> int:
    """Drop the payloads of finished, non-retained trees whose newest update
    is older than ``retention_days``, except trees backing a current cache
    entry. Returns the number of trees purged."""
    if retention_days <= 0:
        return 0
    purged = 0
    while True:
        async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
            await shepherd_db.lift_statement_timeout(conn)
            cur = await conn.execute(
                _STORED_TREES + f"""
                SELECT t.root FROM trees t
                WHERE t.finished AND NOT t.retained
                  AND t.last_update < NOW() - make_interval(days => %(days)s)
                  AND NOT {_LIVE_CACHE_SOURCE}
                ORDER BY t.last_update
                LIMIT %(limit)s
                """,
                {"terminal": _TERMINAL, "days": retention_days, "limit": PURGE_BATCH},
            )
            roots = [r[0] for r in await cur.fetchall()]
        if not roots:
            return purged
        await _drop_payloads(roots, logger)
        purged += len(roots)
        if len(roots) < PURGE_BATCH:
            return purged


async def _eviction_candidates(limit: int) -> List[Dict[str, Any]]:
    """Finished, non-retained trees with payloads, in eviction order: every
    uncached tree, oldest first, then cache-backed trees, least recently
    used first."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        await shepherd_db.lift_statement_timeout(conn)
        cur = await conn.execute(
            _STORED_TREES + """
            SELECT t.root, t.file_bytes, cache.used_at
            FROM trees t
            LEFT JOIN LATERAL (
              SELECT max(COALESCE(c.last_hit_at, c.ready_at, c.created_at))
                     AS used_at,
                     count(*) AS entries
              FROM ars_response_cache c WHERE c.source_pk = t.root
            ) cache ON TRUE
            WHERE t.finished AND NOT t.retained
            ORDER BY cache.entries > 0, COALESCE(cache.used_at, t.last_update)
            LIMIT %(limit)s
            """,
            {"terminal": _TERMINAL, "limit": limit},
        )
        rows = await cur.fetchall()
    return [
        {"root": r[0], "file_bytes": int(r[1] or 0), "cached": r[2] is not None}
        for r in rows
    ]


async def _drop_cache_entries(roots: Sequence[uuid.UUID]) -> int:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            "DELETE FROM ars_response_cache WHERE source_pk = ANY(%s)",
            (list(roots),),
        )
        deleted = cur.rowcount or 0
        await conn.commit()
    return deleted


async def retained_bytes() -> int:
    """Bytes on the payload volume that belong to retained trees."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        await shepherd_db.lift_statement_timeout(conn)
        cur = await conn.execute(
            _STORED_TREES
            + "SELECT COALESCE(sum(file_bytes), 0) FROM trees WHERE retained",
            {"terminal": _TERMINAL},
        )
        row = await cur.fetchone()
    return int(row[0] or 0) if row else 0


async def evict_for_space(logger: logging.Logger) -> Dict[str, Any]:
    """Under disk pressure, evict whole trees until the payload volume is
    back under the low mark. A no-op without ``ars_payload_dir`` or below
    the high mark. Returns what it did, plus the volume's state."""
    result: Dict[str, Any] = {"trees": 0, "freed_bytes": 0, "short_bytes": 0}
    if not payload_store.enabled():
        return result
    used, total = await asyncio.to_thread(payload_store.usage)
    result.update(used_bytes=used, total_bytes=total)
    if not total or used < total * settings.ars_payload_evict_high_pct / 100:
        return result
    to_free = used - total * settings.ars_payload_evict_low_pct / 100
    while result["freed_bytes"] < to_free:
        batch = await _eviction_candidates(EVICT_BATCH)
        if not batch:
            result["short_bytes"] = int(to_free - result["freed_bytes"])
            logger.warning(
                f"Payload volume still {result['short_bytes']} bytes over its "
                "low mark with nothing left to evict: the rest is retained "
                "trees or trees still running"
            )
            break
        # Only as many trees as it takes; the rest of the batch stays.
        chosen = []
        need = to_free - result["freed_bytes"]
        for tree in batch:
            chosen.append(tree)
            need -= tree["file_bytes"]
            if need <= 0:
                break
        cached = [t["root"] for t in chosen if t["cached"]]
        if cached:
            await _drop_cache_entries(cached)
        result["freed_bytes"] += await _drop_payloads(
            [t["root"] for t in chosen], logger
        )
        result["trees"] += len(chosen)
    logger.info(
        f"Payload volume over {settings.ars_payload_evict_high_pct:.0f}%: "
        f"evicted {result['trees']} trees ({result['freed_bytes']} bytes)"
    )
    return result


async def delete_expired_trees(retention_days: int, logger: logging.Logger) -> int:
    """Delete whole finished, non-retained trees whose newest update is
    older than ``retention_days``: payloads, cache entries, subscriptions
    (by cascade) and every row. Trees backing a current cache entry wait
    until it is gone. Returns the number of trees deleted."""
    if retention_days <= 0:
        return 0
    deleted = 0
    while True:
        async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
            await shepherd_db.lift_statement_timeout(conn)
            cur = await conn.execute(
                f"""
                SELECT t.root FROM (
                  SELECT p.id AS root FROM ars_message p
                  WHERE p.ref IS NULL
                    AND NOT p.retain
                    AND p.status = ANY(%(terminal)s)
                    AND p.updated_at < NOW() - make_interval(days => %(days)s)
                    AND NOT EXISTS (
                      SELECT 1 FROM ars_message c
                      WHERE c.ref = p.id
                        AND (c.retain
                             OR c.status <> ALL(%(terminal)s)
                             OR c.updated_at
                                >= NOW() - make_interval(days => %(days)s))
                    )
                  ORDER BY p.updated_at
                  LIMIT %(limit)s
                ) t
                WHERE NOT {_LIVE_CACHE_SOURCE}
                """,
                {"terminal": _TERMINAL, "days": retention_days, "limit": PURGE_BATCH},
            )
            roots = [r[0] for r in await cur.fetchall()]
        if not roots:
            return deleted
        await _drop_payloads(roots, logger)
        await _drop_cache_entries(roots)
        async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
            await shepherd_db.lift_statement_timeout(conn)
            # One statement: the parent's merged_version and the children's
            # ref point inside the tree, and those checks run at its end.
            await conn.execute(
                "DELETE FROM ars_message WHERE id = ANY(%s) OR ref = ANY(%s)",
                (roots, roots),
            )
            await conn.commit()
        deleted += len(roots)
        if len(roots) < PURGE_BATCH:
            return deleted


async def publish_usage(
    eviction: Dict[str, Any], logger: logging.Logger, broker=None
) -> None:
    """Record the payload volume's state where the monitor reads it."""
    if not payload_store.enabled() or "total_bytes" not in eviction:
        return
    if broker is None:
        from shepherd_utils.broker import broker_client as broker
    try:
        retained = await retained_bytes()
    except Exception as e:
        logger.debug(f"Couldn't sum retained payload bytes: {e}")
        retained = None
    state = {
        "ts": time.time(),
        # what the volume holds once this pass's deletes land (a network
        # filesystem's free space can lag behind them)
        "used_bytes": max(0, eviction["used_bytes"] - eviction["freed_bytes"]),
        "total_bytes": eviction["total_bytes"],
        "retained_bytes": retained,
        "evicted_trees": eviction["trees"],
        "short_bytes": eviction["short_bytes"],
        "high_pct": settings.ars_payload_evict_high_pct,
        "low_pct": settings.ars_payload_evict_low_pct,
    }
    try:
        await broker.set(USAGE_KEY, json.dumps(state), ex=USAGE_TTL_SEC)
    except Exception as e:
        logger.debug(f"Couldn't publish payload volume usage: {e}")


def read_usage(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None

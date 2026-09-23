"""Postgres persistence for the ARS message tree.

Ported from NCATSTranslator/Relay @ 3e65975 (tr_sys/tr_ars/models.py). Uses
the shared Shepherd Postgres pool; payload blobs ride Shepherd's Redis data
store (keyed by ``str(message_pk)``) with a durable zstd copy in
``ars_message.data`` once a message goes terminal, so the UI can fetch
merged results long after the Redis TTL.

Upstream's Agent/Channel/Actor registry is not persisted: the de-federated
ARS only talks to the ARAs this Shepherd deployment hosts, a static roster in
``shepherd_utils.ars.aras``. A message row records the agent name it belongs
to (``ars_message.agent``) instead of pointing at an actor row.
"""

import asyncio
import gzip
import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

import orjson
import zstandard
from psycopg.types.json import Jsonb

import shepherd_utils.db as shepherd_db
from shepherd_utils.config import settings

from . import aras
from .statuses import coerce_code, to_letter, validate_letter

MESSAGE_COLUMNS = (
    "id",
    "name",
    "code",
    "status",
    "agent",
    "ref",
    "ts",
    "updated_at",
    "url",
    "result_count",
    "result_stat",
    "retain",
    "merge_semaphore",
    "merged_version",
    "merged_versions_list",
    "params",
)
_MESSAGE_SELECT = ", ".join(f"m.{c}" for c in MESSAGE_COLUMNS)

CLIENT_COLUMNS = (
    "id",
    "client_id",
    "client_secret",
    "callback_url",
    "date_created",
    "date_secret_updated",
    "active",
    "subscriptions",
)
_CLIENT_SELECT = ", ".join(CLIENT_COLUMNS)


def _row_dict(columns, row) -> Dict[str, Any]:
    return dict(zip(columns, row))


def _jsonb(value):
    """Wrap a Python object for a JSONB parameter, passing None through."""
    return Jsonb(value) if value is not None else None


async def _conn():
    return shepherd_db.pool.connection(settings.postgres_pool_timeout)


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


async def create_message(
    agent: str,
    status: str,
    code: int,
    name: str = "",
    ref: Optional[Union[str, uuid.UUID]] = None,
    params: Optional[Dict[str, Any]] = None,
    message_id: Optional[Union[str, uuid.UUID]] = None,
) -> Dict[str, Any]:
    """Insert a message row. Mirrors Message.create + the post_save coercion:
    the long status name maps to its letter and the code is coerced
    ('R'->202, 'D'->200) at write time. ``agent`` is the name the row is
    recorded under (see shepherd_utils.ars.aras)."""
    letter = validate_letter(to_letter(status))
    coerced = coerce_code(letter, code)
    pk = uuid.UUID(str(message_id)) if message_id else uuid.uuid4()
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            INSERT INTO ars_message (id, name, code, status, agent, ref, params)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING {_MESSAGE_SELECT.replace("m.", "")}
            """,
            (
                pk,
                name,
                coerced,
                letter,
                agent,
                uuid.UUID(str(ref)) if ref else None,
                _jsonb(params),
            ),
        )
        row = await cur.fetchone()
        await conn.commit()
    return _row_dict(MESSAGE_COLUMNS, row)


async def get_message_row(
    message_id: Union[str, uuid.UUID],
) -> Optional[Dict[str, Any]]:
    """One message row (payload excluded), with its subscribed client pks."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT {_MESSAGE_SELECT},
                   COALESCE(
                     (SELECT array_agg(s.client_id ORDER BY s.client_id)
                      FROM ars_subscription s WHERE s.message_id = m.id),
                     ARRAY[]::int[]) AS clients
            FROM ars_message m WHERE m.id = %s
            """,
            (uuid.UUID(str(message_id)),),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    record = _row_dict(MESSAGE_COLUMNS, row[: len(MESSAGE_COLUMNS)])
    record["clients"] = list(row[len(MESSAGE_COLUMNS)] or [])
    return record


async def get_children(
    parent_id: Union[str, uuid.UUID],
) -> List[Dict[str, Any]]:
    """All children of a parent, oldest first, each with its ``agent_name``
    and the ``inforesid`` that agent stands for."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT {_MESSAGE_SELECT}
            FROM ars_message m
            WHERE m.ref = %s
            ORDER BY m.ts
            """,
            (uuid.UUID(str(parent_id)),),
        )
        rows = await cur.fetchall()
    children = []
    for r in rows:
        child = _row_dict(MESSAGE_COLUMNS, r)
        child["agent_name"] = child["agent"]
        child["inforesid"] = aras.inforesid_for(child["agent"])
        children.append(child)
    return children


async def update_message(
    message_id: Union[str, uuid.UUID],
    skip_coercion: bool = False,
    **fields: Any,
) -> Optional[Dict[str, Any]]:
    """Update message fields, applying the post_save code coercion.

    Whenever ``status`` is written (and coercion isn't skipped -- the
    upstream ``_skip_post_save`` escape hatch), the code column is forced to
    202 for 'R' / 200 for 'D' regardless of what the caller passed, matching
    ``message_post_save``. ``updated_at`` is always bumped.
    """
    if not fields:
        return await get_message_row(message_id)
    values = dict(fields)
    if "status" in values:
        values["status"] = validate_letter(to_letter(values["status"]))
        if not skip_coercion:
            status = values["status"]
            if status == "R":
                values["code"] = 202
            elif status == "D":
                values["code"] = 200
    for key in ("result_stat", "merged_versions_list", "params"):
        if key in values:
            values[key] = _jsonb(values[key])
    if "merged_version" in values and values["merged_version"] is not None:
        values["merged_version"] = uuid.UUID(str(values["merged_version"]))
    sets = ", ".join(f"{k} = %s" for k in values)
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            UPDATE ars_message SET {sets}, updated_at = NOW()
            WHERE id = %s
            RETURNING {_MESSAGE_SELECT.replace("m.", "")}
            """,
            (*values.values(), uuid.UUID(str(message_id))),
        )
        row = await cur.fetchone()
        await conn.commit()
    return _row_dict(MESSAGE_COLUMNS, row) if row is not None else None


async def claim_terminal_transition(
    message_id: Union[str, uuid.UUID],
    status: str,
    code: int,
    **fields: Any,
) -> Optional[Dict[str, Any]]:
    """Move a message to a terminal status, but only if it is not there yet.

    ``check_parent_completion`` runs from the server and from four workers,
    and two children reaching a terminal status at the same moment make two
    of them evaluate the same complete decision. A plain read-then-write
    guard lets both through, which double-fires the completion notifications
    (and, on the empty branch, synthesizes two merged messages). This does
    the check and the write in one statement: the winner gets the updated
    row, every loser gets ``None`` and stops.

    Only the ``status <> new status`` predicate is conditional; the other
    fields are written exactly as ``update_message`` would.
    """
    letter = validate_letter(to_letter(status))
    values: Dict[str, Any] = {"status": letter, "code": coerce_code(letter, code)}
    values.update(fields)
    for key in ("result_stat", "merged_versions_list", "params"):
        if key in values:
            values[key] = _jsonb(values[key])
    if values.get("merged_version") is not None:
        values["merged_version"] = uuid.UUID(str(values["merged_version"]))
    sets = ", ".join(f"{k} = %s" for k in values)
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            UPDATE ars_message SET {sets}, updated_at = NOW()
            WHERE id = %s AND status <> %s
            RETURNING {_MESSAGE_SELECT.replace("m.", "")}
            """,
            (*values.values(), uuid.UUID(str(message_id)), letter),
        )
        row = await cur.fetchone()
        await conn.commit()
    return _row_dict(MESSAGE_COLUMNS, row) if row is not None else None


async def get_recent_message_pks(limit: int = 10) -> List[Dict[str, Any]]:
    """The most recently created messages as ``{"id", "ts"}``, newest first.

    Only what ``GET /ars/api/messages`` renders. It used to select every
    column plus a correlated subquery for each row's subscribers, and the
    endpoint then loaded and spliced in each message's stored payload --
    tens of MB per row for a listing that is only ever used to see what has
    come through recently.
    """
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            SELECT id, ts FROM ars_message ORDER BY ts DESC LIMIT %s
            """,
            (limit,),
        )
        rows = await cur.fetchall()
    return [{"id": r[0], "ts": r[1]} for r in rows]


async def get_status_rows(pks: List[str]) -> Dict[str, Dict[str, Any]]:
    """pk -> (status, merged_versions_list, params) map for get_status."""
    ids = []
    for pk in pks:
        try:
            ids.append(uuid.UUID(str(pk)))
        except (ValueError, AttributeError, TypeError):
            continue
    if not ids:
        return {}
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            SELECT id, status, merged_versions_list, params
            FROM ars_message WHERE id = ANY(%s)
            """,
            (ids,),
        )
        rows = await cur.fetchall()
    return {
        str(r[0]): {"status": r[1], "merged_versions_list": r[2], "params": r[3]}
        for r in rows
    }


async def retain_tree(parent_id: Union[str, uuid.UUID]) -> None:
    """Set retain=True on a parent and all its children (retain_all)."""
    pk = uuid.UUID(str(parent_id))
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        await conn.execute(
            "UPDATE ars_message SET retain = TRUE WHERE id = %s OR ref = %s",
            (pk, pk),
        )
        await conn.commit()


async def get_report_rows(inforesid: str) -> List[Dict[str, Any]]:
    """24-hour per-message report for an infores (iendswith match, as
    upstream), resolved against the static ARA roster: the rows of every
    agent whose infores ends with ``inforesid``."""
    agents = aras.agents_for_inforesid_suffix(inforesid)
    if not agents:
        return []
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            SELECT m.code, m.id, m.ts, m.updated_at, m.result_count
            FROM ars_message m
            WHERE m.ts > NOW() - INTERVAL '24 hours'
              AND m.agent = ANY(%s)
            """,
            (agents,),
        )
        rows = await cur.fetchall()
    return [
        {
            "code": r[0],
            "id": r[1],
            "ts": r[2],
            "updated_at": r[3],
            "result_count": r[4],
        }
        for r in rows
    ]


# A parent is a submitted query: the only kind of row with no ``ref``.
# (Upstream counted the rows of its default actor, which left the workflow
# actor's parents out of latest_pk; every submitted query counts here.)
_PARENT_WHERE = "ref IS NULL"


async def get_parent_message_counts(days: int) -> Dict[str, int]:
    """Per-day counts of parent messages for latest_pk."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT (ts AT TIME ZONE 'UTC')::date AS day, COUNT(*)
            FROM ars_message WHERE {_PARENT_WHERE}
              AND ts >= NOW() - make_interval(days => %s)
            GROUP BY day
            """,
            (days,),
        )
        rows = await cur.fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


async def get_latest_parent_pks(limit: int) -> List[str]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT id FROM ars_message WHERE {_PARENT_WHERE}
            ORDER BY ts DESC LIMIT %s
            """,
            (limit,),
        )
        rows = await cur.fetchall()
    return [str(r[0]) for r in rows]


async def get_running_parent_pks_24h() -> List[str]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT id FROM ars_message
            WHERE {_PARENT_WHERE} AND status = 'R'
              AND ts > NOW() - INTERVAL '24 hours'
            """,
        )
        rows = await cur.fetchall()
    return [str(r[0]) for r in rows]


async def get_running_messages(
    window_sec: float,
    min_age_sec: float = 0.0,
    limit: int = 2000,
) -> List[Dict[str, Any]]:
    """Running messages the watchdog should consider, oldest first.

    ``min_age_sec`` skips rows too young to have timed out under any
    threshold; ``window_sec`` caps how far back to look, and **0 means no
    cap**. Upstream's 15-minute ceiling on creation time meant a message that
    stayed Running past it could never be timed out again -- so a watchdog
    outage longer than the gap between the timeout threshold and the ceiling
    stranded every message it missed, and their parents with them.
    """
    clauses = ["m.status = 'R'"]
    params: List[Any] = []
    if min_age_sec > 0:
        clauses.append("m.ts < NOW() - make_interval(secs => %s)")
        params.append(float(min_age_sec))
    if window_sec > 0:
        clauses.append("m.ts > NOW() - make_interval(secs => %s)")
        params.append(float(window_sec))
    params.append(int(limit))
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT m.id, m.ts, m.params, m.agent, m.ref
            FROM ars_message m
            WHERE {" AND ".join(clauses)}
            ORDER BY m.ts
            LIMIT %s
            """,
            tuple(params),
        )
        rows = await cur.fetchall()
    return [
        {"id": r[0], "ts": r[1], "params": r[2], "agent_name": r[3], "ref": r[4]}
        for r in rows
    ]


async def purge_old_message_data(retention_days: int) -> int:
    """Null out payload copies for old, non-retained, terminal messages.

    Upstream has no purge job (the retain flag is honored by out-of-band
    cleanup); this is Shepherd's equivalent for the durable bytea copies.
    Row metadata is kept for reports/latest_pk. Returns rows purged.
    """
    if retention_days <= 0:
        return 0
    # Trees backing a live response-cache entry are the cache: purging their
    # payloads would silently empty it. A message's tree root is itself for
    # a parent and ``ref`` for a child / merge child; the root is exempt
    # while an entry of the CURRENT generation points at it. Superseded
    # generations are purged by the watchdog first (ars_cache_stale_grace_sec),
    # after which their sources age out here like any other tree.
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            UPDATE ars_message m SET data = NULL
            WHERE m.data IS NOT NULL
              AND m.retain = FALSE
              AND m.status IN ('D', 'S', 'E', 'U')
              AND m.updated_at < NOW() - make_interval(days => %s)
              AND NOT EXISTS (
                SELECT 1 FROM ars_response_cache c
                JOIN ars_cache_meta g ON g.generation = c.generation
                WHERE c.source_pk = COALESCE(m.ref, m.id)
              )
            """,
            (retention_days,),
        )
        purged = cur.rowcount or 0
        await conn.commit()
    return purged


# ---------------------------------------------------------------------------
# Clients / subscriptions
# ---------------------------------------------------------------------------


async def get_client(client_id: str) -> Optional[Dict[str, Any]]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"SELECT {_CLIENT_SELECT} FROM ars_client WHERE client_id = %s",
            (client_id,),
        )
        row = await cur.fetchone()
    return _row_dict(CLIENT_COLUMNS, row) if row is not None else None


async def get_client_by_pk(pk: int) -> Optional[Dict[str, Any]]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"SELECT {_CLIENT_SELECT} FROM ars_client WHERE id = %s", (pk,)
        )
        row = await cur.fetchone()
    return _row_dict(CLIENT_COLUMNS, row) if row is not None else None


async def add_subscription(message_id: Union[str, uuid.UUID], client_pk: int) -> None:
    """Subscribe a client to a message: M2M row + client.subscriptions JSON."""
    pk = uuid.UUID(str(message_id))
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        await conn.execute(
            """
            INSERT INTO ars_subscription (client_id, message_id)
            VALUES (%s, %s) ON CONFLICT DO NOTHING
            """,
            (client_pk, pk),
        )
        await conn.execute(
            """
            UPDATE ars_client
            SET subscriptions = CASE
                WHEN subscriptions IS NULL THEN %s::jsonb
                WHEN NOT subscriptions @> %s::jsonb
                    THEN subscriptions || %s::jsonb
                ELSE subscriptions END
            WHERE id = %s
            """,
            (
                Jsonb([str(pk)]),
                Jsonb([str(pk)]),
                Jsonb([str(pk)]),
                client_pk,
            ),
        )
        await conn.commit()


async def remove_subscription(
    message_id: Union[str, uuid.UUID], client_pk: int
) -> None:
    pk = uuid.UUID(str(message_id))
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        await conn.execute(
            "DELETE FROM ars_subscription WHERE client_id = %s AND message_id = %s",
            (client_pk, pk),
        )
        await conn.execute(
            """
            UPDATE ars_client
            SET subscriptions = COALESCE(subscriptions, '[]'::jsonb) - %s
            WHERE id = %s
            """,
            (str(pk), client_pk),
        )
        await conn.commit()


async def get_subscribed_clients(
    message_id: Union[str, uuid.UUID],
) -> List[Dict[str, Any]]:
    prefixed = ", ".join(f"c.{col}" for col in CLIENT_COLUMNS)
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT {prefixed} FROM ars_client c
            JOIN ars_subscription s ON s.client_id = c.id
            WHERE s.message_id = %s
            """,
            (uuid.UUID(str(message_id)),),
        )
        rows = await cur.fetchall()
    return [_row_dict(CLIENT_COLUMNS, r) for r in rows]


async def clear_subscriptions(message_id: Union[str, uuid.UUID]) -> None:
    """query_event_unsubscribe(None, pk): detach every client from a message,
    removing the pk from each client's subscriptions JSON too."""
    pk = uuid.UUID(str(message_id))
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        await conn.execute(
            """
            UPDATE ars_client
            SET subscriptions = COALESCE(subscriptions, '[]'::jsonb) - %s
            WHERE id IN (
                SELECT client_id FROM ars_subscription WHERE message_id = %s
            )
            """,
            (str(pk), pk),
        )
        await conn.execute("DELETE FROM ars_subscription WHERE message_id = %s", (pk,))
        await conn.commit()


# ---------------------------------------------------------------------------
# Payload blobs
#
# Hot path: Shepherd's Redis data store, keyed by str(message_pk) (a full
# UUID, so it can't collide with Shepherd's 8-char ids). Durable copy: the
# ars_message.data bytea column, written when a message goes terminal.
# Reads try Redis first and fall back to (and re-warm from) Postgres.
# ---------------------------------------------------------------------------


async def save_message_data(
    message_id: Union[str, uuid.UUID],
    payload: Any,
    logger: logging.Logger,
    raise_on_failure: bool = False,
) -> None:
    await shepherd_db.save_message(
        str(message_id), payload, logger, raise_on_failure=raise_on_failure
    )


# ---------------------------------------------------------------------------
# The merge-ready index: which validated ARA children of a parent still have
# to be folded into its merged message. ars_premerge adds a child here and
# then wakes ars.merge; the merge worker that holds the parent's lock drains
# the index in arrival order (a sorted set scored by arrival time), so a wake
# task is only a hint and a child is never merged twice or skipped because a
# second worker lost the lock. Lives in the data store next to the payloads,
# under the same TTL.
# ---------------------------------------------------------------------------

READY_CHILDREN_PREFIX = "ars:merge-ready:"


def _ready_children_key(parent_pk) -> str:
    return f"{READY_CHILDREN_PREFIX}{parent_pk}"


async def add_ready_child(parent_pk, child_pk, logger: logging.Logger) -> None:
    """Record a validated child as ready to fold into its parent's merge.

    The index is the merge worker's only source of work -- a child missing
    from it is never merged and its results silently vanish from the final
    answer, while the completion arithmetic keeps waiting for a merge child
    that never comes. So this retries through transient Redis pressure
    (ZADD is idempotent) and RAISES if it still cannot land, so the caller
    can fail the child instead of stranding the parent.
    """
    key = _ready_children_key(parent_pk)
    last_error: Optional[Exception] = None
    for attempt in range(3):
        if attempt:
            await asyncio.sleep(0.1 * (2**attempt))
        try:
            async with shepherd_db.data_db_client.pipeline(transaction=True) as pipe:
                pipe.zadd(key, {str(child_pk): time.time()}, nx=True)
                pipe.expire(key, settings.redis_ttl)
                await pipe.execute()
            return
        except Exception as e:
            last_error = e
            logger.error(
                f"Failed to record merge-ready child {child_pk} of {parent_pk} "
                f"(attempt {attempt}): {e}"
            )
    raise RuntimeError(
        f"Could not record merge-ready child {child_pk} of {parent_pk}"
    ) from last_error


async def get_ready_children(parent_pk, logger: logging.Logger) -> List[str]:
    """Child pks waiting to be folded into ``parent_pk``, oldest first."""
    try:
        members = await shepherd_db.data_db_client.zrange(
            _ready_children_key(parent_pk), 0, -1
        )
    except Exception as e:
        logger.error(f"Failed to read merge-ready children of {parent_pk}: {e}")
        return []
    return [m.decode() if isinstance(m, bytes) else m for m in members]


async def clear_ready_child(parent_pk, child_pk, logger: logging.Logger) -> None:
    """Drop a folded (or abandoned) child from its parent's merge-ready index."""
    try:
        await shepherd_db.data_db_client.zrem(
            _ready_children_key(parent_pk), str(child_pk)
        )
    except Exception as e:
        logger.error(
            f"Failed to clear merge-ready child {child_pk} of {parent_pk}: {e}"
        )


# The query's root OTel trace context, stored at submit so callback-side
# stages (premerge/merge/notify) can rejoin the submit trace even when
# an ARA doesn't propagate traceparent into its async callback POST.
# Telemetry only: failures are logged at debug and never break the pipeline.
OTEL_CARRIER_TTL_SECONDS = 7 * 24 * 3600


async def save_otel_carrier(
    message_id: Union[str, uuid.UUID],
    carrier: Dict[str, str],
    logger: logging.Logger,
) -> None:
    try:
        await shepherd_db.data_db_client.set(
            f"ars:otel:{message_id}",
            json.dumps(carrier),
            ex=OTEL_CARRIER_TTL_SECONDS,
        )
    except Exception as e:
        logger.debug(f"Failed to save otel carrier for {message_id}: {e}")


async def load_otel_carrier(
    message_id: Union[str, uuid.UUID],
    logger: logging.Logger,
) -> str:
    """The stored carrier as a JSON string for a task's "otel" field; "{}"
    when absent."""
    try:
        raw = await shepherd_db.data_db_client.get(f"ars:otel:{message_id}")
        if raw:
            return raw.decode() if isinstance(raw, (bytes, bytearray)) else raw
    except Exception as e:
        logger.debug(f"Failed to load otel carrier for {message_id}: {e}")
    return "{}"


ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
GZIP_MAGIC = b"\x1f\x8b"


def _decompress_payload_bytes(blob: bytes) -> bytes:
    """Message.decompress_dict codec, bytes out: zstd magic, gzip fallback,
    else the blob is taken as already-plain JSON."""
    if blob[:4] == ZSTD_MAGIC:
        return shepherd_db.decompress_zstd(blob)
    if blob[:2] == GZIP_MAGIC:
        return gzip.decompress(blob)
    return blob


def _decompress_payload(blob: bytes) -> Any:
    """Message.decompress_dict codec: parsed payload, {} on error."""
    try:
        return orjson.loads(_decompress_payload_bytes(blob))
    except Exception:
        return {}


async def persist_data_copy(
    message_id: Union[str, uuid.UUID],
    logger: logging.Logger,
) -> None:
    """Copy the Redis blob into ars_message.data for durability.

    Retries through transient Redis/Postgres pressure (both operations are
    idempotent); after that it stays best-effort -- the blob is still live
    in Redis and any later terminal update re-attempts the copy."""
    last_error: Optional[Exception] = None
    for attempt in range(3):
        if attempt:
            await asyncio.sleep(0.1 * (2**attempt))
        try:
            blob = await shepherd_db.data_db_client.get(str(message_id))
        except Exception as e:
            last_error = e
            logger.warning(
                f"Failed to read blob for durable copy {message_id} "
                f"(attempt {attempt}): {e}"
            )
            continue
        if blob is None:
            return
        try:
            async with shepherd_db.pool.connection(
                settings.postgres_pool_timeout
            ) as conn:
                await conn.execute(
                    "UPDATE ars_message SET data = %s WHERE id = %s",
                    (blob, uuid.UUID(str(message_id))),
                )
                await conn.commit()
            return
        except Exception as e:
            last_error = e
            logger.warning(
                f"Failed to persist durable copy for {message_id} "
                f"(attempt {attempt}): {e}"
            )
    logger.error(
        f"Failed to persist durable copy for {message_id} after retries: "
        f"{last_error}"
    )


async def load_message_data(
    message_id: Union[str, uuid.UUID],
    logger: logging.Logger,
) -> Optional[Any]:
    """Payload dict for a message, or None when no blob exists anywhere.

    Decompression + JSON parsing of a multi-MB payload is pure CPU and runs
    in a worker thread, so a server request handler (or a worker's task)
    reading a large merged message does not stall its event loop."""
    blob = None
    try:
        blob = await shepherd_db.data_db_client.get(str(message_id))
    except Exception as e:
        logger.warning(f"Redis read failed for {message_id}: {e}")
    if blob is not None:
        return await asyncio.to_thread(shepherd_db.decode_message, blob)
    try:
        async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
            cur = await conn.execute(
                "SELECT data FROM ars_message WHERE id = %s",
                (uuid.UUID(str(message_id)),),
            )
            row = await cur.fetchone()
    except Exception as e:
        logger.error(f"Postgres blob read failed for {message_id}: {e}")
        return None
    if row is None or row[0] is None:
        return None
    payload = await asyncio.to_thread(_decompress_payload, bytes(row[0]))
    # Re-warm Redis so subsequent reads are cheap again.
    try:
        await shepherd_db.save_message(str(message_id), payload, logger)
    except Exception:
        pass
    return payload


async def load_message_bytes(
    message_id: Union[str, uuid.UUID],
    logger: logging.Logger,
) -> Optional[bytes]:
    """The payload as decompressed JSON bytes, never parsed.

    This is the read path for serving a stored message: decompression
    releases the GIL (so the worker thread genuinely frees the event loop),
    and no Python object graph is built -- a parsed payload costs ~4x its
    JSON size in memory and a GIL-holding parse per read. None when no blob
    exists anywhere. A Postgres fallback re-warms Redis with the stored
    compressed bytes as-is."""
    blob = None
    try:
        blob = await shepherd_db.data_db_client.get(str(message_id))
    except Exception as e:
        logger.warning(f"Redis read failed for {message_id}: {e}")
    if blob is not None:
        return await asyncio.to_thread(shepherd_db.decompress_zstd, blob)
    try:
        async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
            cur = await conn.execute(
                "SELECT data FROM ars_message WHERE id = %s",
                (uuid.UUID(str(message_id)),),
            )
            row = await cur.fetchone()
    except Exception as e:
        logger.error(f"Postgres blob read failed for {message_id}: {e}")
        return None
    if row is None or row[0] is None:
        return None
    stored = bytes(row[0])
    try:
        raw = await asyncio.to_thread(_decompress_payload_bytes, stored)
    except Exception as e:
        logger.error(f"Undecodable durable payload for {message_id}: {e}")
        return None
    if stored[:4] == ZSTD_MAGIC:
        try:  # re-warm with the same zstd frame Redis normally holds
            await shepherd_db.data_db_client.set(
                str(message_id), stored, ex=settings.redis_ttl
            )
        except Exception:
            pass
    return raw


async def load_message_compressed(
    message_id: Union[str, uuid.UUID],
    logger: logging.Logger,
) -> Optional[bytes]:
    """The payload as the compressed frame clients get from ``?compress``.

    Redis first, then the durable ``ars_message.data`` copy -- which outlives
    the Redis TTL by ``ars_data_retention_days``. Serving only from Redis (as
    this path used to) made ``?compress`` 404 on messages that were still
    perfectly readable through every other endpoint. A durable copy stored in
    some other codec is re-compressed as zstd so the response always matches
    the advertised ``X-Content-Compression``.
    """
    try:
        blob = await shepherd_db.data_db_client.get(str(message_id))
        if blob is not None:
            return blob
    except Exception as e:
        logger.warning(f"Redis read failed for {message_id}: {e}")
    try:
        async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
            cur = await conn.execute(
                "SELECT data FROM ars_message WHERE id = %s",
                (uuid.UUID(str(message_id)),),
            )
            row = await cur.fetchone()
    except Exception as e:
        logger.error(f"Postgres blob read failed for {message_id}: {e}")
        return None
    if row is None or row[0] is None:
        return None
    stored = bytes(row[0])
    if stored[:4] == ZSTD_MAGIC:
        try:  # re-warm with the same frame Redis normally holds
            await shepherd_db.data_db_client.set(
                str(message_id), stored, ex=settings.redis_ttl
            )
        except Exception:
            pass
        return stored
    try:
        raw = await asyncio.to_thread(_decompress_payload_bytes, stored)
        return await asyncio.to_thread(zstandard.compress, raw)
    except Exception as e:
        logger.error(f"Undecodable durable payload for {message_id}: {e}")
        return None


async def message_has_data(message_id: Union[str, uuid.UUID]) -> bool:
    if await shepherd_db.message_exists(str(message_id)):
        return True
    try:
        async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
            cur = await conn.execute(
                "SELECT data IS NOT NULL FROM ars_message WHERE id = %s",
                (uuid.UUID(str(message_id)),),
            )
            row = await cur.fetchone()
        return bool(row and row[0])
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Response cache index (docs/ARS_RESPONSE_CACHE_PLAN.md)
#
# The cache stores no payloads: ``ars_response_cache`` maps a canonical
# query-graph hash to the parent pk of the one tree that answers it (its
# blobs already live in ``ars_message.data``); identical submits are handed
# that pk. Orchestration lives in ``shepherd_utils.ars.cache``; this section
# is the SQL.
# ---------------------------------------------------------------------------

CACHE_ENTRY_COLUMNS = (
    "generation",
    "cache_key",
    "state",
    "source_pk",
    "label_map",
    "created_at",
    "ready_at",
    "hit_count",
    "last_hit_at",
)
_CACHE_ENTRY_SELECT = ", ".join(f"c.{col}" for col in CACHE_ENTRY_COLUMNS)


async def get_cache_generation() -> int:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute("SELECT generation FROM ars_cache_meta WHERE id")
        row = await cur.fetchone()
    return int(row[0]) if row else 1


async def bump_cache_generation(reason: Optional[str]) -> int:
    """Invalidate the whole cache; returns the new generation."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            UPDATE ars_cache_meta
            SET generation = generation + 1, bumped_at = NOW(), bumped_reason = %s
            WHERE id
            RETURNING generation
            """,
            (reason,),
        )
        row = await cur.fetchone()
        await conn.commit()
    return int(row[0])


async def get_current_cache_entry(
    cache_key: str,
) -> Tuple[int, Optional[Dict[str, Any]]]:
    """The current generation and its entry for a key, in one round trip."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT g.generation, {_CACHE_ENTRY_SELECT}
            FROM ars_cache_meta g
            LEFT JOIN ars_response_cache c
              ON c.generation = g.generation AND c.cache_key = %s
            WHERE g.id
            """,
            (cache_key,),
        )
        row = await cur.fetchone()
    if row is None:
        return 1, None
    generation = int(row[0])
    entry = _row_dict(CACHE_ENTRY_COLUMNS, row[1:]) if row[1] is not None else None
    return generation, entry


async def get_cache_entry(generation: int, cache_key: str) -> Optional[Dict[str, Any]]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"SELECT {_CACHE_ENTRY_SELECT} FROM ars_response_cache c "
            "WHERE c.generation = %s AND c.cache_key = %s",
            (generation, cache_key),
        )
        row = await cur.fetchone()
    return _row_dict(CACHE_ENTRY_COLUMNS, row) if row else None


async def claim_or_get_cache_entry(
    cache_key: str, source_pk: Union[str, uuid.UUID]
) -> Tuple[int, Optional[Dict[str, Any]], bool]:
    """Atomically claim leadership of a key in the current generation, or
    return the existing entry.

    ``(generation, entry, True)`` when this parent inserted the pending row
    and is the leader; ``(generation, entry, False)`` when another entry
    (pending or ready) already holds the key; ``(generation, None, False)``
    only if the row vanished between the conflict and the re-read (caller
    treats it as a plain dispatch). The generation is read inside the same
    statement, so a miss costs one round trip and a conflict two.
    """
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            INSERT INTO ars_response_cache (generation, cache_key, state, source_pk)
            SELECT g.generation, %s, 'pending', %s FROM ars_cache_meta g WHERE g.id
            ON CONFLICT (generation, cache_key) DO NOTHING
            RETURNING {_CACHE_ENTRY_SELECT.replace("c.", "")}
            """,
            (cache_key, uuid.UUID(str(source_pk))),
        )
        row = await cur.fetchone()
        await conn.commit()
        if row is not None:
            entry = _row_dict(CACHE_ENTRY_COLUMNS, row)
            return int(entry["generation"]), entry, True
    generation, entry = await get_current_cache_entry(cache_key)
    return generation, entry, False


async def mark_cache_entry_ready(
    source_pk: Union[str, uuid.UUID], label_map: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Flip the pending entry led by ``source_pk`` to ready. None when the
    entry no longer points at this leader (failed over / invalidated)."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            UPDATE ars_response_cache c
            SET state = 'ready', ready_at = NOW(), label_map = %s
            WHERE c.source_pk = %s AND c.state = 'pending'
            RETURNING {_CACHE_ENTRY_SELECT}
            """,
            (_jsonb(label_map), uuid.UUID(str(source_pk))),
        )
        row = await cur.fetchone()
        await conn.commit()
    return _row_dict(CACHE_ENTRY_COLUMNS, row) if row else None


async def upsert_cache_entry_ready(
    generation: int,
    cache_key: str,
    source_pk: Union[str, uuid.UUID],
    label_map: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """overwrite_cache: point the key at this tree, replacing any entry."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            INSERT INTO ars_response_cache
              (generation, cache_key, state, source_pk, label_map, ready_at)
            VALUES (%s, %s, 'ready', %s, %s, NOW())
            ON CONFLICT (generation, cache_key) DO UPDATE
              SET state = 'ready', source_pk = EXCLUDED.source_pk,
                  label_map = EXCLUDED.label_map, ready_at = NOW()
            RETURNING {_CACHE_ENTRY_SELECT.replace("c.", "")}
            """,
            (generation, cache_key, uuid.UUID(str(source_pk)), _jsonb(label_map)),
        )
        row = await cur.fetchone()
        await conn.commit()
    return _row_dict(CACHE_ENTRY_COLUMNS, row)


async def delete_cache_entry(generation: int, cache_key: str) -> bool:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            "DELETE FROM ars_response_cache WHERE generation = %s AND cache_key = %s",
            (generation, cache_key),
        )
        deleted = (cur.rowcount or 0) > 0
        await conn.commit()
    return deleted


async def delete_pending_cache_entry(source_pk: Union[str, uuid.UUID]) -> bool:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            "DELETE FROM ars_response_cache WHERE source_pk = %s AND state = 'pending'",
            (uuid.UUID(str(source_pk)),),
        )
        deleted = (cur.rowcount or 0) > 0
        await conn.commit()
    return deleted


async def repoint_pending_cache_entry(
    old_source_pk: Union[str, uuid.UUID], new_source_pk: Union[str, uuid.UUID]
) -> bool:
    """Leader fail-over: hand the pending entry to a new leader."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            "UPDATE ars_response_cache SET source_pk = %s, created_at = NOW() "
            "WHERE source_pk = %s AND state = 'pending'",
            (uuid.UUID(str(new_source_pk)), uuid.UUID(str(old_source_pk))),
        )
        updated = (cur.rowcount or 0) > 0
        await conn.commit()
    return updated


async def delete_message(message_id: Union[str, uuid.UUID]) -> bool:
    """Remove a message row that has nothing under it (a parent that lost
    the cache leadership race before anything was dispatched)."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            "DELETE FROM ars_message WHERE id = %s", (uuid.UUID(str(message_id)),)
        )
        deleted = (cur.rowcount or 0) > 0
        await conn.commit()
    return deleted


async def record_cache_hit(generation: int, cache_key: str) -> None:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        await conn.execute(
            "UPDATE ars_response_cache SET hit_count = hit_count + 1, "
            "last_hit_at = NOW() WHERE generation = %s AND cache_key = %s",
            (generation, cache_key),
        )
        await conn.commit()


async def get_stale_pending_cache_entries(max_age_sec: float) -> List[Dict[str, Any]]:
    """Pending entries older than the threshold, with their leader's status
    (None when the leader row is gone)."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT {_CACHE_ENTRY_SELECT}, m.status
            FROM ars_response_cache c
            LEFT JOIN ars_message m ON m.id = c.source_pk
            WHERE c.state = 'pending'
              AND c.created_at < NOW() - make_interval(secs => %s)
            ORDER BY c.created_at
            """,
            (float(max_age_sec),),
        )
        rows = await cur.fetchall()
    n = len(CACHE_ENTRY_COLUMNS)
    entries = []
    for r in rows:
        entry = _row_dict(CACHE_ENTRY_COLUMNS, r[:n])
        entry["leader_status"] = r[n]
        entries.append(entry)
    return entries


async def purge_stale_cache_entries(
    current_generation: int, grace_sec: float, batch: int = 1000
) -> int:
    """Delete index rows of superseded generations once past the grace
    window, a batch at a time. Returns rows deleted."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            DELETE FROM ars_response_cache
            WHERE (generation, cache_key) IN (
              SELECT generation, cache_key FROM ars_response_cache
              WHERE generation < %s
                AND created_at < NOW() - make_interval(secs => %s)
              LIMIT %s
            )
            """,
            (current_generation, float(grace_sec), batch),
        )
        deleted = cur.rowcount or 0
        await conn.commit()
    return deleted


async def purge_expired_ready_cache_entries(
    max_age_sec: float, batch: int = 1000
) -> int:
    """Delete ready entries older than ``max_age_sec``, a batch at a time.

    Without this nothing ever retires a current-generation entry short of an
    explicit invalidation, and because ``purge_old_message_data`` exempts any
    tree a live entry points at, every distinct query ever cached pinned its
    payloads in ``ars_message.data`` forever. Returns rows deleted.
    """
    if max_age_sec <= 0:
        return 0
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            DELETE FROM ars_response_cache
            WHERE (generation, cache_key) IN (
              SELECT generation, cache_key FROM ars_response_cache
              WHERE state = 'ready'
                AND COALESCE(ready_at, created_at)
                    < NOW() - make_interval(secs => %s)
              LIMIT %s
            )
            """,
            (float(max_age_sec), batch),
        )
        deleted = cur.rowcount or 0
        await conn.commit()
    return deleted


async def cache_stats() -> Dict[str, Any]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            "SELECT generation, bumped_at, bumped_reason FROM ars_cache_meta WHERE id"
        )
        meta = await cur.fetchone()
        cur = await conn.execute("""
            SELECT c.generation, c.state, count(*), COALESCE(sum(c.hit_count), 0)
            FROM ars_response_cache c
            GROUP BY c.generation, c.state
            ORDER BY c.generation, c.state
            """)
        rows = await cur.fetchall()
    generation = int(meta[0]) if meta else 1
    current = {"pending": 0, "ready": 0, "hits": 0}
    superseded = {"entries": 0, "hits": 0}
    for gen, state, count, hits in rows:
        if int(gen) == generation:
            current[state] = int(count)
            current["hits"] += int(hits)
        else:
            superseded["entries"] += int(count)
            superseded["hits"] += int(hits)
    return {
        "generation": generation,
        "bumped_at": meta[1] if meta else None,
        "bumped_reason": meta[2] if meta else None,
        "current": current,
        "superseded": superseded,
    }

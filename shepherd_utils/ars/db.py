"""Postgres persistence for the ARS message tree and registry.

Ported from NCATSTranslator/Relay @ dd1e71b (tr_sys/tr_ars/models.py,
api.py get_or_create_agent/get_or_create_actor, apps.py seeding). Uses the
shared Shepherd Postgres pool; payload blobs ride Shepherd's Redis data store
(keyed by ``str(message_pk)``) with a durable zstd copy in
``ars_message.data`` once a message goes terminal, so the UI can fetch
merged results long after the Redis TTL.
"""

import gzip
import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

import zstandard
from psycopg.types.json import Jsonb

import shepherd_utils.db as shepherd_db
from shepherd_utils.config import settings

from .statuses import coerce_code, to_letter

MESSAGE_COLUMNS = (
    "id",
    "name",
    "code",
    "status",
    "actor",
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

AGENT_COLUMNS = (
    "id",
    "name",
    "description",
    "uri",
    "contact",
    "registered",
    "updated",
)
_AGENT_SELECT = ", ".join(AGENT_COLUMNS)

ACTOR_COLUMNS = ("id", "agent", "channel", "path", "inforesid", "active")
_ACTOR_SELECT = ", ".join(f"a.{c}" for c in ACTOR_COLUMNS)

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
# Channels / Agents / Actors (the registry)
# ---------------------------------------------------------------------------


async def get_or_create_channel(
    name: str,
    description: Optional[str] = None,
) -> Tuple[Dict[str, Any], bool]:
    """get_or_create by unique name. Returns (row, created)."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            "SELECT id, name, description FROM ars_channel WHERE name = %s",
            (name,),
        )
        row = await cur.fetchone()
        if row is not None:
            return _row_dict(("id", "name", "description"), row), False
        cur = await conn.execute(
            """
            INSERT INTO ars_channel (name, description) VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id, name, description
            """,
            (name, description),
        )
        row = await cur.fetchone()
        await conn.commit()
        return _row_dict(("id", "name", "description"), row), True


async def get_or_create_agent(data: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
    """Upstream api.get_or_create_agent: (agent envelope-ready row, status).

    201 on creation, 302 when it already existed (updating the uri in place
    when it changed, exactly like upstream).
    """
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"SELECT {_AGENT_SELECT} FROM ars_agent WHERE name = %s",
            (data["name"],),
        )
        row = await cur.fetchone()
        if row is not None:
            agent = _row_dict(AGENT_COLUMNS, row)
            if data.get("uri") is not None and data["uri"] != agent["uri"]:
                await conn.execute(
                    "UPDATE ars_agent SET uri = %s, updated = NOW() WHERE id = %s",
                    (data["uri"], agent["id"]),
                )
                await conn.commit()
                agent["uri"] = data["uri"]
            return agent, 302
        cur = await conn.execute(
            f"""
            INSERT INTO ars_agent (name, uri, description, contact)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING {_AGENT_SELECT}
            """,
            (
                data["name"],
                data.get("uri", ""),
                data.get("description"),
                data.get("contact"),
            ),
        )
        row = await cur.fetchone()
        await conn.commit()
        return _row_dict(AGENT_COLUMNS, row), 201


async def get_agent_by_name(name: str) -> Optional[Dict[str, Any]]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"SELECT {_AGENT_SELECT} FROM ars_agent WHERE name = %s", (name,)
        )
        row = await cur.fetchone()
    return _row_dict(AGENT_COLUMNS, row) if row is not None else None


async def list_agents() -> List[Dict[str, Any]]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"SELECT {_AGENT_SELECT} FROM ars_agent ORDER BY name"
        )
        rows = await cur.fetchall()
    return [_row_dict(AGENT_COLUMNS, r) for r in rows]


async def list_channels() -> List[Dict[str, Any]]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            "SELECT id, name, description FROM ars_channel ORDER BY name"
        )
        rows = await cur.fetchall()
    return [_row_dict(("id", "name", "description"), r) for r in rows]


def serialize_channels(channel_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The Django-serialized channel list stored on Actor.channel.

    Upstream stores json.loads(serializers.serialize('json', channels)) --
    a list of {"model": "tr_ars.channel", "pk": <int>, "fields": {...}}.
    """
    return [
        {
            "model": "tr_ars.channel",
            "pk": row["id"],
            "fields": {
                "name": row["name"],
                "description": row.get("description"),
            },
        }
        for row in channel_rows
    ]


async def get_or_create_actor(
    data: Dict[str, Any],
    inactive_list: Optional[List[str]] = None,
) -> Tuple[Dict[str, Any], int]:
    """Upstream api.get_or_create_actor semantics.

    ``data`` = {"channel": [names or pks], "agent": name|pk|{"name","uri"},
    "path": str, "inforesid": str}. Existing actors get their inforesid
    updated when changed, are deactivated when the inforesid is on the
    inactive list, and get their serialized channel list refreshed. Returns
    (actor row, 302|201).
    """
    inactive = inactive_list if inactive_list is not None else []
    # resolve channels -> serialized list
    channel_rows = []
    for item in data.get("channel", []):
        if isinstance(item, int) or (isinstance(item, str) and item.isnumeric()):
            async with shepherd_db.pool.connection(
                settings.postgres_pool_timeout
            ) as conn:
                cur = await conn.execute(
                    "SELECT id, name, description FROM ars_channel WHERE id = %s",
                    (int(item),),
                )
                row = await cur.fetchone()
            if row is None:
                raise KeyError(f"Unknown channel: {item}")
            channel_rows.append(_row_dict(("id", "name", "description"), row))
        else:
            row, _ = await get_or_create_channel(item)
            channel_rows.append(row)
    serialized_channel = serialize_channels(channel_rows)

    # resolve agent
    agent = data["agent"]
    if isinstance(agent, dict):
        agent_row, _ = await get_or_create_agent(agent)
    elif isinstance(agent, int) or (isinstance(agent, str) and agent.isnumeric()):
        async with shepherd_db.pool.connection(
            settings.postgres_pool_timeout
        ) as conn:
            cur = await conn.execute(
                f"SELECT {_AGENT_SELECT} FROM ars_agent WHERE id = %s",
                (int(agent),),
            )
            row = await cur.fetchone()
        if row is None:
            raise KeyError(f"Unknown agent: {agent}")
        agent_row = _row_dict(AGENT_COLUMNS, row)
    else:
        agent_row = await get_agent_by_name(agent)
        if agent_row is None:
            raise KeyError(f"Unknown agent: {agent}")

    inforesid = data.get("inforesid", "")
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT {_ACTOR_SELECT} FROM ars_actor a
            WHERE a.agent = %s AND a.path = %s
            """,
            (agent_row["id"], data["path"]),
        )
        row = await cur.fetchone()
        if row is not None:
            actor = _row_dict(ACTOR_COLUMNS, row)
            updates = {}
            if inforesid in inactive:
                updates["active"] = False
            if actor["inforesid"] is None or actor["inforesid"] != inforesid:
                updates["inforesid"] = inforesid
            if actor["channel"] != serialized_channel:
                updates["channel"] = _jsonb(serialized_channel)
            if updates:
                sets = ", ".join(f"{k} = %s" for k in updates)
                await conn.execute(
                    f"UPDATE ars_actor SET {sets} WHERE id = %s",
                    (*updates.values(), actor["id"]),
                )
                await conn.commit()
                actor.update(
                    {
                        k: (serialized_channel if k == "channel" else v)
                        for k, v in updates.items()
                    }
                )
            actor["agent_name"] = agent_row["name"]
            actor["agent_uri"] = agent_row["uri"]
            return actor, 302
        cur = await conn.execute(
            f"""
            INSERT INTO ars_actor (agent, channel, path, inforesid, active)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (agent, path) DO UPDATE SET inforesid = EXCLUDED.inforesid
            RETURNING {", ".join(ACTOR_COLUMNS)}
            """,
            (
                agent_row["id"],
                _jsonb(serialized_channel),
                data["path"],
                inforesid,
                inforesid not in inactive,
            ),
        )
        row = await cur.fetchone()
        await conn.commit()
        actor = _row_dict(ACTOR_COLUMNS, row)
        actor["agent_name"] = agent_row["name"]
        actor["agent_uri"] = agent_row["uri"]
        return actor, 201


async def list_actors(exclude_empty_path: bool = False) -> List[Dict[str, Any]]:
    """Actors joined with their agent (name + uri), for fan-out/rendering."""
    where = "WHERE a.path <> ''" if exclude_empty_path else ""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT {_ACTOR_SELECT}, g.name, g.uri FROM ars_actor a
            JOIN ars_agent g ON g.id = a.agent
            {where}
            ORDER BY a.id
            """
        )
        rows = await cur.fetchall()
    actors = []
    for r in rows:
        actor = _row_dict(ACTOR_COLUMNS, r[: len(ACTOR_COLUMNS)])
        actor["agent_name"] = r[len(ACTOR_COLUMNS)]
        actor["agent_uri"] = r[len(ACTOR_COLUMNS) + 1]
        actors.append(actor)
    return actors


async def get_actor(actor_id: int) -> Optional[Dict[str, Any]]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT {_ACTOR_SELECT}, g.name, g.uri FROM ars_actor a
            JOIN ars_agent g ON g.id = a.agent
            WHERE a.id = %s
            """,
            (actor_id,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    actor = _row_dict(ACTOR_COLUMNS, row[: len(ACTOR_COLUMNS)])
    actor["agent_name"] = row[len(ACTOR_COLUMNS)]
    actor["agent_uri"] = row[len(ACTOR_COLUMNS) + 1]
    return actor


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


async def create_message(
    actor_id: int,
    status: str,
    code: int,
    name: str = "",
    ref: Optional[Union[str, uuid.UUID]] = None,
    params: Optional[Dict[str, Any]] = None,
    message_id: Optional[Union[str, uuid.UUID]] = None,
) -> Dict[str, Any]:
    """Insert a message row. Mirrors Message.create + the post_save coercion:
    the long status name maps to its letter and the code is coerced
    ('R'->202, 'D'->200) at write time."""
    letter = to_letter(status)
    coerced = coerce_code(letter, code)
    pk = uuid.UUID(str(message_id)) if message_id else uuid.uuid4()
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            INSERT INTO ars_message (id, name, code, status, actor, ref, params)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING {_MESSAGE_SELECT.replace("m.", "")}
            """,
            (
                pk,
                name,
                coerced,
                letter,
                actor_id,
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
    """All children of a parent, joined with actor + agent details."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT {_MESSAGE_SELECT}, a.inforesid, a.channel, a.path,
                   g.name, a.id
            FROM ars_message m
            JOIN ars_actor a ON a.id = m.actor
            JOIN ars_agent g ON g.id = a.agent
            WHERE m.ref = %s
            ORDER BY m.ts
            """,
            (uuid.UUID(str(parent_id)),),
        )
        rows = await cur.fetchall()
    children = []
    n = len(MESSAGE_COLUMNS)
    for r in rows:
        child = _row_dict(MESSAGE_COLUMNS, r[:n])
        child["inforesid"] = r[n]
        child["actor_channel"] = r[n + 1]
        child["actor_path"] = r[n + 2]
        child["agent_name"] = r[n + 3]
        child["actor_id"] = r[n + 4]
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
        values["status"] = to_letter(values["status"])
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


async def get_recent_messages(limit: int = 10) -> List[Dict[str, Any]]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            f"""
            SELECT {_MESSAGE_SELECT},
                   COALESCE(
                     (SELECT array_agg(s.client_id ORDER BY s.client_id)
                      FROM ars_subscription s WHERE s.message_id = m.id),
                     ARRAY[]::int[]) AS clients
            FROM ars_message m ORDER BY m.ts DESC LIMIT %s
            """,
            (limit,),
        )
        rows = await cur.fetchall()
    out = []
    for row in rows:
        record = _row_dict(MESSAGE_COLUMNS, row[: len(MESSAGE_COLUMNS)])
        record["clients"] = list(row[len(MESSAGE_COLUMNS)] or [])
        out.append(record)
    return out


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
    """24-hour per-message report for an infores (iendswith match)."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            SELECT m.code, m.id, m.ts, m.updated_at, m.result_count
            FROM ars_message m
            JOIN ars_actor a ON a.id = m.actor
            WHERE m.ts > NOW() - INTERVAL '24 hours'
              AND LOWER(a.inforesid) LIKE LOWER(%s)
            """,
            (f"%{inforesid}",),
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


async def get_parent_message_counts(actor_id: int, days: int) -> Dict[str, int]:
    """Per-day counts of parent messages for latest_pk."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            SELECT (ts AT TIME ZONE 'UTC')::date AS day, COUNT(*)
            FROM ars_message WHERE actor = %s
              AND ts >= NOW() - make_interval(days => %s)
            GROUP BY day
            """,
            (actor_id, days),
        )
        rows = await cur.fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


async def get_latest_parent_pks(actor_id: int, limit: int) -> List[str]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            SELECT id FROM ars_message WHERE actor = %s
            ORDER BY ts DESC LIMIT %s
            """,
            (actor_id, limit),
        )
        rows = await cur.fetchall()
    return [str(r[0]) for r in rows]


async def get_running_parent_pks_24h(actor_id: int) -> List[str]:
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            SELECT id FROM ars_message
            WHERE actor = %s AND status = 'R'
              AND ts > NOW() - INTERVAL '24 hours'
            """,
            (actor_id,),
        )
        rows = await cur.fetchall()
    return [str(r[0]) for r in rows]


async def get_running_messages(window_sec: float) -> List[Dict[str, Any]]:
    """Running messages created within the scan window, for the watchdog."""
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            SELECT m.id, m.ts, m.params, g.name, m.ref
            FROM ars_message m
            JOIN ars_actor a ON a.id = m.actor
            JOIN ars_agent g ON g.id = a.agent
            WHERE m.status = 'R'
              AND m.ts > NOW() - make_interval(secs => %s)
            """,
            (float(window_sec),),
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
    async with shepherd_db.pool.connection(settings.postgres_pool_timeout) as conn:
        cur = await conn.execute(
            """
            UPDATE ars_message SET data = NULL
            WHERE data IS NOT NULL
              AND retain = FALSE
              AND status IN ('D', 'S', 'E', 'U')
              AND updated_at < NOW() - make_interval(days => %s)
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
        await conn.execute(
            "DELETE FROM ars_subscription WHERE message_id = %s", (pk,)
        )
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
) -> None:
    await shepherd_db.save_message(str(message_id), payload, logger)


def _decompress_payload(blob: bytes) -> Any:
    """Message.decompress_dict codec: zstd magic, gzip fallback, {} on error."""
    try:
        if blob[:4] == b"\x28\xb5\x2f\xfd":
            raw = zstandard.ZstdDecompressor().decompress(blob)
        elif blob[:2] == b"\x1f\x8b":
            raw = gzip.decompress(blob)
        else:
            raw = blob
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}


async def persist_data_copy(
    message_id: Union[str, uuid.UUID],
    logger: logging.Logger,
) -> None:
    """Copy the Redis blob into ars_message.data for durability."""
    try:
        blob = await shepherd_db.data_db_client.get(str(message_id))
    except Exception as e:
        logger.error(f"Failed to read blob for durable copy {message_id}: {e}")
        return
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
    except Exception as e:
        logger.error(f"Failed to persist durable copy for {message_id}: {e}")


async def load_message_data(
    message_id: Union[str, uuid.UUID],
    logger: logging.Logger,
) -> Optional[Any]:
    """Payload dict for a message, or None when no blob exists anywhere."""
    try:
        return await shepherd_db.get_message(str(message_id), logger)
    except KeyError:
        pass
    except Exception as e:
        logger.warning(f"Redis read failed for {message_id}: {e}")
    try:
        async with shepherd_db.pool.connection(
            settings.postgres_pool_timeout
        ) as conn:
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
    payload = _decompress_payload(bytes(row[0]))
    # Re-warm Redis so subsequent reads are cheap again.
    try:
        await shepherd_db.save_message(str(message_id), payload, logger)
    except Exception:
        pass
    return payload


async def message_has_data(message_id: Union[str, uuid.UUID]) -> bool:
    if await shepherd_db.message_exists(str(message_id)):
        return True
    try:
        async with shepherd_db.pool.connection(
            settings.postgres_pool_timeout
        ) as conn:
            cur = await conn.execute(
                "SELECT data IS NOT NULL FROM ars_message WHERE id = %s",
                (uuid.UUID(str(message_id)),),
            )
            row = await cur.fetchone()
        return bool(row and row[0])
    except Exception:
        return False

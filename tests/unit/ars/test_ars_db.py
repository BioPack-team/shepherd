"""Tests for the ARS Postgres layer's parity-critical write semantics.

The SQL itself is exercised end-to-end elsewhere; what these tests pin is
the write-time behavior ported from upstream:
  - Message.create maps long status names to letters (models.py)
  - message_post_save coercion: 'R' -> 202, 'D' -> 200 (signals.py),
    with the ``_skip_post_save`` escape hatch
"""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import shepherd_utils.db as shepherd_db
from shepherd_utils.ars.db import (
    MESSAGE_COLUMNS,
    create_message,
    serialize_channels,
    update_message,
)


@pytest.fixture
def pg(monkeypatch):
    """Mock pool capturing execute() calls; fetchone returns a message row."""
    conn = AsyncMock()

    def _execute(sql, params=None):
        cursor = AsyncMock()
        # A full-width row so _row_dict can zip it.
        cursor.fetchone.return_value = tuple(
            [uuid.uuid4()] + [None] * (len(MESSAGE_COLUMNS) - 1)
        )
        cursor.fetchall.return_value = []
        result = AsyncMock()
        result.fetchone = cursor.fetchone
        result.fetchall = cursor.fetchall
        return result

    conn.execute.side_effect = _execute

    pool = MagicMock()
    pool.connection.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.connection.return_value.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(shepherd_db, "pool", pool)
    return conn


def _executed_params(conn, call_index=0):
    call = conn.execute.await_args_list[call_index]
    return call.args[1] if len(call.args) > 1 else None


def _executed_sql(conn, call_index=0):
    return conn.execute.await_args_list[call_index].args[0]


async def test_create_message_maps_long_status_and_coerces_code(pg):
    """Message.create('Running', code=202) stores ('R', 202); even a bogus
    code is coerced for R/D statuses."""
    await create_message(actor_id=1, status="Running", code=500)
    params = _executed_params(pg)
    # (pk, name, code, status, actor, ref, params)
    assert params[2] == 202
    assert params[3] == "R"


async def test_create_message_done_coerces_200(pg):
    await create_message(actor_id=1, status="Done", code=202)
    params = _executed_params(pg)
    assert params[2] == 200
    assert params[3] == "D"


async def test_create_message_error_keeps_code(pg):
    await create_message(actor_id=1, status="E", code=598)
    params = _executed_params(pg)
    assert params[2] == 598
    assert params[3] == "E"


async def test_update_message_coerces_running(pg):
    """update with status R forces code 202 even if the caller passed 598."""
    await update_message(uuid.uuid4(), status="R", code=598)
    sql = _executed_sql(pg)
    params = _executed_params(pg)
    fields = dict(
        zip(
            [
                part.split(" = ")[0].strip()
                for part in sql.split("SET ")[1].split("WHERE")[0].split(",")[:-1]
            ],
            params,
        )
    )
    assert fields["status"] == "R"
    assert fields["code"] == 202


async def test_update_message_coerces_done(pg):
    await update_message(uuid.uuid4(), status="D", code=444)
    params = _executed_params(pg)
    assert 200 in params
    assert 444 not in params


async def test_update_message_skip_coercion(pg):
    """The _skip_post_save escape hatch: code passes through untouched."""
    await update_message(uuid.uuid4(), skip_coercion=True, status="D", code=444)
    params = _executed_params(pg)
    assert 444 in params
    assert 200 not in params


async def test_update_message_non_status_write_keeps_code(pg):
    await update_message(uuid.uuid4(), status="E", code=598)
    params = _executed_params(pg)
    assert 598 in params


def test_serialize_channels_matches_django_serializer_shape():
    rows = [
        {"id": 1, "name": "general", "description": "General channel"},
        {"id": 2, "name": "workflow", "description": None},
    ]
    assert serialize_channels(rows) == [
        {
            "model": "tr_ars.channel",
            "pk": 1,
            "fields": {"name": "general", "description": "General channel"},
        },
        {
            "model": "tr_ars.channel",
            "pk": 2,
            "fields": {"name": "workflow", "description": None},
        },
    ]

"""Tests for the ARS actor/subscriber/watchdog postgres helpers."""

import logging
from unittest.mock import AsyncMock

import pytest
from psycopg_pool import AsyncConnectionPool

from shepherd_utils import db

logger = logging.getLogger(__name__)


def _install_pool_mock(mocker, *, cursor_fetchall=None, cursor_fetchone=None):
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=cursor_fetchone)
    mock_cursor.fetchall = AsyncMock(return_value=cursor_fetchall or [])
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = mock_cursor
    mock_pool = AsyncMock(spec=AsyncConnectionPool)
    mock_pool.connection.return_value.__aenter__.return_value = mock_conn
    mock_pool.connection.return_value.__aexit__.return_value = None
    mocker.patch.object(db, "pool", mock_pool)
    return mock_conn


# --- actors ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_actor_runs_on_conflict(mocker):
    conn = _install_pool_mock(mocker)
    await db.upsert_actor(
        "infores:bte", "https://bte/api", "ARA", "BTE", "production", logger
    )
    sql, params = conn.execute.call_args.args
    assert "INSERT INTO ars_actors" in sql
    assert "ON CONFLICT (infores) DO UPDATE" in sql
    assert params[0] == "infores:bte"
    assert params[5] is True  # active
    assert conn.commit.called


@pytest.mark.asyncio
async def test_list_actors_maps_rows(mocker):
    rows = [("infores:x", "u", "ARA", "X", "production", True)]
    _install_pool_mock(mocker, cursor_fetchall=rows)
    out = await db.list_actors(logger)
    assert out == [
        {
            "infores": "infores:x",
            "url": "u",
            "channel": "ARA",
            "agent_name": "X",
            "maturity": "production",
            "active": True,
        }
    ]


@pytest.mark.asyncio
async def test_list_actor_field_distinct(mocker):
    _install_pool_mock(mocker, cursor_fetchall=[("ARA",), ("KP",)])
    assert await db.list_actor_field("channel", logger) == ["ARA", "KP"]


@pytest.mark.asyncio
async def test_list_actor_field_rejects_unknown_column(mocker):
    _install_pool_mock(mocker)
    with pytest.raises(ValueError, match="Unsupported actor field"):
        await db.list_actor_field("url; DROP TABLE", logger)


# --- subscribers ----------------------------------------------------------


@pytest.mark.asyncio
async def test_add_subscriber_inserts(mocker):
    conn = _install_pool_mock(mocker)
    await db.add_subscriber("p1", "http://cb", logger)
    sql, params = conn.execute.call_args.args
    assert "INSERT INTO ars_subscribers" in sql
    assert params == ("p1", "http://cb", None)


@pytest.mark.asyncio
async def test_list_subscribers_flattens(mocker):
    _install_pool_mock(mocker, cursor_fetchall=[("http://a",), ("http://b",)])
    assert await db.list_subscribers("p1", logger) == ["http://a", "http://b"]


# --- watchdog helpers -----------------------------------------------------


@pytest.mark.asyncio
async def test_get_timed_out_ars_parents_maps_array_agg(mocker):
    rows = [("p1", "r1", ["bte", "sipr"])]
    _install_pool_mock(mocker, cursor_fetchall=rows)
    out = await db.get_timed_out_ars_parents(360, logger)
    assert out == [{"qid": "p1", "response_id": "r1", "pending": ["bte", "sipr"]}]


@pytest.mark.asyncio
async def test_mark_ars_children_errored_runs_update(mocker):
    conn = _install_pool_mock(mocker)
    await db.mark_ars_children_errored("p1", ["bte", "sipr"], logger)
    sql, params = conn.execute.call_args.args
    assert "UPDATE ars_children" in sql and "status = 'ERROR'" in sql
    assert params == ("p1", ["bte", "sipr"])


@pytest.mark.asyncio
async def test_mark_ars_children_errored_noop_when_empty(mocker):
    conn = _install_pool_mock(mocker)
    await db.mark_ars_children_errored("p1", [], logger)
    conn.execute.assert_not_called()


# --- agents / channels / clients ------------------------------------------


@pytest.mark.asyncio
async def test_upsert_agent_on_conflict(mocker):
    conn = _install_pool_mock(mocker)
    await db.upsert_agent("BTE", logger, uri="https://bte/api")
    sql, params = conn.execute.call_args.args
    assert "INSERT INTO ars_agents" in sql and "ON CONFLICT (name)" in sql
    assert params == ("BTE", "", "https://bte/api", "")


@pytest.mark.asyncio
async def test_get_agent_maps_row(mocker):
    _install_pool_mock(
        mocker, cursor_fetchone=("BTE", "desc", "https://bte/api", "a@b.c")
    )
    out = await db.get_agent("BTE", logger)
    assert out == {
        "name": "BTE",
        "description": "desc",
        "uri": "https://bte/api",
        "contact": "a@b.c",
    }


@pytest.mark.asyncio
async def test_upsert_channel_on_conflict(mocker):
    conn = _install_pool_mock(mocker)
    await db.upsert_channel("ARA", logger, description="d")
    sql, params = conn.execute.call_args.args
    assert "INSERT INTO ars_channels" in sql and "ON CONFLICT (name)" in sql
    assert params == ("ARA", "d")


@pytest.mark.asyncio
async def test_get_client_maps_row(mocker):
    _install_pool_mock(
        mocker, cursor_fetchone=("c1", "enc", "http://cb", ["p1"], True)
    )
    out = await db.get_client("c1", logger)
    assert out == {
        "client_id": "c1",
        "client_secret": "enc",
        "callback_url": "http://cb",
        "subscriptions": ["p1"],
        "active": True,
    }


@pytest.mark.asyncio
async def test_remove_subscriber_deletes(mocker):
    conn = _install_pool_mock(mocker)
    await db.remove_subscriber("p1", "c1", logger)
    sql, params = conn.execute.call_args.args
    assert "DELETE FROM ars_subscribers" in sql
    assert params == ("p1", "c1")


# --- latest_pk / report ---------------------------------------------------


@pytest.mark.asyncio
async def test_get_latest_pks_shapes_response(mocker):
    # 3 SELECTs: per-day counts, latest n, running. fetchall returns per call.
    mock_cursor = AsyncMock()
    mock_cursor.fetchall = AsyncMock(
        side_effect=[
            [("2026-06-27", 2), ("2026-06-28", 5)],
            [("p9",), ("p8",)],
            [("p9",)],
        ]
    )
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = mock_cursor
    mock_pool = AsyncMock(spec=AsyncConnectionPool)
    mock_pool.connection.return_value.__aenter__.return_value = mock_conn
    mock_pool.connection.return_value.__aexit__.return_value = None
    mocker.patch.object(db, "pool", mock_pool)

    out = await db.get_latest_pks(7, logger)
    assert out["pk_count_last_7_days"] == {"2026-06-27": 2, "2026-06-28": 5}
    assert out["latest_7_pks"] == ["p9", "p8"]
    assert out["latest_24hr_running_pks"] == ["p9"]

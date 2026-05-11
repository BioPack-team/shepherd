"""Tests for the postgres-backed helpers in ``shepherd_utils.db``.

These cover ``add_query``, ``add_callback_id``, ``remove_callback_id``,
``get_running_callbacks``, ``cleanup_callbacks``, ``get_callback_query_id``,
``get_query_state``, ``set_query_completed``, plus the redis-only ``add_query``
storage path.

Each test patches ``shepherd_utils.db.pool`` with a custom AsyncMock chain so
the postgres path uses an in-process fake.
"""

import logging
from unittest.mock import AsyncMock

import pytest
from psycopg import OperationalError
from psycopg_pool import AsyncConnectionPool

from shepherd_utils import db

logger = logging.getLogger(__name__)


def _install_pool_mock(
    mocker, *, cursor_fetchone=None, cursor_fetchall=None, raise_on_execute=None
):
    """Install a postgres pool mock at ``shepherd_utils.db.pool``.

    Returns ``(mock_conn, mock_pool)`` so tests can assert on the mock calls.
    The pool's ``connection(60)`` async context yields ``mock_conn``.
    ``conn.execute`` returns a cursor mock whose ``fetchone()`` / ``fetchall()``
    return the values supplied here.
    """
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(return_value=cursor_fetchone)
    mock_cursor.fetchall = AsyncMock(return_value=cursor_fetchall or [])

    mock_conn = AsyncMock()
    if raise_on_execute is not None:
        mock_conn.execute.side_effect = raise_on_execute
    else:
        mock_conn.execute.return_value = mock_cursor

    mock_pool = AsyncMock(spec=AsyncConnectionPool)
    mock_pool.connection.return_value.__aenter__.return_value = mock_conn
    mock_pool.connection.return_value.__aexit__.return_value = None
    mocker.patch.object(db, "pool", mock_pool)
    return mock_conn, mock_pool


# --- add_query ------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_query_persists_to_redis_and_postgres(redis_mock, mocker):
    """``add_query`` writes the encoded query to redis (twice — query_id and
    response_id) and inserts a row into shepherd_brain."""
    mock_conn, _ = _install_pool_mock(mocker)
    await db.add_query(
        "qid-1", "rid-1", {"message": {}}, callback_url=None, logger=logger
    )

    # Both ids made it into redis.
    assert await redis_mock["data"].exists("qid-1")
    assert await redis_mock["data"].exists("rid-1")

    # Single INSERT issued.
    sql, params = mock_conn.execute.call_args.args
    assert "INSERT INTO shepherd_brain" in sql
    assert params == ("qid-1", "rid-1", None, "QUEUED", "OK")
    assert mock_conn.commit.called


@pytest.mark.asyncio
async def test_add_query_raises_when_redis_save_fails(redis_mock, mocker):
    """If both initial sets fail, add_query raises rather than continuing."""
    mocker.patch.object(
        db.data_db_client,
        "set",
        new_callable=mocker.AsyncMock,
        side_effect=Exception("simulated"),
    )
    with pytest.raises(Exception, match="Failed to save initial query"):
        await db.add_query(
            "qid-1", "rid-1", {"message": {}}, callback_url=None, logger=logger
        )


@pytest.mark.asyncio
async def test_add_query_raises_when_postgres_insert_fails(redis_mock, mocker):
    _install_pool_mock(mocker, raise_on_execute=Exception("pg down"))
    with pytest.raises(Exception, match="Failed to save initial query state"):
        await db.add_query(
            "qid-1",
            "rid-1",
            {"message": {}},
            callback_url="http://cb",
            logger=logger,
        )


# --- add_callback_id / remove_callback_id ---------------------------------


@pytest.mark.asyncio
async def test_add_callback_id_inserts_row(mocker):
    mock_conn, _ = _install_pool_mock(mocker)
    await db.add_callback_id("qid", "cb-1", '{"trace": 1}', logger)
    sql, params = mock_conn.execute.call_args.args
    assert "INSERT INTO callbacks" in sql
    assert params == ("qid", "cb-1", '{"trace": 1}')
    assert mock_conn.commit.called


@pytest.mark.asyncio
async def test_add_callback_id_retries_on_operational_error(mocker):
    """OperationalError causes a retry with exponential backoff. The second
    attempt succeeds."""
    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.execute.side_effect = [
        OperationalError("first failed"),
        mock_cursor,
    ]
    mock_pool = AsyncMock(spec=AsyncConnectionPool)
    mock_pool.connection.return_value.__aenter__.return_value = mock_conn
    mock_pool.connection.return_value.__aexit__.return_value = None
    mocker.patch.object(db, "pool", mock_pool)
    # Don't actually sleep.
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)

    await db.add_callback_id("qid", "cb-1", "{}", logger)
    assert mock_conn.execute.call_count == 2


@pytest.mark.asyncio
async def test_add_callback_id_swallows_non_operational_errors(mocker):
    """Non-OperationalError exceptions are logged and the function returns
    without retrying."""
    _install_pool_mock(mocker, raise_on_execute=ValueError("bad"))
    # Should not raise.
    await db.add_callback_id("qid", "cb-1", "{}", logger)


@pytest.mark.asyncio
async def test_remove_callback_id_runs_delete(mocker):
    mock_conn, _ = _install_pool_mock(mocker)
    await db.remove_callback_id("cb-1", logger)
    sql, params = mock_conn.execute.call_args.args
    assert "DELETE FROM callbacks" in sql
    assert params == ("cb-1",)


@pytest.mark.asyncio
async def test_remove_callback_id_retries_on_operational_error(mocker):
    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.execute.side_effect = [OperationalError("x"), mock_cursor]
    mock_pool = AsyncMock(spec=AsyncConnectionPool)
    mock_pool.connection.return_value.__aenter__.return_value = mock_conn
    mock_pool.connection.return_value.__aexit__.return_value = None
    mocker.patch.object(db, "pool", mock_pool)
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
    await db.remove_callback_id("cb-1", logger)
    assert mock_conn.execute.call_count == 2


# --- get_running_callbacks / cleanup_callbacks ----------------------------


@pytest.mark.asyncio
async def test_get_running_callbacks_returns_rows(mocker):
    rows = [("cb-1",), ("cb-2",)]
    mock_conn, _ = _install_pool_mock(mocker, cursor_fetchall=rows)
    out = await db.get_running_callbacks("qid", logger)
    assert out == rows


@pytest.mark.asyncio
async def test_get_running_callbacks_propagates_non_operational_error(mocker):
    """Non-OperationalError exceptions are re-raised so callers can decide."""
    _install_pool_mock(mocker, raise_on_execute=RuntimeError("kaboom"))
    with pytest.raises(RuntimeError, match="kaboom"):
        await db.get_running_callbacks("qid", logger)


@pytest.mark.asyncio
async def test_cleanup_callbacks_runs_delete(mocker):
    mock_conn, _ = _install_pool_mock(mocker)
    await db.cleanup_callbacks("qid-99", logger)
    sql, params = mock_conn.execute.call_args.args
    assert "DELETE FROM callbacks" in sql
    assert params == ("qid-99",)


# --- get_callback_query_id ------------------------------------------------


@pytest.mark.asyncio
async def test_get_callback_query_id_returns_row(mocker):
    """Found row is returned as-is (a (query_id, otel_trace) tuple)."""
    mock_conn, _ = _install_pool_mock(mocker, cursor_fetchone=("qid-1", "{}"))
    out = await db.get_callback_query_id("cb-1", logger)
    assert out == ("qid-1", "{}")


@pytest.mark.asyncio
async def test_get_callback_query_id_returns_none_when_missing(mocker):
    _install_pool_mock(mocker, cursor_fetchone=None)
    out = await db.get_callback_query_id("missing", logger)
    assert out is None


# --- get_query_state / set_query_completed --------------------------------


@pytest.mark.asyncio
async def test_get_query_state_returns_full_row(mocker):
    fake_row = ("qid-1", "now", None, "QUEUED", "OK", None, None, "rid-1", None)
    _install_pool_mock(mocker, cursor_fetchone=fake_row)
    out = await db.get_query_state("qid-1", logger)
    assert out == fake_row


@pytest.mark.asyncio
async def test_get_query_state_returns_none_when_missing(mocker):
    _install_pool_mock(mocker, cursor_fetchone=None)
    out = await db.get_query_state("ghost", logger)
    assert out is None


@pytest.mark.asyncio
async def test_set_query_completed_runs_update(mocker):
    mock_conn, _ = _install_pool_mock(mocker)
    await db.set_query_completed("qid-1", "OK", logger)
    sql, params = mock_conn.execute.call_args.args
    assert "UPDATE shepherd_brain" in sql
    assert params == ("OK", "qid-1")
    assert mock_conn.commit.called


@pytest.mark.asyncio
async def test_set_query_completed_retries_on_operational_error(mocker):
    mock_cursor = AsyncMock()
    mock_conn = AsyncMock()
    mock_conn.execute.side_effect = [OperationalError("x"), mock_cursor]
    mock_pool = AsyncMock(spec=AsyncConnectionPool)
    mock_pool.connection.return_value.__aenter__.return_value = mock_conn
    mock_pool.connection.return_value.__aexit__.return_value = None
    mocker.patch.object(db, "pool", mock_pool)
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
    await db.set_query_completed("qid-1", "OK", logger)
    assert mock_conn.execute.call_count == 2


# --- initialize_db / shutdown_db ------------------------------------------


@pytest.mark.asyncio
async def test_initialize_and_shutdown_open_and_close_pool(mocker):
    mock_pool = AsyncMock(spec=AsyncConnectionPool)
    mocker.patch.object(db, "pool", mock_pool)
    await db.initialize_db()
    assert mock_pool.open.called
    await db.shutdown_db()
    assert mock_pool.close.called


# --- check_connection -----------------------------------------------------


@pytest.mark.asyncio
async def test_check_connection_executes_select_one():
    mock_conn = AsyncMock()
    await db.check_connection(mock_conn)
    mock_conn.execute.assert_awaited_once_with("SELECT 1")

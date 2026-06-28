"""Tests for the retain feature (ARS parity)."""

import logging
from unittest.mock import AsyncMock

import pytest
from psycopg_pool import AsyncConnectionPool

from shepherd_utils import db

logger = logging.getLogger(__name__)


def _install_pool_mock(mocker, *, fetchone_sequence=None):
    """Pool mock whose cursor.fetchone yields the given sequence in order."""
    mock_cursor = AsyncMock()
    mock_cursor.fetchone = AsyncMock(side_effect=list(fetchone_sequence or []))
    mock_cursor.fetchall = AsyncMock(return_value=[])
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = mock_cursor
    mock_pool = AsyncMock(spec=AsyncConnectionPool)
    mock_pool.connection.return_value.__aenter__.return_value = mock_conn
    mock_pool.connection.return_value.__aexit__.return_value = None
    mocker.patch.object(db, "pool", mock_pool)
    return mock_conn


def _executed_sql(mock_conn):
    return [c.args[0] for c in mock_conn.execute.call_args_list]


@pytest.mark.asyncio
async def test_retain_terminal_parent_succeeds(mocker):
    # 1st fetchone: no child row (qid is a parent). 2nd: state COMPLETED.
    conn = _install_pool_mock(mocker, fetchone_sequence=[None, ("COMPLETED",)])
    out = await db.set_query_retained("p1", logger)
    assert out == {"success": True, "parent_pk": "p1"}
    sql = " ".join(_executed_sql(conn))
    assert "UPDATE shepherd_brain SET retain = TRUE" in sql
    assert conn.commit.called


@pytest.mark.asyncio
async def test_retain_refuses_while_running(mocker):
    conn = _install_pool_mock(mocker, fetchone_sequence=[None, ("QUEUED",)])
    out = await db.set_query_retained("p1", logger)
    assert out == {
        "success": False,
        "parent_pk": "p1",
        "description": "PK still running",
    }
    # No UPDATE issued.
    assert all("UPDATE" not in s for s in _executed_sql(conn))


@pytest.mark.asyncio
async def test_retain_child_pk_resolves_parent(mocker):
    # 1st fetchone: child resolves to parent-x. 2nd: state COMPLETED.
    _install_pool_mock(mocker, fetchone_sequence=[("parent-x",), ("COMPLETED",)])
    out = await db.set_query_retained("child-1", logger)
    assert out == {"success": True, "parent_pk": "parent-x"}


@pytest.mark.asyncio
async def test_retain_invalid_pk(mocker):
    _install_pool_mock(mocker, fetchone_sequence=[None, None])
    out = await db.set_query_retained("ghost", logger)
    assert out == {"success": False, "description": "Invalid PK"}


@pytest.mark.asyncio
async def test_purge_excludes_retained(mocker):
    conn = _install_pool_mock(mocker)
    await db.purge_old_queries(30, logger)
    sql = " ".join(_executed_sql(conn))
    # Both the callbacks subquery and the shepherd_brain delete guard on retain.
    assert sql.count("retain = FALSE") == 2
    assert "DELETE FROM shepherd_brain" in sql

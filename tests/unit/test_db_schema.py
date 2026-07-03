"""Tests for the ARS schema self-heal (``ensure_ars_schema``)."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg_pool import AsyncConnectionPool

from shepherd_utils import db


def _install_pool_mock(mocker, *, sentinel):
    """Pool mock. ``sentinel`` is the ``to_regclass`` fetchone result."""
    check_cursor = AsyncMock()
    check_cursor.fetchone = AsyncMock(return_value=sentinel)

    ddl_cursor = AsyncMock()
    cursor_cm = AsyncMock()
    cursor_cm.__aenter__.return_value = ddl_cursor
    cursor_cm.__aexit__.return_value = None

    mock_conn = AsyncMock()
    mock_conn.execute.return_value = check_cursor
    # conn.cursor() is sync-returning in psycopg3 -> plain MagicMock.
    mock_conn.cursor = MagicMock(return_value=cursor_cm)

    mock_pool = AsyncMock(spec=AsyncConnectionPool)
    mock_pool.connection.return_value.__aenter__.return_value = mock_conn
    mock_pool.connection.return_value.__aexit__.return_value = None
    mocker.patch.object(db, "pool", mock_pool)
    return mock_conn, ddl_cursor


@pytest.mark.asyncio
async def test_ensure_ars_schema_fast_paths_when_present(mocker):
    """When the sentinel table exists, no DDL is run."""
    mock_conn, ddl_cursor = _install_pool_mock(mocker, sentinel=("ars_clients",))
    await db.ensure_ars_schema()
    mock_conn.cursor.assert_not_called()
    ddl_cursor.execute.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_ars_schema_applies_ddl_when_missing(mocker):
    """When the sentinel is absent, the advisory lock + ARS DDL run and commit."""
    mock_conn, ddl_cursor = _install_pool_mock(mocker, sentinel=(None,))
    await db.ensure_ars_schema()
    executed = [c.args[0] for c in ddl_cursor.execute.call_args_list]
    assert any("pg_advisory_xact_lock" in s for s in executed)
    assert any("CREATE TABLE IF NOT EXISTS ars_children" in s for s in executed)
    assert any("ADD COLUMN IF NOT EXISTS retain" in s for s in executed)
    assert mock_conn.commit.called

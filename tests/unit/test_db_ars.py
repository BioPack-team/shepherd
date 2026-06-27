"""Tests for the ARS-specific postgres helpers in ``shepherd_utils.db``.

Covers ``set_query_ars_parent``, ``add_ars_children``, ``set_ars_child_status``,
``get_pending_ars_children``, ``get_ars_children``, ``claim_ars_tail`` and
``list_ars_parents``. Mirrors the pool-mocking style of ``test_db_postgres.py``.
"""

import logging
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from psycopg_pool import AsyncConnectionPool

from shepherd_utils import db

logger = logging.getLogger(__name__)


def _install_pool_mock(
    mocker, *, cursor_fetchone=None, cursor_fetchall=None, raise_on_execute=None
):
    """Install a postgres pool mock at ``shepherd_utils.db.pool``.

    Returns ``(mock_conn, mock_pool)``. ``conn.execute`` returns a cursor whose
    ``fetchone``/``fetchall`` yield the supplied values; ``conn.cursor()`` yields
    a context-managed cursor (used by ``executemany`` paths).
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


# --- set_query_ars_parent -------------------------------------------------


@pytest.mark.asyncio
async def test_set_query_ars_parent_runs_update(mocker):
    mock_conn, _ = _install_pool_mock(mocker)
    await db.set_query_ars_parent("qid-1", logger)
    sql, params = mock_conn.execute.call_args.args
    assert "UPDATE shepherd_brain SET is_ars_parent = TRUE" in sql
    assert params == ("qid-1",)
    assert mock_conn.commit.called


# --- add_ars_children -----------------------------------------------------


@pytest.mark.asyncio
async def test_add_ars_children_inserts_all_rows(mocker):
    mock_conn, _ = _install_pool_mock(mocker)
    children = [
        {
            "ara": "aragorn",
            "child_qid": "cq-a",
            "child_response_id": "cr-a",
            "ars_callback_id": "cb-a",
            "otel_trace": "{}",
        },
        {
            "ara": "bte",
            "child_qid": "cq-b",
            "child_response_id": "cr-b",
            "ars_callback_id": "cb-b",
            "otel_trace": "{}",
        },
    ]
    await db.add_ars_children("parent-1", children, logger)

    # One INSERT per child row.
    all_params = [call.args[1] for call in mock_conn.execute.call_args_list]
    sql = mock_conn.execute.call_args_list[0].args[0]
    assert "INSERT INTO ars_children" in sql
    assert all_params == [
        ("parent-1", "aragorn", "cq-a", "cr-a", "cb-a", "{}", "QUEUED"),
        ("parent-1", "bte", "cq-b", "cr-b", "cb-b", "{}", "QUEUED"),
    ]
    assert mock_conn.commit.called


@pytest.mark.asyncio
async def test_add_ars_children_raises_on_non_operational_error(mocker):
    _install_pool_mock(mocker, raise_on_execute=ValueError("boom"))
    with pytest.raises(ValueError, match="boom"):
        await db.add_ars_children(
            "p", [{"ara": "arax", "child_qid": "q", "child_response_id": "r"}], logger
        )


# --- set_ars_child_status -------------------------------------------------


@pytest.mark.asyncio
async def test_set_ars_child_status_terminal_sets_stop_time(mocker):
    """A DONE update writes the optional fields and stamps stop_time."""
    mock_conn, _ = _install_pool_mock(mocker)
    await db.set_ars_child_status(
        "parent-1",
        "aragorn",
        "DONE",
        logger,
        child_response_id="cr-a",
        code=200,
        result_count=7,
    )
    sql, params = mock_conn.execute.call_args.args
    assert "stop_time = NOW()" in sql
    # status, child_response_id, code, result_count, then WHERE parent + ara
    assert params == ("DONE", "cr-a", 200, 7, "parent-1", "aragorn")


@pytest.mark.asyncio
async def test_set_ars_child_status_running_does_not_set_stop_time(mocker):
    """A non-terminal update omits stop_time and only writes provided fields."""
    mock_conn, _ = _install_pool_mock(mocker)
    await db.set_ars_child_status("parent-1", "bte", "RUNNING", logger)
    sql, params = mock_conn.execute.call_args.args
    assert "stop_time" not in sql
    assert params == ("RUNNING", "parent-1", "bte")


# --- get_pending_ars_children ---------------------------------------------


@pytest.mark.asyncio
async def test_get_pending_ars_children_flattens_rows(mocker):
    _install_pool_mock(mocker, cursor_fetchall=[("aragorn",), ("sipr",)])
    out = await db.get_pending_ars_children("parent-1", logger)
    assert out == ["aragorn", "sipr"]


@pytest.mark.asyncio
async def test_get_pending_ars_children_empty_when_all_done(mocker):
    _install_pool_mock(mocker, cursor_fetchall=[])
    out = await db.get_pending_ars_children("parent-1", logger)
    assert out == []


# --- get_ars_children -----------------------------------------------------


@pytest.mark.asyncio
async def test_get_ars_children_maps_rows_to_dicts(mocker):
    start = datetime(2026, 1, 1, 12, 0, 0)
    rows = [
        ("aragorn", "cq-a", "cr-a", "DONE", 200, 5, 1, start, None),
    ]
    _install_pool_mock(mocker, cursor_fetchall=rows)
    out = await db.get_ars_children("parent-1", logger)
    assert out == [
        {
            "ara": "aragorn",
            "child_qid": "cq-a",
            "child_response_id": "cr-a",
            "status": "DONE",
            "code": 200,
            "result_count": 5,
            "merged_version": 1,
            "start_time": start.isoformat(),
            "stop_time": None,
        }
    ]


# --- claim_ars_tail -------------------------------------------------------


@pytest.mark.asyncio
async def test_claim_ars_tail_true_when_row_returned(mocker):
    """The winning caller gets the RETURNING row back -> True."""
    _install_pool_mock(mocker, cursor_fetchone=("parent-1",))
    assert await db.claim_ars_tail("parent-1", logger) is True


@pytest.mark.asyncio
async def test_claim_ars_tail_false_when_already_launched(mocker):
    """A concurrent caller whose UPDATE matched no row -> False."""
    _install_pool_mock(mocker, cursor_fetchone=None)
    assert await db.claim_ars_tail("parent-1", logger) is False


# --- list_ars_parents -----------------------------------------------------


@pytest.mark.asyncio
async def test_list_ars_parents_maps_rows(mocker):
    start = datetime(2026, 1, 1, 12, 0, 0)
    rows = [("qid-1", start, None, "rid-1", "QUEUED", "OK")]
    _install_pool_mock(mocker, cursor_fetchall=rows)
    out = await db.list_ars_parents(logger)
    assert out == [
        {
            "qid": "qid-1",
            "start_time": start.isoformat(),
            "stop_time": None,
            "response_id": "rid-1",
            "state": "QUEUED",
            "status": "OK",
        }
    ]

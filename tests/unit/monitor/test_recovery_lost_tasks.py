"""A Redis restart can lose a query's queued task while its data survives (#209).

The task streams live in the same Redis as the query blobs. The restart sweep
only finished queries whose blobs were gone, so a query whose blobs had been
persisted but whose next task (or the task a worker was holding) had not was
left with nothing to drive it, until the abandoned-query reaper -- which never
tells the caller.
"""

import logging
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from shepherd_utils import db
from workers.monitor import alerts, recovery

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def broker(redis_mock, monkeypatch):
    monkeypatch.setattr(recovery, "broker_client", redis_mock["broker"])
    return redis_mock["broker"]


@pytest.fixture(autouse=True)
def alerted(mocker):
    return {
        "record": mocker.patch.object(alerts, "_record_alert", AsyncMock()),
        "dispatch": mocker.patch.object(alerts, "dispatch", AsyncMock()),
        "dispatch_batch": mocker.patch.object(alerts, "dispatch_batch", AsyncMock()),
    }


@pytest.fixture
def unfinished(mocker):
    """Serve ``rows`` from Postgres as the unfinished, pre-restart queries; no
    query has lost its blobs."""
    rows = []

    class _Cursor:
        async def fetchall(self):
            return list(rows)

    class _Conn:
        async def execute(self, sql, params):
            return _Cursor()

    @asynccontextmanager
    async def _connection(*args, **kwargs):
        yield _Conn()

    mocker.patch.object(db.pool, "connection", _connection)
    mocker.patch.object(
        recovery, "find_queries_with_lost_data", AsyncMock(return_value=[])
    )
    mocker.patch.object(recovery, "_wait_until_loaded", AsyncMock())
    mocker.patch.object(recovery.asyncio, "sleep", AsyncMock())
    return rows


@pytest.fixture
def finished(mocker):
    return mocker.patch.object(recovery, "add_task", AsyncMock())


def _finished_qids(add_task):
    return sorted(
        call.args[1]["query_id"]
        for call in add_task.await_args_list
        if call.args[0] == "finish_query"
    )


async def test_a_query_with_no_task_left_is_finished(broker, unfinished, finished):
    unfinished.extend([("q-alive", "r-alive"), ("q-lost", "r-lost")])
    await broker.xadd(
        "aragorn.lookup", {"query_id": "q-alive", "response_id": "r-alive"}
    )

    await recovery.reconcile_after_restart(detected_at=0.0)

    assert _finished_qids(finished) == ["q-lost"]
    fields = finished.await_args_list[0].args[1]
    assert fields["status"].startswith("ERROR")
    assert fields["response_id"] == "r-lost"
    assert "restart" in fields["description"]


async def test_a_task_held_by_a_worker_counts(broker, unfinished, finished):
    """A delivered-but-unACKed task is still in its stream (it is only deleted
    on ACK), so a query a worker is running is left alone."""
    unfinished.append(("q-running", "r-running"))
    await broker.xgroup_create("sort_results_score", "consumer", "0", mkstream=True)
    await broker.xadd("sort_results_score", {"query_id": "q-running"})
    await broker.xreadgroup("consumer", "c1", {"sort_results_score": ">"}, count=1)

    await recovery.reconcile_after_restart(detected_at=0.0)

    assert _finished_qids(finished) == []


async def test_a_merge_wake_task_does_not_count_as_the_next_step(
    broker, unfinished, finished
):
    """merge_message tasks merge callbacks; they never advance the workflow, so
    a query whose only task is a merge wake has still lost its driver."""
    unfinished.append(("q-merging", "r-merging"))
    await broker.xadd("merge_message", {"query_id": "q-merging", "callback_id": "cb"})

    await recovery.reconcile_after_restart(detected_at=0.0)

    assert _finished_qids(finished) == ["q-merging"]


async def test_a_query_finished_by_the_blob_sweep_is_not_finished_twice(
    broker, unfinished, finished, mocker
):
    unfinished.append(("q-gone", "r-gone"))
    lost = [{"qid": "q-gone", "response_id": "r-gone", "missing": ["r-gone"]}]
    mocker.patch.object(
        recovery, "find_queries_with_lost_data", AsyncMock(return_value=lost)
    )

    async def _add_task(queue, fields, _logger, **kwargs):
        await broker.xadd(queue, fields)

    finished.side_effect = _add_task

    await recovery.reconcile_after_restart(detected_at=0.0)

    assert _finished_qids(finished) == ["q-gone"]

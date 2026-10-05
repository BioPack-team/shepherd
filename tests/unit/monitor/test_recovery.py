"""Recovering the queries a Redis restart left behind (workers.monitor.recovery).

A restart rolls Redis back to its last snapshot, so in-flight queries can lose
their query/response blobs while Postgres still says they're running. The
monitor spots the restart from Redis' run_id, alerts, and -- once Redis has
loaded and a grace period has passed -- finishes those queries as errors.
"""

import logging
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from shepherd_utils import db
from shepherd_utils.db import find_queries_with_lost_data, save_message
from workers.monitor import alerts, recovery

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def broker(redis_mock, monkeypatch):
    """recovery took its broker client at import, before redis_mock swapped
    the module's; point it at the fake too."""
    monkeypatch.setattr(recovery, "broker_client", redis_mock["broker"])
    return redis_mock["broker"]


@pytest.fixture
def alerted(mocker):
    return {
        "record": mocker.patch.object(alerts, "_record_alert", AsyncMock()),
        "dispatch": mocker.patch.object(alerts, "dispatch", AsyncMock()),
        "dispatch_batch": mocker.patch.object(alerts, "dispatch_batch", AsyncMock()),
    }


def _fake_unfinished_queries(mocker, rows):
    """Serve ``rows`` as the unfinished queries the Postgres lookup finds."""
    calls = []

    class _Cursor:
        async def fetchall(self):
            return rows

    class _Conn:
        async def execute(self, sql, params):
            calls.append((sql, params))
            return _Cursor()

    @asynccontextmanager
    async def _connection(*args, **kwargs):
        yield _Conn()

    mocker.patch.object(db.pool, "connection", _connection)
    return calls


async def _store(*message_ids):
    for message_id in message_ids:
        await save_message(message_id, {"message": {}}, logger)


# --- finding the queries that lost their data ---------------------------------


async def test_finds_unfinished_queries_missing_their_query_or_response(
    redis_mock, mocker
):
    calls = _fake_unfinished_queries(mocker, [("q1", "r1"), ("q2", "r2"), ("q3", "r3")])
    await _store("q1", "r1", "q2")  # q2 lost its response, q3 lost both

    lost = await find_queries_with_lost_data(90, logger)

    assert lost == [
        {"qid": "q2", "response_id": "r2", "missing": ["r2"]},
        {"qid": "q3", "response_id": "r3", "missing": ["q3", "r3"]},
    ]
    sql, params = calls[0]
    assert params == (90.0,)
    # skips finished queries, and ones a lookup is still waiting on
    assert "NOT IN ('COMPLETED', 'ABANDONED')" in sql
    assert "NOT EXISTS (SELECT 1 FROM callbacks" in sql


async def test_a_redis_error_is_not_data_loss(redis_mock, mocker):
    _fake_unfinished_queries(mocker, [("q1", "r1")])
    mocker.patch.object(
        db, "message_exists", AsyncMock(side_effect=ConnectionError("LOADING"))
    )
    with pytest.raises(ConnectionError):
        await find_queries_with_lost_data(90, logger)


# --- finishing them ------------------------------------------------------------


def _lost(qid, missing=("r",)):
    return {"qid": qid, "response_id": f"{qid}-r", "missing": list(missing)}


async def test_lost_queries_are_finished_as_errors(mocker, alerted):
    mocker.patch.object(
        recovery,
        "find_queries_with_lost_data",
        AsyncMock(return_value=[_lost("q1"), _lost("q2")]),
    )
    add_task = mocker.patch.object(recovery, "add_task", AsyncMock())

    finished = await recovery.reconcile_lost_queries(120)

    assert [q["qid"] for q in finished] == ["q1", "q2"]
    streams = {c.args[0] for c in add_task.await_args_list}
    assert streams == {"finish_query"}
    fields = add_task.await_args_list[0].args[1]
    assert fields["query_id"] == "q1"
    assert fields["response_id"] == "q1-r"
    assert fields["status"] == "ERROR"
    assert fields["workflow"] == "[]"
    # one batched alert for the lot
    assert alerted["record"].await_count == 2
    alerted["dispatch_batch"].assert_awaited_once()


async def test_a_query_that_cannot_be_handed_off_is_not_reported(mocker, alerted):
    mocker.patch.object(
        recovery,
        "find_queries_with_lost_data",
        AsyncMock(return_value=[_lost("q1"), _lost("q2")]),
    )
    mocker.patch.object(
        recovery,
        "add_task",
        AsyncMock(side_effect=[ConnectionError("down"), None]),
    )

    finished = await recovery.reconcile_lost_queries(120)

    assert [q["qid"] for q in finished] == ["q2"]
    alerted["dispatch"].assert_awaited_once()


async def test_nothing_lost_nothing_done(mocker, alerted):
    mocker.patch.object(
        recovery, "find_queries_with_lost_data", AsyncMock(return_value=[])
    )
    add_task = mocker.patch.object(recovery, "add_task", AsyncMock())

    assert await recovery.reconcile_lost_queries(120) == []
    add_task.assert_not_awaited()
    alerted["record"].assert_not_awaited()


async def test_a_failed_check_finishes_nothing(mocker, alerted):
    mocker.patch.object(
        recovery,
        "find_queries_with_lost_data",
        AsyncMock(side_effect=ConnectionError("LOADING")),
    )
    add_task = mocker.patch.object(recovery, "add_task", AsyncMock())

    assert await recovery.reconcile_lost_queries(120) == []
    add_task.assert_not_awaited()


async def test_sweep_waits_for_the_load_and_the_grace_period(redis_mock, mocker):
    mocker.patch.object(
        recovery.broker_client,
        "info",
        AsyncMock(side_effect=[{"loading": 1}, {"loading": "1"}, {"loading": 0}]),
    )
    sleep = mocker.patch.object(recovery.asyncio, "sleep", AsyncMock())
    reconcile = mocker.patch.object(
        recovery, "reconcile_lost_queries", AsyncMock(return_value=[])
    )
    mocker.patch.object(recovery.settings, "monitor_redis_restart_grace_sec", 60)
    mocker.patch.object(recovery.time, "time", return_value=1000.0)

    await recovery.reconcile_after_restart(detected_at=900.0)

    # polled the load twice, then waited out the grace period
    assert [c.args[0] for c in sleep.await_args_list] == [
        recovery.LOADING_POLL_SEC,
        recovery.LOADING_POLL_SEC,
        60,
    ]
    # only queries from before the restart
    reconcile.assert_awaited_once_with(100.0)


# --- spotting the restart --------------------------------------------------------


def _snapshot(run_id, uptime=5):
    return {"redis": {"run_id": run_id, "uptime_in_seconds": uptime}}


@pytest.fixture
def sweep(mocker):
    return mocker.patch.object(
        recovery, "reconcile_after_restart", AsyncMock(return_value=[])
    )


async def test_first_sighting_is_not_a_restart(redis_mock, alerted, sweep):
    watcher = recovery.RedisRestartWatcher()

    assert not await watcher.observe(_snapshot("run-a"))
    assert not await watcher.observe(_snapshot("run-a"))
    assert await recovery.broker_client.get(recovery.RUN_ID_KEY) == "run-a"
    alerted["record"].assert_not_awaited()
    sweep.assert_not_called()


async def test_a_new_run_id_is_a_restart(redis_mock, alerted, sweep):
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_snapshot("run-a"))

    assert await watcher.observe(_snapshot("run-b"))
    await watcher._sweep

    event = alerted["record"].await_args.args[0]
    assert event["rule"] == "redis_restarted"
    alerted["dispatch"].assert_awaited_once()
    sweep.assert_awaited_once()
    assert await recovery.broker_client.get(recovery.RUN_ID_KEY) == "run-b"
    # and it settles on the new run_id
    assert not await watcher.observe(_snapshot("run-b"))


async def test_a_restart_is_spotted_across_a_monitor_restart(
    redis_mock, alerted, sweep
):
    """The last run_id is kept in Redis, so a monitor that restarted too still
    sees the change (a rolled-back copy of the key reads the same way)."""
    await recovery.broker_client.set(recovery.RUN_ID_KEY, "run-a")
    watcher = recovery.RedisRestartWatcher()

    assert await watcher.observe(_snapshot("run-b"))
    await watcher._sweep
    sweep.assert_awaited_once()


async def test_a_second_restart_reschedules_one_sweep_from_the_first(
    redis_mock, alerted, mocker
):
    started = []

    async def _slow_sweep(detected_at):
        started.append(detected_at)
        await recovery.asyncio.Event().wait()  # never finishes on its own

    mocker.patch.object(recovery, "reconcile_after_restart", _slow_sweep)
    clock = mocker.patch.object(recovery.time, "time", return_value=100.0)
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_snapshot("run-a"))

    await watcher.observe(_snapshot("run-b"))
    first = watcher._sweep
    await recovery.asyncio.sleep(0)
    clock.return_value = 200.0
    await watcher.observe(_snapshot("run-c"))
    await recovery.asyncio.sleep(0)

    assert first.cancelled()
    assert started == [100.0, 100.0]  # the rescheduled sweep covers both
    watcher.cancel()


async def test_the_run_id_is_recorded_once_redis_takes_writes(
    redis_mock, alerted, sweep, mocker
):
    """Redis refuses writes while it loads; the run_id must still be recorded
    once it can be, or a later monitor restart would report a restart that
    never happened."""
    watcher = recovery.RedisRestartWatcher()
    refusing = mocker.patch.object(
        recovery.broker_client,
        "set",
        AsyncMock(side_effect=ConnectionError("LOADING")),
    )
    await watcher.observe(_snapshot("run-a"))
    mocker.stop(refusing)

    await watcher.observe(_snapshot("run-a"))

    assert await recovery.broker_client.get(recovery.RUN_ID_KEY) == "run-a"
    assert not await recovery.RedisRestartWatcher().observe(_snapshot("run-a"))

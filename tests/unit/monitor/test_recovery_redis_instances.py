"""Restart recovery when Redis is several servers (workers.monitor.recovery).

Each instance's run_id is watched. What a restart sets off depends on what the
instance holds: the data role's lost payloads, the broker role's lost tasks.
With one shared Redis that is both, as before (test_recovery.py is that
oracle). Behind Sentinel a failover changes the run_id too, and is treated
the same way.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from workers.monitor import alerts, recovery, redis_instances

BROKER_INSTANCE = "shepherd_broker:6379"
DATA_INSTANCE = "shepherd_data:6379"


@pytest.fixture(autouse=True)
def broker(redis_mock, monkeypatch):
    monkeypatch.setattr(recovery, "broker_client", redis_mock["broker"])
    return redis_mock["broker"]


@pytest.fixture(autouse=True)
def data_client(mocker):
    """The monitor's own client for the data role's Redis."""
    client = MagicMock(name="data_client")
    client.info = AsyncMock(return_value={"loading": 0})
    mocker.patch.object(redis_instances, "extra_client", return_value=client)
    return client


@pytest.fixture
def alerted(mocker):
    return {
        "record": mocker.patch.object(alerts, "_record_alert", AsyncMock()),
        "dispatch": mocker.patch.object(alerts, "dispatch", AsyncMock()),
    }


@pytest.fixture
def sweep(mocker):
    return mocker.patch.object(
        recovery, "reconcile_after_restart", AsyncMock(return_value=[])
    )


def _split(broker_run, data_run, **data_extra):
    """A snapshot with the data role on a Redis of its own."""
    broker_stats = {
        "instance": BROKER_INSTANCE,
        "roles": ["broker", "lock", "logs"],
        "run_id": broker_run,
        "uptime_in_seconds": 5,
    }
    data_stats = {
        "instance": DATA_INSTANCE,
        "roles": ["data"],
        "run_id": data_run,
        "uptime_in_seconds": 5,
        **data_extra,
    }
    return {
        "redis": broker_stats,
        "redis_instances": {BROKER_INSTANCE: broker_stats, DATA_INSTANCE: data_stats},
    }


def _single(run_id, **extra):
    stats = {
        "instance": BROKER_INSTANCE,
        "roles": ["broker", "data", "lock", "logs"],
        "run_id": run_id,
        "uptime_in_seconds": 5,
        **extra,
    }
    return {"redis": stats, "redis_instances": {BROKER_INSTANCE: stats}}


# --- which sweep a restart runs -------------------------------------------------


async def test_a_data_instance_restart_sweeps_for_lost_payloads_only(
    alerted, sweep, data_client, mocker
):
    mocker.patch.object(recovery.time, "time", return_value=500.0)
    watcher = recovery.RedisRestartWatcher()
    assert not await watcher.observe(_split("broker-a", "data-a"))

    assert await watcher.observe(_split("broker-a", "data-b"))
    await watcher._sweep

    # the tasks are on the broker's Redis, which did not restart; and the
    # sweep waits for the Redis that did
    sweep.assert_awaited_once_with(500.0, lost_tasks=False, wait_for=[data_client])
    event = alerted["record"].await_args.args[0]
    assert event["rule"] == f"redis_restarted@{DATA_INSTANCE}"
    assert event["detail"] == f"Redis `{DATA_INSTANCE}` (data) restarted (up 5s)"
    assert "in-flight query data" in event["message"]
    assert "Queries that lost their data will be finished as errors" in event["message"]
    assert "queued tasks" not in event["message"]


async def test_a_broker_instance_restart_sweeps_for_lost_tasks_not_payloads(
    alerted, sweep, mocker
):
    """The payloads are on the data Redis and are all still there: checking
    them again would only re-finish what the task sweep already handles."""
    mocker.patch.object(recovery.time, "time", return_value=500.0)
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_split("broker-a", "data-a"))

    assert await watcher.observe(_split("broker-b", "data-a"))
    await watcher._sweep

    sweep.assert_awaited_once_with(500.0, lost_payloads=False)
    event = alerted["record"].await_args.args[0]
    # the broker role's instance keeps the alert name it always had
    assert event["rule"] == "redis_restarted"
    assert f"`{BROKER_INSTANCE}` (broker, lock, logs)" in event["message"]
    assert "queued tasks" in event["message"]
    assert "Queries that lost their next task" in event["message"]


async def test_the_scoped_sweep_runs_only_the_steps_it_was_asked_for(mocker):
    mocker.patch.object(recovery, "_wait_until_loaded", AsyncMock())
    mocker.patch.object(recovery.asyncio, "sleep", AsyncMock())
    payloads = mocker.patch.object(
        recovery, "reconcile_lost_queries", AsyncMock(return_value=[{"qid": "p"}])
    )
    tasks = mocker.patch.object(
        recovery, "reconcile_lost_tasks", AsyncMock(return_value=[{"qid": "t"}])
    )

    assert await recovery.reconcile_after_restart(0.0, lost_tasks=False) == [
        {"qid": "p"}
    ]
    payloads.assert_awaited_once()
    tasks.assert_not_awaited()

    payloads.reset_mock()
    assert await recovery.reconcile_after_restart(0.0, lost_payloads=False) == [
        {"qid": "t"}
    ]
    payloads.assert_not_awaited()
    tasks.assert_awaited_once()


async def test_one_shared_redis_restarting_runs_both_sweeps_as_before(
    alerted, sweep, mocker
):
    mocker.patch.object(recovery.time, "time", return_value=500.0)
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_single("run-a"))

    assert await watcher.observe(_single("run-b"))
    await watcher._sweep

    # called exactly as it was before instances existed: everything, and wait
    # on the broker client
    sweep.assert_awaited_once_with(500.0)
    event = alerted["record"].await_args.args[0]
    assert event["rule"] == "redis_restarted"
    assert event["detail"] == "broker Redis restarted (up 5s)"
    assert event["message"].startswith("Broker `redis` restarted and came back from")
    assert "instance" not in event


async def test_both_instances_restarting_together_is_one_sweep_for_both(
    alerted, data_client, mocker
):
    """Two sweeps side by side could each finish the same query: the task
    sweep relies on the payload sweep's finish_query tasks already being
    queued. So a second instance's restart folds into the pending sweep."""
    started = []

    async def _slow_sweep(detected_at, **scope):
        started.append((detected_at, scope))
        await recovery.asyncio.Event().wait()

    mocker.patch.object(recovery, "reconcile_after_restart", _slow_sweep)
    clock = mocker.patch.object(recovery.time, "time", return_value=100.0)
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_split("broker-a", "data-a"))

    # one snapshot shows both restarted (a node reboot)
    clock.return_value = 200.0
    assert await watcher.observe(_split("broker-b", "data-b"))
    await recovery.asyncio.sleep(0)

    assert [e.args[0]["rule"] for e in alerted["record"].await_args_list] == [
        "redis_restarted",
        f"redis_restarted@{DATA_INSTANCE}",
    ]
    # only the last one scheduled ever starts, and it covers both instances
    assert started == [(200.0, {"wait_for": [recovery.broker_client, data_client]})]
    watcher.cancel()


async def test_a_later_restart_of_the_other_instance_reschedules_one_sweep(
    alerted, data_client, mocker
):
    started = []

    async def _slow_sweep(detected_at, **scope):
        started.append((detected_at, scope))
        await recovery.asyncio.Event().wait()

    mocker.patch.object(recovery, "reconcile_after_restart", _slow_sweep)
    clock = mocker.patch.object(recovery.time, "time", return_value=100.0)
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_split("broker-a", "data-a"))

    await watcher.observe(_split("broker-a", "data-b"))
    first = watcher._sweep
    await recovery.asyncio.sleep(0)
    clock.return_value = 130.0
    await watcher.observe(_split("broker-b", "data-b"))
    await recovery.asyncio.sleep(0)

    assert first.cancelled()
    assert started == [
        (100.0, {"lost_tasks": False, "wait_for": [data_client]}),
        # from the first restart, now for what either instance lost
        (100.0, {"wait_for": [data_client, recovery.broker_client]}),
    ]
    watcher.cancel()


async def test_an_instance_holding_only_locks_or_logs_alerts_without_a_sweep(
    alerted, sweep
):
    def _snapshot(lock_run):
        broker_stats = {
            "instance": BROKER_INSTANCE,
            "roles": ["broker", "data", "logs"],
            "run_id": "broker-a",
        }
        lock_stats = {
            "instance": "shepherd_lock:6379",
            "roles": ["lock"],
            "run_id": lock_run,
            "uptime_in_seconds": 2,
        }
        return {
            "redis": broker_stats,
            "redis_instances": {
                BROKER_INSTANCE: broker_stats,
                "shepherd_lock:6379": lock_stats,
            },
        }

    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_snapshot("lock-a"))

    assert await watcher.observe(_snapshot("lock-b"))

    event = alerted["record"].await_args.args[0]
    assert event["rule"] == "redis_restarted@shepherd_lock:6379"
    assert "merge locks" in event["message"]
    assert "finished as errors" not in event["message"]
    sweep.assert_not_called()
    assert watcher._sweep is None


# --- remembering each instance's run_id -------------------------------------------


async def test_each_instance_has_its_own_run_id_key(alerted, sweep, broker):
    watcher = recovery.RedisRestartWatcher()

    await watcher.observe(_split("broker-a", "data-a"))

    # the broker role's instance keeps the key it always had
    assert await broker.get(recovery.RUN_ID_KEY) == "broker-a"
    assert await broker.get(f"{recovery.RUN_ID_KEY}:{DATA_INSTANCE}") == "data-a"
    alerted["record"].assert_not_awaited()


async def test_a_data_restart_is_spotted_across_a_monitor_restart(
    alerted, sweep, broker
):
    await recovery.RedisRestartWatcher().observe(_split("broker-a", "data-a"))

    restarted_monitor = recovery.RedisRestartWatcher()
    assert await restarted_monitor.observe(_split("broker-a", "data-b"))
    await restarted_monitor._sweep

    assert sweep.await_args.kwargs["lost_tasks"] is False


async def test_moving_a_role_to_its_own_redis_is_not_a_restart(alerted, sweep):
    """The new instance's run_id is new to the monitor, but nothing restarted."""
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_single("run-a"))

    assert not await watcher.observe(_split("run-a", "data-a"))

    alerted["record"].assert_not_awaited()
    sweep.assert_not_called()


async def test_an_unreachable_instance_is_not_a_restart(alerted, sweep):
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_split("broker-a", "data-a"))
    down = _split("broker-a", "data-a")
    down["redis_instances"][DATA_INSTANCE] = {
        "instance": DATA_INSTANCE,
        "roles": ["data"],
        "error": "down",
    }

    assert not await watcher.observe(down)
    # back with the run_id it had: it was only unreachable
    assert not await watcher.observe(_split("broker-a", "data-a"))

    sweep.assert_not_called()


# --- waiting for the restarted instance to load -----------------------------------


async def test_the_sweep_waits_for_the_instance_that_restarted_to_load(
    data_client, mocker
):
    data_client.info = AsyncMock(side_effect=[{"loading": 1}, {"loading": 0}])
    broker_info = mocker.patch.object(recovery.broker_client, "info", AsyncMock())
    sleep = mocker.patch.object(recovery.asyncio, "sleep", AsyncMock())

    await recovery._wait_until_loaded([data_client])

    assert data_client.info.await_count == 2
    sleep.assert_awaited_once_with(recovery.LOADING_POLL_SEC)
    # the broker's Redis did not restart, so nobody waits on it
    broker_info.assert_not_awaited()


# --- failover (Sentinel) ----------------------------------------------------------

OLD_REPLID = "a" * 40
NEW_REPLID = "b" * 40


async def test_a_promoted_replica_is_reported_as_a_failover_and_still_swept(
    alerted, sweep, mocker
):
    mocker.patch.object(recovery.time, "time", return_value=500.0)
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(
        _single("run-a", sentinel=True, master_replid=OLD_REPLID, master_replid2="0")
    )

    # another server answers as the primary, carrying on the old one's history
    promoted = _single(
        "run-b",
        sentinel=True,
        master_replid=NEW_REPLID,
        master_replid2=OLD_REPLID,
        uptime_in_seconds=86_400,
    )
    assert await watcher.observe(promoted)
    await watcher._sweep

    event = alerted["record"].await_args.args[0]
    assert event["rule"] == "redis_failover"
    assert event["detail"] == "Redis failed over to a new primary (up 86400s)"
    assert "had not replicated yet" in event["message"]
    # unreplicated writes are lost like unsaved ones: the same sweep runs
    sweep.assert_awaited_once_with(500.0)


async def test_a_sentinel_instance_whose_run_id_changed_restarted_or_failed_over(
    alerted, sweep
):
    """Without a replication id linking the new primary to the old, a restart
    and a failover look the same; the alert says so."""
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_single("run-a", sentinel=True, master_replid=OLD_REPLID))

    restarted = _single(
        "run-b", sentinel=True, master_replid=NEW_REPLID, master_replid2="0" * 40
    )
    assert await watcher.observe(restarted)
    await watcher._sweep

    event = alerted["record"].await_args.args[0]
    assert event["rule"] == "redis_restarted"
    assert event["detail"] == "Redis restarted or failed over (up 5s)"
    assert "restarted or failed over" in event["message"]
    sweep.assert_awaited_once()


async def test_a_failover_of_the_data_instance_sweeps_for_lost_payloads(
    alerted, sweep, data_client, mocker
):
    mocker.patch.object(recovery.time, "time", return_value=500.0)
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(
        _split("broker-a", "data-a", sentinel=True, master_replid=OLD_REPLID)
    )

    await watcher.observe(
        _split(
            "broker-a",
            "data-b",
            sentinel=True,
            master_replid=NEW_REPLID,
            master_replid2=OLD_REPLID,
        )
    )
    await watcher._sweep

    event = alerted["record"].await_args.args[0]
    assert event["rule"] == f"redis_failover@{DATA_INSTANCE}"
    sweep.assert_awaited_once_with(500.0, lost_tasks=False, wait_for=[data_client])


async def test_a_directly_reached_redis_restarting_is_never_called_a_failover(
    alerted, sweep
):
    watcher = recovery.RedisRestartWatcher()
    await watcher.observe(_single("run-a", master_replid=OLD_REPLID))

    await watcher.observe(
        _single("run-b", master_replid=NEW_REPLID, master_replid2="0" * 40)
    )
    await watcher._sweep

    event = alerted["record"].await_args.args[0]
    assert event["rule"] == "redis_restarted"
    assert event["detail"] == "broker Redis restarted (up 5s)"

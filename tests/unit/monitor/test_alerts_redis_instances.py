"""The alert engine watches every Redis instance on its own (workers.monitor.alerts).

With one Redis -- the default -- its alerts are named and worded as they always
were (test_alerts.py is that oracle). With roles moved onto Redis servers of
their own, each instance has its own alert state, and the alerts of an instance
other than the broker role's carry its name, e.g.
``redis_memory_high@shepherd_data:6379``.

Also here, per instance: the replication alerts, the eviction-window alert,
and the persistence alert that says which kind of save failed (#201).
"""

from unittest.mock import AsyncMock

import pytest

from shepherd_utils.config import settings
from workers.monitor import alerts
from workers.monitor.alerts import AlertEngine, Rule

BROKER_INSTANCE = "shepherd_broker:6379"
DATA_INSTANCE = "shepherd_data:6379"


@pytest.fixture
def patched(redis_mock, mocker):
    """Wire the alert engine onto fakeredis and stub out delivery + archive."""
    mocker.patch.object(alerts, "broker_client", redis_mock["broker"])
    archive = mocker.patch("workers.monitor.storage.insert_event", AsyncMock())
    dispatch = mocker.patch.object(alerts, "dispatch", AsyncMock())
    return {"dispatch": dispatch, "archive": archive, "broker": redis_mock["broker"]}


@pytest.fixture
def clock(mocker):
    m = mocker.patch.object(alerts, "time")
    m.time.return_value = 10_000.0
    return m.time


def _fired(patched):
    """Every alert dispatched so far, oldest first."""
    return [call.args[0] for call in patched["dispatch"].call_args_list]


def _rules(patched):
    return [event["rule"] for event in _fired(patched)]


def _single(**redis):
    """A snapshot of the default deployment: one Redis serving every role."""
    stats = {
        "instance": BROKER_INSTANCE,
        "roles": ["broker", "data", "lock", "logs"],
        "role": "master",
        **redis,
    }
    return {
        "ts": 1.0,
        "events": [],
        "streams": {},
        "postgres": {},
        "redis": stats,
        "redis_instances": {BROKER_INSTANCE: stats},
    }


def _split(broker=None, data=None, ts=1.0):
    """A snapshot with the data role on a Redis of its own."""
    broker_stats = {
        "instance": BROKER_INSTANCE,
        "roles": ["broker", "lock", "logs"],
        "role": "master",
        **(broker or {}),
    }
    data_stats = {
        "instance": DATA_INSTANCE,
        "roles": ["data"],
        "role": "master",
        **(data or {}),
    }
    return {
        "ts": ts,
        "events": [],
        "streams": {},
        "postgres": {},
        "redis": broker_stats,
        "redis_instances": {BROKER_INSTANCE: broker_stats, DATA_INSTANCE: data_stats},
    }


def _mem(pct):
    return {"used_memory_bytes": pct, "maxmemory_bytes": 100}


async def _hold(handler, snapshot, clock, start, grace=130):
    """Show ``handler`` the same snapshot at ``start`` and again past the grace."""
    clock.return_value = start
    await handler(snapshot)
    clock.return_value = start + grace
    await handler(snapshot)
    return start + grace


# --- one Redis: nothing changes --------------------------------------------------


async def test_one_redis_alerts_under_the_names_and_words_it_always_had(patched, clock):
    """The same alert whether the snapshot carries the per-instance view
    (what the poller builds now) or only ``redis`` (what it built before)."""
    with_instances = _single(**_mem(88))
    legacy = {k: v for k, v in with_instances.items() if k != "redis_instances"}
    events = []
    for snapshot in (with_instances, legacy):
        patched["dispatch"].reset_mock()
        await _hold(AlertEngine([]).handle_redis_memory, snapshot, clock, 1_000.0)
        events.append(patched["dispatch"].call_args.args[0])

    assert events[0] == events[1]
    assert events[0] == {
        "ts": 1_130.0,
        "rule": "redis_memory_high",
        "severity": "warning",
        "detail": "Redis memory 88.0% of maxmemory (>= 85%)",
        "message": (
            "Redis broker memory is at 88.0% of its maxmemory (>= 85%). "
            "Eviction and OOM-kill risk are near -- shed load, raise the cap, "
            "or lower redis_ttl."
        ),
    }


async def test_one_redis_evicting_keeps_its_rule_name_cooldown_key_and_text(
    patched, clock
):
    rule = Rule({"name": "redis_evicting", "type": "redis_eviction", "threshold": 0})
    engine = AlertEngine([rule])

    fired = await engine.evaluate(_single(evicted_keys_delta=4))

    assert fired == [
        {
            "ts": 1.0,
            "rule": "redis_evicting",
            "severity": "warning",
            "detail": (
                "Redis evicted 4 key(s) in the last interval -- broker is at "
                "its maxmemory cap and shedding data"
            ),
            "message": (
                "Redis evicted 4 key(s) in the last interval -- broker is at "
                "its maxmemory cap and shedding data"
            ),
        }
    ]
    assert await patched["broker"].keys("alert:cooldown:*") == [
        "alert:cooldown:redis_evicting"
    ]


# --- memory pressure, per instance ----------------------------------------------


async def test_the_data_instance_alerts_on_its_own_memory(patched, clock):
    engine = AlertEngine([])

    await _hold(
        engine.handle_redis_memory,
        _split(broker=_mem(20), data=_mem(97)),
        clock,
        1_000.0,
    )

    (event,) = _fired(patched)
    assert event["rule"] == f"redis_memory_critical@{DATA_INSTANCE}"
    assert event["severity"] == "critical"
    # says which instance, and what it is used for
    assert f"`{DATA_INSTANCE}` (data)" in event["message"]
    assert f"`{DATA_INSTANCE}` (data)" in event["detail"]
    assert event["instance"] == DATA_INSTANCE and event["roles"] == ["data"]


async def test_the_broker_instance_keeps_its_alert_name_when_split(patched, clock):
    engine = AlertEngine([])

    await _hold(
        engine.handle_redis_memory,
        _split(broker=_mem(88), data=_mem(20)),
        clock,
        1_000.0,
    )

    (event,) = _fired(patched)
    # the name alert routing already knows ...
    assert event["rule"] == "redis_memory_high"
    # ... but with two instances the text has to say which one it means
    assert f"`{BROKER_INSTANCE}` (broker, lock, logs)" in event["message"]


async def test_each_instance_has_its_own_grace_and_cooldown(patched, clock):
    """The broker's alert being in its day-long cooldown must not swallow the
    data instance's first alert, nor restart the broker's own."""
    engine = AlertEngine([])
    fired_at = await _hold(
        engine.handle_redis_memory,
        _split(broker=_mem(88), data=_mem(20)),
        clock,
        1_000.0,
    )
    assert _rules(patched) == ["redis_memory_high"]

    # an hour on the data instance fills up too: it gets its own grace, then alerts
    both = _split(broker=_mem(88), data=_mem(88))
    clock.return_value = fired_at + 3_600
    await engine.handle_redis_memory(both)
    assert _rules(patched) == ["redis_memory_high"]
    clock.return_value = fired_at + 3_600 + 130
    await engine.handle_redis_memory(both)
    assert _rules(patched) == [
        "redis_memory_high",
        f"redis_memory_high@{DATA_INSTANCE}",
    ]

    # a day after the broker's alert, only the broker's re-notifies
    clock.return_value = fired_at + 86_400 + 1
    await engine.handle_redis_memory(both)
    assert _rules(patched)[2:] == ["redis_memory_high"]


async def test_one_instance_recovering_does_not_clear_the_other(patched, clock):
    engine = AlertEngine([])
    await _hold(
        engine.handle_redis_memory,
        _split(broker=_mem(88), data=_mem(97)),
        clock,
        1_000.0,
    )
    patched["dispatch"].reset_mock()

    await _hold(
        engine.handle_redis_memory,
        _split(broker=_mem(88), data=_mem(30)),
        clock,
        2_000.0,
    )

    assert _rules(patched) == [f"redis_memory_recovered@{DATA_INSTANCE}"]
    assert engine._redis_mem[""].level == "warning"


async def test_the_brink_skips_the_grace_on_whichever_instance_reaches_it(
    patched, clock
):
    engine = AlertEngine([])

    await engine.handle_redis_memory(_split(broker=_mem(20), data=_mem(99)))

    assert _rules(patched) == [f"redis_memory_critical@{DATA_INSTANCE}"]


async def test_an_unreachable_instance_keeps_its_state_and_the_other_still_alerts(
    patched, clock
):
    engine = AlertEngine([])
    await _hold(
        engine.handle_redis_memory,
        _split(broker=_mem(20), data=_mem(88)),
        clock,
        1_000.0,
    )
    assert _rules(patched) == [f"redis_memory_high@{DATA_INSTANCE}"]

    # the data Redis stops answering while the broker's fills up
    await _hold(
        engine.handle_redis_memory,
        _split(broker=_mem(97), data={"error": "down"}),
        clock,
        2_000.0,
    )
    assert _rules(patched)[1:] == ["redis_memory_critical"]

    # back, still high: no "recovered", and no second first-alert
    await _hold(
        engine.handle_redis_memory,
        _split(broker=_mem(97), data=_mem(88)),
        clock,
        3_000.0,
    )
    assert len(_fired(patched)) == 2


# --- eviction, per instance -------------------------------------------------------


def _eviction_rule(**extra):
    return Rule(
        {"name": "redis_evicting", "type": "redis_eviction", "threshold": 0, **extra}
    )


async def test_eviction_on_the_data_instance_alerts_under_its_own_name(patched, clock):
    engine = AlertEngine([_eviction_rule()])

    fired = await engine.evaluate(
        _split(broker={"evicted_keys_delta": 0}, data={"evicted_keys_delta": 9})
    )

    (event,) = fired
    assert event["rule"] == f"redis_evicting@{DATA_INSTANCE}"
    assert event["detail"] == (
        f"Redis `{DATA_INSTANCE}` (data) evicted 9 key(s) in the last interval "
        "-- it is at its maxmemory cap and shedding data"
    )
    assert await patched["broker"].keys("alert:cooldown:*") == [
        f"alert:cooldown:redis_evicting@{DATA_INSTANCE}"
    ]


async def test_one_instances_eviction_cooldown_does_not_mask_the_others(patched, clock):
    engine = AlertEngine([_eviction_rule()])
    first = await engine.evaluate(_split(broker={"evicted_keys_delta": 3}))
    assert [e["rule"] for e in first] == ["redis_evicting"]

    # the broker's alert is now in cooldown; the data instance starts evicting
    second = await engine.evaluate(
        _split(broker={"evicted_keys_delta": 3}, data={"evicted_keys_delta": 9}, ts=4.0)
    )

    assert [e["rule"] for e in second] == [f"redis_evicting@{DATA_INSTANCE}"]


async def test_one_instance_going_quiet_does_not_reset_the_others_streak(
    patched, clock
):
    """``duration`` is a continuous breach; the broker's streak ending must
    not restart the data instance's."""
    engine = AlertEngine([_eviction_rule(duration="30s")])
    evicting = {"evicted_keys_delta": 2}

    assert not await engine.evaluate(_split(broker=evicting, data=evicting, ts=100.0))
    assert not await engine.evaluate(_split(broker={}, data=evicting, ts=115.0))
    fired = await engine.evaluate(_split(broker=evicting, data=evicting, ts=131.0))

    # 31s for the data instance; the broker's began again at 131
    assert [e["rule"] for e in fired] == [f"redis_evicting@{DATA_INSTANCE}"]


async def test_a_configured_message_still_says_which_instance(patched, clock):
    engine = AlertEngine([_eviction_rule(message="Redis is shedding data.")])

    (event,) = await engine.evaluate(_split(data={"evicted_keys_delta": 1}))

    assert event["message"].startswith("Redis is shedding data. (Redis `shepherd_data")


async def test_only_broker_instance_eviction_suppresses_worker_down_alerts(
    patched, clock
):
    """Heartbeat keys live on the broker role's Redis; the data Redis evicting
    payloads cannot make a live worker read as down."""
    engine = AlertEngine([])

    engine._note_redis_eviction(_split(data={"evicted_keys_delta": 50}))
    assert not engine.in_redis_eviction_grace

    engine._note_redis_eviction(_split(broker={"evicted_keys_delta": 1}))
    assert engine.in_redis_eviction_grace


async def test_an_instance_alert_is_archived_without_a_worker(patched, clock):
    """The archive reads a worker out of names like ``worker_crashed:arax``;
    an instance name has a colon of its own and is not one."""
    engine = AlertEngine([])

    await engine.handle_redis_memory(_split(data=_mem(99)))

    assert patched["archive"].await_args.kwargs["worker"] is None


# --- persistence: which save failed, and what it costs (#201) --------------------


async def test_a_failed_aof_write_is_critical_and_says_writes_are_not_persisted(
    patched, clock
):
    engine = AlertEngine([])

    await engine.handle_redis_persistence(_single(aof_last_write_status="err"))

    (event,) = _fired(patched)
    assert event["rule"] == "redis_persistence_failing"
    assert event["severity"] == "critical"
    assert event["detail"] == "Redis AOF write failing"
    assert "AOF write failing: writes are not being persisted" in event["message"]
    assert event["message"].startswith("Broker `redis` persistence: ")


async def test_a_failed_aof_rewrite_alone_is_a_warning_that_no_data_is_lost(
    patched, clock
):
    engine = AlertEngine([])

    await engine.handle_redis_persistence(
        _single(aof_last_write_status="ok", aof_last_bgrewrite_status="err")
    )

    (event,) = _fired(patched)
    assert event["rule"] == "redis_persistence_failing"
    assert event["severity"] == "warning"
    assert event["detail"] == "Redis AOF rewrite failed"
    assert (
        "AOF rewrite failed: no data lost yet, but the AOF will keep growing"
        in event["message"]
    )
    assert "not being persisted" not in event["message"]


async def test_a_failed_rdb_snapshot_says_so_and_whether_the_aof_covers_it(
    patched, clock
):
    without_aof, with_aof = AlertEngine([]), AlertEngine([])

    await without_aof.handle_redis_persistence(
        _single(aof_enabled=0, rdb_last_bgsave_status="err")
    )
    await with_aof.handle_redis_persistence(
        _single(aof_enabled=1, rdb_last_bgsave_status="err")
    )

    bare, covered = _fired(patched)
    assert bare["detail"] == covered["detail"] == "Redis RDB snapshot failed"
    assert bare["severity"] == covered["severity"] == "critical"
    assert "loses everything written since the last good snapshot" in bare["message"]
    assert "the append-only file still holds the data" in covered["message"]


async def test_a_rewrite_failure_that_becomes_a_write_failure_alerts_again(
    patched, clock
):
    engine = AlertEngine([])
    rewrite = _single(aof_last_bgrewrite_status="err")
    both = _single(aof_last_bgrewrite_status="err", aof_last_write_status="err")

    await engine.handle_redis_persistence(rewrite)
    await engine.handle_redis_persistence(rewrite)
    await engine.handle_redis_persistence(both)
    await engine.handle_redis_persistence(both)

    assert [(e["rule"], e["severity"]) for e in _fired(patched)] == [
        ("redis_persistence_failing", "warning"),
        ("redis_persistence_failing", "critical"),
    ]
    assert _fired(patched)[1]["detail"] == (
        "Redis AOF write failing, AOF rewrite failed"
    )

    await engine.handle_redis_persistence(_single(aof_last_write_status="ok"))
    assert _rules(patched)[2:] == ["redis_persistence_recovered"]


async def test_persistence_is_tracked_per_instance(patched, clock):
    engine = AlertEngine([])
    data_failing = _split(
        broker={"aof_last_write_status": "ok"},
        data={"aof_last_write_status": "err"},
    )

    await engine.handle_redis_persistence(data_failing)
    await engine.handle_redis_persistence(data_failing)

    (event,) = _fired(patched)
    assert event["rule"] == f"redis_persistence_failing@{DATA_INSTANCE}"
    assert event["message"].startswith(f"Redis `{DATA_INSTANCE}` (data) persistence: ")

    # the broker's failing now is a separate alert; the data one stays as it is
    await engine.handle_redis_persistence(
        _split(
            broker={"rdb_last_bgsave_status": "err"},
            data={"aof_last_write_status": "err"},
        )
    )
    assert _rules(patched)[1:] == ["redis_persistence_failing"]

    # an unreachable data instance is neither failing nor recovered
    await engine.handle_redis_persistence(
        _split(broker={"rdb_last_bgsave_status": "ok"}, data={"error": "down"})
    )
    assert _rules(patched)[2:] == ["redis_persistence_recovered"]


# --- replication (#201) -----------------------------------------------------------


def _replica(addr="10.0.0.2:6379", state="online", lag_sec=0, lag_bytes=0):
    return {"addr": addr, "state": state, "lag_sec": lag_sec, "lag_bytes": lag_bytes}


def _replicated(*replicas):
    return {"connected_replicas": len(replicas), "replicas": list(replicas)}


async def test_a_redis_that_never_had_replicas_never_alerts(patched, clock):
    engine = AlertEngine([])

    await _hold(engine.handle_redis_replication, _single(**_replicated()), clock, 1_000)
    await _hold(engine.handle_redis_replication, _single(**_replicated()), clock, 9_000)

    patched["dispatch"].assert_not_called()


async def test_a_lost_replica_alerts_after_the_grace_and_recovers(
    patched, clock, monkeypatch
):
    monkeypatch.setattr(settings, "monitor_redis_replica_grace_sec", 60.0)
    engine = AlertEngine([])
    two = _single(**_replicated(_replica(), _replica("10.0.0.3:6379")))
    one = _single(**_replicated(_replica()))

    clock.return_value = 1_000.0
    await engine.handle_redis_replication(two)
    # a replica drops: silent inside the grace (it may just be reconnecting)
    clock.return_value = 1_010.0
    await engine.handle_redis_replication(one)
    clock.return_value = 1_050.0
    await engine.handle_redis_replication(one)
    patched["dispatch"].assert_not_called()
    clock.return_value = 1_071.0
    await engine.handle_redis_replication(one)

    (event,) = _fired(patched)
    assert event["rule"] == "redis_replicas_lost"
    assert event["severity"] == "warning"
    assert event["detail"] == "Redis has 1 of 2 replicas connected"

    # still one: no repeat. Back to two for the grace: recovered.
    clock.return_value = 2_000.0
    await engine.handle_redis_replication(one)
    assert len(_fired(patched)) == 1
    await _hold(engine.handle_redis_replication, two, clock, 3_000.0)
    assert _rules(patched)[1:] == ["redis_replicas_recovered"]


async def test_a_replica_that_reconnects_within_the_grace_is_silent(patched, clock):
    engine = AlertEngine([])
    one = _single(**_replicated(_replica()))
    none = _single(**_replicated())

    clock.return_value = 1_000.0
    await engine.handle_redis_replication(one)
    clock.return_value = 1_003.0
    await engine.handle_redis_replication(none)
    clock.return_value = 1_030.0
    await engine.handle_redis_replication(one)
    # long after: the earlier blip must not count towards a later one
    clock.return_value = 5_000.0
    await engine.handle_redis_replication(none)

    patched["dispatch"].assert_not_called()


async def test_losing_every_replica_is_critical(patched, clock):
    engine = AlertEngine([])
    await engine.handle_redis_replication(_single(**_replicated(_replica())))

    await _hold(engine.handle_redis_replication, _single(**_replicated()), clock, 2_000)

    (event,) = _fired(patched)
    assert event["rule"] == "redis_replicas_lost"
    assert event["severity"] == "critical"
    assert "no redundancy left" in event["message"]


async def test_a_partial_loss_that_becomes_total_alerts_once_more(patched, clock):
    engine = AlertEngine([])
    two = _single(**_replicated(_replica(), _replica("10.0.0.3:6379")))
    await engine.handle_redis_replication(two)
    await _hold(
        engine.handle_redis_replication,
        _single(**_replicated(_replica())),
        clock,
        2_000,
    )

    none = _single(**_replicated())
    await _hold(engine.handle_redis_replication, none, clock, 3_000)

    assert [(e["rule"], e["severity"]) for e in _fired(patched)] == [
        ("redis_replicas_lost", "warning"),
        ("redis_replicas_lost", "critical"),
    ]


async def test_a_lagging_replica_alerts_and_catches_up(patched, clock, monkeypatch):
    monkeypatch.setattr(settings, "monitor_redis_replica_lag_sec", 30.0)
    engine = AlertEngine([])
    behind = _single(**_replicated(_replica(lag_sec=45)))
    level = _single(**_replicated(_replica(lag_sec=1)))

    await _hold(engine.handle_redis_replication, behind, clock, 1_000.0)

    (event,) = _fired(patched)
    assert event["rule"] == "redis_replica_lagging"
    assert event["severity"] == "warning"
    assert "10.0.0.2:6379 last heard from 45s ago" in event["detail"]

    await _hold(engine.handle_redis_replication, level, clock, 2_000.0)
    assert _rules(patched)[1:] == ["redis_replica_caught_up"]


async def test_a_replica_far_behind_in_bytes_or_still_syncing_counts_as_lagging(
    patched, clock, monkeypatch
):
    monkeypatch.setattr(settings, "monitor_redis_replica_lag_bytes", 1_000)
    far_behind, syncing = AlertEngine([]), AlertEngine([])

    await _hold(
        far_behind.handle_redis_replication,
        _single(**_replicated(_replica(lag_bytes=5 * 2**20))),
        clock,
        1_000.0,
    )
    await _hold(
        syncing.handle_redis_replication,
        _single(**_replicated(_replica(state="wait_bgsave"))),
        clock,
        2_000.0,
    )

    first, second = _fired(patched)
    assert first["rule"] == second["rule"] == "redis_replica_lagging"
    assert "5 MiB behind" in first["detail"]
    assert "state wait_bgsave" in second["detail"]


async def test_a_brief_lag_spike_and_a_healthy_replica_are_silent(patched, clock):
    engine = AlertEngine([])
    behind = _single(**_replicated(_replica(lag_sec=45)))
    level = _single(**_replicated(_replica(lag_sec=1, lag_bytes=4_096)))

    clock.return_value = 1_000.0
    await engine.handle_redis_replication(behind)
    clock.return_value = 1_003.0
    await engine.handle_redis_replication(level)
    await _hold(engine.handle_redis_replication, level, clock, 2_000.0)

    patched["dispatch"].assert_not_called()


async def test_replication_is_tracked_per_instance(patched, clock):
    engine = AlertEngine([])
    healthy = _split(broker=_replicated(), data=_replicated(_replica()))
    await engine.handle_redis_replication(healthy)

    await _hold(
        engine.handle_redis_replication,
        _split(broker=_replicated(), data=_replicated()),
        clock,
        2_000.0,
    )

    # the broker's Redis never had a replica, so only the data one alerts
    (event,) = _fired(patched)
    assert event["rule"] == f"redis_replicas_lost@{DATA_INSTANCE}"
    assert f"`{DATA_INSTANCE}` (data)" in event["message"]


async def test_a_connection_to_a_replica_says_nothing_about_replicas(patched, clock):
    """A replica reports zero replicas of its own; only a primary's count is
    the instance's redundancy."""
    engine = AlertEngine([])
    await engine.handle_redis_replication(_single(**_replicated(_replica())))

    demoted = _single(**_replicated())
    demoted["redis"]["role"] = "slave"
    await _hold(engine.handle_redis_replication, demoted, clock, 2_000.0)

    patched["dispatch"].assert_not_called()


# --- eviction window (#201) -------------------------------------------------------


def _window(pct, window_sec, rate=2**20):
    return {
        "used_memory_bytes": pct,
        "maxmemory_bytes": 100,
        "maxmemory_pct": float(pct),
        "write_bytes_per_sec": rate,
        "eviction_window_sec": window_sec,
    }


def test_the_shortest_safe_eviction_window_is_the_longest_query_budget_doubled(
    monkeypatch,
):
    monkeypatch.setattr(settings, "ars_timeout_merge_sec", 480.0)
    monkeypatch.setattr(settings, "monitor_redis_eviction_window_min_sec", 0.0)
    monkeypatch.setattr(settings, "monitor_redis_eviction_window_safety_factor", 2.0)
    assert alerts.eviction_window_min_sec() == 960.0

    monkeypatch.setattr(settings, "monitor_redis_eviction_window_min_sec", 300.0)
    assert alerts.eviction_window_min_sec() == 300.0


async def test_a_short_eviction_window_near_the_cap_alerts_after_the_grace(
    patched, clock
):
    engine = AlertEngine([])
    snapshot = _single(**_window(pct=95, window_sec=300))

    clock.return_value = 1_000.0
    await engine.handle_redis_eviction_window(snapshot)
    clock.return_value = 1_060.0
    await engine.handle_redis_eviction_window(snapshot)
    patched["dispatch"].assert_not_called()
    clock.return_value = 1_130.0
    await engine.handle_redis_eviction_window(snapshot)

    (event,) = _fired(patched)
    assert event["rule"] == "redis_eviction_window_short"
    assert event["severity"] == "critical"
    assert event["detail"] == (
        "Redis eviction window 5.0 min (< 16.0 min) at 95.0% of maxmemory"
    )
    assert "1.0 MiB/s of writes" in event["message"]

    # and it reports the all-clear once the window is long enough again
    await _hold(
        engine.handle_redis_eviction_window,
        _single(**_window(pct=95, window_sec=3_600)),
        clock,
        2_000.0,
    )
    assert _rules(patched)[1:] == ["redis_eviction_window_recovered"]


async def test_a_short_window_far_from_the_cap_is_not_a_problem_yet(patched, clock):
    """Nothing is evicted until the instance is full; a fast write rate into
    a half-empty Redis is ordinary load."""
    engine = AlertEngine([])

    await _hold(
        engine.handle_redis_eviction_window,
        _single(**_window(pct=50, window_sec=60)),
        clock,
        1_000.0,
    )

    patched["dispatch"].assert_not_called()


async def test_a_full_redis_with_a_long_window_is_not_an_eviction_window_alert(
    patched, clock
):
    engine = AlertEngine([])

    await _hold(
        engine.handle_redis_eviction_window,
        _single(**_window(pct=99, window_sec=7_200)),
        clock,
        1_000.0,
    )

    patched["dispatch"].assert_not_called()


async def test_one_burst_does_not_trip_the_eviction_window_alert(patched, clock):
    engine = AlertEngine([])
    burst = _single(**_window(pct=95, window_sec=60))
    calm = _single(**_window(pct=95, window_sec=7_200))

    clock.return_value = 1_000.0
    await engine.handle_redis_eviction_window(burst)
    clock.return_value = 1_003.0
    await engine.handle_redis_eviction_window(calm)
    clock.return_value = 1_200.0
    await engine.handle_redis_eviction_window(calm)

    patched["dispatch"].assert_not_called()


async def test_an_uncapped_or_idle_redis_has_no_eviction_window_to_alert_on(
    patched, clock
):
    engine = AlertEngine([])
    no_window = _single(
        used_memory_bytes=5,
        maxmemory_bytes=0,
        maxmemory_pct=None,
        eviction_window_sec=None,
    )

    await _hold(engine.handle_redis_eviction_window, no_window, clock, 1_000.0)

    patched["dispatch"].assert_not_called()


async def test_the_eviction_window_is_judged_per_instance(patched, clock):
    engine = AlertEngine([])

    await _hold(
        engine.handle_redis_eviction_window,
        _split(
            broker=_window(pct=30, window_sec=30),
            data=_window(pct=96, window_sec=120),
        ),
        clock,
        1_000.0,
    )

    (event,) = _fired(patched)
    assert event["rule"] == f"redis_eviction_window_short@{DATA_INSTANCE}"
    assert f"`{DATA_INSTANCE}` (data)" in event["message"]

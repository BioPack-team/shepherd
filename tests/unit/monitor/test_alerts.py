"""Tests for the monitor alert engine's worker-down coalescing.

When several workers drop to zero close together (the classic case is a laptop
going to sleep, expiring every heartbeat at once), the engine should buffer the
down-alerts for ``monitor_down_debounce_sec`` and deliver a single combined
Slack/email message instead of one per worker. Unrelated alerts (backlog /
threshold) must still dispatch immediately.
"""

from unittest.mock import AsyncMock

import pytest

from workers.monitor import alerts
from workers.monitor.alerts import AlertEngine, Rule


@pytest.fixture
def patched(redis_mock, mocker):
    """Wire the alert engine onto fakeredis and stub out delivery + archive."""
    mocker.patch.object(alerts, "broker_client", redis_mock["broker"])
    # _record_alert archives to Postgres; keep it off the real DB.
    mocker.patch("workers.monitor.storage.insert_event", AsyncMock())
    dispatch = mocker.patch.object(alerts, "dispatch", AsyncMock())
    dispatch_batch = mocker.patch.object(alerts, "dispatch_batch", AsyncMock())
    return {"dispatch": dispatch, "dispatch_batch": dispatch_batch}


def _scale_down(worker, kind="crashed", was=1):
    return {"type": "scale_down", "worker": worker, "from": was, "to": 0, "kind": kind}


def _engine(rules=None):
    engine = AlertEngine(rules or [])
    # Bypass the startup-grace window so worker-down events aren't suppressed.
    engine._boot_time = 0.0
    return engine


@pytest.fixture
def clock(mocker):
    """Deterministic control over the wall clock the engine reads.

    The engine's grace-window checks and the broker state machine call
    ``time.time()``; the rule-streak logic instead uses ``snapshot["ts"]``.
    Tests keep the two in sync by setting this clock to the same value as the
    snapshot ts they pass. Only ``workers.monitor.alerts``'s ``time`` reference
    is replaced, so nothing else is affected.
    """
    m = mocker.patch.object(alerts, "time")
    m.time.return_value = 10_000.0
    return m.time


def _worker_snapshot(ts, worker="arax", alive=0):
    return {
        "ts": ts,
        "events": [],
        "workers": {worker: {"alive": alive, "stale": 0}},
        "streams": {},
        "postgres": {},
    }


async def test_broker_down_fires_once_after_grace(patched, clock):
    engine = _engine()
    # Down edge starts the debounce clock; nothing fires yet.
    clock.return_value = 10_000.0
    await engine.handle_broker_health(False)
    patched["dispatch"].assert_not_called()
    # Still inside the 15s debounce window: silent.
    clock.return_value = 10_010.0
    await engine.handle_broker_health(False)
    patched["dispatch"].assert_not_called()
    # Past the debounce window: one broker_down alert.
    clock.return_value = 10_020.0
    await engine.handle_broker_health(False)
    patched["dispatch"].assert_called_once()
    assert patched["dispatch"].call_args.args[0]["rule"] == "broker_down"
    assert patched["dispatch"].call_args.args[0]["severity"] == "critical"
    # Continued outage does not re-fire.
    clock.return_value = 10_060.0
    await engine.handle_broker_health(False)
    patched["dispatch"].assert_called_once()


async def test_broker_blip_below_grace_stays_silent(patched, clock):
    engine = _engine()
    clock.return_value = 10_000.0
    await engine.handle_broker_health(False)
    # Recovers before the debounce window elapses: no down alert, and no
    # recovery alert either (we never announced an outage).
    clock.return_value = 10_005.0
    await engine.handle_broker_health(True)
    patched["dispatch"].assert_not_called()


async def test_broker_recovery_fires_recovered_alert(patched, clock):
    engine = _engine()
    clock.return_value = 10_000.0
    await engine.handle_broker_health(False)
    clock.return_value = 10_020.0
    await engine.handle_broker_health(False)  # broker_down
    clock.return_value = 10_030.0
    await engine.handle_broker_health(True)  # broker_recovered
    rules_fired = [c.args[0]["rule"] for c in patched["dispatch"].call_args_list]
    assert rules_fired == ["broker_down", "broker_recovered"]


async def test_scale_down_suppressed_while_broker_down(patched, clock):
    engine = _engine()
    engine._broker_up = False  # broker currently unreachable
    clock.return_value = 5_000.0
    await engine.evaluate(
        {"ts": 5_000.0, "events": [_scale_down("arax"), _scale_down("bte")]}
    )
    clock.return_value = 5_010.0
    await engine.evaluate({"ts": 5_010.0, "events": []})  # past flush window
    # The all-workers-down flood is suppressed entirely while the broker is down.
    patched["dispatch"].assert_not_called()
    patched["dispatch_batch"].assert_not_called()


async def test_scale_down_suppressed_during_recovery_grace(patched, clock):
    engine = _engine()
    engine._broker_recovered_at = 1_000.0  # grace window: 1000..1030
    clock.return_value = 1_010.0
    await engine.evaluate(
        {"ts": 1_010.0, "events": [_scale_down("arax"), _scale_down("bte")]}
    )
    clock.return_value = 1_020.0
    await engine.evaluate({"ts": 1_020.0, "events": []})
    patched["dispatch"].assert_not_called()
    patched["dispatch_batch"].assert_not_called()


async def test_postgres_down_fires_once_after_grace_then_recovers(patched, clock):
    engine = _engine()
    # Down edge starts the debounce clock; nothing fires yet.
    clock.return_value = 10_000.0
    await engine.handle_postgres_health(False)
    patched["dispatch"].assert_not_called()
    # Inside the 15s debounce window: still silent.
    clock.return_value = 10_010.0
    await engine.handle_postgres_health(False)
    patched["dispatch"].assert_not_called()
    # Past it: one postgres_down alert; no re-fire while it stays down.
    clock.return_value = 10_020.0
    await engine.handle_postgres_health(False)
    clock.return_value = 10_050.0
    await engine.handle_postgres_health(False)
    patched["dispatch"].assert_called_once()
    assert patched["dispatch"].call_args.args[0]["rule"] == "postgres_down"
    # Recovery announces once.
    clock.return_value = 10_060.0
    await engine.handle_postgres_health(True)
    rules = [c.args[0]["rule"] for c in patched["dispatch"].call_args_list]
    assert rules == ["postgres_down", "postgres_recovered"]


async def test_postgres_blip_below_grace_stays_silent(patched, clock):
    engine = _engine()
    clock.return_value = 10_000.0
    await engine.handle_postgres_health(False)
    clock.return_value = 10_005.0
    await engine.handle_postgres_health(True)
    patched["dispatch"].assert_not_called()


def _redis_snap(used, maxmem, evicted_delta=0):
    return {
        "ts": 1.0,
        "events": [],
        "streams": {},
        "postgres": {},
        "redis": {
            "used_memory_bytes": used,
            "maxmemory_bytes": maxmem,
            "evicted_keys_delta": evicted_delta,
        },
    }


def test_redis_memory_rule_fires_over_threshold():
    rule = Rule({"name": "mem", "type": "redis_memory", "threshold": 85})
    # 9/10 GB = 90% -> breach.
    assert rule.evaluate(_redis_snap(9_000_000_000, 10_000_000_000)) is not None
    # 8/10 GB = 80% -> clear.
    assert rule.evaluate(_redis_snap(8_000_000_000, 10_000_000_000)) is None
    # Uncapped broker (maxmemory 0) -> no-op regardless of usage.
    assert rule.evaluate(_redis_snap(9_000_000_000, 0)) is None


def test_redis_eviction_rule_fires_on_any_delta():
    rule = Rule({"name": "evict", "type": "redis_eviction", "threshold": 0})
    assert rule.evaluate(_redis_snap(1, 10, evicted_delta=0)) is None
    assert rule.evaluate(_redis_snap(1, 10, evicted_delta=5)) is not None


def _stuck_snap(streams):
    return {"ts": 1.0, "events": [], "postgres": {}, "streams": streams}


def test_stuck_pending_rule_detects_wedged_consumer():
    rule = Rule(
        {"name": "stuck", "type": "stuck_pending", "threshold": 1, "idle_ms": 120000}
    )
    # A consumer holding a task, idle well past the threshold -> wedged.
    wedged = _stuck_snap(
        {"arax": {"consumers": [{"name": "c1", "pending": 2, "idle_ms": 300000}]}}
    )
    detail = rule.evaluate(wedged)
    assert detail is not None and "arax/c1" in detail
    # Pending but actively working (low idle) -> not wedged.
    busy = _stuck_snap(
        {"arax": {"consumers": [{"name": "c1", "pending": 2, "idle_ms": 500}]}}
    )
    assert rule.evaluate(busy) is None
    # Idle but nothing held -> not wedged (just an idle consumer).
    idle_empty = _stuck_snap(
        {"arax": {"consumers": [{"name": "c1", "pending": 0, "idle_ms": 300000}]}}
    )
    assert rule.evaluate(idle_empty) is None


async def test_heartbeat_lost_suppressed_in_grace_then_fires_after(patched, clock):
    rule = Rule(
        {"name": "arax_down", "type": "heartbeat_lost", "worker": "arax", "duration": 0}
    )
    engine = _engine([rule])
    engine._broker_recovered_at = 1_000.0  # grace window: 1000..1030

    # Inside the recovery grace: a zero-alive worker is suppressed WITHOUT
    # arming a cooldown, so it can still fire once the window elapses.
    clock.return_value = 1_010.0
    await engine.evaluate(_worker_snapshot(1_010.0))
    patched["dispatch"].assert_not_called()

    # After the grace window: the still-down worker buffers a down-alert...
    clock.return_value = 1_040.0
    await engine.evaluate(_worker_snapshot(1_040.0))
    # ...which flushes on a later tick past the debounce window.
    clock.return_value = 1_047.0
    await engine.evaluate(_worker_snapshot(1_047.0))
    patched["dispatch"].assert_called_once()
    assert patched["dispatch"].call_args.args[0]["rule"] == "arax_down"


async def test_multiple_downed_workers_coalesce_into_one_batch(patched):
    engine = _engine()
    events = [_scale_down("aragorn.lookup"), _scale_down("arax"), _scale_down("bte")]

    # First tick buffers all three; nothing delivered yet (window not elapsed).
    await engine.evaluate({"ts": 1000.0, "events": events})
    patched["dispatch"].assert_not_called()
    patched["dispatch_batch"].assert_not_called()

    # A later tick past the debounce window flushes one combined message.
    await engine.evaluate({"ts": 1000.0 + 6, "events": []})
    patched["dispatch"].assert_not_called()
    patched["dispatch_batch"].assert_called_once()
    batched = patched["dispatch_batch"].call_args.args[0]
    assert {e["rule"].split(":", 1)[1] for e in batched} == {
        "aragorn.lookup",
        "arax",
        "bte",
    }


async def test_single_downed_worker_uses_single_message(patched):
    engine = _engine()
    await engine.evaluate({"ts": 2000.0, "events": [_scale_down("arax")]})
    await engine.evaluate({"ts": 2000.0 + 6, "events": []})
    patched["dispatch"].assert_called_once()
    patched["dispatch_batch"].assert_not_called()


async def test_same_worker_listed_once_within_window(patched):
    engine = _engine()
    # Two ticks reporting the same worker before the window elapses.
    await engine.evaluate({"ts": 3000.0, "events": [_scale_down("arax")]})
    await engine.evaluate({"ts": 3001.0, "events": [_scale_down("arax")]})
    await engine.evaluate({"ts": 3000.0 + 6, "events": []})
    # Deduped to one worker -> single-worker message, not a batch.
    patched["dispatch"].assert_called_once()
    patched["dispatch_batch"].assert_not_called()


async def test_threshold_alert_dispatches_immediately(patched):
    rule = Rule(
        {
            "name": "backlog",
            "type": "threshold",
            "metric": "xlen",
            "stream": "arax",
            "threshold": 10,
            "duration": 0,
        }
    )
    engine = _engine([rule])
    snapshot = {
        "ts": 4000.0,
        "events": [],
        "streams": {"arax": {"xlen": 50, "pending": 0}},
        "postgres": {},
        "workers": {},
    }
    await engine.evaluate(snapshot)
    # Backlog alerts are not worker-down: delivered right away, never batched.
    patched["dispatch"].assert_called_once()
    patched["dispatch_batch"].assert_not_called()


async def test_slack_batch_lists_every_worker(mocker):
    post = mocker.patch.object(alerts, "_post_slack", AsyncMock())
    mocker.patch.object(alerts.settings, "slack_webhook_url", "http://hook.example")
    events = [
        {
            "ts": 1.0,
            "rule": "worker_crashed:arax",
            "severity": "critical",
            "message": "Worker `arax` appears to have crashed.",
        },
        {
            "ts": 1.0,
            "rule": "worker_zero:bte",
            "severity": "critical",
            "message": "Worker `bte` scaled to zero.",
        },
    ]
    await alerts._dispatch_slack_batch(events, "2 workers down")
    post.assert_called_once()
    text = post.call_args.args[1]
    assert "2 workers down" in text
    assert "arax" in text and "bte" in text

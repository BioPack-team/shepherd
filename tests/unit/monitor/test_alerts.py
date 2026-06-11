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

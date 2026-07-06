"""Tests for the monitor janitor's abandoned-query cleanup alerting.

The janitor fails-and-cleans queries that never completed (their driving
worker crashed), clearing the callbacks that otherwise keep re-tripping the
callback-age alert. Each cleanup pass should alert exactly once -- a single
message for one query, a coalesced batch for several -- so abandonment
frequency is trackable without per-cooldown repeats.
"""

from unittest.mock import AsyncMock

from workers.monitor import alerts, janitor


def _abandoned(qid, age_sec=720.0, callbacks=2):
    return {"qid": qid, "age_sec": age_sec, "callbacks_deleted": callbacks}


def _patch_alerts(mocker):
    return {
        "record": mocker.patch.object(alerts, "_record_alert", AsyncMock()),
        "dispatch": mocker.patch.object(alerts, "dispatch", AsyncMock()),
        "dispatch_batch": mocker.patch.object(alerts, "dispatch_batch", AsyncMock()),
    }


async def test_single_abandoned_query_alerts_once(mocker):
    mocker.patch.object(
        janitor, "reap_abandoned_queries_db", AsyncMock(return_value=[_abandoned("q1")])
    )
    p = _patch_alerts(mocker)

    result = await janitor.reap_abandoned_queries()

    assert [q["qid"] for q in result] == ["q1"]
    p["record"].assert_called_once()
    p["dispatch"].assert_called_once()
    p["dispatch_batch"].assert_not_called()
    event = p["dispatch"].call_args.args[0]
    assert event["rule"] == "query_abandoned"
    assert "q1" in event["message"]


async def test_multiple_abandoned_queries_coalesce(mocker):
    mocker.patch.object(
        janitor,
        "reap_abandoned_queries_db",
        AsyncMock(return_value=[_abandoned("q1"), _abandoned("q2"), _abandoned("q3")]),
    )
    p = _patch_alerts(mocker)

    await janitor.reap_abandoned_queries()

    assert p["record"].call_count == 3
    p["dispatch"].assert_not_called()
    p["dispatch_batch"].assert_called_once()
    events, summary = p["dispatch_batch"].call_args.args
    assert summary == "3 abandoned queries"
    assert {e["qid"] for e in events} == {"q1", "q2", "q3"}


async def test_no_abandoned_queries_no_alert(mocker):
    mocker.patch.object(
        janitor, "reap_abandoned_queries_db", AsyncMock(return_value=[])
    )
    p = _patch_alerts(mocker)

    result = await janitor.reap_abandoned_queries()

    assert result == []
    p["record"].assert_not_called()
    p["dispatch"].assert_not_called()
    p["dispatch_batch"].assert_not_called()

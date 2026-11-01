"""Tests for the monitor poller's worker rollup, specifically that per-replica
resource fields (in-flight, RSS, CPU) reported in each heartbeat are carried
through into the snapshot the dashboard's per-replica modal renders from.
"""

import json
import time

from shepherd_utils.heartbeat import HEARTBEAT_REAP_SEC
from workers.monitor import poller


def _hb(consumer, *, in_flight, rss, cpu, task_limit=10, stale=False):
    return {
        "stream": "merge_message",
        "consumer": consumer,
        "started_at": 1000.0,
        "last_seen": 1005.0,
        "task_limit": task_limit,
        "in_flight": in_flight,
        "rss_bytes": rss,
        "cpu_pct": cpu,
        "stale": stale,
    }


def test_rollup_carries_per_replica_resources():
    workers = [
        _hb("c1", in_flight=3, rss=500_000_000, cpu=42.0),
        _hb("c2", in_flight=1, rss=300_000_000, cpu=10.5),
    ]

    rollup = poller._rollup_workers(workers)
    bucket = rollup["merge_message"]

    assert bucket["alive"] == 2
    # Totals for the card face.
    assert bucket["task_limit_total"] == 20
    assert bucket["in_flight_total"] == 4

    consumers = {c["consumer"]: c for c in bucket["consumers"]}
    assert consumers["c1"]["in_flight"] == 3
    assert consumers["c1"]["rss_bytes"] == 500_000_000
    assert consumers["c1"]["cpu_pct"] == 42.0
    assert consumers["c2"]["in_flight"] == 1
    assert consumers["c2"]["cpu_pct"] == 10.5


def test_rollup_tolerates_missing_resource_fields():
    """Heartbeats from a worker that couldn't sample /proc (or an in-flight
    upgrade) omit the fields; the rollup must not crash and totals treat the
    missing in-flight as zero."""
    workers = [{"stream": "merge_message", "consumer": "c1", "task_limit": 5}]

    rollup = poller._rollup_workers(workers)
    bucket = rollup["merge_message"]

    assert bucket["in_flight_total"] == 0
    consumer = bucket["consumers"][0]
    assert consumer["in_flight"] is None
    assert consumer["rss_bytes"] is None
    assert consumer["cpu_pct"] is None


def test_rollup_stale_heartbeats_counted_separately():
    workers = [
        _hb("c1", in_flight=2, rss=100, cpu=1.0),
        _hb("c2", in_flight=0, rss=100, cpu=0.0, stale=True),
    ]

    bucket = poller._rollup_workers(workers)["merge_message"]

    assert bucket["alive"] == 1
    assert bucket["stale"] == 1
    # Both replicas remain visible in the table so the modal can flag the stale one.
    assert len(bucket["consumers"]) == 2


async def test_collect_heartbeats_reaps_dead_persistent_keys(redis_mock, monkeypatch):
    """Persistent heartbeat keys left unrefreshed past the reap window are deleted.

    A fresh worker survives and is returned; a long-dead one (last_seen far
    older than HEARTBEAT_REAP_SEC) is both excluded from the result and removed
    from Redis, so crashed consumers don't accumulate as phantom stale rows.
    """
    broker = redis_mock["broker"]
    monkeypatch.setattr(poller, "broker_client", broker)
    now = time.time()
    await broker.set(
        "worker:heartbeat:merge_message:live",
        json.dumps({"stream": "merge_message", "consumer": "live", "last_seen": now}),
    )
    dead_key = "worker:heartbeat:merge_message:dead"
    await broker.set(
        dead_key,
        json.dumps(
            {
                "stream": "merge_message",
                "consumer": "dead",
                "last_seen": now - HEARTBEAT_REAP_SEC - 10,
            }
        ),
    )

    workers = await poller._collect_heartbeats()

    assert [w["consumer"] for w in workers] == ["live"]
    # The dead worker's key was reaped from the keyspace.
    assert await broker.get(dead_key) is None


def test_rollup_reports_distinct_versions():
    """Each replica's image version is carried through, and the bucket lists the
    distinct versions so the dashboard can flag a mixed fleet mid-rollout.
    Replicas that predate version reporting show as "unknown"."""
    workers = [
        {**_hb("c1", in_flight=0, rss=1, cpu=0.0), "version": "v1.2.0"},
        {**_hb("c2", in_flight=0, rss=1, cpu=0.0), "version": "v1.3.0"},
        {**_hb("c3", in_flight=0, rss=1, cpu=0.0), "version": "v1.3.0"},
        _hb("c4", in_flight=0, rss=1, cpu=0.0),
    ]

    bucket = poller._rollup_workers(workers)["merge_message"]

    assert bucket["versions"] == ["unknown", "v1.2.0", "v1.3.0"]
    consumers = {c["consumer"]: c for c in bucket["consumers"]}
    assert consumers["c1"]["version"] == "v1.2.0"
    assert consumers["c4"]["version"] is None


def _load(xlen, pending, task_limit_total=10):
    rollup = {"merge_message": {"task_limit_total": task_limit_total}}
    poller._attach_load_to_workers(
        rollup, {"merge_message": {"xlen": xlen, "pending": pending}}
    )
    return rollup["merge_message"]


def test_backlog_excludes_tasks_already_running():
    """XLEN still counts entries a worker has read but not finished (they're
    XDEL'd on completion), so one task in progress must not also show as
    backlog."""
    info = _load(xlen=1, pending=1)
    assert info["backlog"] == 0
    assert info["xlen"] == 1

    info = _load(xlen=7, pending=3)
    assert info["backlog"] == 4


def test_backlog_never_negative():
    """XLEN and XPENDING aren't read atomically; a skewed pair clamps to zero."""
    assert _load(xlen=2, pending=5)["backlog"] == 0


def test_utilization_counts_running_and_waiting():
    """Utilization stays total load vs capacity, so a saturated worker type with
    an empty queue still reads as busy."""
    info = _load(xlen=10, pending=10, task_limit_total=10)
    assert info["backlog"] == 0
    assert info["utilization"] == 1.0
    assert _load(xlen=3, pending=0, task_limit_total=0)["utilization"] == 1.0
    assert _load(xlen=0, pending=0, task_limit_total=0)["utilization"] == 0.0

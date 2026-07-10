"""Tests for the monitor poller's worker rollup, specifically that per-replica
resource fields (in-flight, RSS, CPU) reported in each heartbeat are carried
through into the snapshot the dashboard's per-replica modal renders from.
"""

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

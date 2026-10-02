"""The monitor's Postgres collection (workers.monitor.poller).

It runs every poll tick, so it must stay cheap as ``shepherd_brain`` grows over
its retention window: per tick only index-backed queries run (the unfinished
queries' states, the recent windows), while the whole-table finished-query
counts and the on-disk size refresh once per history interval and are reused
in between.
"""

from contextlib import asynccontextmanager

import pytest

from workers.monitor import poller


class _FakePostgres:
    """Answers each query by the first ``(fragment, rows)`` whose fragment it
    contains, and records the SQL it was sent."""

    def __init__(self, answers):
        self.answers = answers
        self.sql = []
        self.fail = None

    @asynccontextmanager
    async def connection(self, *args, **kwargs):
        if self.fail is not None:
            raise self.fail
        yield self

    async def execute(self, sql, params=None):
        self.sql.append(sql)
        for fragment, rows in self.answers:
            if fragment in sql:
                return _Cursor(rows)
        raise AssertionError(f"unexpected query: {sql}")


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    async def fetchall(self):
        return self.rows

    async def fetchone(self):
        return self.rows[0]


@pytest.fixture
def pg(monkeypatch):
    fake = _FakePostgres(
        [
            ("state NOT IN", [("QUEUED", 3), ("RUNNING", 2)]),
            ("FILTER", [("aragorn", 40, 4), ("bte", 10, 1), (None, 5, 0)]),
            ("FROM callbacks", [(7, 125.0)]),
            ("pg_stat_activity", [(12,)]),
            ("state IS NULL OR", [("COMPLETED", 900), ("ABANDONED", 6), (None, 1)]),
            ("pg_database_size", [(1000,)]),
            ("pg_ls_waldir", [(24,)]),
        ]
    )
    monkeypatch.setattr(poller, "pg_pool", fake)
    monkeypatch.setattr(poller, "_pg_slow", {})
    monkeypatch.setattr(poller, "_pg_slow_at", 0.0)
    return fake


async def test_tick_counts(pg):
    snapshot = await poller._collect_postgres()

    assert snapshot == {
        "state_counts": {"QUEUED": 3, "RUNNING": 2},
        # the windows add up across ARAs, rows without one included
        "queries_last_1h": 5,
        "queries_last_24h": 55,
        "per_ara_24h": {"aragorn": 40, "bte": 10},
        "callbacks_pending": 7,
        "oldest_callback_age_sec": 125.0,
        "connection_count": 12,
    }


async def test_a_tick_never_scans_the_finished_queries(pg):
    await poller._collect_postgres()

    joined = "\n".join(pg.sql)
    assert "GROUP BY status" not in joined  # nothing reads it
    assert "state IS NULL OR" not in joined
    assert "pg_database_size" not in joined
    # every shepherd_brain read is bounded by an index: the unfinished
    # partial index, or a start_time window
    for sql in pg.sql:
        if "FROM shepherd_brain" in sql:
            assert "state NOT IN ('COMPLETED', 'ABANDONED')" in sql or (
                "WHERE start_time >" in sql
            ), sql


async def test_postgres_down_is_reported(pg):
    pg.fail = ConnectionError("connection refused")

    snapshot = await poller._collect_postgres()

    assert "connection refused" in snapshot["error"]


async def test_slow_counts_refresh_once_per_history_interval(pg, monkeypatch):
    monkeypatch.setattr(poller.settings, "monitor_history_interval_sec", 30)
    clock = [1000.0]
    monkeypatch.setattr(poller.time, "time", lambda: clock[0])

    first = await poller._collect_postgres_slow()
    assert first["finished_state_counts"] == {
        "COMPLETED": 900,
        "ABANDONED": 6,
        "UNKNOWN": 1,
    }
    assert first["db_size_bytes"] == 1000
    queries = len(pg.sql)

    clock[0] += 29
    assert await poller._collect_postgres_slow() is first
    assert len(pg.sql) == queries, "reused, not re-queried"

    clock[0] += 2
    await poller._collect_postgres_slow()
    assert len(pg.sql) > queries


async def test_a_failed_slow_refresh_keeps_the_last_counts(pg, monkeypatch):
    first = await poller._collect_postgres_slow()
    monkeypatch.setattr(poller, "_pg_slow_at", 0.0)  # due again
    pg.fail = ConnectionError("down")

    assert await poller._collect_postgres_slow() is first


async def test_snapshot_merges_live_and_cached_state_counts(pg, monkeypatch):
    async def _nothing(*args, **kwargs):
        return {}

    async def _no_workers():
        return []

    monkeypatch.setattr(poller, "_collect_heartbeats", _no_workers)
    monkeypatch.setattr(poller, "_collect_shutdown_markers", lambda: _nothing())
    monkeypatch.setattr(poller, "_known_workers", lambda: _nothing())
    monkeypatch.setattr(poller, "_collect_streams", _nothing)
    monkeypatch.setattr(poller, "_collect_redis_info", _nothing)

    async def _no_state_changes(rollup, markers, now):
        return rollup, []

    monkeypatch.setattr(poller, "_resolve_worker_states", _no_state_changes)

    snapshot = await poller.collect_snapshot()

    assert snapshot["postgres"]["state_counts"] == {
        "QUEUED": 3,
        "RUNNING": 2,
        "COMPLETED": 900,
        "ABANDONED": 6,
        "UNKNOWN": 1,
    }
    assert snapshot["postgres"]["db_size_bytes"] == 1000
    assert snapshot["postgres"]["wal_size_bytes"] == 24

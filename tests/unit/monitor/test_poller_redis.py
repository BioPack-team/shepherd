"""The broker numbers the monitor collects from Redis' INFO."""

from unittest.mock import AsyncMock

from shepherd_utils.config import settings
from workers.monitor import poller


async def test_footprint_counts_fork_copy_on_write_against_the_limit(
    mocker, monkeypatch
):
    info = {
        "used_memory": 4 * 2**30,
        "maxmemory": 10 * 2**30,
        "used_memory_rss": 6 * 2**30,
        # a snapshot fork has copied this much since it started
        "current_cow_size": 4 * 2**30,
        "rdb_bgsave_in_progress": 1,
        "aof_enabled": 1,
        "aof_last_write_status": "ok",
        "rdb_last_bgsave_status": "err",
    }
    mocker.patch.object(poller, "broker_client").info = AsyncMock(return_value=info)
    monkeypatch.setattr(settings, "redis_memory_limit", "20Gi")

    redis = await poller._collect_redis_info()

    assert redis["maxmemory_pct"] == 40.0
    assert redis["footprint_bytes"] == 10 * 2**30
    assert redis["memory_limit_pct"] == 50.0
    assert redis["fork_in_progress"] == 1
    assert redis["rdb_last_bgsave_status"] == "err"


async def test_no_container_limit_leaves_the_footprint_unmeasured(mocker, monkeypatch):
    info = {"used_memory": 1, "maxmemory": 10, "used_memory_rss": 5}
    mocker.patch.object(poller, "broker_client").info = AsyncMock(return_value=info)
    monkeypatch.setattr(settings, "redis_memory_limit", "")

    redis = await poller._collect_redis_info()

    assert redis["memory_limit_bytes"] == 0
    assert redis["memory_limit_pct"] is None

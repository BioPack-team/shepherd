"""The monitor collects Redis' numbers per instance (workers.monitor.poller).

Shepherd's four Redis roles are one server by default and may be several
(shepherd_utils/redis_clients.py). The poller takes one INFO per distinct
server; ``snapshot["redis"]`` stays the broker role's, and
``snapshot["redis_instances"]`` has them all.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from shepherd_utils.config import settings
from workers.monitor import poller, redis_instances

BROKER_INSTANCE = "shepherd_broker:6379"
DATA_INSTANCE = "shepherd_data:6379"

# What _collect_redis_info returned before instances existed. Their values
# for the default deployment must not change.
LEGACY_KEYS = {
    "used_memory_human",
    "used_memory_bytes",
    "maxmemory_bytes",
    "maxmemory_pct",
    "maxmemory_policy",
    "evicted_keys",
    "evicted_keys_delta",
    "mem_fragmentation_ratio",
    "used_memory_rss_bytes",
    "footprint_bytes",
    "memory_limit_bytes",
    "memory_limit_pct",
    "aof_enabled",
    "aof_last_write_status",
    "aof_last_bgrewrite_status",
    "rdb_last_bgsave_status",
    "rdb_last_save_time",
    "fork_in_progress",
    "latest_fork_usec",
    "connected_clients",
    "instantaneous_ops_per_sec",
    "uptime_in_seconds",
    "run_id",
    "loading",
}


def _info(**overrides):
    info = {
        "used_memory": 40,
        "used_memory_human": "40B",
        "maxmemory": 100,
        "maxmemory_policy": "volatile-ttl",
        "evicted_keys": 0,
        "used_memory_rss": 50,
        "aof_enabled": 1,
        "aof_last_write_status": "ok",
        "aof_last_bgrewrite_status": "ok",
        "rdb_last_bgsave_status": "ok",
        "connected_clients": 7,
        "instantaneous_ops_per_sec": 11,
        "uptime_in_seconds": 99,
        "run_id": "run-a",
        "role": "master",
        "connected_slaves": 0,
    }
    info.update(overrides)
    return info


def _client(*infos, error=None):
    """A Redis whose successive INFO calls return ``infos`` (the last one
    repeating), or raise ``error``."""
    client = MagicMock()
    if error is not None:
        client.info = AsyncMock(side_effect=error)
    else:
        replies = list(infos)
        client.info = AsyncMock(
            side_effect=lambda: replies.pop(0) if len(replies) > 1 else replies[0]
        )
    return client


@pytest.fixture(autouse=True)
def fresh_instance_state(monkeypatch):
    """The poller's between-tick memory is module state; start each test clean."""
    monkeypatch.setattr(poller, "_instance_state", {})
    monkeypatch.setattr(settings, "redis_memory_limit", "")


@pytest.fixture
def clock(mocker):
    m = mocker.patch.object(poller, "time")
    m.time.return_value = 1_000.0
    return m.time


def _split(monkeypatch, mocker, broker, data):
    """Move the data role onto a Redis of its own, served by ``data``."""
    monkeypatch.setattr(settings, "redis_data_host", "shepherd_data")
    mocker.patch.object(poller, "broker_client", broker)
    extra = mocker.patch.object(redis_instances, "extra_client", return_value=data)
    return extra


# --- the default: one Redis ----------------------------------------------------


async def test_one_redis_is_one_instance_and_is_snapshot_redis(mocker):
    broker = _client(_info())
    mocker.patch.object(poller, "broker_client", broker)

    instances = await poller._collect_redis_instances()

    assert list(instances) == [BROKER_INSTANCE]
    redis = instances[BROKER_INSTANCE]
    assert redis["roles"] == ["broker", "data", "lock", "logs"]
    # one INFO for the server, not one per role
    assert broker.info.await_count == 1
    # every number the single-Redis monitor reported is still there, unchanged
    assert LEGACY_KEYS <= set(redis)
    assert {k: redis[k] for k in LEGACY_KEYS} == {
        "used_memory_human": "40B",
        "used_memory_bytes": 40,
        "maxmemory_bytes": 100,
        "maxmemory_pct": 40.0,
        "maxmemory_policy": "volatile-ttl",
        "evicted_keys": 0,
        "evicted_keys_delta": 0,
        "mem_fragmentation_ratio": None,
        "used_memory_rss_bytes": 50,
        "footprint_bytes": 50,
        "memory_limit_bytes": 0,
        "memory_limit_pct": None,
        "aof_enabled": 1,
        "aof_last_write_status": "ok",
        "aof_last_bgrewrite_status": "ok",
        "rdb_last_bgsave_status": "ok",
        "rdb_last_save_time": None,
        "fork_in_progress": 0,
        "latest_fork_usec": None,
        "connected_clients": 7,
        "instantaneous_ops_per_sec": 11,
        "uptime_in_seconds": 99,
        "run_id": "run-a",
        "loading": 0,
    }


async def test_one_redis_writes_the_history_series_it_always_did(mocker):
    mocker.patch.object(poller, "broker_client", _client(_info()))
    record = mocker.patch.object(poller.history, "record_many", AsyncMock())
    mocker.patch.object(poller.storage, "insert_metrics", AsyncMock())
    instances = await poller._collect_redis_instances()
    snapshot = {
        "ts": 1.0,
        "streams": {},
        "workers": {},
        "postgres": {},
        "redis": instances[BROKER_INSTANCE],
        "redis_instances": instances,
    }

    await poller.write_history(snapshot)

    samples = record.await_args.args[0]
    assert {k: v for k, v in samples.items() if k.startswith("redis")} == {
        "redis:used_memory_bytes": 40,
        "redis:maxmemory_bytes": 100,
        "redis:evicted_keys": 0,
        "redis:connected_clients": 7,
        "redis:ops_per_sec": 11,
    }


# --- roles on Redis servers of their own ---------------------------------------


async def test_a_split_configuration_collects_each_instance_once(mocker, monkeypatch):
    broker = _client(_info(used_memory=10, run_id="broker-run"))
    data = _client(_info(used_memory=90, run_id="data-run"))
    extra = _split(monkeypatch, mocker, broker, data)

    instances = await poller._collect_redis_instances()

    # the broker role's instance first: it is snapshot["redis"]
    assert list(instances) == [BROKER_INSTANCE, DATA_INSTANCE]
    assert instances[BROKER_INSTANCE]["roles"] == ["broker", "lock", "logs"]
    assert instances[BROKER_INSTANCE]["maxmemory_pct"] == 10.0
    assert instances[DATA_INSTANCE]["roles"] == ["data"]
    assert instances[DATA_INSTANCE]["instance"] == DATA_INSTANCE
    assert instances[DATA_INSTANCE]["maxmemory_pct"] == 90.0
    assert instances[DATA_INSTANCE]["run_id"] == "data-run"
    # three roles on the broker's Redis are still one INFO
    assert broker.info.await_count == 1 and data.info.await_count == 1
    extra.assert_called_once_with(DATA_INSTANCE, ["data"])


async def test_the_eviction_delta_is_kept_per_instance(mocker, monkeypatch):
    """One shared "previous evicted_keys" would read the difference between
    two servers' counters as evictions on every tick."""
    broker = _client(_info(evicted_keys=1000), _info(evicted_keys=1000))
    data = _client(_info(evicted_keys=5), _info(evicted_keys=8))
    _split(monkeypatch, mocker, broker, data)

    await poller._collect_redis_instances()
    instances = await poller._collect_redis_instances()

    assert instances[BROKER_INSTANCE]["evicted_keys_delta"] == 0
    assert instances[DATA_INSTANCE]["evicted_keys_delta"] == 3


async def test_an_unreachable_instance_does_not_hide_the_other(mocker, monkeypatch):
    broker = _client(_info(used_memory=10))
    data = _client(error=ConnectionError("data redis is down"))
    _split(monkeypatch, mocker, broker, data)

    instances = await poller._collect_redis_instances()

    assert instances[DATA_INSTANCE] == {
        "instance": DATA_INSTANCE,
        "roles": ["data"],
        "sentinel": False,
        "error": "data redis is down",
    }
    assert instances[BROKER_INSTANCE]["used_memory_bytes"] == 10
    assert "error" not in instances[BROKER_INSTANCE]


async def test_an_unreachable_broker_instance_does_not_hide_the_data_one(
    mocker, monkeypatch
):
    broker = _client(error=TimeoutError("slow"))
    data = _client(_info(used_memory=90))
    _split(monkeypatch, mocker, broker, data)

    instances = await poller._collect_redis_instances()

    assert instances[BROKER_INSTANCE]["error"] == "slow"
    assert instances[DATA_INSTANCE]["maxmemory_pct"] == 90.0


async def test_a_split_snapshot_keeps_redis_as_the_broker_instance(mocker, monkeypatch):
    _split(
        monkeypatch,
        mocker,
        _client(_info(run_id="broker-run")),
        _client(_info(run_id="data-run")),
    )

    async def _nothing(*args):
        return {}

    for name in ("_collect_heartbeats", "_collect_shutdown_markers", "_known_workers"):
        monkeypatch.setattr(poller, name, lambda: _nothing())
    for name in (
        "_collect_streams",
        "_collect_postgres_slow",
        "_collect_payload_store",
    ):
        monkeypatch.setattr(poller, name, _nothing)

    async def _postgres():
        return {"state_counts": {}}

    async def _no_state_changes(rollup, markers, now):
        return rollup, []

    monkeypatch.setattr(poller, "_collect_postgres", _postgres)
    monkeypatch.setattr(poller, "_resolve_worker_states", _no_state_changes)

    snapshot = await poller.collect_snapshot()

    assert snapshot["redis"]["run_id"] == "broker-run"
    assert snapshot["redis"] is snapshot["redis_instances"][BROKER_INSTANCE]
    assert snapshot["redis_instances"][DATA_INSTANCE]["run_id"] == "data-run"


async def test_another_instance_records_history_under_its_own_name(mocker, monkeypatch):
    _split(
        monkeypatch,
        mocker,
        _client(_info(used_memory=10)),
        _client(_info(used_memory=90)),
    )
    record = mocker.patch.object(poller.history, "record_many", AsyncMock())
    mocker.patch.object(poller.storage, "insert_metrics", AsyncMock())
    instances = await poller._collect_redis_instances()

    await poller.write_history(
        {
            "ts": 1.0,
            "streams": {},
            "workers": {},
            "postgres": {},
            "redis": instances[BROKER_INSTANCE],
            "redis_instances": instances,
        }
    )

    samples = record.await_args.args[0]
    # the broker role's instance keeps the names stored history already uses
    assert samples["redis:used_memory_bytes"] == 10
    assert samples[f"redis@{DATA_INSTANCE}:used_memory_bytes"] == 90
    assert samples[f"redis@{DATA_INSTANCE}:maxmemory_bytes"] == 100


# --- the container memory limit -------------------------------------------------


async def test_redis_memory_limit_describes_the_broker_instance_only(
    mocker, monkeypatch
):
    """The data Redis is another container: measuring its footprint against
    the broker's limit would be a made-up percentage."""
    _split(
        monkeypatch,
        mocker,
        _client(_info(used_memory_rss=50)),
        _client(_info(used_memory_rss=80)),
    )
    monkeypatch.setattr(settings, "redis_memory_limit", "100")

    instances = await poller._collect_redis_instances()

    assert instances[BROKER_INSTANCE]["memory_limit_pct"] == 50.0
    assert instances[DATA_INSTANCE]["memory_limit_bytes"] == 0
    assert instances[DATA_INSTANCE]["memory_limit_pct"] is None


async def test_a_role_with_its_own_redis_can_have_its_own_memory_limit(
    mocker, monkeypatch
):
    _split(
        monkeypatch,
        mocker,
        _client(_info(used_memory_rss=50)),
        _client(_info(used_memory_rss=80)),
    )
    monkeypatch.setattr(settings, "redis_memory_limit", "100")
    monkeypatch.setattr(settings, "redis_data_memory_limit", "200")

    instances = await poller._collect_redis_instances()

    assert instances[DATA_INSTANCE]["memory_limit_pct"] == 40.0
    assert instances[BROKER_INSTANCE]["memory_limit_pct"] == 50.0


def test_a_shared_redis_ignores_the_per_role_memory_limits(monkeypatch):
    monkeypatch.setattr(settings, "redis_memory_limit", "100")
    monkeypatch.setattr(settings, "redis_data_memory_limit", "200")

    assert redis_instances.memory_limit_bytes(["broker", "data", "lock", "logs"]) == 100


# --- write rate and eviction window ---------------------------------------------


async def test_write_rate_is_the_growth_of_net_input_per_second(mocker, clock):
    broker = _client(
        _info(total_net_input_bytes=1_000, maxmemory=6_000),
        _info(total_net_input_bytes=1_300, maxmemory=6_000),
    )
    mocker.patch.object(poller, "broker_client", broker)

    first = await poller._collect_redis_info()
    clock.return_value = 1_003.0
    second = await poller._collect_redis_info()

    # nothing to compare the first reading with
    assert first["write_bytes_per_sec"] is None
    assert first["eviction_window_sec"] is None
    assert second["write_bytes_per_sec"] == 100.0
    # 6000 bytes of cap filled at 100 bytes/s: a new key lasts a minute
    assert second["eviction_window_sec"] == 60.0


async def test_one_burst_barely_moves_the_smoothed_write_rate(
    mocker, clock, monkeypatch
):
    monkeypatch.setattr(settings, "monitor_redis_write_rate_window_sec", 60.0)
    totals = [0, 300, 600, 600 + 300_000]  # steady 100 B/s, then a 100 kB/s burst
    broker = _client(*[_info(total_net_input_bytes=t) for t in totals])
    mocker.patch.object(poller, "broker_client", broker)

    rates = []
    for tick in range(len(totals)):
        clock.return_value = 1_000.0 + 3 * tick
        rates.append((await poller._collect_redis_info())["write_bytes_per_sec"])

    assert rates[:3] == [None, 100.0, 100.0]
    # one 3s sample in a 60s window carries ~5% of the weight
    assert 4_000 < rates[3] < 6_000


async def test_a_counter_reset_is_a_new_baseline_not_a_negative_rate(mocker, clock):
    broker = _client(
        _info(total_net_input_bytes=5_000),
        _info(total_net_input_bytes=5_300),
        _info(total_net_input_bytes=40),  # restarted: the counter began again
        _info(total_net_input_bytes=340),
    )
    mocker.patch.object(poller, "broker_client", broker)

    rates = []
    for tick in range(4):
        clock.return_value = 1_000.0 + 3 * tick
        rates.append((await poller._collect_redis_info())["write_bytes_per_sec"])

    assert rates == [None, 100.0, 100.0, 100.0]


async def test_an_uncapped_instance_has_no_eviction_window(mocker, clock):
    broker = _client(
        _info(total_net_input_bytes=0, maxmemory=0),
        _info(total_net_input_bytes=300, maxmemory=0),
    )
    mocker.patch.object(poller, "broker_client", broker)

    await poller._collect_redis_info()
    clock.return_value = 1_003.0
    redis = await poller._collect_redis_info()

    assert redis["write_bytes_per_sec"] == 100.0
    assert redis["eviction_window_sec"] is None


async def test_write_rate_and_eviction_window_go_into_history(mocker, clock):
    broker = _client(
        _info(total_net_input_bytes=0, maxmemory=6_000),
        _info(total_net_input_bytes=300, maxmemory=6_000),
    )
    mocker.patch.object(poller, "broker_client", broker)
    await poller._collect_redis_info()
    clock.return_value = 1_003.0

    samples = poller._redis_samples("redis", await poller._collect_redis_info())

    assert samples["redis:write_bytes_per_sec"] == 100.0
    assert samples["redis:eviction_window_sec"] == 60.0


# --- replication ----------------------------------------------------------------


async def test_replicas_and_their_lag_are_read_from_info(mocker):
    broker = _client(
        _info(
            connected_slaves=2,
            master_replid="replid-a",
            master_replid2="0" * 40,
            master_repl_offset=10_000,
            # as redis-py parses the ``slaveN:ip=...,port=...`` lines
            slave0={
                "ip": "10.0.0.2",
                "port": 6379,
                "state": "online",
                "offset": 10_000,
                "lag": 0,
            },
            slave1={
                "ip": "10.0.0.3",
                "port": 6379,
                "state": "online",
                "offset": 4_000,
                "lag": 7,
            },
        )
    )
    mocker.patch.object(poller, "broker_client", broker)

    redis = await poller._collect_redis_info()

    assert redis["role"] == "master"
    assert redis["connected_replicas"] == 2
    assert redis["master_replid"] == "replid-a"
    assert redis["replicas"] == [
        {"addr": "10.0.0.2:6379", "state": "online", "lag_sec": 0, "lag_bytes": 0},
        {"addr": "10.0.0.3:6379", "state": "online", "lag_sec": 7, "lag_bytes": 6_000},
    ]
    assert redis["replica_max_lag_sec"] == 7
    assert redis["replica_max_lag_bytes"] == 6_000
    assert poller._redis_samples("redis", redis)["redis:connected_replicas"] == 2


async def test_a_redis_without_replicas_reports_none_and_records_no_series(mocker):
    mocker.patch.object(poller, "broker_client", _client(_info()))

    redis = await poller._collect_redis_info()

    assert redis["connected_replicas"] == 0
    assert redis["replicas"] == []
    assert redis["replica_max_lag_sec"] is None
    assert not any("replica" in name for name in poller._redis_samples("redis", redis))


async def test_an_instance_behind_sentinel_is_named_for_its_master(mocker, monkeypatch):
    monkeypatch.setattr(settings, "redis_sentinels", "sentinel-0:26379")
    monkeypatch.setattr(settings, "redis_sentinel_master", "shepherd")
    mocker.patch.object(poller, "broker_client", _client(_info()))

    instances = await poller._collect_redis_instances()

    assert list(instances) == ["sentinel:shepherd"]
    assert instances["sentinel:shepherd"]["sentinel"] is True

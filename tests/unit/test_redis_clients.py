"""Which Redis each role connects to (shepherd_utils.redis_clients): one
shared server by default, a Redis per role or a Sentinel-managed primary
when configured (#252 stage 4, #201). Pure configuration; the behaviour
against real Sentinels is in test_redis_sentinel.py.
"""

import pytest
import redis.asyncio as aioredis

from shepherd_utils import redis_clients
from shepherd_utils.config import settings
from shepherd_utils.redis_clients import BROKER, DATA, LOCK, LOGS

ROLE_SETTINGS = [
    f"redis_{role}_{suffix}"
    for role in redis_clients.ROLES
    for suffix in ("host", "port", "password", "sentinel_master")
]


@pytest.fixture(autouse=True)
def defaults(monkeypatch):
    """A known baseline, whatever the environment sets."""
    monkeypatch.setattr(settings, "redis_host", "shepherd_broker")
    monkeypatch.setattr(settings, "redis_port", 6379)
    monkeypatch.setattr(settings, "redis_password", "shared-password")
    monkeypatch.setattr(settings, "redis_sentinels", "")
    monkeypatch.setattr(settings, "redis_sentinel_master", "")
    monkeypatch.setattr(settings, "redis_sentinel_password", "")
    for name in ROLE_SETTINGS:
        default = None if name.endswith(("port", "password")) else ""
        monkeypatch.setattr(settings, name, default)


def test_by_default_every_role_is_one_redis_told_apart_by_database():
    targets = {role: redis_clients.target(role) for role in redis_clients.ROLES}

    assert {t.host for t in targets.values()} == {"shepherd_broker"}
    assert {t.port for t in targets.values()} == {6379}
    assert {t.password for t in targets.values()} == {"shared-password"}
    # the database numbers the code has always used
    assert {role: t.db for role, t in targets.items()} == {
        BROKER: 0,
        DATA: 1,
        LOCK: 2,
        LOGS: 3,
    }
    assert redis_clients.instances() == {
        "shepherd_broker:6379": [BROKER, DATA, LOCK, LOGS]
    }


def test_a_role_can_be_given_a_redis_of_its_own(monkeypatch):
    monkeypatch.setattr(settings, "redis_data_host", "shepherd_data")

    data = redis_clients.target(DATA)
    assert (data.host, data.port, data.password) == (
        "shepherd_data",
        6379,  # the shared port and password, unless it names its own
        "shared-password",
    )
    assert data.db == 1  # a role keeps its database wherever it lives
    assert redis_clients.target(BROKER).host == "shepherd_broker"
    assert redis_clients.instances() == {
        "shepherd_broker:6379": [BROKER, LOCK, LOGS],
        "shepherd_data:6379": [DATA],
    }


def test_a_role_with_its_own_redis_can_have_its_own_port_and_password(monkeypatch):
    monkeypatch.setattr(settings, "redis_data_host", "shepherd_data")
    monkeypatch.setattr(settings, "redis_data_port", 6380)
    monkeypatch.setattr(settings, "redis_data_password", "data-password")

    data = redis_clients.target(DATA)
    assert (data.port, data.password) == (6380, "data-password")
    # ... which nobody else picks up
    assert redis_clients.target(LOGS).password == "shared-password"


def test_an_empty_password_of_its_own_is_respected(monkeypatch):
    """ "" means no auth on that Redis; only unset falls back to the shared one."""
    monkeypatch.setattr(settings, "redis_data_host", "shepherd_data")
    monkeypatch.setattr(settings, "redis_data_password", "")
    assert redis_clients.target(DATA).password == ""


def test_a_port_or_password_without_a_host_of_its_own_changes_nothing(monkeypatch):
    """They describe a role's own Redis; on the shared one they would
    silently point one role somewhere else."""
    monkeypatch.setattr(settings, "redis_data_port", 6380)
    monkeypatch.setattr(settings, "redis_data_password", "data-password")
    data = redis_clients.target(DATA)
    assert (data.host, data.port, data.password) == (
        "shepherd_broker",
        6379,
        "shared-password",
    )


def test_roles_sharing_a_redis_of_their_own_are_one_instance(monkeypatch):
    monkeypatch.setattr(settings, "redis_broker_host", "shepherd_fast")
    monkeypatch.setattr(settings, "redis_lock_host", "shepherd_fast")
    assert redis_clients.instances() == {
        "shepherd_fast:6379": [BROKER, LOCK],
        "shepherd_broker:6379": [DATA, LOGS],
    }


def test_sentinel_replaces_the_address_for_every_role(monkeypatch):
    monkeypatch.setattr(settings, "redis_sentinels", "s1:26379, s2:26380,s3")
    monkeypatch.setattr(settings, "redis_sentinel_master", "shepherd")

    for role in redis_clients.ROLES:
        where = redis_clients.target(role)
        assert where.uses_sentinel
        assert where.sentinel_master == "shepherd"
        assert where.sentinels == (("s1", 26379), ("s2", 26380), ("s3", 26379))
        assert where.db == redis_clients.ROLE_DB[role]
    assert redis_clients.instances() == {
        "sentinel:shepherd": [BROKER, DATA, LOCK, LOGS]
    }


def test_sentinels_without_a_master_name_are_not_used(monkeypatch):
    monkeypatch.setattr(settings, "redis_sentinels", "s1:26379")
    assert not redis_clients.target(BROKER).uses_sentinel


def test_a_role_can_have_a_sentinel_master_of_its_own(monkeypatch):
    monkeypatch.setattr(settings, "redis_sentinels", "s1:26379")
    monkeypatch.setattr(settings, "redis_sentinel_master", "broker")
    monkeypatch.setattr(settings, "redis_data_sentinel_master", "data")

    assert redis_clients.target(DATA).sentinel_master == "data"
    assert redis_clients.target(BROKER).sentinel_master == "broker"
    assert redis_clients.instances() == {
        "sentinel:broker": [BROKER, LOCK, LOGS],
        "sentinel:data": [DATA],
    }


def test_a_roles_own_host_opts_it_out_of_the_shared_sentinel_master(monkeypatch):
    """The most specific setting wins: a role's own sentinel master, then
    its own host, then the shared sentinel master, then the shared host."""
    monkeypatch.setattr(settings, "redis_sentinels", "s1:26379")
    monkeypatch.setattr(settings, "redis_sentinel_master", "shepherd")
    monkeypatch.setattr(settings, "redis_logs_host", "plain-logs")

    logs = redis_clients.target(LOGS)
    assert not logs.uses_sentinel and logs.host == "plain-logs"

    monkeypatch.setattr(settings, "redis_logs_sentinel_master", "logs")
    assert redis_clients.target(LOGS).sentinel_master == "logs"


def test_a_roles_sentinel_master_without_sentinels_is_a_configuration_error(
    monkeypatch,
):
    monkeypatch.setattr(settings, "redis_data_sentinel_master", "data")
    with pytest.raises(ValueError, match="redis_sentinels is empty"):
        redis_clients.target(DATA)


def test_an_unknown_role_is_refused():
    with pytest.raises(ValueError):
        redis_clients.target("cache")


def test_direct_clients_are_built_on_a_blocking_pool_for_the_roles_redis(monkeypatch):
    monkeypatch.setattr(settings, "redis_data_host", "shepherd_data")

    pool = redis_clients.async_pool(DATA, max_connections=7, timeout=3)

    assert isinstance(pool, aioredis.BlockingConnectionPool)
    assert pool.max_connections == 7
    assert pool.connection_kwargs["host"] == "shepherd_data"
    assert pool.connection_kwargs["db"] == 1
    assert pool.connection_kwargs["password"] == "shared-password"
    assert pool.connection_kwargs["socket_keepalive"] is True


def test_sentinel_clients_block_for_a_connection_and_retry_a_failover(monkeypatch):
    monkeypatch.setattr(settings, "redis_sentinels", "s1:26379")
    monkeypatch.setattr(settings, "redis_sentinel_master", "shepherd")

    pool = redis_clients.async_pool(LOCK, max_connections=4, retry_on_timeout=True)

    assert isinstance(pool, redis_clients.BlockingSentinelConnectionPool)
    assert isinstance(pool, aioredis.BlockingConnectionPool)
    assert pool.max_connections == 4
    assert pool.service_name == "shepherd" and pool.is_master
    assert pool.connection_kwargs["db"] == 2
    retry = pool.connection_kwargs["retry"]
    assert retry._retries == redis_clients.FAILOVER_RETRIES
    for error in redis_clients.FAILOVER_ERRORS:
        assert error in pool.connection_kwargs["retry_on_error"]


def test_the_sync_clients_follow_the_same_rules(monkeypatch):
    monkeypatch.setattr(settings, "redis_data_host", "shepherd_data")
    direct = redis_clients.sync_client(DATA, socket_timeout=5)
    assert direct.connection_pool.connection_kwargs["host"] == "shepherd_data"
    assert direct.connection_pool.connection_kwargs["db"] == 1

    monkeypatch.setattr(settings, "redis_sentinels", "s1:26379")
    monkeypatch.setattr(settings, "redis_data_sentinel_master", "data")
    managed = redis_clients.sync_client(DATA, socket_timeout=5)
    assert managed.connection_pool.service_name == "data"
    assert managed.connection_pool.connection_kwargs["db"] == 1


def test_the_module_level_clients_are_built_by_the_factory():
    """broker.py and db.py build their pools at import; with the default
    settings those are the shared Redis, databases 0-3, as before."""
    from shepherd_utils import broker, db

    for pool, database in (
        (broker.broker_redis_pool, 0),
        (db.data_db_pool, 1),
        (broker.lock_redis_pool, 2),
        (db.logs_db_pool, 3),
    ):
        assert isinstance(pool, aioredis.BlockingConnectionPool)
        assert pool.connection_kwargs["db"] == database

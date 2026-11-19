"""Sentinel-managed Redis, against real processes (#201).

The clients built by ``shepherd_utils.redis_clients`` are pointed at a real
primary, replica and three Sentinels started for the test, and must:

* find the primary through Sentinel rather than by address;
* queue for a pooled connection instead of failing when the pool is busy
  (redis-py's own Sentinel pool raises);
* carry on after a failover, both when the old primary is demoted (it
  answers READONLY) and when it is killed.

Skipped where ``redis-server`` is not installed (it is not in the tox
environment); the configuration logic itself is covered without processes
in test_redis_clients.py.
"""

import asyncio
import shutil
import socket
import subprocess
import time

import pytest
import redis

from shepherd_utils import redis_clients
from shepherd_utils.config import settings

pytestmark = pytest.mark.skipif(
    shutil.which("redis-server") is None, reason="redis-server is not installed"
)

PASSWORD = "sentinel-test-password"
MASTER = "shepherd-test"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait(check, timeout=20.0, what="condition"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except Exception as e:  # not up yet
            last = e
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {what}: {last}")


class Cluster:
    """A primary, a replica and three Sentinels on loopback."""

    def __init__(self, tmp_path):
        self.tmp = tmp_path
        self.procs = {}
        self.redis_ports = [_free_port(), _free_port()]
        self.sentinel_ports = [_free_port() for _ in range(3)]

    def start_redis(self, port, replica_of=None):
        directory = self.tmp / f"redis-{port}"
        directory.mkdir(exist_ok=True)
        args = [
            "redis-server",
            "--port", str(port),
            "--bind", "127.0.0.1",
            "--dir", str(directory),
            "--save", "",
            "--appendonly", "no",
            "--requirepass", PASSWORD,
            "--masterauth", PASSWORD,
        ]  # fmt: skip
        if replica_of:
            args += ["--replicaof", "127.0.0.1", str(replica_of)]
        self.procs[port] = subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

    def start_sentinel(self, port):
        conf = self.tmp / f"sentinel-{port}.conf"
        conf.write_text(
            f"port {port}\n"
            "bind 127.0.0.1\n"
            f"dir {self.tmp}\n"
            f"sentinel monitor {MASTER} 127.0.0.1 {self.redis_ports[0]} 2\n"
            f"sentinel auth-pass {MASTER} {PASSWORD}\n"
            f"sentinel down-after-milliseconds {MASTER} 1000\n"
            f"sentinel failover-timeout {MASTER} 5000\n"
        )
        self.procs[port] = subprocess.Popen(
            ["redis-server", str(conf), "--sentinel"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def start(self):
        primary, replica = self.redis_ports
        self.start_redis(primary)
        self.start_redis(replica, replica_of=primary)
        _wait(lambda: self.direct(primary).ping(), what="the primary")
        _wait(
            lambda: self.direct(replica).info("replication")["master_link_status"]
            == "up",
            what="the replica to sync",
        )
        for port in self.sentinel_ports:
            self.start_sentinel(port)
        # every Sentinel must know the replica and its peers before a
        # failover can be agreed
        for port in self.sentinel_ports:
            _wait(
                lambda p=port: self.sentinel(p).sentinel_master(MASTER)[
                    "num-other-sentinels"
                ]
                == 2
                and self.sentinel(p).sentinel_master(MASTER)["num-slaves"] == 1,
                what=f"sentinel {port} to see the topology",
            )

    def direct(self, port):
        return redis.Redis(
            host="127.0.0.1", port=port, password=PASSWORD, socket_timeout=2
        )

    def direct_db(self, port, role=redis_clients.DATA):
        client = self.direct(port)
        client.select(redis_clients.ROLE_DB[role])
        return client

    def sentinel(self, port):
        return redis.Redis(host="127.0.0.1", port=port, socket_timeout=2)

    def primary_port(self):
        return int(
            self.sentinel(self.sentinel_ports[0]).sentinel_get_master_addr_by_name(
                MASTER
            )[1]
        )

    def kill(self, port):
        proc = self.procs.pop(port)
        proc.kill()
        proc.wait()

    def stop(self):
        for proc in self.procs.values():
            proc.kill()
        for proc in self.procs.values():
            proc.wait()


@pytest.fixture
def cluster(tmp_path, monkeypatch):
    c = Cluster(tmp_path)
    try:
        c.start()
        monkeypatch.setattr(
            settings,
            "redis_sentinels",
            ",".join(f"127.0.0.1:{p}" for p in c.sentinel_ports),
        )
        monkeypatch.setattr(settings, "redis_sentinel_master", MASTER)
        monkeypatch.setattr(settings, "redis_password", PASSWORD)
        # nothing in the settings names the Redis itself
        monkeypatch.setattr(settings, "redis_host", "no-such-host.invalid")
        yield c
    finally:
        c.stop()


async def _eventually(operation, timeout=30.0):
    """Run ``operation`` until it stops raising: a caller's own retry loop
    during the seconds a failover takes."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return await operation()
        except Exception:
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.2)


async def test_clients_find_the_primary_through_sentinel(cluster):
    client = redis_clients.async_client(redis_clients.DATA, socket_timeout=5)
    try:
        assert await client.set("k", "v")
        # written to the primary, in the role's own database
        primary = cluster.direct(cluster.redis_ports[0])
        primary.select(redis_clients.ROLE_DB[redis_clients.DATA])
        assert primary.get("k") == b"v"
    finally:
        await client.aclose()

    sync = redis_clients.sync_client(redis_clients.DATA, socket_timeout=5)
    assert sync.get("k") == b"v"
    sync.close()


async def test_a_busy_pool_makes_callers_wait_instead_of_failing(cluster):
    """Every pool in Shepherd is a blocking pool: a burst queues for a
    connection. redis-py's Sentinel pool on its own raises 'Too many
    connections' instead."""
    client = redis_clients.async_client(
        redis_clients.BROKER, max_connections=2, timeout=10, socket_timeout=5
    )
    try:
        results = await asyncio.gather(*(client.incr("counter") for _ in range(40)))
        assert sorted(results) == list(range(1, 41))
    finally:
        await client.aclose()


async def test_clients_follow_a_failover_when_the_primary_is_demoted(cluster):
    """A planned failover: the old primary stays up as a replica and answers
    writes with READONLY. Existing connections must move to the new primary."""
    client = redis_clients.async_client(redis_clients.DATA, socket_timeout=5)
    sync = redis_clients.sync_client(redis_clients.DATA, socket_timeout=5)
    try:
        await client.set("before", "1")
        assert sync.get("before") == b"1"
        old = cluster.primary_port()

        cluster.sentinel(cluster.sentinel_ports[0]).execute_command(
            "SENTINEL", "FAILOVER", MASTER
        )
        _wait(lambda: cluster.primary_port() != old, what="the failover")
        new = cluster.primary_port()
        # Sentinel promotes the replica first and demotes the old primary a
        # moment later. Until then the old primary still accepts writes --
        # and loses them when it resyncs; that window is Redis's, not the
        # client's. What is tested here is the state after it: connections
        # still open to a node that now answers READONLY.
        _wait(
            lambda: cluster.direct(old).info("replication")["role"] == "slave",
            what="the old primary to be demoted",
        )

        assert await _eventually(lambda: client.set("after", "2"))
        assert cluster.direct_db(new).get("after") == b"2"
        assert await client.get("before") == b"1"  # replicated before the switch
        sync.set("after-sync", "3")
        assert cluster.direct_db(new).get("after-sync") == b"3"
    finally:
        await client.aclose()
        sync.close()


async def test_clients_follow_a_failover_when_the_primary_dies(cluster):
    client = redis_clients.async_client(redis_clients.BROKER, socket_timeout=5)
    try:
        await client.set("before", "1")
        old = cluster.primary_port()

        cluster.kill(old)
        _wait(lambda: cluster.primary_port() != old, timeout=30, what="the failover")

        assert await _eventually(lambda: client.set("after", "2"))
        assert int(await client.incr("counter")) == 1
    finally:
        await client.aclose()


async def test_a_role_can_have_its_own_sentinel_master(cluster, monkeypatch):
    """The data role on its own Redis, found through the same Sentinels;
    the broker left on a directly addressed one."""
    monkeypatch.setattr(settings, "redis_sentinel_master", "")
    monkeypatch.setattr(settings, "redis_data_sentinel_master", MASTER)
    monkeypatch.setattr(settings, "redis_host", "127.0.0.1")
    monkeypatch.setattr(settings, "redis_port", cluster.redis_ports[0])

    assert redis_clients.target(redis_clients.DATA).uses_sentinel
    assert not redis_clients.target(redis_clients.BROKER).uses_sentinel
    data = redis_clients.async_client(redis_clients.DATA, socket_timeout=5)
    broker = redis_clients.async_client(redis_clients.BROKER, socket_timeout=5)
    try:
        assert await data.set("payload", "x")
        assert await broker.set("stream", "y")
    finally:
        await data.aclose()
        await broker.aclose()

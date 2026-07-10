"""Tests for ``shepherd_utils.broker``.

Exercise the redis-streams add/get/ack flow against fakeredis, and the
pubsub-backed lock acquisition / release scripts.
"""

import asyncio
import logging

import pytest

from shepherd_utils import broker as broker_module
from shepherd_utils.broker import (
    BrokerHealth,
    _KEEPALIVE_OPTIONS,
    acquire_lock,
    add_task,
    broker_health,
    create_consumer_group,
    get_task,
    mark_task_as_complete,
    refresh_lock,
    remove_lock,
    try_lock,
)

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_create_consumer_group_swallows_existing_group(redis_mock):
    """Creating a group twice on the same stream should not raise."""
    await create_consumer_group("stream1", "consumer", logger)
    # Second call would normally raise BUSYGROUP; broker swallows it.
    await create_consumer_group("stream1", "consumer", logger)


@pytest.mark.asyncio
async def test_add_and_get_task_roundtrip(redis_mock):
    payload = {
        "query_id": "q1",
        "response_id": "r1",
        "workflow": "[]",
        "log_level": "20",
        "otel": "{}",
    }
    await add_task("teststream", payload, logger)
    msg_id, fields = await get_task("teststream", "consumer", "test", logger)
    assert isinstance(msg_id, str)
    assert fields == payload


@pytest.mark.asyncio
async def test_get_task_returns_none_when_no_messages(redis_mock):
    """A short, empty xreadgroup call should return None and not raise."""
    # The broker's get_task calls create_consumer_group first; we still hit
    # the block timeout. fakeredis returns immediately because no messages
    # are in the stream after the group is created.
    out = await get_task("empty_stream", "consumer", "test", logger)
    assert out is None


# --- broker health tracking -------------------------------------------------


def test_broker_health_starts_fresh_and_tracks_failures():
    """A new tracker reads as recently-successful so a worker booting during an
    outage gets a full grace window; failures accumulate until a success."""
    h = BrokerHealth()
    assert h.seconds_since_success() < 1.0
    assert h.consecutive_failures == 0

    h.record_failure()
    h.record_failure()
    assert h.consecutive_failures == 2

    h.record_success()
    assert h.consecutive_failures == 0
    assert h.seconds_since_success() < 1.0


def test_broker_health_seconds_since_success_grows(monkeypatch):
    """seconds_since_success reflects wall-clock (monotonic) time since the last
    successful read -- this is what the self-exit threshold is compared against."""
    h = BrokerHealth()
    clock = {"t": 1000.0}
    monkeypatch.setattr(broker_module.time, "monotonic", lambda: clock["t"])
    h.record_success()
    clock["t"] += 42.0
    assert h.seconds_since_success() == pytest.approx(42.0)


@pytest.mark.asyncio
async def test_get_task_records_success_on_empty_read(redis_mock):
    """An empty (but answered) read counts as broker health, resetting failures."""
    broker_health.record_failure()
    assert broker_health.consecutive_failures >= 1
    out = await get_task("empty_stream2", "consumer", "test", logger)
    assert out is None
    assert broker_health.consecutive_failures == 0
    assert broker_health.seconds_since_success() < 1.0


@pytest.mark.asyncio
async def test_get_task_records_failure_on_broker_error(redis_mock, mocker):
    """A raising xreadgroup is swallowed (returns None) but bumps the failure
    count so the poll loop can eventually self-exit."""
    broker_health.record_success()
    mocker.patch.object(
        broker_module.broker_client,
        "xreadgroup",
        side_effect=ConnectionError("Timeout reading from shepherd-broker:6379"),
    )
    out = await get_task("some_stream", "consumer", "test", logger)
    assert out is None
    assert broker_health.consecutive_failures == 1
    # Leave the shared singleton healthy for other tests.
    broker_health.record_success()


def test_keepalive_options_are_configured():
    """On Linux the pools get explicit TCP keepalive tuning (empty dict on
    platforms that don't expose the options, e.g. a macOS dev box)."""
    import socket

    if hasattr(socket, "TCP_KEEPIDLE"):
        assert _KEEPALIVE_OPTIONS[socket.TCP_KEEPIDLE] == 30
        assert _KEEPALIVE_OPTIONS[socket.TCP_KEEPINTVL] == 10
        assert _KEEPALIVE_OPTIONS[socket.TCP_KEEPCNT] == 3


@pytest.mark.asyncio
async def test_mark_task_as_complete_acks_message(redis_mock):
    """An ACKed message should leave the pending list."""
    await add_task("ackstream", {"q": "1"}, logger)
    msg_id, _ = await get_task("ackstream", "consumer", "tester", logger)

    pending_before = await redis_mock["broker"].xpending("ackstream", "consumer")
    assert pending_before["pending"] == 1

    await mark_task_as_complete("ackstream", "consumer", msg_id, logger)

    pending_after = await redis_mock["broker"].xpending("ackstream", "consumer")
    assert pending_after["pending"] == 0


@pytest.mark.asyncio
async def test_acquire_lock_returns_true_when_lock_is_free(redis_mock):
    got = await acquire_lock("resource-1", "consumer-A", logger)
    assert got is True
    val = await redis_mock["lock"].get("resource-1")
    assert val == "consumer-A"


@pytest.mark.asyncio
async def test_remove_lock_releases_only_when_token_matches(redis_mock, mocker):
    """The unlock Lua script only removes the key when the consumer id matches.

    fakeredis does not implement ``evalsha``, so we replace ``register_script``
    with a python emulator that runs the same logic (compare-and-delete).
    """

    def fake_register_script(_script):
        async def _runner(keys, args):
            (key,) = keys
            (token,) = args
            current = await redis_mock["lock"].get(key)
            if current == token:
                await redis_mock["lock"].delete(key)
                return 1
            return 0

        return _runner

    mocker.patch.object(
        redis_mock["lock"], "register_script", side_effect=fake_register_script
    )

    await acquire_lock("resource-2", "consumer-A", logger)
    # Wrong owner can't release.
    await remove_lock("resource-2", "consumer-B", logger)
    assert await redis_mock["lock"].get("resource-2") == "consumer-A"

    await remove_lock("resource-2", "consumer-A", logger)
    assert await redis_mock["lock"].get("resource-2") is None


@pytest.mark.asyncio
async def test_acquire_lock_blocks_until_other_consumer_releases(redis_mock, mocker):
    """A second acquire while the lock is held should fail.

    We patch the pubsub message wait so the loop iterates fast instead of
    really waiting 5s per try.
    """

    # Patch pubsub.get_message to always raise asyncio.TimeoutError so the
    # loop falls through quickly. This emulates "no notification arrived".
    real_pubsub = redis_mock["lock"].pubsub

    class FastPubSub:
        def __init__(self):
            self._inner = real_pubsub()

        async def subscribe(self, channel):
            return await self._inner.subscribe(channel)

        async def unsubscribe(self, channel):
            return await self._inner.unsubscribe(channel)

        async def aclose(self):
            return await self._inner.aclose()

        async def get_message(self, ignore_subscribe_messages=True, timeout=5):
            raise asyncio.TimeoutError

    mocker.patch.object(redis_mock["lock"], "pubsub", FastPubSub)

    # First consumer holds the lock.
    assert await acquire_lock("resource-3", "consumer-A", logger) is True

    # Second consumer tries; can't get it. With our fast pubsub stub this
    # falls through 12 iterations almost instantly.
    got = await acquire_lock("resource-3", "consumer-B", logger)
    assert got is False
    # And the original holder's value is unchanged.
    assert await redis_mock["lock"].get("resource-3") == "consumer-A"


@pytest.mark.asyncio
async def test_try_lock_is_non_blocking(redis_mock):
    """try_lock grabs a free lock and, crucially, returns immediately (False)
    when the lock is already held -- it never waits. This is what lets a
    contended merge_message worker go do other work instead of sitting idle."""
    assert await try_lock("resource-tl", "consumer-A", logger) is True
    assert await redis_mock["lock"].get("resource-tl") == "consumer-A"

    # Second attempt while held returns immediately, without blocking.
    got = await asyncio.wait_for(
        try_lock("resource-tl", "consumer-B", logger), timeout=1
    )
    assert got is False
    assert await redis_mock["lock"].get("resource-tl") == "consumer-A"


@pytest.mark.asyncio
async def test_refresh_lock_extends_only_own_lock(redis_mock, mocker):
    """refresh_lock (compare-and-pexpire) bumps the TTL only when we still hold
    the lock. fakeredis lacks evalsha, so emulate the Lua compare-and-pexpire."""

    def fake_register_script(_script):
        async def _runner(keys, args):
            (key,) = keys
            token, ttl_ms = args
            current = await redis_mock["lock"].get(key)
            if current == token:
                await redis_mock["lock"].pexpire(key, int(ttl_ms))
                return 1
            return 0

        return _runner

    mocker.patch.object(
        redis_mock["lock"], "register_script", side_effect=fake_register_script
    )

    await try_lock("resource-rl", "consumer-A", logger, ttl_sec=45)
    # Owner refreshes -> bumps TTL and returns True.
    assert await refresh_lock("resource-rl", "consumer-A", 60000, logger) is True
    ttl = await redis_mock["lock"].pttl("resource-rl")
    assert ttl > 45000

    # A non-owner cannot refresh.
    assert await refresh_lock("resource-rl", "consumer-B", 90000, logger) is False

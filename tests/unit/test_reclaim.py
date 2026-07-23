"""Regression tests for ``shepherd_utils.reclaim.reclaim_orphaned``.

These pin the bug where reclaim used ``execute_command("XPENDING", ...)`` with
the extended (IDLE/start/end/count) form. redis-py registers a response
callback on the XPENDING *command name* that only parses the summary form, so
the extended call raised ``IndexError`` -- swallowed by reclaim's own
``except`` -- and reclaim silently returned ``[]`` for every stream, so
orphaned pending messages were never reclaimed. fakeredis subclasses the real
redis-py client and shares that callback, so it reproduces the bug faithfully.

Note: fakeredis mishandles ``XPENDING ... IDLE 0`` (returns nothing), so the
tests pin a tiny positive idle floor (~1ms) via ``min_idle_sec_for`` and let
the message accrue a little idle before reclaiming -- matching how real Redis
treats the idle filter.
"""

import asyncio
import json
import logging
import time

import pytest

from shepherd_utils import reclaim

logger = logging.getLogger(__name__)


def _fresh_heartbeat(last_seen: "float | None" = None) -> str:
    """A heartbeat payload that reads as alive (recent ``last_seen``)."""
    return json.dumps(
        {"last_seen": last_seen if last_seen is not None else time.time()}
    )


@pytest.fixture()
def tiny_idle_floor(monkeypatch):
    """Force a ~1ms idle floor so the fakeredis IDLE filter behaves."""
    monkeypatch.setattr(reclaim, "min_idle_sec_for", lambda *a, **k: 0.001)


async def _seed_pending(broker, stream, group, consumer):
    """Add a message and leave it pending (unacked) under ``consumer``."""
    msg_id = await broker.xadd(stream, {"query_id": "q1", "response_id": "r1"})
    await broker.xgroup_create(stream, group, "0", mkstream=True)
    await broker.xreadgroup(group, consumer, {stream: ">"}, count=1)
    # Let it accrue a few ms of idle so it clears the (tiny) idle floor.
    await asyncio.sleep(0.02)
    return msg_id


@pytest.mark.asyncio
async def test_reclaim_orphaned_claims_dead_consumer_message(
    redis_mock, tiny_idle_floor, monkeypatch
):
    """A pending message owned by a dead consumer is reclaimed to the live one."""
    broker = redis_mock["broker"]
    monkeypatch.setattr(reclaim, "broker_client", broker)

    stream, group = "aragorn.score", "consumer"
    msg_id = await _seed_pending(broker, stream, group, "deadworker")

    # No heartbeat keys exist -> deadworker is not alive -> reclaimable.
    reclaimed = await reclaim.reclaim_orphaned(stream, group, "liveworker", logger)

    assert [m[0] for m in reclaimed] == [msg_id]
    # Ownership moved to the live worker so it can reprocess + ack it.
    summary = await broker.xpending(stream, group)
    assert [c["name"] for c in summary["consumers"]] == ["liveworker"]


@pytest.mark.asyncio
async def test_reclaim_orphaned_skips_live_owner(
    redis_mock, tiny_idle_floor, monkeypatch
):
    """A message whose owner still has a heartbeat is never claimed."""
    broker = redis_mock["broker"]
    monkeypatch.setattr(reclaim, "broker_client", broker)

    stream, group = "aragorn.score", "consumer"
    await _seed_pending(broker, stream, group, "busyworker")
    # busyworker has a fresh heartbeat -> protected from reclaim.
    await broker.set(f"worker:heartbeat:{stream}:busyworker", _fresh_heartbeat())

    reclaimed = await reclaim.reclaim_orphaned(stream, group, "liveworker", logger)

    assert reclaimed == []
    summary = await broker.xpending(stream, group)
    assert [c["name"] for c in summary["consumers"]] == ["busyworker"]


@pytest.mark.asyncio
async def test_reclaim_orphaned_claims_from_stale_heartbeat_owner(
    redis_mock, tiny_idle_floor, monkeypatch
):
    """A lingering-but-stale heartbeat key does NOT shield a dead consumer.

    Heartbeat keys are persistent, so a crashed consumer's key can outlive it.
    Reclaim must judge liveness by last_seen freshness, not key existence, or
    orphaned work would never be recovered while the stale key sits there.
    """
    broker = redis_mock["broker"]
    monkeypatch.setattr(reclaim, "broker_client", broker)

    stream, group = "aragorn.score", "consumer"
    msg_id = await _seed_pending(broker, stream, group, "deadworker")
    # Key still present, but last_seen is far older than HEARTBEAT_TTL_SEC.
    await broker.set(
        f"worker:heartbeat:{stream}:deadworker", _fresh_heartbeat(time.time() - 3600)
    )

    reclaimed = await reclaim.reclaim_orphaned(stream, group, "liveworker", logger)

    assert [m[0] for m in reclaimed] == [msg_id]


@pytest.mark.asyncio
async def test_reclaim_orphaned_skips_own_pending(
    redis_mock, tiny_idle_floor, monkeypatch
):
    """reclaim never yanks the current consumer's own pending entries."""
    broker = redis_mock["broker"]
    monkeypatch.setattr(reclaim, "broker_client", broker)

    stream, group = "aragorn.score", "consumer"
    await _seed_pending(broker, stream, group, "liveworker")

    reclaimed = await reclaim.reclaim_orphaned(stream, group, "liveworker", logger)

    assert reclaimed == []


@pytest.mark.asyncio
async def test_reclaim_orphaned_populates_delivery_counts(
    redis_mock, tiny_idle_floor, monkeypatch
):
    """The optional out-param is filled with each candidate's delivery count.

    This is the signal the worker loop uses to break a poison-pill retry loop.
    A message that has only been delivered once reads back as 1.
    """
    broker = redis_mock["broker"]
    monkeypatch.setattr(reclaim, "broker_client", broker)

    stream, group = "aragorn.score", "consumer"
    msg_id = await _seed_pending(broker, stream, group, "deadworker")

    delivery_counts: dict = {}
    reclaimed = await reclaim.reclaim_orphaned(
        stream, group, "liveworker", logger, delivery_counts=delivery_counts
    )

    assert [m[0] for m in reclaimed] == [msg_id]
    # Delivered exactly once so far (the initial XREADGROUP), before this claim.
    assert delivery_counts == {msg_id: 1}

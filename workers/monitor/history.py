"""Rolling timeseries storage for monitor metrics.

Each metric lives in a Redis sorted set keyed by ``monitor:hist:{name}``, with
``score=unix_timestamp_seconds`` and ``member="<ts>:<value>"`` (the timestamp
prefix on the member keeps writes unique even when the same value lands at the
same second). On every write we trim entries older than the retention window.
"""

import json
import time
from typing import Any, List, Tuple

from shepherd_utils.broker import broker_client
from shepherd_utils.config import settings

HISTORY_PREFIX = "monitor:hist"
HISTORY_INDEX = "monitor:hist:index"
# Cap each series by count as well as age. With history written every ~30s
# this allows roughly 3.5 days of headroom -- the retention window stays the
# real bound, but the count cap protects against runaway growth if the poller
# tick rate is misconfigured.
HISTORY_MAX_SAMPLES = 10000


def _key(name: str) -> str:
    return f"{HISTORY_PREFIX}:{name}"


def retention_seconds() -> int:
    return int(settings.monitor_history_days * 86400)


async def record(name: str, value: Any, ts: float | None = None) -> None:
    """Append a single sample to the named series and trim old entries."""
    ts = ts if ts is not None else time.time()
    cutoff = ts - retention_seconds()
    member = f"{ts}:{json.dumps(value, default=str)}"
    pipe = broker_client.pipeline()
    pipe.zadd(_key(name), {member: ts})
    pipe.zremrangebyscore(_key(name), 0, cutoff)
    pipe.zremrangebyrank(_key(name), 0, -HISTORY_MAX_SAMPLES - 1)
    pipe.sadd(HISTORY_INDEX, name)
    await pipe.execute()


async def record_many(samples: dict[str, Any], ts: float | None = None) -> None:
    """Record many series at the same timestamp in one pipeline."""
    ts = ts if ts is not None else time.time()
    cutoff = ts - retention_seconds()
    pipe = broker_client.pipeline()
    for name, value in samples.items():
        member = f"{ts}:{json.dumps(value, default=str)}"
        pipe.zadd(_key(name), {member: ts})
        pipe.zremrangebyscore(_key(name), 0, cutoff)
        pipe.zremrangebyrank(_key(name), 0, -HISTORY_MAX_SAMPLES - 1)
        pipe.sadd(HISTORY_INDEX, name)
    await pipe.execute()


async def fetch(name: str, since: float | None = None) -> List[Tuple[float, Any]]:
    """Return ``[(timestamp, value), ...]`` for the named series, oldest first."""
    min_score = since if since is not None else 0
    raw = await broker_client.zrangebyscore(_key(name), min_score, "+inf", withscores=True)
    out: List[Tuple[float, Any]] = []
    for member, score in raw:
        # member encoded as "<ts>:<json>"
        _, _, payload = member.partition(":")
        try:
            value = json.loads(payload)
        except json.JSONDecodeError:
            value = payload
        out.append((score, value))
    return out


async def list_series() -> List[str]:
    members = await broker_client.smembers(HISTORY_INDEX)
    return sorted(members)

"""Drain per-stream task duration queues from Redis and store aggregates.

Workers push completion durations onto ``monitor:task_durations:{stream}``
lists. The monitor runs this aggregator on its own cadence (default 30s),
drains each list atomically, computes percentiles for the window, and writes
one row per stream into ``monitor_task_latency`` in Postgres.

We use exact percentiles instead of t-digest because each 30s bucket is
small enough (at most ``_DURATION_QUEUE_CAP`` entries per stream) to sort in
memory. Aggregation cost stays inside the monitor process; workers do a
single LPUSH per completed task.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple

from shepherd_utils.broker import broker_client

from . import storage

logger = logging.getLogger("shepherd.monitor.latency")

DURATION_KEY_PREFIX = "monitor:task_durations"
DURATION_SCAN_PATTERN = f"{DURATION_KEY_PREFIX}:*"
LATENCY_AGGREGATE_INTERVAL_SEC = 30


def _percentile(sorted_values: List[float], p: float) -> Optional[float]:
    if not sorted_values:
        return None
    n = len(sorted_values)
    idx = max(0, min(n - 1, int(round(p / 100.0 * (n - 1)))))
    return sorted_values[idx]


async def _drain_stream(stream: str) -> List[float]:
    """Atomically read and clear the duration list for a stream."""
    key = f"{DURATION_KEY_PREFIX}:{stream}"
    try:
        pipe = broker_client.pipeline()
        pipe.lrange(key, 0, -1)
        pipe.delete(key)
        results = await pipe.execute()
    except Exception as e:
        logger.debug(f"Drain failed for {key}: {e}")
        return []
    raw = results[0] if results else []
    out: List[float] = []
    for v in raw:
        try:
            out.append(float(v))
        except (TypeError, ValueError):
            continue
    return out


async def _list_streams() -> List[str]:
    streams: List[str] = []
    async for key in broker_client.scan_iter(match=DURATION_SCAN_PATTERN, count=200):
        prefix_len = len(DURATION_KEY_PREFIX) + 1  # account for ":"
        if len(key) > prefix_len:
            streams.append(key[prefix_len:])
    return streams


def _summarize(durations: List[float]) -> Dict[str, float]:
    durations.sort()
    n = len(durations)
    return {
        "count": n,
        "mean_ms": sum(durations) / n,
        "p50_ms": _percentile(durations, 50),
        "p90_ms": _percentile(durations, 90),
        "p95_ms": _percentile(durations, 95),
        "p99_ms": _percentile(durations, 99),
        "min_ms": durations[0],
        "max_ms": durations[-1],
    }


async def aggregate_once() -> List[Tuple[str, Dict[str, float]]]:
    """Run one pass; safe to call from an admin endpoint."""
    streams = await _list_streams()
    if not streams:
        return []
    ts = time.time()
    out: List[Tuple[str, Dict[str, float]]] = []
    for stream in streams:
        durations = await _drain_stream(stream)
        if not durations:
            continue
        summary = _summarize(durations)
        try:
            await storage.insert_latency_aggregate(
                stream=stream,
                unix_ts=ts,
                count=int(summary["count"]),
                mean_ms=summary["mean_ms"],
                p50_ms=summary["p50_ms"],
                p90_ms=summary["p90_ms"],
                p95_ms=summary["p95_ms"],
                p99_ms=summary["p99_ms"],
                min_ms=summary["min_ms"],
                max_ms=summary["max_ms"],
            )
        except Exception as e:
            logger.warning(f"Failed to persist latency for {stream}: {e}")
            continue
        out.append((stream, summary))
    return out


async def aggregator_loop() -> None:
    while True:
        try:
            results = await aggregate_once()
            if results:
                total = sum(int(s["count"]) for _, s in results)
                logger.info(
                    f"Latency aggregate: {total} tasks across {len(results)} streams"
                )
        except Exception as e:
            logger.exception(f"Latency aggregator failed: {e}")
        await asyncio.sleep(LATENCY_AGGREGATE_INTERVAL_SEC)

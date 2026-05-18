"""Postgres-backed historical archive for the monitor dashboard.

The live dashboard reads from Redis (recent, fast). This module owns the 30-day
durable archive used by the History tab:

* ``monitor_metrics`` -- generic time-series, one row per (metric, ts).
* ``monitor_events`` -- discrete events (crashes, scale changes, alerts).
* ``monitor_task_latency`` -- per-stream task duration aggregates.

On monitor startup ``ensure_schema`` runs ``CREATE TABLE IF NOT EXISTS`` so
existing Postgres deployments pick up the new tables without a fresh init.
Query helpers automatically downsample by time bucket based on window size
so the History UI can render a 30-day chart without pulling 90k points.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from shepherd_utils.db import pool as pg_pool

logger = logging.getLogger("shepherd.monitor.storage")


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS monitor_metrics (
  ts TIMESTAMPTZ NOT NULL,
  metric TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (metric, ts)
);
CREATE INDEX IF NOT EXISTS idx_monitor_metrics_ts ON monitor_metrics (ts);

CREATE TABLE IF NOT EXISTS monitor_events (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  type TEXT NOT NULL,
  worker TEXT,
  severity TEXT,
  detail TEXT,
  payload JSONB
);
CREATE INDEX IF NOT EXISTS idx_monitor_events_ts ON monitor_events (ts);
CREATE INDEX IF NOT EXISTS idx_monitor_events_type_ts ON monitor_events (type, ts);

CREATE TABLE IF NOT EXISTS monitor_task_latency (
  ts TIMESTAMPTZ NOT NULL,
  stream TEXT NOT NULL,
  count INT NOT NULL,
  mean_ms DOUBLE PRECISION,
  p50_ms DOUBLE PRECISION,
  p90_ms DOUBLE PRECISION,
  p95_ms DOUBLE PRECISION,
  p99_ms DOUBLE PRECISION,
  min_ms DOUBLE PRECISION,
  max_ms DOUBLE PRECISION,
  PRIMARY KEY (stream, ts)
);
CREATE INDEX IF NOT EXISTS idx_monitor_latency_ts ON monitor_task_latency (ts);
"""


async def ensure_schema() -> None:
    """Idempotent table creation. Safe to call repeatedly on startup."""
    try:
        async with pg_pool.connection(10) as conn:
            async with conn.cursor() as cur:
                await cur.execute(SCHEMA_SQL)
            await conn.commit()
        logger.info("Monitor history tables ensured")
    except Exception as e:
        logger.error(f"Failed to ensure monitor history schema: {e}")


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def _ts(unix_seconds: float) -> datetime:
    return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)


async def insert_metrics(samples: Dict[str, Any], unix_ts: float) -> None:
    """Persist a tick's worth of metric samples in one batch."""
    if not samples:
        return
    ts = _ts(unix_ts)
    rows = []
    for name, value in samples.items():
        try:
            rows.append((ts, name, float(value)))
        except (TypeError, ValueError):
            continue
    if not rows:
        return
    try:
        async with pg_pool.connection(10) as conn:
            async with conn.cursor() as cur:
                # ON CONFLICT covers the rare case where the same tick fires
                # twice (e.g. /api/snapshot called in parallel).
                await cur.executemany(
                    "INSERT INTO monitor_metrics (ts, metric, value) VALUES (%s, %s, %s) "
                    "ON CONFLICT (metric, ts) DO NOTHING",
                    rows,
                )
            await conn.commit()
    except Exception as e:
        logger.warning(f"insert_metrics failed: {e}")


async def insert_event(
    event_type: str,
    worker: Optional[str],
    severity: Optional[str],
    detail: Optional[str],
    payload: Optional[Dict[str, Any]] = None,
    unix_ts: Optional[float] = None,
) -> None:
    ts = _ts(unix_ts if unix_ts is not None else time.time())
    try:
        async with pg_pool.connection(10) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO monitor_events (ts, type, worker, severity, detail, payload) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        ts,
                        event_type,
                        worker,
                        severity,
                        detail,
                        json.dumps(payload) if payload is not None else None,
                    ),
                )
            await conn.commit()
    except Exception as e:
        logger.warning(f"insert_event failed: {e}")


async def insert_latency_aggregate(
    stream: str,
    unix_ts: float,
    count: int,
    mean_ms: float,
    p50_ms: float,
    p90_ms: float,
    p95_ms: float,
    p99_ms: float,
    min_ms: float,
    max_ms: float,
) -> None:
    ts = _ts(unix_ts)
    try:
        async with pg_pool.connection(10) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO monitor_task_latency (ts, stream, count, mean_ms, "
                    "p50_ms, p90_ms, p95_ms, p99_ms, min_ms, max_ms) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (stream, ts) DO NOTHING",
                    (
                        ts,
                        stream,
                        count,
                        mean_ms,
                        p50_ms,
                        p90_ms,
                        p95_ms,
                        p99_ms,
                        min_ms,
                        max_ms,
                    ),
                )
            await conn.commit()
    except Exception as e:
        logger.warning(f"insert_latency_aggregate failed: {e}")


# ---------------------------------------------------------------------------
# Readers
#
# All read helpers auto-downsample to roughly 200 points per series based on
# the window size, so a 30-day query doesn't return 90k points per metric.
# Bucketing uses ``to_timestamp(floor(epoch / N) * N)`` since date_trunc only
# supports specific units.
# ---------------------------------------------------------------------------


def _choose_bucket_seconds(since: float, until: float) -> int:
    """Pick a sensible bucket so a chart has ~200 points."""
    span = max(60.0, until - since)
    target_points = 200
    raw = span / target_points
    # Round up to the next "natural" bucket size.
    for s in (30, 60, 300, 600, 900, 1800, 3600, 7200, 14400, 43200, 86400):
        if raw <= s:
            return s
    return 86400


def _bucket_expr(seconds: int) -> str:
    if seconds <= 30:
        # Raw resolution; no bucketing.
        return "ts"
    return f"to_timestamp(floor(EXTRACT(EPOCH FROM ts) / {seconds}) * {seconds})"


async def query_metrics(
    metrics: Iterable[str],
    since: float,
    until: float,
    bucket_seconds: Optional[int] = None,
) -> Dict[str, List[Tuple[float, float]]]:
    """Return ``{metric: [(unix_ts, value), ...]}`` aggregated into buckets."""
    metric_list = [m for m in metrics if m]
    if not metric_list:
        return {}
    bucket = bucket_seconds or _choose_bucket_seconds(since, until)
    bucket_sql = _bucket_expr(bucket)
    sql = (
        f"SELECT {bucket_sql} AS bucket, metric, AVG(value)::double precision AS v "
        "FROM monitor_metrics "
        "WHERE metric = ANY(%s) AND ts >= to_timestamp(%s) AND ts < to_timestamp(%s) "
        "GROUP BY bucket, metric "
        "ORDER BY bucket"
    )
    out: Dict[str, List[Tuple[float, float]]] = {m: [] for m in metric_list}
    try:
        async with pg_pool.connection(20) as conn:
            cur = await conn.execute(sql, (metric_list, since, until))
            rows = await cur.fetchall()
    except Exception as e:
        logger.warning(f"query_metrics failed: {e}")
        return out
    for bucket_ts, metric, value in rows:
        out.setdefault(metric, []).append((bucket_ts.timestamp(), float(value)))
    return out


async def query_metrics_by_prefix(
    prefix: str,
    since: float,
    until: float,
    bucket_seconds: Optional[int] = None,
) -> Dict[str, List[Tuple[float, float]]]:
    """Same shape as ``query_metrics`` but matches by metric name prefix."""
    if not prefix:
        return {}
    bucket = bucket_seconds or _choose_bucket_seconds(since, until)
    bucket_sql = _bucket_expr(bucket)
    sql = (
        f"SELECT {bucket_sql} AS bucket, metric, AVG(value)::double precision AS v "
        "FROM monitor_metrics "
        "WHERE metric LIKE %s AND ts >= to_timestamp(%s) AND ts < to_timestamp(%s) "
        "GROUP BY bucket, metric "
        "ORDER BY bucket"
    )
    out: Dict[str, List[Tuple[float, float]]] = {}
    try:
        async with pg_pool.connection(20) as conn:
            cur = await conn.execute(sql, (prefix + "%", since, until))
            rows = await cur.fetchall()
    except Exception as e:
        logger.warning(f"query_metrics_by_prefix failed: {e}")
        return out
    for bucket_ts, metric, value in rows:
        out.setdefault(metric, []).append((bucket_ts.timestamp(), float(value)))
    return out


async def query_latency(
    streams: Optional[Iterable[str]],
    since: float,
    until: float,
    bucket_seconds: Optional[int] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Return ``{stream: [{ts, count, mean, p50, p90, p95, p99, min, max}, ...]}``."""
    bucket = bucket_seconds or _choose_bucket_seconds(since, until)
    bucket_sql = _bucket_expr(bucket)
    params: List[Any] = [since, until]
    sql = (
        f"SELECT {bucket_sql} AS bucket, stream, "
        "SUM(count) AS count, "
        "AVG(mean_ms) AS mean_ms, "
        "AVG(p50_ms) AS p50_ms, "
        "AVG(p90_ms) AS p90_ms, "
        "AVG(p95_ms) AS p95_ms, "
        "AVG(p99_ms) AS p99_ms, "
        "MIN(min_ms) AS min_ms, "
        "MAX(max_ms) AS max_ms "
        "FROM monitor_task_latency "
        "WHERE ts >= to_timestamp(%s) AND ts < to_timestamp(%s) "
    )
    stream_list = list(streams) if streams else []
    if stream_list:
        sql += "AND stream = ANY(%s) "
        params.append(stream_list)
    sql += "GROUP BY bucket, stream ORDER BY bucket"

    out: Dict[str, List[Dict[str, Any]]] = {}
    try:
        async with pg_pool.connection(20) as conn:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()
    except Exception as e:
        logger.warning(f"query_latency failed: {e}")
        return out
    for row in rows:
        bucket_ts, stream, count, mean_ms, p50, p90, p95, p99, mn, mx = row
        out.setdefault(stream, []).append(
            {
                "ts": bucket_ts.timestamp(),
                "count": int(count or 0),
                "mean_ms": float(mean_ms) if mean_ms is not None else None,
                "p50_ms": float(p50) if p50 is not None else None,
                "p90_ms": float(p90) if p90 is not None else None,
                "p95_ms": float(p95) if p95 is not None else None,
                "p99_ms": float(p99) if p99 is not None else None,
                "min_ms": float(mn) if mn is not None else None,
                "max_ms": float(mx) if mx is not None else None,
            }
        )
    return out


async def query_events(
    since: float,
    until: float,
    type_filter: Optional[str] = None,
    severity_filter: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    params: List[Any] = [since, until]
    sql = (
        "SELECT id, ts, type, worker, severity, detail, payload "
        "FROM monitor_events "
        "WHERE ts >= to_timestamp(%s) AND ts < to_timestamp(%s) "
    )
    if type_filter:
        sql += "AND type = %s "
        params.append(type_filter)
    if severity_filter:
        sql += "AND severity = %s "
        params.append(severity_filter)
    sql += "ORDER BY ts DESC LIMIT %s"
    params.append(limit)

    out: List[Dict[str, Any]] = []
    try:
        async with pg_pool.connection(20) as conn:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()
    except Exception as e:
        logger.warning(f"query_events failed: {e}")
        return out
    for row in rows:
        _id, ts, etype, worker, severity, detail, payload = row
        out.append(
            {
                "id": _id,
                "ts": ts.timestamp(),
                "type": etype,
                "worker": worker,
                "severity": severity,
                "detail": detail,
                "payload": payload,
            }
        )
    return out


async def query_summary(since: float, until: float) -> Dict[str, Any]:
    """Top-line stats for the History tab header."""
    summary: Dict[str, Any] = {
        "since": since,
        "until": until,
        "queries_started": 0,
        "crashes": 0,
        "scale_events": 0,
        "alert_count": 0,
        "peak_backlog": {},
        "task_volume": {},
    }
    try:
        async with pg_pool.connection(20) as conn:
            # Total queries started in the window (from shepherd_brain).
            cur = await conn.execute(
                "SELECT COUNT(*) FROM shepherd_brain "
                "WHERE start_time >= to_timestamp(%s) AND start_time < to_timestamp(%s)",
                (since, until),
            )
            row = await cur.fetchone()
            summary["queries_started"] = int(row[0] or 0)

            # Event counts.
            cur = await conn.execute(
                "SELECT type, COUNT(*) FROM monitor_events "
                "WHERE ts >= to_timestamp(%s) AND ts < to_timestamp(%s) "
                "GROUP BY type",
                (since, until),
            )
            for etype, count in await cur.fetchall():
                if etype == "crash":
                    summary["crashes"] = int(count)
                elif etype in ("scale_up", "scale_down"):
                    summary["scale_events"] = summary.get("scale_events", 0) + int(
                        count
                    )
                elif etype == "alert":
                    summary["alert_count"] = int(count)

            # Peak backlog per stream.
            cur = await conn.execute(
                "SELECT metric, MAX(value) FROM monitor_metrics "
                "WHERE metric LIKE 'xlen:%%' AND ts >= to_timestamp(%s) AND ts < to_timestamp(%s) "
                "GROUP BY metric "
                "ORDER BY MAX(value) DESC LIMIT 10",
                (since, until),
            )
            for metric, peak in await cur.fetchall():
                stream = metric.split(":", 1)[1]
                summary["peak_backlog"][stream] = int(peak or 0)

            # Total task volume per stream (sum of count column).
            cur = await conn.execute(
                "SELECT stream, SUM(count) FROM monitor_task_latency "
                "WHERE ts >= to_timestamp(%s) AND ts < to_timestamp(%s) "
                "GROUP BY stream "
                "ORDER BY SUM(count) DESC LIMIT 20",
                (since, until),
            )
            for stream, total in await cur.fetchall():
                summary["task_volume"][stream] = int(total or 0)
    except Exception as e:
        logger.warning(f"query_summary failed: {e}")
        summary["error"] = str(e)
    return summary


# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------


async def purge_older_than(days: int) -> Dict[str, int]:
    """Delete rows older than ``days`` days from each history table."""
    cutoff_sql = f"NOW() - INTERVAL '{int(days)} days'"
    deletions: Dict[str, int] = {}
    try:
        async with pg_pool.connection(60) as conn:
            for table in ("monitor_metrics", "monitor_events", "monitor_task_latency"):
                cur = await conn.execute(f"DELETE FROM {table} WHERE ts < {cutoff_sql}")
                deletions[table] = cur.rowcount or 0
            await conn.commit()
        total = sum(deletions.values())
        if total:
            logger.info(f"Purged {total} history rows older than {days}d: {deletions}")
    except Exception as e:
        logger.warning(f"purge_older_than failed: {e}")
    return deletions

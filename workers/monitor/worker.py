"""Shepherd monitoring dashboard worker.

Combines:
  - A periodic poller that snapshots Redis and Postgres state.
  - An alert engine that evaluates rules on each snapshot and dispatches
    Slack/email notifications with cooldowns.
  - A FastAPI app that serves a single-page dashboard, JSON endpoints for
    one-shot snapshots and historical timeseries, and a websocket that pushes
    each new snapshot to connected clients.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from shepherd_utils.config import settings
from shepherd_utils.db import initialize_db, shutdown_db
from shepherd_utils.logger import setup_logging

from . import alerts, history, janitor, latency, poller, storage

setup_logging()
logger = logging.getLogger("shepherd.monitor")

STATIC_DIR = Path(__file__).parent / "static"


def _root_path() -> str:
    """Normalize MONITOR_ROOT_PATH into a clean prefix (no trailing slash)."""
    rp = os.environ.get("MONITOR_ROOT_PATH", "").strip()
    if not rp:
        return ""
    if not rp.startswith("/"):
        rp = "/" + rp
    return rp.rstrip("/")


def _base_href() -> str:
    """``<base href>`` value for the HTML templates -- always trailing-slash.

    Local dev: empty MONITOR_ROOT_PATH → "/" → app served at the document root.
    Behind an ingress prefix: e.g. "/monitor" → "/monitor/" so relative links
    in the HTML and JS resolve correctly via ``document.baseURI``.
    """
    rp = _root_path()
    return (rp or "") + "/"


ROOT_PATH = _root_path()

# In-process state shared by the poller loop and websocket clients.
_clients: Set[WebSocket] = set()
_clients_lock = asyncio.Lock()
_latest_snapshot: Dict[str, Any] = {}


async def _broadcast(payload: Dict[str, Any]) -> None:
    """Send to every connected websocket; drop ones that fail."""
    async with _clients_lock:
        targets = list(_clients)
    if not targets:
        return
    message = json.dumps(payload, default=str)
    dead: List[WebSocket] = []
    for ws in targets:
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    if dead:
        async with _clients_lock:
            for ws in dead:
                _clients.discard(ws)


async def _poll_loop(engine: alerts.AlertEngine) -> None:
    global _latest_snapshot
    interval = max(0.5, settings.monitor_poll_interval_sec)
    history_interval = max(interval, settings.monitor_history_interval_sec)
    last_history_write = 0.0
    while True:
        try:
            snapshot = await poller.collect_snapshot()
            _latest_snapshot = snapshot
            # Persist history at a slower cadence than the live UI tick to
            # keep Redis memory bounded.
            if snapshot["ts"] - last_history_write >= history_interval:
                await poller.write_history(snapshot)
                last_history_write = snapshot["ts"]
            fired = await engine.evaluate(snapshot)
            if fired:
                snapshot = {**snapshot, "new_alerts": fired}
            await _broadcast({"type": "snapshot", "data": snapshot})
        except Exception as e:
            logger.exception(f"Poll loop iteration failed: {e}")
        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await initialize_db()
    # Self-heal the schema so the History tab works on existing deployments
    # (Postgres only runs docker-entrypoint-initdb.d scripts on a fresh data
    # dir, which means upgrading an existing instance wouldn't see the new
    # tables without this).
    await storage.ensure_schema()
    rules = alerts.load_rules(settings.monitor_alerts_config)
    engine = alerts.AlertEngine(rules)
    poll_task = asyncio.create_task(_poll_loop(engine))
    janitor_task = asyncio.create_task(janitor.janitor_loop())
    latency_task = asyncio.create_task(latency.aggregator_loop())
    try:
        yield
    finally:
        for t in (poll_task, janitor_task, latency_task):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        await shutdown_db()


APP = FastAPI(lifespan=lifespan, title="Shepherd Monitor", root_path=ROOT_PATH)

if STATIC_DIR.exists():
    APP.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# Templated HTML cache: read once, substitute the base-href placeholder on
# every request. ``{{BASE_HREF}}`` is the only template variable; substitution
# happens at serve time so swapping MONITOR_ROOT_PATH at deploy time works
# without rebuilding the image.
_HTML_TEMPLATE_CACHE: Dict[str, str] = {}


def _serve_templated_html(filename: str) -> Any:
    cached = _HTML_TEMPLATE_CACHE.get(filename)
    if cached is None:
        path = STATIC_DIR / filename
        if not path.exists():
            return JSONResponse(
                {"error": f"{filename} not found"}, status_code=500
            )
        cached = path.read_text(encoding="utf-8")
        _HTML_TEMPLATE_CACHE[filename] = cached
    return HTMLResponse(cached.replace("{{BASE_HREF}}", _base_href()))


@APP.get("/")
async def index():
    return _serve_templated_html("index.html")


@APP.get("/history")
async def history_page():
    """Historical dashboard tab."""
    return _serve_templated_html("history.html")


@APP.get("/api/health")
async def health():
    return {"ok": True}


# ----- Historical (Postgres-backed) endpoints --------------------------------


def _default_window() -> tuple[float, float]:
    import time as _time

    now = _time.time()
    return now - 24 * 3600, now


@APP.get("/api/historical/metrics")
async def api_historical_metrics(
    metric: str,
    since: float | None = None,
    until: float | None = None,
    bucket_seconds: int | None = None,
):
    default_since, default_until = _default_window()
    series = await storage.query_metrics(
        [m for m in metric.split(",") if m],
        since=since if since is not None else default_since,
        until=until if until is not None else default_until,
        bucket_seconds=bucket_seconds,
    )
    return {"series": series, "since": since, "until": until}


@APP.get("/api/historical/metrics_by_prefix")
async def api_historical_metrics_by_prefix(
    prefix: str,
    since: float | None = None,
    until: float | None = None,
    bucket_seconds: int | None = None,
):
    default_since, default_until = _default_window()
    series = await storage.query_metrics_by_prefix(
        prefix=prefix,
        since=since if since is not None else default_since,
        until=until if until is not None else default_until,
        bucket_seconds=bucket_seconds,
    )
    return {"series": series}


@APP.get("/api/historical/latency")
async def api_historical_latency(
    streams: str | None = None,
    since: float | None = None,
    until: float | None = None,
    bucket_seconds: int | None = None,
):
    default_since, default_until = _default_window()
    stream_list = [s for s in streams.split(",") if s] if streams else None
    series = await storage.query_latency(
        streams=stream_list,
        since=since if since is not None else default_since,
        until=until if until is not None else default_until,
        bucket_seconds=bucket_seconds,
    )
    return {"series": series}


@APP.get("/api/historical/events")
async def api_historical_events(
    since: float | None = None,
    until: float | None = None,
    type: str | None = None,
    severity: str | None = None,
    limit: int = 500,
):
    default_since, default_until = _default_window()
    events = await storage.query_events(
        since=since if since is not None else default_since,
        until=until if until is not None else default_until,
        type_filter=type,
        severity_filter=severity,
        limit=limit,
    )
    return {"events": events}


@APP.get("/api/historical/summary")
async def api_historical_summary(
    since: float | None = None,
    until: float | None = None,
):
    default_since, default_until = _default_window()
    return await storage.query_summary(
        since=since if since is not None else default_since,
        until=until if until is not None else default_until,
    )


@APP.get("/api/snapshot")
async def api_snapshot():
    if _latest_snapshot:
        return _latest_snapshot
    return await poller.collect_snapshot()


@APP.get("/api/history/{metric:path}")
async def api_history(metric: str, since: float | None = None):
    series = await history.fetch(metric, since=since)
    return {"metric": metric, "samples": series}


@APP.get("/api/history")
async def api_history_index():
    return {"series": await history.list_series()}


@APP.get("/api/alerts")
async def api_alerts(limit: int = 50):
    return {"alerts": await alerts.recent_alerts(limit=limit)}


@APP.post("/api/admin/cleanup")
async def api_admin_cleanup():
    """Run the janitor immediately. Returns what got trimmed/reaped."""
    return await janitor.run_once()


@APP.post("/api/admin/reclaim_dead_consumers")
async def api_admin_reclaim_dead_consumers(
    min_idle_seconds: int = 3600,
    dry_run: bool = False,
):
    """Drop pending messages stuck on dead consumers and remove the consumers.

    DESTRUCTIVE: the pending messages are discarded, not retried. Default
    ``min_idle_seconds=3600`` only touches consumers that have been silent for
    at least an hour. Pass ``dry_run=true`` first to preview what would be
    dropped.
    """
    return await janitor.reclaim_dead_consumers(
        min_idle_seconds=min_idle_seconds,
        dry_run=dry_run,
    )


@APP.post("/api/admin/forget_worker")
async def api_admin_forget_worker(name: str):
    """Remove a worker type from the dashboard's known-workers set.

    Use this for a worker that's been retired permanently. The next snapshot
    will drop its card. If a worker by that name reappears (heartbeats again),
    it'll be re-learned automatically.
    """
    from shepherd_utils.broker import broker_client

    pipe = broker_client.pipeline()
    pipe.srem("monitor:known_workers", name)
    pipe.delete(f"monitor:worker_state:{name}")
    await pipe.execute()
    return {"forgot": name}


@APP.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    async with _clients_lock:
        _clients.add(ws)
    try:
        # Send the most recent snapshot immediately so the page renders on connect.
        if _latest_snapshot:
            await ws.send_text(
                json.dumps({"type": "snapshot", "data": _latest_snapshot}, default=str)
            )
        while True:
            # We don't expect inbound messages, but keep the receive loop alive
            # so disconnects are noticed promptly.
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        async with _clients_lock:
            _clients.discard(ws)


def main() -> None:
    port = int(os.environ.get("MONITOR_PORT", settings.monitor_port))
    # ``root_path`` only affects how FastAPI constructs self-referential URLs
    # (OpenAPI / redirects). The ingress is expected to strip the prefix
    # before forwarding, so the route paths themselves are unprefixed.
    uvicorn.run(
        APP,
        host="0.0.0.0",
        port=port,
        log_level=settings.log_level.lower(),
        root_path=ROOT_PATH,
    )


if __name__ == "__main__":
    main()

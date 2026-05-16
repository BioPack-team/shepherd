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
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from shepherd_utils.config import settings
from shepherd_utils.db import initialize_db, shutdown_db
from shepherd_utils.logger import setup_logging

from . import alerts, history, janitor, poller

setup_logging()
logger = logging.getLogger("shepherd.monitor")

STATIC_DIR = Path(__file__).parent / "static"

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
    rules = alerts.load_rules(settings.monitor_alerts_config)
    engine = alerts.AlertEngine(rules)
    poll_task = asyncio.create_task(_poll_loop(engine))
    janitor_task = asyncio.create_task(janitor.janitor_loop())
    try:
        yield
    finally:
        for t in (poll_task, janitor_task):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        await shutdown_db()


APP = FastAPI(lifespan=lifespan, title="Shepherd Monitor")

if STATIC_DIR.exists():
    APP.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@APP.get("/")
async def index():
    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        return JSONResponse({"error": "dashboard UI not found"}, status_code=500)
    return FileResponse(str(index_path))


@APP.get("/api/health")
async def health():
    return {"ok": True}


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
    uvicorn.run(
        APP,
        host="0.0.0.0",
        port=port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()

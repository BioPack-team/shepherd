"""ARS subscriber / WebSocket fan-out service.

Subscribes to the Redis ``ars_events`` channel (published by ars_accumulate and
the watchdog), broadcasts each status event to connected WebSocket clients, and
POSTs it to any callback registered via ``/ars/subscribe/{pk}``. The broadcast
pattern mirrors the monitor worker's ``_broadcast``.
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Set

import httpx
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from shepherd_utils.ars_notify import subscribe_ars_events
from shepherd_utils.config import settings
from shepherd_utils.db import initialize_db, list_subscribers, shutdown_db
from shepherd_utils.logger import setup_logging

setup_logging()
logger = logging.getLogger("shepherd.ars_ws")

_clients: Set[WebSocket] = set()
_clients_lock = asyncio.Lock()


async def _broadcast(event: Dict[str, Any]) -> None:
    """Send an event to every connected websocket; drop ones that fail."""
    async with _clients_lock:
        targets = list(_clients)
    if not targets:
        return
    message = json.dumps(event, default=str)
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


async def _notify_subscribers(event: Dict[str, Any]) -> None:
    """POST the event to any callback subscribed to this parent query."""
    parent_qid = event.get("parent_qid")
    if not parent_qid:
        return
    try:
        urls = await list_subscribers(parent_qid, logger)
    except Exception as e:
        logger.error(f"Failed to load subscribers for {parent_qid}: {e}")
        return
    if not urls:
        return
    async with httpx.AsyncClient(timeout=30) as client:
        for url in urls:
            try:
                await client.post(url, json=event)
            except Exception as e:
                logger.error(f"Failed to notify subscriber {url}: {e}")


async def _event_loop() -> None:
    """Consume ARS status events and fan them out."""
    while True:
        try:
            async for event in subscribe_ars_events():
                await _broadcast(event)
                await _notify_subscribers(event)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"ARS event loop failed; retrying: {e}", exc_info=True)
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await initialize_db()
    event_task = asyncio.create_task(_event_loop())
    try:
        yield
    finally:
        event_task.cancel()
        try:
            await event_task
        except asyncio.CancelledError:
            pass
        await shutdown_db()


APP = FastAPI(title="Shepherd ARS WebSocket", lifespan=lifespan)


@APP.get("/api/health")
async def health():
    return {"ok": True}


async def ws_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    async with _clients_lock:
        _clients.add(websocket)
    try:
        while True:
            # We only push; keep the socket open until the client disconnects.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with _clients_lock:
            _clients.discard(websocket)


APP.add_api_websocket_route("/ws", ws_endpoint)


if __name__ == "__main__":
    uvicorn.run(APP, host="0.0.0.0", port=settings.ars_ws_port)

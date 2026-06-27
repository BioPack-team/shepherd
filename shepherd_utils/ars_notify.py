"""Lightweight pub/sub for ARS status events.

``ars_accumulate`` (and the watchdog) publish status transitions to a Redis
channel; the ``ars_ws`` service subscribes, broadcasts to connected websocket
clients, and POSTs to registered subscriber callbacks. Reuses the broker's
``lock_client`` (db 2, ``decode_responses=True``) for pub/sub.
"""

import json
import logging
from typing import Any, AsyncGenerator, Dict

from shepherd_utils.broker import lock_client
from shepherd_utils.config import settings


async def publish_ars_event(event: Dict[str, Any], logger: logging.Logger) -> None:
    """Publish a status event. Best-effort: never raises into the caller."""
    try:
        await lock_client.publish(settings.ars_events_channel, json.dumps(event))
    except Exception as e:
        logger.error(f"Failed to publish ARS event: {e}")


async def subscribe_ars_events() -> AsyncGenerator[Dict[str, Any], None]:
    """Yield ARS status events as they are published."""
    pubsub = lock_client.pubsub()
    await pubsub.subscribe(settings.ars_events_channel)
    try:
        async for message in pubsub.listen():
            if message.get("type") != "message":
                continue
            data = message.get("data")
            try:
                yield json.loads(data)
            except (TypeError, ValueError):
                continue
    finally:
        await pubsub.unsubscribe(settings.ars_events_channel)
        await pubsub.aclose()

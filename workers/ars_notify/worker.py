"""ARS notification worker.

Delivers event notifications to subscribed clients. Port of
NCATSTranslator/Relay @ 3e65975 tasks.py notify_subscribers_task +
notify_one_client_task: the payload base is {pk, timestamp, code} plus the
event fields (built upstream in Message.notify_subscribers, here by
shepherd_utils.ars.notify before the task was enqueued);
last_merged_completed forces code 200; one POST per subscribed client, body
= compact sorted-key JSON, signed with HMAC-SHA256 of the client's
AES-decrypted secret in x-event-signature; failures retried with
exponential backoff (cap 300s, jitter, max 8 attempts).
"""

import asyncio
import datetime
import json
import logging
import random
import uuid

import httpx

import shepherd_utils.ars.db as ars_db
from shepherd_utils.ars import crypto
from shepherd_utils.broker import mark_task_as_complete
from shepherd_utils.db import save_logs
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks

STREAM = "ars.notify"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

MAX_RETRIES = 8
BACKOFF_CAP_SEC = 300.0


async def notify_one_client(client, notification, logger: logging.Logger):
    """notify_one_client_task with in-process retries."""
    callback = client["callback_url"]
    secret = crypto.decrypt_secret(client["client_secret"], crypto.master_key())
    body, digest = crypto.notification_body_and_signature(notification, secret)
    headers = {"Content-Type": "application/json", "x-event-signature": digest}
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Notifying client {client['id']} at {callback}")
            async with httpx.AsyncClient(timeout=10) as http_client:
                r = await http_client.post(url=callback, content=body, headers=headers)
            if r.status_code == 200:
                return
            logger.warning(
                f"notify failed: status={r.status_code}, body={r.text[:200]}"
            )
        except httpx.HTTPError as e:
            logger.warning(f"notify failed: {e}")
        if attempt < MAX_RETRIES - 1:
            backoff = min(BACKOFF_CAP_SEC, 2**attempt) * random.uniform(0.8, 1.2)
            await asyncio.sleep(backoff)
    logger.error(
        f"Giving up notifying client {client['id']} after {MAX_RETRIES} attempts"
    )


async def ars_notify(task, logger: logging.Logger):
    message_pk = task[1]["message_pk"]
    code = int(task[1].get("code", 200))
    try:
        fields = json.loads(task[1].get("fields", "null"))
    except (json.JSONDecodeError, TypeError):
        fields = None
    notification = {
        "pk": str(message_pk),
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "code": code,
    }
    if fields:
        for k, v in fields.items():
            if k == "event_type" and v == "last_merged_completed":
                notification["code"] = 200
            notification[k] = v
    clients = await ars_db.get_subscribed_clients(message_pk)
    logger.info(
        f"Sending notification for {message_pk} to {len(clients)} client(s): "
        f"{notification.get('event_type')}"
    )
    # one delivery per client so one slow/bad callback doesn't block others
    await asyncio.gather(
        *(notify_one_client(client, notification, logger) for client in clients)
    )


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    with tracer.start_as_current_span(STREAM, context=parent_ctx):
        try:
            await ars_notify(task, logger)
        except Exception as e:
            logger.error(f"Task {task[0]} failed: {e}", exc_info=True)
        finally:
            try:
                await mark_task_as_complete(STREAM, GROUP, task[0], logger)
            except Exception as e:
                logger.error(f"Task {task[0]}: failed to ack: {e}")
            await save_logs(task[1].get("message_pk", "ars"), logger)
            limiter.release()


async def poll_for_tasks():
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, TASK_LIMIT
            ):
                asyncio.create_task(process_task(task, parent_ctx, logger, limiter))
        except asyncio.CancelledError:
            LOGGER.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())

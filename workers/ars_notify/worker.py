"""ARS notification worker.

Delivers event notifications to subscribed clients. Port of
NCATSTranslator/Relay @ 3e65975 tasks.py notify_subscribers_task +
notify_one_client_task: the payload base is {pk, timestamp, code} plus the
event fields (built upstream in Message.notify_subscribers, here by
shepherd_utils.ars.notify before the task was enqueued);
last_merged_completed forces code 200; one POST per subscribed client, body
= compact sorted-key JSON, signed with HMAC-SHA256 of the client's
AES-decrypted secret in x-event-signature; failures retried with
exponential backoff (cap 300s, jitter, max 8 attempts, bounded by
``ars_notify_max_delivery_sec``).

Deliveries are detached from the stream task and run against their own
in-flight budget (``ars_notify_max_inflight``). Retrying inline meant a
single unreachable callback pinned a TASK_LIMIT slot for the whole backoff
ladder; notifications are best-effort either way (upstream drops them after
its own retry budget too), and this keeps one bad client from stalling
everyone else's events.

Recipients normally arrive in the task (``client_pks``), resolved when the
event was emitted: the completion path clears a parent's subscriptions
right after its final events, so resolving them here would find nobody.
Tasks without the field fall back to the subscriber list.
"""

import asyncio
import datetime
import json
import logging
import random
import uuid

from typing import Optional

import httpx

import shepherd_utils.ars.db as ars_db
from shepherd_utils.config import settings
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

# Deliveries run detached from the stream task that produced them. Retrying
# in-task meant one unreachable client callback held a TASK_LIMIT slot for the
# whole backoff ladder (~4 minutes), and a handful of dead callbacks stalled
# every other query's notifications behind them. The stream now drains at
# queue speed and deliveries drain against their own budget.
_inflight: set = set()
_delivery_slots: Optional[asyncio.Semaphore] = None


def _slots() -> asyncio.Semaphore:
    """The in-flight delivery budget, created on the running loop."""
    global _delivery_slots
    if _delivery_slots is None:
        _delivery_slots = asyncio.Semaphore(settings.ars_notify_max_inflight)
    return _delivery_slots


def _spawn_delivery(coro) -> None:
    """Run a delivery detached, keeping a strong reference so it is not
    garbage collected mid-flight."""
    task = asyncio.create_task(coro)
    _inflight.add(task)
    task.add_done_callback(_inflight.discard)


async def drain_deliveries(timeout: Optional[float] = None) -> None:
    """Wait for the detached deliveries to finish.

    Used on shutdown so a clean stop still flushes what it has in flight,
    and by the tests, which need a join point now that ``ars_notify`` hands
    its deliveries off instead of awaiting them.
    """
    while _inflight:
        pending = set(_inflight)
        _, still = await asyncio.wait(pending, timeout=timeout)
        if still:
            return


async def notify_one_client(client, notification, logger: logging.Logger):
    """notify_one_client_task, retried against its own time budget.

    Held open for at most ``ars_notify_max_delivery_sec``; a backoff that
    would run past the deadline ends the attempt ladder instead of sleeping
    through it.
    """
    callback = client["callback_url"]
    try:
        secret = crypto.decrypt_secret(client["client_secret"], crypto.master_key())
    except Exception as e:
        logger.error(f"Cannot decrypt secret for client {client['id']}: {e}")
        return
    body, digest = crypto.notification_body_and_signature(notification, secret)
    headers = {"Content-Type": "application/json", "x-event-signature": digest}
    deadline = asyncio.get_running_loop().time() + settings.ars_notify_max_delivery_sec
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
        if attempt >= MAX_RETRIES - 1:
            break
        backoff = min(BACKOFF_CAP_SEC, 2**attempt) * random.uniform(0.8, 1.2)
        if asyncio.get_running_loop().time() + backoff >= deadline:
            logger.error(
                f"Giving up notifying client {client['id']}: delivery budget "
                f"({settings.ars_notify_max_delivery_sec:.0f}s) exhausted"
            )
            return
        await asyncio.sleep(backoff)
    logger.error(
        f"Giving up notifying client {client['id']} after {MAX_RETRIES} attempts"
    )


async def _deliver(client, notification, logger: logging.Logger):
    """One detached delivery, against the shared in-flight budget."""
    async with _slots():
        try:
            await notify_one_client(client, notification, logger)
        except Exception as e:
            logger.error(f"Delivery to client {client['id']} failed: {e}")


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
    raw_pks = task[1].get("client_pks")
    if raw_pks is not None:
        # recipients were resolved when the event was emitted (before the
        # completion path cleared the subscriptions) or the event is a
        # replay addressed to one late subscriber
        try:
            client_pks = [int(pk) for pk in json.loads(raw_pks)]
        except (json.JSONDecodeError, TypeError, ValueError):
            client_pks = []
        clients = []
        for pk in client_pks:
            client = await ars_db.get_client_by_pk(pk)
            if client is not None:
                clients.append(client)
    else:
        clients = await ars_db.get_subscribed_clients(message_pk)
    logger.info(
        f"Sending notification for {message_pk} to {len(clients)} client(s): "
        f"{notification.get('event_type')}"
    )
    # Detached, one per client: the stream task is done once the deliveries
    # are handed off, so a slow or unreachable callback delays neither its
    # siblings nor the next notification off the queue. They log to the
    # worker logger, not the per-query one, because the task's log buffer is
    # flushed and dropped as soon as this returns.
    for client in clients:
        _spawn_delivery(_deliver(client, notification, LOGGER))


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
            await drain_deliveries(timeout=settings.ars_notify_drain_sec)
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())

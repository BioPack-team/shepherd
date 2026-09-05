"""ARS merge worker.

Folds a validated ARA result into the query's cumulative merged message.
Port of NCATSTranslator/Relay @ 3e65975 utils.py merge_and_post_process
(lock + bookkeeping) and merge_received (the fold): one merge at a time per
parent, a fresh merge-child row per fold, parent.merged_version /
merged_versions_list / params.stats advanced, a merged_version_begun
notification, then a hand-off to ars_postprocess.

Infrastructure substitutions (behavior-preserving): the DB merge_semaphore +
Celery-retry loop becomes a broker lock (per-parent mutual exclusion either
way; the semaphore column is still maintained for envelope parity), and the
fold runs in a process-pool child fetching blobs with the sync Redis client
so no payload crosses IPC (same pattern as the merge_message worker). A fold
failure leaves the shell merge-child Running, exactly like upstream's
swallowed merge_received exception -- the 8-minute watchdog 598s it.
"""

import asyncio
import json
import logging
import uuid

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
from shepherd_utils.ars.merge import (
    TranslatorMessage,
    get_msg_stats,
    mergeMessages,
)
from shepherd_utils.ars.notify import notify_subscribers
from shepherd_utils.broker import (
    acquire_lock,
    add_task,
    mark_task_as_complete,
    remove_lock,
)
from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.db import get_message_sync, save_logs, save_message_sync
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import get_tasks

STREAM = "ars.merge"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

_pool = None
_loop = None


def merge_in_child(current_pk, child_pk, new_pk):
    """Pool-side fold: fetch by id, merge, save once; only stats cross IPC.

    merge_received's data path: the incoming child blob's ``message`` part is
    the newcomer; when a merged version already exists its ``message`` part
    is folded in via mergeMessages, otherwise the newcomer becomes the first
    merged version.
    """
    to_merge = get_message_sync(str(child_pk))["message"]
    t_to_merge = TranslatorMessage(to_merge)
    if current_pk is not None:
        current = get_message_sync(str(current_pk))["message"]
        merged = mergeMessages([TranslatorMessage(current), t_to_merge], str(new_pk))
    else:
        merged = t_to_merge
    merged_dict = merged.to_dict()
    stats = get_msg_stats(merged_dict)
    save_message_sync(str(new_pk), merged_dict)
    return stats


async def _run_merge_in_pool(current_pk, child_pk, new_pk, logger):
    """Indirection for tests; production runs merge_in_child in the pool."""
    if _pool is not None and _loop is not None:
        return await _pool.run(_loop, merge_in_child, current_pk, child_pk, new_pk)
    return await asyncio.to_thread(merge_in_child, current_pk, child_pk, new_pk)


def _lock_key(parent_pk) -> str:
    return f"ars-merge:{parent_pk}"


async def ars_merge(task, logger: logging.Logger):
    parent_pk = task[1]["parent_pk"]
    child_pk = task[1]["child_pk"]
    agent_name = task[1]["agent_name"]

    got_lock = await acquire_lock(_lock_key(parent_pk), CONSUMER, logger)
    if not got_lock:
        # someone else is merging this parent: requeue, like celery retry
        logger.debug(f"[{child_pk}] merge lock busy for {parent_pk}; requeueing")
        await add_task(
            STREAM,
            {k: v for k, v in task[1].items() if k != "_started_at"},
            logger,
        )
        return

    merged_created = None
    try:
        parent = await ars_db.get_message_row(parent_pk)
        if parent is None:
            logger.error(f"Merge: parent {parent_pk} does not exist; skipping")
            return
        await ars_db.update_message(parent_pk, merge_semaphore=True)
        ars_actor = await lifecycle.ensure_ars_actor()
        merged_created = await ars_db.create_message(
            actor_id=ars_actor["id"], status="Running", code=202, ref=parent_pk
        )
        new_pk = merged_created["id"]
        current_pk = parent.get("merged_version")
        logger.info(
            f"Beginning merge for agent {agent_name} with current_pk: {current_pk}"
        )
        try:
            stats = await _run_merge_in_pool(
                str(current_pk) if current_pk else None,
                str(child_pk),
                str(new_pk),
                logger,
            )
        except Exception as e:
            # merge_received swallows and returns {} -- the shell merge child
            # stays Running for the watchdog to reap
            logger.error(f"problem with merging for {agent_name}: {e}", exc_info=True)
            await ars_db.update_message(parent_pk, merge_semaphore=False)
            return

        params = parent.get("params") or {}
        params["stats"] = stats
        mvl = parent.get("merged_versions_list")
        pk_infores_merge = [str(new_pk), agent_name]
        if mvl is None:
            mvl = [pk_infores_merge]
        else:
            mvl = list(mvl) + [pk_infores_merge]
        updated_parent = await ars_db.update_message(
            parent_pk,
            merged_version=str(new_pk),
            merged_versions_list=mvl,
            params=params,
            merge_semaphore=False,
        )
        await notify_subscribers(
            updated_parent if updated_parent else parent,
            {
                "event_type": "merged_version_begun",
                "complete": False,
                "merged_versions_list": mvl if mvl is not None else [],
            },
            logger,
        )
        await add_task(
            "ars.postprocess",
            {
                "merged_pk": str(new_pk),
                "parent_pk": str(parent_pk),
                "agent_name": agent_name,
                "stats": json.dumps(stats),
                "query_id": str(parent_pk),
                "otel": task[1].get("otel", "{}"),
            },
            logger,
        )
        logger.info(
            f"returning new_merged_message to be post processed with pk: {new_pk}"
        )
    finally:
        await remove_lock(_lock_key(parent_pk), CONSUMER, logger)


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    with tracer.start_as_current_span(STREAM, context=parent_ctx):
        try:
            await ars_merge(task, logger)
        except Exception as e:
            logger.error(f"Task {task[0]} failed: {e}", exc_info=True)
        finally:
            try:
                await mark_task_as_complete(STREAM, GROUP, task[0], logger)
            except Exception as e:
                logger.error(f"Task {task[0]}: failed to ack: {e}")
            await save_logs(task[1].get("parent_pk", "ars"), logger)
            limiter.release()


async def poll_for_tasks():
    global _pool, _loop
    _loop = asyncio.get_running_loop()
    max_workers = resolve_pool_workers(TASK_LIMIT, LOGGER)
    LOGGER.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    _pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="ars_merge process pool",
        task_timeout=settings.pool_task_timeout_sec,
    )
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, max_workers
            ):
                asyncio.create_task(process_task(task, parent_ctx, logger, limiter))
        except asyncio.CancelledError:
            LOGGER.info("Poll loop cancelled, shutting down.")
            _pool.shutdown()
            return
        except Exception as e:
            LOGGER.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())

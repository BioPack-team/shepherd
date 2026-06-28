"""Cross-ARA accumulator.

Folds each per-ARA response (posted back via ``/ars/callback`` and enqueued
here) into the parent query's accumulating message, serialized by a Redis lock
on the parent ``response_id``. When every ARA has reported back, a single winner
(via the ``ars_tail_launched`` latch) enqueues the post-merge tail workflow:
node normalization -> annotation -> answer appraiser -> blocklist -> filter ->
finish_query.

This is deliberately separate from ``merge_message`` (which does the
intra-ARA creative/lookup-rule merge): here we only need a plain knowledge-graph
merge plus result concatenation across ARAs.
"""

import asyncio
import json
import logging
import time
import uuid

from opentelemetry.trace import Status, StatusCode

from shepherd_utils.broker import (
    acquire_lock,
    add_task,
    mark_task_as_complete,
    remove_lock,
)
from shepherd_utils.db import (
    claim_ars_tail,
    get_message,
    get_pending_ars_children,
    save_message,
    set_ars_child_status,
)
from shepherd_utils.ars_merge import (
    average_result_scores,
    merge_aux_graphs,
    merge_result_maps,
)
from shepherd_utils.ars_notify import publish_ars_event
from shepherd_utils.ars_workflow import ARS_TAIL_WORKFLOW as TAIL_WORKFLOW
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, merge_kgraph

# Queue name
STREAM = "ars_accumulate"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)

def merge_child_into_parent(parent_msg, child_msg, source, logger):
    """Merge one child response into the accumulating parent message.

    Returns ``(parent_msg, child_result_count)``. Pure/CPU-bound so it can be run
    off the event loop via ``asyncio.to_thread``.
    """
    parent_message = parent_msg["message"]
    child_message = child_msg.get("message", {}) or {}

    parent_kg = parent_message.get("knowledge_graph") or {"nodes": {}, "edges": {}}
    child_kg = child_message.get("knowledge_graph") or {"nodes": {}, "edges": {}}
    parent_message["knowledge_graph"] = merge_kgraph(
        parent_kg, child_kg, source, logger
    )

    # Dedup identical answers across ARAs (Relay parity): results binding the
    # same node curies collapse into one, concatenating analyses and
    # accumulating differing score fields for later averaging.
    child_results = child_message.get("results") or []
    parent_message["results"] = merge_result_maps(
        parent_message.get("results") or [], child_results, logger
    )

    parent_aux = parent_message.get("auxiliary_graphs")
    if parent_aux is None:
        parent_aux = {}
        parent_message["auxiliary_graphs"] = parent_aux
    merge_aux_graphs(parent_aux, child_message.get("auxiliary_graphs") or {})

    return parent_msg, len(child_results)


async def ars_accumulate(task, logger: logging.Logger):
    """Merge one ARA's response into the parent and gate the post-merge tail."""
    parent_qid = task[1]["query_id"]
    parent_response_id = task[1]["response_id"]
    callback_id = task[1]["callback_id"]
    ara = task[1]["ara"]
    log_level = task[1].get("log_level", 20)
    otel = task[1]["otel"]

    got_lock = await acquire_lock(parent_response_id, CONSUMER, logger)
    if not got_lock:
        logger.error(
            f"Failed to lock {parent_response_id} for {ara}; discarding callback."
        )
        return

    try:
        parent_msg = await get_message(parent_response_id, logger)
        child_msg = await get_message(callback_id, logger)
        source = "infores:shepherd"
        parent_msg, result_count = await asyncio.to_thread(
            merge_child_into_parent, parent_msg, child_msg, source, logger
        )
        await save_message(parent_response_id, parent_msg, logger)
        logger.info(
            f"Merged {result_count} results from {ara} into parent {parent_qid}."
        )
    finally:
        await remove_lock(parent_response_id, CONSUMER, logger)

    # Mark this ARA done only after its results are safely merged, so the
    # completion gate below never fires before the last merge has landed.
    await set_ars_child_status(
        parent_qid, ara, "DONE", logger, result_count=result_count
    )

    pending = await get_pending_ars_children(parent_qid, logger)
    await publish_ars_event(
        {
            "parent_qid": parent_qid,
            "ara": ara,
            "status": "DONE",
            "result_count": result_count,
            "pending": pending,
        },
        logger,
    )
    if pending:
        logger.info(f"Parent {parent_qid} still waiting on ARAs: {pending}")
        return

    # All ARAs are in. Exactly one caller wins the latch and launches the tail.
    if await claim_ars_tail(parent_qid, logger):
        logger.info(
            f"All ARAs done for {parent_qid}; launching post-merge tail."
        )
        # Finalize once: average any accumulated normalized_score lists across
        # the deduped answers (Relay mergeMessagesRecursive finalize). The latch
        # guarantees a single winner, so this runs exactly once.
        got_final_lock = await acquire_lock(parent_response_id, CONSUMER, logger)
        if got_final_lock:
            try:
                final_msg = await get_message(parent_response_id, logger)
                await asyncio.to_thread(
                    average_result_scores,
                    final_msg.get("message", {}).get("results") or [],
                )
                await save_message(parent_response_id, final_msg, logger)
            finally:
                await remove_lock(parent_response_id, CONSUMER, logger)
        await publish_ars_event(
            {"parent_qid": parent_qid, "status": "merging"}, logger
        )
        await add_task(
            "node_norm",
            {
                "query_id": parent_qid,
                "response_id": parent_response_id,
                "workflow": json.dumps(TAIL_WORKFLOW),
                "log_level": log_level,
                "otel": otel,
                "metadata": json.dumps({}),
            },
            logger,
        )


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process an accumulate task and ACK it. Does not auto-chain."""
    start = time.time()
    with tracer.start_as_current_span(STREAM, context=parent_ctx) as span:
        try:
            await ars_accumulate(task, logger)
        except asyncio.CancelledError:
            logger.warning(f"Task {task[0]} was cancelled")
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(
                f"Task {task[0]} failed with unhandled error: {e}", exc_info=True
            )
        finally:
            try:
                await mark_task_as_complete(STREAM, GROUP, task[0], logger)
            except Exception as e:
                logger.error(f"Task {task[0]}: Failed to wrap up task: {e}")
            limiter.release()
            logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, TASK_LIMIT
            ):
                asyncio.create_task(process_task(task, parent_ctx, logger, limiter))
        except asyncio.CancelledError:
            logging.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            logging.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())

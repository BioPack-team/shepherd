"""ARS pre-merge worker.

Runs the per-callback pipeline that upstream executes inline in its Django
result-callback view: pre_merge_process (scrub null attributes, decorate
edge sources with the agent's infores, normalize scores), phantom
support-graph removal, and TRAPI validation. Moved off the server because
this is the CPU-heavy stretch of the callback path and it saturated the
server under concurrent load (documented deviation in the parity register;
the outcome contract below is upstream's, applied asynchronously).

Outcomes, exactly as upstream's view produced them:
  - success           -> premerged payload saved over the raw one, ars.merge
                         enqueued for ara- agents, child flips to D/200 (or
                         the tr_ars.message.status header value carried on
                         the task), parent completion check
  - invalid TRAPI     -> child E/422 + ara_failed_validation notification
                         (upstream also answered HTTP 422 inline; that
                         response is now a 201, see the register)
  - pipeline crash    -> child E/500 with the "Internal ARS Server Error"
                         log entry, upstream's generic handler behavior
"""

import asyncio
import json
import logging
import uuid

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
from shepherd_utils.ars.notify import notify_subscribers
from shepherd_utils.ars.premerge import (
    pre_merge_process,
    remove_phantom_support_graphs,
)
from shepherd_utils.ars.trapi import validate
from shepherd_utils.broker import add_task, mark_task_as_complete
from shepherd_utils.db import save_logs
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks

STREAM = "ars.premerge"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)


async def ars_premerge(task, logger: logging.Logger):
    child_pk = task[1]["child_pk"]
    parent_pk = task[1]["parent_pk"]
    agent_name = task[1]["agent_name"]
    inforesid = task[1].get("inforesid") or None
    status = task[1].get("status", "D")

    mesg = await ars_db.get_message_row(child_pk)
    if mesg is None:
        logger.error(f"Premerge: child {child_pk} not found")
        return
    data = await ars_db.load_message_data(child_pk, logger)
    if data is None:
        data = {}
    parent = await ars_db.get_message_row(parent_pk)
    params = mesg.get("params") or {}

    try:
        message_to_merge = data
        await asyncio.to_thread(
            pre_merge_process, message_to_merge, str(child_pk), agent_name, inforesid
        )
        if "validate" in params.keys() and not params["validate"]:
            valid = True
        else:
            await asyncio.to_thread(remove_phantom_support_graphs, message_to_merge)
            valid = await asyncio.to_thread(validate, message_to_merge)
    except Exception as e:
        # upstream's generic callback handler: E/500 + the log entry
        logger.error(f"premerge failed for {child_pk}: {e}", exc_info=True)
        log_entry = {
            "message": "Internal ARS Server Error",
            "timestamp": str(mesg.get("updated_at")),
            "level": "ERROR",
        }
        if "logs" in data.keys():
            data["logs"].append(log_entry)
        else:
            data["logs"] = [log_entry]
        await ars_db.save_message_data(child_pk, data, logger)
        await ars_db.update_message(child_pk, status="E", code=500)
        await ars_db.persist_data_copy(child_pk, logger)
        await lifecycle.check_parent_completion(parent_pk, logger)
        return

    if valid:
        await ars_db.save_message_data(child_pk, message_to_merge, logger)
        if agent_name.startswith("ara-"):
            # merge before the child's status flip, matching upstream's
            # apply_async-then-save ordering
            await add_task(
                "ars.merge",
                {
                    "parent_pk": str(parent_pk),
                    "child_pk": str(child_pk),
                    "agent_name": agent_name,
                    "query_id": str(parent_pk),
                    "otel": task[1].get("otel", "{}"),
                },
                logger,
            )
        updated = await ars_db.update_message(child_pk, status=status, code=200)
        await ars_db.persist_data_copy(child_pk, logger)
        if updated and updated["status"] in ("D", "S", "E", "U"):
            await lifecycle.check_parent_completion(parent_pk, logger)
    else:
        logger.debug(
            f"Validation problem found for agent {agent_name} with pk {parent_pk}"
        )
        await ars_db.save_message_data(child_pk, data, logger)
        await ars_db.update_message(child_pk, status="E", code=422)
        await ars_db.persist_data_copy(child_pk, logger)
        if parent is not None:
            res = (data.get("message") or {}).get("results")
            await notify_subscribers(
                parent,
                {
                    "event_type": "ara_failed_validation",
                    "ara_name": inforesid,
                    "child_uuid": str(mesg["id"]),
                    "ara_response_status": "E",
                    "ara_n_results": len(res) if res is not None else None,
                },
                logger,
            )
        await lifecycle.check_parent_completion(parent_pk, logger)


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    with tracer.start_as_current_span(STREAM, context=parent_ctx):
        try:
            await ars_premerge(task, logger)
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

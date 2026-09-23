"""ARS pre-merge worker.

Receives an ARA's response off the broker and runs the per-response pipeline
that upstream executes inline in its Django result-callback view: the intake
state machine (guards, counts, the ara_response_complete notification), then
pre_merge_process (decorate edge sources with the agent's infores,
normalize scores), phantom support-graph removal, and TRAPI validation.
Moved off the server because this is the CPU-heavy stretch of the callback
path and it saturated the server under concurrent load
(documented deviation in the parity register; the outcome contract below is
upstream's, applied asynchronously).

Outcomes, exactly as upstream's view produced them:
  - success           -> premerged payload saved over the raw one, the child
                         recorded as merge-ready and ars.merge woken (ara-
                         agents), child flips to D/200 (or the status carried
                         on the task), parent completion check
  - invalid TRAPI     -> child E/422 + ara_failed_validation notification
                         (upstream also answered HTTP 422 inline; that
                         response is now a 201, see the register)
  - pipeline crash    -> child E/500 with the "Internal ARS Server Error"
                         log entry, upstream's generic handler behavior

The premerge stages are pure-Python CPU over a multi-MB dict, so they run in
a process pool: the pool child fetches the child's blob by pk with the sync
Redis client, processes it, and writes it back, and only the verdict crosses
IPC (the same pattern as ars_merge and merge_message). Nothing large stays
resident in this process past the intake.

finish_query enqueues {intake_child_pk, response_id} when an ARA pipeline
finishes an ARS-originated query (the de-federated ARS receives every
response over the broker; there is no callback endpoint), and
intake_internal_response runs the upstream callback view's intake state
machine here first -- same guards, counts, notifications, and terminal
shapes -- before filling in the {child_pk, parent_pk, agent_name, ...}
fields the premerge stage reads.
"""

import asyncio
import logging
import uuid
from typing import Dict

from opentelemetry.propagate import extract, inject

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
from shepherd_utils.ars import aras
from shepherd_utils.ars.notify import notify_subscribers
from shepherd_utils.ars.premerge import (
    ScoreStatCalc,
    get_safe,
    pre_merge_process,
    remove_phantom_support_graphs,
)
from shepherd_utils.ars.statuses import coerce_status
from shepherd_utils.ars.trapi import validate
from shepherd_utils.broker import add_task, mark_task_as_complete
from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.db import (
    get_logs,
    get_message,
    get_message_sync,
    save_logs,
    save_message_sync,
)
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_pool_child_tracer, setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import get_tasks

STREAM = "ars.premerge"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

_pool = None
_loop = None


def premerge_in_child(
    child_pk, agent_name, inforesid, do_validate, otel_carrier=None
) -> bool:
    """Pool-side premerge: fetch the child's blob by pk, process it in place,
    write it back, and return the validation verdict.

    The processed payload is saved whether or not it validates, as upstream
    saved the (already premerged) data on both branches of its view.
    ``otel_carrier`` is the parent's span context; the stage span starts
    under it (see shepherd_utils.otel.setup_pool_child_tracer).
    """
    setup_pool_child_tracer(STREAM)
    parent_ctx = extract(otel_carrier) if otel_carrier else None
    with tracer.start_as_current_span(
        "ars.premerge.process", context=parent_ctx
    ) as span:
        span.set_attribute("agent", agent_name)
        span.set_attribute("premerge.child_pk", str(child_pk))
        span.set_attribute("premerge.validate", bool(do_validate))
        data = get_message_sync(str(child_pk))
        pre_merge_process(data, str(child_pk), agent_name, inforesid)
        if do_validate:
            remove_phantom_support_graphs(data)
            valid = validate(data)
        else:
            valid = True
        save_message_sync(str(child_pk), data)
        span.set_attribute("premerge.valid", bool(valid))
    return bool(valid)


async def _run_premerge_in_pool(
    child_pk, agent_name, inforesid, do_validate, otel_carrier, logger
):
    """Indirection for tests; production runs premerge_in_child in the pool."""
    args = (child_pk, agent_name, inforesid, do_validate, otel_carrier)
    if _pool is not None and _loop is not None:
        return await _pool.run(_loop, premerge_in_child, *args)
    return await asyncio.to_thread(premerge_in_child, *args)


async def _terminal_error(child_pk, parent_pk, mesg, data, logger):
    """Upstream's generic callback handler: E/500 + the log entry.

    ``data`` may be None when the failure happened in the pool child (the
    payload was never resident here); the stored blob is loaded to carry
    the log entry, as upstream appended it to the payload it was holding.
    """
    if data is None:
        data = await ars_db.load_message_data(child_pk, logger)
    if not isinstance(data, dict):
        data = {}
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


async def intake_internal_response(fields, logger: logging.Logger):
    """Receive an ARA's response off the queue.

    finish_query enqueues {intake_child_pk, response_id}; this runs the
    upstream callback view's state machine over the stored response (same
    guards, counts, notification, and terminal shapes, minus the HTTP
    answers nobody read).

    On the result-bearing path it saves the response under the child pk,
    fills the standard premerge task fields into ``fields`` and returns
    True so ``ars_premerge`` continues in this same task; every other
    outcome is handled here and returns None.
    """
    child_pk = fields["intake_child_pk"]
    response_id = fields["response_id"]
    mesg = await ars_db.get_message_row(child_pk)
    if mesg is None:
        logger.error(f"Intake: child {child_pk} not found")
        return None
    if mesg.get("ref"):
        # so the finally-block log flush lands under the query's parent
        fields["parent_pk"] = str(mesg["ref"])
    try:
        data = await get_message(response_id, logger)
    except Exception as e:
        # no stored response = the callback never arrived; the child stays
        # Running for the watchdog, like an undelivered HTTP callback
        logger.error(f"Intake: no response blob {response_id} for {child_pk}: {e}")
        return None
    try:
        # the endpoint receives the payload with the query's logs already
        # spliced in by finish_query; splice them here instead
        try:
            logs = await get_logs(response_id, logger)
            data.pop("logs", None)
            data["logs"] = logs
        except Exception as e:
            logger.warning(f"Intake: proceeding without logs for {response_id}: {e}")

        status = "D"  # broker deliveries carry no tr_ars.message.status
        res = get_safe(data, "message", "results")
        agent_name = str(mesg.get("agent"))
        inforesid = aras.inforesid_for(agent_name)
        parent = await ars_db.get_message_row(mesg["ref"]) if mesg.get("ref") else None
        if parent is None:
            logger.error(f"Intake: unknown parent for child {child_pk}")
            return None
        result_length = len(res) if res is not None else None
        await notify_subscribers(
            parent,
            {
                "event_type": "ara_response_complete",
                "ara_name": inforesid,
                "child_uuid": str(mesg["id"]),
                "ara_response_status": status,
                "ara_n_results": result_length,
            },
            logger,
        )
        logger.info(
            f"received msg from agent: {inforesid} with parent pk: "
            f"{mesg['ref']} and result: {result_length}"
        )
        # the callback endpoint's guard order: dup-Done, repeated results,
        # errored child -- all skips here (there is no HTTP answer to give)
        if mesg["status"] == "D":
            logger.info(f"Intake: {child_pk} already has results; skipping")
            return None
        if mesg.get("result_count") is not None and mesg["result_count"] > 0:
            logger.info(f"Intake: {child_pk} already has a response; skipping")
            return None
        if mesg["status"] == "E":
            logger.info(f"Intake: {child_pk} already errored; response rejected")
            return None
        if res is not None and result_length > 0:
            result_stat = await asyncio.to_thread(ScoreStatCalc, res)
            await ars_db.save_message_data(
                child_pk, data, logger, raise_on_failure=True
            )
            await ars_db.update_message(
                child_pk, result_count=result_length, result_stat=result_stat
            )
            fields["child_pk"] = str(child_pk)
            fields["agent_name"] = agent_name
            fields["inforesid"] = str(inforesid or "")
            fields["status"] = status
            return True
        # no results: terminal inline, nothing to premerge or validate
        await ars_db.save_message_data(child_pk, data, logger)
        updates = {"status": status, "code": 200}
        if res is None:
            updates["result_count"] = 0
        updated = await ars_db.update_message(child_pk, **updates)
        await ars_db.persist_data_copy(child_pk, logger)
        if updated and updated["status"] in ("D", "S", "E", "U"):
            await lifecycle.check_parent_completion(mesg["ref"], logger)
        return None
    except Exception as e:
        logger.error(f"Intake failed for {child_pk}: {e}", exc_info=True)
        await _terminal_error(child_pk, mesg.get("ref"), mesg, data, logger)
        return None


async def _hand_to_merge(parent_pk, child_pk, agent_name, otel, logger):
    """Record the child as merge-ready and wake the merge worker.

    The index entry is what gets the child merged; the wake task is only a
    hint (the worker holding the parent's lock drains the index in arrival
    order). Both are attempted strictly: a child that validated but can
    never be merged would leave its parent waiting on a merge child that
    never comes, so the caller fails the child instead.
    """
    await ars_db.add_ready_child(parent_pk, child_pk, logger)
    await add_task(
        "ars.merge",
        {
            "parent_pk": str(parent_pk),
            "child_pk": str(child_pk),
            "agent_name": agent_name,
            "query_id": str(parent_pk),
            "otel": otel,
        },
        logger,
        raise_on_failure=True,
    )


async def ars_premerge(task, logger: logging.Logger):
    if "intake_child_pk" in task[1]:
        if await intake_internal_response(task[1], logger) is None:
            return
    child_pk = task[1]["child_pk"]
    parent_pk = task[1]["parent_pk"]
    agent_name = task[1]["agent_name"]
    inforesid = task[1].get("inforesid") or None
    # carried from the callback's tr_ars.message.status header; re-clamped
    # here because the task payload is just as untrusted as the header was
    status = coerce_status(task[1].get("status", "D"), "D")

    mesg = await ars_db.get_message_row(child_pk)
    if mesg is None:
        logger.error(f"Premerge: child {child_pk} not found")
        return
    parent = await ars_db.get_message_row(parent_pk)
    params = mesg.get("params") or {}
    do_validate = not ("validate" in params.keys() and not params["validate"])

    # the pool child continues this trace under the current task span
    carrier: Dict[str, str] = {}
    inject(carrier)
    try:
        valid = await _run_premerge_in_pool(
            str(child_pk), agent_name, inforesid, do_validate, carrier, logger
        )
    except Exception as e:
        logger.error(f"premerge failed for {child_pk}: {e}", exc_info=True)
        await _terminal_error(child_pk, parent_pk, mesg, None, logger)
        return

    if valid:
        if agent_name.startswith("ara-"):
            # merge before the child's status flip, matching upstream's
            # apply_async-then-save ordering
            try:
                await _hand_to_merge(
                    parent_pk, child_pk, agent_name, task[1].get("otel", "{}"), logger
                )
            except Exception as e:
                logger.error(
                    f"Could not hand {child_pk} to the merge worker: {e}",
                    exc_info=True,
                )
                await _terminal_error(child_pk, parent_pk, mesg, None, logger)
                return
        updated = await ars_db.update_message(child_pk, status=status, code=200)
        await ars_db.persist_data_copy(child_pk, logger)
        if updated and updated["status"] in ("D", "S", "E", "U"):
            await lifecycle.check_parent_completion(parent_pk, logger)
    else:
        logger.debug(
            f"Validation problem found for agent {agent_name} with pk {parent_pk}"
        )
        await ars_db.update_message(child_pk, status="E", code=422)
        await ars_db.persist_data_copy(child_pk, logger)
        if parent is not None:
            await notify_subscribers(
                parent,
                {
                    "event_type": "ara_failed_validation",
                    "ara_name": inforesid,
                    "child_uuid": str(mesg["id"]),
                    "ara_response_status": "E",
                    # the count the intake recorded (upstream: len(results)
                    # of the payload it was holding; premerge only runs
                    # when there are results, so they are the same number)
                    "ara_n_results": mesg.get("result_count"),
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
    global _pool, _loop
    _loop = asyncio.get_running_loop()
    # Sized by the pod's CPU allocation: each in-flight task runs one pool
    # child holding one ARA response, so pool size == concurrency bounds
    # both CPU and peak memory. POOL_MAX_WORKERS overrides.
    max_workers = resolve_pool_workers(TASK_LIMIT, LOGGER)
    LOGGER.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    _pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="ars_premerge process pool",
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

"""ARS fan-out orchestrator.

Receives a submitted parent query and dispatches a child copy to every ARA in
``settings.ars_aras`` using their existing internal Shepherd workflows. Each
child's ``finish_query`` posts its final response back to
``/ars/callback/{ars_callback_id}``; completion is tracked per-ARA in the
``ars_children`` table and the cross-ARA merge runs in the ``ars_accumulate``
worker. This worker therefore does NOT chain via ``wrap_up_task`` -- the
post-merge tail is launched event-driven once every ARA has reported back.
"""

import asyncio
import json
import logging
import uuid

from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode

from shepherd_utils.ars_notify import publish_ars_event
from shepherd_utils.ars_workflow import ARS_TAIL_WORKFLOW
from shepherd_utils.broker import add_task, mark_task_as_complete
from shepherd_utils.config import settings
from shepherd_utils.db import (
    add_ars_children,
    add_query,
    claim_ars_tail,
    get_message,
    get_timed_out_ars_parents,
    mark_ars_children_errored,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks

# Queue name
STREAM = "ars"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def ars(task, logger: logging.Logger):
    """Fan the parent query out to each configured ARA."""
    parent_qid = task[1]["query_id"]
    log_level = task[1].get("log_level", 20)
    message = await get_message(parent_qid, logger)

    children = []
    for ara in settings.ars_aras:
        child_qid = str(uuid.uuid4())[:8]
        child_response_id = str(uuid.uuid4())[:8]
        ars_callback_id = str(uuid.uuid4())
        callback_url = f"{settings.callback_host}/ars/callback/{ars_callback_id}"

        # Persist the child query + response under their own ids. The child runs
        # the ARA's normal internal pipeline and posts its final response to our
        # ARS callback URL.
        await add_query(
            child_qid,
            child_response_id,
            message,
            callback_url,
            logger,
            target=ara,
        )

        # Each child carries its own otel carrier so the per-ARA trace nests
        # under the parent span captured at delivery.
        span_carrier = {}
        inject(span_carrier)
        children.append(
            {
                "ara": ara,
                "child_qid": child_qid,
                "child_response_id": child_response_id,
                "ars_callback_id": ars_callback_id,
                "otel_trace": json.dumps(span_carrier),
            }
        )

    # Record all child rows (QUEUED) before dispatching so a fast callback always
    # finds its row.
    await add_ars_children(parent_qid, children, logger)

    for child in children:
        await add_task(
            child["ara"],
            {
                "query_id": child["child_qid"],
                "response_id": child["child_response_id"],
                "workflow": json.dumps(None),
                "log_level": log_level,
                "otel": child["otel_trace"],
                "metadata": json.dumps({}),
            },
            logger,
        )
    logger.info(
        f"Fanned parent {parent_qid} out to {len(children)} ARAs: "
        f"{[c['ara'] for c in children]}"
    )


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a fan-out task and ACK it. Does not chain to a next operation."""
    with tracer.start_as_current_span(STREAM, context=parent_ctx) as span:
        try:
            await ars(task, logger)
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


async def run_watchdog_once(logger: logging.Logger):
    """Force timed-out parents to finish so the submitter still gets a result.

    For each parent past its budget with ARAs still pending, mark those ARAs
    ERROR and (winning the same single-launch latch as ars_accumulate) enqueue
    the post-merge tail on whatever partial results have accumulated.
    """
    timed_out = await get_timed_out_ars_parents(
        settings.ars_overall_timeout_sec, logger
    )
    for parent in timed_out:
        parent_qid = parent["qid"]
        pending = parent["pending"]
        await mark_ars_children_errored(parent_qid, pending, logger)
        logger.warning(
            f"ARS parent {parent_qid} timed out; errored pending ARAs {pending}."
        )
        await publish_ars_event(
            {"parent_qid": parent_qid, "status": "timeout", "errored": pending},
            logger,
        )
        if await claim_ars_tail(parent_qid, logger):
            await add_task(
                "node_norm",
                {
                    "query_id": parent_qid,
                    "response_id": parent["response_id"],
                    "workflow": json.dumps(ARS_TAIL_WORKFLOW),
                    "log_level": 20,
                    "otel": json.dumps({}),
                    "metadata": json.dumps({}),
                },
                logger,
            )


async def watchdog_loop():
    """Periodically sweep for timed-out ARS parents."""
    logger = logging.getLogger("shepherd.ars.watchdog")
    interval = max(5.0, float(settings.ars_watchdog_interval_sec))
    while True:
        # Sleep first so the poll loop has initialized the db pool.
        await asyncio.sleep(interval)
        try:
            await run_watchdog_once(logger)
        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.error(f"ARS watchdog sweep failed: {e}", exc_info=True)


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


async def main():
    """Run the fan-out poll loop and the timeout watchdog together."""
    await asyncio.gather(poll_for_tasks(), watchdog_loop())


if __name__ == "__main__":
    asyncio.run(main())

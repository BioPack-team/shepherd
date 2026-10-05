"""ARS fan-out worker.

Broadcasts a submitted query to every ARA the ARS is configured to use,
creating one child message per ARA and dispatching the query. Port of
NCATSTranslator/Relay @ 3e65975 signals.py message_post_save +
pubsub.py send_messages + tasks.py send_message, reduced to what a
de-federated ARS needs: the roster is the static set of Shepherd-hosted ARAs
(shepherd_utils/ars/aras.py) rather than actors matched on channels, and
there is no HTTP. Dispatch does what POSTing to /{ara}/asyncquery would have
done -- persist the query record and enqueue the ARA's worker task -- with
the callback set to the broker-handoff sentinel, so finish_query hands the
response straight to ars.premerge. The child stays R/202 exactly like an
async accept; a dispatch failure is the same E/500 shape upstream recorded
for a failed POST.
"""

import asyncio
import copy
import json
import logging
import uuid

from opentelemetry.propagate import inject

import shepherd_utils.ars.db as ars_db
import shepherd_utils.ars.lifecycle as lifecycle
import shepherd_utils.db as shepherd_db
from shepherd_utils.ars import aras
from shepherd_utils.ars.handoff import handoff_callback_url
from shepherd_utils.broker import add_task, mark_task_as_complete
from shepherd_utils.config import settings
from shepherd_utils.db import save_logs
from shepherd_utils.logger import get_worker_logger, resolve_log_level
from shepherd_utils.task_deadline import deadline_field, query_deadline
from shepherd_utils.trapi import query_log_level
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks

STREAM = "ars.fanout"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)


async def _finalize_child(child_pk, parent_pk, payload, updates, logger):
    """Persist a child transition; run the completion check when terminal."""
    if payload is not None:
        await ars_db.save_message_data(child_pk, payload, logger)
    updated = await ars_db.update_message(child_pk, **updates)
    if updated and updated["status"] in ("D", "S", "E", "U"):
        await ars_db.persist_data_copy(child_pk, logger)
        await lifecycle.check_parent_completion(parent_pk, logger)


async def dispatch(ara: aras.ARA, child_pk, parent, data, logger):
    """Enqueue the query on the ARA's worker stream.

    Persists the same query record ``POST /{ara}/asyncquery`` would have,
    with the handoff sentinel as its callback so the finished response comes
    back over the broker. The child stays R/202 like an async accept; a
    failure here is the child's E/500.
    """
    callback = handoff_callback_url(child_pk)
    data["callback"] = callback
    query_id = str(uuid.uuid4())[:8]
    response_id = str(uuid.uuid4())[:8]
    # TRAPI 2.0: the client's log level is parameters.log_level
    level_number = resolve_log_level(
        query_log_level(data), resolve_log_level(settings.log_level)
    )
    carrier = {}
    inject(carrier)
    try:
        await shepherd_db.add_query(
            query_id, response_id, data, callback, logger, target=ara.name
        )
        deadline = query_deadline(data)
        await add_task(
            ara.name,
            {
                "query_id": query_id,
                "response_id": response_id,
                "workflow": json.dumps(data.get("workflow")),
                "log_level": level_number,
                "otel": json.dumps(carrier),
                "metadata": json.dumps({}),
                **deadline_field(deadline),
            },
            logger,
            raise_on_failure=True,
        )
        logger.info(f"[{child_pk}] dispatched to {ara.name} as {query_id}")
    except Exception as e:
        logger.error(
            f"Dispatch to {ara.name} failed for pk: {child_pk}: {e}",
            exc_info=True,
        )
        await _finalize_child(
            child_pk, parent["id"], data, {"status": "E", "code": 500}, logger
        )


async def send_to_ara(ara: aras.ARA, parent, parent_data, logger):
    """tasks.send_message, one ARA: create the child, dispatch the query."""
    child = await ars_db.create_message(
        agent=ara.agent,
        status="Running",
        code=202,
        name=parent.get("name", ""),
        ref=parent["id"],
        params=parent.get("params"),
    )
    data = copy.deepcopy(parent_data) if parent_data else {}
    await dispatch(ara, child["id"], parent, data, logger)


async def ars_fanout(task, logger: logging.Logger):
    parent_pk = task[1]["parent_pk"]
    parent = await ars_db.get_message_row(parent_pk)
    if parent is None:
        logger.error(f"Fanout: parent {parent_pk} not found")
        return
    targets = aras.enabled_aras()
    logger.info(
        f"Fanning out {parent_pk} to {len(targets)} ARA(s): "
        f"{[a.name for a in targets]}"
    )
    if not targets:
        # Nothing will ever answer, and parents are watchdog-exempt: run the
        # completion check now so the query finishes as an empty result
        # instead of sitting Running forever.
        logger.warning(f"No ARAs enabled; completing {parent_pk} empty")
        await lifecycle.check_parent_completion(parent["id"], logger)
        return
    parent_data = await ars_db.load_message_data(parent_pk, logger)
    await asyncio.gather(
        *(send_to_ara(ara, parent, parent_data, logger) for ara in targets)
    )


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Hand-rolled lifecycle: this stream is not a TRAPI workflow hop."""
    with tracer.start_as_current_span(STREAM, context=parent_ctx):
        try:
            await ars_fanout(task, logger)
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

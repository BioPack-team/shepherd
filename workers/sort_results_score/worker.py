"""Example ARA module."""

import asyncio
import json
import logging
import uuid
from shepherd_utils.db import (
    enforce_response_size_limit,
    get_message,
    save_message,
    get_query_state,
)
from shepherd_utils.shared import get_tasks, run_task_lifecycle
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "sort_results_score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def sort_results_score(task, logger: logging.Logger):
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])
    # Refuse to load a response so large that decoding + sorting it would OOM
    # the worker. Raising here (before get_message) lets run_task_lifecycle fail
    # the task cleanly to finish_query instead of the process being SIGKILL'd
    # mid-load and the message crash-looping on every reclaim.
    await enforce_response_size_limit(response_id, logger)
    message = await get_message(response_id, logger)
    results = message["message"].get("results", [])
    current_op = workflow[0]
    aord = current_op.get("ascending_or_descending", "descending")
    reverse = aord == "descending"
    # Sort in place (list.sort, not sorted()) throughout. The decoded response is
    # already by far the largest object in memory, so allocating a second copy of
    # the results list -- and of every result's analyses list -- is wasted peak
    # memory on exactly the payloads big enough to be a problem. ``results`` is
    # the same list object as message["message"]["results"], so sorting it in
    # place reorders the message too.
    for result in results:
        result["analyses"].sort(
            key=lambda x: x.get("score", 0),
            reverse=reverse,
        )
    if reverse:
        results.sort(
            key=lambda x: x["analyses"][0].get("score", 0) if x["analyses"] else 0,
            reverse=reverse,
        )
    else:
        results.sort(
            key=lambda x: x["analyses"][-1].get("score", 0) if x["analyses"] else 0,
            reverse=reverse,
        )
    # Reattach so the key exists even when the response arrived without a
    # ``results`` field (get returned the default []); this is a rebind of the
    # same list object, not a copy.
    message["message"]["results"] = results
    logger.info("Returning sorted results.")

    # save merged message back to db
    await save_message(response_id, message, logger)


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, sort_results_score
    )


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

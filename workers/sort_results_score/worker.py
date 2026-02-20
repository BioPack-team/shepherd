"""Example ARA module."""

import asyncio
import json
import logging
import time
import uuid
from shepherd_utils.db import get_message, save_message, get_query_state
from shepherd_utils.shared import get_tasks, wrap_up_task
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "sort_results_score"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def sort_results_score(task, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    response_id = task[1]["response_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(response_id, logger)
    results = message["message"].get("results", [])
    current_op = workflow[0]
    aord = current_op.get("ascending_or_descending", "descending")
    reverse = aord == "descending"
    try:
        for ind, result in enumerate(results):
            message["message"]["results"][ind]["analyses"] = sorted(
                result["analyses"],
                key=lambda x: x.get("score", 0),
                reverse=reverse,
            )
        if reverse:
            message["message"]["results"] = sorted(
                results,
                key=lambda x: x["analyses"][0].get("score", 0),
                reverse=reverse,
            )
        else:
            message["message"]["results"] = sorted(
                results,
                key=lambda x: x["analyses"][-1].get("score", 0),
                reverse=reverse,
            )
    except KeyError as e:
        # can't find the right structure of message
        err = f"Error sorting results: {e}"
        logger.error(err)
        raise KeyError(err)
    logger.info("Returning sorted results.")

    # save merged message back to db
    await save_message(response_id, message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await sort_results_score(task, logger)
    finally:
        span.end()
        limiter.release()


async def poll_for_tasks():
    async for task, parent_ctx, logger, limiter in get_tasks(
        STREAM, GROUP, CONSUMER, TASK_LIMIT
    ):
        asyncio.create_task(process_task(task, parent_ctx, logger, limiter))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())

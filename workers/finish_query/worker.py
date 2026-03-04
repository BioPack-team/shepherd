"""Mark a query as completed and do any callbacks."""

import asyncio
import httpx
import logging
import time
import uuid


from shepherd_utils.broker import mark_task_as_complete
from shepherd_utils.db import (
    get_logs,
    get_message,
    get_query_state,
    set_query_completed,
)
from shepherd_utils.shared import get_tasks
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "finish_query"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)
CALLBACK_RETRIES = 3


async def finish_query(task, logger: logging.Logger):
    """Do all the wrap up necessary for a query."""
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    response_id = task[1]["response_id"]
    status = task[1].get("status", "OK")
    query_state = await get_query_state(query_id, logger)

    if query_state is None:
        logger.error(f"Query id {query_id} not found in db.")
    else:
        callback_url = query_state[8]
        if callback_url is not None:
            # this was an async query, need to send message back
            message = await get_message(response_id, logger)
            logs = await get_logs(response_id, logger)
            message["logs"] = logs
            for attempt in range(CALLBACK_RETRIES):
                try:
                    async with httpx.AsyncClient(timeout=120) as client:
                        response = await client.post(
                            callback_url,
                            json=message,
                        )
                        response.raise_for_status()
                        logger.info(f"Sent response back to {callback_url}")
                        break
                except Exception as e:
                    logger.error(f"Failed to send callback to {callback_url}: {e}")
                    await asyncio.sleep(1 * (2**attempt))

        await set_query_completed(query_id, status, logger)

    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    start = time.time()
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await finish_query(task, logger)
    except asyncio.CancelledError:
        logger.warning(f"Task {task[0]} was cancelled")
    except Exception as e:
        logger.error(f"Task {task[0]} failed with unhandled error: {e}", exc_info=True)
    finally:
        # Always wrap up the task to ACK it in the broker
        try:
            await mark_task_as_complete(STREAM, GROUP, task[0], logger)
        except Exception as e:
            logger.error(f"Task {task[0]}: Failed to wrap up task: {e}")
        span.end()
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

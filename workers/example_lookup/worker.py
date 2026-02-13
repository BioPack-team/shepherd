"""Example ARA module."""

import asyncio
import json
import logging
import time
import uuid
from pathlib import Path

import httpx

from shepherd_utils.db import (
    add_callback_id,
    get_message,
    get_running_callbacks,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "example.lookup"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def example_lookup(task, logger: logging.Logger):
    """Example lookup function.

    Just sends a test response back to the server callback endpoint.
    """
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    try:
        message = await get_message(query_id, logger)
    except Exception as e:
        logger.error(f"Task {task[0]}: Failed to get message for query {query_id}: {e}")
        raise

    # Do query expansion or whatever lookup process
    # We're going to stub a response
    start = time.time()
    test_response = Path(__file__).parent / "test_response.json"
    with open(test_response, "r", encoding="utf-8") as f:
        response = json.load(f)
    logger.debug(f"Loading json took {time.time() - start}")

    requests = []

    try:
        async with httpx.AsyncClient(timeout=100) as client:
            for _ in range(5):
                callback_id = str(uuid.uuid4())[:8]
                try:
                    # Put callback UID and query ID in postgres
                    await add_callback_id(query_id, callback_id, logger)
                    # put lookup query graph in redis
                    await save_message(
                        f"{callback_id}_query_graph",
                        response["message"]["query_graph"],
                        logger
                    )
                except Exception as e:
                    logger.error(
                        f"Task {task[0]}: Failed to register callback {callback_id}: {e}"
                    )
                    # Skip this callback but continue with others
                    continue

                # this is just fake, this could take minutes

                request = client.post(
                    f"http://shepherd_server:5439/callback/{callback_id}",
                    json=response,
                )
                requests.append(request)
                # Then we can retrieve all callback ids from query id to see which are still
                # being looked up
            results = await asyncio.gather(*requests, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Task {task[0]}: Callback request {i} failed: {result}")
    except httpx.HTTPError as e:
        logger.error(f"Task {task[0]}: HTTP client error during callbacks: {e}")
    except Exception as e:
        logger.error(f"Task {task[0]}: Unexpected error during callback dispatch: {e}")

    # this worker might have a timeout set for if the lookups don't finish within a
    # certain amount of time
    MAX_QUERY_TIME = 300
    start_time = time.time()
    try:
        while time.time() - start_time < MAX_QUERY_TIME:
            try:
                # see if there are existing lookups going
                running_callback_ids = await get_running_callbacks(query_id, logger)
            except Exception as e:
                logger.error(
                    f"Task {task[0]}: Failed to check running callbacks: {e}"
                )
                # Brief backoff then retry the check rather than giving up
                await asyncio.sleep(5)
                continue
            # logger.info(f"Got back {len(running_callback_ids)} running lookups")
            # if there aren't, lookup is complete and we need to pass on to next
            # workflow operation
            if len(running_callback_ids) == 0:
                break

            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.warning(
            f"Task {task[0]}: Cancelled while waiting for callbacks."
        )

    # Always wrap up the task to ACK it in the broker
    try:
        await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    except Exception as e:
        logger.error(f"Task {task[0]}: Failed to wrap up task: {e}")
        raise
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await example_lookup(task, logger)
    except asyncio.CancelledError:
        logger.warning(f"Task {task[0]} was cancelled")
    except Exception as e:
        logger.error(f"Task {task[0]} failed with unhandled error: {e}", exc_info=True)
        # Attempt to ACK the task so it doesn't get redelivered forever
        try:
            workflow = json.loads(task[1])["workflow"]
            await wrap_up_task(STREAM, GROUP, task, workflow, logger)
        except Exception as wrap_err:
            logger.error(
                f"Task {task[0]}: Also failed wrap up after error: {wrap_err}"
            )
    finally:
        span.end()
        limiter.release()


async def poll_for_tasks():
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

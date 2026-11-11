"""Example ARA module."""

import asyncio
import json
import logging
import time
import uuid
from pathlib import Path

import httpx

from shepherd_utils.config import settings
from shepherd_utils.db import (
    new_id,
    abandon_lookup_if_data_lost,
    add_callback_id,
    cleanup_callbacks,
    get_message,
    get_running_callbacks,
    save_message,
    wait_for_lookups,
)
from shepherd_utils.logger import get_worker_logger
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import (
    lookup_wait_seconds,
    run_task_lifecycle,
    run_poll_loop,
)

# Queue name
STREAM = "example.lookup"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)


async def example_lookup(task, logger: logging.Logger):
    """Example lookup function.

    Just sends a test response back to the server callback endpoint.
    """
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    response_id = task[1]["response_id"]
    message = await get_message(query_id, logger)
    parameters = message.get("parameters") or {}
    parameters["timeout"] = parameters.get("timeout", settings.lookup_timeout)

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
                callback_id = new_id()
                try:
                    # Put callback UID and query ID in postgres
                    await add_callback_id(
                        query_id, callback_id, task[1].get("otel", "{}"), logger
                    )
                    # put lookup query graph in redis
                    await save_message(
                        f"{callback_id}_query_graph",
                        response["message"]["query_graph"],
                        logger,
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
                    logger.error(
                        f"Task {task[0]}: Callback request {i} failed: {result}"
                    )
    except httpx.HTTPError as e:
        logger.error(f"Task {task[0]}: HTTP client error during callbacks: {e}")
    except Exception as e:
        logger.error(f"Task {task[0]}: Unexpected error during callback dispatch: {e}")

    # this worker might have a timeout set for if the lookups don't finish within a
    # certain amount of time
    MAX_QUERY_TIME = lookup_wait_seconds(message["parameters"])
    running_callback_ids = await wait_for_lookups(
        query_id,
        response_id,
        MAX_QUERY_TIME,
        logger,
        get_running_callbacks,
        abandon_lookup_if_data_lost,
    )

    if running_callback_ids:
        logger.warning(
            f"Timed out getting lookup callbacks. {len(running_callback_ids)} queries were still running..."
        )
        # logger.warning(f"Running callbacks: {running_callback_ids}")
        await cleanup_callbacks(query_id, logger)


async def process_task(task, parent_ctx, logger: logging.Logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, example_lookup
    )


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    await run_poll_loop(STREAM, GROUP, CONSUMER, TASK_LIMIT, process_task)


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())

"""Example ARA module."""

import asyncio
import httpx
import json
import logging
from pathlib import Path
import time
import uuid
from shepherd_utils.db import (
    get_message,
    add_callback_id,
    get_running_callbacks,
    save_message,
)
from shepherd_utils.shared import get_tasks, wrap_up_task
from shepherd_utils.otel import setup_tracer

# Queue name
STREAM = "example.lookup"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
setup_tracer(STREAM)


async def example_lookup(task, otel, logger: logging.Logger):
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    message = await get_message(query_id, logger)

    # Do query expansion or whatever lookup process
    # We're going to stub a response
    start = time.time()
    test_response = Path(__file__).parent / "test_response.json"
    with open(test_response, "r") as f:
        response = json.load(f)
    logger.debug(f"Loading json took {time.time() - start}")

    requests = []

    async with httpx.AsyncClient(timeout=100) as client:
        for _ in range(5):
            callback_id = str(uuid.uuid4())[:8]
            # Put callback UID and query ID in postgres
            await add_callback_id(query_id, callback_id, logger)
            # put lookup query graph in redis
            await save_message(
                f"{callback_id}_query_graph", response["message"]["query_graph"], logger
            )

            # this is just fake, this could take minutes

            request = client.post(
                f"http://shepherd_server:5439/callback/{callback_id}",
                json=response,
            )
            requests.append(request)
            # Then we can retrieve all callback ids from query id to see which are still
            # being looked up
        await asyncio.gather(*requests)

    # this worker might have a timeout set for if the lookups don't finish within a certain
    # amount of time
    MAX_QUERY_TIME = 300
    start_time = time.time()
    while time.time() - start_time < MAX_QUERY_TIME:
        # see if there are existing lookups going
        running_callback_ids = await get_running_callbacks(query_id, logger)
        # logger.info(f"Got back {len(running_callback_ids)} running lookups")
        # if there are, continue to wait
        if len(running_callback_ids) > 0:
            await asyncio.sleep(1)
            continue
        # if there aren't, lookup is complete and we need to pass on to next workflow operation
        if len(running_callback_ids) == 0:
            break

    await wrap_up_task(STREAM, GROUP, task, workflow, otel, logger)
    logger.info(f"Finished task {task[0]} in {time.time() - start}")


async def poll_for_tasks():
    async for task, otel, logger in get_tasks(STREAM, GROUP, CONSUMER):
        asyncio.create_task(example_lookup(task, otel, logger))


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())

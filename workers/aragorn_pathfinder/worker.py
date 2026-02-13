"""Aragorn ARA Pathfinder module."""

import asyncio
import json
import logging
import time
import uuid

from shepherd_utils.config import settings
from shepherd_utils.db import (
    add_callback_id,
    cleanup_callbacks,
    get_message,
    get_running_callbacks,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import (
    add_task,
    get_tasks,
    wrap_up_task,
)

# Queue name
STREAM = "aragorn.pathfinder"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def shadowfax(task, logger: logging.Logger):
    """Processes pathfinder queries. This is done by using literature
    co-occurrence to find nodes that occur in publications with our input
    nodes, then finding paths that connect our input nodes through these
    intermediate nodes."""
    start = time.time()
    # given a task, get the message from the db
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    response_id = task[1]["response_id"]
    message = await get_message(query_id, logger)
    parameters = message.get("parameters") or {}
    parameters["timeout"] = parameters.get("timeout", settings.lookup_timeout)
    # parameters["tiers"] = parameters.get("tiers") or [0]
    message["parameters"] = parameters

    qgraph = message["message"]["query_graph"]
    pinned_node_keys = []
    pinned_node_ids = []
    for node_key, node in qgraph["nodes"].items():
        pinned_node_keys.append(node_key)
        if node.get("ids", None) is not None:
            # TODO: silently only grabbing the first id
            pinned_node_ids.append(node["ids"][0])
    if len(set(pinned_node_ids)) != 2:
        logger.error("Pathfinder queries require two pinned nodes.")
        # TODO: Update to wrap up task
        return message, 500

    intermediate_categories = []
    path_key = next(iter(qgraph["paths"].keys()))
    qpath = qgraph["paths"][path_key]
    if qpath.get("constraints", None) is not None:
        constraints = qpath["constraints"]
        # TODO: need to wrap up tasks
        if len(constraints) > 1:
            logger.error("Pathfinder queries do not support multiple constraints.")
            return message, 500
        if len(constraints) > 0:
            intermediate_categories = (
                constraints[0].get("intermediate_categories", None) or []
            )
        if len(intermediate_categories) > 1:
            logger.error(
                "Pathfinder queries do not support multiple intermediate categories"
            )
            return message, 500
    else:
        intermediate_categories = ["biolink:NamedThing"]

    # Create 3-hop query
    threehop = {
        "message": {
            "query_graph": {
                "nodes": {
                    pinned_node_keys[0]: {
                        "ids": [pinned_node_ids[0]]
                    },
                    "intermediate_0": {
                        "categories": intermediate_categories,
                    },
                    "intermediate_1": {
                        "categories": intermediate_categories,
                    },
                    pinned_node_keys[1]: {
                        "ids": [pinned_node_ids[1]]
                    },
                },
                "edges": {
                    "e0": {
                        "subject": pinned_node_keys[0],
                        "object": "intermediate_0",
                        "predicates": ["biolink:related_to"],
                    },
                    "e1": {
                        "subject": "intermediate_0",
                        "object": "intermediate_1",
                        "predicates": ["biolink:related_to"],
                    },
                    "e2": {
                        "subject": "intermediate_1",
                        "object": pinned_node_keys[1],
                        "predicates": ["biolink:related_to"],
                    }
                },
            },
        },
    }

    callback_id = str(uuid.uuid4())[:8]
    # Put callback UID and query ID in postgres
    await add_callback_id(query_id, callback_id, logger)
    logger.debug(
        """Sending pathfinder lookup query to gandalf."""
    )

    await save_message(callback_id, threehop, logger)

    await add_task(
        "gandalf",
        {
            "target": "aragorn",
            "query_id": query_id,
            "response_id": response_id,
            "callback_id": callback_id,
            "log_level": task[1].get("log_level", 20),
            "otel": task[1]["otel"],
        },
        logger,
    )

    # this worker might have a timeout set for if the lookups don't finish within a certain
    # amount of time
    MAX_QUERY_TIME = message["parameters"]["timeout"]
    start_time = time.time()
    running_callback_ids = [""]
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
            logger.debug("Got all lookups back. Continuing...")
            break

    if time.time() - start_time > MAX_QUERY_TIME:
        logger.warning(
            f"Timed out getting lookup callbacks. {len(running_callback_ids)} queries were still running..."
        )
        logger.warning(f"Running callbacks: {running_callback_ids}")
        await cleanup_callbacks(query_id, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Task took {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await shadowfax(task, logger)
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

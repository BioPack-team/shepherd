"""Arax ARA Pathfinder module."""

import requests
import asyncio
import json
import logging
import time
import uuid
from pathlib import Path
from pathfinder.Pathfinder import Pathfinder
from biolink_helper_pkg import BiolinkHelper

from shepherd_utils.config import settings
from shepherd_utils.db import (
    get_message,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import (
    get_tasks,
    wrap_up_task,
)

# Queue name
STREAM = "arax.pathfinder"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)

NUM_TOTAL_HOPS = 4
MAX_PATHFINDER_PATHS = 500

OUT_PATH = Path("general_concepts.json")


def download_file(url: str, out_path: Path, overwrite: bool = False) -> Path:
    out_path = Path(out_path)

    if out_path.exists() and not overwrite:
        return out_path

    out_path.parent.mkdir(parents=True, exist_ok=True)

    r = requests.get(url, timeout=60)
    r.raise_for_status()

    out_path.write_bytes(r.content)
    return out_path


def get_blocked_list():
    download_file(settings.arax_blocked_list_url, OUT_PATH, False)

    with open(OUT_PATH, "r") as file:
        json_block_list = json.load(file)
    synonyms = set(s.lower() for s in json_block_list["synonyms"])
    return set(json_block_list["curies"]), synonyms


async def pathfinder(task, logger: logging.Logger):
    start = time.time()
    query_id = task[1]["query_id"]
    workflow = json.loads(task[1]["workflow"])
    response_id = task[1]["response_id"]
    message = await get_message(query_id, logger)
    parameters = message.get("parameters") or {}
    parameters["timeout"] = parameters.get("timeout", settings.lookup_timeout)
    parameters["tiers"] = parameters.get("tiers") or [0]
    message["parameters"] = parameters

    qgraph = message["message"]["query_graph"]
    pinned_node_keys = []
    pinned_node_ids = []
    for node_key, node in qgraph["nodes"].items():
        pinned_node_keys.append(node_key)
        if node.get("ids", None) is not None:
            pinned_node_ids.append(node["ids"][0])
    if len(set(pinned_node_ids)) != 2:
        logger.error("Pathfinder queries require two pinned nodes.")
        return message, 500

    intermediate_categories = []
    path_key = next(iter(qgraph["paths"].keys()))
    qpath = qgraph["paths"][path_key]
    if qpath.get("constraints", None) is not None:
        constraints = qpath["constraints"]
        if len(constraints) > 1:
            logger.error("Pathfinder queries do not support multiple constraints.")
            return message, 500
        if len(constraints) > 0:
            intermediate_categories = (constraints[0].get("intermediate_categories", None) or [])
        if len(intermediate_categories) > 1:
            logger.error(
                "Pathfinder queries do not support multiple intermediate categories"
            )
            return message, 500
    else:
        intermediate_categories = ["biolink:NamedThing"]

    blocked_curies, blocked_synonyms = get_blocked_list()
    pathfinder = Pathfinder(
        "MLRepo",
        settings.plover_url,
        settings.curie_ngd_addr,
        settings.node_degree_addr,
        blocked_curies,
        blocked_synonyms,
        logger,
    )

    biolink_cache_dir = "/tmp/biolink"
    Path(biolink_cache_dir).mkdir(parents=True, exist_ok=True)
    biolink_helper = BiolinkHelper(settings.arax_biolink_version, biolink_cache_dir)
    descendants = set(biolink_helper.get_descendants(intermediate_categories[0]))

    try:
        result, aux_graphs, knowledge_graph = pathfinder.get_paths(
            pinned_node_ids[0],
            pinned_node_ids[1],
            pinned_node_keys[0],
            pinned_node_keys[1],
            NUM_TOTAL_HOPS,
            NUM_TOTAL_HOPS,
            MAX_PATHFINDER_PATHS,
            descendants,
        )
        res = []
        if result is not None:
            res.append(
                {
                    "id": result["id"],
                    "analyses": result['analyses'],
                    "node_bindings": result['node_bindings'],
                    "essence": "result"
                }
            )
        if aux_graphs is None:
            aux_graphs = {}
        if knowledge_graph is None:
            knowledge_graph = {}
        message["message"]["knowledge_graph"] = knowledge_graph
        message["message"]["auxiliary_graphs"] = aux_graphs
        message["message"]["results"] = res
        await save_message(response_id, message, logger)
    except Exception as e:
        logger.error(
            f"PathFinder failed to find paths between {pinned_node_keys[0]} and {pinned_node_keys[1]}. "
            f"Error message is: {e}"
        )
        message = {"status": "error", "error": str(e)}
        await save_message(response_id, message, logger)

    await wrap_up_task(STREAM, GROUP, task, workflow, logger)
    logger.info(f"Task took {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter):
    span = tracer.start_span(STREAM, context=parent_ctx)
    try:
        await pathfinder(task, logger)
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

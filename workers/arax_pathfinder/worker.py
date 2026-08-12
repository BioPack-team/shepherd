"""Arax ARA Pathfinder module."""

import asyncio
import json
import logging
import time
import uuid
from pathlib import Path

import httpx
from biolink_helper_pkg import BiolinkHelper
from pathfinder.Pathfinder import Pathfinder

from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.data_download import (
    arax_pathfinder_sqlite_paths,
    ensure_arax_pathfinder_dbs,
    ensure_http_files_dataset,
)
from shepherd_utils.db import (
    get_message_sync,
    save_message_sync,
)
from shepherd_utils.inject_shepherd_arax_provenance import (
    add_shepherd_arax_to_edge_sources,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import get_tasks, run_task_lifecycle

# Queue name
STREAM = "arax.pathfinder"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)

NUM_TOTAL_HOPS = 4
MAX_HOPS_TO_EXPLORE = 4
MAX_PATHFINDER_PATHS = 500
PRUNE_TOP_K = 75
NODE_DEGREE_THRESHOLD = 10000

# The ARAX blocked-concept list, fetched once at worker startup (see
# poll_for_tasks) and read by each pool child. Kept in the working directory
# (/app in the image) so it needs no extra volume mount.
BLOCKED_LIST_DIR = "."
BLOCKED_LIST_FILENAME = "general_concepts.json"
BLOCKED_LIST_PATH = Path(BLOCKED_LIST_DIR) / BLOCKED_LIST_FILENAME

BIOLINK_CACHE_DIR = "/tmp/biolink"

REHYDRATE_TIMEOUT_SEC = 30.0

# Per-child caches. Children are spawned once and reused for up to
# ``settings.pool_max_tasks_per_child`` tasks, so the blocked list and the
# Biolink model are parsed once per child instead of once per task (previously
# every task re-read the JSON and rebuilt the BiolinkHelper, and re-fetched the
# blocked list over HTTP). Only ``Pathfinder`` is still built per task, because
# it takes the task's logger.
_blocked_list_cache = None
_biolink_helper = None
_descendants_cache: dict = {}


def ensure_blocked_list(logger: logging.Logger) -> None:
    """Fetch the ARAX blocked-concept list if it isn't on disk yet.

    Uses the shared downloader so the file lands via a temp file + atomic
    rename; the previous per-task ``requests.get`` wrote the destination
    directly, so concurrent tasks could race on a half-written file.
    Idempotent, so it is safe to call at startup and again lazily in a child.
    """
    ensure_http_files_dataset(
        name="arax_blocked_list",
        target_dir=BLOCKED_LIST_DIR,
        file_sources={BLOCKED_LIST_FILENAME: settings.arax_blocked_list_url},
        logger=logger,
    )


def get_blocked_list(logger: logging.Logger):
    """``(blocked_curies, blocked_synonyms)``, parsed once per pool child."""
    global _blocked_list_cache
    if _blocked_list_cache is None:
        if not BLOCKED_LIST_PATH.exists():
            # Startup fetch failed or this child outlived a wiped working dir.
            ensure_blocked_list(logger)
        with open(BLOCKED_LIST_PATH, "r") as file:
            json_block_list = json.load(file)
        synonyms = set(s.lower() for s in json_block_list["synonyms"])
        _blocked_list_cache = (set(json_block_list["curies"]), synonyms)
    return _blocked_list_cache


def get_descendants(category: str):
    """Biolink descendants of ``category``, memoized per pool child."""
    global _biolink_helper
    if category not in _descendants_cache:
        if _biolink_helper is None:
            Path(BIOLINK_CACHE_DIR).mkdir(parents=True, exist_ok=True)
            _biolink_helper = BiolinkHelper(
                settings.arax_biolink_version, BIOLINK_CACHE_DIR
            )
        _descendants_cache[category] = set(_biolink_helper.get_descendants(category))
    return _descendants_cache[category]


def rehydrate(kg, rehydrate_url, logger):
    """POST the knowledge graph to the retriever and return the rehydrated one.

    Synchronous because it runs inside the process-pool child alongside the
    pathfinding, so the (potentially very large) knowledge graph never has to
    be pickled back to the parent or re-encoded on its event loop.
    """
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    payload = {
        "message": {"knowledge_graph": kg},
        "parameters": {"rehydrate": True, "tier": 0},
    }

    try:
        with httpx.Client(timeout=REHYDRATE_TIMEOUT_SEC) as client:
            res = client.post(rehydrate_url, headers=headers, json=payload)
        res.raise_for_status()
        return res.json()["message"]["knowledge_graph"]

    except httpx.HTTPStatusError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        if http_err.response.text:
            logger.error(f"Error details: {http_err.response.text}")
        raise
    except httpx.ConnectError as conn_err:
        logger.error(f"Connection error occurred: {conn_err}")
        raise
    except httpx.TimeoutException as timeout_err:
        logger.error(f"Timeout error occurred: {timeout_err}")
        raise
    except httpx.RequestError as req_err:
        logger.error(f"An unexpected error occurred: {req_err}")
        raise
    except json.JSONDecodeError:
        logger.error("Failed to parse the rehydrate response as JSON.")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise e


def parse_query_graph(qgraph):
    """Pull the pinned nodes and intermediate category out of the query graph.

    Raises ``ValueError`` on anything Pathfinder can't answer. Raising (rather
    than returning a status code, which the old caller discarded) lets
    ``run_task_lifecycle`` record the failure on the span and route the query to
    ``finish_query`` with an ERROR status, instead of continuing the workflow
    with no response message ever written.
    """
    pinned_node_keys = []
    pinned_node_ids = []
    for node_key, node in qgraph["nodes"].items():
        pinned_node_keys.append(node_key)
        if node.get("ids", None) is not None:
            pinned_node_ids.append(node["ids"][0])
    if len(set(pinned_node_ids)) != 2:
        raise ValueError("Pathfinder queries require two pinned nodes.")

    intermediate_categories = []
    path_key = next(iter(qgraph["paths"].keys()))
    qpath = qgraph["paths"][path_key]
    if (
        qpath.get("constraints", None) is not None
        and len(qpath.get("constraints", [])) > 0
    ):
        constraints = qpath["constraints"]
        if len(constraints) > 1:
            raise ValueError("Pathfinder queries do not support multiple constraints.")
        if len(constraints) > 0:
            intermediate_categories = (
                constraints[0].get("intermediate_categories", None) or []
            )
        if len(intermediate_categories) > 1:
            raise ValueError(
                "Pathfinder queries do not support multiple intermediate categories"
            )
    else:
        intermediate_categories = ["biolink:NamedThing"]

    return pinned_node_keys, pinned_node_ids, intermediate_categories


def execute_pathfinding(
    pinned_node_ids, pinned_node_keys, intermediate_categories, logger
):
    blocked_curies, blocked_synonyms = get_blocked_list(logger)

    curie_ngd_path, node_degree_path = arax_pathfinder_sqlite_paths()
    pathfinder_instance = Pathfinder(
        f"retriever:{settings.sync_kg_retrieval_url}",
        f"sqlite:{curie_ngd_path}",
        f"sqlite:{node_degree_path}",
        blocked_curies,
        blocked_synonyms,
        logger,
    )

    descendants = get_descendants(intermediate_categories[0])

    start = time.perf_counter()
    logger.info("Starting pathfinder.get_paths()")

    result, aux_graphs, knowledge_graph = pathfinder_instance.get_paths(
        pinned_node_ids[0],
        pinned_node_ids[1],
        pinned_node_keys[0],
        pinned_node_keys[1],
        NUM_TOTAL_HOPS,
        MAX_HOPS_TO_EXPLORE,
        MAX_PATHFINDER_PATHS,
        PRUNE_TOP_K,
        NODE_DEGREE_THRESHOLD,
        descendants,
    )

    elapsed = time.perf_counter() - start
    logger.info(f"pathfinder.get_paths() finished in {elapsed:.3f} seconds")

    return result, aux_graphs, knowledge_graph


def arax_pathfinder_task(
    query_id: str, response_id: str, logger: logging.Logger
) -> None:
    """Process-pool entrypoint: load, search, rehydrate, and save in the child.

    Only the two small ids cross the process-pool boundary; the (potentially
    very large) message is read from Redis, searched over, and written back
    inside the child. That keeps the payload off the parent's heap and, more
    importantly, keeps the graph search off the parent's event loop -- it used
    to run via ``asyncio.to_thread``, where the GIL meant a handful of
    concurrent searches could starve the heartbeat past HEARTBEAT_TTL_SEC and
    get a live worker's tasks reclaimed out from under it (matching
    aragorn_score / arax_rank).
    """
    start = time.time()
    message = get_message_sync(query_id)
    parameters = message.get("parameters") or {}
    parameters["timeout"] = parameters.get("timeout", settings.lookup_timeout)
    parameters["tiers"] = parameters.get("tiers") or [0]
    message["parameters"] = parameters

    pinned_node_keys, pinned_node_ids, intermediate_categories = parse_query_graph(
        message["message"]["query_graph"]
    )

    try:
        result, aux_graphs, knowledge_graph = execute_pathfinding(
            pinned_node_ids,
            pinned_node_keys,
            intermediate_categories,
            logger,
        )
        logger.info("Rehydrating knowledge graph with retriever")
        knowledge_graph = rehydrate(knowledge_graph, settings.kg_rehydrate_url, logger)
    except Exception as e:
        # Let the failure reach run_task_lifecycle, which records it on the span
        # and routes the query to finish_query with an ERROR status. Previously
        # this saved a non-TRAPI {"status": "error"} blob and reported success,
        # so the bogus payload flowed on down the workflow.
        logger.error(
            f"PathFinder failed to find paths between {pinned_node_keys[0]} and "
            f"{pinned_node_keys[1]}. Error message is: {e}"
        )
        raise

    res = []
    if result is not None:
        res.append(
            {
                "id": result["id"],
                "analyses": result["analyses"],
                "node_bindings": result["node_bindings"],
                "essence": "result",
            }
        )
    if aux_graphs is None:
        aux_graphs = {}
    if knowledge_graph is None:
        knowledge_graph = {}
    message["message"]["knowledge_graph"] = knowledge_graph
    message["message"]["auxiliary_graphs"] = aux_graphs
    message["message"]["results"] = res

    message = add_shepherd_arax_to_edge_sources(message)

    save_message_sync(response_id, message)
    logger.info(f"Task took {time.time() - start}")


async def process_task(task, parent_ctx, logger, limiter, loop, pool):
    """Process a given task and ACK in redis.

    Pathfinding is CPU-bound, so it is dispatched to a process pool while the
    span, wrap-up, and error handling are shared with every worker. Only the
    ids are handed to the child; the message load/save happen there (see
    ``arax_pathfinder_task``) so the payload never crosses the process boundary.

    Dispatch goes through ``pool`` (a ProcessPoolManager) so a child dying on an
    oversized message replaces the pool instead of poisoning it for good, and a
    search that runs away is time-bounded rather than holding its slot forever.
    """

    async def _run(task, logger):
        await pool.run(
            loop,
            arax_pathfinder_task,
            task[1]["query_id"],
            task[1]["response_id"],
            logger,
        )

    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, _run)


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    startup_logger = logging.getLogger(STREAM)
    # Ensure the two sqlite databases exist before any task tries to open them
    # (a first-run local `docker compose up` starts with the volume-mounted
    # directory empty). No-op once present or when no scp source is configured
    # (e.g. production, where the data is mounted out of band).
    ensure_arax_pathfinder_dbs(startup_logger)
    # Fetch the blocked-concept list once here rather than per task, so the pool
    # children only ever read it.
    ensure_blocked_list(startup_logger)
    loop = asyncio.get_running_loop()
    # Size the pool by the pod's actual CPU allocation (cgroup limit), not
    # os.cpu_count() -- see aragorn_score.poll_for_tasks. Each child loads a full
    # message, so this also bounds peak memory. POOL_MAX_WORKERS overrides.
    max_workers = resolve_pool_workers(TASK_LIMIT, startup_logger)
    logging.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="arax.pathfinder process pool",
        task_timeout=settings.pool_task_timeout_sec,
    )
    while True:
        try:
            async for task, parent_ctx, logger, limiter in get_tasks(
                STREAM, GROUP, CONSUMER, max_workers
            ):
                asyncio.create_task(
                    process_task(task, parent_ctx, logger, limiter, loop, pool)
                )
        except asyncio.CancelledError:
            logging.info("Poll loop cancelled, shutting down.")
        except Exception as e:
            logging.error(f"Error in task polling loop: {e}", exc_info=True)
            await asyncio.sleep(5)  # back off before retrying


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())

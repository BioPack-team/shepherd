"""Gandalf lookup module for Shepherd workflow runner."""

import asyncio
import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from bmt import Toolkit
from gandalf import CSRGraph, lookup

from shepherd_utils.db import (
    get_callback_query_id,
    get_message,
    get_query_state,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import add_task, get_tasks, mark_task_as_complete

# Queue name
STREAM = "gandalf"
# Consumer group
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
# Process one task at a time to avoid GC state races and memory multiplication.
# Scale horizontally by adding more container replicas.
TASK_LIMIT = 1

tracer = setup_tracer(STREAM)

# Graph loading configuration
GRAPH_PATH = os.environ.get("GANDALF_GRAPH_PATH", "./gandalf_mmap")
GRAPH_FORMAT = os.environ.get("GANDALF_GRAPH_FORMAT", "auto")

# Debug response logging (disabled by default in production)
DEBUG_RESPONSES = os.environ.get("GANDALF_DEBUG_RESPONSES", "false").lower() == "true"

logger = logging.getLogger(__name__)


def load_graph(path: str, fmt: str = "auto") -> CSRGraph:
    """Load graph from disk.

    Args:
        path: Path to graph file (pickle) or directory (mmap).
        fmt: "auto" (detect from path), "pickle", or "mmap".

    Returns:
        Loaded CSRGraph.
    """
    path = Path(path)

    if fmt == "auto":
        if path.is_dir():
            fmt = "mmap"
        elif path.suffix == ".pkl":
            fmt = "pickle"
        else:
            raise ValueError(f"Cannot auto-detect format for: {path}")

    if fmt == "mmap":
        return CSRGraph.load_mmap(path)
    elif fmt == "pickle":
        return CSRGraph.load(path)
    else:
        raise ValueError(f"Unknown format: {fmt}")


def gandalf_lookup(graph, bmt, in_message, task_logger: logging.Logger):
    """Run a Gandalf lookup for a single task."""
    task_logger.info("Starting Gandalf lookup")
    return lookup(graph, in_message, bmt=bmt)


async def poll_for_tasks(graph: CSRGraph, bmt: Toolkit):
    """Poll Redis Streams for tasks and process them."""
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1)

    async for task, parent_ctx, task_logger, limiter in get_tasks(
        STREAM, GROUP, CONSUMER, TASK_LIMIT
    ):
        span = tracer.start_span(STREAM, context=parent_ctx)
        start = time.time()
        try:
            task_logger.info("Got task for Gandalf")
            response_id = task[1]["response_id"]
            callback_id = task[1]["callback_id"]
            target = task[1].get("target", "unknown")

            task_logger.info("Getting message")
            message = await get_message(callback_id, task_logger)
            if message is None:
                task_logger.error(f"Failed to get {response_id} for lookup.")
                continue

            query_id = await get_callback_query_id(callback_id, task_logger)
            task_logger.info(f"Got original query id: {query_id}")
            if query_id is None:
                task_logger.error("Failed to get original query id.")
                continue

            query_state = await get_query_state(query_id, task_logger)
            if query_state is None:
                task_logger.error("Failed to get query state.")
                continue

            response_id = query_state[7]

            lookup_response = await loop.run_in_executor(
                executor,
                gandalf_lookup,
                graph,
                bmt,
                message,
                task_logger,
            )

            if DEBUG_RESPONSES and len(lookup_response["message"]["results"]) > 0:
                debug_dir = Path("debug")
                debug_dir.mkdir(exist_ok=True)
                debug_path = debug_dir / f"{query_id}_{callback_id}_response.json"
                with open(debug_path, "w", encoding="utf-8") as f:
                    json.dump(lookup_response, f, indent=2)

            task_logger.info(f"Saving callback {callback_id} to redis")
            await save_message(callback_id, lookup_response, task_logger)
            task_logger.info(f"Saved callback {callback_id} to redis")

            await add_task(
                "merge_message",
                {
                    "target": target,
                    "query_id": query_id,
                    "response_id": response_id,
                    "callback_id": callback_id,
                    "log_level": task[1].get("log_level", 20),
                    "otel": task[1]["otel"],
                },
                task_logger,
            )
        except Exception:
            task_logger.exception(f"Task {task[0]} failed")
        finally:
            await mark_task_as_complete(STREAM, GROUP, task[0], logger)
            task_logger.info(f"Finished task {task[0]} in {time.time() - start:.2f}s")
            span.end()
            limiter.release()


if __name__ == "__main__":
    logger.info(f"Loading graph from {GRAPH_PATH} (format={GRAPH_FORMAT})...")
    graph = load_graph(GRAPH_PATH, GRAPH_FORMAT)
    logger.info("Graph loaded.")

    logger.info("Initializing Biolink Model Toolkit...")
    bmt = Toolkit()
    logger.info("BMT initialized.")

    logger.info(f"Starting Gandalf worker (consumer={CONSUMER})...")
    asyncio.run(poll_for_tasks(graph, bmt))

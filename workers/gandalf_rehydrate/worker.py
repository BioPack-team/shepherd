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
from gandalf import CSRGraph, enrich_knowledge_graph

from shepherd_utils.db import (
    get_message,
    save_message,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, wrap_up_task

# Queue name
STREAM = "gandalf.rehydrate"
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


def gandalf_rehydration(graph, bmt, in_message, task_logger: logging.Logger):
    """Run a Gandalf lookup for a single task."""
    task_logger.info("Starting Gandalf lookup")
    return enrich_knowledge_graph(in_message, graph)


async def poll_for_tasks(graph: CSRGraph, bmt: Toolkit):
    """Poll Redis Streams for tasks and process them."""
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(max_workers=1)

    async for task, parent_ctx, task_logger, limiter in get_tasks(
        STREAM, GROUP, CONSUMER, TASK_LIMIT
    ):
        span = tracer.start_span(STREAM, context=parent_ctx)
        start = time.time()
        task_logger.info("Got task for Gandalf Rehydration")
        workflow = json.loads(task[1]["workflow"])
        response_id = task[1]["response_id"]
        try:
            message = await get_message(response_id, task_logger)
            if message is None:
                task_logger.error(f"Failed to get {response_id} for rehydration.")
                continue

            hydrated_response = await loop.run_in_executor(
                executor,
                gandalf_rehydration,
                graph,
                bmt,
                message,
                task_logger,
            )

            if DEBUG_RESPONSES and len(hydrated_response["message"]["results"]) > 0:
                debug_dir = Path("debug")
                debug_dir.mkdir(exist_ok=True)
                debug_path = debug_dir / f"{response_id}_response.json"
                with open(debug_path, "w", encoding="utf-8") as f:
                    json.dump(hydrated_response, f, indent=2)

            task_logger.info(f"Saving response {response_id} to redis")
            await save_message(response_id, hydrated_response, task_logger)
            task_logger.info(f"Saved response {response_id} to redis")

        except Exception:
            task_logger.exception(f"Task {task[0]} failed")
        finally:
            await wrap_up_task(STREAM, GROUP, task, workflow, logger)
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

"""Aragorn set enrichment (AnswerCoalesce) module.

A local port of AnswerCoalesce's multi-curie query (MCQ) mode
(https://github.com/ranking-agent/AnswerCoalesce): given a set of input
curies (a query node with ``set_interpretation: MANY`` and ``member_ids``), it
finds the nodes linked to more of the set than chance predicts and returns
each as a result. See ``mcq.py`` for the answer's shape and ``store.py`` for
the statistics.

The graph is a read-only DuckDB database (``ANSWER_COALESCE_DUCKDB_PATH``),
volume-mounted like the omnicorp LMDBs; ``build_duckdb.py`` builds one from KGX
JSONL. The enrichment query is CPU- and memory-heavy, so it runs in a process
pool, each child holding its own read-only connection.

This is the first operation of Aragorn's MCQ workflow: it reads the query and
writes the response.
"""

import asyncio
import json
import logging
import uuid

from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.db import get_message_sync, save_response_sync
from shepherd_utils.logger import (
    get_worker_logger,
    ingest_query_logs,
    pool_task_logger,
)
from shepherd_utils.otel import setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import run_poll_loop, run_task_lifecycle
from shepherd_utils.trapi import operation_parameters, query_parameters

from .mcq import answer_mcq

# Queue name
STREAM = "aragorn.coalesce"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
# In-flight task cap; the pool (sized from the CPU allocation) bounds how many
# enrichments actually run at once.
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)
LOGGER = get_worker_logger(STREAM)

#: How a query that ran out of time here is told which step it was.
ENRICHMENT_STEP = "Set enrichment (answer coalesce)"


def aragorn_coalesce(
    query_id: str,
    response_id: str,
    operation: dict,
    log_level: int = logging.INFO,
) -> list[dict]:
    """Process-pool entrypoint: load the query, enrich, and save the response.

    Only ids and the (small) operation cross the process boundary; the query
    and the response stay in the child. Returns the child's log records for
    the parent to fold into the query's logs.
    """
    with pool_task_logger(STREAM, log_level) as (logger, query_log_handler):
        query = get_message_sync(query_id)
        # The query's parameters, overridden by the workflow operation's.
        parameters = {
            **query_parameters(query),
            **operation_parameters(operation),
        }
        logger.info("Running set enrichment.")
        response = answer_mcq(query, parameters)
        logger.info(
            f"Set enrichment found {len(response['message']['results'])} results."
        )
        save_response_sync(response_id, response)
        return query_log_handler.drain()


async def process_task(
    task, parent_ctx, logger: logging.Logger, limiter, loop, pool: ProcessPoolManager
):
    """Run one task's enrichment in the pool and ACK it in redis."""

    async def _run(task, logger):
        workflow = json.loads(task[1].get("workflow") or "null") or []
        operation = workflow[0] if workflow and workflow[0]["id"] == STREAM else {}
        entries = await pool.run_within_budget(
            loop,
            task[1],
            ENRICHMENT_STEP,
            aragorn_coalesce,
            task[1]["query_id"],
            task[1]["response_id"],
            operation,
            logger.getEffectiveLevel(),
        )
        ingest_query_logs(logger, entries)

    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, _run)


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
    loop = asyncio.get_running_loop()
    # Each child runs DuckDB with its own memory limit
    # (ANSWER_COALESCE_DUCKDB_MEMORY_LIMIT), so the pool size times that limit
    # is what bounds the worker's memory; size it from the cgroup's CPUs (or
    # POOL_MAX_WORKERS), not the host's.
    max_workers = resolve_pool_workers(TASK_LIMIT, LOGGER)
    LOGGER.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="aragorn.coalesce process pool",
        task_timeout=settings.pool_task_timeout_sec,
    )
    await run_poll_loop(
        STREAM,
        GROUP,
        CONSUMER,
        max_workers,
        lambda *task_args: process_task(*task_args, loop, pool),
        on_stop=pool.shutdown,
    )


if __name__ == "__main__":
    asyncio.run(poll_for_tasks())

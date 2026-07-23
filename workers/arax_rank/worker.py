"""
ARAX Rank Worker for Shepherd.

This worker implements ARAX ranking algorithm as a Shepherd worker.
It listens on the 'arax.rank' stream and ranks TRAPI message results
using the ARAX ranking algorithms (using max flow, longest path, Frobenius norm).

The ranking algorithm is a direct replication of RTX/code/ARAX/ARAXQuery/ARAX_ranker.py,
adapted for Shepherd's dict-based TRAPI message structure.

"""

import asyncio
import json
import logging
import uuid

from shepherd_utils.config import settings
from shepherd_utils.cpu import resolve_pool_workers
from shepherd_utils.db import get_message_sync, save_message_sync
from shepherd_utils.otel import setup_tracer
from shepherd_utils.process_pool import ProcessPoolManager
from shepherd_utils.shared import get_tasks, run_task_lifecycle

from ranker import arax_rank

# Queue name
STREAM = "arax.rank"
# Consumer group, most likely you don't need to change this.
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)


def rank_message(in_message: dict, logger: logging.Logger) -> dict:
    """
    Rank a TRAPI message using ARAX algorithms.

    This function is designed to be run in a process pool executor
    for CPU-intensive ranking operations.

    Args:
        in_message: TRAPI message dict
        logger: Logger instance

    Returns:
        Ranked message
    """
    # save the logs for the response (if any)
    if "logs" not in in_message or in_message["logs"] is None:
        in_message["logs"] = []
    else:
        # Convert timestamps to strings for JSON serialization
        for log in in_message.get("logs", []):
            if "timestamp" in log:
                log["timestamp"] = str(log["timestamp"])

    # Check if message has results to rank
    if not in_message.get("message"):
        logger.warning("No message found in input")
        return in_message

    msg = in_message["message"]

    if not msg.get("results"):
        logger.info("No results to rank")
        return in_message

    try:
        # Run ARAX ranking
        ranked_message = arax_rank(in_message, logger)
        logger.info(f"Successfully ranked {len(msg.get('results', []))} results")
        return ranked_message

    except Exception as e:
        logger.exception(f"ARAX ranking failed: {e}")
        # Return original message on failure
        return in_message


def arax_rank_task(response_id: str, logger: logging.Logger) -> None:
    """Process-pool entrypoint: load, rank, and save entirely in the child.

    Only the small ``response_id`` crosses the process-pool boundary; the
    (potentially very large) message is read from Redis, ranked, and written
    back inside the child. Previously the parent loaded the whole message on the
    event loop and pickled it across to the child and back -- so a big message
    sat on the parent's heap and was resident several times over, and the
    decode blocked the loop. Reading and writing here keeps the payload off the
    parent entirely (matching aragorn_omnicorp / aragorn_score).
    """
    message = get_message_sync(response_id)
    ranked_message = rank_message(message, logger)
    if ranked_message is None:
        ranked_message = message
    save_message_sync(response_id, ranked_message)


async def process_task(task, parent_ctx, logger, limiter, loop, pool):
    """Process a given task and ACK in redis.

    Ranking is CPU-bound, so it is dispatched to a process pool while the
    span, wrap-up, and error handling are shared with every worker. Only the
    ``response_id`` is handed to the child; the message load/save happen there
    (see ``arax_rank_task``) so the payload never crosses the process boundary.
    """

    async def _run(task, logger):
        response_id = task[1]["response_id"]
        await pool.run(loop, arax_rank_task, response_id, logger)

    await run_task_lifecycle(STREAM, GROUP, task, parent_ctx, logger, limiter, _run)


async def poll_for_tasks() -> None:
    """
    Main loop to poll for and process ranking tasks.

    Creates a single self-healing process pool that is reused across all tasks
    for better performance.
    """
    loop = asyncio.get_running_loop()
    # Size the pool by the pod's actual CPU allocation (cgroup limit), not
    # os.cpu_count() -- see aragorn_omnicorp.poll_for_tasks. Each child loads a
    # full message, so this also bounds peak memory. POOL_MAX_WORKERS overrides.
    max_workers = resolve_pool_workers(TASK_LIMIT, logging.getLogger(STREAM))
    logging.info(f"{STREAM}: process pool sized to {max_workers} worker(s).")
    pool = ProcessPoolManager(
        max_workers,
        max_tasks_per_child=settings.pool_max_tasks_per_child,
        name="arax.rank process pool",
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

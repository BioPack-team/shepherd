"""Node normalization worker.

Thin worker wrapper around :mod:`shepherd_utils.ars_norm`, which canonicalizes a
message's node curies (merging duplicates, rewriting edges/bindings, and emitting
``biolink:xref``/``biolink:same_as`` attributes). The ARS pipeline normalizes each
ARA response *before* the cross-ARA merge (in ``ars_accumulate``) so this worker
is not part of the ARS post-merge tail, but it remains a valid standalone
``node_norm`` workflow operation.

Faithful to Relay's ``canonizeMessage``: on any normalizer failure the message is
passed through unchanged so a flaky service never drops results.
"""

import asyncio
import logging
import uuid

from shepherd_utils.ars_norm import (
    canonize_message,
    get_normalized_nodes,
    normalize_message,
)
from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, run_task_lifecycle

# Re-exported for callers/tests that import these from this module.
__all__ = [
    "canonize_message",
    "get_normalized_nodes",
    "normalize_message",
    "node_norm",
]

# Queue name
STREAM = "node_norm"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 100
tracer = setup_tracer(STREAM)


async def node_norm(task, logger: logging.Logger):
    """Canonicalize all node curies in the accumulated message."""
    response_id = task[1]["response_id"]
    message = await get_message(response_id, logger)
    if await normalize_message(message, logger):
        await save_message(response_id, message, logger)


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, node_norm
    )


async def poll_for_tasks():
    """On initialization, poll indefinitely for available tasks."""
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

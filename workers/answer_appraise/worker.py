"""Answer appraiser worker.

Sends the merged message to the answer appraiser (``settings.appraiser_url``),
which returns ``ordering_components`` per result, then percentile-ranks result
scores 0-100. Faithful to Relay's ``appraise`` + ``normalizeScores``: on any
appraiser failure every result still gets a default ``ordering_components`` so
downstream ordering never sees a missing field.
"""

import asyncio
import logging
import uuid

import httpx
import orjson
import zstandard
from scipy.stats import rankdata

from shepherd_utils.config import settings
from shepherd_utils.db import get_message, save_message
from shepherd_utils.otel import setup_tracer
from shepherd_utils.shared import get_tasks, run_task_lifecycle

# Queue name
STREAM = "answer_appraise"
GROUP = "consumer"
CONSUMER = str(uuid.uuid4())[:8]
TASK_LIMIT = 10
tracer = setup_tracer(STREAM)

DEFAULT_ORDERING_COMPONENTS = {
    "novelty": 0,
    "confidence": 0,
    "clinical_evidence": 0,
}


def _result_score(result: dict) -> float:
    """Best analysis score for a result (0 when unscored)."""
    scores = [a.get("score", 0) or 0 for a in result.get("analyses", []) or []]
    return max(scores) if scores else 0


def normalize_scores(results: list[dict]) -> None:
    """Percentile-rank result scores onto a 0-100 ``normalized_score`` in place."""
    if not results:
        return
    scores = [_result_score(r) for r in results]
    n = len(scores)
    if n == 1:
        results[0]["normalized_score"] = 100.0 if scores[0] else 0.0
        return
    ranks = rankdata(scores, method="average")
    for result, rank in zip(results, ranks):
        # Map average rank (1..n) onto 0..100.
        result["normalized_score"] = float((rank - 1) / (n - 1) * 100)


def apply_defaults(results: list[dict]) -> None:
    """Ensure every result has ordering_components (used on appraiser failure)."""
    for result in results:
        result.setdefault("ordering_components", dict(DEFAULT_ORDERING_COMPONENTS))


async def appraise(message: dict, logger: logging.Logger) -> None:
    """POST to the appraiser and merge ordering_components onto each result."""
    results = message.get("message", {}).get("results", []) or []
    if not results:
        return
    try:
        payload = zstandard.compress(orjson.dumps({"message": message["message"]}))
        headers = {
            "Content-Type": "application/json",
            "Content-Encoding": "zstd",
        }
        async with httpx.AsyncClient(timeout=270) as client:
            response = await client.post(
                settings.appraiser_url, content=payload, headers=headers
            )
            response.raise_for_status()
        appraised = (response.json() or {}).get("message", {}).get("results", []) or []
        # The appraiser returns results in the same order; merge by index.
        for result, appraised_result in zip(results, appraised):
            oc = appraised_result.get("ordering_components")
            if oc is not None:
                result["ordering_components"] = oc
        apply_defaults(results)
        logger.info(f"Appraised {len(appraised)} results.")
    except Exception as e:
        logger.error(f"Appraiser failed; applying default ordering_components: {e}")
        apply_defaults(results)


async def answer_appraise(task, logger: logging.Logger):
    """Appraise and normalize the merged message's results."""
    response_id = task[1]["response_id"]
    message = await get_message(response_id, logger)
    await appraise(message, logger)
    normalize_scores(message.get("message", {}).get("results", []) or [])
    await save_message(response_id, message, logger)


async def process_task(task, parent_ctx, logger, limiter):
    """Process a given task and ACK in redis."""
    await run_task_lifecycle(
        STREAM, GROUP, task, parent_ctx, logger, limiter, answer_appraise
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

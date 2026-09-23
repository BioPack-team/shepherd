"""Fail a query whose response has grown too large to process.

A TRAPI response is loaded into memory as a Python object tree several times
larger than its JSON, by every worker that touches it, and the merge worker
reloads the whole accumulated response on every pass. A response big enough
blows past a pod's memory limit and is OOM-killed -- an uncatchable SIGKILL --
and the task that killed it is retried on the next pod, which dies the same
way. Observed in production as the merge_message worker growing to tens of GB
on a single query and then crash-looping once capped.

This module is the one place that decides what happens when a response is
judged too large, however that was detected (a callback rejected at the door,
a size cap tripped before a load, or a merge that keeps crashing the worker):

* the response is **discarded whole**. Whatever had been merged so far is
  replaced with an empty results message that says why -- not trimmed, not
  partially kept. A partial answer that silently omits most of the data is
  worse than an honest error;
* a marker is written under the response id so every later stage agrees. The
  merge worker drops any callback that arrives afterwards instead of growing
  the response again, and ``finish_query`` records the ``RESPONSE_TOO_LARGE``
  status and delivers the empty message;
* the query's outstanding callbacks are cleared so the lookup worker stops
  waiting and the workflow runs to ``finish_query`` promptly;
* the event is logged at CRITICAL, in the worker's own output and in the
  query's log list, so it is impossible to miss.
"""

import logging
from typing import Any, Dict, Optional

from . import db as _db
from .config import settings

# ``status`` recorded in ``shepherd_brain`` (and reported by
# ``/asyncquery_status``) for a query whose response was discarded as too
# large. Distinct from ERROR so it can be told apart afterwards.
TOO_LARGE_STATUS = "RESPONSE_TOO_LARGE"

# TRAPI ``status`` / ``description`` stamped on the empty response we deliver.
TOO_LARGE_RESPONSE_STATUS = "Error"
TOO_LARGE_DESCRIPTION_PREFIX = "Response too large"

# Greppable marker at the head of every CRITICAL line this module emits.
TOO_LARGE_LOG_MARKER = "RESPONSE_TOO_LARGE"

_TOO_LARGE_PREFIX = "response_too_large:"


def too_large_key(response_id: str) -> str:
    return f"{_TOO_LARGE_PREFIX}{response_id}"


def build_too_large_response(
    query_graph: Optional[Dict[str, Any]], reason: str
) -> Dict[str, Any]:
    """The empty TRAPI response delivered in place of a discarded one.

    Carries the original query graph when there is one (so the caller can
    still tell which query this answers), an empty knowledge graph and result
    list, and a ``status`` + ``description`` saying what happened. TRAPI 2.0
    forbids an empty ``auxiliary_graphs``, and a query graph without nodes, so
    both are simply absent. ``logs`` is left out on purpose: they are added
    when the response is delivered (``finalize_response``).
    """
    message: Dict[str, Any] = {
        "knowledge_graph": {"nodes": {}, "edges": {}},
        "results": [],
    }
    if query_graph:
        message["query_graph"] = query_graph
    return {
        "message": message,
        "status": TOO_LARGE_RESPONSE_STATUS,
        "description": f"{TOO_LARGE_DESCRIPTION_PREFIX}: {reason}",
    }


async def get_too_large_reason(response_id: str) -> Optional[str]:
    """The reason ``response_id`` was discarded, or ``None`` if it wasn't."""
    raw = await _db.data_db_client.get(too_large_key(response_id))
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    return str(raw)


async def is_response_too_large(response_id: str) -> bool:
    return await get_too_large_reason(response_id) is not None


async def mark_response_too_large(response_id: str, reason: str) -> None:
    """Write the marker every later stage checks. Same TTL as the payloads."""
    await _db.data_db_client.set(
        too_large_key(response_id), reason, ex=settings.redis_ttl
    )


async def write_too_large_response(
    query_id: str,
    response_id: str,
    reason: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """Replace the stored response with the empty too-large message.

    Idempotent, and safe to call again later: a merge pass that was already in
    flight when the query was failed can save the big accumulator over the
    top of this, so ``finish_query`` rewrites it before delivering.
    """
    query_graph = None
    try:
        query = await _db.get_message(query_id, logger)
        query_graph = query.get("message", {}).get("query_graph")
    except Exception as e:
        logger.warning(
            f"Couldn't read query {query_id} for the too-large response: {e}"
        )
    response = build_too_large_response(query_graph, reason)
    await _db.save_message(response_id, response, logger)
    return response


async def fail_response_too_large(
    query_id: str,
    response_id: str,
    reason: str,
    logger: logging.Logger,
) -> None:
    """Discard ``response_id`` as too large and settle everything hanging off it.

    Order matters. The marker goes first so that any merge or callback racing
    with this call sees it and stands down; the tombstone response next, so
    nothing downstream can load the big one; then the ready index and the
    callback rows, which unblocks the lookup worker waiting on them and lets
    the workflow proceed to ``finish_query``; and the logs are flushed last so
    the CRITICAL line is in the query's log list before ``finish_query`` reads
    it into the delivered response.
    """
    logger.critical(
        f"{TOO_LARGE_LOG_MARKER} query={query_id} response={response_id}: "
        f"{reason}. Discarding the whole response; the query will finish with "
        f"status {TOO_LARGE_STATUS} and no results."
    )
    await mark_response_too_large(response_id, reason)
    await write_too_large_response(query_id, response_id, reason, logger)
    await _db.clear_ready_callbacks(response_id, logger)
    try:
        await _db.cleanup_callbacks(query_id, logger)
    except Exception as e:
        logger.error(f"Failed to clear callbacks for too-large query {query_id}: {e}")
    try:
        await _db.save_logs(response_id, logger)
    except Exception as e:
        logger.error(f"Failed to save logs for too-large query {query_id}: {e}")

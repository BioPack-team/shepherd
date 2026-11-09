"""Workflow operations cheap enough to run back to back in one pass (#219).

Every default ARAGORN/BTE workflow ends ``sort_results_score`` ->
``filter_results_top_n`` -> ``filter_kgraph_orphans``. Run as three workers,
that was three broker hops, and three full load (decompress, parse) / save
(serialize, compress) cycles of the whole response, for work that takes
milliseconds. Now whichever of these workers gets the task applies its own
operation and every consecutive local one after it, on one loaded message,
and advances the workflow past all of them.
"""

import json
import logging
from typing import Any, Callable, Dict, List

from .shared import filter_kgraph_orphans
from .trapi import operation_parameters


def _score(analysis: Dict[str, Any]):
    return analysis.get("score")


def _scored_first(score, reverse: bool):
    """Sort key putting scored items first in either direction (C5-8).

    An unscored item used to sort as 0, so with negative scores it ranked
    above every scored one.
    """
    if score is None:
        # Last in both directions: the key is reversed for descending.
        return (0, 0) if reverse else (1, 0)
    return (1, score) if reverse else (0, score)


def sort_results(message: Dict[str, Any], operation: Dict[str, Any], logger) -> None:
    """``sort_results_score``: order each result's analyses and the results
    by score, in place."""
    results = message["message"].get("results") or []
    aord = operation_parameters(operation).get("ascending_or_descending", "descending")
    reverse = aord == "descending"
    # Sort in place (list.sort, not sorted()) throughout: the decoded response
    # is by far the largest object in memory, so a second copy of the results
    # list -- and of every result's analyses -- is wasted peak memory.
    # TRAPI 2.0: ``Result.analyses`` is optional (minItems 1 when present).
    for result in results:
        if result.get("analyses"):
            result["analyses"].sort(
                key=lambda a: _scored_first(_score(a), reverse), reverse=reverse
            )

    def _result_key(result):
        analyses = result.get("analyses")
        # After the sort above, the best analysis is first either way.
        return _scored_first(_score(analyses[0]) if analyses else None, reverse)

    results.sort(key=_result_key, reverse=reverse)
    # Reattach so the key exists even when the response had no ``results``.
    message["message"]["results"] = results
    logger.info("Returning sorted results.")


def filter_top_n(message: Dict[str, Any], operation: Dict[str, Any], logger) -> None:
    """``filter_results_top_n``: keep the first ``max_results`` results, and
    drop the knowledge-graph nodes and edges only the dropped ones used (C5-7)."""
    results = message["message"].get("results") or []
    n = operation_parameters(operation).get("max_results", 500)
    dropped = len(results) > n
    # Truncate in place (del results[n:]) rather than slicing: a slice would
    # keep the dropped results alive until the save.
    del results[n:]
    message["message"]["results"] = results
    if dropped:
        filter_kgraph_orphans(message, logger)
    logger.info("Returning filtered results.")


def remove_orphans(message: Dict[str, Any], operation: Dict[str, Any], logger) -> None:
    """``filter_kgraph_orphans``: drop knowledge-graph elements no result uses."""
    filter_kgraph_orphans(message, logger)


LOCAL_OPERATIONS: Dict[str, Callable[[Dict[str, Any], Dict[str, Any], Any], None]] = {
    "sort_results_score": sort_results,
    "filter_results_top_n": filter_top_n,
    "filter_kgraph_orphans": remove_orphans,
}


def apply_local_steps(
    stream: str, task, message: Dict[str, Any], logger: logging.Logger
) -> List[str]:
    """Apply ``stream``'s operation and the local ones that follow it.

    ``task``'s workflow is rewritten so that ``wrap_up_task`` hands the query
    to the first step after the run. Returns the operations applied, in order.
    """
    workflow = json.loads(task[1].get("workflow") or "null") or []
    if not workflow or workflow[0].get("id") != stream:
        # Not at the head of the workflow (e.g. called as an entry worker):
        # just this operation, as before.
        LOCAL_OPERATIONS[stream](message, {"id": stream}, logger)
        return [stream]
    end = 1
    while end < len(workflow) and workflow[end].get("id") in LOCAL_OPERATIONS:
        end += 1
    applied = []
    for operation in workflow[:end]:
        LOCAL_OPERATIONS[operation["id"]](message, operation, logger)
        applied.append(operation["id"])
    if end > 1:
        logger.info(f"Ran {', '.join(applied)} in one pass.")
        # wrap_up_task drops the head of the workflow (this stream) and sends
        # the query to what follows, so the ones run here go with it.
        task[1]["workflow"] = json.dumps([workflow[0]] + workflow[end:])
    return applied

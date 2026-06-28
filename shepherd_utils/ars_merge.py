"""Cross-ARA result deduplication, ported from Relay's ``mergeDicts``.

The ARS dedups answers that are identical across ARAs: two results binding the
same set of node curies (the first binding id per qnode) are one answer. When
they collapse, their ``analyses`` are concatenated and differing result-level
scalar score fields accumulate into a list that is arithmetically averaged at the
end (Relay ``mergeMessagesRecursive`` finalize). This module is pure so it is
fully unit-testable.
"""

import logging
from typing import Any, Dict, FrozenSet, List


def result_key(result: Dict[str, Any]) -> FrozenSet[str]:
    """The dedup key for a result: frozenset of the first binding id per qnode.

    Mirrors Relay's ``getResultMap`` (``nodes.add(nb[q][0]["id"])``).
    """
    nodes = set()
    for bindings in (result.get("node_bindings") or {}).values():
        if bindings:
            binding = bindings[0]
            if "id" in binding:
                nodes.add(binding["id"])
    return frozenset(nodes)


def merge_two_results(
    merged: Dict[str, Any], current: Dict[str, Any], logger: logging.Logger
) -> Dict[str, Any]:
    """Merge ``current`` into ``merged`` in place (Relay ``mergeDicts`` for a
    result dict). Both bind the same answer, so node_bindings are kept as-is."""
    for key, cv in current.items():
        if key not in merged:
            merged[key] = cv
            continue
        mv = merged[key]
        if key == "analyses":
            merged[key] = (mv or []) + (cv or [])
            continue
        if key == "node_bindings":
            # Identical answer by construction; keep the existing bindings.
            continue
        if isinstance(mv, dict) and isinstance(cv, dict):
            merged[key] = {**mv, **cv}
            continue
        # An earlier merge already turned this scalar into an accumulator list;
        # keep appending (Relay: ``isinstance(mv,list) and not isinstance(cv,list)``).
        if isinstance(mv, list) and not isinstance(cv, list):
            mv.append(cv)
            continue
        if isinstance(mv, list) and isinstance(cv, list):
            merged[key] = mv + [x for x in cv if x not in mv]
            continue
        # Scalar tail (Relay's final else): equal/None values are left alone;
        # differing values accumulate, with ``score`` renamed to ``scores``.
        if mv == cv or cv is None or mv is None:
            continue
        if key == "score":
            del merged["score"]
            merged["scores"] = [mv, cv]
        elif key == "query_ids":
            merged["query_ids"] = [mv, cv]
        elif key == "name":
            continue
        else:
            merged[key] = [mv, cv]
    return merged


def merge_result_maps(
    parent_results: List[Dict[str, Any]],
    child_results: List[Dict[str, Any]],
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """Dedup parent + child results by ``result_key``, merging collisions.

    Associative across the N incremental per-ARA callbacks: a result already
    carrying accumulator lists (from prior merges) keeps accumulating.
    """
    result_map: Dict[FrozenSet[str], Dict[str, Any]] = {}
    for result in parent_results or []:
        result_map[result_key(result)] = result
    for result in child_results or []:
        key = result_key(result)
        existing = result_map.get(key)
        if existing is None:
            result_map[key] = result
        else:
            merge_two_results(existing, result, logger)
    return list(result_map.values())


def average_result_scores(results: List[Dict[str, Any]]) -> None:
    """Replace any accumulated ``normalized_score`` list with its mean in place
    (Relay ``mergeMessagesRecursive`` finalize)."""
    for result in results or []:
        ns = result.get("normalized_score")
        if isinstance(ns, list) and len(ns) > 0:
            result["normalized_score"] = sum(ns) / len(ns)


def merge_aux_graphs(
    parent_aux: Dict[str, Any], child_aux: Dict[str, Any]
) -> Dict[str, Any]:
    """Merge auxiliary graphs by id (Relay ``mergeDicts`` aux handling).

    For a shared aux id, union the edge lists and keep any other keys.
    """
    for aux_id, aux in (child_aux or {}).items():
        existing = parent_aux.get(aux_id)
        if existing is None:
            parent_aux[aux_id] = aux
            continue
        existing_edges = existing.get("edges")
        new_edges = aux.get("edges")
        if isinstance(existing_edges, list) and isinstance(new_edges, list):
            existing["edges"] = list(dict.fromkeys(existing_edges + new_edges))
        for key, value in aux.items():
            if key == "edges":
                continue
            existing.setdefault(key, value)
    return parent_aux

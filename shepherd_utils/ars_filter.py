"""Result filters, ported verbatim from Relay's ``tr_ars/utils.py``.

Each filter takes the message ``results`` (and, for ``node_type``, the kgraph
nodes) plus an argument and returns the kept results. ``apply_filters`` runs a
list of ``(filter_type, value)`` pairs in order over a TRAPI message in place.
"""

import logging
from typing import Any, Dict, List, Tuple


def hop_level_filter(results: List[Dict], hop_limit: int) -> List[Dict]:
    """Keep results whose number of bound qnodes is < ``hop_limit``."""
    return [r for r in results if len(r.get("node_bindings", {}).keys()) < hop_limit]


def score_filter(results: List[Dict], score_range: List[float]) -> List[Dict]:
    """Keep results with ``range[0] < normalized_score < range[1]``."""
    scored = [r for r in results if r.get("normalized_score") is not None]
    return [
        r for r in scored if score_range[0] < r["normalized_score"] < score_range[1]
    ]


def node_type_filter(
    kg_nodes: Dict[str, Any], results: List[Dict], forbidden_category: List[str]
) -> List[Dict]:
    """Drop results binding a node whose category is in ``forbidden_category``.

    Categories are compared with the ``biolink:`` prefix stripped (as in Relay).
    """
    forbidden_nodes = set()
    for node, value in kg_nodes.items():
        present = []
        for entity in value.get("categories", []) or []:
            present.append(entity.split(":")[1] if "biolink:" in entity else entity)
        if any(item in forbidden_category for item in present):
            forbidden_nodes.add(node)
    kept = []
    for result in results:
        ids = [
            str(val["id"])
            for res_value in result.get("node_bindings", {}).values()
            for val in res_value
        ]
        if not any(item in ids for item in forbidden_nodes):
            kept.append(result)
    return kept


def specific_node_filter(results: List[Dict], forbidden_node: List[str]) -> List[Dict]:
    """Drop results binding any curie in ``forbidden_node``."""
    kept = []
    for result in results:
        ids = [
            str(val["id"])
            for res_value in result.get("node_bindings", {}).values()
            for val in res_value
        ]
        if not any(item in ids for item in forbidden_node):
            kept.append(result)
    return kept


def apply_filters(
    message: Dict[str, Any],
    filters: List[Tuple[str, Any]],
    logger: logging.Logger,
) -> int:
    """Apply an ordered list of ``(filter_type, value)`` to a message in place.

    Returns the final result count. Unknown filter types are skipped.
    """
    msg = message.get("message", {})
    results = msg.get("results") or []
    kg_nodes = (msg.get("knowledge_graph") or {}).get("nodes", {}) or {}
    for filter_type, value in filters:
        if filter_type == "hop":
            results = hop_level_filter(results, value)
        elif filter_type == "score":
            results = score_filter(results, value)
        elif filter_type == "node_type":
            results = node_type_filter(kg_nodes, results, value)
        elif filter_type == "spec_node":
            results = specific_node_filter(results, value)
        else:
            logger.warning(f"Unknown filter type: {filter_type}")
    msg["results"] = results
    return len(results)

"""Tests for ``workers.arax_rank`` against TRAPI 2.0 messages.

The ranker is imported the way the worker imports it inside its container
(``from ranker import arax_rank``), so its directory goes on ``sys.path``.
"""

import copy
import logging
import sys
from pathlib import Path

from translator_tom import Response

sys.path.insert(0, str(Path(__file__).parents[2] / "workers" / "arax_rank"))

from workers.arax_rank.ranker import (  # noqa: E402
    EDGE_CONFIDENCE_MANUAL_AGENT,
    ARAXRanker,
    arax_rank,
)
from workers.arax_rank.worker import rank_message  # noqa: E402

logger = logging.getLogger(__name__)


def _edge(subject, obj, source, agent_type="not_provided", attributes=None):
    edge = {
        "subject": subject,
        "predicate": "biolink:related_to",
        "object": obj,
        "knowledge_level": "not_provided",
        "agent_type": agent_type,
        "sources": [
            {"resource_id": source, "resource_role": "primary_knowledge_source"}
        ],
    }
    if attributes:
        edge["attributes"] = attributes
    return edge


def _result(n0, n1, edge_id):
    return {
        "node_bindings": {"n0": {"ids": [n0]}, "n1": {"ids": [n1]}},
        "analyses": [
            {"resource_id": "infores:arax", "edge_bindings": {"e0": {"ids": [edge_id]}}}
        ],
    }


RESPONSE = {
    "message": {
        "query_graph": {
            "nodes": {"n0": {"ids": ["X:1"]}, "n1": {}},
            "edges": {"e0": {"subject": "n0", "object": "n1"}},
        },
        "knowledge_graph": {
            "nodes": {
                "X:1": {"categories": ["biolink:NamedThing"]},
                "Y:1": {"categories": ["biolink:NamedThing"]},
                "Y:2": {"categories": ["biolink:NamedThing"]},
            },
            "edges": {
                "manual": _edge("X:1", "Y:1", "infores:drugbank", "manual_agent"),
                "mined": _edge("X:1", "Y:2", "infores:semmeddb", "text_mining_agent"),
            },
        },
        "results": [_result("X:1", "Y:2", "mined"), _result("X:1", "Y:1", "manual")],
    }
}


def test_ranked_message_is_valid_trapi_2_and_has_no_confidence_on_edges():
    """Edge confidences are an internal ranking detail: a TRAPI 2.0 Edge has
    additionalProperties false, so none may be left on the saved edges."""
    ranked = rank_message(copy.deepcopy(RESPONSE), logger)
    for edge in ranked["message"]["knowledge_graph"]["edges"].values():
        assert "confidence" not in edge
    assert "logs" not in ranked
    Response.from_dict(ranked)
    scores = [r["analyses"][0]["score"] for r in ranked["message"]["results"]]
    assert scores == sorted(scores, reverse=True)
    # The manual-agent edge's result outranks the text-mined one.
    top = ranked["message"]["results"][0]
    assert top["analyses"][0]["edge_bindings"]["e0"]["ids"] == ["manual"]


def test_manual_agent_is_read_from_top_level_agent_type():
    ranker = ARAXRanker(logger)
    msg = copy.deepcopy(RESPONSE["message"])
    ranker._build_edge_lookup_and_stats(msg)
    ranker._score_all_edges(msg)
    assert ranker.edge_confidence["manual"] == EDGE_CONFIDENCE_MANUAL_AGENT
    assert ranker.edge_confidence["mined"] != EDGE_CONFIDENCE_MANUAL_AGENT


def test_top_level_agent_type_is_authoritative_over_legacy_attribute():
    edge = _edge(
        "X:1",
        "Y:1",
        "infores:x",
        "automated_agent",
        attributes=[
            {"attribute_type_id": "biolink:agent_type", "value": "manual_agent"}
        ],
    )
    assert ARAXRanker._is_manual_agent(edge) is False
    legacy = {
        "attributes": [
            {"attribute_type_id": "biolink:agent_type", "value": "manual_agent"}
        ]
    }
    assert ARAXRanker._is_manual_agent(legacy) is True


def test_result_without_analyses_is_not_given_a_synthetic_analysis():
    """An Analysis needs a resource_id and bindings in 2.0, so the ranker must
    not invent ``{"score": ...}`` analyses for results that have none."""
    response = copy.deepcopy(RESPONSE)
    del response["message"]["results"][0]["analyses"]
    ranked = arax_rank(response, logger)
    results = ranked["message"]["results"]
    assert sum("analyses" not in r for r in results) == 1
    # The result without analyses sorts last (score 0).
    assert "analyses" not in results[-1]
    Response.from_dict(ranked)

"""The comparison logic of scripts/arax_compare.py (the side-by-side parity
run of Shepherd's ARAX port against a deployed upstream ARAX)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import arax_compare as AC  # noqa: E402


def _response(results, edges=None, status="Success", logs=()):
    return {
        "status": status,
        "logs": list(logs),
        "message": {
            "results": [
                {
                    "node_bindings": {"n0": {"ids": ["A:1"]}, "n1": {"ids": [curie]}},
                    "analyses": [{"resource_id": "infores:arax", "score": score}],
                }
                for curie, score in results
            ],
            "knowledge_graph": {
                "nodes": {},
                "edges": {
                    key: {
                        "subject": "A:1",
                        "predicate": "biolink:treats",
                        "object": obj,
                        "sources": [
                            {
                                "resource_id": "infores:x",
                                "resource_role": "primary_knowledge_source",
                            }
                        ],
                    }
                    for key, obj in (edges or {}).items()
                },
            },
        },
    }


def _verdict(port, upstream, http=(200, 200)):
    return AC.compare(AC.summarize(http[0], port), AC.summarize(http[1], upstream), 0.9)


def test_identical_answers_are_same():
    r = _response([("B:1", 0.9), ("B:2", 0.5)], {"e1": "B:1"})
    assert _verdict(r, r)["verdict"] == "SAME"


def test_edge_keys_do_not_matter_only_the_edges():
    a = _response([("B:1", 0.9)], {"e1": "B:1"})
    b = _response([("B:1", 0.9)], {"other_key": "B:1"})
    assert _verdict(a, b)["verdict"] == "SAME"


def test_reranked_results_are_similar():
    a = _response([("B:1", 0.9), ("B:2", 0.5)])
    b = _response([("B:2", 0.95), ("B:1", 0.4)])
    diff = _verdict(a, b)
    assert diff["verdict"] == "SIMILAR"
    assert diff["results"]["max_rank_shift"] == 1
    assert diff["results"]["max_score_diff"] == 0.5


def test_different_results_or_outcome_are_a_diff():
    a = _response([("B:1", 0.9), ("B:2", 0.5)])
    assert _verdict(a, _response([("B:3", 0.9)]))["verdict"] == "DIFF"
    error = _response([], status="ERROR", logs=[{"level": "ERROR", "code": "X"}])
    diff = _verdict(a, error, http=(200, 400))
    assert diff["verdict"] == "DIFF"
    assert diff["outcome"]["http"] == {"port": 200, "upstream": 400}
    assert diff["outcome"]["error_code"] == {"port": None, "upstream": "X"}


def test_cases_are_trapi_2_queries_without_shepherd_only_ones():
    cases = AC.comparable_cases()
    assert cases
    names = {c.name for c in cases}
    assert "message_uris_reuse_a_response" not in names
    assert all(c.body and not c.method for c in cases)
    assert not any((c.arax or {}).get("skip") for c in cases)


def test_bodies_differ_only_by_shepherds_switch():
    (c,) = AC.comparable_cases(only=["lookup_one_hop_treats"])

    class Args:
        bypass_cache = True
        arax_internal = True

    port, upstream = AC.bodies(c, Args)
    assert port["parameters"].pop("arax_internal") is True
    assert port == upstream
    assert upstream["parameters"]["bypass_cache"] is True


def test_same_edges_with_other_attributes_are_counted():
    a = _response([("B:1", 0.9)], {"e1": "B:1"})
    b = _response([("B:1", 0.9)], {"e1": "B:1"})
    b["message"]["knowledge_graph"]["edges"]["e1"]["attributes"] = [
        {"attribute_type_id": "biolink:publications", "value": ["PMID:1"]}
    ]
    diff = _verdict(a, b)
    assert diff["verdict"] == "SIMILAR"
    (example,) = diff["edge_details"]["examples"]
    assert example["attributes_only_upstream"] == ["biolink:publications"]


def test_errors_are_kept_for_each_side():
    error = _response(
        [],
        status="UnhandledError",
        logs=[{"level": "ERROR", "code": "UnhandledError", "message": "boom"}],
    )
    diff = _verdict(_response([("B:1", 0.9)]), error, http=(200, 400))
    assert diff["errors"] == {"port": [], "upstream": ["boom"]}

"""ARAGORN ranker and omnicorp hot paths (#220)."""

import copy
import logging
import time

import pytest

from workers.aragorn_omnicorp import worker as omnicorp
from workers.aragorn_score import worker as score

logger = logging.getLogger(__name__)


def _ranker_message(n_support_nodes=40):
    """One answer whose support graph strings together many nodes."""
    nodes = {f"N:{i}": {"categories": ["biolink:NamedThing"]} for i in range(60)}
    edges = {}
    chain = [f"N:{i}" for i in range(n_support_nodes)]
    support = []
    for a, b in zip(chain, chain[1:]):
        edge_id = f"s-{a}-{b}"
        edges[edge_id] = {
            "subject": a,
            "object": b,
            "predicate": "biolink:related_to",
            "sources": [
                {
                    "resource_id": "infores:semmeddb",
                    "resource_role": "primary_knowledge_source",
                }
            ],
            "attributes": [
                {"attribute_type_id": "biolink:publications", "value": ["PMID:1"]}
            ],
        }
        support.append(edge_id)
    edges["inferred"] = {
        "subject": "N:0",
        "object": chain[-1],
        "predicate": "biolink:related_to",
        "sources": [
            {
                "resource_id": "infores:aragorn",
                "resource_role": "primary_knowledge_source",
            }
        ],
        "attributes": [
            {"attribute_type_id": "biolink:support_graphs", "value": ["aux"]}
        ],
    }
    return {
        "query_graph": {
            "nodes": {"n0": {"ids": ["N:0"]}, "n1": {}},
            "edges": {"e0": {"subject": "n0", "object": "n1"}},
        },
        "knowledge_graph": {"nodes": nodes, "edges": edges},
        "auxiliary_graphs": {"aux": {"edges": support}},
        "results": [
            {
                "node_bindings": {
                    "n0": {"ids": ["N:0"]},
                    "n1": {"ids": [chain[-1]]},
                },
                "analyses": [
                    {
                        "resource_id": "infores:aragorn",
                        "edge_bindings": {"e0": {"ids": ["inferred"]}},
                    }
                ],
            }
        ],
    }


# --- C6-5: the ranker ---------------------------------------------------------


def test_scoring_does_not_deep_copy_each_result(mocker):
    message = _ranker_message()
    ranker = score.Ranker(message, logger)
    deepcopies = mocker.spy(copy, "deepcopy")
    original = copy.deepcopy(message["results"])
    deepcopies.reset_mock()

    ranker.rank(message["results"])

    assert deepcopies.call_count == 0
    # the result is read, not written: no scratch keys left behind
    for result, before in zip(message["results"], original):
        for analysis, analysis_before in zip(result["analyses"], before["analyses"]):
            assert set(analysis) - {"score"} == set(analysis_before)


def test_probes_are_computed_once_per_ranker(mocker):
    message = _ranker_message()
    message["results"] = message["results"] * 5
    ranker = score.Ranker(message, logger)
    found = mocker.spy(score.Ranker, "_find_probes")

    ranker.rank(message["results"])

    assert found.call_count == 1


def test_the_laplacian_visits_only_pairs_that_carry_a_weight():
    """It used to loop over every pair of r-graph nodes in Python, creating an
    entry per pair: n**2 time and memory per analysis."""
    message = _ranker_message(n_support_nodes=40)
    ranker = score.Ranker(message, logger)
    (r_graph,) = ranker.get_rgraph(message["results"][0])

    laplacian, _, details = ranker.graph_laplacian(r_graph, ranker.probes())

    entries = sum(len(by_object) for by_object in details["edge_values"].values())
    assert entries <= 2 * len(r_graph["edges"])
    assert laplacian.shape[0] >= 40


# --- omnicorp ---------------------------------------------------------------------


def _omnicorp_message(dangling=False):
    edges = {
        "e1": {
            "subject": "A",
            "object": "B",
            "predicate": "biolink:related_to",
            "sources": [
                {
                    "resource_id": "infores:kp",
                    "resource_role": "primary_knowledge_source",
                }
            ],
        }
    }
    bound = ["e1", "missing-edge"] if dangling else ["e1"]
    return {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": ["A"]}, "n1": {}},
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            },
            "knowledge_graph": {
                "nodes": {c: {"categories": ["biolink:NamedThing"]} for c in "ABC"},
                "edges": edges,
            },
            "results": [
                {
                    "node_bindings": {"n0": {"ids": ["A"]}, "n1": {"ids": ["B"]}},
                    "analyses": [
                        {
                            "resource_id": "infores:aragorn",
                            "edge_bindings": {"e0": {"ids": bound}},
                        }
                    ],
                },
                {
                    "node_bindings": {"n0": {"ids": ["A"]}, "n1": {"ids": ["C"]}},
                    "analyses": [
                        {
                            "resource_id": "infores:aragorn",
                            "edge_bindings": {"e0": {"ids": ["e1"]}},
                        }
                    ],
                },
            ],
        }
    }


@pytest.fixture
def lmdb(monkeypatch):
    curies = {"A": 0, "B": 1, "C": 2}

    def _curie_query(keys):
        return {
            k: ({"pmc": 10, "index": curies[k]} if k in curies else {}) for k in keys
        }

    shared = {"reads": []}

    def _shared_count_query(keys):
        shared["reads"].append(list(keys))
        if shared.get("fail"):
            raise ValueError("shared-counts LMDB value is not ASCII decimal")
        if shared.get("slow"):
            time.sleep(shared["slow"])
        return {k: 3 for k in keys}

    monkeypatch.setattr(omnicorp, "curie_query", _curie_query)
    monkeypatch.setattr(omnicorp, "shared_count_query", _shared_count_query)
    return shared


def _co_occurrence_edges(message):
    return [
        e
        for e in message["message"]["knowledge_graph"]["edges"].values()
        if e.get("predicate") == "biolink:occurs_together_in_literature_with"
    ]


def test_a_binding_to_a_missing_edge_does_not_abort_the_overlay(lmdb):
    """C6-10: it raised KeyError after the node counts were written, so the
    message read as overlaid and the pair overlay was never applied."""
    message = omnicorp.omnicorp_overlay(_omnicorp_message(dangling=True), logger)

    assert _co_occurrence_edges(message)


def test_a_failed_overlay_leaves_the_message_retryable(lmdb):
    """N-2: a bad shared-counts value must not leave node counts behind -- they
    mark the message overlaid, and no retry would ever add the pairs."""
    lmdb["fail"] = True
    message = omnicorp.omnicorp_overlay(_omnicorp_message(), logger)

    assert not omnicorp._already_overlaid(message["message"]["knowledge_graph"])


def test_shared_count_reads_stop_at_the_time_budget(lmdb, monkeypatch, caplog):
    """C6-6: past the budget the counts read so far are applied (a partial
    overlay) and the cut is logged, rather than overrunning the pool timeout
    and applying nothing."""
    monkeypatch.setattr(omnicorp, "LMDB_BATCH_SIZE", 1)
    monkeypatch.setattr(omnicorp, "OMNICORP_TIME_BUDGET_SEC", 0.05, raising=False)
    lmdb["slow"] = 0.1

    with caplog.at_level(logging.WARNING):
        message = omnicorp.omnicorp_overlay(_omnicorp_message(), logger)

    assert len(lmdb["reads"]) == 1  # the first batch, then the budget ran out
    assert _co_occurrence_edges(message)  # what was read is applied
    assert "partial" in caplog.text

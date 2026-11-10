"""ARAGORN ranker and omnicorp hot paths (#220)."""

import asyncio
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


def test_a_binding_to_a_missing_edge_fails_loudly(lmdb):
    """C6-10: a binding to an edge the knowledge graph doesn't have is a
    malformed message. It used to be logged and the overlay quietly skipped
    (a catch-all swallowed the KeyError); now the overlay raises, so the query
    fails rather than being scored without that evidence."""
    with pytest.raises(ValueError, match="missing-edge"):
        omnicorp.omnicorp_overlay(_omnicorp_message(dangling=True), logger)


def test_a_failed_overlay_fails_and_leaves_the_message_retryable(lmdb):
    """N-2: a bad shared-counts value used to be swallowed -- the query went on
    as OK without literature support. It now fails, and leaves no node counts
    behind, since they mark the message overlaid and would block a retry."""
    lmdb["fail"] = True
    message = _omnicorp_message()

    with pytest.raises(ValueError):
        omnicorp.omnicorp_overlay(message, logger)

    assert not omnicorp._already_overlaid(message["message"]["knowledge_graph"])


# --- the slot is held no longer than the query can use the answer -------------
# (the bound itself is ProcessPoolManager.run_within_budget, tested in
# test_process_pool.py)


async def test_the_overlay_runs_within_its_querys_budget(monkeypatch):
    from shepherd_utils import shared

    calls = []

    class _Pool:
        async def run_within_budget(self, loop, task_fields, step, fn, *args):
            calls.append((task_fields, step, fn, args))

    async def _noop(*args, **kwargs):
        return None

    monkeypatch.setattr(shared, "wrap_up_task", _noop)
    monkeypatch.setattr(shared, "_guard_response_size", _noop)

    class _Limiter:
        def release(self):
            pass

    task = ("1-0", {"query_id": "q", "response_id": "r", "query_deadline": "1"})
    await omnicorp.process_task(
        task, None, logger, _Limiter(), asyncio.get_running_loop(), _Pool()
    )

    ((task_fields, step, fn, args),) = calls
    assert task_fields is task[1]
    assert "omnicorp" in step
    assert fn is omnicorp.aragorn_omnicorp
    assert args[0] == "r"


async def test_a_query_timeout_is_stored_as_timeout(redis_mock):
    from shepherd_utils import shared
    from shepherd_utils.query_errors import QueryTimedOut

    task = (
        "1-0",
        {
            "query_id": "q",
            "response_id": "r",
            "otel": "{}",
            "metadata": "{}",
        },
    )
    await shared.handle_task_failure(
        "aragorn.omnicorp", "consumer", task, logger, error=QueryTimedOut("late")
    )

    (entry,) = await redis_mock["broker"].xrange("finish_query")
    assert entry[1]["status"] == "TIMEOUT"
    assert entry[1]["description"] == "late"

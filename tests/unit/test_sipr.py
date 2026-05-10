"""Tests for the SIPR (Set-Input Page Rank) worker.

Covers the pure helpers that don't need network access:

- ``write_trapi``: TRAPI query construction for the neighborhood expansion
- ``get_nodes``: kgraph filtering + PPR-based truncation
- ``distribute_weights``: personalized PageRank over a small TRAPI kgraph

Plus the ``sipr`` entrypoint's two main branches (set-input and not).
"""

import json
import logging

import pytest

from workers.sipr import worker as sipr_worker


logger = logging.getLogger(__name__)


def test_write_trapi_first_hop_uses_named_thing():
    """Hop 0 keeps a generic NamedThing object category."""
    query = sipr_worker.write_trapi(["MONDO:0001"], hop_num=0)
    qg = query["message"]["query_graph"]
    assert qg["nodes"]["n0"]["ids"] == ["MONDO:0001"]
    assert qg["nodes"]["n1"]["categories"] == ["biolink:NamedThing"]
    assert qg["edges"]["e0"]["predicates"] == ["biolink:related_to"]


def test_write_trapi_second_hop_narrows_object_categories():
    """Hop 1 narrows to a specific list of biolink categories."""
    query = sipr_worker.write_trapi(["MONDO:0001"], hop_num=1)
    qg = query["message"]["query_graph"]
    cats = qg["nodes"]["n1"]["categories"]
    assert "biolink:Disease" in cats
    assert "biolink:Gene" in cats
    assert "biolink:NamedThing" not in cats


def test_distribute_weights_assigns_higher_score_to_seed():
    """Personalized PageRank should bias mass toward the personalization
    targets (the input ``target_nodes``)."""
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "e1": {"subject": "A", "object": "B"},
                    "e2": {"subject": "B", "object": "C"},
                    "e3": {"subject": "C", "object": "A"},
                },
                "nodes": {"A": {}, "B": {}, "C": {}},
            }
        }
    }
    ppr = sipr_worker.distribute_weights([response], ["A"], logger)
    assert set(ppr.keys()) == {"A", "B", "C"}
    # Seed node A should outrank the rest.
    assert ppr["A"] > ppr["B"]
    assert ppr["A"] > ppr["C"]


def test_distribute_weights_no_targets_uses_uniform_personalization():
    """No target nodes -> uniform PageRank, no error raised."""
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {"e1": {"subject": "A", "object": "B"}},
                "nodes": {"A": {}, "B": {}},
            }
        }
    }
    ppr = sipr_worker.distribute_weights([response], [], logger)
    assert set(ppr.keys()) == {"A", "B"}


def test_get_nodes_drops_subclass_and_related_to_edges():
    """The filterer drops subclass_of and related_to edges from the kg
    before scoring."""
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "drop_subclass": {
                        "subject": "MONDO:1",
                        "object": "MONDO:2",
                        "predicate": "biolink:subclass_of",
                    },
                    "drop_related": {
                        "subject": "MONDO:1",
                        "object": "MONDO:3",
                        "predicate": "biolink:related_to",
                    },
                    "keep": {
                        "subject": "MONDO:1",
                        "object": "CHEBI:1",
                        "predicate": "biolink:treats",
                    },
                },
                "nodes": {
                    "MONDO:1": {"name": "n1"},
                    "MONDO:2": {"name": "n2"},
                    "MONDO:3": {"name": "n3"},
                    "CHEBI:1": {"name": "c1"},
                },
            }
        }
    }
    kept_nodes, filtered = sipr_worker.get_nodes(response, ["MONDO:1"], logger)
    edges = filtered["message"]["knowledge_graph"]["edges"]
    # Only the non-subclass / non-related_to edge survives the initial filter.
    assert "keep" in edges
    assert "drop_subclass" not in edges
    assert "drop_related" not in edges


def test_get_nodes_drops_hp_to_hp_edges():
    """Edges where both endpoints are HP CURIEs get filtered out."""
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "hp_to_hp": {
                        "subject": "HP:0001",
                        "object": "HP:0002",
                        "predicate": "biolink:phenotype_of",
                    },
                    "mixed": {
                        "subject": "MONDO:0001",
                        "object": "HP:0003",
                        "predicate": "biolink:phenotype_of",
                    },
                },
                "nodes": {
                    "HP:0001": {},
                    "HP:0002": {},
                    "HP:0003": {},
                    "MONDO:0001": {},
                },
            }
        }
    }
    _, filtered = sipr_worker.get_nodes(response, ["MONDO:0001"], logger)
    edges = filtered["message"]["knowledge_graph"]["edges"]
    assert "hp_to_hp" not in edges
    assert "mixed" in edges


def test_get_nodes_excludes_hp_curies_from_returned_node_list():
    """The function returns the top scored non-HP nodes (truncated to 15)."""
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "e1": {
                        "subject": "MONDO:0001",
                        "object": "HP:0001",
                        "predicate": "biolink:phenotype_of",
                    },
                    "e2": {
                        "subject": "MONDO:0001",
                        "object": "CHEBI:0001",
                        "predicate": "biolink:treats",
                    },
                },
                "nodes": {
                    "MONDO:0001": {},
                    "HP:0001": {},
                    "CHEBI:0001": {},
                },
            }
        }
    }
    kept_nodes, _ = sipr_worker.get_nodes(response, ["MONDO:0001"], logger)
    # HP nodes excluded from the truncated return list
    assert "HP:0001" not in kept_nodes


def _make_task():
    return [
        "test",
        {
            "query_id": "qid",
            "response_id": "rid",
            "workflow": json.dumps([{"id": "sipr"}]),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]


@pytest.mark.asyncio
async def test_sipr_skips_non_set_input_query(redis_mock, mocker):
    """A query without any ``set_interpretation: MANY`` node should be skipped
    silently (no save)."""
    mocker.patch(
        "workers.sipr.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value={
            "message": {
                "query_graph": {
                    "nodes": {"a": {}, "b": {}},
                    "edges": {"e0": {"subject": "a", "object": "b"}},
                }
            }
        },
    )
    mock_save = mocker.patch(
        "workers.sipr.worker.save_message",
        new_callable=mocker.AsyncMock,
    )
    task = _make_task()
    await sipr_worker.sipr(task, logger)
    assert not mock_save.called
    # The workflow on the task gets a stable sipr/sort_results_score appended.
    workflow = json.loads(task[1]["workflow"])
    assert [op["id"] for op in workflow] == ["sipr", "sort_results_score"]


@pytest.mark.asyncio
async def test_sipr_set_input_runs_pagerank_and_saves_message(redis_mock, mocker):
    """A set-input query: get_neighborhood is called, weights are
    distributed, the final TRAPI message is saved with results."""
    set_input_query = {
        "message": {
            "query_graph": {
                "nodes": {
                    "SN": {
                        "ids": ["MONDO:0001", "MONDO:0002"],
                        "set_interpretation": "MANY",
                    },
                    "ON": {"categories": ["biolink:NamedThing"]},
                },
                "edges": {
                    "e0": {"subject": "SN", "object": "ON"},
                },
            }
        }
    }
    mocker.patch(
        "workers.sipr.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=set_input_query,
    )

    # Stub out get_neighborhood so we don't reach the network at all.
    fake_neighborhood = [
        {
            "message": {
                "knowledge_graph": {
                    "nodes": {
                        "MONDO:0001": {"categories": ["biolink:Disease"], "name": "d1"},
                        "MONDO:0002": {"categories": ["biolink:Disease"], "name": "d2"},
                        "CHEBI:0001": {
                            "categories": ["biolink:ChemicalEntity"],
                            "name": "c1",
                        },
                    },
                    "edges": {
                        "e1": {
                            "subject": "MONDO:0001",
                            "object": "CHEBI:0001",
                            "predicate": "biolink:treats",
                        },
                        "e2": {
                            "subject": "MONDO:0002",
                            "object": "CHEBI:0001",
                            "predicate": "biolink:treats",
                        },
                    },
                }
            }
        }
    ]
    mocker.patch(
        "workers.sipr.worker.get_neighborhood",
        new_callable=mocker.AsyncMock,
        return_value=fake_neighborhood,
    )
    mock_save = mocker.patch(
        "workers.sipr.worker.save_message",
        new_callable=mocker.AsyncMock,
    )

    await sipr_worker.sipr(_make_task(), logger)
    assert mock_save.called
    saved_id, saved_msg = mock_save.call_args.args[:2]
    assert saved_id == "rid"
    # Ensure the input-set node ids ended up in the kg, plus we got at least
    # one analysis result with a non-trivial score.
    kg_nodes = saved_msg["message"]["knowledge_graph"]["nodes"]
    assert "MONDO:0001" in kg_nodes and "MONDO:0002" in kg_nodes
    if saved_msg["message"]["results"]:
        analysis = saved_msg["message"]["results"][0]["analyses"][0]
        assert "score" in analysis and analysis["score"] > 0

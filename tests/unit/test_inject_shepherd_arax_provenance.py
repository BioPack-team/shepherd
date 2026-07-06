"""Tests for ``workers.arax.inject_shepherd_arax_provenance``.

The shepherd-arax injector tags every kgraph edge with an aggregator
``infores:shepherd-arax`` source so downstream consumers can attribute
provenance back through the Shepherd layer.

Note: ``workers/arax/worker.py`` uses a bare relative import
(``from inject_shepherd_arax_provenance import ...``) which is not valid
under the project's package layout. We import the helper module directly
through its package path here.
"""

import copy

from workers.arax.inject_shepherd_arax_provenance import (
    SHEPHERD_ARAX_SOURCE,
    add_shepherd_arax_to_edge_sources,
)


def test_adds_source_when_missing_entirely():
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "e1": {"subject": "A", "object": "B"},
                },
                "nodes": {"A": {}, "B": {}},
            }
        }
    }
    out = add_shepherd_arax_to_edge_sources(response)
    sources = out["message"]["knowledge_graph"]["edges"]["e1"]["sources"]
    assert len(sources) == 1
    assert sources[0]["resource_id"] == "infores:shepherd-arax"


def test_appends_source_when_other_sources_already_present():
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "e1": {
                        "subject": "A",
                        "object": "B",
                        "sources": [
                            {
                                "resource_id": "infores:other",
                                "resource_role": "primary_knowledge_source",
                            }
                        ],
                    }
                },
                "nodes": {"A": {}, "B": {}},
            }
        }
    }
    out = add_shepherd_arax_to_edge_sources(response)
    sources = out["message"]["knowledge_graph"]["edges"]["e1"]["sources"]
    ids = [s["resource_id"] for s in sources]
    assert "infores:other" in ids
    assert "infores:shepherd-arax" in ids


def test_idempotent_when_source_already_present():
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "e1": {
                        "subject": "A",
                        "object": "B",
                        "sources": [dict(SHEPHERD_ARAX_SOURCE)],
                    }
                },
                "nodes": {"A": {}, "B": {}},
            }
        }
    }
    snapshot = copy.deepcopy(response)
    out = add_shepherd_arax_to_edge_sources(response)
    # No duplicate source appended.
    assert len(out["message"]["knowledge_graph"]["edges"]["e1"]["sources"]) == 1
    # Equivalent to the original.
    assert out == snapshot


def test_adds_source_to_every_edge():
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "e1": {"subject": "A", "object": "B"},
                    "e2": {"subject": "B", "object": "C"},
                    "e3": {"subject": "C", "object": "D"},
                },
                "nodes": {"A": {}, "B": {}, "C": {}, "D": {}},
            }
        }
    }
    out = add_shepherd_arax_to_edge_sources(response)
    edges = out["message"]["knowledge_graph"]["edges"]
    for eid, edge in edges.items():
        assert any(
            s.get("resource_id") == "infores:shepherd-arax" for s in edge["sources"]
        ), f"missing shepherd-arax source on {eid}"


def test_returns_input_unchanged_when_message_missing():
    response = {"not_a_message": True}
    out = add_shepherd_arax_to_edge_sources(response)
    assert out is response


def test_returns_input_unchanged_when_kg_not_a_dict():
    response = {"message": {"knowledge_graph": "not-a-dict"}}
    out = add_shepherd_arax_to_edge_sources(response)
    assert out is response


def test_returns_input_unchanged_when_edges_not_a_dict():
    response = {"message": {"knowledge_graph": {"edges": []}}}
    out = add_shepherd_arax_to_edge_sources(response)
    # The function returns early; edges stays a list.
    assert out["message"]["knowledge_graph"]["edges"] == []


def test_skips_non_dict_edge_entries():
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "valid": {"subject": "A", "object": "B"},
                    "garbage": "not-a-dict",
                },
                "nodes": {"A": {}, "B": {}},
            }
        }
    }
    out = add_shepherd_arax_to_edge_sources(response)
    edges = out["message"]["knowledge_graph"]["edges"]
    # Valid edge gets the source.
    assert edges["valid"]["sources"][0]["resource_id"] == "infores:shepherd-arax"
    # Garbage entry untouched.
    assert edges["garbage"] == "not-a-dict"


def test_skips_edge_with_non_list_sources():
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "e1": {
                        "subject": "A",
                        "object": "B",
                        "sources": "not-a-list",
                    }
                },
                "nodes": {"A": {}, "B": {}},
            }
        }
    }
    out = add_shepherd_arax_to_edge_sources(response)
    # ``sources`` was not a list; injector skips it without crashing.
    assert out["message"]["knowledge_graph"]["edges"]["e1"]["sources"] == "not-a-list"


def test_each_inserted_source_is_an_independent_copy():
    """Edges should not share a mutable source dict instance."""
    response = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    "e1": {"subject": "A", "object": "B"},
                    "e2": {"subject": "C", "object": "D"},
                },
                "nodes": {"A": {}, "B": {}, "C": {}, "D": {}},
            }
        }
    }
    out = add_shepherd_arax_to_edge_sources(response)
    s1 = out["message"]["knowledge_graph"]["edges"]["e1"]["sources"][0]
    s2 = out["message"]["knowledge_graph"]["edges"]["e2"]["sources"][0]
    # Same content, different identity.
    assert s1 == s2
    assert s1 is not s2

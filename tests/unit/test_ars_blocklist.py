"""Tests for the ARS blocklist worker (Relay ``remove_blocked`` parity)."""

import json
import logging

from workers.ars_blocklist.worker import apply_blocklist, load_blocklist

logger = logging.getLogger(__name__)


def _message():
    return {
        "message": {
            "knowledge_graph": {
                "nodes": {"A:1": {"name": "a"}, "B:2": {"name": "bad"}, "C:3": {}},
                "edges": {
                    "e_ok": {"subject": "A:1", "object": "C:3"},
                    "e_bad": {"subject": "B:2", "object": "C:3"},
                },
            },
            "auxiliary_graphs": {
                "aux_bad": {"edges": ["e_bad"], "attributes": []},
                "aux_mixed": {"edges": ["e_ok", "e_bad"], "attributes": []},
            },
            "results": [
                {
                    "node_bindings": {"n0": [{"id": "A:1"}], "n1": [{"id": "C:3"}]},
                    "analyses": [{"edge_bindings": {"e": [{"id": "e_ok"}]}}],
                },
                {
                    "node_bindings": {"n0": [{"id": "B:2"}], "n1": [{"id": "C:3"}]},
                    "analyses": [{"edge_bindings": {"e": [{"id": "e_bad"}]}}],
                },
            ],
        }
    }


# --- load_blocklist -------------------------------------------------------


def test_load_blocklist_curie_keyed_object(tmp_path):
    """The real Relay format: an object keyed by CURIE (with name/type values)."""
    path = tmp_path / "bl.json"
    path.write_text(
        json.dumps(
            {
                "B:2": {"name": "bad", "type": ["biolink:NamedThing"]},
                "X:9": {"name": "worse", "type": ["biolink:Gene"]},
            }
        )
    )
    assert load_blocklist(str(path), logger) == {"B:2", "X:9"}


def test_load_blocklist_list_form(tmp_path):
    path = tmp_path / "bl.json"
    path.write_text(json.dumps(["B:2", "X:9"]))
    assert load_blocklist(str(path), logger) == {"B:2", "X:9"}


def test_load_blocklist_missing_file():
    assert load_blocklist("/no/such/file.json", logger) == set()


# --- apply_blocklist ------------------------------------------------------


def test_apply_blocklist_removes_node_and_dependents():
    msg = _message()
    counts = apply_blocklist(msg, {"B:2"}, logger)
    kg = msg["message"]["knowledge_graph"]
    # The blocked node and the edge touching it are gone; clean ones remain.
    assert "B:2" not in kg["nodes"]
    assert "e_bad" not in kg["edges"]
    assert "e_ok" in kg["edges"]
    # The result binding the blocked node is dropped; the clean result remains.
    result_bindings = [
        r["node_bindings"]["n0"][0]["id"] for r in msg["message"]["results"]
    ]
    assert result_bindings == ["A:1"]
    assert counts["nodes"] == 1
    assert counts["edges"] == 1
    assert counts["results"] == 1


def test_apply_blocklist_prunes_aux_graphs():
    msg = _message()
    apply_blocklist(msg, {"B:2"}, logger)
    aux = msg["message"]["auxiliary_graphs"]
    # aux_bad had only the removed edge -> whole graph gone.
    assert "aux_bad" not in aux
    # aux_mixed keeps its surviving edge.
    assert aux["aux_mixed"]["edges"] == ["e_ok"]


def test_apply_blocklist_noop_when_nothing_matches():
    msg = _message()
    counts = apply_blocklist(msg, {"Z:99"}, logger)
    assert counts["nodes"] == 0
    assert len(msg["message"]["knowledge_graph"]["nodes"]) == 3


def test_apply_blocklist_drops_edge_when_all_support_graphs_removed():
    msg = {
        "message": {
            "knowledge_graph": {
                "nodes": {"BAD:1": {}, "S:1": {}, "O:1": {}},
                "edges": {
                    "e_sup": {"subject": "BAD:1", "object": "S:1"},
                    "e_inferred": {
                        "subject": "S:1",
                        "object": "O:1",
                        "attributes": [
                            {
                                "attribute_type_id": "biolink:support_graphs",
                                "value": ["aux1"],
                            }
                        ],
                    },
                },
            },
            "auxiliary_graphs": {"aux1": {"edges": ["e_sup"], "attributes": []}},
            "results": [],
        }
    }
    counts = apply_blocklist(msg, {"BAD:1"}, logger)
    edges = msg["message"]["knowledge_graph"]["edges"]
    # e_sup removed (touches BAD:1); aux1 emptied -> removed; e_inferred loses its
    # only support graph -> also removed.
    assert "e_sup" not in edges
    assert "e_inferred" not in edges
    assert "aux1" not in msg["message"]["auxiliary_graphs"]
    assert counts["auxiliary_graphs"] == 1

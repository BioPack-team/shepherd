"""Tests for the ARS blocklist worker."""

import json
import logging

from workers.ars_blocklist.worker import apply_blocklist, load_blocklist

logger = logging.getLogger(__name__)


def _message():
    return {
        "message": {
            "knowledge_graph": {
                "nodes": {"A:1": {}, "B:2": {}, "C:3": {}},
                "edges": {
                    "e_ok": {
                        "subject": "A:1",
                        "object": "C:3",
                        "sources": [{"resource_id": "infores:good"}],
                    },
                    "e_badsrc": {
                        "subject": "A:1",
                        "object": "C:3",
                        "sources": [{"resource_id": "infores:bad"}],
                    },
                    "e_badnode": {
                        "subject": "B:2",
                        "object": "C:3",
                        "sources": [{"resource_id": "infores:good"}],
                    },
                },
            }
        }
    }


def test_apply_blocklist_removes_blocked_source_edges():
    msg = _message()
    removed = apply_blocklist(msg, {"infores:bad"}, set(), logger)
    edges = msg["message"]["knowledge_graph"]["edges"]
    assert removed == 1
    assert "e_badsrc" not in edges
    assert "e_ok" in edges and "e_badnode" in edges


def test_apply_blocklist_removes_blocked_nodes_and_their_edges():
    msg = _message()
    removed = apply_blocklist(msg, set(), {"B:2"}, logger)
    kg = msg["message"]["knowledge_graph"]
    # node B:2 + the edge touching it.
    assert removed == 2
    assert "B:2" not in kg["nodes"]
    assert "e_badnode" not in kg["edges"]


def test_apply_blocklist_noop_when_nothing_blocked():
    msg = _message()
    removed = apply_blocklist(msg, set(), set(), logger)
    assert removed == 0
    assert len(msg["message"]["knowledge_graph"]["edges"]) == 3


# --- load_blocklist -------------------------------------------------------


def test_load_blocklist_list_form(tmp_path):
    path = tmp_path / "bl.json"
    path.write_text(json.dumps(["A:1", "B:2"]))
    sources, nodes = load_blocklist(str(path), logger)
    assert sources == set()
    assert nodes == {"A:1", "B:2"}


def test_load_blocklist_dict_form(tmp_path):
    path = tmp_path / "bl.json"
    path.write_text(
        json.dumps({"knowledge_sources": ["infores:bad"], "nodes": ["X:1"]})
    )
    sources, nodes = load_blocklist(str(path), logger)
    assert sources == {"infores:bad"}
    assert nodes == {"X:1"}


def test_load_blocklist_missing_file():
    assert load_blocklist("/no/such/file.json", logger) == (set(), set())

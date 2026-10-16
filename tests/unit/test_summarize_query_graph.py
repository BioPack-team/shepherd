"""Tests for how the /callback endpoint logs a lookup's query graph."""

from shepherd_server.base_routes import MAX_LOGGED_NODE_IDS, summarize_query_graph


def _query_graph(n0_ids, n1_ids):
    return {
        "nodes": {
            "n0": {"ids": n0_ids, "categories": ["biolink:Gene"]},
            "n1": {"ids": n1_ids},
            "n2": {"categories": ["biolink:Disease"]},
        },
        "edges": {"e0": {"subject": "n0", "object": "n1"}},
    }


def test_a_long_id_list_logs_as_its_count():
    many = [f"X:{i}" for i in range(5000)]
    few = ["Y:1", "Y:2"]
    query_graph = _query_graph(many, few)

    summary = summarize_query_graph(query_graph)

    assert summary == {
        "nodes": {
            "n0": {"ids": "5000 curies", "categories": ["biolink:Gene"]},
            "n1": {"ids": ["Y:1", "Y:2"]},
            "n2": {"categories": ["biolink:Disease"]},
        },
        "edges": {"e0": {"subject": "n0", "object": "n1"}},
    }
    # the callback goes on to merge the graph it was sent, unchanged
    assert query_graph["nodes"]["n0"]["ids"] is many


def test_ids_up_to_the_threshold_are_logged():
    ids = [f"X:{i}" for i in range(MAX_LOGGED_NODE_IDS)]

    summary = summarize_query_graph(_query_graph(ids, ids + ["X:extra"]))

    assert summary["nodes"]["n0"]["ids"] == ids
    assert summary["nodes"]["n1"]["ids"] == f"{MAX_LOGGED_NODE_IDS + 1} curies"


def test_anything_but_a_query_graph_is_logged_as_is():
    assert summarize_query_graph(None) is None
    assert summarize_query_graph({"edges": {}}) == {"edges": {}}
    assert summarize_query_graph({"nodes": {"n0": None}}) == {"nodes": {"n0": None}}

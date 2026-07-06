"""Tests for ``workers.sipr.graph_filterer``.

Covers ``remove_isolated``, ``add_hub_node``, ``filter_graph_by_weight``,
and ``apply_to_graph``. See PR notes: ``apply_list_to_graph`` references
an undefined ``G`` and is currently broken; we don't test it.
"""

import networkx as nx

from workers.sipr import graph_filterer as gf


def test_remove_isolated_strips_disconnected_nodes():
    g = nx.Graph()
    g.add_edge("A", "B")
    g.add_node("LONELY")
    out = gf.remove_isolated(g)
    assert "LONELY" not in out.nodes
    assert {"A", "B"} == set(out.nodes)


def test_remove_isolated_returns_copy_does_not_mutate_input():
    g = nx.Graph()
    g.add_edge("A", "B")
    g.add_node("LONELY")
    _ = gf.remove_isolated(g)
    assert "LONELY" in g.nodes  # original untouched


def test_add_hub_node_connects_to_every_other_node():
    g = nx.Graph()
    g.add_edges_from([("A", "B"), ("B", "C")])
    out = gf.add_hub_node(g, hub_name="HUB")
    assert "HUB" in out.nodes
    for n in {"A", "B", "C"}:
        assert out.has_edge("HUB", n)
    # Original graph is untouched.
    assert "HUB" not in g.nodes


def test_add_hub_node_default_name():
    g = nx.Graph()
    g.add_edge("A", "B")
    out = gf.add_hub_node(g)
    assert "Hub" in out.nodes


def test_filter_graph_by_weight_keep_above_drops_low_edges():
    g = nx.Graph()
    g.add_edge("A", "B", weight=0.9)
    g.add_edge("B", "C", weight=0.1)
    g.add_edge("C", "D", weight=0.5)
    out = gf.filter_graph_by_weight(g, weight_cutoff=0.5, keep_above=True)
    assert out.has_edge("A", "B")
    assert out.has_edge("C", "D")
    assert not out.has_edge("B", "C")


def test_filter_graph_by_weight_keep_below_keeps_low_edges():
    g = nx.Graph()
    g.add_edge("A", "B", weight=0.9)
    g.add_edge("B", "C", weight=0.1)
    out = gf.filter_graph_by_weight(g, weight_cutoff=0.5, keep_above=False)
    assert not out.has_edge("A", "B")
    assert out.has_edge("B", "C")


def test_filter_graph_by_weight_drops_orphans_after_filter():
    """After dropping the only edge a node has, the node should be removed
    too (post-filter remove_isolated)."""
    g = nx.Graph()
    g.add_edge("A", "B", weight=0.1)
    out = gf.filter_graph_by_weight(g, weight_cutoff=0.5, keep_above=True)
    assert "A" not in out.nodes and "B" not in out.nodes


def test_filter_graph_by_weight_preserves_node_attributes():
    g = nx.Graph()
    g.add_node("A", category="biolink:Disease")
    g.add_node("B", category="biolink:Drug")
    g.add_edge("A", "B", weight=0.9)
    out = gf.filter_graph_by_weight(g, weight_cutoff=0.1, keep_above=True)
    assert out.nodes["A"]["category"] == "biolink:Disease"
    assert out.nodes["B"]["category"] == "biolink:Drug"


def test_filter_graph_by_weight_default_weight_when_missing():
    """Edges without a 'weight' attribute default to 0; filtering with
    keep_above=True and a positive cutoff should drop them."""
    g = nx.Graph()
    g.add_edge("A", "B")  # no weight attribute
    out = gf.filter_graph_by_weight(g, weight_cutoff=0.1, keep_above=True)
    assert not out.has_edge("A", "B")


def test_apply_to_graph_runs_function_on_copy():
    g = nx.Graph()
    g.add_edge("A", "B", weight=0.9)
    g.add_edge("B", "C", weight=0.1)
    out = gf.apply_to_graph(
        g, lambda x: gf.filter_graph_by_weight(x, weight_cutoff=0.5, keep_above=True)
    )
    assert out.has_edge("A", "B")
    assert not out.has_edge("B", "C")
    # Original unaffected.
    assert g.has_edge("B", "C")

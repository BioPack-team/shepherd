"""Response-cache key canonicalization.

Pure functions from shepherd_utils.ars.cache: value canonicalization
(key order, null/missing/empty, set-like lists), structural canonicalization
(node/edge/path ids are labels), key material selection and request-mode
resolution.
"""

import copy

import pytest

from shepherd_utils.ars import cache

QG = {
    "nodes": {
        "sn": {"ids": ["MONDO:0005148"], "categories": ["biolink:Disease"]},
        "on": {"categories": ["biolink:ChemicalEntity"]},
    },
    "edges": {
        "t_edge": {
            "subject": "on",
            "object": "sn",
            "predicates": ["biolink:treats"],
            "knowledge_type": "inferred",
        }
    },
}


def body(qg, **extra):
    return {"message": {"query_graph": qg}, **extra}


def key(qg, **extra):
    return cache.cache_key(body(qg, **extra))[0]


# ---------------------------------------------------------------------------
# value canonicalization
# ---------------------------------------------------------------------------


def test_canonicalize_drops_null_and_empty_members():
    assert cache.canonicalize({"a": None, "b": {}, "c": [], "d": 1}) == {"d": 1}


def test_canonicalize_drops_members_that_become_empty():
    assert cache.canonicalize({"a": {"b": None, "c": []}, "d": [None, {}]}) is None


def test_canonicalize_keeps_empty_strings_and_falsy_scalars():
    assert cache.canonicalize({"a": "", "b": 0, "c": False}) == {
        "a": "",
        "b": 0,
        "c": False,
    }


def test_canonicalize_sorts_lists_as_sets():
    assert cache.canonicalize({"ids": ["B", "A"]}) == {"ids": ["A", "B"]}
    assert cache.canonicalize([{"y": 2, "x": 1}, {"a": 0}]) == [
        {"a": 0},
        {"x": 1, "y": 2},
    ]


def test_key_order_independent():
    reordered = {
        "edges": QG["edges"],
        "nodes": {
            "on": {"categories": ["biolink:ChemicalEntity"]},
            "sn": {"categories": ["biolink:Disease"], "ids": ["MONDO:0005148"]},
        },
    }
    assert key(QG) == key(reordered)


def test_null_missing_empty_equivalent():
    variants = [
        copy.deepcopy(QG),
        copy.deepcopy(QG),
        copy.deepcopy(QG),
        copy.deepcopy(QG),
    ]
    variants[1]["nodes"]["on"]["ids"] = None
    variants[2]["nodes"]["on"]["ids"] = []
    variants[3]["nodes"]["on"]["constraints"] = []
    variants[3]["edges"]["t_edge"]["attribute_constraints"] = None
    variants[3]["paths"] = {}
    keys = {key(v) for v in variants}
    assert len(keys) == 1


def test_list_order_independent():
    a = copy.deepcopy(QG)
    a["nodes"]["sn"]["ids"] = ["MONDO:1", "MONDO:2"]
    b = copy.deepcopy(QG)
    b["nodes"]["sn"]["ids"] = ["MONDO:2", "MONDO:1"]
    assert key(a) == key(b)


def test_different_curie_differs():
    b = copy.deepcopy(QG)
    b["nodes"]["sn"]["ids"] = ["MONDO:0000001"]
    assert key(QG) != key(b)


def test_added_constraint_differs():
    b = copy.deepcopy(QG)
    b["edges"]["t_edge"]["attribute_constraints"] = [{"id": "x", "value": 1}]
    assert key(QG) != key(b)


# ---------------------------------------------------------------------------
# structural canonicalization: labels are not content
# ---------------------------------------------------------------------------


def relabel(qg, node_map, edge_map=None, path_map=None):
    out = {"nodes": {node_map[k]: copy.deepcopy(v) for k, v in qg["nodes"].items()}}
    for section, m in (("edges", edge_map or {}), ("paths", path_map or {})):
        if section not in qg:
            continue
        out[section] = {}
        for k, v in qg[section].items():
            v = copy.deepcopy(v)
            v["subject"] = node_map[v["subject"]]
            v["object"] = node_map[v["object"]]
            out[section][m.get(k, k)] = v
    return out


def test_renamed_labels_same_key():
    renamed = relabel(QG, {"sn": "n0", "on": "n1"}, {"t_edge": "e01"})
    again = relabel(QG, {"sn": "b", "on": "a"}, {"t_edge": "x"})
    assert key(QG) == key(renamed) == key(again)


def test_swapped_endpoints_differ():
    b = copy.deepcopy(QG)
    b["edges"]["t_edge"]["subject"], b["edges"]["t_edge"]["object"] = (
        b["edges"]["t_edge"]["object"],
        b["edges"]["t_edge"]["subject"],
    )
    assert key(QG) != key(b)


def test_canonical_graph_shape_and_label_map():
    graph, label_map = cache.canonical_graph(QG)
    assert set(graph["nodes"]) == {"n0", "n1"}
    assert set(graph["edges"]) == {"e0"}
    edge = graph["edges"]["e0"]
    assert {edge["subject"], edge["object"]} == {"n0", "n1"}
    assert set(label_map["nodes"]) == {"sn", "on"}
    assert label_map["edges"] == {"t_edge": "e0"}
    assert label_map["paths"] == {}
    # the map is consistent with the canonical graph
    assert graph["nodes"][label_map["nodes"]["sn"]]["ids"] == ["MONDO:0005148"]
    assert edge["subject"] == label_map["nodes"]["on"]


def test_two_hop_with_blank_middle_nodes_is_label_invariant():
    chain = {
        "nodes": {"a": {"ids": ["X:1"]}, "b": {}, "c": {}, "d": {"ids": ["Y:2"]}},
        "edges": {
            "e1": {"subject": "a", "object": "b"},
            "e2": {"subject": "b", "object": "c"},
            "e3": {"subject": "c", "object": "d"},
        },
    }
    renamed = relabel(
        chain, {"a": "q", "b": "z", "c": "y", "d": "p"}, {"e1": "k", "e2": "j", "e3": "i"}
    )
    assert key(chain) == key(renamed)
    # the blank nodes are distinguishable by position: b touches the pinned
    # X:1 node, c touches Y:2 -- so the maps agree on structure
    _, m1 = cache.canonical_graph(chain)
    _, m2 = cache.canonical_graph(renamed)
    assert m1["nodes"]["b"] == m2["nodes"]["z"]
    assert m1["nodes"]["c"] == m2["nodes"]["y"]


def test_symmetric_nodes_same_key_and_valid_map():
    """Two identical blank neighbors of one pinned node are interchangeable:
    any labeling gives the same key, and the map still describes a valid
    rename (each source label maps to a distinct canonical label)."""
    star = {
        "nodes": {"hub": {"ids": ["X:1"]}, "l1": {}, "l2": {}},
        "edges": {
            "e1": {"subject": "hub", "object": "l1"},
            "e2": {"subject": "hub", "object": "l2"},
        },
    }
    renamed = relabel(star, {"hub": "h", "l1": "b", "l2": "a"}, {"e1": "y", "e2": "x"})
    assert key(star) == key(renamed)
    _, m = cache.canonical_graph(star)
    assert len(set(m["nodes"].values())) == 3
    assert len(set(m["edges"].values())) == 2


def test_pathfinder_paths_relabel_like_edges():
    pf = {
        "nodes": {"n0": {"ids": ["CHEBI:45783"]}, "n1": {"ids": ["MONDO:0004979"]}},
        "paths": {"p0": {"subject": "n0", "object": "n1", "predicates": ["biolink:related_to"]}},
    }
    renamed = relabel(pf, {"n0": "start", "n1": "end"}, path_map={"p0": "route"})
    assert key(pf) == key(renamed)
    graph, m = cache.canonical_graph(renamed)
    assert set(graph["paths"]) == {"p0"}
    assert m["paths"] == {"route": "p0"}
    assert "edges" not in graph


def test_edge_content_differs_even_with_same_labels():
    b = copy.deepcopy(QG)
    b["edges"]["t_edge"]["predicates"] = ["biolink:affects"]
    assert key(QG) != key(b)


def test_malformed_graph_falls_back_to_value_canonicalization():
    graph, m = cache.canonical_graph({"nodes": "not-a-dict"})
    assert graph == {"nodes": "not-a-dict"}
    assert m == {"nodes": {}, "edges": {}, "paths": {}}
    graph, m = cache.canonical_graph(None)
    assert graph is None
    # no query graph at all still yields a key
    assert len(cache.cache_key({"message": {}})[0]) == 64


def test_tie_permutation_cap_falls_back_deterministically(monkeypatch):
    monkeypatch.setattr(cache, "MAX_TIE_PERMUTATIONS", 1)
    star = {
        "nodes": {"hub": {"ids": ["X:1"]}, "l1": {}, "l2": {}},
        "edges": {
            "e1": {"subject": "hub", "object": "l1"},
            "e2": {"subject": "hub", "object": "l2"},
        },
    }
    k1 = key(star)
    k2 = key(copy.deepcopy(star))
    assert k1 == k2


# ---------------------------------------------------------------------------
# key material
# ---------------------------------------------------------------------------


def test_workflow_included_when_non_empty():
    assert key(QG) != key(QG, workflow=[{"id": "lookup"}])
    assert key(QG) == key(QG, workflow=[])
    assert key(QG, workflow=[{"id": "lookup"}]) == key(QG, workflow=[{"id": "lookup"}])


def test_irrelevant_body_fields_excluded():
    assert key(QG) == key(
        QG,
        submitter="me",
        callback="http://x",
        log_level="DEBUG",
        name="q",
        bypass_cache=True,
        parameters={"overwrite_cache": True, "timeout": 30},
        validate=False,
    )


def test_version_bump_changes_every_key(monkeypatch):
    before = key(QG)
    monkeypatch.setattr(cache, "CACHE_KEY_VERSION", "test-bump")
    assert key(QG) != before


def test_resolve_mode():
    assert cache.resolve_mode(body(QG)) == cache.MODE_NORMAL
    assert cache.resolve_mode(body(QG, bypass_cache=True)) == cache.MODE_BYPASS
    assert cache.resolve_mode(body(QG, bypass_cache="true")) == cache.MODE_NORMAL
    assert (
        cache.resolve_mode(body(QG, parameters={"overwrite_cache": True}))
        == cache.MODE_OVERWRITE
    )
    # bypass wins when both are set
    assert (
        cache.resolve_mode(
            body(QG, bypass_cache=True, parameters={"overwrite_cache": True})
        )
        == cache.MODE_BYPASS
    )
    assert cache.resolve_mode(None) == cache.MODE_NORMAL


# ---------------------------------------------------------------------------
# log line helper
# ---------------------------------------------------------------------------


def test_append_log():
    payload = {"logs": [{"message": "a"}]}
    cache.append_log(payload, "hello")
    assert len(payload["logs"]) == 2
    assert payload["logs"][-1]["message"] == "hello"
    assert payload["logs"][-1]["level"] == "INFO"
    payload = {"logs": "garbage"}
    cache.append_log(payload, "x")
    assert [e["message"] for e in payload["logs"]] == ["x"]
    payload = {}
    cache.append_log(payload, "x")
    assert payload["logs"][0]["message"] == "x"
    assert cache.append_log("nope", "x") == "nope"


@pytest.mark.parametrize("bad", [None, 5, "x", [1, 2]])
def test_cache_key_never_raises_on_bad_bodies(bad):
    digest, label_map = cache.cache_key(bad)
    assert len(digest) == 64
    assert label_map == {"nodes": {}, "edges": {}, "paths": {}}

"""Regression tests for the upstream ARS bugs the port deliberately fixes.

Each test names the upstream behavior it replaces. The golden suite pins the
corpus outputs; this file pins the *rules*, on inputs the corpus does not
happen to contain, so a future "restore parity" edit fails loudly here.

See docs/ARS_PARITY_REGISTER.md, "Deliberate divergences from upstream".
"""

import copy

import pytest

from shepherd_utils.ars import merge as ars_merge
from shepherd_utils.ars import premerge as ars_premerge
from shepherd_utils.ars import statuses
from shepherd_utils.ars.blocklist import remove_blocked

# ---------------------------------------------------------------------------
# premerge.decorate_edges_with_infores
# ---------------------------------------------------------------------------


def _edges(sources):
    return {"message": {"knowledge_graph": {"edges": {"e1": {"sources": sources}}}}}


def test_sources_without_a_primary_do_not_crash():
    """Upstream read has_primary, which it only ever assigned inside the loop
    body -- a non-empty sources list with no primary raised UnboundLocalError
    and failed the whole callback."""
    data = _edges(
        [{"resource_id": "infores:x", "resource_role": "aggregator_knowledge_source"}]
    )
    ars_premerge.decorate_edges_with_infores(data, "infores:aragorn")
    sources = data["message"]["knowledge_graph"]["edges"]["e1"]["sources"]
    assert len(sources) == 2
    assert sources[1] == {
        "resource_id": "infores:aragorn",
        "resource_role": "primary_knowledge_source",
        "source_record_urls": None,
        "upstream_resource_ids": [],
    }


def test_existing_primary_makes_self_an_aggregator():
    data = _edges(
        [{"resource_id": "infores:x", "resource_role": "primary_knowledge_source"}]
    )
    ars_premerge.decorate_edges_with_infores(data, "infores:aragorn")
    sources = data["message"]["knowledge_graph"]["edges"]["e1"]["sources"]
    assert sources[1]["resource_role"] == "aggregator_knowledge_source"


def test_each_edge_gets_its_own_source_dict():
    """Upstream built ONE self_source for the whole graph and mutated its
    role per edge, so the last edge to need a role silently rewrote the role
    every earlier edge had already been given."""
    data = {
        "message": {
            "knowledge_graph": {
                "edges": {
                    # needs an aggregator role (a primary is already present)
                    "e1": [
                        {
                            "resource_id": "infores:x",
                            "resource_role": "primary_knowledge_source",
                        }
                    ],
                    # needs a primary role (none present)
                    "e2": [
                        {
                            "resource_id": "infores:y",
                            "resource_role": "aggregator_knowledge_source",
                        }
                    ],
                }
            }
        }
    }
    data["message"]["knowledge_graph"]["edges"] = {
        k: {"sources": v}
        for k, v in data["message"]["knowledge_graph"]["edges"].items()
    }
    ars_premerge.decorate_edges_with_infores(data, "infores:aragorn")
    edges = data["message"]["knowledge_graph"]["edges"]
    assert edges["e1"]["sources"][1]["resource_role"] == "aggregator_knowledge_source"
    assert edges["e2"]["sources"][1]["resource_role"] == "primary_knowledge_source"


def test_edge_without_sources_key_is_not_a_typeerror():
    """scrub_null_attributes iterated get_safe(edge, "sources") straight,
    which is None for an edge that has no sources key."""
    data = {"message": {"knowledge_graph": {"nodes": {}, "edges": {"e1": {}}}}}
    ars_premerge.scrub_null_attributes(data)  # no raise


# ---------------------------------------------------------------------------
# premerge.normalizeScores
# ---------------------------------------------------------------------------


def test_mixed_scored_and_unscored_results_do_not_raise():
    """Upstream ranked only the score-bearing results but popped one rank per
    RESULT, so a mixed response ran the list dry (IndexError, failing the
    callback) after handing earlier results the wrong scores."""
    results = [
        {"analyses": [{}]},  # no score
        {"analyses": [{"score": 0.9}]},
        {"analyses": [{}]},  # no score
        {"analyses": [{"score": 0.1}]},
    ]
    out = ars_premerge.normalizeScores(copy.deepcopy(results))
    assert "normalized_score" not in out[0]
    assert "normalized_score" not in out[2]
    # ranks go to the results they were computed from: 0.9 above 0.1
    assert out[1]["normalized_score"] == 100.0
    assert out[3]["normalized_score"] == 50.0


def test_all_scored_results_are_unaffected():
    results = [{"analyses": [{"score": s}]} for s in (0.2, 0.8)]
    out = ars_premerge.normalizeScores(copy.deepcopy(results))
    assert [r["normalized_score"] for r in out] == [50.0, 100.0]


# ---------------------------------------------------------------------------
# merge.mergeDicts
# ---------------------------------------------------------------------------


def test_keys_after_attributes_are_still_merged():
    """Upstream returned out of the attributes branch, abandoning every key
    it had not reached yet."""
    current = {"attributes": [], "qualifiers": [{"qualifier_type_id": "q"}], "zzz": 1}
    merged = {"attributes": [], "qualifiers": [], "zzz": 2}
    out = ars_merge.mergeDicts(copy.deepcopy(current), copy.deepcopy(merged))
    assert out["qualifiers"] == [{"qualifier_type_id": "q"}]
    # reached at all: the scalar rule turns a real conflict into a list
    assert out["zzz"] == [2, 1]


def test_keys_after_analyses_are_still_merged():
    current = {"analyses": [{"a": 1}], "zzz": 1}
    merged = {"analyses": [{"b": 2}], "zzz": 2}
    out = ars_merge.mergeDicts(copy.deepcopy(current), copy.deepcopy(merged))
    assert out["analyses"] == [{"b": 2}, {"a": 1}]
    assert out["zzz"] == [2, 1]


def test_node_bindings_union_every_current_only_binding():
    """Upstream's else hung off the for, so only the LAST current-only id was
    carried -- and into a local map it never wrote back."""
    current = {
        "node_bindings": {"n0": [{"id": "A"}], "n1": [{"id": "B"}], "n2": [{"id": "D"}]}
    }
    merged = {
        "node_bindings": {"n0": [{"id": "A"}], "n1": [{"id": "C"}], "n2": [{"id": "E"}]}
    }
    out = ars_merge.mergeDicts(copy.deepcopy(current), copy.deepcopy(merged))
    nb = out["node_bindings"]
    assert nb["n0"] == [{"id": "A"}]
    assert {b["id"] for b in nb["n1"]} == {"B", "C"}
    assert {b["id"] for b in nb["n2"]} == {"D", "E"}


def test_node_bindings_past_the_first_are_not_ignored():
    """Upstream keyed off node_value[0] only, so a node's second binding
    onward never took part in the merge."""
    current = {"node_bindings": {"n0": [{"id": "A"}, {"id": "B"}]}}
    merged = {"node_bindings": {"n0": [{"id": "A"}]}}
    out = ars_merge.mergeDicts(copy.deepcopy(current), copy.deepcopy(merged))
    assert {b["id"] for b in out["node_bindings"]["n0"]} == {"A", "B"}


def test_node_bindings_for_a_node_only_the_newcomer_has():
    current = {"node_bindings": {"n0": [{"id": "A"}], "n9": [{"id": "Z"}]}}
    merged = {"node_bindings": {"n0": [{"id": "A"}]}}
    out = ars_merge.mergeDicts(copy.deepcopy(current), copy.deepcopy(merged))
    assert out["node_bindings"]["n9"] == [{"id": "Z"}]


def test_empty_message_wrappers_do_not_raise():
    """QueryGraph/KnowledgeGraph/Results returned early on None, leaving
    their attributes unset so the next getter raised AttributeError."""
    tm = ars_merge.TranslatorMessage({})
    assert tm.to_dict() == {
        "message": {
            "query_graph": {},
            "knowledge_graph": {},
            "results": [],
            "auxiliary_graphs": {},
        }
    }
    assert ars_merge.QueryGraph(None).getNodes() == {}
    assert ars_merge.KnowledgeGraph(None).getEdges() == {}
    assert ars_merge.Results(None).getRaw() == []


def test_results_default_to_a_list_not_a_dict():
    """Upstream's to_dict emitted "results": {} -- results is a TRAPI array."""
    tm = ars_merge.TranslatorMessage({"knowledge_graph": {"nodes": {}, "edges": {}}})
    assert tm.to_dict()["message"]["results"] == []


# ---------------------------------------------------------------------------
# blocklist.remove_blocked
# ---------------------------------------------------------------------------


BLOCKED = {"BAD:1": {"name": "bad"}}


@pytest.mark.parametrize(
    "message",
    [
        pytest.param({"results": []}, id="no_knowledge_graph"),
        pytest.param(
            {"knowledge_graph": {"nodes": {"X:1": {}}, "edges": {}}, "results": []},
            id="no_auxiliary_graphs",
        ),
        pytest.param(
            {"knowledge_graph": {"nodes": {"X:1": {}}}, "auxiliary_graphs": {}},
            id="no_edges",
        ),
        pytest.param({}, id="empty_message"),
    ],
)
def test_remove_blocked_survives_ordinary_shapes(message):
    """Upstream bound its accumulators inside conditional branches and read
    them unconditionally, so any of these shapes -- an ordinary response with
    no aux graphs above all -- died on an UnboundLocalError."""
    pk, removed, dropped = remove_blocked({"message": message}, BLOCKED, "pk")
    assert pk == "pk"
    assert removed == [] and dropped == []


def test_path_bindings_are_pruned_for_every_path_id():
    """Upstream's removal loop sat outside the per-path loop and reused the
    loop variable's name, so it pruned only the last path id."""
    data = {
        "message": {
            "knowledge_graph": {
                "nodes": {"BAD:1": {"name": "bad"}, "G:1": {}},
                "edges": {
                    "e0": {"subject": "BAD:1", "object": "G:1"},
                    "e9": {"subject": "G:1", "object": "G:1"},
                },
            },
            # "kept" needs an edge that survives: an aux graph whose edges are
            # all removed is itself removed, and upstream counts an EMPTY
            # edge list as "all removed" too (a quirk the port keeps).
            "auxiliary_graphs": {"gone": {"edges": ["e0"]}, "kept": {"edges": ["e9"]}},
            "results": [
                {
                    "node_bindings": {"n0": [{"id": "G:1"}]},
                    "analyses": [
                        {
                            "path_bindings": {
                                "p0": [{"id": "gone"}, {"id": "kept"}],
                                "p1": [{"id": "gone"}, {"id": "kept"}],
                            }
                        }
                    ],
                }
            ],
        }
    }
    remove_blocked(data, BLOCKED, "pk")
    pb = data["message"]["results"][0]["analyses"][0]["path_bindings"]
    assert pb == {"p0": [{"id": "kept"}], "p1": [{"id": "kept"}]}


def test_empty_path_bindings_do_not_raise():
    data = {
        "message": {
            "knowledge_graph": {"nodes": {"BAD:1": {}}, "edges": {}},
            "auxiliary_graphs": {},
            "results": [
                {
                    "node_bindings": {"n0": [{"id": "G:1"}]},
                    "analyses": [{"path_bindings": {}}],
                }
            ],
        }
    }
    remove_blocked(data, BLOCKED, "pk")  # no raise


def test_analysis_support_graphs_match_removed_aux_graphs():
    """Upstream compared analysis support_graphs (aux graph ids) against
    removed EDGE ids, which never matches, so a support graph that really had
    gone away stayed on the analysis."""
    data = {
        "message": {
            "knowledge_graph": {
                "nodes": {"BAD:1": {"name": "bad"}, "G:1": {}},
                "edges": {"e0": {"subject": "BAD:1", "object": "G:1"}},
            },
            "auxiliary_graphs": {"gone": {"edges": ["e0"]}},
            "results": [
                {
                    "node_bindings": {"n0": [{"id": "G:1"}]},
                    "analyses": [
                        {
                            "edge_bindings": {},
                            "support_graphs": ["gone", "still_here"],
                        }
                    ],
                }
            ],
        }
    }
    remove_blocked(data, BLOCKED, "pk")
    analysis = data["message"]["results"][0]["analyses"][0]
    assert analysis["support_graphs"] == ["still_here"]


# ---------------------------------------------------------------------------
# statuses
# ---------------------------------------------------------------------------


def test_unknown_status_never_reaches_the_column():
    """Upstream wrote whatever it was handed. A one-character nonsense status
    is not terminal (the parent never completes) and is not 'R' (the watchdog
    never reaps it), so the message was stranded for good."""
    assert statuses.coerce_status("X", "D") == "D"
    assert statuses.coerce_status("not a status", "D") == "D"
    assert statuses.coerce_status(None, "U") == "U"
    assert statuses.coerce_status(42, "U") == "U"
    # known letters and long names still pass through
    assert statuses.coerce_status("E", "D") == "E"
    assert statuses.coerce_status("Stopped", "D") == "S"
    with pytest.raises(statuses.InvalidStatus):
        statuses.validate_letter("X")

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
    # TRAPI 2.0: no null source_record_urls, no empty upstream_resource_ids
    # (upstream set both; 2.0 forbids nulls and upstream_resource_ids has
    # minItems 1)
    assert sources[1] == {
        "resource_id": "infores:aragorn",
        "resource_role": "primary_knowledge_source",
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


def _attrs(type_id, value):
    return {"attributes": [{"attribute_type_id": type_id, "value": value}]}


def test_object_valued_attributes_are_not_dropped():
    """Upstream deduped the union with set(). A value list of OBJECTS --
    publications carrying metadata, say -- raised "unhashable type: 'dict'",
    the generic except swallowed it, the break never ran, and because this is
    the else-branch of "append the whole attribute" the current agent's
    values were dropped from the merged message entirely."""
    out = ars_merge.mergeDicts(
        copy.deepcopy(_attrs("biolink:publications", [{"id": "PMID:2"}])),
        copy.deepcopy(_attrs("biolink:publications", [{"id": "PMID:1"}])),
    )
    assert len(out["attributes"]) == 1
    ids = [v["id"] for v in out["attributes"][0]["value"]]
    assert sorted(ids) == ["PMID:1", "PMID:2"]


def test_identical_object_values_are_deduped():
    out = ars_merge.mergeDicts(
        copy.deepcopy(_attrs("p", [{"id": "PMID:1"}])),
        copy.deepcopy(_attrs("p", [{"id": "PMID:1"}])),
    )
    assert out["attributes"][0]["value"] == [{"id": "PMID:1"}]


def test_scalar_values_still_union_and_dedupe():
    out = ars_merge.mergeDicts(
        copy.deepcopy(_attrs("p", ["PMID:2", "PMID:1"])),
        copy.deepcopy(_attrs("p", ["PMID:1", "PMID:3"])),
    )
    assert sorted(out["attributes"][0]["value"]) == ["PMID:1", "PMID:2", "PMID:3"]


def test_value_union_is_order_stable():
    """Upstream's set() union made the order depend on the hash seed, which
    the golden harness had to work around. Merged side first, then whatever
    the current side adds, both in their own order."""
    out = ars_merge.mergeDicts(
        copy.deepcopy(_attrs("p", ["c", "a"])),
        copy.deepcopy(_attrs("p", ["b", "a"])),
    )
    assert out["attributes"][0]["value"] == ["b", "a", "c"]


def test_mixed_scalar_and_object_values_survive():
    out = ars_merge.mergeDicts(
        copy.deepcopy(_attrs("p", ["PMID:2", {"id": "X"}])),
        copy.deepcopy(_attrs("p", [{"id": "X"}, "PMID:1"])),
    )
    value = out["attributes"][0]["value"]
    assert {"id": "X"} in value
    assert "PMID:1" in value and "PMID:2" in value
    assert len(value) == 3  # the duplicate object collapsed


def test_non_list_merged_value_is_coerced():
    """occurence_count == 1 only checks the CURRENT value is a list; upstream
    then did scalar + list, another TypeError into the same swallow."""
    out = ars_merge.mergeDicts(
        copy.deepcopy(_attrs("p", ["PMID:2"])),
        copy.deepcopy(_attrs("p", "PMID:1")),
    )
    assert sorted(out["attributes"][0]["value"]) == ["PMID:1", "PMID:2"]


def test_value_dedupe_does_not_collapse_distinct_types():
    out = ars_merge.mergeDicts(
        copy.deepcopy(_attrs("p", [1, "1"])),
        copy.deepcopy(_attrs("p", [True])),
    )
    assert len(out["attributes"][0]["value"]) == 3


def test_object_lists_without_a_keying_field_survive():
    """Objects in a list are matched on resource_id / qualifier_type_id.
    Upstream dropped every object carrying neither -- and from BOTH sides,
    since it replaced the merged list with the keyed map's values -- so a
    list of objects of any other shape merged to []."""
    out = ars_merge.mergeDicts({"a": [{"x": 1}]}, {"a": [{"y": 2}]})
    assert {"x": 1} in out["a"] and {"y": 2} in out["a"]


def test_unkeyed_objects_survive_alongside_keyed_ones():
    out = ars_merge.mergeDicts(
        {"sources": [{"resource_id": "infores:b"}, {"note": "keep me"}]},
        {"sources": [{"resource_id": "infores:a"}]},
    )
    assert {"note": "keep me"} in out["sources"]
    assert {"resource_id": "infores:a"} in out["sources"]
    assert {"resource_id": "infores:b"} in out["sources"]


def test_identical_unkeyed_objects_are_deduped():
    out = ars_merge.mergeDicts({"a": [{"x": 1}]}, {"a": [{"x": 1}]})
    assert out["a"] == [{"x": 1}]


def test_sources_sharing_a_resource_id_still_merge():
    """The keyed path is unchanged: same resource_id folds into one entry."""
    out = ars_merge.mergeDicts(
        {"sources": [{"resource_id": "infores:a", "upstream_resource_ids": ["u2"]}]},
        {"sources": [{"resource_id": "infores:a", "upstream_resource_ids": ["u1"]}]},
    )
    assert len(out["sources"]) == 1
    assert sorted(out["sources"][0]["upstream_resource_ids"]) == ["u1", "u2"]


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


def _nb(**ids):
    """TRAPI 2.0 node bindings: one {"ids": [...]} object per query node."""
    return {"node_bindings": {k: {"ids": list(v)} for k, v in ids.items()}}


def test_node_bindings_union_every_current_only_binding():
    """Upstream's else hung off the for, so only the LAST current-only id was
    carried -- and into a local map it never wrote back. (TRAPI 2.0: the
    union is of each query node's ``ids``, merged side first.)"""
    current = _nb(n0=["A"], n1=["B"], n2=["D"])
    merged = _nb(n0=["A"], n1=["C"], n2=["E"])
    out = ars_merge.mergeDicts(copy.deepcopy(current), copy.deepcopy(merged))
    nb = out["node_bindings"]
    assert nb["n0"] == {"ids": ["A"]}
    assert nb["n1"] == {"ids": ["C", "B"]}
    assert nb["n2"] == {"ids": ["E", "D"]}


def test_node_bindings_past_the_first_are_not_ignored():
    """Upstream keyed off node_value[0] only, so a node's second binding
    onward never took part in the merge."""
    current = _nb(n0=["A", "B"])
    merged = _nb(n0=["A"])
    out = ars_merge.mergeDicts(copy.deepcopy(current), copy.deepcopy(merged))
    assert out["node_bindings"]["n0"] == {"ids": ["A", "B"]}


def test_node_bindings_for_a_node_only_the_newcomer_has():
    current = _nb(n0=["A"], n9=["Z"])
    merged = _nb(n0=["A"])
    out = ars_merge.mergeDicts(copy.deepcopy(current), copy.deepcopy(merged))
    assert out["node_bindings"]["n9"] == {"ids": ["Z"]}


def test_node_binding_extra_members_are_folded_not_lost():
    """A NodeBinding allows additional properties; the ids union must not
    drop what else a binding carries."""
    current = {"node_bindings": {"n0": {"ids": ["B"], "note": "cur"}}}
    merged = {"node_bindings": {"n0": {"ids": ["A"], "other": 1}}}
    out = ars_merge.mergeDicts(copy.deepcopy(current), copy.deepcopy(merged))
    assert out["node_bindings"]["n0"] == {"ids": ["A", "B"], "other": 1, "note": "cur"}


def test_empty_message_wrappers_do_not_raise():
    """QueryGraph/KnowledgeGraph/Results returned early on None, leaving
    their attributes unset so the next getter raised AttributeError. And
    upstream's to_dict filled every absent component with {} -- an empty
    query graph / knowledge graph (no ``nodes``) and an empty
    auxiliary_graphs map are all invalid TRAPI 2.0, so they are omitted."""
    tm = ars_merge.TranslatorMessage({})
    assert tm.to_dict() == {"message": {"results": []}}
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
                    "node_bindings": {"n0": {"ids": ["G:1"]}},
                    "analyses": [
                        {
                            "path_bindings": {
                                "p0": {"ids": ["gone", "kept"]},
                                "p1": {"ids": ["gone", "kept"]},
                            }
                        }
                    ],
                }
            ],
        }
    }
    remove_blocked(data, BLOCKED, "pk")
    pb = data["message"]["results"][0]["analyses"][0]["path_bindings"]
    assert pb == {"p0": {"ids": ["kept"]}, "p1": {"ids": ["kept"]}}


def test_empty_path_bindings_do_not_raise():
    data = {
        "message": {
            "knowledge_graph": {"nodes": {"BAD:1": {}}, "edges": {}},
            "auxiliary_graphs": {},
            "results": [
                {
                    "node_bindings": {"n0": {"ids": ["G:1"]}},
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
                    "node_bindings": {"n0": {"ids": ["G:1"]}},
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

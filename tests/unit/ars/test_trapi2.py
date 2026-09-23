"""TRAPI 2.0 rules in the ARS port (Shepherd speaks 2.0; upstream Relay 1.5).

The golden suite pins the corpus outputs (2.0 translations of the Relay
goldens); this file pins the 2.0-specific rules on inputs the corpus does not
contain. See docs/ARS_PARITY_REGISTER.md, "TRAPI 2.0".
"""

import copy
import datetime
import json
import pathlib

import pytest
from translator_tom import LogEntry, Response

from shepherd_utils.ars import merge as ars_merge
from shepherd_utils.ars import premerge as ars_premerge
from shepherd_utils.ars.blocklist import remove_blocked
from shepherd_utils.ars.trapi import strip_nulls, validate
from shepherd_utils.trapi import finalize_response

CORPUS = pathlib.Path(__file__).resolve().parents[2] / "fixtures/ars_corpus"


def load(name):
    return json.loads((CORPUS / name).read_text())


def _edge(kl="not_provided", at="not_provided", **extra):
    edge = {
        "subject": "A:1",
        "object": "B:1",
        "predicate": "biolink:affects",
        "sources": [
            {"resource_id": "infores:x", "resource_role": "primary_knowledge_source"}
        ],
        "knowledge_level": kl,
        "agent_type": at,
    }
    edge.update(extra)
    return edge


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


def test_validate_is_pure():
    data = load("response_aragorn.json")
    before = copy.deepcopy(data)
    assert validate(data)
    assert data == before


def test_validate_accepts_null_on_an_optional_member_as_tom_does():
    """validate() is TOM's verdict; reading nulls as absent is strip_nulls'
    job, which premerge runs first (so the ARS never passes nulls on)."""
    data = load("response_aragorn.json")
    data["message"]["knowledge_graph"]["nodes"]["CHEBI:6801"]["is_set"] = None
    assert validate(data)
    assert validate(strip_nulls(data))


def test_null_on_a_required_member_fails_once_stripped():
    data = load("response_aragorn.json")
    data["message"]["knowledge_graph"]["edges"]["e1"]["attributes"][0]["value"] = None
    assert not validate(strip_nulls(data))


@pytest.mark.parametrize(
    "mutate",
    [
        lambda m: m["results"][0]["analyses"][0].pop("edge_bindings"),
        lambda m: m["query_graph"].pop("edges"),
    ],
    ids=["analysis_without_bindings", "query_graph_without_edges_or_paths"],
)
def test_validate_enforces_the_schema_any_of_rules(mutate):
    """2.0 merged 1.5's Analysis/PathfinderAnalysis and QueryGraph/
    PathfinderQueryGraph into one object each with an anyOf, which TOM's
    pydantic models do not express."""
    data = load("response_aragorn.json")
    mutate(data["message"])
    Response.from_dict(data)  # the models alone accept it...
    assert not validate(data)  # ...the ARS verdict does not


def test_strip_nulls_leaves_free_form_values_alone():
    doc = {"a": None, "attributes": [{"value": [None, {"k": None}], "x": None}]}
    assert strip_nulls(doc) == {"attributes": [{"value": [None, {"k": None}]}]}


def test_strip_nulls_is_not_recursion_bound():
    doc = node = {}
    for _ in range(5000):
        node["n"] = {"gone": None}
        node = node["n"]
    strip_nulls(doc)
    assert "gone" not in node


# ---------------------------------------------------------------------------
# premerge
# ---------------------------------------------------------------------------


def test_add_attribute_emits_no_nulls():
    """Upstream built every added attribute from a template of eight
    None-valued members."""
    node = {"categories": ["biolink:Gene"]}
    ars_premerge.add_attribute(
        node, {"attribute_type_id": "biothings_annotations", "value": {"a": 1}}
    )
    ars_premerge.add_attribute(
        node, {"attribute_type_id": "t", "value": 1, "description": None, "junk": 2}
    )
    assert node["attributes"] == [
        {"attribute_type_id": "biothings_annotations", "value": {"a": 1}},
        {"attribute_type_id": "t", "value": 1},
    ]
    Response.from_dict(
        {"message": {"knowledge_graph": {"nodes": {"G:1": node}, "edges": {}}}}
    )


def test_log_timestamp_is_rfc3339():
    """Upstream stamped log entries %H:%M:%S; 2.0 requires a zoned
    date-time."""
    stamp = ars_premerge.log_timestamp()
    LogEntry.from_dict({"timestamp": stamp, "message": "m"})
    assert stamp.endswith("Z")
    naive = datetime.datetime(2026, 9, 1, 12, 0, 0)
    assert ars_premerge.log_timestamp(naive) == "2026-09-01T12:00:00Z"
    aware = datetime.datetime(
        2026, 9, 1, 8, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=-4))
    )
    assert ars_premerge.log_timestamp(aware) == "2026-09-01T12:00:00Z"


def test_score_passes_skip_results_without_analyses():
    """2.0 makes Result.analyses optional (and forbids []); upstream (1.5)
    abandoned the whole batch at the first result without them."""
    results = [
        {"analyses": [{"score": 0.2}]},
        {"node_bindings": {}},  # no analyses at all
        {"analyses": [{"score": 0.8}]},
        {"analyses": [{"score": 0.5}]},
    ]
    out = ars_premerge.normalizeScores(copy.deepcopy(results))
    assert [r.get("normalized_score") for r in out] == [
        pytest.approx(100 / 3),
        None,
        100.0,
        pytest.approx(200 / 3),
    ]
    stat = ars_premerge.ScoreStatCalc(copy.deepcopy(results))
    assert stat["minimum"] == 0.2 and stat["maximum"] == 0.8
    assert stat["mean"] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# merge
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "first, then, expected",
    [
        # the first (already-merged) value stands
        ("knowledge_assertion", "prediction", "knowledge_assertion"),
        # ...unless it is not_provided and the other is not
        ("not_provided", "prediction", "prediction"),
        ("prediction", "not_provided", "prediction"),
        ("not_provided", "not_provided", "not_provided"),
    ],
)
@pytest.mark.parametrize("member", ["knowledge_level", "agent_type"])
def test_knowledge_level_and_agent_type_conflicts(member, first, then, expected):
    """Upstream turned any conflicting scalar into [merged, current] -- for
    these required string members an invalid 2.0 edge."""
    current = {member: first}  # dcurrent: the accumulated merged version
    merged = {member: then}  # dmerged: the newly arriving ARA's copy
    out = ars_merge.mergeDicts(current, merged)
    assert out[member] == expected


@pytest.mark.parametrize(
    "member", ["predicate", "subject", "object", "is_set", "resource_role"]
)
def test_other_single_valued_members_keep_the_first_value(member):
    out = ars_merge.mergeDicts({member: "first"}, {member: "second"})
    assert out[member] == "first"


def test_free_form_and_extra_members_keep_upstreams_list_of_both():
    """Attribute.value is free-form JSON and extra result properties are
    the ARS's own: upstream's [merged, current] is still valid there."""
    assert ars_merge.mergeDicts({"value": 1}, {"value": 2})["value"] == [2, 1]
    out = ars_merge.mergeDicts({"normalized_score": 10.0}, {"normalized_score": 90.0})
    assert out["normalized_score"] == [90.0, 10.0]


def test_merged_conflicting_edges_are_valid_trapi2():
    a = {"knowledge_graph": {"nodes": {}, "edges": {"e": _edge("prediction")}}}
    b = {
        "knowledge_graph": {
            "nodes": {},
            "edges": {"e": _edge("knowledge_assertion", "manual_agent")},
        }
    }
    out = ars_merge.mergeMessages(
        [ars_merge.TranslatorMessage(a), ars_merge.TranslatorMessage(b)], "pk"
    ).to_dict()
    edge = out["message"]["knowledge_graph"]["edges"]["e"]
    assert edge["knowledge_level"] == "prediction"
    assert edge["agent_type"] == "manual_agent"
    Response.from_dict(finalize_response(out))


def test_result_map_keys_on_single_id_bindings():
    """2.0: a node binding is one {"ids"} object; a query node bound to
    several ids stays out of the key, as upstream's multi-binding rule."""
    tm = ars_merge.TranslatorMessage(
        {
            "results": [
                {"node_bindings": {"n0": {"ids": ["A"]}, "n1": {"ids": ["B"]}}},
                {"node_bindings": {"n0": {"ids": ["A"]}, "n1": {"ids": ["C", "D"]}}},
            ]
        }
    )
    assert set(tm.getResultMap()) == {frozenset({"A", "B"}), frozenset({"A"})}


def test_messages_without_a_knowledge_graph_or_results_merge():
    """Both are optional in 2.0; upstream dereferenced them unconditionally."""
    with_results = {
        "results": [
            {
                "node_bindings": {"n0": {"ids": ["A"]}},
                "analyses": [
                    {"resource_id": "infores:a", "edge_bindings": {"e": {"ids": ["x"]}}}
                ],
            }
        ]
    }
    out = ars_merge.mergeMessages(
        [ars_merge.TranslatorMessage({}), ars_merge.TranslatorMessage(with_results)],
        "pk",
    ).to_dict()
    assert out["message"]["results"] == with_results["results"]
    assert "knowledge_graph" not in out["message"]
    out = ars_merge.mergeMessages(
        [ars_merge.TranslatorMessage(with_results), ars_merge.TranslatorMessage({})],
        "pk",
    ).to_dict()
    assert out["message"]["results"] == with_results["results"]


def test_to_dict_omits_empty_components():
    tm = ars_merge.TranslatorMessage(
        {"query_graph": {}, "knowledge_graph": {}, "auxiliary_graphs": {}}
    )
    assert tm.to_dict() == {"message": {"results": []}}
    tm = ars_merge.TranslatorMessage({"knowledge_graph": {"edges": {"e": _edge()}}})
    # a knowledge graph with edges but no nodes gains the required nodes map
    assert tm.to_dict()["message"]["knowledge_graph"]["nodes"] == {}


def test_get_msg_stats_tolerates_absent_components():
    assert ars_merge.get_msg_stats({"message": {"results": [{}]}}) == {
        "query_graph": 0,
        "knowledge_graph_nodes": 0,
        "knowledge_graph_edges": 0,
        "results": 1,
        "auxiliary_graphs": 0,
    }
    assert (
        ars_merge.get_msg_stats(
            {"message": {"knowledge_graph": {"nodes": {"a": {}}, "edges": None}}}
        )["knowledge_graph_nodes"]
        == 1
    )


# ---------------------------------------------------------------------------
# blocklist
# ---------------------------------------------------------------------------

BLOCKED = {"BAD:1": {"name": "bad"}}


def _blocklist_message():
    return {
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "BAD:1": {"name": "bad", "categories": ["biolink:Gene"]},
                    "G:1": {"categories": ["biolink:Gene"]},
                    "G:2": {"categories": ["biolink:Gene"]},
                },
                "edges": {
                    "bad1": _edge(subject="BAD:1", object="G:1"),
                    "bad2": _edge(subject="G:1", object="BAD:1"),
                    "ok": _edge(subject="G:1", object="G:2"),
                },
            },
            "auxiliary_graphs": {"aux": {"edges": ["bad1", "bad2"]}},
            "results": [
                {
                    "node_bindings": {"n0": {"ids": ["G:1"]}},
                    "analyses": [
                        {
                            # both ids go: the analysis goes (upstream kept it
                            # with an empty binding list)
                            "resource_id": "infores:a",
                            "edge_bindings": {"e": {"ids": ["bad1", "bad2"]}},
                        },
                        {
                            "resource_id": "infores:b",
                            "edge_bindings": {"e": {"ids": ["bad1", "ok"]}},
                            "support_graphs": ["aux"],
                        },
                    ],
                },
                {
                    # a set binding with one blocked id: the result goes
                    "node_bindings": {"n0": {"ids": ["G:2", "BAD:1"]}},
                    "analyses": [
                        {
                            "resource_id": "infores:a",
                            "edge_bindings": {"e": {"ids": ["ok"]}},
                        }
                    ],
                },
            ],
        }
    }


def test_blocklist_prunes_2_0_bindings_and_leaves_no_forbidden_empties():
    data = _blocklist_message()
    remove_blocked(data, BLOCKED, "pk")
    message = data["message"]
    (result,) = message["results"]
    (analysis,) = result["analyses"]
    assert analysis["resource_id"] == "infores:b"
    assert analysis["edge_bindings"] == {"e": {"ids": ["ok"]}}
    # the removed aux graph was its only support graph: the member goes
    assert "support_graphs" not in analysis
    # and it was the message's only aux graph: the map goes too
    assert "auxiliary_graphs" not in message
    assert set(message["knowledge_graph"]["edges"]) == {"ok"}
    for entry in data["logs"]:
        LogEntry.from_dict(entry)
    assert validate(data)

"""Upstream trapi2.0's orchestration changes in the ARAX port
(docs/ARAX_TRAPI2_UPSTREAM_SYNC.md, items 1-8)."""

import gzip
import json
import os
import shutil
import threading

import pytest

from shepherd_utils.config import settings

HERE = os.path.dirname(os.path.abspath(__file__))
EXPAND = os.path.join(HERE, "expand_parity")


@pytest.fixture(scope="module")
def _biolink_dir(tmp_path_factory):
    """Biolink 4.4.4's lookup map and model YAML, so nothing is fetched."""
    path = tmp_path_factory.mktemp("biolink")
    shutil.copy(os.path.join(EXPAND, "biolink_lookup_map_4.4.4_v5.pickle"), path)
    with (
        gzip.open(os.path.join(EXPAND, "biolink_model_4.4.4.yaml.gz")) as f_in,
        open(path / "biolink_model_4.4.4.yaml", "wb") as f_out,
    ):
        shutil.copyfileobj(f_in, f_out)
    return str(path)


@pytest.fixture(autouse=True)
def _biolink(monkeypatch, _biolink_dir):
    from shepherd_utils.arax import ARAX_query

    monkeypatch.setattr(settings, "arax_biolink_cache_dir", _biolink_dir)
    ARAX_query._get_valid_biolink_predicates.cache_clear()
    ARAX_query._get_valid_biolink_qualifier_types.cache_clear()
    yield
    ARAX_query._get_valid_biolink_predicates.cache_clear()
    ARAX_query._get_valid_biolink_qualifier_types.cache_clear()


def _query(qedge_extra=None, parameters=None, actions=None, **extra):
    query = {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"ids": ["CHEBI:1"]},
                    "n1": {"categories": ["biolink:Gene"]},
                },
                "edges": {
                    "e0": {"subject": "n0", "object": "n1", **(qedge_extra or {})}
                },
            }
        },
        "operations": {"actions": actions or ["return(message=true, store=false)"]},
        **extra,
    }
    if parameters is not None:
        query["parameters"] = parameters
    return query


def _run(query):
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    araxq = ARAXQuery()
    araxq.query(query)
    return araxq.response


def _stream(araxq, query):
    """query_return_stream's lines, failing rather than hanging if it never ends."""
    lines = []

    def consume():
        lines.extend(araxq.query_return_stream(query))

    thread = threading.Thread(target=consume, daemon=True)
    thread.start()
    thread.join(timeout=60)
    assert not thread.is_alive(), "query_return_stream never finished"
    return [json.loads(line) for line in lines]


# --- item 1 ----------------------------------------------------------------


@pytest.mark.parametrize(
    "target",
    [
        "shepherd_utils.arax.ARAX_query.ARAXQuery.examine_incoming_query",
        "shepherd_utils.arax.ARAX_query.ARAXQueryGraphInterpreter.translate_to_araxi",
    ],
)
def test_an_uncaught_error_outside_the_action_loop_ends_the_stream(monkeypatch, target):
    """upstream trapi2.0 (775b97a1): asynchronous_query records the exception
    and still sets DONE, so query_return_stream ends."""
    import time

    from shepherd_utils.arax.ARAX_query import ARAXQuery

    def boom(*args, **kwargs):
        # Not at once: query_return_stream (upstream's) yields nothing at all
        # when the query thread is already DONE at its first status check
        time.sleep(0.5)
        raise RuntimeError("boom")

    monkeypatch.setattr(target, boom)
    query = _query()
    del query["operations"]  # the interpreter runs on a bare query graph
    araxq = ARAXQuery()
    lines = _stream(araxq, query)
    assert araxq.response.status == "ERROR"
    assert araxq.response.error_code == "UncaughtError"
    assert "boom" in araxq.response.message
    assert "message" in lines[-1]  # the envelope is the last line


def test_asynchronous_query_sets_done_after_an_uncaught_error(monkeypatch):
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(ARAXQuery, "examine_incoming_query", boom)
    araxq = ARAXQuery()
    araxq.lock = threading.Lock()
    araxq.asynchronous_query(_query())
    assert araxq.response.status == "DONE,ERROR"
    assert araxq.response.error_code == "UncaughtError"


def test_query_return_message_records_an_uncaught_error_and_reraises(monkeypatch):
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(ARAXQuery, "examine_incoming_query", boom)
    finished = []
    monkeypatch.setattr(
        ARAXQuery, "track_query_finish", lambda self: finished.append(True)
    )
    araxq = ARAXQuery()
    with pytest.raises(RuntimeError):
        araxq.query_return_message(_query())
    assert araxq.response.error_code == "UncaughtError"
    assert finished == [True]


# --- item 2 ----------------------------------------------------------------


def test_a_non_biolink_predicate_is_invalid():
    response = _run(_query({"predicates": ["biolink:treats", "biolink:foo"]}))
    assert response.status == "ERROR"
    assert response.error_code == "InvalidPredicate"
    assert response.http_status == 400
    assert "'biolink:foo'" in response.message


def test_a_non_biolink_qualifier_type_is_invalid():
    response = _run(
        _query(
            {
                "constraints": {
                    "qualifiers": [
                        {
                            "biolink:object_direction_qualifier": "increased",
                            "biolink:foo_qualifier": "x",
                        }
                    ]
                }
            }
        )
    )
    assert response.status == "ERROR"
    assert response.error_code == "InvalidQualifier"
    assert response.http_status == 422
    assert "'biolink:foo_qualifier'" in response.message


def test_qualifier_check_is_skipped_with_a_warning_when_biolink_yaml_is_unavailable(
    monkeypatch,
):
    from biolink_helper_pkg import BiolinkHelper

    def fail(self):
        raise RuntimeError("Biolink YAML download failed")

    monkeypatch.setattr(BiolinkHelper, "download_biolink_model", fail)
    qualifiers = {"qualifiers": [{"biolink:foo_qualifier": "x"}]}
    response = _run(_query({"constraints": qualifiers}))
    assert response.status == "OK", response.show()
    warnings = [m["message"] for m in response.messages if m["level"] == "WARNING"]
    assert any("skipping the qualifier type check" in w for w in warnings), warnings
    # predicates are still validated
    response = _run(_query({"predicates": ["biolink:foo"], "constraints": qualifiers}))
    assert response.error_code == "InvalidPredicate"


def test_predicates_are_checked_against_biolink_4_4_4():
    """DEC-32: ARAX's Biolink is 4.4.4 (upstream trapi2.0), not 4.2.5."""
    from shepherd_utils.arax.BiolinkHelper.biolink_helper import (
        get_current_arax_biolink_version,
    )

    assert get_current_arax_biolink_version() == "4.4.4"
    # new in 4.4.x
    response = _run(_query({"predicates": ["biolink:has_preventative_intervention"]}))
    assert response.status == "OK", response.show()
    # 4.2.5 only
    response = _run(_query({"predicates": ["biolink:affects_response_to"]}))
    assert response.error_code == "InvalidPredicate"


def test_biolink_predicates_and_qualifiers_are_valid():
    from shepherd_utils.arax.ARAX_query import (
        _get_valid_biolink_predicates,
        _get_valid_biolink_qualifier_types,
    )

    response = _run(
        _query(
            {
                "predicates": ["biolink:affects", "biolink:related_to"],
                "constraints": {
                    "qualifiers": [
                        {
                            "biolink:object_aspect_qualifier": "activity",
                            "biolink:qualified_predicate": "biolink:causes",
                        }
                    ]
                },
            }
        )
    )
    assert response.status == "OK", response.show()
    assert "biolink:similar_to" in _get_valid_biolink_predicates()  # DEC-28
    assert "biolink:subject_aspect_qualifier" in _get_valid_biolink_qualifier_types()
    assert "biolink:qualifier" not in _get_valid_biolink_qualifier_types()


# --- items 3 and 4 ---------------------------------------------------------


def test_tiers_other_than_tier_0_are_warned_about():
    warning = "ARAX only answers queries using Tier 0 data; ignoring the requested tiers [0, 1]"
    response = _run(_query(parameters={"tiers": [0, 1]}))
    assert response.status == "OK", response.show()
    assert [m["message"] for m in response.messages if m["level"] == "WARNING"] == [
        warning
    ]
    response = _run(_query(parameters={"tiers": [0]}))
    assert not [m for m in response.messages if m["level"] == "WARNING"]


def test_the_stream_leaves_out_log_lines_below_the_requested_level():
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    araxq = ARAXQuery()
    lines = _stream(araxq, _query(parameters={"log_level": "WARNING", "tiers": [1]}))
    levels = [line["level"] for line in lines if "level" in line and "message" in line]
    assert levels == ["WARNING"]
    # the envelope keeps every entry (the arax worker filters what it stores)
    envelope = lines[-1]
    assert {entry["level"] for entry in envelope["logs"]} >= {"DEBUG", "INFO"}

    araxq = ARAXQuery()
    lines = _stream(araxq, _query(parameters={"log_level": "DEBUG"}))
    assert "DEBUG" in {line.get("level") for line in lines[:-1]}


# --- item 5 ----------------------------------------------------------------


def test_the_envelope_names_the_tier0_build(monkeypatch):
    monkeypatch.setattr(settings, "arax_tier_version", "tier0-20991231")
    response = _run(_query())
    assert response.envelope.to_dict()["data_release_versions"] == {
        "infores:arax": "tier0-20991231"
    }


# --- item 6 ----------------------------------------------------------------


def _interpreted(qedge_extra=None):
    from shepherd_utils.arax.ARAX_messenger import ARAXMessenger
    from shepherd_utils.arax.ARAX_query_graph_interpreter import (
        ARAXQueryGraphInterpreter,
    )
    from shepherd_utils.arax.ARAX_response import ARAXResponse

    response = ARAXResponse()
    ARAXMessenger().create_envelope(response)
    response.envelope.message = ARAXMessenger().from_dict(
        {
            "query_graph": {
                "nodes": {
                    "n0": {"ids": ["MONDO:1"]},
                    "n1": {"categories": ["biolink:ChemicalEntity"]},
                },
                "edges": {
                    "e0": {"subject": "n1", "object": "n0", **(qedge_extra or {})}
                },
            }
        }
    )
    ARAXQueryGraphInterpreter().translate_to_araxi(response)
    assert response.status == "OK", response.show()
    return response.data["araxi_commands"], [m["message"] for m in response.messages]


@pytest.mark.parametrize(
    "constraints",
    [
        {"knowledge_level": {"allow": ["knowledge_assertion"]}},
        {"agent_type": {"deny": ["text_mining_agent"]}},
    ],
)
def test_the_ngd_overlay_is_skipped_under_kl_at_constraints(constraints):
    commands, _ = _interpreted()
    assert any("action=compute_ngd" in c for c in commands)
    commands, logs = _interpreted({"constraints": constraints})
    assert commands and not any("action=compute_ngd" in c for c in commands)
    assert (
        "Skipping the NGD overlay because the query graph has knowledge_level, "
        "agent_type, or qualifier constraints"
    ) in logs


# --- item 7 ----------------------------------------------------------------


def test_connect_knodes_translation():
    """upstream trapi2.0 (2add4986): one NGD per qnode pair, no COHD, Fisher
    only for qedge-connected pairs, Jaccard only through a connected middle."""
    from shepherd_utils.arax.ARAX_response import ARAXResponse
    from shepherd_utils.arax.operation_to_ARAXi import WorkflowToARAXi

    query_graph = {
        "nodes": {
            "a": {"categories": ["biolink:Gene"]},
            "b": {"ids": ["CHEBI:1"], "categories": ["biolink:ChemicalEntity"]},
            "c": {"categories": ["biolink:Disease"]},
        },
        "edges": {
            "e1": {"subject": "b", "object": "a"},
            "e2": {"subject": "a", "object": "c"},
        },
    }
    araxi = WorkflowToARAXi().translate(
        [{"id": "overlay_connect_knodes"}], query_graph, ARAXResponse()
    )
    ngd = "overlay(action=compute_ngd,default_value=inf,virtual_relation_label=connect_knodes_ngd_{0}_{1},subject_qnode_key={0},object_qnode_key={1})"
    fisher = "overlay(action=fisher_exact_test,virtual_relation_label=connect_knodes_fisher_{0}_{1},subject_qnode_key={2},object_qnode_key={3})"
    assert araxi == [
        ngd.format("a", "b"),
        ngd.format("a", "c"),
        ngd.format("b", "c"),
        fisher.format("a", "b", "a", "b"),
        fisher.format("a", "b", "b", "a"),
        fisher.format("a", "c", "a", "c"),
        fisher.format("a", "c", "c", "a"),
        "overlay(action=compute_jaccard,virtual_relation_label=connect_knodes_jaccard_b_c,start_node_key=b,end_node_key=c,intermediate_node_key=a)",
    ]


# --- item 8 ----------------------------------------------------------------


def test_expand_options_come_from_additional_properties_and_query_options(
    monkeypatch,
):
    from shepherd_utils.arax import ARAX_query

    seen = []

    def apply(self, response, parameters, mode=None):
        seen.append(dict(parameters))
        return response

    monkeypatch.setattr(ARAX_query.ARAXExpander, "apply", apply)
    response = _run(
        _query(
            actions=["expand(edge_key=e0)", "return(message=true, store=false)"],
            query_options={"kp_timeout": 7, "prune_threshold": 10},
            additional_properties={
                "prune_threshold": "20",
                "return_minimal_metadata": True,
            },
        )
    )
    assert response.status == "OK", response.show()
    (parameters,) = seen
    assert parameters["kp_timeout"] == 7
    assert parameters["prune_threshold"] == 20  # additional_properties wins
    assert parameters["return_minimal_metadata"] is True
    # the response still carries query_options, and no additional_properties
    envelope = response.envelope.to_dict()
    assert envelope["query_options"]["prune_threshold"] == 10
    assert "additional_properties" not in envelope


@pytest.mark.parametrize(
    "extra",
    [
        {"additional_properties": {"max_path_length": 3, "max_pathfinder_paths": 9}},
        {"query_options": {"max_path_length": 3, "max_pathfinder_paths": 9}},
        {
            "query_options": {"max_path_length": 5, "max_pathfinder_paths": 9},
            "additional_properties": {"max_path_length": 3},
        },
    ],
)
def test_pathfinder_options_come_from_either_place(monkeypatch, extra):
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    planned = []

    def execute_processing_plan(self, query, mode="ARAX"):
        planned.extend(query["operations"]["actions"])
        return self.response

    monkeypatch.setattr(ARAXQuery, "execute_processing_plan", execute_processing_plan)
    query = {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": ["CHEBI:1"]}, "n1": {"ids": ["MONDO:1"]}},
                "paths": {"p0": {"subject": "n0", "object": "n1"}},
            }
        },
        **extra,
    }
    response = _run(query)
    assert response.status == "OK", response.show()
    assert planned == [
        "connect(action=connect_nodes, max_path_length=3, max_pathfinder_paths=9)"
    ]

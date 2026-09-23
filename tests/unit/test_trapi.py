"""TRAPI 2.0 helpers shared by the server and workers (shepherd_utils.trapi)."""

import doctest

import pytest
from translator_tom import Query, Response

import shepherd_utils.trapi as trapi
from shepherd_utils.trapi import (
    SCHEMA_VERSION,
    TRAPIRequestError,
    finalize_response,
    is_trapi_1_response,
    upgrade_trapi_1_response,
    validate_query,
)


def test_doctests():
    """The module's examples are part of its contract."""
    failures, _ = doctest.testmod(trapi)
    assert failures == 0


def _query(**qedge_extra):
    return {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": ["MONDO:0005148"]}, "n1": {}},
                "edges": {
                    "e0": {
                        "subject": "n1",
                        "object": "n0",
                        "predicates": ["biolink:treats"],
                        **qedge_extra,
                    }
                },
            }
        },
        "parameters": {"log_level": "DEBUG", "timeout": 30},
    }


def test_valid_2_0_query_is_accepted():
    query = _query(
        constraints={
            "qualifiers": [{"biolink:object_direction_qualifier": "increased"}]
        }
    )
    validate_query(query)
    Query.from_dict(query)


@pytest.mark.parametrize(
    "mutate, replacement",
    [
        (lambda q: q.update(log_level="DEBUG"), "parameters.log_level"),
        (lambda q: q.update(bypass_cache=True), "parameters.bypass_cache"),
        (
            lambda q: q["message"]["query_graph"]["edges"]["e0"].update(
                qualifier_constraints=[{"qualifier_set": []}]
            ),
            "constraints.qualifiers",
        ),
        (
            lambda q: q["message"]["query_graph"]["edges"]["e0"].update(
                attribute_constraints=[]
            ),
            "constraints.attributes",
        ),
        (
            lambda q: q["message"]["query_graph"].update(
                paths={
                    "p0": {
                        "subject": "n0",
                        "object": "n1",
                        "constraints": [{"intermediate_categories": ["biolink:Gene"]}],
                    }
                }
            ),
            "required_intermediate_categories",
        ),
    ],
)
def test_1_x_spellings_are_rejected_naming_the_fix(mutate, replacement):
    """2.0 allows extra properties there, so they'd otherwise be ignored."""
    query = _query()
    mutate(query)
    with pytest.raises(TRAPIRequestError, match=replacement):
        validate_query(query)


def test_1_x_node_bindings_shape_is_invalid():
    query = _query()
    query["message"]["results"] = [{"node_bindings": {"n0": [{"id": "X:1"}]}}]
    with pytest.raises(TRAPIRequestError, match="node_bindings"):
        validate_query(query)


def test_nulls_in_the_query_graph_mean_absent():
    query = _query(constraints=None)
    query["message"]["query_graph"]["nodes"]["n1"]["ids"] = None
    query["message"]["knowledge_graph"] = None
    validate_query(query)
    assert "ids" not in query["message"]["query_graph"]["nodes"]["n1"]
    assert "constraints" not in query["message"]["query_graph"]["edges"]["e0"]
    assert "knowledge_graph" not in query["message"]


def test_shepherd_workflow_operations_are_left_to_shepherd():
    """``aragorn.lookup`` isn't a standard operation, but Shepherd runs it."""
    query = _query()
    query["workflow"] = [{"id": "aragorn.lookup"}, {"id": "score_paths"}]
    validate_query(query)


def _v1_response():
    return {
        "message": {
            "query_graph": _query()["message"]["query_graph"],
            "knowledge_graph": {
                "nodes": {
                    "MONDO:0005148": {"categories": ["biolink:Disease"]},
                    "CHEBI:1": {"categories": ["biolink:ChemicalEntity"]},
                },
                "edges": {
                    "k0": {
                        "subject": "CHEBI:1",
                        "object": "MONDO:0005148",
                        "predicate": "biolink:treats",
                        "sources": [
                            {
                                "resource_id": "infores:x",
                                "resource_role": "primary_knowledge_source",
                            }
                        ],
                        "attributes": [
                            {
                                "attribute_type_id": "biolink:knowledge_level",
                                "value": "knowledge_assertion",
                            },
                            {
                                "attribute_type_id": "biolink:agent_type",
                                "value": "manual_agent",
                            },
                        ],
                    }
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "n0": [{"id": "MONDO:0005148", "attributes": []}],
                        "n1": [{"id": "CHEBI:1", "attributes": []}],
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:x",
                            "edge_bindings": {"e0": [{"id": "k0", "attributes": []}]},
                        }
                    ],
                }
            ],
            "auxiliary_graphs": {},
        },
        "logs": [],
    }


def test_a_1_x_response_is_converted_to_2_0():
    v1 = _v1_response()
    assert is_trapi_1_response(v1)
    v2 = upgrade_trapi_1_response(v1)
    assert not is_trapi_1_response(v2)
    result = v2["message"]["results"][0]
    assert result["node_bindings"]["n1"] == {"ids": ["CHEBI:1"]}
    assert result["analyses"][0]["edge_bindings"]["e0"] == {"ids": ["k0"]}
    edge = v2["message"]["knowledge_graph"]["edges"]["k0"]
    assert edge["knowledge_level"] == "knowledge_assertion"
    assert edge["agent_type"] == "manual_agent"
    assert "attributes" not in edge
    Response.from_dict(v2)


def test_a_2_0_response_is_passed_through_untouched():
    v2 = upgrade_trapi_1_response(_v1_response())
    assert upgrade_trapi_1_response(v2) is v2


def test_finalize_response_makes_a_valid_2_0_response():
    """The stored response starts as a copy of the query; what leaves is a Response."""
    query = _query()
    response = upgrade_trapi_1_response(_v1_response())
    response.update(
        callback="http://example.org/cb",
        submitter="someone",
        parameters={"log_level": "DEBUG", "stale": True},
    )
    response["message"]["auxiliary_graphs"] = {}
    response["message"]["results"][0]["analyses"][0]["support_graphs"] = []
    response["message"]["knowledge_graph"]["edges"]["k0"]["qualifiers"] = []
    response["message"]["knowledge_graph"]["edges"]["k0"]["sources"][0][
        "upstream_resource_ids"
    ] = []
    logs = [{"timestamp": "2026-01-01T00:00:00+00:00", "level": "INFO", "message": "x"}]

    finalize_response(response, query, logs)

    assert response["schema_version"] == SCHEMA_VERSION == "2.0.0"
    assert response["biolink_version"] == trapi.BIOLINK_VERSION
    assert response["parameters"] == query["parameters"]
    assert "callback" not in response and "submitter" not in response
    assert "auxiliary_graphs" not in response["message"]
    assert list(response)[-1] == "logs"
    Response.from_dict(response)


def test_finalize_response_omits_empty_logs_and_keeps_empty_results():
    response = finalize_response(
        {"message": {"knowledge_graph": {"nodes": {}, "edges": {}}, "results": []}},
        {"message": {}},
        [],
    )
    assert "logs" not in response
    assert "parameters" not in response
    assert response["message"]["results"] == []
    assert list(response)[-1] == "message"
    Response.from_dict(response)


def test_finalize_response_drops_analyses_that_bind_nothing():
    response = {
        "message": {
            "results": [
                {
                    "node_bindings": {"n0": {"ids": ["X:1"]}},
                    "analyses": [{"resource_id": "infores:x", "edge_bindings": {}}],
                }
            ]
        }
    }
    finalize_response(response)
    assert "analyses" not in response["message"]["results"][0]
    Response.from_dict(response)


@pytest.mark.parametrize(
    "query_graph, problem",
    [
        ({"nodes": {}, "edges": {"e0": {"subject": "a", "object": "b"}}}, "no nodes"),
        ({"nodes": {"a": {}}, "edges": {}}, "edges is empty"),
        ({"nodes": {"a": {}}, "paths": {}}, "paths is empty"),
        ({"nodes": {"a": {}}}, "needs edges or paths"),
    ],
)
def test_empty_query_graph_members_are_rejected(query_graph, problem):
    """TRAPI 2.0 gives nodes / edges / paths a minProperties of 1."""
    with pytest.raises(TRAPIRequestError, match=problem):
        validate_query({"message": {"query_graph": query_graph}})

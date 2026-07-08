"""Tests for per-response message hygiene (``shepherd_utils.ars_clean``)."""

from shepherd_utils.ars_clean import (
    decorate_edges_with_infores,
    remove_phantom_support_graphs,
    scrub_null_attributes,
)


def _msg(nodes=None, edges=None, aux=None):
    return {
        "message": {
            "knowledge_graph": {"nodes": nodes or {}, "edges": edges or {}},
            "auxiliary_graphs": aux if aux is not None else {},
            "results": [],
        }
    }


# --- scrub_null_attributes -------------------------------------------------


def test_scrub_removes_none_node_and_edge_attributes():
    msg = _msg(
        nodes={"n1": {"attributes": [None, {"a": 1}, None]}},
        edges={"e1": {"attributes": [None, {"b": 2}], "sources": []}},
    )
    scrub_null_attributes(msg)
    kg = msg["message"]["knowledge_graph"]
    assert kg["nodes"]["n1"]["attributes"] == [{"a": 1}]
    assert kg["edges"]["e1"]["attributes"] == [{"b": 2}]


def test_scrub_drops_sources_with_null_resource_id():
    msg = _msg(
        edges={
            "e1": {
                "sources": [
                    {"resource_id": "infores:x", "upstream_resource_ids": ["a"]},
                    {"resource_id": None},
                    {},
                ]
            }
        }
    )
    scrub_null_attributes(msg)
    sources = msg["message"]["knowledge_graph"]["edges"]["e1"]["sources"]
    assert len(sources) == 1
    assert sources[0]["resource_id"] == "infores:x"


def test_scrub_defaults_and_cleans_upstream_resource_ids():
    msg = _msg(
        edges={
            "e1": {
                "sources": [
                    {"resource_id": "infores:x"},  # missing upstream -> []
                    {"resource_id": "infores:y", "upstream_resource_ids": ["a", None]},
                ]
            }
        }
    )
    scrub_null_attributes(msg)
    sources = msg["message"]["knowledge_graph"]["edges"]["e1"]["sources"]
    assert sources[0]["upstream_resource_ids"] == []
    assert sources[1]["upstream_resource_ids"] == ["a"]


def test_scrub_defaults_aux_graph_attributes():
    msg = _msg(aux={"aux1": {"edges": [], "attributes": None}})
    scrub_null_attributes(msg)
    assert msg["message"]["auxiliary_graphs"]["aux1"]["attributes"] == []


def test_scrub_defaults_nested_edge_attribute_attributes():
    msg = _msg(
        edges={"e1": {"attributes": [{"attribute_type_id": "x", "attributes": None}]}}
    )
    scrub_null_attributes(msg)
    attr = msg["message"]["knowledge_graph"]["edges"]["e1"]["attributes"][0]
    assert attr["attributes"] == []


# --- decorate_edges_with_infores -------------------------------------------


def test_decorate_adds_primary_when_no_sources():
    msg = _msg(edges={"e1": {"subject": "a", "object": "b"}})
    decorate_edges_with_infores(msg, "infores:aragorn")
    sources = msg["message"]["knowledge_graph"]["edges"]["e1"]["sources"]
    assert sources == [
        {
            "resource_id": "infores:aragorn",
            "resource_role": "primary_knowledge_source",
            "source_record_urls": None,
            "upstream_resource_ids": [],
        }
    ]


def test_decorate_adds_self_as_aggregator_when_primary_exists():
    msg = _msg(
        edges={
            "e1": {
                "sources": [
                    {
                        "resource_id": "infores:other",
                        "resource_role": "primary_knowledge_source",
                    }
                ]
            }
        }
    )
    decorate_edges_with_infores(msg, "infores:aragorn")
    sources = msg["message"]["knowledge_graph"]["edges"]["e1"]["sources"]
    self_src = next(s for s in sources if s["resource_id"] == "infores:aragorn")
    assert self_src["resource_role"] == "aggregator_knowledge_source"


def test_decorate_adds_self_as_primary_when_no_primary_present():
    msg = _msg(
        edges={
            "e1": {
                "sources": [
                    {
                        "resource_id": "infores:other",
                        "resource_role": "aggregator_knowledge_source",
                    }
                ]
            }
        }
    )
    decorate_edges_with_infores(msg, "infores:aragorn")
    self_src = next(
        s
        for s in msg["message"]["knowledge_graph"]["edges"]["e1"]["sources"]
        if s["resource_id"] == "infores:aragorn"
    )
    assert self_src["resource_role"] == "primary_knowledge_source"


def test_decorate_noop_when_self_already_present():
    msg = _msg(
        edges={
            "e1": {
                "sources": [
                    {
                        "resource_id": "infores:aragorn",
                        "resource_role": "primary_knowledge_source",
                    }
                ]
            }
        }
    )
    decorate_edges_with_infores(msg, "infores:aragorn")
    assert len(msg["message"]["knowledge_graph"]["edges"]["e1"]["sources"]) == 1


def test_decorate_uses_unknown_when_inforesid_none():
    msg = _msg(edges={"e1": {}})
    decorate_edges_with_infores(msg, None)
    assert (
        msg["message"]["knowledge_graph"]["edges"]["e1"]["sources"][0]["resource_id"]
        == "infores:unknown"
    )


def test_decorate_builds_distinct_source_per_edge():
    """Regression: each edge gets its own source object, not a shared mutated one."""
    msg = _msg(edges={"e1": {}, "e2": {}})
    decorate_edges_with_infores(msg, "infores:aragorn")
    edges = msg["message"]["knowledge_graph"]["edges"]
    assert edges["e1"]["sources"][0] is not edges["e2"]["sources"][0]


# --- remove_phantom_support_graphs -----------------------------------------


def test_phantom_strips_reference_to_missing_aux_graph():
    msg = _msg(
        edges={
            "e1": {
                "attributes": [
                    {
                        "attribute_type_id": "biolink:support_graphs",
                        "value": ["missing_aux"],
                    }
                ]
            }
        },
        aux={"real_aux": {"edges": []}},
    )
    remove_phantom_support_graphs(msg)
    assert msg["message"]["knowledge_graph"]["edges"]["e1"]["attributes"] == []


def test_phantom_keeps_valid_support_graph():
    attr = {"attribute_type_id": "biolink:support_graphs", "value": ["real_aux"]}
    msg = _msg(
        edges={"e1": {"attributes": [attr]}},
        aux={"real_aux": {"edges": []}},
    )
    remove_phantom_support_graphs(msg)
    assert msg["message"]["knowledge_graph"]["edges"]["e1"]["attributes"] == [attr]


def test_phantom_ignores_non_support_graph_attributes():
    attr = {"attribute_type_id": "biolink:other", "value": ["x"]}
    msg = _msg(edges={"e1": {"attributes": [attr]}}, aux={"a": {}})
    remove_phantom_support_graphs(msg)
    assert msg["message"]["knowledge_graph"]["edges"]["e1"]["attributes"] == [attr]


def test_phantom_noop_without_aux_graphs():
    msg = _msg(
        edges={
            "e1": {
                "attributes": [
                    {"attribute_type_id": "biolink:support_graphs", "value": ["x"]}
                ]
            }
        },
        aux=None,
    )
    # No auxiliary_graphs section at all -> nothing to validate against, no error.
    msg["message"]["auxiliary_graphs"] = None
    remove_phantom_support_graphs(msg)
    assert len(msg["message"]["knowledge_graph"]["edges"]["e1"]["attributes"]) == 1

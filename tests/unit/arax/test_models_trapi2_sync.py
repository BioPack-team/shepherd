"""Upstream trapi2.0's model fixes in the ARAX port
(docs/ARAX_TRAPI2_UPSTREAM_SYNC.md, item 40)."""

import pytest

from shepherd_utils.arax.openapi_server.models.attribute_constraint import (
    AttributeConstraint,
)
from shepherd_utils.arax.openapi_server.models.knowledge_graph import KnowledgeGraph
from shepherd_utils.arax.openapi_server.models.query_graph import QueryGraph

FDA = "biolink:highest_FDA_approval_status"


@pytest.mark.parametrize(
    "constraint",
    [
        {"id": FDA, "operator": "==", "value": "regular approval"},
        {"id": FDA, "name": None, "operator": "==", "value": "regular approval"},
    ],
)
def test_attribute_constraint_name_is_optional(constraint):
    """TRAPI 2.0: a constraint needs only id, operator and value."""
    qg = QueryGraph.from_dict(
        {
            "nodes": {
                "n0": {"categories": ["biolink:Drug"], "constraints": [constraint]}
            },
            "edges": {},
        }
    )
    parsed = qg.nodes["n0"].constraints[0]
    assert isinstance(parsed, AttributeConstraint)
    assert parsed.name is None
    assert parsed.to_dict() == {
        "id": FDA,
        "not": False,
        "operator": "==",
        "value": "regular approval",
    }


@pytest.mark.parametrize(
    "field,value",
    [("id", None), ("operator", None), ("value", None), ("operator", "!=")],
)
def test_attribute_constraint_required_fields_are_checked(field, value):
    constraint = {"id": FDA, "operator": "==", "value": "regular approval"}
    constraint[field] = value
    with pytest.raises(ValueError):
        AttributeConstraint.from_dict(constraint)


def test_knowledge_graph_edges_are_optional():
    """TRAPI 2.0: KnowledgeGraph.edges may be left out (or null)."""
    kg = KnowledgeGraph.from_dict({"nodes": {}, "edges": None})
    assert kg.edges is None
    assert kg.to_dict() == {"nodes": {}}
    assert KnowledgeGraph.from_dict({"nodes": {}}).edges is None
    with pytest.raises(ValueError):
        KnowledgeGraph.from_dict({"nodes": None})

"""D-25 fix: a TRAPI qnode given only by name is resolved to ids, as ARAXi's
add_qnode(name=...) resolves one. The parity case trapi_qnode_name runs it end
to end (test_query_parity.py)."""

import pytest

import shepherd_utils.arax.ARAX_query as arax_query
from shepherd_utils.arax.ARAX_query import ARAXQuery
from shepherd_utils.arax.ARAX_response import ARAXResponse

RESOLVED = {
    "type 2 diabetes mellitus": {"preferred_curie": "MONDO:0005148"},
    "metformin": {"preferred_curie": "CHEBI:6801"},
}


@pytest.fixture
def lookups(monkeypatch):
    calls = []

    def get_canonical_curies(self, curies=None, names=None, **kwargs):
        calls.append((curies, names))
        return {name: RESOLVED.get(name) for name in names}

    monkeypatch.setattr(
        arax_query.NodeSynonymizer, "__init__", lambda self, *a, **k: None
    )
    monkeypatch.setattr(
        arax_query.NodeSynonymizer, "get_canonical_curies", get_canonical_curies
    )
    return calls


def _resolve(nodes):
    araxq = ARAXQuery.__new__(ARAXQuery)
    araxq.response = ARAXResponse()
    message = {"query_graph": {"nodes": nodes}}
    araxq.resolve_qnode_names(message)
    return araxq.response, message["query_graph"]["nodes"]


def test_names_resolve_to_ids_in_one_lookup(lookups):
    response, nodes = _resolve(
        {
            "n0": {
                "name": "type 2 diabetes mellitus",
                "categories": ["biolink:Disease"],
            },
            "n1": {"name": "metformin"},
            "n2": {"categories": ["biolink:Gene"]},
        }
    )
    assert response.status == "OK"
    assert nodes["n0"]["ids"] == ["MONDO:0005148"]
    assert nodes["n0"]["categories"] == ["biolink:Disease"]
    assert nodes["n1"]["ids"] == ["CHEBI:6801"]
    assert "ids" not in nodes["n2"]
    # the call ARAXi's add_qnode makes, once for every name
    ((curies, names),) = lookups
    assert curies == names == ["metformin", "type 2 diabetes mellitus"]


def test_ids_win_over_a_name(lookups):
    response, nodes = _resolve({"n0": {"ids": ["CHEBI:1"], "name": "metformin"}})
    assert response.status == "OK"
    assert nodes["n0"]["ids"] == ["CHEBI:1"]
    assert lookups == []


def test_an_unresolvable_name_is_araxis_error(lookups):
    response, _ = _resolve({"n0": {"name": "no such thing"}})
    assert response.status == "ERROR"
    assert response.error_code == "UnresolvableNodeName"
    assert (
        response.message
        == "A node with name 'no such thing' is not in our knowledge graph"
    )

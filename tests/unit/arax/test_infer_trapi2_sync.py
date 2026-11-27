"""Upstream trapi2.0's Infer change in the ARAX port
(docs/ARAX_TRAPI2_UPSTREAM_SYNC.md, item 35; f7812633, #2922): the xDTD curies
are used as given, with no NodeSynonymizer (DEC-9). End to end, the infer
cases in test_query_parity.py cover it."""

import pandas as pd
import pytest

from shepherd_utils.arax import ARAX_infer
from shepherd_utils.arax.ARAX_response import ARAXResponse
from shepherd_utils.arax.NodeSynonymizer import node_synonymizer
from shepherd_utils.arax.openapi_server.models.message import Message
from shepherd_utils.arax.openapi_server.models.q_edge import QEdge
from shepherd_utils.arax.openapi_server.models.q_node import QNode
from shepherd_utils.arax.openapi_server.models.query_graph import QueryGraph
from shepherd_utils.arax.openapi_server.models.response import Response


class NoSynonymizer:
    def __init__(self, *_, **__):
        raise AssertionError("Infer must not use the NodeSynonymizer")


@pytest.fixture
def xdtd_calls(monkeypatch):
    calls = []

    class XDTD:
        def get_score_table(self, drug_curie_ids=None, disease_curie_ids=None):
            calls.append(("scores", drug_curie_ids, disease_curie_ids))
            return pd.DataFrame()

        def get_top_path(self, drug_curie_ids=None, disease_curie_ids=None):
            calls.append(("paths", drug_curie_ids, disease_curie_ids))
            return {}

    monkeypatch.setattr(ARAX_infer, "ExplainableDTD", XDTD)
    monkeypatch.setattr(node_synonymizer, "NodeSynonymizer", NoSynonymizer)
    monkeypatch.setattr(ARAX_infer, "NodeSynonymizer", NoSynonymizer, raising=False)
    return calls


def _response(query_graph=None):
    response = ARAXResponse()
    response.envelope = Response(message=Message(query_graph=query_graph))
    return response


def test_infer_has_no_synonymizer():
    assert not hasattr(ARAX_infer, "NodeSynonymizer")
    assert not hasattr(ARAX_infer.ARAXInfer, "_normalize_curie")


def test_curies_without_a_query_graph_are_used_as_given(xdtd_calls):
    response = ARAX_infer.ARAXInfer().apply(
        _response(),
        {
            "action": "drug_treatment_graph_expansion",
            "disease_curie": "DOID:9352",
        },
    )
    assert xdtd_calls == [
        ("scores", None, "DOID:9352"),
        ("paths", None, "DOID:9352"),
    ]
    logs = [m["message"] for m in response.messages]
    assert not any("ormalize" in line for line in logs)


def test_curies_in_a_query_graph_are_used_as_given(xdtd_calls):
    qg = QueryGraph(
        nodes={
            "n0": QNode(categories=["biolink:ChemicalEntity"]),
            "n1": QNode(ids=["DOID:9352"], categories=["biolink:Disease"]),
        },
        edges={"e0": QEdge(subject="n0", object="n1")},
    )
    ARAX_infer.ARAXInfer().apply(
        _response(qg),
        {
            "action": "drug_treatment_graph_expansion",
            "disease_curie": "DOID:9352",
            "qedge_id": "e0",
        },
    )
    assert xdtd_calls == [
        ("scores", None, "DOID:9352"),
        ("paths", None, "DOID:9352"),
    ]

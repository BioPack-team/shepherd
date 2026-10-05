"""Every edge the ARAX port creates is a valid TRAPI 2.0 Edge.

TRAPI 2.0 makes knowledge_level and agent_type required top-level Edge
properties (they were biolink:knowledge_level / biolink:agent_type attributes,
when set at all). These run the overlays that add virtual edges (NGD, Jaccard,
Fisher's exact test, COHD clinical info) with their data sources stubbed, and
Infer's xDTD on the query-parity data files, and check each created edge with
translator_tom (schema and semantic validation), its KL/AT values, and that no
KL/AT attribute is left behind. The values:

- NGD: statistical_association / automated_agent, upstream's own attributes.
- Jaccard, FET: upstream sets none; the same values as NGD, ARAX's other
  computed virtual edges (primary source infores:arax).
- COHD clinical info: upstream sets none; statistical_association /
  data_analysis_pipeline, what the primary source (infores:cohd, EHR
  co-occurrence statistics) asserts for its own edges.
- xDTD predicted treats edges: prediction / computational_model, upstream's
  attributes. xDTD explanation-path edges: the mapping DB's values.

The last test checks that filter_kg reads KL/AT from the top-level properties.
"""

import json
import os
import subprocess
import sys

import pytest
import translator_tom
from translator_tom.validation import semantic_validate

import shepherd_utils.arax.Overlay.overlay_utilities as ou
from shepherd_utils.arax.ARAX_response import ARAXResponse
from shepherd_utils.arax.Overlay import compute_jaccard, compute_ngd
from shepherd_utils.arax.Overlay import fisher_exact_test, overlay_clinical_info
from shepherd_utils.arax.openapi_server.models.analysis import Analysis
from shepherd_utils.arax.openapi_server.models.attribute import Attribute
from shepherd_utils.arax.openapi_server.models.edge import Edge
from shepherd_utils.arax.openapi_server.models.edge_binding import EdgeBinding
from shepherd_utils.arax.openapi_server.models.knowledge_graph import KnowledgeGraph
from shepherd_utils.arax.openapi_server.models.message import Message
from shepherd_utils.arax.openapi_server.models.node import Node
from shepherd_utils.arax.openapi_server.models.node_binding import NodeBinding
from shepherd_utils.arax.openapi_server.models.q_edge import QEdge
from shepherd_utils.arax.openapi_server.models.q_node import QNode
from shepherd_utils.arax.openapi_server.models.query_graph import QueryGraph
from shepherd_utils.arax.openapi_server.models.result import Result
from shepherd_utils.arax.openapi_server.models.retrieval_source import (
    RetrievalSource,
)

KL_AT_ATTRIBUTES = {"biolink:knowledge_level", "biolink:agent_type"}
# Upstream's virtual-edge predicates that are not Biolink predicates (as in
# TRAPI 1.x; DEC-1 keeps them): the only semantic errors tolerated here
NON_BIOLINK_PREDICATES = {
    "biolink:has_jaccard_index_with",
    "biolink:has_fisher_exact_test_p_value_with",
}


def assert_valid_edges(kg_dict, edge_keys, knowledge_level, agent_type):
    """The edges are valid 2.0 edges in a valid 2.0 KG, with these KL/AT."""
    assert edge_keys
    kg = translator_tom.KnowledgeGraph.from_dict(kg_dict)
    _, errors = semantic_validate(kg)
    tolerated = {
        f"Predicate `{p}` is not a valid BioLink predicate."
        for p in NON_BIOLINK_PREDICATES
    }
    assert [e.message for e in errors if e.message not in tolerated] == []
    for key in edge_keys:
        edge = kg_dict["edges"][key]
        translator_tom.Edge.from_dict(edge)
        assert (edge["knowledge_level"], edge["agent_type"]) == (
            knowledge_level,
            agent_type,
        ), key
        assert not {a["attribute_type_id"] for a in edge["attributes"]} & (
            KL_AT_ATTRIBUTES
        )
        assert edge["sources"]
        assert edge.get("qualifiers", None) != []


def kg_edge(subject, object, qedge_key):
    edge = Edge(
        subject=subject,
        object=object,
        predicate="biolink:related_to",
        attributes=[],
        sources=[
            RetrievalSource(
                resource_id="infores:ctd", resource_role="primary_knowledge_source"
            )
        ],
        knowledge_level="knowledge_assertion",
        agent_type="manual_agent",
    )
    edge.qedge_keys = [qedge_key]
    return edge


def kg_node(category, qnode_key, name):
    node = Node(name=name, categories=[category], attributes=[])
    node.qnode_keys = [qnode_key]
    return node


def two_hop_message():
    """n0 (a drug) -e0- n1 (genes) -e1- n2 (diseases), resultified."""
    qg = QueryGraph(
        nodes={
            "n0": QNode(ids=["CHEBI:1"], categories=["biolink:SmallMolecule"]),
            "n1": QNode(categories=["biolink:Gene"]),
            "n2": QNode(categories=["biolink:Disease"]),
        },
        edges={
            "e0": QEdge(subject="n0", object="n1"),
            "e1": QEdge(subject="n1", object="n2"),
        },
    )
    nodes = {"CHEBI:1": kg_node("biolink:SmallMolecule", "n0", "drug")}
    edges = {}
    for g in (1, 2, 3):
        nodes[f"G:{g}"] = kg_node("biolink:Gene", "n1", f"gene {g}")
        edges[f"e0_{g}"] = kg_edge("CHEBI:1", f"G:{g}", "e0")
    for d in (1, 2):
        nodes[f"D:{d}"] = kg_node("biolink:Disease", "n2", f"disease {d}")
    for g, d in ((1, 1), (2, 1), (3, 2)):
        edges[f"e1_{g}_{d}"] = kg_edge(f"G:{g}", f"D:{d}", "e1")
    results = [
        Result(
            node_bindings={
                "n0": NodeBinding(ids=["CHEBI:1"]),
                "n1": NodeBinding(ids=[f"G:{g}"]),
                "n2": NodeBinding(ids=[f"D:{d}"]),
            },
            analyses=[
                Analysis(
                    resource_id="infores:arax",
                    edge_bindings={
                        "e0": EdgeBinding(ids=[f"e0_{g}"]),
                        "e1": EdgeBinding(ids=[f"e1_{g}_{d}"]),
                    },
                )
            ],
        )
        for g, d in ((1, 1), (2, 1), (3, 2))
    ]
    return Message(
        query_graph=qg,
        knowledge_graph=KnowledgeGraph(nodes=nodes, edges=edges),
        results=results,
    )


def new_edge_keys(message, before):
    return sorted(set(message.knowledge_graph.edges) - before)


def assert_results_valid(message):
    for result in message.results:
        translator_tom.Result.from_dict(result.to_dict())


def test_jaccard_virtual_edges():
    message = two_hop_message()
    before = set(message.knowledge_graph.edges)
    response = ARAXResponse()
    compute_jaccard.ComputeJaccard(
        response,
        message,
        {
            "start_node_key": "n0",
            "intermediate_node_key": "n1",
            "end_node_key": "n2",
            "virtual_relation_label": "J1",
        },
    ).compute_jaccard()
    assert response.status == "OK", response.show()
    created = new_edge_keys(message, before)
    assert_valid_edges(
        message.knowledge_graph.to_dict(),
        created,
        "statistical_association",
        "automated_agent",
    )


@pytest.fixture
def ngd(monkeypatch):
    cls = compute_ngd.ComputeNGD
    monkeypatch.setattr(cls, "_setup_ngd_database", lambda self: (None, None))
    monkeypatch.setattr(cls, "_close_database", lambda self: None, raising=False)
    monkeypatch.setattr(
        cls, "_get_canonical_curies_map", lambda self, curies: {c: c for c in curies}
    )
    monkeypatch.setattr(cls, "load_curie_to_pmids_data", lambda self, curies: None)
    monkeypatch.setattr(
        cls, "calculate_ngd_fast", lambda self, s, o: (0.25, {12345, 67890})
    )
    # which node pairs to overlay comes from Resultify (not what is tested here)
    pairs = {
        ("n0", "n1"): {("CHEBI:1", "G:1"), ("CHEBI:1", "G:2"), ("CHEBI:1", "G:3")},
        ("n1", "n2"): {("G:1", "D:1"), ("G:2", "D:1"), ("G:3", "D:2")},
    }
    monkeypatch.setattr(
        ou, "get_node_pairs_to_overlay", lambda s, o, qg, kg, log: pairs[(s, o)]
    )
    return cls


@pytest.mark.parametrize("with_qnode_keys", [True, False])
def test_ngd_virtual_edges(ngd, with_qnode_keys):
    message = two_hop_message()
    before = set(message.knowledge_graph.edges)
    parameters = {"default_value": "inf", "virtual_relation_label": "N1"}
    if with_qnode_keys:
        parameters.update(subject_qnode_key="n1", object_qnode_key="n2")
    response = ARAXResponse()
    ngd(response, message, parameters).compute_ngd()
    assert response.status == "OK", response.show()
    created = new_edge_keys(message, before)
    assert_valid_edges(
        message.knowledge_graph.to_dict(),
        created,
        "statistical_association",
        "automated_agent",
    )
    # bound into the results: unioned into each result's EdgeBinding for the
    # qedge between the overlaid qnodes (both qedges without qnode keys)
    for result in message.results:
        edge_bindings = result.analyses[0].edge_bindings
        for qedge_key in ("e1",) if with_qnode_keys else ("e0", "e1"):
            _, *overlay_ids = edge_bindings[qedge_key].ids
            assert len(overlay_ids) == 1 and overlay_ids[0] in created
        if with_qnode_keys:
            assert len(edge_bindings["e0"].ids) == 1
    assert_results_valid(message)


def test_fisher_exact_test_virtual_edges(monkeypatch, tmp_path):
    class Synonymizer:
        def get_canonical_curies(self, curie):
            return {curie: None}

    sqlite = tmp_path / "tier0.sqlite"
    sqlite.write_bytes(b"")
    monkeypatch.setattr(fisher_exact_test, "NodeSynonymizer", Synonymizer)
    cls = fisher_exact_test.ComputeFTEST
    monkeypatch.setattr(
        cls,
        "query_size_of_adjacent_nodes",
        lambda self, node_curie, **kw: ({curie: 20 for curie in node_curie}, []),
    )
    monkeypatch.setattr(cls, "size_of_given_type_in_KP", lambda self, node_type: 5000)
    message = two_hop_message()
    before = set(message.knowledge_graph.edges)
    response = ARAXResponse()
    fet = cls(
        response,
        message,
        {
            "subject_qnode_key": "n1",
            "object_qnode_key": "n2",
            "virtual_relation_label": "F1",
        },
    )
    fet.sqlite_file_path = sqlite
    fet.fisher_exact_test()
    assert response.status == "OK", response.show()
    created = new_edge_keys(message, before)
    assert_valid_edges(
        message.knowledge_graph.to_dict(),
        created,
        "statistical_association",
        "automated_agent",
    )
    assert_results_valid(message)


def test_clinical_info_virtual_edges(monkeypatch):
    class COHDIndex:
        def get_concept_ids(self, curies):
            return {curie: [1] for curie in curies}

    monkeypatch.setattr(overlay_clinical_info, "COHDIndex", COHDIndex)
    monkeypatch.setattr(overlay_clinical_info, "get_biolink_helper", lambda: None)
    monkeypatch.setattr(
        overlay_clinical_info.OverlayClinicalInfo,
        "make_edge_attribute_from_curies",
        lambda self, s, o, **kw: Attribute(
            attribute_type_id="EDAM-DATA:0951",
            original_attribute_name=kw["name"],
            value="0.5",
        ),
    )
    message = two_hop_message()
    before = set(message.knowledge_graph.edges)
    response = ARAXResponse()
    overlay_clinical_info.OverlayClinicalInfo(
        response,
        message,
        {
            "subject_qnode_key": "n1",
            "object_qnode_key": "n2",
            "virtual_relation_label": "C1",
        },
    ).add_virtual_edge(name="paired_concept_frequency", default=0)
    assert response.status == "OK", response.show()
    created = new_edge_keys(message, before)
    assert_valid_edges(
        message.knowledge_graph.to_dict(),
        created,
        "statistical_association",
        "data_analysis_pipeline",
    )
    assert_results_valid(message)


QP = os.path.join(os.path.dirname(os.path.abspath(__file__)), "query_parity")
INFER_CASES = ["dsl_infer_no_qg", "dsl_infer_with_qg", "creative_treats"]


@pytest.fixture(scope="module")
def infer_outputs(tmp_path_factory):
    """Infer's xDTD on the query-parity data files (its ExplainableDTD and
    mapping tables), through the whole port as the parity test runs it."""
    out_path = tmp_path_factory.mktemp("infer") / "port.json"
    proc = subprocess.run(
        [sys.executable, os.path.join(QP, "run_port.py"), str(out_path), *INFER_CASES],
        env=dict(os.environ, PYTHONHASHSEED="0"),
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    with open(out_path) as f:
        return json.load(f)


@pytest.mark.parametrize("case", INFER_CASES)
def test_infer_edges(infer_outputs, case):
    rec = infer_outputs[case]
    assert rec["status"] == "OK", rec["message"]
    kg = rec["envelope"]["message"]["knowledge_graph"]
    predicted = [k for k in kg["edges"] if k.startswith("creative_DTD_prediction_")]
    assert_valid_edges(kg, predicted, "prediction", "computational_model")
    for key, edge in kg["edges"].items():
        translator_tom.Edge.from_dict(edge)
        assert not {a["attribute_type_id"] for a in edge["attributes"]} & (
            KL_AT_ATTRIBUTES
        ), key
        assert edge.get("qualifiers", None) != []


@pytest.mark.parametrize(
    "edge_attribute, value",
    [
        ("biolink:knowledge_level", "prediction"),  # the 1.x attribute name
        ("knowledge_level", "prediction"),  # the 2.0 Edge property
        ("biolink:agent_type", "computational_model"),
        ("agent_type", "computational_model"),
    ],
)
def test_filter_kg_reads_top_level_knowledge_level_and_agent_type(
    edge_attribute, value
):
    """filter_kg(remove_edges_by_discrete_attribute) on KL/AT reads the
    top-level Edge properties (1.x read biolink:* attributes)."""
    from shepherd_utils.arax.ARAX_filter_kg import ARAXFilterKG

    message = two_hop_message()
    predicted = message.knowledge_graph.edges["e1_3_2"]
    predicted.knowledge_level = "prediction"
    predicted.agent_type = "computational_model"
    response = ARAXResponse()
    response.envelope = type("Envelope", (), {"message": message})()
    ARAXFilterKG().apply(
        response,
        {
            "action": "remove_edges_by_discrete_attribute",
            "edge_attribute": edge_attribute,
            "value": value,
        },
    )
    assert response.status == "OK", response.show()
    assert "e1_3_2" not in message.knowledge_graph.edges
    assert len(message.knowledge_graph.edges) == 5

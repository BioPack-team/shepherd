"""ARAX's own Resultify tests, run against the Shepherd port.

Copied from RTXteam/RTX @ 9485431, code/ARAX/test/test_ARAX_resultify.py:
every test that runs offline (no live KPs / ARAXQuery), unchanged apart from
imports and the envelope setup in ``_run_resultify_directly`` (noted inline).
"""

from typing import List, Dict, Tuple, Set, Iterable

import pytest

from shepherd_utils.arax import ARAX_resultify
from shepherd_utils.arax.ARAX_response import ARAXResponse
from shepherd_utils.arax.ARAX_resultify import ARAXResultify
from shepherd_utils.arax.actions_parser import ActionsParser
from shepherd_utils.arax.openapi_server.models.edge import Edge
from shepherd_utils.arax.openapi_server.models.node import Node
from shepherd_utils.arax.openapi_server.models.q_edge import QEdge
from shepherd_utils.arax.openapi_server.models.q_node import QNode
from shepherd_utils.arax.openapi_server.models.query_graph import QueryGraph
from shepherd_utils.arax.openapi_server.models.knowledge_graph import KnowledgeGraph
from shepherd_utils.arax.openapi_server.models.result import Result
from shepherd_utils.arax.openapi_server.models.message import Message
from shepherd_utils.arax.openapi_server.models.response import Response
from shepherd_utils.arax.openapi_server.models.retrieval_source import RetrievalSource

DIABETES_CURIE = "MONDO:0005015"
TYPE_1_DIABETES_CURIE = "MONDO:0005147"
INSULIN_CURIE = "CHEBI:5931"
HEART_DISEASE_CURIE = "MONDO:0005267"


def _slim_kg(kg: KnowledgeGraph) -> KnowledgeGraph:
    slimmed_nodes = {
        node_key: Node(
            categories=node.categories, name=node.name, qnode_keys=node.qnode_keys
        )
        for node_key, node in kg.nodes.items()
    }
    slimmed_edges = {
        edge_key: Edge(
            subject=edge.subject,
            object=edge.object,
            predicate=edge.predicate,
            qedge_keys=edge.qedge_keys,
        )
        for edge_key, edge in kg.edges.items()
    }
    return KnowledgeGraph(nodes=slimmed_nodes, edges=slimmed_edges)


def _create_nodes(kg_node_info: Iterable[Dict[str, any]]) -> Dict[str, Node]:
    nodes_dict = dict()
    for kg_node in kg_node_info:
        node = Node(categories=kg_node.get("categories"), name=kg_node.get("name"))
        node.qnode_keys = kg_node["qnode_keys"]
        nodes_dict[kg_node["node_key"]] = node
    return nodes_dict


def _create_edges(kg_edge_info: Iterable[Dict[str, any]]) -> Dict[str, Edge]:
    edges_dict = dict()
    for kg_edge in kg_edge_info:
        edge = Edge(
            subject=kg_edge["subject"],
            object=kg_edge["object"],
            predicate=kg_edge.get("predicate", "biolink:related_to"),
            sources=[
                RetrievalSource(
                    resource_id="infores:arax",
                    resource_role="aggregator_knowledge_source",
                )
            ],
        )
        edge.qedge_keys = kg_edge["qedge_keys"]
        edges_dict[kg_edge["edge_key"]] = edge
    return edges_dict


def _create_qnodes(qg_node_info: Iterable[Dict[str, any]]) -> Dict[str, QNode]:
    return {
        qnode_info["node_key"]: QNode(
            categories=qnode_info["categories"], is_set=qnode_info["is_set"]
        )
        for qnode_info in qg_node_info
    }


def _create_qedges(qg_edge_info: Iterable[Dict[str, any]]) -> Dict[str, QEdge]:
    return {
        qedge_info["edge_key"]: QEdge(
            subject=qedge_info["subject"], object=qedge_info["object"]
        )
        for qedge_info in qg_edge_info
    }


def _print_results_for_debug(message: Message):
    print()
    qg = message.query_graph
    kg = message.knowledge_graph
    for result in message.results:
        print(result.essence)
        for qnode_key, node_bindings_list in result.node_bindings.items():
            qnode = qg.nodes[qnode_key]
            print(
                f"  qnode {qnode_key}{f' (option group {qnode.option_group_id})' if qnode.option_group_id else ''}:"
            )
            for node_binding in node_bindings_list:
                print(f"    {node_binding.id} {kg.nodes[node_binding.id].name}")
        for qedge_key, edge_bindings_list in result.analyses[0].edge_bindings.items():
            qedge = qg.edges[qedge_key]
            print(
                f"  qedge {qedge_key}{f' (option group {qedge.option_group_id})' if qedge.option_group_id else ''}:"
            )
            for edge_binding in edge_bindings_list:
                print(f"    {edge_binding.id}")
    # Display the query graph
    import graphviz

    dot = graphviz.Digraph(comment="QG")
    for qnode_key, qnode in qg.nodes.items():
        node_id_line = f"{qnode_key}{f' (group {qnode.option_group_id})' if qnode.option_group_id else ''}"
        if qnode.ids:
            node_details_line = ", ".join(qnode.ids)
        elif qnode.categories:
            node_details_line = ", ".join(qnode.categories)
        else:
            node_details_line = ""
        dot.node(qnode_key, f"{node_id_line}\n{node_details_line}")
    for qedge_key, qedge in qg.edges.items():
        dot.edge(
            qedge.subject,
            qedge.object,
            label=f"{qedge_key}{f' (NOT)' if qedge.exclude else ''}{f' (group {qedge.option_group_id})' if qedge.option_group_id else ''}\n{', '.join(qedge.predicates) if qedge.predicates else ''}",
        )
    dot.render("qg.gv", view=True)


def _get_result_node_keys_by_qg_key(result: Result) -> Dict[str, Set[str]]:
    return {
        qnode_key: {node_binding.id for node_binding in result.node_bindings[qnode_key]}
        for qnode_key in result.node_bindings
    }


def _get_result_edge_keys_by_qg_key(result: Result) -> Dict[str, Set[str]]:
    return {
        qedge_key: {
            edge_binding.id
            for edge_binding in result.analyses[0].edge_bindings[qedge_key]
        }
        for qedge_key in result.analyses[0].edge_bindings
    }


def _run_resultify_directly(
    query_graph: QueryGraph,
    knowledge_graph: KnowledgeGraph,
    ignore_edge_direction=True,
    debug=False,
) -> Tuple[ARAXResponse, Message]:
    response = ARAXResponse()
    # Upstream uses ARAXMessenger().create_envelope(response); the messenger is
    # not ported yet, and Resultify only needs response.envelope.message.
    response.envelope = Response(message=Message())
    actions_parser = ActionsParser()
    actions_list = [f"resultify(ignore_edge_direction={ignore_edge_direction})"]
    result = actions_parser.parse(actions_list)
    response.merge(result)
    actions = result.data["actions"]
    assert result.status == "OK"
    resultifier = ARAXResultify()
    message_original = Message(
        query_graph=query_graph, knowledge_graph=knowledge_graph, results=[]
    )
    # Upstream: ARAXMessenger().from_dict(...), which only listifies scalar
    # predicates/categories/ids (none here) before Message.from_dict.
    message = Message.from_dict(message_original.to_dict())
    # qnode_keys/qedge_keys are lost when grabbing message from_dict() - so we add them back
    for node_key, node in message_original.knowledge_graph.nodes.items():
        message.knowledge_graph.nodes[node_key].qnode_keys = node.qnode_keys
    for edge_key, edge in message_original.knowledge_graph.edges.items():
        message.knowledge_graph.edges[edge_key].qedge_keys = edge.qedge_keys
    response.envelope.message = message
    parameters = actions[0]["parameters"]
    parameters["debug"] = "true"
    resultifier.apply(response, parameters)
    if response.status != "OK":
        if debug:
            _print_results_for_debug(message)
        print(response.show(level=response.DEBUG))
    return response, message


def _convert_shorthand_to_qg(
    shorthand_qnodes: Dict[str, str], shorthand_qedges: Dict[str, str]
) -> QueryGraph:
    return QueryGraph(
        nodes={
            qnode_key: QNode(is_set=bool(is_set))
            for qnode_key, is_set in shorthand_qnodes.items()
        },
        edges={
            qedge_key: QEdge(
                subject=qnodes.split("--")[0], object=qnodes.split("--")[1]
            )
            for qedge_key, qnodes in shorthand_qedges.items()
        },
    )


def _convert_shorthand_to_kg(
    shorthand_nodes: Dict[str, List[str]], shorthand_edges: Dict[str, List[str]]
) -> KnowledgeGraph:
    nodes_dict = {}
    for qnode_key, nodes_list in shorthand_nodes.items():
        for node_key in nodes_list:
            node = nodes_dict.get(node_key, Node())
            if not hasattr(node, "qnode_keys"):
                node.qnode_keys = []
            node.qnode_keys.append(qnode_key)
            nodes_dict[node_key] = node
    edges_dict = {}
    for qedge_key, edges_list in shorthand_edges.items():
        for edge_key in edges_list:
            source_node_key = edge_key.split("--")[0]
            target_node_key = edge_key.split("--")[1]
            edge = edges_dict.get(
                edge_key,
                Edge(
                    subject=source_node_key,
                    object=target_node_key,
                    predicate="biolink:related_to",
                    sources=[
                        RetrievalSource(
                            resource_id="infores:arax",
                            resource_role="aggregator_knowledge_source",
                        )
                    ],
                ),
            )
            if not hasattr(edge, "qedge_keys"):
                edge.qedge_keys = []
            edge.qedge_keys.append(qedge_key)
            edges_dict[f"{qedge_key}:{edge_key}"] = edge
    return KnowledgeGraph(nodes=nodes_dict, edges=edges_dict)


def _get_kg_edge_keys_using_node(node_key: str, kg: KnowledgeGraph) -> Set[str]:
    return {
        edge_key
        for edge_key, edge in kg.edges.items()
        if node_key in {edge.subject, edge.object}
    }


def test01():
    kg_node_info = (
        {"node_key": "UniProtKB:12345", "categories": "protein", "qnode_keys": ["n01"]},
        {"node_key": "UniProtKB:23456", "categories": "protein", "qnode_keys": ["n01"]},
        {
            "node_key": "DOID:12345",
            "categories": "disease",
            "qnode_keys": ["DOID:12345"],
        },
        {
            "node_key": "HP:56789",
            "categories": "phenotypic_feature",
            "qnode_keys": ["n02"],
        },
        {
            "node_key": "HP:67890",
            "categories": "phenotypic_feature",
            "qnode_keys": ["n02"],
        },
        {
            "node_key": "HP:34567",
            "categories": "phenotypic_feature",
            "qnode_keys": ["n02"],
        },
    )

    kg_edge_info = (
        {
            "edge_key": "ke01",
            "subject": "UniProtKB:12345",
            "object": "DOID:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke02",
            "subject": "UniProtKB:23456",
            "object": "DOID:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke03",
            "subject": "DOID:12345",
            "object": "HP:56789",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke04",
            "subject": "DOID:12345",
            "object": "HP:67890",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke05",
            "subject": "DOID:12345",
            "object": "HP:34567",
            "qedge_keys": ["qe02"],
        },
    )

    kg_nodes = _create_nodes(kg_node_info)
    kg_edges = _create_edges(kg_edge_info)

    knowledge_graph = KnowledgeGraph(kg_nodes, kg_edges)

    qg_node_info = (
        {"node_key": "n01", "categories": "protein", "is_set": False},
        {"node_key": "DOID:12345", "categories": "disease", "is_set": False},
        {"node_key": "n02", "categories": "phenotypic_feature", "is_set": True},
    )

    qg_edge_info = (
        {"edge_key": "qe01", "subject": "n01", "object": "DOID:12345"},
        {"edge_key": "qe02", "subject": "DOID:12345", "object": "n02"},
    )

    qg_nodes = _create_qnodes(qg_node_info)
    qg_edges = _create_qedges(qg_edge_info)
    query_graph = QueryGraph(qg_nodes, qg_edges)

    results_list = ARAX_resultify._get_results_for_kg_by_qg(
        knowledge_graph, query_graph
    )

    assert len(results_list) == 2


def test02():
    kg_node_info = (
        {"node_key": "UniProtKB:12345", "categories": "protein", "qnode_keys": ["n01"]},
        {"node_key": "UniProtKB:23456", "categories": "protein", "qnode_keys": ["n01"]},
        {
            "node_key": "DOID:12345",
            "categories": "disease",
            "qnode_keys": ["DOID:12345"],
        },
        {
            "node_key": "HP:56789",
            "categories": "phenotypic_feature",
            "qnode_keys": ["n02"],
        },
        {
            "node_key": "HP:67890",
            "categories": "phenotypic_feature",
            "qnode_keys": ["n02"],
        },
        {
            "node_key": "HP:34567",
            "categories": "phenotypic_feature",
            "qnode_keys": ["n02"],
        },
    )

    kg_edge_info = (
        {
            "edge_key": "ke01",
            "subject": "UniProtKB:12345",
            "object": "DOID:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke02",
            "subject": "UniProtKB:23456",
            "object": "DOID:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke03",
            "subject": "DOID:12345",
            "object": "HP:56789",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke04",
            "subject": "DOID:12345",
            "object": "HP:67890",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke05",
            "subject": "DOID:12345",
            "object": "HP:34567",
            "qedge_keys": ["qe02"],
        },
    )

    kg_nodes = _create_nodes(kg_node_info)
    kg_edges = _create_edges(kg_edge_info)

    knowledge_graph = KnowledgeGraph(kg_nodes, kg_edges)

    qg_node_info = (
        {"node_key": "n01", "categories": "protein", "is_set": None},
        {"node_key": "DOID:12345", "categories": "disease", "is_set": False},
        {"node_key": "n02", "categories": "phenotypic_feature", "is_set": True},
    )

    qg_edge_info = (
        {"edge_key": "qe01", "subject": "n01", "object": "DOID:12345"},
        {"edge_key": "qe02", "subject": "DOID:12345", "object": "n02"},
    )

    qg_nodes = _create_qnodes(qg_node_info)
    qg_edges = _create_qedges(qg_edge_info)
    query_graph = QueryGraph(qg_nodes, qg_edges)

    results_list = ARAX_resultify._get_results_for_kg_by_qg(
        knowledge_graph, query_graph
    )
    assert len(results_list) == 2


def test03():
    kg_node_info = (
        {"node_key": "UniProtKB:12345", "categories": "protein", "qnode_keys": ["n01"]},
        {"node_key": "UniProtKB:23456", "categories": "protein", "qnode_keys": ["n01"]},
        {
            "node_key": "DOID:12345",
            "categories": "disease",
            "qnode_keys": ["DOID:12345"],
        },
        {
            "node_key": "HP:56789",
            "categories": "phenotypic_feature",
            "qnode_keys": ["n02"],
        },
        {
            "node_key": "HP:67890",
            "categories": "phenotypic_feature",
            "qnode_keys": ["n02"],
        },
        {
            "node_key": "HP:34567",
            "categories": "phenotypic_feature",
            "qnode_keys": ["n02"],
        },
    )

    kg_edge_info = (
        {
            "edge_key": "ke01",
            "subject": "DOID:12345",
            "object": "UniProtKB:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke02",
            "subject": "UniProtKB:23456",
            "object": "DOID:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke03",
            "subject": "DOID:12345",
            "object": "HP:56789",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke04",
            "subject": "DOID:12345",
            "object": "HP:67890",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke05",
            "subject": "DOID:12345",
            "object": "HP:34567",
            "qedge_keys": ["qe02"],
        },
    )

    kg_nodes = _create_nodes(kg_node_info)
    kg_edges = _create_edges(kg_edge_info)

    knowledge_graph = KnowledgeGraph(kg_nodes, kg_edges)

    qg_node_info = (
        {"node_key": "n01", "categories": "protein", "is_set": None},
        {"node_key": "DOID:12345", "categories": "disease", "is_set": False},
        {"node_key": "n02", "categories": "phenotypic_feature", "is_set": True},
    )

    qg_edge_info = (
        {"edge_key": "qe01", "subject": "n01", "object": "DOID:12345"},
        {"edge_key": "qe02", "subject": "DOID:12345", "object": "n02"},
    )

    qg_nodes = _create_qnodes(qg_node_info)
    qg_edges = _create_qedges(qg_edge_info)
    query_graph = QueryGraph(qg_nodes, qg_edges)

    results_list = ARAX_resultify._get_results_for_kg_by_qg(
        knowledge_graph, query_graph, ignore_edge_direction=True
    )
    assert len(results_list) == 2


def test04():
    kg_node_info = (
        {"node_key": "UniProtKB:12345", "categories": "protein", "qnode_keys": ["n01"]},
        {"node_key": "UniProtKB:23456", "categories": "protein", "qnode_keys": ["n01"]},
        {
            "node_key": "DOID:12345",
            "categories": "disease",
            "qnode_keys": ["DOID:12345"],
        },
        {"node_key": "UniProtKB:56789", "categories": "protein", "qnode_keys": ["n01"]},
        {
            "node_key": "ChEMBL.COMPOUND:12345",
            "categories": "chemical_substance",
            "qnode_keys": ["n02"],
        },
        {
            "node_key": "ChEMBL.COMPOUND:23456",
            "categories": "chemical_substance",
            "qnode_keys": ["n02"],
        },
    )

    kg_edge_info = (
        {
            "edge_key": "ke01",
            "subject": "ChEMBL.COMPOUND:12345",
            "object": "UniProtKB:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke02",
            "subject": "ChEMBL.COMPOUND:12345",
            "object": "UniProtKB:23456",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke03",
            "subject": "ChEMBL.COMPOUND:23456",
            "object": "UniProtKB:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke04",
            "subject": "ChEMBL.COMPOUND:23456",
            "object": "UniProtKB:23456",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke05",
            "subject": "DOID:12345",
            "object": "UniProtKB:12345",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke06",
            "subject": "DOID:12345",
            "object": "UniProtKB:23456",
            "qedge_keys": ["qe02"],
        },
    )

    kg_nodes = _create_nodes(kg_node_info)
    kg_edges = _create_edges(kg_edge_info)

    knowledge_graph = KnowledgeGraph(kg_nodes, kg_edges)

    qg_node_info = (
        {"node_key": "n01", "categories": "protein", "is_set": True},
        {"node_key": "DOID:12345", "categories": "disease", "is_set": False},
        {"node_key": "n02", "categories": "chemical_substance", "is_set": False},
    )

    qg_edge_info = (
        {"edge_key": "qe01", "subject": "n02", "object": "n01"},
        {"edge_key": "qe02", "subject": "DOID:12345", "object": "n01"},
    )

    qg_nodes = _create_qnodes(qg_node_info)
    qg_edges = _create_qedges(qg_edge_info)
    query_graph = QueryGraph(qg_nodes, qg_edges)

    results_list = ARAX_resultify._get_results_for_kg_by_qg(
        knowledge_graph, query_graph, ignore_edge_direction=True
    )
    assert len(results_list) == 2


def test05():
    kg_node_info = (
        {"node_key": "UniProtKB:12345", "categories": "protein", "qnode_keys": ["n01"]},
        {"node_key": "UniProtKB:23456", "categories": "protein", "qnode_keys": ["n01"]},
        {
            "node_key": "DOID:12345",
            "categories": "disease",
            "qnode_keys": ["DOID:12345"],
        },
        {"node_key": "UniProtKB:56789", "categories": "protein", "qnode_keys": ["n01"]},
        {
            "node_key": "ChEMBL.COMPOUND:12345",
            "categories": "chemical_substance",
            "qnode_keys": ["n02"],
        },
        {
            "node_key": "ChEMBL.COMPOUND:23456",
            "categories": "chemical_substance",
            "qnode_keys": ["n02"],
        },
    )

    kg_edge_info = (
        {
            "edge_key": "ke01",
            "subject": "ChEMBL.COMPOUND:12345",
            "object": "UniProtKB:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke02",
            "subject": "ChEMBL.COMPOUND:12345",
            "object": "UniProtKB:23456",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke03",
            "subject": "ChEMBL.COMPOUND:23456",
            "object": "UniProtKB:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke04",
            "subject": "ChEMBL.COMPOUND:23456",
            "object": "UniProtKB:23456",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke05",
            "subject": "DOID:12345",
            "object": "UniProtKB:12345",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke06",
            "subject": "DOID:12345",
            "object": "UniProtKB:23456",
            "qedge_keys": ["qe02"],
        },
    )

    kg_nodes = _create_nodes(kg_node_info)
    kg_edges = _create_edges(kg_edge_info)
    knowledge_graph = KnowledgeGraph(kg_nodes, kg_edges)

    qg_node_info = (
        {"node_key": "n01", "categories": "protein", "is_set": True},
        {"node_key": "DOID:12345", "categories": "disease", "is_set": False},
        {"node_key": "n02", "categories": "chemical_substance", "is_set": False},
    )

    qg_edge_info = (
        {"edge_key": "qe01", "subject": "n02", "object": "n01"},
        {"edge_key": "qe02", "subject": "DOID:12345", "object": "n01"},
    )

    qg_nodes = _create_qnodes(qg_node_info)
    qg_edges = _create_qedges(qg_edge_info)
    query_graph = QueryGraph(qg_nodes, qg_edges)

    response, message = _run_resultify_directly(
        query_graph, knowledge_graph, ignore_edge_direction=True
    )
    assert response.status == "OK"
    assert len(message.results) == 2


def test07():
    kg_node_info = (
        {"node_key": "UniProtKB:12345", "categories": "protein", "qnode_keys": ["n01"]},
        {"node_key": "UniProtKB:23456", "categories": "protein", "qnode_keys": ["n01"]},
        {
            "node_key": "DOID:12345",
            "categories": "disease",
            "qnode_keys": ["DOID:12345"],
        },
        {"node_key": "UniProtKB:56789", "categories": "protein", "qnode_keys": ["n01"]},
        {
            "node_key": "ChEMBL.COMPOUND:12345",
            "categories": "chemical_substance",
            "qnode_keys": ["n02"],
        },
        {
            "node_key": "ChEMBL.COMPOUND:23456",
            "categories": "chemical_substance",
            "qnode_keys": ["n02"],
        },
    )

    kg_edge_info = (
        {
            "edge_key": "ke01",
            "subject": "ChEMBL.COMPOUND:12345",
            "object": "UniProtKB:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke02",
            "subject": "ChEMBL.COMPOUND:12345",
            "object": "UniProtKB:23456",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke03",
            "subject": "ChEMBL.COMPOUND:23456",
            "object": "UniProtKB:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke04",
            "subject": "ChEMBL.COMPOUND:23456",
            "object": "UniProtKB:23456",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke05",
            "subject": "DOID:12345",
            "object": "UniProtKB:12345",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke06",
            "subject": "DOID:12345",
            "object": "UniProtKB:23456",
            "qedge_keys": ["qe02"],
        },
    )

    kg_nodes = _create_nodes(kg_node_info)
    kg_edges = _create_edges(kg_edge_info)

    knowledge_graph = KnowledgeGraph(kg_nodes, kg_edges)

    qg_node_info = (
        {"node_key": "n01", "categories": "protein", "is_set": True},
        {"node_key": "DOID:12345", "categories": "disease", "is_set": False},
        {"node_key": "n02", "categories": "chemical_substance", "is_set": False},
    )

    qg_edge_info = (
        {"edge_key": "qe01", "subject": "n02", "object": "n01"},
        {"edge_key": "qe02", "subject": "DOID:12345", "object": "n01"},
    )

    qg_nodes = _create_qnodes(qg_node_info)
    qg_edges = _create_qedges(qg_edge_info)
    query_graph = QueryGraph(qg_nodes, qg_edges)

    response, message = _run_resultify_directly(
        query_graph, knowledge_graph, ignore_edge_direction=True
    )
    assert len(message.results) == 2
    assert response.status == "OK"


def test08():
    shorthand_qnodes = {"n00": "", "n01": ""}
    shorthand_qedges = {"e00": "n00--n01"}
    query_graph = _convert_shorthand_to_qg(shorthand_qnodes, shorthand_qedges)
    shorthand_kg_nodes = {
        "n00": ["DOID:731"],
        "n01": ["HP:01", "HP:02", "HP:03", "HP:04"],
    }
    shorthand_kg_edges = {
        "e00": [
            "DOID:731--HP:01",
            "DOID:731--HP:02",
            "DOID:731--HP:03",
            "DOID:731--HP:04",
        ]
    }
    knowledge_graph = _convert_shorthand_to_kg(shorthand_kg_nodes, shorthand_kg_edges)
    response, message = _run_resultify_directly(query_graph, knowledge_graph)
    assert response.status == "OK"
    n01_nodes = {
        node_key
        for node_key, node in message.knowledge_graph.nodes.items()
        if "n01" in node.qnode_keys
    }
    assert message.results and len(message.results) == len(n01_nodes)


def test10():
    resultifier = ARAXResultify()
    desc = resultifier.describe_me()
    assert "description" in desc[0]
    assert "ignore_edge_direction" in desc[0]["parameters"]


def test_bfs():
    qg_node_info = (
        {"node_key": "n01", "categories": "protein", "is_set": None},
        {"node_key": "DOID:12345", "categories": "disease", "is_set": False},
        {"node_key": "n02", "categories": "phenotypic_feature", "is_set": True},
    )

    qg_edge_info = (
        {"edge_key": "qe01", "subject": "n01", "object": "DOID:12345"},
        {"edge_key": "qe02", "subject": "DOID:12345", "object": "n02"},
    )

    qg_nodes = _create_qnodes(qg_node_info)
    qg_edges = _create_qedges(qg_edge_info)
    qg = QueryGraph(qg_nodes, qg_edges)
    adj_map = ARAX_resultify._make_adj_maps(qg, directed=False, droploops=True)["both"]
    bfs_dists = ARAX_resultify._bfs_dists(adj_map, "n01")
    assert bfs_dists == {"n01": 0, "DOID:12345": 1, "n02": 2}
    bfs_dists = ARAX_resultify._bfs_dists(adj_map, "DOID:12345")
    assert bfs_dists == {"n01": 1, "DOID:12345": 0, "n02": 1}


def test_bfs_in_essence_code():
    kg_node_info = (
        {"node_key": "DOID:12345", "categories": "disease", "qnode_keys": ["n00"]},
        {"node_key": "UniProtKB:12345", "categories": "protein", "qnode_keys": ["n01"]},
        {"node_key": "UniProtKB:23456", "categories": "protein", "qnode_keys": ["n01"]},
        {"node_key": "FOO:12345", "categories": "gene", "qnode_keys": ["n02"]},
        {
            "node_key": "HP:56789",
            "categories": "phenotypic_feature",
            "qnode_keys": ["n03"],
        },
    )

    kg_edge_info = (
        {
            "edge_key": "ke01",
            "object": "UniProtKB:12345",
            "subject": "DOID:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke02",
            "object": "UniProtKB:23456",
            "subject": "DOID:12345",
            "qedge_keys": ["qe01"],
        },
        {
            "edge_key": "ke03",
            "subject": "UniProtKB:12345",
            "object": "FOO:12345",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke04",
            "subject": "UniProtKB:23456",
            "object": "FOO:12345",
            "qedge_keys": ["qe02"],
        },
        {
            "edge_key": "ke05",
            "subject": "FOO:12345",
            "object": "HP:56789",
            "qedge_keys": ["qe03"],
        },
    )

    kg_nodes = _create_nodes(kg_node_info)
    kg_edges = _create_edges(kg_edge_info)

    knowledge_graph = KnowledgeGraph(kg_nodes, kg_edges)

    qg_node_info = (
        {"node_key": "n00", "categories": "disease", "is_set": False},  # DOID:12345
        {"node_key": "n01", "categories": "protein", "is_set": False},
        {"node_key": "n02", "categories": "gene", "is_set": False},
        {
            "node_key": "n03",  # HP:56789
            "categories": "phenotypic_feature",
            "is_set": False,
        },
    )

    qg_edge_info = (
        {"edge_key": "qe01", "subject": "n00", "object": "n01"},
        {"edge_key": "qe02", "subject": "n01", "object": "n02"},
        {"edge_key": "qe03", "subject": "n02", "object": "n03"},
    )

    qg_nodes = _create_qnodes(qg_node_info)
    qg_edges = _create_qedges(qg_edge_info)
    query_graph = QueryGraph(qg_nodes, qg_edges)

    results_list = ARAX_resultify._get_results_for_kg_by_qg(
        knowledge_graph, query_graph
    )
    assert len(results_list) == 2
    assert results_list[0].essence is not None


def test_issue727():
    # Check resultify ignores edge direction appropriately
    shorthand_qnodes = {"n00": "", "n01": ""}
    shorthand_qedges = {"e00": "n00--n01"}
    query_graph = _convert_shorthand_to_qg(shorthand_qnodes, shorthand_qedges)
    shorthand_kg_nodes = {"n00": ["DOID:111"], "n01": ["PR:01", "PR:02"]}
    shorthand_kg_edges = {
        "e00": ["PR:01--DOID:111", "PR:02--DOID:111"]
    }  # Edges are reverse direction of QG
    knowledge_graph = _convert_shorthand_to_kg(shorthand_kg_nodes, shorthand_kg_edges)
    response, message = _run_resultify_directly(query_graph, knowledge_graph)
    assert response.status == "OK"
    assert len(message.results) == 2


def test_issue731():
    # Return no results if QG is unfulfilled
    shorthand_qnodes = {"n0": "", "n1": "is_set", "n2": ""}
    shorthand_qedges = {"e0": "n0--n1", "e1": "n1--n2"}
    query_graph = _convert_shorthand_to_qg(shorthand_qnodes, shorthand_qedges)
    shorthand_kg_nodes = {
        "n0": [],
        "n1": ["UniProtKB:123", "UniProtKB:124"],
        "n2": ["DOID:122"],
    }
    shorthand_kg_edges = {
        "e0": [],
        "e1": ["UniProtKB:123--DOID:122", "UniProtKB:124--DOID:122"],
    }
    knowledge_graph = _convert_shorthand_to_kg(shorthand_kg_nodes, shorthand_kg_edges)
    response, message = _run_resultify_directly(query_graph, knowledge_graph)
    assert response.status == "OK"
    assert len(message.results) == 0


def test_issue731c():
    qg = QueryGraph(
        nodes={
            "n0": QNode(ids="MONDO:0005737", categories="biolink:Disease"),
            "n1": QNode(categories="biolink:Protein"),
            "n2": QNode(categories="biolink:Disease"),
        },
        edges={
            "e0": QEdge(subject="n0", object="n1"),
            "e1": QEdge(subject="n1", object="n2"),
        },
    )
    kg_node_info = (
        {"node_key": "MONDO:0005737", "categories": "disease", "qnode_keys": ["n0"]},
        {"node_key": "UniProtKB:Q14943", "categories": "protein", "qnode_keys": ["n1"]},
        {"node_key": "DOID:12297", "categories": "disease", "qnode_keys": ["n2"]},
        {"node_key": "DOID:11077", "categories": "disease", "qnode_keys": ["n2"]},
    )
    kg_edge_info = (
        {
            "edge_key": "UniProtKB:Q14943--MONDO:0005737",
            "object": "MONDO:0005737",
            "subject": "UniProtKB:Q14943",
            "qedge_keys": ["e0"],
        },
        {
            "edge_key": "DOID:12297--UniProtKB:Q14943",
            "object": "UniProtKB:Q14943",
            "subject": "DOID:12297",
            "qedge_keys": ["e1"],
        },
    )

    kg_nodes = _create_nodes(kg_node_info)
    kg_edges = _create_edges(kg_edge_info)

    kg = KnowledgeGraph(nodes=kg_nodes, edges=kg_edges)
    results = ARAX_resultify._get_results_for_kg_by_qg(kg, qg)
    indexes_results_with_single_edge = [
        index
        for index, result in enumerate(results)
        if len(result.analyses[0].edge_bindings) == 1
    ]
    assert len(indexes_results_with_single_edge) == 0


def test_issue740():
    # Tests that self-edges are handled properly
    shorthand_qnodes = {"n00": "", "n01": ""}
    shorthand_qedges = {"e00": "n00--n01"}
    query_graph = _convert_shorthand_to_qg(shorthand_qnodes, shorthand_qedges)
    shorthand_kg_nodes = {
        "n00": ["UMLS:C0004572"],  # Babesia
        "n01": ["HP:01", "HP:02", "UMLS:C0004572"],
    }
    shorthand_kg_edges = {
        "e00": [
            "UMLS:C0004572--HP:01",
            "UMLS:C0004572--HP:02",
            "UMLS:C0004572--UMLS:C0004572",
        ]
    }
    knowledge_graph = _convert_shorthand_to_kg(shorthand_kg_nodes, shorthand_kg_edges)
    response, message = _run_resultify_directly(query_graph, knowledge_graph)
    assert response.status == "OK"
    assert len(message.results) == 3


def test_issue692():
    kg = KnowledgeGraph(nodes=dict(), edges=dict())
    qg = QueryGraph(nodes=dict(), edges=dict())
    results_list = ARAX_resultify._get_results_for_kg_by_qg(kg, qg)
    assert len(results_list) == 0


def test_issue692b():
    query_graph = QueryGraph(nodes=dict(), edges=dict())
    knowledge_graph = KnowledgeGraph(nodes=dict(), edges=dict())
    response, message = _run_resultify_directly(query_graph, knowledge_graph)
    assert (
        "no results returned; empty knowledge graph"
        in response.messages_list()[0]["message"]
    )


def test_issue833_extraneous_intermediate_nodes():
    # Test for extraneous intermediate nodes
    shorthand_qnodes = {"n00": "", "n01": "is_set", "n02": "is_set", "n03": ""}
    shorthand_qedges = {"e00": "n00--n01", "e01": "n01--n02", "e02": "n02--n03"}
    query_graph = _convert_shorthand_to_qg(shorthand_qnodes, shorthand_qedges)
    shorthand_kg_nodes = {
        "n00": ["DOID:1056"],
        "n01": ["UniProtKB:111", "UniProtKB:222"],
        "n02": ["MONDO:111", "MONDO:222"],  # Last one is dead-end
        "n03": ["CHEBI:111"],
    }
    shorthand_kg_edges = {
        "e00": ["DOID:1056--UniProtKB:111", "DOID:1056--UniProtKB:222"],
        "e01": ["UniProtKB:111--MONDO:111", "UniProtKB:222--MONDO:222"],
        "e02": ["MONDO:111--CHEBI:111"],
    }
    knowledge_graph = _convert_shorthand_to_kg(shorthand_kg_nodes, shorthand_kg_edges)
    response, message = _run_resultify_directly(query_graph, knowledge_graph)
    assert response.status == "OK"
    for result in message.results:
        result_n01_nodes = {
            node_binding.id for node_binding in result.node_bindings["n01"]
        }
        result_e01_edges = {
            edge_binding.id for edge_binding in result.analyses[0].edge_bindings["e01"]
        }
        result_e00_edges = {
            edge_binding.id for edge_binding in result.analyses[0].edge_bindings["e00"]
        }
        for n01_node_key in result_n01_nodes:
            kg_edges_using_this_node = _get_kg_edge_keys_using_node(
                n01_node_key, message.knowledge_graph
            )
            assert result_e01_edges.intersection(kg_edges_using_this_node)
            assert result_e00_edges.intersection(kg_edges_using_this_node)


def test_parallel_edges_between_nodes():
    qg_nodes = {"n00": "", "n01": "is_set", "n02": ""}
    qg_edges = {"e00": "n00--n01", "e01": "n01--n02", "parallel01": "n01--n02"}
    query_graph = _convert_shorthand_to_qg(qg_nodes, qg_edges)
    kg_nodes = {
        "n00": ["DOID:11830"],
        "n01": ["UniProtKB:P39060", "UniProtKB:P20849"],
        "n02": ["CHEBI:85164", "CHEBI:29057"],
    }
    kg_edges = {
        "e00": ["DOID:11830--UniProtKB:P39060", "DOID:11830--UniProtKB:P20849"],
        "e01": ["UniProtKB:P39060--CHEBI:85164", "UniProtKB:P20849--CHEBI:29057"],
        "parallel01": [
            "UniProtKB:P39060--CHEBI:85164",
            "UniProtKB:P20849--CHEBI:29057",
            "UniProtKB:P39060--CHEBI:29057",
        ],
    }
    kg_before_resultify = _convert_shorthand_to_kg(kg_nodes, kg_edges)
    response, message = _run_resultify_directly(query_graph, kg_before_resultify)
    kg = message.knowledge_graph
    assert response.status == "OK"
    n02_nodes = {
        node_key for node_key, node in kg.nodes.items() if "n02" in node.qnode_keys
    }
    assert message.results and len(message.results) == len(n02_nodes)
    # Make sure every n01 node is connected to both an e01 edge and a parallel01 edge in each result
    for result in message.results:
        result_node_keys_by_qg_key = _get_result_node_keys_by_qg_key(result)
        result_edge_keys_by_qg_key = _get_result_edge_keys_by_qg_key(result)
        node_keys_used_by_e01_edges = {
            node_key
            for edge_key in result_edge_keys_by_qg_key["e01"]
            for node_key in {kg.edges[edge_key].subject, kg.edges[edge_key].object}
        }
        node_keys_used_by_parallel01_edges = {
            node_key
            for edge_key in result_edge_keys_by_qg_key["parallel01"]
            for node_key in {kg.edges[edge_key].subject, kg.edges[edge_key].object}
        }
        for node_key in result_node_keys_by_qg_key["n01"]:
            assert node_key in node_keys_used_by_e01_edges
            assert node_key in node_keys_used_by_parallel01_edges


def test_issue912_clean_up_kg():
    # Tests that the returned knowledge graph contains only nodes used in the results
    qg_nodes = {"n00": "", "n01": "is_set", "n02": ""}
    qg_edges = {"e00": "n00--n01", "e01": "n01--n02"}
    query_graph = _convert_shorthand_to_qg(qg_nodes, qg_edges)
    kg_nodes = {
        "n00": ["DOID:11", "DOID:NotConnected"],
        "n01": ["PR:110", "PR:111", "PR:DeadEnd"],
        "n02": ["CHEBI:11", "CHEBI:NotConnected"],
    }
    kg_edges = {
        "e00": ["DOID:11--PR:110", "DOID:11--PR:111", "DOID:11--PR:DeadEnd"],
        "e01": ["PR:110--CHEBI:11", "PR:111--CHEBI:11"],
    }
    knowledge_graph = _convert_shorthand_to_kg(kg_nodes, kg_edges)
    response, message = _run_resultify_directly(query_graph, knowledge_graph)
    assert response.status == "OK"
    assert len(message.results) == 1
    returned_kg_node_keys = set(message.knowledge_graph.nodes)
    assert returned_kg_node_keys == {"DOID:11", "PR:110", "PR:111", "CHEBI:11"}
    orphan_edges = {
        edge_key
        for edge_key, edge in message.knowledge_graph.edges.items()
        if not {edge.subject, edge.object}.issubset(returned_kg_node_keys)
    }
    assert not orphan_edges


def test_recompute_qg_keys():
    shorthand_qnodes = {"n00": "", "n01": ""}
    shorthand_qedges = {"e00": "n00--n01"}
    query_graph = _convert_shorthand_to_qg(shorthand_qnodes, shorthand_qedges)
    shorthand_kg_nodes = {
        "n00": ["DOID:731"],
        "n01": ["HP:01", "HP:02", "HP:03", "HP:04"],
    }
    shorthand_kg_edges = {
        "e00": [
            "DOID:731--HP:01",
            "DOID:731--HP:02",
            "DOID:731--HP:03",
            "DOID:731--HP:04",
        ]
    }
    knowledge_graph = _convert_shorthand_to_kg(shorthand_kg_nodes, shorthand_kg_edges)
    response, message = _run_resultify_directly(query_graph, knowledge_graph)
    assert response.status == "OK"
    assert message.results
    # Clear all qnode_keys/qedge_keys from the KG
    for node_key, node in message.knowledge_graph.nodes.items():
        node.qnode_keys = []
    for edge_key, edge in message.knowledge_graph.edges.items():
        edge.qedge_keys = []
    # Then recompute qg keys and make sure look ok
    resultifier = ARAXResultify()
    resultifier.recompute_qg_keys(response)
    assert response.status == "OK"
    kg = response.envelope.message.knowledge_graph
    assert kg.nodes and kg.edges
    for node_key, node in kg.nodes.items():
        assert (
            node.qnode_keys == ["n00"]
            if node_key in shorthand_kg_nodes["n00"]
            else ["n01"]
        )
    for edge_key, edge in kg.edges.items():
        assert edge.qedge_keys == ["e00"]


def test_multi_node_edgeless_qg():
    shorthand_qnodes = {"n00": "", "n01": ""}
    shorthand_qedges = {}
    query_graph = _convert_shorthand_to_qg(shorthand_qnodes, shorthand_qedges)
    shorthand_kg_nodes = {"n00": ["CHEMBL.COMPOUND:CHEMBL635"], "n01": ["MESH:D052638"]}
    shorthand_kg_edges = {}
    knowledge_graph = _convert_shorthand_to_kg(shorthand_kg_nodes, shorthand_kg_edges)
    response, message = _run_resultify_directly(query_graph, knowledge_graph)
    assert response.status == "OK"
    assert len(message.results) == 1

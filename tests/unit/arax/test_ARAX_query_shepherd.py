"""Shepherd stand-ins used by the ported ARAXQuery: the response store (DEC-3),
the query tracker, and the xDTD database check."""

import pytest

import shepherd_utils.db as db
from shepherd_utils.arax.ARAX_query_tracker import ARAXQueryTracker
from shepherd_utils.arax.ARAX_response import ARAXResponse
from shepherd_utils.arax.ARAX_messenger import ARAXMessenger
from shepherd_utils.arax.ResponseCache.response_cache import ResponseCache, response_url
from shepherd_utils.config import settings


@pytest.fixture(autouse=True)
def _server_url(monkeypatch):
    monkeypatch.setattr(settings, "server_url", "http://shepherd.test/")


def _response():
    response = ARAXResponse()
    ARAXMessenger().create_envelope(response)
    return response


def test_response_url():
    assert response_url("abc") == "http://shepherd.test/arax/response/abc"


def test_add_new_response_uses_the_callers_id():
    response = _response()
    assert ResponseCache("abc").add_new_response(response) == "abc"
    assert response.envelope.id == "http://shepherd.test/arax/response/abc"


def test_add_new_response_without_an_id_makes_one():
    response = _response()
    response_id = ResponseCache().add_new_response(response)
    assert len(response_id) == 36
    assert response.envelope.id == response_url(response_id)


def test_get_response_reads_shepherds_store(monkeypatch):
    stored = {"message": {"results": []}}
    monkeypatch.setattr(
        db, "get_message_sync", lambda i: stored if i == "abc" else {}[i]
    )
    cache = ResponseCache()
    assert cache.get_response("abc") is stored
    assert cache.get_response("missing") is None
    assert cache.get_response(None) is None


def test_message_uris_load_a_stored_shepherd_response(monkeypatch):
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    stored = {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": ["CHEBI:1"]}, "n1": {}},
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            },
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "results": [],
        }
    }
    requested = []

    def fake_get(message_id):
        requested.append(message_id)
        return stored

    monkeypatch.setattr(db, "get_message_sync", fake_get)
    araxq = ARAXQuery(response_id="new")
    araxq.query(
        {
            "operations": {
                "message_uris": ["http://shepherd.test/arax/response/abc"],
                "actions": ["return(message=true, store=true)"],
            }
        }
    )
    response = araxq.response
    assert response.status == "OK", response.show()
    assert requested == ["abc"]
    assert set(response.envelope.message.query_graph.nodes) == {"n0", "n1"}
    assert response.envelope.id == "http://shepherd.test/arax/response/new"


def test_message_uris_unknown_shepherd_response_is_an_error(monkeypatch):
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    monkeypatch.setattr(db, "get_message_sync", lambda i: {}[i])
    araxq = ARAXQuery()
    araxq.query(
        {
            "operations": {
                "message_uris": ["http://shepherd.test/arax/response/nope"],
                "actions": ["return(message=true)"],
            }
        }
    )
    assert araxq.response.error_code == "CannotLoadPreviousResponseById"


def test_response_false_returns_shepherds_url():
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    araxq = ARAXQuery(response_id="abc")
    araxq.response = ARAXResponse()
    result = araxq.execute_processing_plan(
        {
            "operations": {
                "actions": ["create_message", "return(response=false, store=true)"]
            }
        }
    )
    assert result == (
        {
            "status": 200,
            "response_id": "abc",
            "n_results": 0,
            "url": "http://shepherd.test/arax/response/abc",
        },
        200,
    )


def test_tracker_never_denies():
    tracker = ARAXQueryTracker()
    assert tracker.create_tracker_entry({"submitter": "x"}) is None
    assert tracker.update_tracker_entry(None, {}) is None
    assert tracker.alter_tracker_entry(None, {}) is None


def test_missing_xdtd_database_raises(tmp_path):
    from shepherd_utils.arax.Infer.scripts.ExplianableDTD_db import ExplainableDTD

    with pytest.raises(FileNotFoundError):
        ExplainableDTD(database_name="nope.db", outdir=str(tmp_path))


# --------------------------------------------------------------------------
# TRAPI 2.0 envelope and results
# --------------------------------------------------------------------------


def _edge(subject, object_, agent_type="automated_agent", attributes=None):
    edge = {
        "subject": subject,
        "object": object_,
        "predicate": "biolink:related_to",
        "sources": [
            {"resource_id": "infores:kp", "resource_role": "primary_knowledge_source"}
        ],
        "knowledge_level": "knowledge_assertion",
        "agent_type": agent_type,
    }
    if attributes is not None:
        edge["attributes"] = attributes
    return edge


def _two_hop_query(parameters=None):
    """A 2.0 message with results already bound (ARAX recomputes its qg keys from
    them), rerun through resultify, the ranker and the ResultTransformer."""
    query = {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"ids": ["CHEBI:1"]},
                    "n1": {"categories": ["biolink:Gene"]},
                },
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            },
            "knowledge_graph": {
                "nodes": {
                    "CHEBI:1": {
                        "name": "chem",
                        "categories": ["biolink:SmallMolecule"],
                    },
                    "NCBIGene:1": {"name": "g1", "categories": ["biolink:Gene"]},
                    "NCBIGene:2": {"name": "g2", "categories": ["biolink:Gene"]},
                },
                "edges": {
                    "x--infores:kp": _edge(
                        "CHEBI:1", "NCBIGene:1", agent_type="manual_agent"
                    ),
                    "y--infores:kp": _edge("CHEBI:1", "NCBIGene:2", attributes=[]),
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "n0": {"ids": ["CHEBI:1"]},
                        "n1": {"ids": [gene]},
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:kp",
                            "edge_bindings": {"e0": {"ids": [edge_id]}},
                        }
                    ],
                }
                for gene, edge_id in (
                    ("NCBIGene:1", "x--infores:kp"),
                    ("NCBIGene:2", "y--infores:kp"),
                )
            ],
        },
        "operations": {
            "actions": [
                "resultify(ignore_edge_direction=true)",
                "return(message=true, store=false)",
            ]
        },
    }
    if parameters is not None:
        query["parameters"] = parameters
    return query


def _assert_valid_trapi2(envelope_dict):
    from translator_tom.v2_0 import Response as TOMResponse

    TOMResponse.from_dict(envelope_dict)


def test_query_answers_a_valid_trapi2_response():
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    parameters = {"log_level": "DEBUG", "timeout": 30, "tiers": [0]}
    araxq = ARAXQuery()
    araxq.query(_two_hop_query(parameters))
    response = araxq.response
    assert response.status == "OK", response.show()
    envelope = response.envelope.to_dict()
    # the query's parameters are repeated
    assert envelope["parameters"] == parameters
    # no nulls, no empty auxiliary_graphs, logs in 2.0 shape
    assert "auxiliary_graphs" not in envelope["message"]
    assert envelope["logs"]
    for entry in envelope["logs"]:
        assert entry["level"] in {"ERROR", "WARNING", "INFO", "DEBUG"}
    _assert_valid_trapi2(envelope)
    results = envelope["message"]["results"]
    assert len(results) == 2
    for result in results:
        assert set(result["node_bindings"]) == {"n0", "n1"}
        (analysis,) = result["analyses"]
        assert analysis["resource_id"] == "infores:arax"
        assert set(analysis["edge_bindings"]) == {"e0"}
        assert len(analysis["edge_bindings"]["e0"]["ids"]) == 1


def test_ranker_reads_top_level_agent_type():
    """2.0 moves biolink:agent_type out of the attributes: a manual_agent edge
    still gets ARAX's 0.90 confidence and so outranks the other result."""
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    araxq = ARAXQuery()
    araxq.query(_two_hop_query())
    response = araxq.response
    assert response.status == "OK", response.show()
    edges = response.envelope.message.knowledge_graph.edges
    assert edges["x--infores:kp"].confidence == 0.90
    # an infores data source with no scoring attributes gets the 0.5 default base
    assert edges["y--infores:kp"].confidence == 0.5
    results = response.envelope.message.results
    assert results[0].node_bindings["n1"].ids == ["NCBIGene:1"]
    assert results[0].analyses[0].score > results[1].analyses[0].score


def test_edgeless_result_has_no_analyses():
    """An edgeless QG's results have nothing to bind in an Analysis: ARAX keeps
    one in memory (and ranks it, as upstream), and the 2.0 Response omits
    `analyses`."""
    import copy

    from shepherd_utils.arax.ARAX_query import finalize_trapi2_envelope
    from shepherd_utils.arax.ARAX_ranker import ARAXRanker
    from shepherd_utils.arax.ARAX_resultify import ARAXResultify
    from shepherd_utils.arax.openapi_server.models.node import Node
    from shepherd_utils.arax.openapi_server.models.q_node import QNode
    from shepherd_utils.arax.result_transformer import ResultTransformer

    response = _response()
    message = response.envelope.message
    message.query_graph.nodes["n0"] = QNode(ids=["CHEBI:1"])
    node = Node(name="c", categories=["biolink:Drug"])
    node.qnode_keys = ["n0"]
    message.knowledge_graph.nodes["CHEBI:1"] = node
    response.original_query_graph = copy.deepcopy(message.query_graph)
    ARAXResultify().apply(response, {})
    ARAXRanker().aggregate_scores_dmk(response)
    ResultTransformer.transform(response)
    assert response.status == "OK", response.show()
    (result,) = message.results
    assert result.analyses[0].edge_bindings == {}
    assert result.analyses[0].score is not None
    finalize_trapi2_envelope(response.envelope, {"timeout": 5})
    envelope = response.envelope.to_dict()
    (result,) = envelope["message"]["results"]
    assert result["node_bindings"] == {"n0": {"ids": ["CHEBI:1"]}}
    assert "analyses" not in result
    assert "edges" not in envelope["message"]["query_graph"]
    # the ranker's score survives in row_data
    assert result["row_data"][0] > 0
    _assert_valid_trapi2(envelope)


def test_finalize_drops_forbidden_empties():
    from shepherd_utils.arax.ARAX_query import finalize_trapi2_envelope
    from shepherd_utils.arax.openapi_server.models.analysis import Analysis
    from shepherd_utils.arax.openapi_server.models.edge_binding import EdgeBinding
    from shepherd_utils.arax.openapi_server.models.node_binding import NodeBinding
    from shepherd_utils.arax.openapi_server.models.q_edge import QEdge
    from shepherd_utils.arax.openapi_server.models.q_node import QNode
    from shepherd_utils.arax.openapi_server.models.result import Result

    response = _response()
    message = response.envelope.message
    message.query_graph.nodes["n0"] = QNode(ids=[], categories=[])
    message.query_graph.nodes["n1"] = QNode(categories=["biolink:Gene"])
    message.query_graph.edges["e0"] = QEdge(subject="n0", object="n1", predicates=[])
    message.auxiliary_graphs = {}
    message.results = [
        Result(
            node_bindings={
                "n0": NodeBinding(ids=["A:1"]),
                "n1": NodeBinding(ids=[]),
            },
            analyses=[
                Analysis(
                    resource_id="infores:arax",
                    edge_bindings={
                        "e0": EdgeBinding(ids=["x"]),
                        "e1": EdgeBinding(ids=[]),
                    },
                    support_graphs=[],
                ),
                Analysis(resource_id="infores:arax", edge_bindings={}),
            ],
        )
    ]
    finalize_trapi2_envelope(response.envelope, {"timeout": 5})
    envelope = response.envelope.to_dict()
    assert envelope["parameters"] == {"timeout": 5}
    assert "auxiliary_graphs" not in envelope["message"]
    assert envelope["message"]["query_graph"]["nodes"]["n0"] == {"is_set": False}
    assert "predicates" not in envelope["message"]["query_graph"]["edges"]["e0"]
    (result,) = envelope["message"]["results"]
    assert result["node_bindings"] == {"n0": {"ids": ["A:1"]}}
    assert result["analyses"] == [
        {"resource_id": "infores:arax", "edge_bindings": {"e0": {"ids": ["x"]}}}
    ]


def test_incoming_qedge_constraints_are_accepted_and_1x_ones_rejected():
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    def run(qedge_extra):
        query = _two_hop_query()
        query["message"]["query_graph"]["edges"]["e0"].update(qedge_extra)
        araxq = ARAXQuery()
        araxq.query(query)
        return araxq.response

    response = run(
        {"constraints": {"qualifiers": [{"biolink:object_direction_qualifier": "up"}]}}
    )
    assert response.status == "OK", response.show()
    response = run({"qualifier_constraints": []})
    assert response.error_code == "UnknownQEdgeProperty"


def test_add_qpath_makes_a_pathfinder_query_graph():
    response = _response()
    messenger = ARAXMessenger()
    messenger.create_envelope(response)
    for key in ("n0", "n1"):
        messenger.add_qnode(response, {"key": key, "categories": "biolink:Gene"})
    messenger.add_qpath(response, {"subject": "n0", "object": "n1"})
    assert response.status == "OK", response.show()
    query_graph = response.envelope.message.query_graph
    assert set(query_graph.paths) == {"p00"}
    assert query_graph.edges is None
    assert query_graph.paths["p00"].subject == "n0"


def test_result_transformer_moves_virtual_bindings_to_support_graphs():
    import copy

    from shepherd_utils.arax.openapi_server.models.analysis import Analysis
    from shepherd_utils.arax.openapi_server.models.edge import Edge
    from shepherd_utils.arax.openapi_server.models.edge_binding import EdgeBinding
    from shepherd_utils.arax.openapi_server.models.node import Node
    from shepherd_utils.arax.openapi_server.models.node_binding import NodeBinding
    from shepherd_utils.arax.openapi_server.models.q_edge import QEdge
    from shepherd_utils.arax.openapi_server.models.q_node import QNode
    from shepherd_utils.arax.openapi_server.models.result import Result
    from shepherd_utils.arax.result_transformer import ResultTransformer

    response = _response()
    message = response.envelope.message
    qg = message.query_graph
    qg.nodes.update({"n0": QNode(ids=["A:1"]), "n1": QNode()})
    qg.edges["e0"] = QEdge(subject="n0", object="n1")
    response.original_query_graph = copy.deepcopy(qg)
    qg.edges["N1"] = QEdge(subject="n0", object="n1")  # an overlay's virtual qedge
    message.knowledge_graph.nodes.update({"A:1": Node(), "B:1": Node(), "B:2": Node()})
    message.knowledge_graph.edges.update(
        {
            "kp": Edge(subject="A:1", object="B:1", predicate="biolink:related_to"),
            "ngd": Edge(subject="A:1", object="B:1", predicate="biolink:has_ngd"),
        }
    )
    message.results = [
        Result(
            node_bindings={
                "n0": NodeBinding(ids=["A:1"]),
                # B:2 is bound but used by no edge of the result: pruned
                "n1": NodeBinding(ids=["B:1", "B:2"]),
            },
            analyses=[
                Analysis(
                    resource_id="infores:arax",
                    edge_bindings={
                        "e0": EdgeBinding(ids=["kp"]),
                        "N1": EdgeBinding(ids=["ngd"]),
                    },
                )
            ],
        )
    ]
    ResultTransformer.transform(response)
    assert response.status == "OK", response.show()
    (result,) = message.results
    assert result.node_bindings["n1"].ids == ["B:1"]
    analysis = result.analyses[0]
    assert set(analysis.edge_bindings) == {"e0"}
    assert analysis.support_graphs == ["aux_graph_ngd"]
    assert message.auxiliary_graphs["aux_graph_ngd"].to_dict() == {"edges": ["ngd"]}
    assert set(message.knowledge_graph.nodes) == {"A:1", "B:1"}
    assert message.query_graph is response.original_query_graph


def test_node_only_query_graph_is_an_edgeless_one():
    """2.0 has no `edges: {}`: a QG with nodes only is the edgeless QG, not a
    MissingQEdgeAndQPath error (as a 1.x QG without edges and paths was)."""
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    araxq = ARAXQuery()
    araxq.query(
        {
            "message": {"query_graph": {"nodes": {"n0": {"ids": ["CHEBI:1"]}}}},
            "operations": {"actions": ["return(message=true, store=false)"]},
        }
    )
    response = araxq.response
    assert response.status == "OK", response.show()
    assert response.envelope.to_dict()["message"]["query_graph"] == {
        "nodes": {"n0": {"ids": ["CHEBI:1"], "is_set": False}}
    }

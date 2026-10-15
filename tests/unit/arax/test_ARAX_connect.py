"""Shepherd-specific behavior of the ported ARAX Connect (DEC-4, DEC-7, DEC-18).

Upstream parity for the pathfinder is not a target (DEC-7), and xCRG runs in
the catrax-xcrg package on both sides, so these check the port's wiring: which
Retriever, data files and limits it uses, and how the results land in the
envelope.
"""

import json

import pytest

import shepherd_utils.arax.ARAX_connect as connect_module
from shepherd_utils.arax.ARAX_connect import ARAXConnect
from shepherd_utils.arax.ARAX_messenger import ARAXMessenger
from shepherd_utils.arax.ARAX_response import ARAXResponse
from shepherd_utils.arax.openapi_server.models.pathfinder_analysis import (
    PathfinderAnalysis,
)
from shepherd_utils.arax.RTXConfiguration import RTXConfiguration
from shepherd_utils.config import settings

RETRIEVER = "http://retriever.test/query"
REHYDRATE = "http://retriever.test/rehydrate"


@pytest.fixture(autouse=True)
def _settings(monkeypatch):
    monkeypatch.setattr(settings, "sync_kg_retrieval_url", RETRIEVER)
    monkeypatch.setattr(settings, "kg_rehydrate_url", REHYDRATE)
    monkeypatch.delenv("ARAX_XCRG_RETRIEVER_URL", raising=False)


def _response(query_graph):
    response = ARAXResponse()
    messenger = ARAXMessenger()
    messenger.create_envelope(response)
    response.envelope.message = messenger.from_dict({"query_graph": query_graph})
    return response


PATHFINDER_QG = {
    "nodes": {"n0": {"ids": ["CHEBI:1"]}, "n1": {"ids": ["MONDO:1"]}},
    "paths": {"p0": {"subject": "n0", "object": "n1"}},
}


class FakeSynonymizer:
    def get_canonical_curies(self, curies=None, **kw):
        return {curies: {"preferred_curie": f"{curies}.canonical"}}


class FakePathfinder:
    instances = []

    def __init__(self, *args):
        self.args = args
        self.calls = []
        FakePathfinder.instances.append(self)

    def get_paths(self, **kwargs):
        self.calls.append(kwargs)
        result = {
            "id": "result",
            "node_bindings": {"n0": [{"id": "CHEBI:1"}], "n1": [{"id": "MONDO:1"}]},
            "analyses": [
                {
                    "resource_id": "infores:arax",
                    "path_bindings": {"p0": [{"id": "path_1"}]},
                    "score": 0.7,
                }
            ],
        }
        aux_graphs = {"path_1": {"edges": ["e1"]}}
        kg = {"nodes": {}, "edges": {"e1": {"subject": "CHEBI:1", "object": "MONDO:1"}}}
        return result, aux_graphs, kg


class FakeHTTPResponse:
    text = ""

    def raise_for_status(self):
        pass

    def json(self):
        return {
            "message": {
                "knowledge_graph": {
                    "nodes": {
                        "CHEBI:1": {"categories": ["biolink:SmallMolecule"]},
                        "MONDO:1": {"categories": ["biolink:Disease"]},
                    },
                    "edges": {
                        "e1": {
                            "subject": "CHEBI:1",
                            "object": "MONDO:1",
                            "predicate": "biolink:treats",
                            "sources": [
                                {
                                    "resource_id": "infores:ctd",
                                    "resource_role": "primary_knowledge_source",
                                }
                            ],
                        }
                    },
                }
            }
        }


@pytest.fixture
def pathfinder(monkeypatch):
    FakePathfinder.instances = []
    posts = []

    def fake_post(url, **kwargs):
        posts.append((url, kwargs))
        return FakeHTTPResponse()

    monkeypatch.setattr(connect_module, "Pathfinder", FakePathfinder)
    monkeypatch.setattr(connect_module, "NodeSynonymizer", FakeSynonymizer)
    monkeypatch.setattr(connect_module.requests, "post", fake_post)
    return posts


def test_connect_nodes_uses_shepherds_pathfinder_setup(pathfinder):
    response = _response(PATHFINDER_QG)
    ARAXConnect().apply(
        response,
        {
            "action": "connect_nodes",
            "max_path_length": "2",
            "max_pathfinder_paths": "10",
        },
    )
    assert response.status == "OK", response.show()
    (instance,) = FakePathfinder.instances
    config = RTXConfiguration()
    assert instance.args[:3] == (
        f"retriever:{RETRIEVER}",
        f"sqlite:{config.curie_ngd_path}",
        f"sqlite:{config.kg2c_sqlite_path}",
    )
    (call,) = instance.calls
    # DEC-7: Shepherd's limits, whatever max_path_length / max_pathfinder_paths say
    assert call["hops_numbers"] == call["max_hops_to_explore"] == 4
    assert call["limit"] == 500
    assert call["src_node_id"] == "CHEBI:1.canonical"
    assert call["dst_node_id"] == "MONDO:1.canonical"
    assert call["category_constraints"] == []
    ((url, kwargs),) = pathfinder
    assert url == REHYDRATE
    assert kwargs["json"]["parameters"] == {"rehydrate": True, "tier": 0}

    message = response.envelope.message
    (result,) = message.results
    assert result.essence == "result"
    assert isinstance(result.analyses[0], PathfinderAnalysis)
    assert result.analyses[0].path_bindings["p0"][0].id == "path_1"
    assert message.auxiliary_graphs["path_1"].edges == ["e1"]
    assert set(message.knowledge_graph.edges) == {"e1"}
    assert hasattr(response, "original_query_graph")


def test_connect_nodes_still_validates_max_path_length(pathfinder):
    response = _response(PATHFINDER_QG)
    ARAXConnect().apply(response, {"action": "connect_nodes", "max_path_length": "7"})
    assert response.status == "ERROR"
    assert response.error_code == "ValueError"
    assert FakePathfinder.instances == []


def test_connect_nodes_with_no_paths_warns_and_adds_no_results(pathfinder, monkeypatch):
    def no_paths(self, **kwargs):
        return {"id": "result", "node_bindings": {}, "analyses": []}, {}, {}

    monkeypatch.setattr(FakePathfinder, "get_paths", no_paths)
    response = _response(PATHFINDER_QG)
    ARAXConnect().apply(response, {"action": "connect_nodes"})
    assert response.status == "OK"
    assert not response.envelope.message.results
    assert any(
        m["level"] == "WARNING" and "Could not connect the nodes" in m["message"]
        for m in response.messages
    )


def _cached_kp_curies(store):
    return sorted(
        json.loads(store.get(key))["kp_curie"]
        for key in store.scan_iter(match="arax_kp_cache:record:*")
    )


def test_connect_nodes_stores_its_result_but_does_not_read_it_back(
    pathfinder, arax_kp_cache_store
):
    """As upstream (CON-05): the PathFinder result is stored, but a cached one
    is only used when the incoming message already has results."""
    for _ in range(2):
        response = _response(PATHFINDER_QG)
        ARAXConnect().apply(response, {"action": "connect_nodes"})
        assert response.status == "OK", response.show()
        assert any(
            m["message"] == "Storing resulting dict in the cache"
            for m in response.messages
        )
    assert len(FakePathfinder.instances) == 2
    assert _cached_kp_curies(arax_kp_cache_store) == ["PathFinder"]


XCRG_QG = {
    "nodes": {
        "chem": {"categories": ["biolink:ChemicalEntity"]},
        "gene": {"ids": ["NCBIGene:1"], "categories": ["biolink:Gene"]},
    },
    "edges": {
        "t": {
            "subject": "chem",
            "object": "gene",
            "predicates": ["biolink:affects"],
            "knowledge_type": "inferred",
        }
    },
}


@pytest.fixture
def xcrg(monkeypatch):
    calls = []

    def fake_run_xcrg(query, config, logger):
        calls.append((query, config))
        logger.info("xcrg ran with %s TFs", 3)
        return {
            "schema_version": "1.6.0",
            "biolink_version": "4.2.5",
            "message": {
                "query_graph": query["message"]["query_graph"],
                "knowledge_graph": {"nodes": {}, "edges": {}},
                "results": [
                    {"node_bindings": {"gene": [{"id": "NCBIGene:1"}]}, "analyses": []},
                    {"node_bindings": {"gene": [{"id": "NCBIGene:1"}]}, "analyses": []},
                ],
            },
        }

    monkeypatch.setattr(connect_module, "run_xcrg", fake_run_xcrg)
    return calls


def test_xcrg_uses_shepherds_retriever_and_data(xcrg):
    response = _response(XCRG_QG)
    ARAXConnect().apply(response, {"action": "xcrg"})
    assert response.status == "OK", response.show()
    ((query, config),) = xcrg
    assert set(query) == {"message"}  # parameters/submitter are not passed (XCR-02)
    rtx = RTXConfiguration()
    assert config.retriever_url == RETRIEVER
    assert str(config.ngd_db_path) == rtx.curie_ngd_path
    assert str(config.curie_to_pmids_db_path) == rtx.curie_to_pmids_path
    assert config.timeout == 210
    assert config.tf_batch_size == 200
    assert list(config.tiers) == [0]
    assert config.resource_id == "infores:arax"
    assert config.trapi_schema_version == "1.6.0"
    assert config.biolink_version == "4.2.5"

    assert response.total_results_count == 2
    assert response.data["xcrg_connect"] is True
    assert len(response.envelope.message.results) == 2
    plan = response.query_plan["qedge_keys"]["t"]["arax-xcrg"]
    assert plan["status"] == "Done"
    assert any(m["message"] == "xcrg ran with 3 TFs" for m in response.messages)


def test_xcrg_result_is_read_back_from_the_cache(xcrg, arax_kp_cache_store):
    first = _response(XCRG_QG)
    ARAXConnect().apply(first, {"action": "xcrg"})
    second = _response(XCRG_QG)
    ARAXConnect().apply(second, {"action": "xcrg"})

    assert len(xcrg) == 1  # the second query did not run xCRG
    assert second.status == "OK", second.show()
    assert any(
        m["message"].startswith(
            "Found a cached result with response_code=200, n_results=2"
        )
        for m in second.messages
    )
    assert second.data["xcrg_connect"] is True
    assert second.total_results_count == 2
    assert len(second.envelope.message.results) == 2
    assert _cached_kp_curies(arax_kp_cache_store) == ["xCRG"]


def test_xcrg_bypass_cache_runs_it_again(xcrg):
    ARAXConnect().apply(_response(XCRG_QG), {"action": "xcrg"})
    response = _response(XCRG_QG)
    response.envelope.query_options = {"bypass_cache": True}
    ARAXConnect().apply(response, {"action": "xcrg"})
    assert len(xcrg) == 2
    assert any(
        m["message"] == "bypass_cache is set; skipping cache lookup for xCRG"
        for m in response.messages
    )


def test_xcrg_env_overrides(xcrg, monkeypatch):
    monkeypatch.setenv("ARAX_XCRG_RETRIEVER_URL", "http://other.test/query")
    monkeypatch.setenv("ARAX_XCRG_TIMEOUT", "30")
    monkeypatch.setenv("ARAX_XCRG_TF_BATCH_SIZE", "not a number")
    ARAXConnect().apply(_response(XCRG_QG), {"action": "xcrg"})
    ((_, config),) = xcrg
    assert config.retriever_url == "http://other.test/query"
    assert config.timeout == 30
    assert config.tf_batch_size == 200


def test_xcrg_failure_is_a_500(monkeypatch):
    def boom(query, config, logger):
        raise RuntimeError("no TFs")

    monkeypatch.setattr(connect_module, "run_xcrg", boom)
    response = _response(XCRG_QG)
    ARAXConnect().apply(response, {"action": "xcrg"})
    assert response.status == "ERROR"
    assert response.http_status == 500
    assert response.query_plan["qedge_keys"]["t"]["arax-xcrg"]["status"] == "Error"

"""Shepherd's changes to the vendored xCRG package (DEC-21).

xCRG's own unit tests, adapted to TRAPI 2.0, are in ``xcrg_suite/``; the
query parity case ``mvp2_xcrg_route`` runs it end to end through ARAXQuery.
These pin what Shepherd changed on top of the 2.0 conversion: how a query
dict is read, what the Retriever lookups carry, the database formats the
readers accept, ranking on 2.0's required KL/AT, and NaN in the response.
"""

import json
import logging
import math
import sqlite3

import pytest
from translator_tom import Edge, Query, Response

from shepherd_utils.arax.xcrg import (
    XCRGConfig,
    async_run_xcrg,
    is_xcrg_mvp2_query,
    ngd,
    pmid,
    queries,
    ranking,
    retriever,
    runner,
)
from shepherd_utils.arax.xcrg.context import RunContext
from shepherd_utils.arax.xcrg.models import Direction
from shepherd_utils.arax.xcrg.reporting import StubReporter

QUALIFIERS = {
    "biolink:object_aspect_qualifier": "activity_or_abundance",
    "biolink:object_direction_qualifier": "decreased",
}
SOURCES = [{"resource_id": "infores:test", "resource_role": "primary_knowledge_source"}]


def _query(**extra):
    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "chem": {"categories": ["biolink:ChemicalEntity"]},
                    "gene": {"ids": ["NCBIGene:3"], "categories": ["biolink:Gene"]},
                },
                "edges": {
                    "t": {
                        "subject": "chem",
                        "object": "gene",
                        "predicates": ["biolink:affects"],
                        "knowledge_type": "inferred",
                        "constraints": {"qualifiers": [dict(QUALIFIERS)]},
                    }
                },
            }
        },
        **extra,
    }


def _config(**kwargs):
    return XCRGConfig(retriever_url="http://retriever.test/query", **kwargs)


def _context(query=None, config=None):
    return RunContext.new(
        query_id="test",
        query=runner.query_from_dict(query or _query()),
        config=config or _config(),
        reporter=StubReporter(),
    )


def _edge(subject, obj, **kwargs):
    return {
        "subject": subject,
        "object": obj,
        "predicate": "biolink:affects",
        "sources": SOURCES,
        "knowledge_level": kwargs.pop("knowledge_level", "knowledge_assertion"),
        "agent_type": kwargs.pop("agent_type", "manual_agent"),
        **kwargs,
    }


def test_an_arax_envelope_is_read_for_its_query_graph():
    """ARAX's envelope carries empty containers TRAPI 2.0 forbids (here an
    empty auxiliary_graphs); xCRG reads only the query graph, parameters and
    submitter, so the MVP2 check still sees the query."""
    envelope = _query(submitter="infores:arax", status="OK", schema_version="2.0.0")
    envelope["message"].update(
        results=[], knowledge_graph={"nodes": {}, "edges": {}}, auxiliary_graphs={}
    )
    envelope["message"]["query_graph"]["nodes"]["chem"]["is_set"] = False
    with pytest.raises(Exception):
        Query.from_dict(envelope)
    assert is_xcrg_mvp2_query(envelope)
    query = runner.query_from_dict(envelope)
    assert query.submitter == "infores:arax"
    assert query.message.results is None
    assert query.message.auxiliary_graphs is None


def test_the_mvp2_check_reads_2_0_qualifiers():
    assert is_xcrg_mvp2_query(_query())
    query = _query()
    query["message"]["query_graph"]["edges"]["t"]["constraints"]["qualifiers"] = [
        {"biolink:object_aspect_qualifier": "activity_or_abundance"}
    ]
    assert not is_xcrg_mvp2_query(query)
    query = _query()
    query["message"]["query_graph"]["edges"]["t"]["qualifier_constraints"] = query[
        "message"
    ]["query_graph"]["edges"]["t"].pop("constraints")
    assert not is_xcrg_mvp2_query(query)


def test_retriever_lookups_carry_2_0_qualifiers_and_parameters():
    """The two-hop lookups put their qualifiers in constraints.qualifiers, and
    every lookup sends parameters.timeout and parameters.tiers (Shepherd's
    Retriever reads both), as the pinned catrax-xcrg did."""
    ctx = _context(config=_config(timeout=99, tiers=[1]))
    two_hop = queries.build_two_hop_query(
        ctx, ["NCBIGene:9"], Direction.INCREASED, Direction.DECREASED
    ).to_dict()
    edges = two_hop["message"]["query_graph"]["edges"]
    assert edges["e0"]["constraints"]["qualifiers"] == [
        {
            "biolink:object_aspect_qualifier": "activity_or_abundance",
            "biolink:object_direction_qualifier": "increased",
        }
    ]
    assert edges["e1"]["constraints"]["qualifiers"][0][
        "biolink:object_direction_qualifier"
    ] == ("decreased")
    one_hop = queries.build_one_hop_query(ctx).to_dict()
    assert "knowledge_type" not in one_hop["message"]["query_graph"]["edges"]["direct"]
    for query in (two_hop, one_hop):
        assert query["parameters"] == {"timeout": 99, "tiers": [1]}
        assert "auxiliary_graphs" not in query["message"]


def test_lookup_parameters_keep_the_querys_own():
    ctx = _context(
        _query(parameters={"timeout": 5, "tiers": [0, 1], "bypass_cache": True})
    )
    parameters = queries.build_lookup_parameters(ctx).to_dict()
    assert parameters == {"timeout": 5, "tiers": [0, 1], "bypass_cache": True}


def _db(path, table, columns, rows):
    with sqlite3.connect(path) as con:
        con.execute(f"CREATE TABLE {table} ({', '.join(columns)})")
        con.executemany(
            f"INSERT INTO {table} VALUES ({', '.join('?' * len(columns))})", rows
        )
    return path


@pytest.fixture(autouse=True)
def _clear_db_caches():
    yield
    for cache in (
        ngd._NGD_CONNECTIONS,
        ngd._NGD_NEIGHBOR_CACHE,
        pmid._PMID_CONNECTIONS,
        pmid._PMID_CACHE,
    ):
        cache.clear()


def test_ngd_reads_shepherds_pairs_and_rtxs_triples(tmp_path):
    """Shepherd's curie_ngd (ARAX's v1.0 artifacts, which pathfinder also
    reads) lists [neighbor, ngd] pairs; RTX's newer schema adds a member."""
    db = _db(
        tmp_path / "curie_ngd.sqlite",
        "curie_ngd",
        ["curie TEXT", "ngd TEXT", "pmid_length INTEGER"],
        [
            ("A:1", json.dumps([["B:1", 0.5], ["C:1", 0.25]]), 3),
            ("A:2", json.dumps([["B:2", 0.75, [1, 2]]]), 2),
        ],
    )
    assert ngd.get_ngd_neighbors(db, StubReporter(), "A:1") == {"B:1": 0.5, "C:1": 0.25}
    assert ngd.get_ngd_neighbors(db, StubReporter(), "A:2") == {"B:2": 0.75}


def test_pmids_read_shepherds_json_and_rtxs_u32_blob(tmp_path):
    blob = b"".join(n.to_bytes(4, "little") for n in (7, 8))
    db = _db(
        tmp_path / "curie_to_pmids.sqlite",
        "curie_to_pmids",
        ["curie TEXT", "pmids"],
        [("A:1", json.dumps([5, "PMID:6"])), ("A:2", blob)],
    )
    reporter = WarningRecorder()
    assert pmid.get_curie_pmids(db, reporter, "A:1") == {"5", "6"}
    assert pmid.get_curie_pmids(db, reporter, "A:2") == {"7", "8"}
    assert pmid.get_curie_pmids(db, reporter, "A:3") == set()
    # a CURIE without PMIDs is not an error
    assert reporter.warnings == []


class WarningRecorder(StubReporter):
    def __init__(self):
        self.warnings = []

    def warning(self, msg, *args):
        self.warnings.append(msg % args)


@pytest.mark.parametrize(
    "knowledge_level, agent_type, want_level, want_agent",
    [
        ("knowledge_assertion", "manual_agent", "knowledge_assertion", "manual_agent"),
        # not_provided (2.0's required members' "unknown") and values ranking
        # has no weight for count as unknown, as a missing 1.x attribute did
        ("not_provided", "not_provided", "not_provided", None),
        ("prediction", "data_analysis_pipeline", "prediction", None),
        ("some_future_level", "automated_agent", "not_provided", "automated_agent"),
    ],
)
def test_ranking_reads_2_0_knowledge_level_and_agent_type(
    knowledge_level, agent_type, want_level, want_agent
):
    edge = Edge.from_dict(
        _edge(
            "A:1",
            "B:1",
            knowledge_level=knowledge_level,
            agent_type=agent_type,
            attributes=[
                {
                    "attribute_type_id": "biolink:publications",
                    "value": ["PMID:1", "PMID:2"],
                }
            ],
        )
    )
    stmt = ranking.get_qualified_stmt(edge)
    assert (stmt.knowledge_level, stmt.agent_type) == (want_level, want_agent)
    assert stmt.num_publications == 2
    assert ranking.Custom_Ranker().score_qualified_stmt(stmt) > 0


def _retriever_answer(query):
    """A TRAPI 2.0 Retriever answer: a direct edge for the one-hop lookup, a
    TF path for the decreased/increased template, nothing otherwise."""
    qgraph = query["message"]["query_graph"]
    nodes = {
        "CHEBI:1": {"categories": ["biolink:SmallMolecule"], "name": "one"},
        "CHEBI:2": {"categories": ["biolink:SmallMolecule"], "name": "two"},
        "NCBIGene:3": {"categories": ["biolink:Gene"], "name": "three"},
        "NCBIGene:4066": {"categories": ["biolink:Gene"], "name": "tf"},
    }
    if "e0" not in qgraph["edges"]:
        (qedge_id,) = qgraph["edges"]
        edges = {
            "d1": _edge(
                "CHEBI:1",
                "NCBIGene:3",
                attributes=[
                    {"attribute_type_id": "biolink:p_value", "value": math.nan}
                ],
            )
        }
        results = [
            {
                "node_bindings": {
                    "chem": {"ids": ["CHEBI:1"]},
                    "gene": {"ids": ["NCBIGene:3"]},
                },
                "analyses": [
                    {
                        "resource_id": "infores:retriever",
                        "edge_bindings": {qedge_id: {"ids": ["d1"]}},
                    }
                ],
            }
        ]
    else:
        directions = [
            qedge["constraints"]["qualifiers"][0]["biolink:object_direction_qualifier"]
            for qedge in (qgraph["edges"]["e0"], qgraph["edges"]["e1"])
        ]
        if (
            directions != ["decreased", "increased"]
            or "NCBIGene:4066" not in qgraph["nodes"]["tf"]["ids"]
        ):
            return {
                "message": {
                    "knowledge_graph": {"nodes": {}, "edges": {}},
                    "results": [],
                }
            }
        edges = {
            "p0": _edge("CHEBI:2", "NCBIGene:4066", agent_type="not_provided"),
            "p1": _edge("NCBIGene:4066", "NCBIGene:3"),
        }
        results = [
            {
                "node_bindings": {
                    "chem": {"ids": ["CHEBI:2"]},
                    "tf": {"ids": ["NCBIGene:4066"]},
                    "gene": {"ids": ["NCBIGene:3"]},
                },
                "analyses": [
                    {
                        "resource_id": "infores:retriever",
                        "edge_bindings": {"e0": {"ids": ["p0"]}, "e1": {"ids": ["p1"]}},
                    }
                ],
            }
        ]
    return {
        "status": "Success",
        "message": {
            "query_graph": qgraph,
            "knowledge_graph": {"nodes": nodes, "edges": edges},
            "results": results,
        },
    }


@pytest.fixture
def fake_retriever(monkeypatch):
    sent = []

    async def lookup(ctx, cache, query):
        body = json.loads(json.dumps(query.to_dict()))
        sent.append(body)
        return 200, Response.from_dict(_retriever_answer(body))

    monkeypatch.setattr(retriever, "_get_trapi_response_from_retriever", lookup)
    return sent


async def test_an_inferred_query_answers_in_trapi_2_0(fake_retriever):
    response = await async_run_xcrg(
        _query(), config=_config(tf_batch_size=500), logger=logging.getLogger("test")
    )
    # one direct lookup, then one per sign template (all TFs in one batch)
    assert len(fake_retriever) == 3
    Response.from_dict(response)  # valid TRAPI 2.0
    message = response["message"]
    assert response["schema_version"] == "2.0.0"
    answers = {
        result["node_bindings"]["chem"]["ids"][0]: result
        for result in message["results"]
    }
    assert set(answers) == {"CHEBI:1", "CHEBI:2"}
    assert answers["CHEBI:1"]["analyses"][0]["edge_bindings"] == {"t": {"ids": ["d1"]}}
    ((inferred_id,),) = [
        analysis["edge_bindings"]["t"]["ids"]
        for analysis in answers["CHEBI:2"]["analyses"]
    ]
    inferred = message["knowledge_graph"]["edges"][inferred_id]
    assert (inferred["knowledge_level"], inferred["agent_type"]) == (
        "prediction",
        "computational_model",
    )
    assert inferred["qualifiers"] == [
        {"qualifier_type_id": type_id, "qualifier_value": value}
        for type_id, value in QUALIFIERS.items()
    ]
    (support_graph,) = [
        attribute["value"]
        for attribute in inferred["attributes"]
        if attribute["attribute_type_id"] == "biolink:support_graphs"
    ]
    # (in the order of a set of edge ids, as upstream builds it)
    assert sorted(message["auxiliary_graphs"][support_graph[0]]["edges"]) == [
        "p0",
        "p1",
    ]
    for result in message["results"]:
        (analysis,) = result["analyses"]
        (ngd_support,) = analysis["support_graphs"]
        (ngd_edge,) = message["auxiliary_graphs"][ngd_support]["edges"]
        assert message["knowledge_graph"]["edges"][ngd_edge]["knowledge_level"] == (
            "statistical_association"
        )
    # NaN from Retriever survives (ARAX's models reject the null JSON mode makes of it)
    (p_value,) = message["knowledge_graph"]["edges"]["d1"]["attributes"]
    assert math.isnan(p_value["value"])


async def test_a_lookup_without_ids_answers_nothing_new(fake_retriever):
    """Upstream's MVP2 check also accepts the direct lookup shape, which runs
    Retriever's one-hop lookup only."""
    query = _query()
    qgraph = query["message"]["query_graph"]
    qgraph["edges"]["t"].pop("knowledge_type")
    assert is_xcrg_mvp2_query(query)
    response = await async_run_xcrg(query, config=_config())
    assert len(fake_retriever) == 1
    assert response["message"]["results"][0]["node_bindings"]["chem"] == {
        "ids": ["CHEBI:1"]
    }


def test_only_inferred_mvp2_queries_route_to_xcrg():
    """DEC-21: a one-hop lookup of the MVP2 shape stays on ARAX's own path."""
    from shepherd_utils.arax.ARAX_query_graph_interpreter import is_xcrg_inferred_query

    assert is_xcrg_inferred_query(_query())
    lookup = _query()
    lookup["message"]["query_graph"]["edges"]["t"].pop("knowledge_type")
    assert is_xcrg_mvp2_query(lookup)
    assert not is_xcrg_inferred_query(lookup)

"""ARAX's side of xCRG (DEC-25): upstream's PyPI package catrax-xcrg.

The query parity case ``mvp2_xcrg_route`` runs it end to end through
ARAXQuery. These pin what ARAX hands the package (the query, its config, the
log adapter) and how an MVP2 query is routed, plus -- as strict xfails -- the
known bugs of the pinned release that Shepherd tracks: a fixed release makes
them pass, and the xfail then fails, so the pin can be bumped and the marker
removed.
"""

import logging
from datetime import timedelta

import pytest
import xcrg
from xcrg import queries, ranking
from xcrg.context import Run_Context
from xcrg.models import Direction

from shepherd_utils.arax.ARAX_connect import ARAX_XCRG_Reporter, xcrg_query

QUALIFIERS = {
    "biolink:object_aspect_qualifier": "activity_or_abundance",
    "biolink:object_direction_qualifier": "decreased",
}


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


def _envelope():
    """An ARAX envelope as the port holds it: the message carries empty
    containers TRAPI 2.0 forbids (an empty auxiliary_graphs)."""
    envelope = _query(
        parameters={"bypass_cache": True},
        status="OK",
        schema_version="2.0.0",
    )
    envelope["message"].update(
        results=[], knowledge_graph={"nodes": {}, "edges": {}}, auxiliary_graphs={}
    )
    return envelope


def test_xcrg_is_given_the_query_graph_and_parameters():
    """The package rejects the envelope's empty auxiliary_graphs, so ARAX
    hands it the query graph and the query's parameters only."""
    envelope = _envelope()
    assert not xcrg.is_valid_query(envelope)
    query = xcrg_query(envelope)
    assert query == {
        "message": {"query_graph": envelope["message"]["query_graph"]},
        "parameters": {"bypass_cache": True},
    }
    assert xcrg.is_valid_query(query)


def test_inferred_and_lookup_mvp2_queries_route_to_xcrg():
    """DEC-25 (upstream trapi2.0): xcrg.is_valid_query decides, and it accepts
    the one-hop lookup shape as well as the inferred one."""
    from shepherd_utils.arax.ARAX_query_graph_interpreter import (
        ARAXQueryGraphInterpreter,
    )
    from shepherd_utils.arax.ARAX_messenger import ARAXMessenger
    from shepherd_utils.arax.ARAX_response import ARAXResponse

    lookup = _query()
    lookup["message"]["query_graph"]["edges"]["t"].pop("knowledge_type")
    for query in (_query(), lookup):
        response = ARAXResponse()
        response.envelope = ARAXMessenger().create_envelope(response).envelope
        response.envelope.message = ARAXMessenger().from_dict(query["message"])
        ARAXQueryGraphInterpreter().translate_to_araxi(response)
        assert response.status == "OK", response.show()
        assert response.data["araxi_commands"] == ["connect(action=xcrg)"]


def test_reporter_maps_xcrg_log_levels():
    class Recorder:
        def __init__(self):
            self.calls = []

        def __getattr__(self, level):
            return lambda msg, **kwargs: self.calls.append((level, msg, kwargs))

    recorder = Recorder()
    reporter = ARAX_XCRG_Reporter(recorder)
    for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR):
        reporter.handle_message(xcrg.Log_Message(level, "n=%s", (3,)))
    reporter.handle_message(xcrg.Progress_Message(50.0))
    assert recorder.calls == [
        ("debug", "n=3", {}),
        ("info", "n=3", {}),
        ("warning", "n=3", {}),
        ("error", "n=3", {"http_status": 500}),
    ]


def _context(config=None):
    return Run_Context.new(
        query_id="test",
        query=xcrg.runner.Query.from_dict(_query()),
        config=config
        or xcrg.Config(retriever_url="http://retriever.test/query", tiers=[0]),
        reporter=xcrg.Stub_Reporter(),
    )


@pytest.mark.xfail(
    strict=True,
    reason="BioPack-team/shepherd#274: catrax-xcrg 1.0.1.dev2 sends no "
    "parameters.tiers / parameters.timeout to Retriever",
)
def test_retriever_lookups_carry_tiers_and_timeout():
    ctx = _context(
        xcrg.Config(
            retriever_url="http://retriever.test/query",
            tiers=[0],
            timeout=timedelta(seconds=99),
        )
    )
    for query in (
        queries.build_two_hop_query(
            ctx, ["NCBIGene:9"], Direction.INCREASED, Direction.DECREASED
        ),
        queries.build_one_hop_query(ctx),
    ):
        parameters = query.to_dict().get("parameters") or {}
        assert parameters.get("tiers") == [0]
        assert parameters.get("timeout") == 99


@pytest.mark.parametrize(
    "agent_type",
    [
        "not_provided",
        "data_analysis_pipeline",
        "image_processing_agent",
        "manual_validation_of_automated_agent",
    ],
)
@pytest.mark.xfail(
    strict=True,
    raises=ValueError,
    reason="BioPack-team/shepherd#273: catrax-xcrg 1.0.1.dev2's ranker raises "
    "on a Biolink agent_type it has no weight for",
)
def test_ranker_scores_every_biolink_agent_type(agent_type):
    ranker = ranking.Custom_Ranker()
    statement = ranking.Qualified_Statement(
        num_publications=0,
        num_studies=0,
        evidence_count=0,
        knowledge_level="knowledge_assertion",
        agent_type=agent_type,
        confidence_score=None,
    )
    assert ranker.score_qualified_stmt(statement) > 0

"""The aragorn.coalesce worker: AnswerCoalesce's set enrichment (MCQ) ported
to Shepherd, with its DuckDB builder.

The databases are built here from small KGX fixtures. The builder's Biolink
lookups (bmt, which fetches the model over the network) are stubbed with a
tiny hand-written hierarchy.
"""

import copy
import json
import logging

import duckdb
import pytest
from scipy.stats import poisson

from shepherd_utils.config import settings
from shepherd_utils.query_errors import QueryError
from shepherd_utils.shared import is_set_input_query
from workers.aragorn_coalesce import build_duckdb, mcq, store
from workers.aragorn_coalesce import worker as coalesce_worker

SET_ID = "uuid:00000000-0000-0000-0000-000000000001"

#: predicate -> its Biolink ancestors (not reflexive), for the stub
PREDICATE_ANCESTORS = {
    "biolink:treats": ("biolink:related_to",),
    "biolink:associated_with": ("biolink:related_to",),
    "biolink:causes": ("biolink:contributes_to", "biolink:related_to"),
    "biolink:contributes_to": ("biolink:related_to",),
    "biolink:affects": ("biolink:related_to",),
    "biolink:related_to": (),
}
SYMMETRIC = {"biolink:related_to", "biolink:associated_with"}


@pytest.fixture(autouse=True)
def fake_biolink(monkeypatch):
    monkeypatch.setattr(
        build_duckdb,
        "is_qualifier",
        lambda key: key.endswith("_qualifier") or key == "qualified_predicate",
    )
    monkeypatch.setattr(build_duckdb, "is_symmetric", lambda p: p in SYMMETRIC)
    monkeypatch.setattr(
        build_duckdb,
        "_predicate_ancestors",
        lambda p: PREDICATE_ANCESTORS.get(p, ("biolink:related_to",)),
    )
    monkeypatch.setattr(build_duckdb, "_qualifier_ancestors", lambda v, e: (v,))


@pytest.fixture(autouse=True)
def _close_store():
    store.close_connection()
    yield
    store.close_connection()


def _write_jsonl(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def build(tmp_path, monkeypatch, nodes, edges, name="ac.duckdb"):
    """Build a database from ``nodes``/``edges`` and point the worker at it."""
    node_file, edge_file = tmp_path / "nodes.jsonl", tmp_path / "edges.jsonl"
    _write_jsonl(node_file, nodes)
    _write_jsonl(edge_file, edges)
    database = tmp_path / name
    build_duckdb.build_database(node_file, edge_file, database, blocklist=set())
    monkeypatch.setattr(settings, "answer_coalesce_duckdb_path", str(database))
    store.close_connection()
    return database


def source(resource_id="infores:test"):
    return [{"resource_id": resource_id, "resource_role": "primary_knowledge_source"}]


def mcq_query(
    members,
    predicate,
    *,
    set_is_subject=True,
    set_category="biolink:Drug",
    answer_category="biolink:Disease",
    parameters=None,
    qualifiers=None,
):
    edge = {
        "subject": "input" if set_is_subject else "output",
        "object": "output" if set_is_subject else "input",
        "predicates": [predicate],
    }
    if qualifiers:
        edge["constraints"] = {"qualifiers": [qualifiers]}
    query = {
        "message": {
            "query_graph": {
                "nodes": {
                    "input": {
                        "ids": [SET_ID],
                        "member_ids": list(members),
                        "set_interpretation": "MANY",
                        "categories": [set_category],
                    },
                    "output": {"categories": [answer_category]},
                },
                "edges": {"e0": edge},
            }
        }
    }
    query["parameters"] = {
        "pvalue_threshold": 1.0,
        "max_results": 10,
        **(parameters or {}),
    }
    return query


def answer_ids(response):
    return {
        curie
        for result in response["message"]["results"]
        for curie in result["node_bindings"]["output"]["ids"]
    }


# --- a direction-sensitive graph (ported from AnswerCoalesce) ---------------------

DIRECTION_NODES = [
    {"id": "DRUG:1", "name": "drug one", "category": ["biolink:Drug"]},
    {"id": "DRUG:2", "name": "drug two", "category": ["biolink:Drug"]},
    {"id": "DRUG:3", "category": ["biolink:Drug"]},
    {"id": "DISEASE:FORWARD", "name": "forward", "category": ["biolink:Disease"]},
    {"id": "DISEASE:REVERSE", "category": ["biolink:Disease"]},
    {"id": "DISEASE:SYMMETRIC", "category": ["biolink:Disease"]},
]
DIRECTION_EDGES = [
    {
        "id": "f1",
        "subject": "DRUG:1",
        "predicate": "biolink:treats",
        "object": "DISEASE:FORWARD",
        "sources": source(),
        "knowledge_level": "knowledge_assertion",
        "agent_type": "manual_agent",
    },
    {
        "id": "f2",
        "subject": "DRUG:2",
        "predicate": "biolink:treats",
        "object": "DISEASE:FORWARD",
        "sources": source(),
    },
    {
        "id": "r1",
        "subject": "DISEASE:REVERSE",
        "predicate": "biolink:treats",
        "object": "DRUG:1",
        "sources": source(),
    },
    {
        "id": "r2",
        "subject": "DISEASE:REVERSE",
        "predicate": "biolink:treats",
        "object": "DRUG:2",
        "sources": source(),
    },
    {
        "id": "s1",
        "subject": "DISEASE:SYMMETRIC",
        "predicate": "biolink:related_to",
        "object": "DRUG:1",
        "sources": source(),
    },
    {
        "id": "s2",
        "subject": "DISEASE:SYMMETRIC",
        "predicate": "biolink:related_to",
        "object": "DRUG:2",
        "sources": source(),
    },
]


@pytest.fixture
def direction_db(tmp_path, monkeypatch):
    return build(tmp_path, monkeypatch, DIRECTION_NODES, DIRECTION_EDGES)


def test_asymmetric_relations_follow_the_query_direction(direction_db):
    forward = mcq.answer_mcq(
        mcq_query(["DRUG:1", "DRUG:2"], "biolink:treats"), {"pvalue_threshold": 1.0}
    )
    assert answer_ids(forward) == {"DISEASE:FORWARD"}
    reverse = mcq.answer_mcq(
        mcq_query(["DRUG:1", "DRUG:2"], "biolink:treats", set_is_subject=False),
        {"pvalue_threshold": 1.0},
    )
    assert answer_ids(reverse) == {"DISEASE:REVERSE"}


@pytest.mark.parametrize("set_is_subject", [True, False])
def test_symmetric_relations_answer_either_direction(direction_db, set_is_subject):
    response = mcq.answer_mcq(
        mcq_query(
            ["DRUG:1", "DRUG:2"], "biolink:related_to", set_is_subject=set_is_subject
        ),
        {"pvalue_threshold": 1.0},
    )
    # related_to is implied by treats, so every disease linked to the set
    # either way round qualifies; only the symmetric one is linked by
    # related_to itself in both directions.
    assert "DISEASE:SYMMETRIC" in answer_ids(response)


def test_response_is_valid_trapi_2_with_support_graphs(direction_db):
    from translator_tom import Response

    response = mcq.answer_mcq(
        mcq_query(["DRUG:1", "DRUG:2", "DRUG:3"], "biolink:treats"),
        {"pvalue_threshold": 1.0},
    )
    Response.from_dict(response)
    message = response["message"]
    kg = message["knowledge_graph"]

    (result,) = message["results"]
    assert result["node_bindings"] == {
        "input": {"ids": [SET_ID]},
        "output": {"ids": ["DISEASE:FORWARD"]},
    }
    (analysis,) = result["analyses"]
    assert analysis["resource_id"] == "infores:shepherd-answercoalesce"
    (inferred_id,) = analysis["edge_bindings"]["e0"]["ids"]
    inferred = kg["edges"][inferred_id]
    assert (inferred["subject"], inferred["predicate"], inferred["object"]) == (
        SET_ID,
        "biolink:treats",
        "DISEASE:FORWARD",
    )
    assert inferred["knowledge_level"] == "prediction"
    assert inferred["agent_type"] == "computational_model"
    attributes = {a["attribute_type_id"]: a["value"] for a in inferred["attributes"]}
    # 2 of 3 drugs treat it; 2 of the graph's 3 drugs do overall
    assert attributes["biolink:p_value"] == pytest.approx(poisson.sf(1, 2 * 3 / 3))
    assert analysis["score"] == mcq.pvalue_to_score(attributes["biolink:p_value"])

    # one auxiliary graph per supporting member: its support edge + member_of
    support = attributes["biolink:support_graphs"]
    assert len(support) == 2
    members = set()
    for aux_id in support:
        aux_edges = [
            kg["edges"][e] for e in message["auxiliary_graphs"][aux_id]["edges"]
        ]
        by_predicate = {e["predicate"]: e for e in aux_edges}
        assert set(by_predicate) == {"biolink:treats", "biolink:member_of"}
        member_of = by_predicate["biolink:member_of"]
        assert member_of["object"] == SET_ID
        assert by_predicate["biolink:treats"]["subject"] == member_of["subject"]
        members.add(member_of["subject"])
    assert members == {"DRUG:1", "DRUG:2"}

    # every member and the set itself are in the knowledge graph
    assert kg["nodes"][SET_ID]["is_set"] is True
    assert {"DRUG:1", "DRUG:2", "DRUG:3"} <= set(kg["nodes"])
    assert kg["nodes"]["DRUG:1"]["name"] == "drug one"
    assert kg["nodes"]["DISEASE:FORWARD"] == {
        "categories": ["biolink:Disease"],
        "name": "forward",
    }


def test_support_edges_keep_their_provenance_and_klat(direction_db):
    response = mcq.answer_mcq(
        mcq_query(["DRUG:1", "DRUG:2"], "biolink:treats"), {"pvalue_threshold": 1.0}
    )
    support = {
        edge["subject"]: edge
        for edge in response["message"]["knowledge_graph"]["edges"].values()
        if edge["predicate"] == "biolink:treats" and edge["subject"] != SET_ID
    }
    assert support["DRUG:1"]["knowledge_level"] == "knowledge_assertion"
    assert support["DRUG:1"]["agent_type"] == "manual_agent"
    # recorded nowhere in the KGX: TRAPI 2.0's "not_provided"
    assert support["DRUG:2"]["knowledge_level"] == "not_provided"
    assert support["DRUG:2"]["agent_type"] == "not_provided"
    assert support["DRUG:1"]["sources"] == [
        {"resource_id": "infores:test", "resource_role": "primary_knowledge_source"},
        {
            "resource_id": "infores:shepherd-answercoalesce",
            "resource_role": "aggregator_knowledge_source",
            "upstream_resource_ids": ["infores:test"],
        },
    ]


def test_rebuilding_the_response_is_deterministic(direction_db):
    query = mcq_query(["DRUG:1", "DRUG:2"], "biolink:treats")
    assert mcq.answer_mcq(copy.deepcopy(query), {}) == mcq.answer_mcq(
        copy.deepcopy(query), {}
    )


def test_pvalue_threshold_and_max_results(direction_db):
    query = mcq_query(["DRUG:1", "DRUG:2"], "biolink:related_to")
    everything = mcq.answer_mcq(query, {"pvalue_threshold": 1.0})
    assert len(everything["message"]["results"]) > 1
    one = mcq.answer_mcq(query, {"pvalue_threshold": 1.0, "max_results": 1})
    assert len(one["message"]["results"]) == 1
    # AnswerCoalesce's default threshold (1e-5) is far below anything a
    # 6-node graph can reach
    none = mcq.answer_mcq(query, {})
    assert none["message"]["results"] == []
    assert "auxiliary_graphs" not in none["message"]


def test_unknown_members_find_nothing(direction_db):
    response = mcq.answer_mcq(
        mcq_query(["DRUG:404", "DRUG:405"], "biolink:treats"), {"pvalue_threshold": 1.0}
    )
    assert response["message"]["results"] == []


@pytest.mark.parametrize(
    "parameters",
    [{"max_results": 0}, {"max_results": "lots"}, {"pvalue_threshold": "small"}],
)
def test_bad_parameters_are_a_client_error(direction_db, parameters):
    with pytest.raises(QueryError) as e:
        mcq.answer_mcq(mcq_query(["DRUG:1", "DRUG:2"], "biolink:treats"), parameters)
    assert e.value.status_code == 400


# --- enrichment statistics and qualifiers -----------------------------------------


def test_duplicate_evidence_does_not_inflate_enrichment(tmp_path, monkeypatch):
    """Ported from AnswerCoalesce: two source edges for one fact count once in
    the statistics but both appear as support."""
    genes = [
        {
            "id": f"GENE:{i}",
            "name": f"g{i}",
            "category": ["biolink:NamedThing", "biolink:Gene"],
        }
        for i in (1, 2, 3)
    ]
    nodes = genes + [
        {"id": "DISEASE:1", "category": ["biolink:NamedThing", "biolink:Disease"]}
    ]
    edges = [
        {
            "id": "e1",
            "subject": "GENE:1",
            "predicate": "biolink:associated_with",
            "object": "DISEASE:1",
            "sources": source("infores:primary"),
        },
        {
            "id": "e2",
            "subject": "GENE:1",
            "predicate": "biolink:associated_with",
            "object": "DISEASE:1",
            "sources": source("infores:other-primary"),
        },
        {
            "id": "e3",
            "subject": "GENE:2",
            "predicate": "biolink:associated_with",
            "object": "DISEASE:1",
            "sources": source("infores:primary"),
        },
    ]
    build(tmp_path, monkeypatch, nodes, edges)
    candidates, total = store.enrichment_candidates(
        ["GENE:1", "GENE:2"],
        "biolink:Gene",
        node_constraints=["biolink:Disease"],
        predicate_constraints=[{"predicate": "biolink:associated_with"}],
    )
    assert total == 3
    (candidate,) = candidates
    assert (candidate.support_count, candidate.background_count) == (2, 2)
    assert candidate.p_value == pytest.approx(poisson.sf(1, 4 / 3))

    response = mcq.answer_mcq(
        mcq_query(
            ["GENE:1", "GENE:2"], "biolink:associated_with", set_category="biolink:Gene"
        ),
        {"pvalue_threshold": 1.0},
    )
    support = [
        edge
        for edge in response["message"]["knowledge_graph"]["edges"].values()
        if edge["predicate"] == "biolink:associated_with"
        and SET_ID not in (edge["subject"], edge["object"])
    ]
    assert sorted(edge["sources"][0]["resource_id"] for edge in support) == [
        "infores:other-primary",
        "infores:primary",
        "infores:primary",
    ]


def test_query_qualifiers_constrain_and_are_kept(tmp_path, monkeypatch):
    nodes = [
        {"id": "CHEM:1", "category": ["biolink:ChemicalEntity"]},
        {"id": "CHEM:2", "category": ["biolink:ChemicalEntity"]},
        {"id": "GENE:UP", "category": ["biolink:Gene"]},
        {"id": "GENE:DOWN", "category": ["biolink:Gene"]},
    ]

    def affects(chem, gene, direction):
        return {
            "subject": chem,
            "predicate": "biolink:affects",
            "object": gene,
            "qualified_predicate": "biolink:causes",
            "object_direction_qualifier": direction,
            "sources": source(),
        }

    edges = [
        affects("CHEM:1", "GENE:UP", "increased"),
        affects("CHEM:2", "GENE:UP", "increased"),
        affects("CHEM:1", "GENE:DOWN", "decreased"),
        affects("CHEM:2", "GENE:DOWN", "decreased"),
    ]
    build(tmp_path, monkeypatch, nodes, edges)
    query = mcq_query(
        ["CHEM:1", "CHEM:2"],
        "biolink:affects",
        set_category="biolink:ChemicalEntity",
        answer_category="biolink:Gene",
        qualifiers={
            "biolink:qualified_predicate": "biolink:causes",
            "biolink:object_direction_qualifier": "increased",
        },
    )
    response = mcq.answer_mcq(query, {"pvalue_threshold": 1.0})
    assert answer_ids(response) == {"GENE:UP"}
    for edge in response["message"]["knowledge_graph"]["edges"].values():
        if edge["predicate"] == "biolink:affects":
            assert {
                q["qualifier_type_id"]: q["qualifier_value"] for q in edge["qualifiers"]
            } == {
                "biolink:qualified_predicate": "biolink:causes",
                "biolink:object_direction_qualifier": "increased",
            }


def test_pvalue_to_score_matches_answercoalesce():
    # sigmoid(0.5 * (-log10(p) - 5)), rounded to 5 places
    assert mcq.pvalue_to_score(1e-5) == 0.5
    assert mcq.pvalue_to_score(1e-15) == pytest.approx(0.99331, abs=1e-5)
    assert mcq.pvalue_to_score(1.0) == pytest.approx(0.07586, abs=1e-5)
    assert mcq.pvalue_to_score(0.0) == 1.0
    assert mcq.pvalue_to_score(1e-300) == 1.0


# --- query shape ------------------------------------------------------------------


def test_is_set_input_query():
    query_graph = mcq_query(["A:1", "A:2"], "biolink:related_to")["message"][
        "query_graph"
    ]
    assert is_set_input_query(query_graph)
    batch = copy.deepcopy(query_graph)
    del batch["nodes"]["input"]["set_interpretation"]
    assert not is_set_input_query(batch)
    no_members = copy.deepcopy(query_graph)
    del no_members["nodes"]["input"]["member_ids"]
    assert not is_set_input_query(no_members)
    two_hop = copy.deepcopy(query_graph)
    two_hop["nodes"]["n2"] = {}
    two_hop["edges"]["e1"] = {"subject": "output", "object": "n2"}
    assert not is_set_input_query(two_hop)
    assert not is_set_input_query(None)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda qg: qg["nodes"]["input"].update(ids=[SET_ID, "uuid:2"]),
        lambda qg: qg["edges"]["e0"].update(
            predicates=["biolink:treats", "biolink:causes"]
        ),
        lambda qg: qg["edges"]["e0"].update(
            constraints={
                "qualifiers": [
                    {"biolink:object_direction_qualifier": "increased"},
                    {"biolink:object_direction_qualifier": "decreased"},
                ]
            }
        ),
        lambda qg: qg["nodes"]["input"].pop("set_interpretation"),
    ],
)
def test_unanswerable_queries_are_client_errors(mutate):
    query_graph = mcq_query(["A:1", "A:2"], "biolink:related_to")["message"][
        "query_graph"
    ]
    mutate(query_graph)
    with pytest.raises(QueryError) as e:
        mcq.parse_mcq(query_graph)
    assert e.value.status_code == 400


def test_parse_mcq_reads_trapi_2_constraints():
    query_graph = mcq_query(
        ["A:1", "A:1", "A:2"],
        "biolink:affects",
        set_is_subject=False,
        qualifiers={"biolink:species_context_qualifier": "NCBITaxon:9606"},
    )["message"]["query_graph"]
    parsed = mcq.parse_mcq(query_graph)
    assert parsed.member_ids == ("A:1", "A:2")
    assert parsed.set_is_subject is False
    assert parsed.relation == {
        "predicate": "biolink:affects",
        "species_context_qualifier": "NCBITaxon:9606",
    }


# --- builder and schema -----------------------------------------------------------


def test_builder_records_knowledge_level_and_agent_type(direction_db):
    connection = duckdb.connect(str(direction_db), read_only=True)
    assert connection.execute(
        "SELECT value FROM metadata WHERE key = 'schema_version'"
    ).fetchone() == ("8",)
    rows = dict(
        connection.execute(
            "SELECT original_edge_id, [knowledge_level, agent_type] FROM evidence"
        ).fetchall()
    )
    connection.close()
    assert rows["f1"] == ["knowledge_assertion", "manual_agent"]
    assert rows["f2"] == [None, None]


def test_a_schema_7_database_still_answers(tmp_path, monkeypatch):
    """AnswerCoalesce's own databases (schema 7) have no KL/AT columns."""
    database = build(tmp_path, monkeypatch, DIRECTION_NODES, DIRECTION_EDGES)
    connection = duckdb.connect(str(database))
    connection.execute("""
        UPDATE metadata SET value = '7' WHERE key = 'schema_version';
        DROP INDEX evidence_fact_idx;
        ALTER TABLE evidence DROP COLUMN knowledge_level;
        ALTER TABLE evidence DROP COLUMN agent_type;
        """)
    connection.close()
    store.close_connection()
    response = mcq.answer_mcq(
        mcq_query(["DRUG:1", "DRUG:2"], "biolink:treats"), {"pvalue_threshold": 1.0}
    )
    assert answer_ids(response) == {"DISEASE:FORWARD"}
    assert {
        edge["knowledge_level"]
        for edge in response["message"]["knowledge_graph"]["edges"].values()
        if edge["predicate"] == "biolink:treats" and edge["subject"] != SET_ID
    } == {"not_provided"}


def test_unsupported_schema_is_refused(tmp_path, monkeypatch):
    database = tmp_path / "old.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(
        "CREATE TABLE metadata (key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL);"
        "INSERT INTO metadata VALUES ('schema_version', '6');"
    )
    connection.close()
    monkeypatch.setattr(settings, "answer_coalesce_duckdb_path", str(database))
    with pytest.raises(RuntimeError, match="schema version 6"):
        store.connection()


def test_missing_database_names_the_setting(tmp_path, monkeypatch):
    monkeypatch.setattr(
        settings, "answer_coalesce_duckdb_path", str(tmp_path / "absent.duckdb")
    )
    with pytest.raises(FileNotFoundError, match="ANSWER_COALESCE_DUCKDB_PATH"):
        store.connection()


def test_robokop_provenance_becomes_trapi_sources():
    edge = {
        "biolink:primary_knowledge_source": "infores:primary",
        "biolink:aggregator_knowledge_source": ["infores:agg1", "infores:agg2"],
    }
    assert build_duckdb.extract_prov(edge) == edge
    assert store.trapi_sources(edge) == [
        {"resource_id": "infores:primary", "resource_role": "primary_knowledge_source"},
        {
            "resource_id": "infores:agg1",
            "resource_role": "aggregator_knowledge_source",
            "upstream_resource_ids": ["infores:primary"],
        },
        {
            "resource_id": "infores:agg2",
            "resource_role": "aggregator_knowledge_source",
            "upstream_resource_ids": ["infores:primary"],
        },
    ]


def test_kgx_sources_drop_non_trapi_fields():
    sources = [
        {
            "id": "infores:primary",
            "category": ["biolink:RetrievalSource"],
            "resource_id": "infores:primary",
            "resource_role": "primary_knowledge_source",
            "source_record_urls": ["https://example.org/1"],
        },
    ]
    assert build_duckdb.extract_prov({"id": "e", "sources": sources}) == sources
    assert store.trapi_sources(sources) == [
        {
            "resource_id": "infores:primary",
            "resource_role": "primary_knowledge_source",
            "source_record_urls": ["https://example.org/1"],
        },
    ]


@pytest.mark.parametrize(
    "sources",
    [
        [],
        [{"resource_id": "infores:x", "resource_role": "aggregator_knowledge_source"}],
        source("infores:one") + source("infores:two"),
    ],
)
def test_builder_requires_exactly_one_primary_source(sources):
    with pytest.raises(ValueError, match="exactly one primary_knowledge_source"):
        build_duckdb.extract_prov({"id": "bad", "sources": sources})


# --- the worker -------------------------------------------------------------------


def test_worker_entrypoint_reads_the_query_and_saves_the_response(direction_db, mocker):
    query = mcq_query(
        ["DRUG:1", "DRUG:2"], "biolink:treats", parameters={"pvalue_threshold": 1e-30}
    )
    get = mocker.patch.object(coalesce_worker, "get_message_sync", return_value=query)
    save = mocker.patch.object(coalesce_worker, "save_response_sync")

    # the workflow operation's settings win over the query's parameters
    operation = {"id": "aragorn.coalesce", "parameters": {"pvalue_threshold": 1.0}}
    logs = coalesce_worker.aragorn_coalesce("q1", "r1", operation, logging.INFO)

    get.assert_called_once_with("q1")
    (response_id, response), _ = save.call_args
    assert response_id == "r1"
    assert answer_ids(response) == {"DISEASE:FORWARD"}
    assert isinstance(logs, list)

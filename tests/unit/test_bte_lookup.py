"""Tests for ``workers.bte_lookup.worker`` helpers and the lookup entry point.

Covers ``examine_query``, ``get_params``, ``match_templates``,
``fill_templates``, ``expand_bte_query``, plus pure-lookup and
inferred-with-templates branches of ``bte_lookup``.
"""

import copy
import json
import logging

import pytest

from tests.helpers.generate_messages import creative_query
from workers.bte_lookup.worker import (
    AsyncResponse,
    bte_lookup,
    examine_query,
    expand_bte_query,
    fill_templates,
    get_params,
    match_templates,
)


logger = logging.getLogger(__name__)


# --- examine_query ---------------------------------------------------------


def test_examine_query_lookup_only():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        }
    }
    infer, _, _, pathfinder = examine_query(msg)
    assert (infer, pathfinder) == (False, False)


def test_examine_query_inferred_returns_creative_pair():
    msg = copy.deepcopy(creative_query)
    infer, q, a, _ = examine_query(msg)
    assert infer is True
    assert q == "ON" and a == "SN"


# --- get_params ------------------------------------------------------------


def test_get_params_extracts_full_tuple():
    qg = {
        "nodes": {
            "a": {"categories": ["biolink:ChemicalEntity"], "ids": ["CHEBI:1"]},
            "b": {"categories": ["biolink:Disease"]},
        },
        "edges": {
            "e0": {
                "subject": "a",
                "object": "b",
                "predicates": ["biolink:treats"],
                "qualifier_constraints": [
                    {
                        "qualifier_set": [
                            {
                                "qualifier_type_id": "biolink:object_aspect_qualifier",
                                "qualifier_value": "activity",
                            }
                        ]
                    }
                ],
            }
        },
    }
    s_key, s_type, s_curie, o_key, o_type, o_curie, predicate, qualifiers = get_params(qg)
    assert s_key == "a"
    assert s_type == "biolink:ChemicalEntity"
    assert s_curie == "CHEBI:1"
    assert o_key == "b"
    assert o_type == "biolink:Disease"
    assert o_curie is None
    assert predicate == "biolink:treats"
    assert qualifiers == {"biolink:object_aspect_qualifier": "activity"}


def test_get_params_no_qualifier_constraints_returns_empty_dict():
    qg = {
        "nodes": {
            "a": {"categories": ["biolink:ChemicalEntity"], "ids": ["CHEBI:1"]},
            "b": {"categories": ["biolink:Disease"]},
        },
        "edges": {
            "e0": {
                "subject": "a",
                "object": "b",
                "predicates": ["biolink:treats"],
            }
        },
    }
    *_, qualifiers = get_params(qg)
    assert qualifiers == {}


# --- match_templates -------------------------------------------------------


def test_match_templates_returns_paths_for_drug_treats_disease():
    """A subject/object/predicate combo that the production
    ``template_groups.json`` lists as Drug-treats-Disease should match
    actual template files on disk."""
    paths = match_templates(
        subject_type="biolink:Drug",
        object_type="biolink:Disease",
        predicate="biolink:treats",
        qualifiers={},
        logger=logger,
    )
    # Should at least find one template for the Drug-treats-Disease group.
    assert paths
    assert all(p.suffix == ".json" for p in paths)


def test_match_templates_no_match_returns_empty():
    """Nonsense types should not match any group."""
    paths = match_templates(
        subject_type="biolink:NotAThing",
        object_type="biolink:NotAThing",
        predicate="biolink:not_real",
        qualifiers={},
        logger=logger,
    )
    assert paths == []


def test_match_templates_strips_biolink_prefix_when_matching():
    """The matcher removes the ``biolink:`` prefix before checking
    template_groups.json (which lists bare names like 'Drug')."""
    no_prefix = match_templates(
        subject_type="Drug",
        object_type="Disease",
        predicate="treats",
        qualifiers={},
        logger=logger,
    )
    with_prefix = match_templates(
        subject_type="biolink:Drug",
        object_type="biolink:Disease",
        predicate="biolink:treats",
        qualifiers={},
        logger=logger,
    )
    # Both forms should yield the same set of templates.
    assert {p.name for p in no_prefix} == {p.name for p in with_prefix}


# --- fill_templates --------------------------------------------------------


def test_fill_templates_substitutes_subject_curie():
    """When the subject curie is provided, templates substitute source_id and
    delete the target's ids."""
    paths = match_templates(
        subject_type="biolink:Drug",
        object_type="biolink:Disease",
        predicate="biolink:treats",
        qualifiers={},
        logger=logger,
    )
    assert paths
    filled = fill_templates(
        paths=[paths[0]],
        query_body={"parameters": {"timeout": 60}, "submitter": "test"},
        subject_key="SN",
        subject_curie="CHEBI:1",
        object_key="ON",
        object_curie=None,
    )
    assert len(filled) == 1
    qg = filled[0]["message"]["query_graph"]
    # Source got the curie; target has no ids
    assert qg["nodes"]["SN"]["ids"] == ["CHEBI:1"]
    assert "ids" not in qg["nodes"]["ON"]
    assert filled[0]["workflow"] == [{"id": "lookup"}]
    assert filled[0]["submitter"] == "test"


def test_fill_templates_substitutes_object_curie_when_subject_unset():
    """The mirror direction: object pinned, subject empty."""
    paths = match_templates(
        subject_type="biolink:Drug",
        object_type="biolink:Disease",
        predicate="biolink:treats",
        qualifiers={},
        logger=logger,
    )
    filled = fill_templates(
        paths=[paths[0]],
        query_body={"parameters": {"timeout": 60}, "submitter": "test"},
        subject_key="SN",
        subject_curie=None,
        object_key="ON",
        object_curie="MONDO:1",
    )
    qg = filled[0]["message"]["query_graph"]
    assert qg["nodes"]["ON"]["ids"] == ["MONDO:1"]
    assert "ids" not in qg["nodes"]["SN"]


# --- expand_bte_query ------------------------------------------------------


def test_expand_bte_query_returns_empty_for_missing_query_graph():
    out = expand_bte_query({"message": {}}, logger)
    assert out == []


def test_expand_bte_query_includes_direct_query_first():
    """The first message should be the direct (non-inferred) version of the
    query; subsequent messages are template expansions."""
    msg = copy.deepcopy(creative_query)
    msg["message"]["query_graph"]["nodes"]["SN"]["categories"] = [
        "biolink:Drug",
    ]
    msg["message"]["query_graph"]["nodes"]["ON"]["categories"] = [
        "biolink:Disease",
    ]
    msg["parameters"] = {"timeout": 60}
    msg["submitter"] = "test"
    out = expand_bte_query(msg, logger)
    assert len(out) >= 1
    direct = out[0]
    # Direct query has had knowledge_type stripped from each edge
    assert "knowledge_type" not in direct["message"]["query_graph"]["edges"]["e0"]


# --- AsyncResponse dataclass ----------------------------------------------


def test_async_response_dataclass_defaults():
    r = AsyncResponse(status_code=200, success=True, callback_id="abc")
    assert r.error is None
    assert r.success is True


# --- bte_lookup entry point -----------------------------------------------


def _make_task():
    return [
        "test",
        {
            "query_id": "qid",
            "response_id": "rid",
            "workflow": json.dumps([{"id": "bte.lookup"}]),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]


@pytest.mark.asyncio
async def test_bte_lookup_pure_lookup_calls_kg_retrieval(redis_mock, mocker):
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        },
        "parameters": {"timeout": 5},
    }
    mocker.patch(
        "workers.bte_lookup.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    mocker.patch(
        "workers.bte_lookup.worker.add_callback_id",
        new_callable=mocker.AsyncMock,
    )
    mocker.patch(
        "workers.bte_lookup.worker.get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    mock_post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    await bte_lookup(_make_task(), logger)
    assert mock_post.called


@pytest.mark.asyncio
async def test_bte_lookup_rejects_pathfinder_query(redis_mock, mocker):
    pathfinder_msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}, "c": {}, "d": {}},
                "edges": {
                    "e0": {"subject": "a", "object": "b", "knowledge_type": "inferred"},
                    "e1": {"subject": "b", "object": "c", "knowledge_type": "inferred"},
                    "e2": {"subject": "c", "object": "d", "knowledge_type": "inferred"},
                },
            }
        },
        "parameters": {"timeout": 5},
    }
    mocker.patch(
        "workers.bte_lookup.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=pathfinder_msg,
    )
    with pytest.raises(Exception, match="does not support Pathfinder"):
        await bte_lookup(_make_task(), logger)

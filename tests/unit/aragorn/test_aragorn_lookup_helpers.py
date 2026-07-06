"""Tests for the pure helpers in ``workers.aragorn_lookup.worker``.

Covers ``examine_query``, ``get_infer_parameters``, ``get_rule_key``, and
``expand_aragorn_query``. The async ``aragorn_lookup`` entrypoint is already
covered in ``test_aragorn_lookup.py`` for the creative path; here we add
coverage for the non-infer / gandalf branches by mocking out the network.
"""

import copy
import json
import logging

import pytest

from tests.helpers.generate_messages import creative_query
from workers.aragorn_lookup import worker as lookup_worker
from workers.aragorn_lookup.worker import (
    aragorn_lookup,
    examine_query,
    expand_aragorn_query,
    get_infer_parameters,
    get_rule_key,
)

logger = logging.getLogger(__name__)


def test_examine_query_lookup_no_pinned_required():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        }
    }
    infer, q, a, pathfinder = examine_query(msg)
    assert (infer, pathfinder) == (False, False)
    assert q is None and a is None


def test_examine_query_inferred_returns_question_and_answer():
    msg = copy.deepcopy(creative_query)
    infer, q, a, pathfinder = examine_query(msg)
    assert infer is True
    assert pathfinder is False
    assert (q, a) == ("ON", "SN")


def test_examine_query_pathfinder_three_inferred_edges():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}, "c": {}, "d": {}},
                "edges": {
                    "e0": {"subject": "a", "object": "b", "knowledge_type": "inferred"},
                    "e1": {"subject": "b", "object": "c", "knowledge_type": "inferred"},
                    "e2": {"subject": "c", "object": "d", "knowledge_type": "inferred"},
                },
            }
        }
    }
    _, _, _, pathfinder = examine_query(msg)
    assert pathfinder is True


def test_examine_query_rejects_two_inferred_when_not_pathfinder():
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {
                    "e0": {"subject": "a", "object": "b", "knowledge_type": "inferred"},
                    "e1": {"subject": "a", "object": "b", "knowledge_type": "inferred"},
                },
            }
        }
    }
    with pytest.raises(Exception, match="single infer edge"):
        examine_query(msg)


def test_get_infer_parameters_extracts_source_input_form():
    """A creative query with the subject pinned: source_input=True, input_id is the
    subject's id."""
    msg = copy.deepcopy(creative_query)
    # creative_query pins ON (the object). Force the subject to be pinned for this case.
    msg["message"]["query_graph"]["nodes"]["SN"]["ids"] = ["CHEBI:1"]
    msg["message"]["query_graph"]["nodes"]["ON"].pop("ids", None)
    input_id, predicate, qualifiers, source, source_input, target, qedge = (
        get_infer_parameters(msg)
    )
    assert input_id == "CHEBI:1"
    assert predicate == "biolink:treats"
    assert qualifiers == {}
    assert source == "SN"
    assert target == "ON"
    assert source_input is True
    assert qedge == "e0"


def test_get_infer_parameters_extracts_target_input_form():
    """When the object is pinned (creative_query default): source_input=False."""
    msg = copy.deepcopy(creative_query)
    input_id, _, _, _, source_input, _, _ = get_infer_parameters(msg)
    assert source_input is False
    assert input_id == "MONDO:0001"


def test_get_infer_parameters_with_qualifier_constraints():
    msg = copy.deepcopy(creative_query)
    msg["message"]["query_graph"]["edges"]["e0"]["qualifier_constraints"] = [
        {
            "qualifier_set": [
                {
                    "qualifier_type_id": "biolink:object_aspect_qualifier",
                    "qualifier_value": "activity",
                }
            ]
        }
    ]
    _, _, qualifiers, _, _, _, _ = get_infer_parameters(msg)
    assert qualifiers == {
        "qualifier_constraints": [
            {
                "qualifier_set": [
                    {
                        "qualifier_type_id": "biolink:object_aspect_qualifier",
                        "qualifier_value": "activity",
                    }
                ]
            }
        ]
    }


def test_get_rule_key_no_qualifiers_returns_predicate_only():
    key = get_rule_key("biolink:treats", {}, logger)
    assert json.loads(key) == {"predicate": "biolink:treats"}


def test_get_rule_key_with_aspect_and_direction():
    qualifiers = {
        "qualifier_constraints": [
            {
                "qualifier_set": [
                    {
                        "qualifier_type_id": "biolink:object_aspect_qualifier",
                        "qualifier_value": "activity",
                    },
                    {
                        "qualifier_type_id": "biolink:object_direction_qualifier",
                        "qualifier_value": "increased",
                    },
                ]
            }
        ]
    }
    key = get_rule_key("biolink:affects", qualifiers, logger)
    assert json.loads(key) == {
        "object_aspect_qualifier": "activity",
        "object_direction_qualifier": "increased",
        "predicate": "biolink:affects",
    }


def test_get_rule_key_empty_qualifier_constraints_falls_back_to_predicate():
    """If qualifier_constraints is an empty list, only predicate ends up in the key."""
    key = get_rule_key("biolink:treats", {"qualifier_constraints": []}, logger)
    assert json.loads(key) == {"predicate": "biolink:treats"}


def test_get_rule_key_empty_qualifier_set_falls_back_to_predicate():
    key = get_rule_key(
        "biolink:treats", {"qualifier_constraints": [{"qualifier_set": []}]}, logger
    )
    assert json.loads(key) == {"predicate": "biolink:treats"}


def test_expand_aragorn_query_includes_direct_query_with_no_expansions(mocker):
    """Without any matching AMIE rule, expand_aragorn_query still emits a single
    direct (non-inferred) query."""
    mocker.patch(
        "workers.aragorn_lookup.worker.json.load",
        return_value={},  # empty AMIE expansions
    )
    msg = copy.deepcopy(creative_query)
    msg["parameters"] = {"timeout": 60, "tiers": [0]}
    msg["submitter"] = "test"
    out = expand_aragorn_query(msg, logger)
    assert len(out) == 1
    direct = out[0]
    assert "knowledge_type" not in direct["message"]["query_graph"]["edges"]["e0"]


def test_expand_aragorn_query_appends_amie_rule_template(mocker):
    """When AMIE has a rule matching the query key, an extra expanded message is
    appended."""
    msg = copy.deepcopy(creative_query)
    msg["parameters"] = {"timeout": 60, "tiers": [0]}
    msg["submitter"] = "test"

    # A trivial 1-edge expansion template; the worker expects the template to
    # contain a top-level "query_graph" key.
    rule_template = {
        "query_graph": {
            "nodes": {
                "$source": {
                    "categories": ["biolink:ChemicalEntity"],
                    "ids": ["$source_id"],
                },
                "$target": {
                    "categories": ["biolink:DiseaseOrPhenotypicFeature"],
                    "ids": ["$target_id"],
                },
            },
            "edges": {
                "expanded": {
                    "subject": "$source",
                    "object": "$target",
                    "predicates": ["biolink:related_to"],
                }
            },
        }
    }

    mocker.patch(
        "workers.aragorn_lookup.worker.json.load",
        return_value={
            json.dumps({"predicate": "biolink:treats"}): [{"template": rule_template}],
        },
    )

    out = expand_aragorn_query(msg, logger)
    # Direct query + one AMIE expansion.
    assert len(out) == 2
    expanded = out[1]
    qg = expanded["message"]["query_graph"]
    # Target was the unpinned node; its IDs list should have been removed.
    assert "ids" not in qg["nodes"]["SN"]
    # Source (pinned input) keeps its CURIE through template substitution.
    assert qg["nodes"]["ON"]["ids"] == ["MONDO:0001"]


@pytest.mark.asyncio
async def test_aragorn_lookup_handles_examine_query_failure(redis_mock, mocker):
    """When examine_query raises (e.g. mixed query), aragorn_lookup logs and
    returns ``(None, 500)`` rather than propagating the exception."""
    bad_message = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {
                    "e0": {"subject": "a", "object": "b", "knowledge_type": "inferred"},
                    "e1": {"subject": "a", "object": "b"},  # mixed -> raises
                },
            }
        }
    }
    mocker.patch(
        "workers.aragorn_lookup.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=bad_message,
    )

    task = [
        "test",
        {
            "query_id": "test",
            "response_id": "test_response",
            "workflow": json.dumps([{"id": "aragorn.lookup"}]),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]
    out = await aragorn_lookup(task, logger)
    assert out == (None, 500)


@pytest.mark.asyncio
async def test_aragorn_lookup_pure_lookup_path_calls_kg_retrieval(redis_mock, mocker):
    """A non-inferred query (no creative work needed) goes straight to the kg
    retrieval URL without expansion."""
    msg = {
        "message": {
            "query_graph": {
                "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
                "edges": {"e0": {"subject": "a", "object": "b"}},
            }
        },
        # Non-zero timeout so the polling loop body executes once and breaks
        # when get_running_callbacks returns [].
        "parameters": {"timeout": 5},
    }
    mocker.patch(
        "workers.aragorn_lookup.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=msg,
    )
    mocker.patch(
        "workers.aragorn_lookup.worker.add_callback_id",
        new_callable=mocker.AsyncMock,
    )
    mock_running = mocker.patch(
        "workers.aragorn_lookup.worker.get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    mock_post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )

    task = [
        "test",
        {
            "query_id": "test",
            "response_id": "test_response",
            "workflow": json.dumps([{"id": "aragorn.lookup"}]),
            "log_level": "20",
            "otel": json.dumps({}),
        },
    ]
    await aragorn_lookup(task, logger)
    assert mock_post.called
    assert mock_running.called

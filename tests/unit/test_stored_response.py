"""Tests for the stored form of a query's response.

A stored response never carries the TRAPI delivery envelope (versions,
parameters, logs) or query-only members (callback, submitter, ...):
``finish_query`` writes the envelope around the stored bytes without decoding
them, so every write of a response goes through ``save_response`` /
``save_response_sync``, and ``add_query`` seeds the response in that form.
"""

import logging

import pytest

from shepherd_utils import db
from shepherd_utils.trapi import ENVELOPE_MEMBERS, prepare_stored_response

logger = logging.getLogger(__name__)

QUERY_GRAPH = {
    "nodes": {"n0": {"ids": ["X:1"]}, "n1": {}},
    "edges": {"e0": {"subject": "n0", "object": "n1"}},
}


def _full_response():
    """A response carrying everything the stored form must drop."""
    return {
        "schema_version": "2.0.0",
        "biolink_version": "4.4.4",
        "parameters": {"timeout": 30, "log_level": "DEBUG"},
        "logs": [],
        "callback": "http://callback",
        "submitter": "infores:someone",
        "log_level": "DEBUG",
        "bypass_cache": True,
        "status": "Success",
        "workflow": [{"id": "lookup"}],
        "message": {
            "query_graph": QUERY_GRAPH,
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "results": [
                {
                    "node_bindings": {"n0": {"ids": ["X:1"]}},
                    "analyses": [
                        {"resource_id": "infores:x", "edge_bindings": {}},
                    ],
                }
            ],
            "auxiliary_graphs": {},
        },
    }


def _assert_stored_form(stored):
    for member in (
        *ENVELOPE_MEMBERS,
        "callback",
        "submitter",
        "log_level",
        "bypass_cache",
    ):
        assert member not in stored, member
    # Response members that are not envelope are kept.
    assert stored["workflow"] == [{"id": "lookup"}]
    assert stored["status"] == "Success"
    # ...and the content is pruned of what 2.0 forbids.
    message = stored["message"]
    assert "auxiliary_graphs" not in message
    assert "analyses" not in message["results"][0]
    assert message["results"][0]["node_bindings"] == {"n0": {"ids": ["X:1"]}}


def test_prepare_stored_response_strips_envelope_and_prunes():
    response = _full_response()
    assert prepare_stored_response(response) is response
    _assert_stored_form(response)


@pytest.mark.asyncio
async def test_save_response_stores_the_stored_form(redis_mock):
    await db.save_response("rid", _full_response(), logger)
    _assert_stored_form(await db.get_message("rid", logger))


def test_save_response_sync_stores_the_stored_form(mocker):
    storage = {}
    sync_client = mocker.Mock()
    sync_client.set.side_effect = lambda key, blob, ex=None: storage.__setitem__(
        key, blob
    )
    sync_client.get.side_effect = lambda key: storage.get(key)
    mocker.patch("shepherd_utils.db._get_sync_data_db", return_value=sync_client)

    db.save_response_sync("rid", _full_response())

    _assert_stored_form(db.get_message_sync("rid"))


@pytest.mark.asyncio
async def test_add_query_keeps_query_members_on_the_query_only(redis_mock, mocker):
    """The response blob starts as the query's message, without the query's
    parameters / callback / submitter (added back, or not at all, on
    delivery); the query blob keeps everything. ``workflow`` is a Response
    member too, so it stays on both."""
    from .test_db_postgres import _install_pool_mock

    _install_pool_mock(mocker)
    query = {
        "message": {"query_graph": QUERY_GRAPH},
        "parameters": {"timeout": 30},
        "callback": "http://callback",
        "submitter": "infores:someone",
        "workflow": [{"id": "lookup"}],
    }
    await db.add_query("qid", "rid", query, "http://callback", logger)

    stored_query = await db.get_message("qid", logger)
    assert stored_query == query
    stored_response = await db.get_message("rid", logger)
    assert stored_response == {
        "message": {"query_graph": QUERY_GRAPH},
        "workflow": [{"id": "lookup"}],
    }
    # The query dict the caller handed in is not modified.
    assert query["parameters"] == {"timeout": 30}

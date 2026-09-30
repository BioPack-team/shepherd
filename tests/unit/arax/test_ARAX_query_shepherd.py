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

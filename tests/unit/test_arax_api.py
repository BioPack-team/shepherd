"""Tests for Shepherd's ARAX API (``shepherd_server/aras/arax.py``).

The streaming tests connect the real arax worker -- running ARAX's own
``query_return_stream`` on an ARAXi plan that needs no KP -- to the server's
relay through an in-memory stand-in for the Redis progress list.
"""

import json
import logging
import time

import pytest

import shepherd_server.aras.arax as api
import workers.arax.worker as worker
from shepherd_utils.arax_progress import DONE_MARKER

logger = logging.getLogger(__name__)

PLAN = {
    "operations": {
        "actions": [
            "add_qnode(key=n0, categories=biolink:SmallMolecule)",
            "add_qnode(key=n1, categories=biolink:Disease)",
            "add_qedge(key=e0, subject=n0, object=n1)",
            "return(message=true, store=true)",
        ]
    }
}


def _row(state="COMPLETED", status="OK"):
    """A shepherd_brain row, in the column order get_query_state returns."""
    return ("qid", None, None, None, None, None, None, "rid", None, state, status, None)


class FakeShepherd:
    """The data store and progress list both sides share."""

    def __init__(self, mocker, query):
        self.messages = {"qid": json.loads(json.dumps(query))}
        self.progress = []
        self.state = _row(state="QUEUED")
        # worker side
        mocker.patch.object(worker, "get_message_sync", side_effect=self._get)
        mocker.patch.object(worker, "save_message_sync", side_effect=self._save)
        mocker.patch.object(worker, "push_progress", side_effect=self._push)
        mocker.patch.object(
            worker,
            "finish_progress",
            side_effect=lambda rid: self._push(rid, DONE_MARKER),
        )
        # server side
        mocker.patch.object(
            api,
            "run_query",
            new_callable=mocker.AsyncMock,
            return_value=("qid", "rid", logger),
        )
        mocker.patch.object(api, "read_progress", side_effect=self._read)
        mocker.patch.object(api, "get_query_state", side_effect=self._state)
        mocker.patch(
            "shepherd_server.base_routes.get_query_state", side_effect=self._state
        )
        mocker.patch.object(api, "get_message", side_effect=self._aget)
        mocker.patch.object(
            api,
            "get_logs",
            new_callable=mocker.AsyncMock,
            return_value=[{"shepherd": 1}],
        )
        mocker.patch(
            "shepherd_server.base_routes.asyncio.sleep", new_callable=mocker.AsyncMock
        )
        mocker.patch.object(api.asyncio, "sleep", new_callable=mocker.AsyncMock)

    def _get(self, key):
        return json.loads(json.dumps(self.messages[key]))

    def _save(self, key, message):
        self.messages[key] = json.loads(json.dumps(message))

    def _push(self, rid, line):
        assert rid == "rid"
        self.progress.append(line)

    async def _read(self, rid, start):
        return self.progress[start:]

    async def _state(self, qid, logger):
        return self.state

    async def _aget(self, key, logger):
        return self._get(key) if key in self.messages else None

    def run_worker(self):
        summary = worker.arax_query_task("qid", "rid")
        self.state = _row()
        return summary


async def _collect(response):
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk if isinstance(chunk, str) else chunk.decode())
    return "".join(chunks).splitlines()


@pytest.mark.asyncio
async def test_stream_relays_arax_progress_then_the_response(mocker):
    shepherd = FakeShepherd(mocker, dict(PLAN, stream_progress=True))
    # Let the stream loop start before the query finishes (see the next test)
    from shepherd_utils.arax.ARAX_query import ARAXQuery

    query = ARAXQuery.query

    def slow_query(self, *args, **kwargs):
        time.sleep(0.3)
        return query(self, *args, **kwargs)

    mocker.patch.object(ARAXQuery, "query", slow_query)
    summary = shepherd.run_worker()
    assert summary["http_status"] == 200
    assert shepherd.progress[-1] == DONE_MARKER

    response = await api.arax_stream_query(dict(PLAN, stream_progress=True))
    assert response.status_code == 200
    assert response.media_type == "text/event-stream"
    lines = [json.loads(line) for line in await _collect(response)]

    # ARAX's own stream: log entries and the pid token (query_plan updates too,
    # when a plan has any) ...
    messages = [x["message"] for x in lines if "message" in x and "level" in x]
    assert any("Processing action 'add_qedge'" in m for m in messages)
    tokens = [x for x in lines if set(x) == {"pid", "authorization"}]
    assert len(tokens) == 1
    # ... then the response, as saved
    final = lines[-1]
    assert final["status"] == "Success"
    assert set(final["message"]["query_graph"]["nodes"]) == {"n0", "n1"}
    assert final == shepherd.messages["rid"]
    # nothing but the done marker is left out
    assert len(lines) == len(shepherd.progress)


@pytest.mark.asyncio
async def test_stream_of_a_failed_query_ends_with_arax_error_envelope(mocker):
    query = {"stream_progress": True, "submitter": "tester"}
    shepherd = FakeShepherd(mocker, query)
    summary = shepherd.run_worker()
    assert summary["http_status"] == 400

    lines = [
        json.loads(line) for line in await _collect(await api.arax_stream_query(query))
    ]
    # ARAX's stream yields nothing when its query thread finishes before the
    # stream loop starts (as an input error does), so only the response comes
    final = lines[-1]
    assert final["tool_version"].startswith("ARAX ")
    assert any(
        x.get("code") == "NoQueryMessageOrOperations" and x.get("level") == "ERROR"
        for x in final["logs"]
    )


@pytest.mark.asyncio
async def test_stream_without_worker_progress_ends_when_the_query_does(mocker):
    """A pathfinder query goes to arax.pathfinder, which relays nothing."""
    shepherd = FakeShepherd(mocker, {})
    shepherd.messages["rid"] = {"message": {"results": [1]}}
    shepherd.state = _row()
    lines = await _collect(await api.arax_stream_query({"stream_progress": True}))
    assert [json.loads(line) for line in lines] == [
        {"logs": [{"shepherd": 1}], "message": {"results": [1]}}
    ]


@pytest.mark.asyncio
async def test_stream_times_out(mocker):
    FakeShepherd(mocker, {})
    lines = await _collect(
        await api.arax_stream_query(
            {"stream_progress": True, "parameters": {"timeout": -1}}
        )
    )
    assert json.loads(lines[-1]) == {
        "status": "TIMEOUT",
        "description": "Query timeout",
    }


@pytest.mark.asyncio
async def test_sync_query_returns_arax_envelope_logs_and_status(mocker):
    shepherd = FakeShepherd(mocker, PLAN)
    shepherd.run_worker()
    response = await api.arax_sync_query(dict(PLAN))
    body = json.loads(bytes(response.body))
    assert response.status_code == 200
    assert body["http_status"] == 200
    # ARAX's own log, not Shepherd's
    assert any("Processing action" in entry["message"] for entry in body["logs"])


@pytest.mark.asyncio
async def test_sync_query_arax_error_keeps_arax_http_status(mocker):
    query = {"submitter": "tester"}
    shepherd = FakeShepherd(mocker, query)
    shepherd.run_worker()
    shepherd.state = _row(status="ERROR")
    response = await api.arax_sync_query(query)
    body = json.loads(bytes(response.body))
    assert response.status_code == 400
    assert body["status"] == "NoQueryMessageOrOperations"


@pytest.mark.asyncio
async def test_sync_query_non_arax_response_is_finished_like_shepherds(mocker):
    shepherd = FakeShepherd(mocker, {})
    shepherd.messages["rid"] = {"message": {}, "status": "Error", "description": "x"}
    shepherd.state = _row(status="ERROR")
    response = await api.arax_sync_query({})
    body = json.loads(bytes(response.body))
    assert response.status_code == 500
    assert body["logs"] == [{"shepherd": 1}]

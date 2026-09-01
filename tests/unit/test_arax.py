"""Tests for ``workers.arax.worker``.

Focused on what happens to the status code ARAX answers with: it used to be
logged and then dropped -- every failure became the same ``{"status": "error"}``
blob that the workflow reported as a success -- so nothing downstream had a
status code to report.
"""

import json
import logging

import httpx
import pytest

from workers.arax.worker import (
    BAD_GATEWAY,
    GATEWAY_TIMEOUT,
    ARAXServiceError,
    arax,
)

logger = logging.getLogger(__name__)

QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"a": {"ids": ["MONDO:0005148"]}, "b": {}},
            "edges": {"e0": {"subject": "a", "object": "b"}},
        }
    }
}

PATHFINDER_QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"a": {"ids": ["MONDO:0005148"]}, "b": {"ids": ["CHEBI:15365"]}},
            "paths": {"p0": {"subject": "a", "object": "b"}},
        }
    }
}

ARAX_RESPONSE = {
    "message": {
        "query_graph": QUERY["message"]["query_graph"],
        "knowledge_graph": {
            "nodes": {"MONDO:0005148": {}},
            "edges": {"e0": {"subject": "a", "object": "b"}},
        },
        "results": [],
    }
}


def _task():
    return [
        "task_id",
        {
            "query_id": "query_id",
            "response_id": "response_id",
            "workflow": json.dumps([{"id": "arax"}]),
            "log_level": "20",
            "otel": json.dumps({}),
            "metadata": json.dumps({}),
        },
    ]


def _patch_db(mocker, message=None):
    """Stub the two db calls the worker makes, returning the save mock."""
    mocker.patch(
        "workers.arax.worker.get_message",
        new_callable=mocker.AsyncMock,
        return_value=message if message is not None else dict(QUERY),
    )
    return mocker.patch(
        "workers.arax.worker.save_message",
        new_callable=mocker.AsyncMock,
    )


def _patch_post(mocker, response=None, side_effect=None):
    return mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=response,
        side_effect=side_effect,
    )


def _patch_span(mocker):
    span = mocker.MagicMock()
    mocker.patch("workers.arax.worker.get_current_span", return_value=span)
    return span


def _http_response(status_code, json_body=None, text=None):
    """A real httpx.Response, so is_success/.json()/.content behave as in prod."""
    request = httpx.Request("POST", "https://arax.example/query")
    if json_body is not None:
        return httpx.Response(status_code, json=json_body, request=request)
    return httpx.Response(status_code, text=text or "", request=request)


@pytest.mark.asyncio
async def test_successful_query_saves_response_and_advances_workflow(mocker):
    save = _patch_db(mocker)
    span = _patch_span(mocker)
    _patch_post(mocker, _http_response(200, json_body=ARAX_RESPONSE))
    task = _task()

    await arax(task, logger)

    span.set_attribute.assert_any_call("arax.status_code", 200)
    saved = save.await_args.args[1]
    assert saved["message"]["results"] == []
    # Provenance is still injected on the way through.
    assert saved["message"]["knowledge_graph"]["edges"]["e0"]["sources"] == [
        {
            "resource_id": "infores:shepherd-arax",
            "resource_role": "aggregator_knowledge_source",
            "source_record_urls": None,
            "upstream_resource_ids": ["infores:arax"],
        }
    ]
    assert json.loads(task[1]["workflow"]) == [{"id": "arax"}]


@pytest.mark.parametrize("status_code", [400, 404, 429, 500, 502])
@pytest.mark.asyncio
async def test_error_status_is_propagated(mocker, status_code):
    """ARAX's own status code reaches the exception, the span and the response."""
    save = _patch_db(mocker)
    span = _patch_span(mocker)
    _patch_post(mocker, _http_response(status_code, text="upstream said no"))

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    assert excinfo.value.status_code == status_code
    assert f"HTTP {status_code}" in str(excinfo.value)
    assert "upstream said no" in str(excinfo.value)
    span.set_attribute.assert_any_call("arax.status_code", status_code)

    saved = save.await_args.args[1]
    assert saved["status"] == "Error"
    assert f"[HTTP {status_code}]" in saved["description"]
    # Still a TRAPI response for the query that was asked.
    assert saved["message"]["query_graph"] == QUERY["message"]["query_graph"]
    assert saved["message"]["results"] == []


@pytest.mark.asyncio
async def test_error_body_is_truncated(mocker):
    _patch_db(mocker)
    _patch_span(mocker)
    _patch_post(mocker, _http_response(500, text="x" * 5000))

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    message = str(excinfo.value)
    assert "x" * 500 in message and "x" * 501 not in message


@pytest.mark.asyncio
async def test_timeout_reports_gateway_timeout(mocker):
    save = _patch_db(mocker)
    span = _patch_span(mocker)
    _patch_post(mocker, side_effect=httpx.ReadTimeout("timed out"))

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    assert excinfo.value.status_code == GATEWAY_TIMEOUT
    span.set_attribute.assert_any_call("arax.status_code", GATEWAY_TIMEOUT)
    assert f"[HTTP {GATEWAY_TIMEOUT}]" in save.await_args.args[1]["description"]


@pytest.mark.asyncio
async def test_transport_error_reports_bad_gateway(mocker):
    save = _patch_db(mocker)
    span = _patch_span(mocker)
    _patch_post(mocker, side_effect=httpx.ConnectError(""))

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    assert excinfo.value.status_code == BAD_GATEWAY
    # httpx.ConnectError stringifies to nothing, so the class name carries it.
    assert "ConnectError" in str(excinfo.value)
    span.set_attribute.assert_any_call("arax.status_code", BAD_GATEWAY)
    assert f"[HTTP {BAD_GATEWAY}]" in save.await_args.args[1]["description"]


@pytest.mark.asyncio
async def test_unparseable_success_body_keeps_the_status_code(mocker):
    save = _patch_db(mocker)
    _patch_span(mocker)
    _patch_post(mocker, _http_response(200, text="<html>not json</html>"))

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    assert excinfo.value.status_code == 200
    assert "[HTTP 200]" in save.await_args.args[1]["description"]


@pytest.mark.asyncio
async def test_pathfinder_query_is_routed_without_calling_arax(mocker):
    save = _patch_db(mocker, message=dict(PATHFINDER_QUERY))
    post = _patch_post(mocker, _http_response(200, json_body=ARAX_RESPONSE))
    task = _task()

    await arax(task, logger)

    assert not post.called
    assert not save.called
    assert json.loads(task[1]["workflow"]) == [{"id": "arax.pathfinder"}]


# --- TRAPI-level failures reported inside a 200 -----------------------------
#
# ARAX answers HTTP 200 for most of its own failures and puts the failure in
# the TRAPI status field, so checking the HTTP code alone reports every one of
# them as a healthy query.


def _trapi(status=None, description=None):
    body = {"message": {"query_graph": {}, "knowledge_graph": {}, "results": []}}
    if status is not None:
        body["status"] = status
    if description is not None:
        body["description"] = description
    return body


@pytest.mark.parametrize("status", ["Error", "ERROR", "InternalError", "Failed"])
@pytest.mark.asyncio
async def test_trapi_error_status_in_a_200_fails_the_query(mocker, status):
    save = _patch_db(mocker)
    span = _patch_span(mocker)
    _patch_post(
        mocker,
        _http_response(200, json_body=_trapi(status, "internal issues upstream")),
    )
    task = _task()

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(task, logger)

    assert excinfo.value.status_code == 200
    assert status in str(excinfo.value)
    assert "internal issues upstream" in str(excinfo.value)
    span.set_attribute.assert_any_call("arax.trapi_status", status)
    # ARAX's own body is what the caller gets -- its status and description say
    # more than anything we could synthesize.
    assert save.await_args.args[1]["status"] == status
    assert save.await_args.args[1]["description"] == "internal issues upstream"


@pytest.mark.parametrize("status", ["Success", "OK", "QueryNotTraversable", None])
@pytest.mark.asyncio
async def test_non_error_trapi_status_still_succeeds(mocker, status):
    """Only statuses naming an error fail the query; the rest are outcomes."""
    save = _patch_db(mocker)
    _patch_span(mocker)
    _patch_post(mocker, _http_response(200, json_body=_trapi(status)))
    task = _task()

    await arax(task, logger)

    assert save.await_args.args[1].get("status") == status
    assert json.loads(task[1]["workflow"]) == [{"id": "arax"}]


@pytest.mark.asyncio
async def test_json_body_that_is_not_trapi_fails_the_query(mocker):
    save = _patch_db(mocker)
    _patch_span(mocker)
    _patch_post(mocker, _http_response(200, json_body={"detail": "Internal Error"}))

    with pytest.raises(ARAXServiceError) as excinfo:
        await arax(_task(), logger)

    assert excinfo.value.status_code == 200
    assert "not a TRAPI response" in str(excinfo.value)
    # Nothing usable came back, so the caller gets one we build.
    assert save.await_args.args[1]["status"] == "Error"
    assert "[HTTP 200]" in save.await_args.args[1]["description"]

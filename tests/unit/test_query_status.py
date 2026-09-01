"""Tests for how a query's outcome is reported back to the caller.

``/asyncquery_status/{qid}`` used to answer ``{"status": "Queued"}`` for every
query it was ever asked about (a hardcoded stub with a TODO), so a query that
failed was indistinguishable from a healthy one. The sync path had the milder
version of the same problem: it returned the stored response with no hint that
the query had finished with anything other than OK.
"""

import json
import logging

import pytest

from shepherd_server.base_routes import apply_query_status, query_status

logger = logging.getLogger(__name__)


def _row(state="QUEUED", status="OK", description=None):
    """A shepherd_brain row, in the column order get_query_state returns."""
    return (
        "qid",
        "start",
        "stop",
        "submitter",
        "ip",
        "domain",
        "hostname",
        "response_id",
        None,
        state,
        status,
        description,
    )


def _patch_state(mocker, row, logs=None):
    mocker.patch(
        "shepherd_server.base_routes.get_query_state",
        new_callable=mocker.AsyncMock,
        return_value=row,
    )
    mocker.patch(
        "shepherd_server.base_routes.get_logs",
        new_callable=mocker.AsyncMock,
        return_value=logs if logs is not None else [],
    )


def _body(response):
    return json.loads(bytes(response.body))


@pytest.mark.asyncio
async def test_status_unknown_query_is_not_found(mocker):
    _patch_state(mocker, None)
    response = await query_status("qid")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_status_in_flight_query_is_running(mocker):
    _patch_state(mocker, _row(state="QUEUED", status="OK"))
    assert _body(await query_status("qid"))["status"] == "Running"


@pytest.mark.asyncio
async def test_status_finished_query_is_completed(mocker):
    _patch_state(mocker, _row(state="COMPLETED", status="OK"))
    assert _body(await query_status("qid"))["status"] == "Completed"


@pytest.mark.parametrize(
    "state,status",
    [
        ("COMPLETED", "ERROR"),
        ("COMPLETED", "TIMEOUT"),
        ("ABANDONED", "Abandoned: no completion within budget"),
    ],
)
@pytest.mark.asyncio
async def test_status_failed_query_is_reported_as_failed(mocker, state, status):
    _patch_state(mocker, _row(state=state, status=status))
    body = _body(await query_status("qid"))
    assert body["status"] == "Failed"
    assert status in body["description"]


@pytest.mark.asyncio
async def test_status_carries_the_query_logs(mocker):
    """The logs are where the upstream status code is recorded."""
    logs = [{"level": "ERROR", "message": "ARAX service returned HTTP 500"}]
    _patch_state(mocker, _row(state="COMPLETED", status="ERROR"), logs=logs)
    assert _body(await query_status("qid"))["logs"] == logs


# --- apply_query_status ----------------------------------------------------


def test_ok_query_is_left_untouched():
    response = {"message": {}}
    apply_query_status(response, "OK")
    assert response == {"message": {}}


def test_failed_query_is_marked_on_the_response():
    response = {"message": {}}
    apply_query_status(response, "ERROR")
    assert response["status"] == "Error"
    assert "ERROR" in response["description"]


def test_ara_reported_error_is_not_overwritten():
    """ARAX's own status is more specific than the query-level one."""
    response = {"message": {}, "status": "InternalError", "description": "upstream"}
    apply_query_status(response, "ERROR")
    assert response["status"] == "InternalError"
    assert response["description"] == "upstream"


def test_success_claimed_for_a_failed_query_is_corrected():
    response = {"message": {}, "status": "Success"}
    apply_query_status(response, "TIMEOUT")
    assert response["status"] == "Error"

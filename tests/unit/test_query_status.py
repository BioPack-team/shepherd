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

from shepherd_server.base_routes import (
    ARATargetEnum,
    QueryIntakeError,
    apply_query_status,
    query_status,
    query_status_code,
    run_sync_query,
)

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


# --- /query ----------------------------------------------------------------
#
# The body has carried a TRAPI error status since apply_query_status went in,
# but the HTTP code stayed 200, so a caller checking the code rather than
# parsing the payload saw every failed query as a successful one.


def _patch_sync_query(mocker, row, response=None):
    mocker.patch(
        "shepherd_server.base_routes.run_query",
        new_callable=mocker.AsyncMock,
        return_value=("qid", "response_id", logger),
    )
    mocker.patch(
        "shepherd_server.base_routes.get_message",
        new_callable=mocker.AsyncMock,
        return_value=response,
    )
    _patch_state(mocker, row)


@pytest.mark.asyncio
async def test_query_returns_200_for_a_healthy_query(mocker):
    _patch_sync_query(
        mocker, _row(state="COMPLETED", status="OK"), response={"message": {}}
    )
    response = await run_sync_query(ARATargetEnum.ARAX, {"message": {}})
    assert response.status_code == 200
    assert "status" not in _body(response)


@pytest.mark.parametrize(
    "status,code",
    [
        # An operation failed: a genuine internal error.
        ("ERROR", 500),
        # Out of budget, or reaped without ever completing: the work behind
        # Shepherd didn't finish in time.
        ("TIMEOUT", 504),
        ("Abandoned: no completion within budget", 504),
    ],
)
@pytest.mark.asyncio
async def test_query_returns_the_code_for_how_it_failed(mocker, status, code):
    _patch_sync_query(
        mocker, _row(state="COMPLETED", status=status), response={"message": {}}
    )
    response = await run_sync_query(ARATargetEnum.ARAX, {"message": {}})
    assert response.status_code == code
    # The body still says which kind of failure it was.
    assert _body(response)["status"] == "Error"
    assert status in _body(response)["description"]


@pytest.mark.asyncio
async def test_query_returns_an_error_code_when_the_response_is_missing(mocker):
    _patch_sync_query(mocker, _row(state="COMPLETED", status="OK"), response=None)
    response = await run_sync_query(ARATargetEnum.ARAX, {"message": {}})
    assert response.status_code == 500
    assert _body(response)["description"] == "Unable to get response"


@pytest.mark.asyncio
async def test_query_returns_an_error_code_when_it_times_out(mocker):
    """The caller's own timeout elapsed with the query still in flight."""
    _patch_sync_query(mocker, _row(state="QUEUED", status="OK"))
    response = await run_sync_query(
        ARATargetEnum.ARAX, {"message": {}, "parameters": {"timeout": 0}}
    )
    assert response.status_code == 504
    assert _body(response)["status"] == "TIMEOUT"


@pytest.mark.asyncio
async def test_query_returns_unavailable_when_intake_fails(mocker):
    """The query was never accepted, so the caller can retry it as-is."""
    mocker.patch(
        "shepherd_server.base_routes.run_query",
        new_callable=mocker.AsyncMock,
        side_effect=QueryIntakeError("datastore unavailable"),
    )
    response = await run_sync_query(ARATargetEnum.ARAX, {"message": {}})
    assert response.status_code == 503
    assert "datastore unavailable" in _body(response)["description"]


@pytest.mark.parametrize(
    "status,code",
    [
        (None, 200),
        ("OK", 200),
        ("ERROR", 500),
        ("TIMEOUT", 504),
        ("Abandoned: no completion within budget", 504),
        ("something nobody writes today", 500),
    ],
)
def test_query_status_code_mapping(status, code):
    assert query_status_code(status) == code

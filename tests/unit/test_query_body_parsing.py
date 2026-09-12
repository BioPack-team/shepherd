"""Tests for /query and /asyncquery body parsing.

Both routes take the raw ``Request`` and parse it with orjson rather than
letting FastAPI do it, because Starlette's ``Request.json()`` goes through
stdlib ``json.loads`` -- slow on the populated knowledge graphs TRAPI bodies
carry, and blocking on the single event loop the server runs on.

Dropping the ``query: dict = Body(...)`` signature also dropped the checks
FastAPI ran for free -- content-type, valid JSON, and a JSON object -- so these
tests pin the replacements in ``parse_query_body`` and confirm the OpenAPI
request body the routes now declare by hand still matches what that signature
generated.
"""

from unittest import mock

import pytest
from fastapi.testclient import TestClient

from shepherd_server import base_routes
from shepherd_server.aras.aragorn import ARAGORN
from shepherd_server.aras.arax import ARAX
from shepherd_server.aras.bte import BTE
from shepherd_server.aras.sipr import SIPR
from shepherd_server.base_routes import (
    QueryBodyError,
    default_input_query,
    parse_query_body,
)

from .test_callback_size_limit import _make_request

ALL_APPS = (ARAGORN, ARAX, BTE, SIPR)


# --- parse_query_body ----------------------------------------------------


async def test_parses_a_json_object():
    request = _make_request(b'{"message": {"results": []}}', {})
    assert await parse_query_body(request) == {"message": {"results": []}}


@pytest.mark.parametrize("body", [b"{not json", b"", b'{"message":'])
async def test_rejects_malformed_json(body):
    with pytest.raises(QueryBodyError, match="not valid JSON"):
        await parse_query_body(_make_request(body, {}))


@pytest.mark.parametrize("body", [b"[1, 2, 3]", b'"hello"', b"42", b"null"])
async def test_rejects_non_object_json(body):
    # pydantic used to reject these for free via the ``dict`` annotation.
    # Without the check, they'd reach ``query.get(...)`` downstream and 500.
    with pytest.raises(QueryBodyError, match="expected a JSON object"):
        await parse_query_body(_make_request(body, {}))


@pytest.mark.parametrize(
    "content_type",
    [
        "application/json",
        # Casing and parameters are part of the header grammar, not the type.
        "APPLICATION/JSON",
        "application/json; charset=utf-8",
        # Structured suffix: what a TRAPI-specific media type would look like.
        "application/trapi+json",
    ],
)
async def test_accepts_json_content_types(content_type):
    request = _make_request(b'{"message": {}}', {"content-type": content_type})
    assert await parse_query_body(request) == {"message": {}}


async def test_accepts_a_missing_content_type():
    """FastAPI parsed a body with no content-type as JSON; so do we."""
    assert await parse_query_body(_make_request(b'{"message": {}}', {})) == {
        "message": {}
    }


@pytest.mark.parametrize(
    "content_type",
    ["text/plain", "application/x-www-form-urlencoded", "multipart/form-data"],
)
async def test_rejects_non_json_content_types(content_type):
    """``Body(...)`` only parsed application/json; keep that rather than
    silently accepting a form-encoded post that happens to hold JSON."""
    request = _make_request(b'{"message": {}}', {"content-type": content_type})
    with pytest.raises(QueryBodyError, match="content-type"):
        await parse_query_body(request)


# --- route behavior ------------------------------------------------------


@pytest.fixture
def client_factory():
    """Build a TestClient with query intake stubbed out.

    ``run_query`` is patched at the ``base_routes`` name the route handlers
    resolve, so nothing touches Redis or Postgres; the captured call lets the
    tests assert the parsed body arrived intact.
    """
    captured = {}

    def _factory(app):
        async def fake_run_query(target, query, callback_url=None):
            captured.update(target=target, query=query, callback_url=callback_url)
            return "qid12345", "rid12345", mock.MagicMock()

        patch = mock.patch.object(base_routes, "run_query", fake_run_query)
        patch.start()
        return TestClient(app), captured, patch

    yield _factory
    mock.patch.stopall()


@pytest.mark.parametrize("app", ALL_APPS)
def test_asyncquery_passes_the_parsed_body_through(app, client_factory):
    client, captured, _ = client_factory(app)
    body = dict(default_input_query, callback="http://callback/1")

    response = client.post("/asyncquery", json=body)

    assert response.status_code == 200
    assert response.json()["status"] == "Accepted"
    assert response.json()["job_id"] == "qid12345"
    # orjson round-trips the whole body, nested query graph included.
    assert captured["query"] == body
    assert captured["callback_url"] == "http://callback/1"


@pytest.mark.parametrize("app", ALL_APPS)
def test_asyncquery_still_requires_a_callback(app, client_factory):
    """The pre-existing missing-callback 422 is unchanged by the parser swap."""
    client, _, _ = client_factory(app)

    response = client.post("/asyncquery", json=default_input_query)

    assert response.status_code == 422
    assert response.json() == {
        "status": "Failed",
        "description": "callback URL missing",
    }


@pytest.mark.parametrize("path", ["/query", "/asyncquery"])
def test_malformed_body_returns_422(path, client_factory):
    client, captured, _ = client_factory(ARAGORN)

    response = client.post(
        path, content=b"{not json", headers={"content-type": "application/json"}
    )

    assert response.status_code == 422
    assert "not valid JSON" in response.json()["description"]
    # Rejected before intake, so no query was ever registered.
    assert captured == {}


@pytest.mark.parametrize("path", ["/query", "/asyncquery"])
def test_non_object_body_returns_422(path, client_factory):
    client, captured, _ = client_factory(ARAGORN)

    response = client.post(path, json=[1, 2, 3])

    assert response.status_code == 422
    assert "expected a JSON object" in response.json()["description"]
    assert captured == {}


@pytest.mark.parametrize("path", ["/query", "/asyncquery"])
def test_non_json_content_type_returns_422(path, client_factory):
    client, captured, _ = client_factory(ARAGORN)

    response = client.post(
        path,
        content=b'{"message": {}, "callback": "http://callback/1"}',
        headers={"content-type": "text/plain"},
    )

    assert response.status_code == 422
    assert "content-type" in response.json()["description"]
    assert captured == {}


def test_every_asyncquery_422_uses_one_body_shape(client_factory):
    """A caller shouldn't have to branch on which 422 it got.

    /asyncquery rejects a bad body and a missing callback with the same code;
    they used to answer in two different shapes, only one of which the
    hand-written OpenAPI 422 described.
    """
    client, _, _ = client_factory(ARAGORN)

    bad_body = client.post(
        "/asyncquery",
        content=b"{not json",
        headers={"content-type": "application/json"},
    )
    no_callback = client.post("/asyncquery", json=default_input_query)

    assert bad_body.status_code == no_callback.status_code == 422
    assert (
        sorted(bad_body.json())
        == sorted(no_callback.json())
        == [
            "description",
            "status",
        ]
    )
    assert bad_body.json()["status"] == no_callback.json()["status"] == "Failed"


# --- OpenAPI -------------------------------------------------------------


@pytest.mark.parametrize("app", ALL_APPS)
@pytest.mark.parametrize("path", ["/query", "/asyncquery"])
def test_openapi_still_documents_the_request_body(app, path):
    """The hand-written ``openapi_extra`` must match what ``Body(...)`` produced.

    Taking a raw ``Request`` leaves FastAPI nothing to infer a schema from, so
    an unnoticed regression here would silently publish a TRAPI endpoint with no
    documented request body.
    """
    operation = app.openapi()["paths"][path]["post"]

    request_body = operation["requestBody"]
    assert request_body["required"] is True
    schema = request_body["content"]["application/json"]["schema"]
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is True
    assert schema["examples"] == [default_input_query]

    assert "422" in operation["responses"]
    error_schema = operation["responses"]["422"]["content"]["application/json"][
        "schema"
    ]
    # Documents the shape every 422 these routes return actually uses -- the
    # body rejections and /asyncquery's missing-callback alike.
    assert error_schema["properties"] == {
        "status": {"type": "string"},
        "description": {"type": "string"},
    }


def test_each_route_gets_its_own_openapi_extra():
    """``query_openapi_extra`` hands out a fresh dict per route.

    Eight routes sharing one mutable module-level dict would let any in-place
    merge by FastAPI leak from one app's operation into every other's.
    """
    first, second = base_routes.query_openapi_extra(), base_routes.query_openapi_extra()
    assert first == second
    assert first is not second
    assert first["responses"] is not second["responses"]

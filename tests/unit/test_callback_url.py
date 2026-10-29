"""Tests for the /asyncquery callback-URL guard (SSRF, issue #180).

``validate_callback_url`` refuses callbacks Shepherd must not POST a response
to: non-http(s) schemes, hosts that resolve to private / loopback / link-local
addresses unless allowlisted, and cloud metadata addresses always. It runs at
intake (``run_async_query``) and again before ``finish_query`` delivers.
"""

import ipaddress
import logging
from unittest import mock

import pytest
from fastapi.testclient import TestClient

from shepherd_server import base_routes
from shepherd_server.aras.aragorn import ARAGORN
from shepherd_server.base_routes import default_input_query
from shepherd_utils import callback_url
from shepherd_utils.callback_url import (
    CallbackURLError,
    check_addresses,
    validate_callback_url,
)

pytestmark = pytest.mark.real_callback_dns

PUBLIC = "93.184.215.14"


def _resolving_to(*addresses):
    """Stub DNS so every hostname resolves to ``addresses``."""

    async def fake_resolve(host, port):
        try:
            return [ipaddress.ip_address(host)]
        except ValueError:
            return [ipaddress.ip_address(a) for a in addresses]

    return mock.patch.object(callback_url, "resolve_host", fake_resolve)


# --- The probe's four callbacks, plus their close relatives ---------------

_REFUSED = [
    "http://169.254.169.254/latest/meta-data/",
    "http://127.0.0.1:6379/",
    "file:///etc/passwd",
    "gopher://127.0.0.1:6379/_INFO",
    "http://[::1]:5432/",
    "http://[::ffff:127.0.0.1]/",
    "http://10.0.0.5/cb",
    "http://192.168.1.1/cb",
    "http://172.17.0.1:8765/callback/x",
    "http://0.0.0.0/",
    "http://100.64.0.1/",  # carrier-grade NAT, not public
    "http://[fd00:ec2::254]/",
    "shepherd-ars://callback/00000000-0000-0000-0000-000000000000",
    "ftp://example.com/",
    "http:///no-host",
    "http://example.com:notaport/",
    "",
    None,
    123,
]


@pytest.mark.parametrize("url", _REFUSED)
async def test_refused(url):
    with _resolving_to(PUBLIC), pytest.raises(CallbackURLError):
        await validate_callback_url(url, allowed_hosts="")


@pytest.mark.parametrize(
    "url",
    [
        "http://ars.example.org/ars/api/messages/abc",
        "https://ars.example.org:8443/cb",
        f"http://{PUBLIC}/cb",
        "HTTPS://Example.COM./cb",
    ],
)
async def test_public_callbacks_are_accepted(url):
    with _resolving_to(PUBLIC):
        assert await validate_callback_url(url, allowed_hosts="") == url


async def test_a_name_resolving_to_metadata_is_refused():
    """A public-looking name is judged by what it resolves to."""
    with _resolving_to("169.254.169.254"), pytest.raises(CallbackURLError) as e:
        await validate_callback_url("http://innocent.example/", allowed_hosts="")
    assert "metadata" in str(e.value)


async def test_any_private_address_among_several_is_refused():
    with _resolving_to(PUBLIC, "10.1.2.3"), pytest.raises(CallbackURLError):
        await validate_callback_url("http://mixed.example/", allowed_hosts="")


async def test_unresolvable_host_is_refused():
    async def fail(host, port):
        raise CallbackURLError("Invalid callback URL: its host could not be resolved.")

    with mock.patch.object(callback_url, "resolve_host", fail):
        with pytest.raises(CallbackURLError, match="could not be resolved"):
            await validate_callback_url("http://nowhere.invalid/", allowed_hosts="")


# --- The allowlist (local/Test flow) ---------------------------------------


async def test_allowlisted_name_may_resolve_privately():
    with _resolving_to("192.168.65.254"):
        await validate_callback_url(
            "http://host.docker.internal:8765/callback/x",
            allowed_hosts="host.docker.internal",
        )


async def test_allowlisted_network_admits_its_addresses_only():
    allowed = "172.16.0.0/12"
    await validate_callback_url("http://172.17.0.1:8765/cb", allowed_hosts=allowed)
    with pytest.raises(CallbackURLError):
        await validate_callback_url("http://10.0.0.1/cb", allowed_hosts=allowed)


async def test_metadata_is_refused_even_when_allowlisted():
    with pytest.raises(CallbackURLError, match="metadata"):
        await validate_callback_url(
            "http://169.254.169.254/", allowed_hosts="169.254.0.0/16"
        )
    with _resolving_to("169.254.169.254"), pytest.raises(CallbackURLError):
        await validate_callback_url("http://meta.local/", allowed_hosts="meta.local")


def test_check_addresses_unwraps_ipv4_mapped():
    with pytest.raises(CallbackURLError):
        check_addresses("x", [ipaddress.ip_address("::ffff:169.254.169.254")])


async def test_defaults_to_the_configured_allowlist(monkeypatch):
    from shepherd_utils.config import settings

    monkeypatch.setattr(settings, "callback_allowed_hosts", "127.0.0.1")
    await validate_callback_url("http://127.0.0.1:8765/cb")
    monkeypatch.setattr(settings, "callback_allowed_hosts", "")
    with pytest.raises(CallbackURLError):
        await validate_callback_url("http://127.0.0.1:8765/cb")


# --- Intake: /asyncquery answers 422 and stores nothing --------------------


@pytest.fixture
def client():
    captured = {}

    async def fake_run_query(target, query, callback_url=None):
        captured.update(callback_url=callback_url)
        return "qid12345", "rid12345", mock.MagicMock()

    with mock.patch.object(base_routes, "run_query", fake_run_query):
        yield TestClient(ARAGORN), captured


@pytest.mark.parametrize(
    "url",
    [
        "http://169.254.169.254/latest/meta-data/",
        "http://127.0.0.1:6379/",
        "file:///etc/passwd",
        "gopher://127.0.0.1:6379/_INFO",
    ],
)
def test_asyncquery_rejects_the_probe_callbacks(url, client):
    test_client, captured = client
    with _resolving_to(PUBLIC):
        resp = test_client.post(
            "/asyncquery", json=dict(default_input_query, callback=url)
        )
    assert resp.status_code == 422, resp.text
    body = resp.json()
    assert body["status"] == "Failed"
    assert body["description"].startswith("Invalid callback URL")
    assert captured == {}


def test_asyncquery_accepts_a_public_callback(client):
    test_client, captured = client
    with _resolving_to(PUBLIC):
        resp = test_client.post(
            "/asyncquery",
            json=dict(default_input_query, callback="https://ars.example.org/cb"),
        )
    assert resp.status_code == 200, resp.text
    assert captured["callback_url"] == "https://ars.example.org/cb"


# --- Delivery: finish_query re-checks before POSTing ------------------------


async def test_finish_query_does_not_post_to_a_refused_callback(
    redis_mock, mocker, caplog
):
    """A stored callback that fails the check (accepted before intake checked,
    or DNS changed since) is not POSTed; the query still completes."""
    from workers.finish_query.worker import finish_query

    mocker.patch(
        "workers.finish_query.worker.get_query_state",
        new_callable=mocker.AsyncMock,
        return_value=["", "", "", "", "", "", "", "rid", "http://127.0.0.1:6379/"],
    )
    completed = mocker.patch(
        "workers.finish_query.worker.set_query_completed",
        new_callable=mocker.AsyncMock,
    )

    async def _get(message_id, logger, *args, raw=False, **kwargs):
        return b'{"message": {}}' if raw else {"message": {}}

    mocker.patch("workers.finish_query.worker.get_message", side_effect=_get)
    mocker.patch(
        "workers.finish_query.worker.get_logs",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    post = mocker.patch("httpx.AsyncClient.post", new_callable=mocker.AsyncMock)
    task = ["test", {"query_id": "q", "response_id": "rid", "log_level": "20"}]

    with caplog.at_level(logging.ERROR):
        await finish_query(task, logging.getLogger("test"))

    post.assert_not_called()
    completed.assert_awaited_once()
    assert any("Not sending the response" in r.message for r in caplog.records)

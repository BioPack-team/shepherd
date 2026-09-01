"""Parity tests for SmartAPI discovery + registry seeding.

Upstream reference: NCATSTranslator/Relay @ dd1e71b
tr_sys/tr_smartapi_client/smart_api_discover.py and tr_sys/utils2.py.
"""

import logging
from unittest.mock import AsyncMock

import shepherd_utils.ars.db as ars_db
import shepherd_utils.smartapi as smartapi
from shepherd_utils.ars.registry_config import (
    endpoint_override,
    inactive_clients,
    legacy_url,
    params_override,
    seed_actor_specs,
)

LOGGER = logging.getLogger(__name__)


def hit(infores, url, maturity="production", updated="2026-01-01", version="1.5.0"):
    return {
        "_id": "x",
        "_meta": {"last_updated": updated},
        "info": {
            "x-trapi": {"version": version},
            "x-translator": {"infores": infores, "team": ["t"]},
        },
        "servers": [{"url": url, "x-maturity": maturity}],
    }


def test_by_infores_latest_picks_matching_maturity_and_newest():
    j = {
        "hits": [
            hit("infores:aragorn", "https://old.example", updated="2025-01-01"),
            hit("infores:aragorn", "https://new.example", updated="2026-01-01"),
            hit("infores:aragorn", "https://dev.example", maturity="development"),
        ]
    }
    result = smartapi._by_infores_latest(j, "production", None)
    assert result["infores:aragorn"]["urlServer"] == "https://new.example"


def test_by_infores_latest_prefers_matching_version():
    j = {
        "hits": [
            hit(
                "infores:arax",
                "https://wrongver.example",
                updated="2026-06-01",
                version="1.4.0",
            ),
            hit(
                "infores:arax",
                "https://rightver.example",
                updated="2025-01-01",
                version="1.5.0",
            ),
        ]
    }
    result = smartapi._by_infores_latest(j, "production", "1.5.0")
    assert result["infores:arax"]["urlServer"] == "https://rightver.example"


def test_url_server_prefers_dynamic_then_legacy(mocker):
    disc = smartapi.SmartApiDiscoverer()
    disc._t_next_refresh = float("inf")  # no live fetch
    disc._map_dynamic = {"infores:aragorn": {"urlServer": "https://dyn.example"}}
    assert disc.url_server("infores:aragorn") == "https://dyn.example"
    # falls back to the bundled url-config-legacy.yaml
    assert disc.url_server("infores:arax") == legacy_url("infores:arax")
    assert disc.url_server("infores:not-a-thing") is None


def test_endpoint_and_params_from_bundled_config():
    assert endpoint_override("infores:aragorn") == "asyncquery"
    assert endpoint_override("infores:improving-agent") == "query"
    assert params_override("infores:automat-cam-kp") == "limit=100"
    assert params_override("infores:aragorn") is None


def test_url_remote_joins_endpoint_and_params(mocker):
    mocker.patch.object(
        smartapi, "url_server", return_value="https://server.example/base/"
    )
    mocker.patch.object(smartapi, "endpoint", return_value="query")
    mocker.patch.object(smartapi, "params", return_value="limit=100")
    assert (
        smartapi.url_remote_from_inforesid("infores:x")
        == "https://server.example/base/query?limit=100"
    )
    mocker.patch.object(smartapi, "endpoint", return_value=None)
    mocker.patch.object(smartapi, "params", return_value=None)
    assert (
        smartapi.url_remote_from_inforesid("infores:x") == "https://server.example/base"
    )


def test_inactive_clients_from_bundled_config():
    assert "infores:aragorn-ranker-exp" in inactive_clients()


def test_seed_specs_cover_all_apps():
    specs = seed_actor_specs()
    agents = {s["agent"]["name"] for s in specs}
    assert "ara-aragorn" in agents
    assert "ara-shepherd-aragorn" in agents
    assert "kp-genetics" in agents
    assert all(s["path"] == "runquery" for s in specs)
    aragorn = next(s for s in specs if s["agent"]["name"] == "ara-aragorn")
    assert aragorn["channel"] == ["general", "workflow"]
    assert aragorn["agent"]["uri"] == "/ara-aragorn/api/"
    wfr = next(s for s in specs if s["agent"]["name"] == "ara-wfr")
    assert wfr["channel"] == ["workflow"]


async def test_seed_registry_upserts_every_actor(mocker):
    import shepherd_utils.ars.lifecycle as lifecycle

    goca = mocker.patch.object(
        ars_db,
        "get_or_create_actor",
        new_callable=AsyncMock,
        return_value=({"id": 1}, 302),
    )
    await lifecycle.seed_registry(LOGGER)
    specs = seed_actor_specs()
    # the three built-in actors + every app actor, inactive list threaded in
    assert goca.await_count == 3 + len(specs)
    app_calls = goca.await_args_list[3:]
    for call in app_calls:
        assert call.kwargs.get("inactive_list") == inactive_clients()

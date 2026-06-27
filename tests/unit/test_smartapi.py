"""Tests for SmartAPI-backed ARS actor discovery."""

import logging

import pytest

from shepherd_utils import smartapi
from shepherd_utils.smartapi import parse_smartapi_hits, refresh_actors

logger = logging.getLogger(__name__)


def _hit(infores, title, servers, component="ARA"):
    return {
        "info": {
            "title": title,
            "x-translator": {"infores": infores, "component": component},
        },
        "servers": servers,
    }


def test_parse_smartapi_hits_one_actor_per_server():
    hits = [
        _hit(
            "infores:aragorn",
            "ARAGORN",
            [
                {"url": "https://aragorn.prod/api", "x-maturity": "production"},
                {"url": "https://aragorn.ci/api", "x-maturity": "staging"},
            ],
        )
    ]
    actors = parse_smartapi_hits(hits)
    assert len(actors) == 2
    assert {a["maturity"] for a in actors} == {"production", "staging"}
    assert actors[0]["infores"] == "infores:aragorn"
    assert actors[0]["agent_name"] == "ARAGORN"
    assert actors[0]["channel"] == "ARA"


def test_parse_smartapi_hits_skips_without_infores_or_server():
    hits = [
        {"info": {"title": "no infores", "x-translator": {}}, "servers": [{"url": "u"}]},
        _hit("infores:x", "X", []),  # no servers
        _hit("infores:y", "Y", [{"url": ""}]),  # blank url
    ]
    assert parse_smartapi_hits(hits) == []


def test_parse_smartapi_hits_handles_empty():
    assert parse_smartapi_hits([]) == []
    assert parse_smartapi_hits(None) == []


@pytest.mark.asyncio
async def test_refresh_actors_upserts_each(mocker):
    hits = [
        _hit("infores:bte", "BTE", [{"url": "https://bte/api", "x-maturity": "production"}]),
    ]
    mocker.patch.object(
        smartapi,
        "fetch_smartapi_hits",
        new_callable=mocker.AsyncMock,
        return_value=hits,
    )
    upsert = mocker.patch.object(
        smartapi, "upsert_actor", new_callable=mocker.AsyncMock
    )
    count = await refresh_actors(logger)
    assert count == 1
    upsert.assert_awaited_once()
    args = upsert.await_args.args
    assert args[0] == "infores:bte"
    assert args[1] == "https://bte/api"

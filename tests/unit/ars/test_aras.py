"""Tests for the static ARA roster and the broker handoff sentinel."""

import json
import time
import uuid

from shepherd_utils.ars import aras
from shepherd_utils.ars.handoff import (
    HANDOFF_PREFIX,
    handoff_callback_url,
    parse_handoff_callback,
)
from shepherd_utils.config import settings
from shepherd_utils.heartbeat import heartbeat_key

# ---------------------------------------------------------------------------
# roster
# ---------------------------------------------------------------------------


def test_roster_is_the_hosted_aras():
    assert [a.name for a in aras.ARAS] == ["aragorn", "arax", "bte"]
    for ara in aras.ARAS:
        assert ara.inforesid == f"infores:shepherd-{ara.name}"
        assert ara.agent == f"ara-shepherd-{ara.name}"
        assert aras.by_name(ara.name) is ara
        assert aras.by_agent(ara.agent) is ara


def test_lookups_miss_cleanly():
    assert aras.by_name("nope") is None
    assert aras.by_name(None) is None
    assert aras.by_agent("ara-aragorn") is None
    assert aras.by_agent("") is None


def test_inforesid_for_every_agent_kind():
    assert aras.inforesid_for("ara-shepherd-bte") == "infores:shepherd-bte"
    assert aras.inforesid_for(aras.MERGE_AGENT) == "infores:ars"
    assert aras.inforesid_for(aras.DEFAULT_AGENT) == ""
    assert aras.inforesid_for(None) == ""
    # an agent a pre-de-federation volume may still carry
    assert aras.inforesid_for("ara-aragorn") == ""


def test_reports_suffix_match_is_case_insensitive_endswith():
    """GET /reports/<inforesid> matched actors by inforesid iendswith."""
    assert aras.agents_for_inforesid_suffix("shepherd-aragorn") == [
        "ara-shepherd-aragorn"
    ]
    assert aras.agents_for_inforesid_suffix("ARAX") == ["ara-shepherd-arax"]
    assert aras.agents_for_inforesid_suffix("infores:ars") == [aras.MERGE_AGENT]
    assert aras.agents_for_inforesid_suffix("infores:aragorn") == []
    # the empty suffix matches everything, like upstream's LIKE '%'
    assert len(aras.agents_for_inforesid_suffix("")) == len(aras.ARAS) + 1


def test_enabled_defaults_to_every_hosted_ara(monkeypatch):
    monkeypatch.setattr(settings, "ars_enabled_aras", "")
    assert aras.enabled_aras() == list(aras.ARAS)
    monkeypatch.setattr(settings, "ars_enabled_aras", "   ")
    assert aras.enabled_aras() == list(aras.ARAS)


def test_enabled_subset_from_settings(monkeypatch):
    monkeypatch.setattr(settings, "ars_enabled_aras", " bte ,aragorn,bte")
    assert [a.name for a in aras.enabled_aras()] == ["bte", "aragorn"]
    assert aras.is_enabled(aras.by_name("bte"))
    assert not aras.is_enabled(aras.by_name("arax"))


def test_enabled_ignores_unknown_names(monkeypatch, caplog):
    """A typo disables one ARA, not the whole fan-out."""
    monkeypatch.setattr(settings, "ars_enabled_aras", "aragorn,typo")
    with caplog.at_level("WARNING", logger="shepherd.ars.aras"):
        assert [a.name for a in aras.enabled_aras()] == ["aragorn"]
    assert "typo" in caplog.text


# ---------------------------------------------------------------------------
# live worker counts (from heartbeats)
# ---------------------------------------------------------------------------


async def _beat(broker, stream, consumer, age_sec=0.0):
    await broker.set(
        heartbeat_key(stream, consumer),
        json.dumps({"stream": stream, "last_seen": time.time() - age_sec}),
    )


async def test_live_worker_counts_reads_fresh_heartbeats(redis_mock):
    broker = redis_mock["broker"]
    await _beat(broker, "aragorn", "a1")
    await _beat(broker, "aragorn", "a2")
    await _beat(broker, "arax", "x1", age_sec=3600)  # stale: not alive
    # a lookup worker's stream shares the prefix but is not the ARA's stream
    await _beat(broker, "aragorn_lookup", "l1")
    counts = await aras.live_worker_counts(["aragorn", "arax", "bte"])
    assert counts == {"aragorn": 2, "arax": 0, "bte": 0}


async def test_live_worker_counts_survive_a_broker_error(mocker, redis_mock):
    import shepherd_utils.broker as broker_mod

    mocker.patch.object(
        broker_mod.broker_client, "scan_iter", side_effect=RuntimeError("down")
    )
    counts = await aras.live_worker_counts(["aragorn"])
    assert counts == {"aragorn": 0}


# ---------------------------------------------------------------------------
# handoff sentinel
# ---------------------------------------------------------------------------


def test_handoff_sentinel_round_trips():
    pk = uuid.uuid4()
    url = handoff_callback_url(pk)
    assert url.startswith(HANDOFF_PREFIX)
    assert not url.startswith("http")
    assert parse_handoff_callback(url) == str(pk)


def test_handoff_parse_rejects_everything_else():
    assert parse_handoff_callback(None) is None
    assert parse_handoff_callback("") is None
    assert parse_handoff_callback("https://ars.example/ars/api/messages/x") is None
    assert parse_handoff_callback(f"{HANDOFF_PREFIX}not-a-uuid") is None

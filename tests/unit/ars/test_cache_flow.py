"""Response-cache orchestration: the two submit-side steps (lookup before
any row exists, claim-or-serve once one does), the render-time read note,
the completion hook and the watchdog repair sweep.

shepherd_utils.ars.cache against an AsyncMock'd ars_db (same convention as
the lifecycle / watchdog tests). The SQL itself is not exercised here.
"""

import datetime
import json
import logging
import uuid
from unittest.mock import AsyncMock

import pytest

from shepherd_utils.ars import cache
from shepherd_utils.config import settings

LOGGER = logging.getLogger(__name__)
UTC = datetime.timezone.utc
TS = datetime.datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)

QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"n0": {"ids": ["MONDO:0005148"]}, "n1": {}},
            "edges": {"e": {"subject": "n1", "object": "n0"}},
        }
    }
}
KEY, CALLER_MAP = cache.cache_key(QUERY)
_, SOURCE_MAP = cache.canonical_graph(QUERY["message"]["query_graph"])

# the same query under other labels
RELABELED = {
    "message": {
        "query_graph": {
            "nodes": {"dz": {"ids": ["MONDO:0005148"]}, "cx": {}},
            "edges": {"rel": {"subject": "cx", "object": "dz"}},
        }
    }
}
assert cache.cache_key(RELABELED)[0] == KEY


def row(pk=None, status="R", code=202, actor=1, ref=None, params=None, **extra):
    base = {
        "id": pk or uuid.uuid4(),
        "name": "",
        "status": status,
        "code": code,
        "actor": actor,
        "ref": ref,
        "ts": TS,
        "updated_at": TS,
        "url": None,
        "result_count": None,
        "result_stat": None,
        "retain": False,
        "merge_semaphore": False,
        "merged_version": None,
        "merged_versions_list": None,
        "params": params if params is not None else {"query_type": "standard"},
        "clients": [],
    }
    base.update(extra)
    return base


def entry(source_pk, state="ready", generation=1, key=KEY, label_map=None, hit_count=0):
    return {
        "generation": generation,
        "cache_key": key,
        "state": state,
        "source_pk": source_pk,
        "label_map": label_map if label_map is not None else SOURCE_MAP,
        "created_at": TS,
        "ready_at": TS if state == "ready" else None,
        "hit_count": hit_count,
        "last_hit_at": None,
    }


MERGED_PAYLOAD = {
    "message": {
        "query_graph": json.loads(json.dumps(QUERY["message"]["query_graph"])),
        "knowledge_graph": {"nodes": {"MONDO:0005148": {}}, "edges": {}},
        "results": [
            {
                "node_bindings": {"n0": [{"id": "MONDO:0005148"}], "n1": []},
                "analyses": [{"resource_id": "infores:x", "edge_bindings": {"e": []}}],
            }
        ],
    },
    "logs": [{"message": "merged", "level": "INFO"}],
}


@pytest.fixture
def db(mocker):
    """AsyncMock every ars_db function the cache module touches."""
    names = [
        "get_cache_generation",
        "claim_or_get_cache_entry",
        "get_cache_entry",
        "get_ready_cache_entry_by_source",
        "mark_cache_entry_ready",
        "upsert_cache_entry_ready",
        "delete_cache_entry",
        "delete_pending_cache_entry",
        "record_cache_hit",
        "get_stale_pending_cache_entries",
        "purge_stale_cache_entries",
        "delete_message",
        "get_message_row",
        "get_children",
        "update_message",
        "load_message_data",
    ]
    mocks = {
        n: mocker.patch.object(cache.ars_db, n, new_callable=AsyncMock) for n in names
    }
    mocks["get_cache_generation"].return_value = 1
    mocks["get_cache_entry"].return_value = None
    mocks["get_stale_pending_cache_entries"].return_value = []
    mocks["purge_stale_cache_entries"].return_value = 0
    mocks["update_message"].side_effect = lambda pk, **kw: {
        **row(pk=pk),
        **{k: v for k, v in kw.items() if k != "skip_coercion"},
    }
    mocks["redis_delete"] = mocker.patch.object(
        cache.shepherd_db.data_db_client, "delete", new_callable=AsyncMock
    )
    return mocks


def _source(db, merged_pk=None, status="D"):
    """A completed source tree: parent Done pointing at a merged message."""
    merged_pk = merged_pk or uuid.uuid4()
    source = row(
        status=status,
        code=200 if status == "D" else 202,
        merged_version=merged_pk if status == "D" else None,
        result_count=1,
        params={"query_type": "standard", "stats": {"results": 1}},
    )
    db["get_message_row"].side_effect = lambda pk: source if pk == source["id"] else None
    db["load_message_data"].side_effect = lambda pk, *a: (
        json.loads(json.dumps(MERGED_PAYLOAD)) if pk == merged_pk else None
    )
    return source


# ---------------------------------------------------------------------------
# lookup (before any row exists)
# ---------------------------------------------------------------------------


async def test_lookup_disabled_or_non_normal_mode_is_none(db, monkeypatch):
    assert await cache.lookup(dict(QUERY, bypass_cache=True), LOGGER) is None
    assert await cache.lookup(dict(QUERY, parameters={"overwrite_cache": True}), LOGGER) is None
    monkeypatch.setattr(settings, "ars_cache_enabled", False)
    assert await cache.lookup(QUERY, LOGGER) is None
    db["get_cache_entry"].assert_not_awaited()


async def test_lookup_miss_is_none(db):
    assert await cache.lookup(QUERY, LOGGER) is None
    db["get_cache_entry"].assert_awaited_once_with(1, KEY)


async def test_lookup_ready_hit_serves_source_pk_with_converted_data(db):
    source = _source(db)
    db["get_cache_entry"].return_value = entry(source["id"])
    outcome, served_row, payload = await cache.lookup(RELABELED, LOGGER)
    assert outcome == cache.SERVED
    assert served_row is source  # the caller gets the source parent's pk
    qg = payload["message"]["query_graph"]
    assert set(qg["nodes"]) == {"dz", "cx"}
    assert qg["edges"] == {"rel": {"subject": "cx", "object": "dz"}}
    result = payload["message"]["results"][0]
    assert set(result["node_bindings"]) == {"dz", "cx"}
    assert set(result["analyses"][0]["edge_bindings"]) == {"rel"}
    assert "MONDO:0005148" in payload["message"]["knowledge_graph"]["nodes"]
    assert payload["logs"][0]["message"] == "merged"
    assert "Served from ARS response cache" in payload["logs"][-1]["message"]
    assert str(source["id"]) in payload["logs"][-1]["message"]
    db["record_cache_hit"].assert_awaited_once_with(1, KEY)
    db["delete_message"].assert_not_awaited()


async def test_lookup_same_labels_is_identity(db):
    source = _source(db)
    db["get_cache_entry"].return_value = entry(source["id"])
    _, _, payload = await cache.lookup(QUERY, LOGGER)
    assert set(payload["message"]["query_graph"]["nodes"]) == {"n0", "n1"}
    assert set(payload["message"]["results"][0]["node_bindings"]) == {"n0", "n1"}


async def test_lookup_pending_hands_back_leader_pk(db):
    leader = row(status="R")
    db["get_message_row"].return_value = leader
    db["get_cache_entry"].return_value = entry(leader["id"], "pending")
    outcome, served_row, payload = await cache.lookup(RELABELED, LOGGER)
    assert outcome == cache.WAITING
    assert served_row is leader
    assert payload is RELABELED  # the caller's own body, nothing to convert yet
    db["record_cache_hit"].assert_not_awaited()


async def test_lookup_pending_with_missing_leader_drops_entry(db):
    db["get_message_row"].return_value = None
    db["get_cache_entry"].return_value = entry(uuid.uuid4(), "pending")
    assert await cache.lookup(QUERY, LOGGER) is None
    db["delete_cache_entry"].assert_awaited_once_with(1, KEY)


@pytest.mark.parametrize("breakage", ["missing_source", "source_running", "no_merged_version", "no_payload"])
async def test_lookup_broken_ready_entry_is_dropped(db, breakage):
    merged_pk = uuid.uuid4()
    source = _source(db, merged_pk=merged_pk)
    if breakage == "missing_source":
        db["get_message_row"].side_effect = lambda pk: None
    elif breakage == "source_running":
        source["status"] = "R"
    elif breakage == "no_merged_version":
        source["merged_version"] = None
    elif breakage == "no_payload":
        db["load_message_data"].side_effect = lambda pk, *a: None
    db["get_cache_entry"].return_value = entry(source["id"])
    assert await cache.lookup(QUERY, LOGGER) is None
    db["delete_cache_entry"].assert_awaited_once_with(1, KEY)
    db["record_cache_hit"].assert_not_awaited()


async def test_lookup_failure_degrades_to_miss(db):
    db["get_cache_generation"].side_effect = RuntimeError("pg down")
    assert await cache.lookup(QUERY, LOGGER) is None


# ---------------------------------------------------------------------------
# claim_or_serve (a parent row + blob exist)
# ---------------------------------------------------------------------------


async def test_claim_disabled_is_plain_dispatch(db, monkeypatch):
    monkeypatch.setattr(settings, "ars_cache_enabled", False)
    parent = row()
    outcome, out, payload = await cache.claim_or_serve(parent, QUERY, LOGGER)
    assert (outcome, out, payload) == (cache.DISPATCH, parent, QUERY)
    db["claim_or_get_cache_entry"].assert_not_awaited()


async def test_bypass_and_overwrite_record_role_and_dispatch(db):
    for body, role in (
        (dict(QUERY, bypass_cache=True), "bypass"),
        (dict(QUERY, parameters={"overwrite_cache": True}), "overwrite"),
    ):
        outcome, out, payload = await cache.claim_or_serve(row(), body, LOGGER)
        assert outcome == cache.DISPATCH and payload is body
        assert out["params"]["cache"] == {"role": role, "key": KEY, "generation": 1}
    db["claim_or_get_cache_entry"].assert_not_awaited()


async def test_miss_claims_leadership(db):
    parent = row()
    db["claim_or_get_cache_entry"].return_value = (entry(parent["id"], "pending"), True)
    outcome, out, payload = await cache.claim_or_serve(parent, QUERY, LOGGER)
    assert outcome == cache.DISPATCH and payload is QUERY
    assert out["params"]["cache"]["role"] == "leader"
    db["claim_or_get_cache_entry"].assert_awaited_once_with(1, KEY, parent["id"])
    db["delete_message"].assert_not_awaited()


async def test_lost_race_to_ready_entry_serves_and_discards_own_row(db):
    parent = row()
    source = _source(db)
    db["claim_or_get_cache_entry"].return_value = (entry(source["id"]), False)
    outcome, out, payload = await cache.claim_or_serve(parent, RELABELED, LOGGER)
    assert outcome == cache.SERVED and out is source
    assert set(payload["message"]["query_graph"]["nodes"]) == {"dz", "cx"}
    db["delete_message"].assert_awaited_once_with(parent["id"])
    db["redis_delete"].assert_awaited_once_with(str(parent["id"]))


async def test_lost_race_to_pending_entry_waits_on_leader(db):
    parent = row()
    leader = row(status="R")
    db["get_message_row"].return_value = leader
    db["claim_or_get_cache_entry"].return_value = (entry(leader["id"], "pending"), False)
    outcome, out, payload = await cache.claim_or_serve(parent, QUERY, LOGGER)
    assert outcome == cache.WAITING and out is leader and payload is QUERY
    db["delete_message"].assert_awaited_once_with(parent["id"])


async def test_broken_entry_then_claim_again(db):
    parent = row()
    db["get_message_row"].return_value = None  # broken source
    db["claim_or_get_cache_entry"].side_effect = [
        (entry(uuid.uuid4()), False),
        (entry(parent["id"], "pending"), True),
    ]
    outcome, out, _ = await cache.claim_or_serve(parent, QUERY, LOGGER)
    assert outcome == cache.DISPATCH
    assert out["params"]["cache"]["role"] == "leader"
    db["delete_cache_entry"].assert_awaited_once()
    db["delete_message"].assert_not_awaited()


async def test_entry_keeps_vanishing_dispatches_uncached(db):
    parent = row()
    db["claim_or_get_cache_entry"].return_value = (None, False)
    outcome, out, _ = await cache.claim_or_serve(parent, QUERY, LOGGER)
    assert outcome == cache.DISPATCH
    assert out["params"]["cache"]["role"] == "uncached"


async def test_claim_failure_degrades_to_dispatch(db):
    parent = row()
    db["get_cache_generation"].side_effect = RuntimeError("pg down")
    outcome, out, payload = await cache.claim_or_serve(parent, QUERY, LOGGER)
    assert (outcome, out, payload) == (cache.DISPATCH, parent, QUERY)


# ---------------------------------------------------------------------------
# annotate_cached_read (GET)
# ---------------------------------------------------------------------------

MERGE_ACTOR = {"inforesid": "infores:ars", "agent_name": "ars-ars-agent"}


async def test_read_note_on_source_merged_message(db):
    merged_pk = uuid.uuid4()
    source = _source(db, merged_pk=merged_pk)
    merged_row = row(pk=merged_pk, ref=source["id"], status="D", code=200)
    db["get_ready_cache_entry_by_source"].return_value = entry(source["id"], hit_count=4)
    payload = json.loads(json.dumps(MERGED_PAYLOAD))
    out = await cache.annotate_cached_read(merged_row, MERGE_ACTOR, payload, LOGGER)
    assert out is payload
    note = payload["logs"][-1]["message"]
    assert "response cache entry" in note and "served 4 time(s)" in note
    assert len(payload["logs"]) == 2


async def test_read_note_skipped_when_not_applicable(db, monkeypatch):
    source = _source(db)
    db["get_ready_cache_entry_by_source"].return_value = entry(source["id"])
    base = json.loads(json.dumps(MERGED_PAYLOAD))
    # not a merge child
    p = json.loads(json.dumps(base))
    await cache.annotate_cached_read(row(ref=source["id"]), {"inforesid": "infores:aragorn"}, p, LOGGER)
    assert p == base
    # a parent (no ref)
    p = json.loads(json.dumps(base))
    await cache.annotate_cached_read(row(), MERGE_ACTOR, p, LOGGER)
    assert p == base
    # an intermediate merge, not the parent's merged_version
    p = json.loads(json.dumps(base))
    await cache.annotate_cached_read(row(ref=source["id"]), MERGE_ACTOR, p, LOGGER)
    assert p == base
    # no entry for this tree
    db["get_ready_cache_entry_by_source"].return_value = None
    p = json.loads(json.dumps(base))
    await cache.annotate_cached_read(row(pk=source["merged_version"], ref=source["id"]), MERGE_ACTOR, p, LOGGER)
    assert p == base
    # disabled
    monkeypatch.setattr(settings, "ars_cache_enabled", False)
    db["get_ready_cache_entry_by_source"].return_value = entry(source["id"])
    p = json.loads(json.dumps(base))
    await cache.annotate_cached_read(row(pk=source["merged_version"], ref=source["id"]), MERGE_ACTOR, p, LOGGER)
    assert p == base
    # non-dict payload
    assert await cache.annotate_cached_read(row(), MERGE_ACTOR, None, LOGGER) is None


async def test_read_note_survives_db_errors(db):
    db["get_ready_cache_entry_by_source"].side_effect = RuntimeError("pg")
    payload = {"logs": []}
    assert await cache.annotate_cached_read(row(ref=uuid.uuid4()), MERGE_ACTOR, payload, LOGGER) is payload
    assert payload["logs"] == []


# ---------------------------------------------------------------------------
# completion hook
# ---------------------------------------------------------------------------


def _leader(role="leader", status="D", **extra):
    return row(
        status=status,
        code=200 if status == "D" else 500,
        params={"query_type": "standard", "cache": {"key": KEY, "generation": 1, "role": role}},
        **extra,
    )


def _ara(status, result_count=None):
    return dict(row(status=status, code=200 if status == "D" else 598), agent_name="ara-x", inforesid="infores:x", result_count=result_count)


def _merge(pk, result_count):
    return dict(row(pk=pk, status="D", code=200), agent_name="ars-ars-agent", inforesid="infores:ars", result_count=result_count)


async def test_on_parent_complete_ignores_non_leaders(db):
    for role in (None, "bypass", "uncached"):
        params = {"query_type": "standard"}
        if role:
            params["cache"] = {"role": role, "key": KEY, "generation": 1}
        await cache.on_parent_complete(row(status="D", params=params), LOGGER)
    db["get_children"].assert_not_awaited()
    db["mark_cache_entry_ready"].assert_not_awaited()


async def test_leader_publishes_entry_with_label_map(db):
    merged_pk = uuid.uuid4()
    leader = _leader(merged_version=merged_pk)
    db["get_children"].return_value = [_ara("D", 3), _merge(merged_pk, 3)]
    db["load_message_data"].return_value = RELABELED
    db["mark_cache_entry_ready"].return_value = entry(leader["id"])
    await cache.on_parent_complete(leader, LOGGER)
    _, label_map = cache.cache_key(RELABELED)
    db["mark_cache_entry_ready"].assert_awaited_once_with(leader["id"], label_map)
    db["delete_pending_cache_entry"].assert_not_awaited()


async def test_leader_empty_with_errors_drops_pending_entry(db):
    merged_pk = uuid.uuid4()
    leader = _leader(merged_version=merged_pk)
    db["get_children"].return_value = [_ara("E"), _ara("D", 0), _merge(merged_pk, 0)]
    await cache.on_parent_complete(leader, LOGGER)
    db["delete_pending_cache_entry"].assert_awaited_once_with(leader["id"])
    db["mark_cache_entry_ready"].assert_not_awaited()


async def test_leader_empty_without_errors_is_cached(db):
    merged_pk = uuid.uuid4()
    leader = _leader(merged_version=merged_pk)
    db["get_children"].return_value = [_ara("D", 0), _merge(merged_pk, 0)]
    db["load_message_data"].return_value = QUERY
    db["mark_cache_entry_ready"].return_value = entry(leader["id"])
    await cache.on_parent_complete(leader, LOGGER)
    db["mark_cache_entry_ready"].assert_awaited_once()


async def test_partial_results_cached_unless_store_partial_off(db, monkeypatch):
    merged_pk = uuid.uuid4()
    leader = _leader(merged_version=merged_pk)
    db["get_children"].return_value = [_ara("E"), _ara("D", 5), _merge(merged_pk, 5)]
    db["load_message_data"].return_value = QUERY
    db["mark_cache_entry_ready"].return_value = entry(leader["id"])
    await cache.on_parent_complete(leader, LOGGER)
    db["mark_cache_entry_ready"].assert_awaited_once()

    db["mark_cache_entry_ready"].reset_mock()
    monkeypatch.setattr(settings, "ars_cache_store_partial", False)
    await cache.on_parent_complete(leader, LOGGER)
    db["mark_cache_entry_ready"].assert_not_awaited()
    db["delete_pending_cache_entry"].assert_awaited_once_with(leader["id"])


async def test_overwrite_upserts_entry(db):
    merged_pk = uuid.uuid4()
    parent = _leader(role="overwrite", merged_version=merged_pk)
    db["get_children"].return_value = [_ara("D", 2), _merge(merged_pk, 2)]
    db["load_message_data"].return_value = QUERY
    db["upsert_cache_entry_ready"].return_value = entry(parent["id"])
    await cache.on_parent_complete(parent, LOGGER)
    _, label_map = cache.cache_key(QUERY)
    db["upsert_cache_entry_ready"].assert_awaited_once_with(1, KEY, parent["id"], label_map)
    db["mark_cache_entry_ready"].assert_not_awaited()


async def test_overwrite_not_cacheable_leaves_existing_entry(db):
    merged_pk = uuid.uuid4()
    parent = _leader(role="overwrite", merged_version=merged_pk)
    db["get_children"].return_value = [_ara("E"), _merge(merged_pk, 0)]
    await cache.on_parent_complete(parent, LOGGER)
    db["upsert_cache_entry_ready"].assert_not_awaited()
    db["delete_pending_cache_entry"].assert_not_awaited()


async def test_hook_failure_is_swallowed(db):
    db["get_children"].side_effect = RuntimeError("pg")
    await cache.on_parent_complete(_leader(), LOGGER)  # no raise


async def test_on_parent_failed_drops_pending_entry_for_leaders_only(db):
    await cache.on_parent_failed(_leader(role="bypass", status="E"), LOGGER)
    db["delete_pending_cache_entry"].assert_not_awaited()
    leader = _leader(status="E")
    await cache.on_parent_failed(leader, LOGGER)
    db["delete_pending_cache_entry"].assert_awaited_once_with(leader["id"])


# ---------------------------------------------------------------------------
# repair sweep / invalidation
# ---------------------------------------------------------------------------


async def test_repair_sweep_disabled(db, monkeypatch):
    monkeypatch.setattr(settings, "ars_cache_enabled", False)
    assert await cache.repair_sweep(LOGGER) == {"stale_pending": 0, "purged": 0}
    db["get_stale_pending_cache_entries"].assert_not_awaited()


async def test_repair_sweep_finishes_done_leader_and_drops_others(db, mocker):
    done_leader = _leader()
    stuck_pk, gone_pk = uuid.uuid4(), uuid.uuid4()
    db["get_stale_pending_cache_entries"].return_value = [
        dict(entry(done_leader["id"], "pending"), leader_status="D"),
        dict(entry(stuck_pk, "pending"), leader_status="R"),
        dict(entry(gone_pk, "pending"), leader_status=None),
    ]
    db["get_message_row"].return_value = done_leader
    complete = mocker.patch.object(cache, "on_parent_complete", new_callable=AsyncMock)
    counts = await cache.repair_sweep(LOGGER)
    assert counts["stale_pending"] == 3
    complete.assert_awaited_once_with(done_leader, LOGGER)
    dropped = [c.args[0] for c in db["delete_pending_cache_entry"].await_args_list]
    assert dropped == [done_leader["id"], stuck_pk, gone_pk]
    db["get_stale_pending_cache_entries"].assert_awaited_once_with(settings.ars_cache_pending_max_sec)


async def test_repair_sweep_purges_superseded_generations(db):
    db["get_cache_generation"].return_value = 4
    db["purge_stale_cache_entries"].return_value = 12
    counts = await cache.repair_sweep(LOGGER)
    assert counts["purged"] == 12
    db["purge_stale_cache_entries"].assert_awaited_once_with(4, settings.ars_cache_stale_grace_sec)


async def test_repair_sweep_isolates_failures(db):
    db["get_stale_pending_cache_entries"].return_value = [
        dict(entry(uuid.uuid4(), "pending"), leader_status="R")
    ]
    db["delete_pending_cache_entry"].side_effect = RuntimeError("x")
    db["purge_stale_cache_entries"].side_effect = RuntimeError("y")
    counts = await cache.repair_sweep(LOGGER)  # no raise
    assert counts == {"stale_pending": 1, "purged": 0}


async def test_invalidate_all_bumps_generation(mocker):
    bump = mocker.patch.object(cache.ars_db, "bump_cache_generation", new_callable=AsyncMock, return_value=7)
    assert await cache.invalidate_all("new KG") == 7
    bump.assert_awaited_once_with("new KG")

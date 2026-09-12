"""Response-cache orchestration: submit-side outcomes, tree copying,
completion hook, leader fail-over and the watchdog repair sweep.

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


def entry(source_pk, state="ready", generation=1, key=KEY, label_map=None):
    return {
        "generation": generation,
        "cache_key": key,
        "state": state,
        "source_pk": source_pk,
        "label_map": label_map,
        "created_at": TS,
        "ready_at": TS if state == "ready" else None,
        "hit_count": 0,
        "last_hit_at": None,
    }


@pytest.fixture
def db(mocker):
    """AsyncMock every ars_db function the cache module touches."""
    names = [
        "get_cache_generation",
        "claim_or_get_cache_entry",
        "get_cache_entry",
        "mark_cache_entry_ready",
        "upsert_cache_entry_ready",
        "delete_cache_entry",
        "delete_pending_cache_entry",
        "repoint_pending_cache_entry",
        "record_cache_hit",
        "add_cache_waiter",
        "get_cache_waiters",
        "delete_cache_waiter",
        "claim_cache_waiter",
        "repoint_cache_waiters",
        "get_stale_pending_cache_entries",
        "get_stuck_cache_waiters",
        "purge_stale_cache_entries",
        "delete_children",
        "get_message_row",
        "get_children",
        "create_message",
        "update_message",
        "save_message_data",
        "load_message_data",
        "persist_data_copy",
        "load_otel_carrier",
    ]
    mocks = {
        n: mocker.patch.object(cache.ars_db, n, new_callable=AsyncMock) for n in names
    }
    mocks["get_cache_generation"].return_value = 1
    mocks["get_cache_waiters"].return_value = []
    mocks["get_stale_pending_cache_entries"].return_value = []
    mocks["get_stuck_cache_waiters"].return_value = []
    mocks["purge_stale_cache_entries"].return_value = 0
    mocks["load_otel_carrier"].return_value = "{}"
    mocks["claim_cache_waiter"].return_value = True
    mocks["update_message"].side_effect = lambda pk, **kw: {
        **row(pk=pk),
        **{k: v for k, v in kw.items() if k != "skip_coercion"},
    }
    created_rows = []

    def _create(**kw):
        new = row(
            status=kw.get("status", "R"),
            code=kw.get("code", 202),
            actor=kw.get("actor_id"),
            ref=kw.get("ref"),
            params=kw.get("params"),
        )
        created_rows.append(new)
        return new

    mocks["create_message"].side_effect = _create
    mocks["created_rows"] = created_rows
    mocks["add_task"] = mocker.patch.object(cache, "add_task", new_callable=AsyncMock)
    mocks["notify"] = mocker.patch.object(
        cache, "notify_subscribers", new_callable=AsyncMock
    )
    return mocks


# ---------------------------------------------------------------------------
# before_dispatch
# ---------------------------------------------------------------------------


async def test_disabled_is_plain_dispatch(db, monkeypatch):
    monkeypatch.setattr(settings, "ars_cache_enabled", False)
    parent = row()
    outcome, out = await cache.before_dispatch(parent, QUERY, LOGGER)
    assert outcome == cache.DISPATCH and out is parent
    db["get_cache_generation"].assert_not_awaited()


async def test_bypass_and_overwrite_skip_lookup(db):
    for body, role in (
        (dict(QUERY, bypass_cache=True), "bypass"),
        (dict(QUERY, parameters={"overwrite_cache": True}), "overwrite"),
    ):
        db["update_message"].reset_mock()
        outcome, out = await cache.before_dispatch(row(), body, LOGGER)
        assert outcome == cache.DISPATCH
        assert out["params"]["cache"]["role"] == role
        assert out["params"]["cache"]["key"] == KEY
        assert out["params"]["cache"]["generation"] == 1
    db["claim_or_get_cache_entry"].assert_not_awaited()


async def test_miss_claims_leadership(db):
    parent = row()
    db["claim_or_get_cache_entry"].return_value = (entry(parent["id"], "pending"), True)
    outcome, out = await cache.before_dispatch(parent, QUERY, LOGGER)
    assert outcome == cache.DISPATCH
    assert out["params"]["cache"]["role"] == "leader"
    db["claim_or_get_cache_entry"].assert_awaited_once_with(1, KEY, parent["id"])


async def test_ready_hit_is_served(db, mocker):
    parent = row()
    source_pk = uuid.uuid4()
    ready = entry(source_pk)
    db["claim_or_get_cache_entry"].return_value = (ready, False)
    served = row(pk=parent["id"], status="D", code=200)
    mat = mocker.patch.object(cache, "materialize", new_callable=AsyncMock, return_value=served)
    outcome, out = await cache.before_dispatch(parent, QUERY, LOGGER)
    assert outcome == cache.SERVED and out is served
    mat.assert_awaited_once()
    assert mat.await_args.args[1] is ready
    assert mat.await_args.args[2] == CALLER_MAP


async def test_broken_ready_entry_then_lead(db, mocker):
    parent = row()
    db["claim_or_get_cache_entry"].side_effect = [
        (entry(uuid.uuid4()), False),
        (entry(parent["id"], "pending"), True),
    ]
    mocker.patch.object(cache, "materialize", new_callable=AsyncMock, return_value=None)
    outcome, out = await cache.before_dispatch(parent, QUERY, LOGGER)
    assert outcome == cache.DISPATCH
    assert out["params"]["cache"]["role"] == "leader"


async def test_pending_entry_joins_as_waiter(db):
    parent = row()
    leader_pk = uuid.uuid4()
    pending = entry(leader_pk, "pending")
    db["claim_or_get_cache_entry"].return_value = (pending, False)
    db["get_cache_entry"].return_value = pending
    outcome, out = await cache.before_dispatch(parent, QUERY, LOGGER)
    assert outcome == cache.WAITING
    assert out["params"]["cache"]["role"] == "follower"
    assert out["params"]["cache"]["leader_pk"] == str(leader_pk)
    db["add_cache_waiter"].assert_awaited_once_with(parent["id"], leader_pk, 1, KEY)


async def test_waiter_race_with_completing_leader_is_served(db, mocker):
    """Leader flips to ready between our conflict and our waiter insert:
    nobody else will copy for us, so we do it ourselves."""
    parent = row()
    leader_pk = uuid.uuid4()
    db["claim_or_get_cache_entry"].return_value = (entry(leader_pk, "pending"), False)
    db["get_cache_entry"].return_value = entry(leader_pk, "ready")
    served = row(pk=parent["id"], status="D")
    mocker.patch.object(cache, "materialize", new_callable=AsyncMock, return_value=served)
    outcome, out = await cache.before_dispatch(parent, QUERY, LOGGER)
    assert outcome == cache.SERVED and out is served
    db["delete_cache_waiter"].assert_awaited_once_with(parent["id"])


async def test_entry_vanished_dispatches_uncached(db):
    parent = row()
    db["claim_or_get_cache_entry"].return_value = (None, False)
    outcome, out = await cache.before_dispatch(parent, QUERY, LOGGER)
    assert outcome == cache.DISPATCH
    assert out["params"]["cache"]["role"] == "uncached"


async def test_bookkeeping_failure_degrades_to_dispatch(db):
    parent = row()
    db["get_cache_generation"].side_effect = RuntimeError("pg down")
    outcome, out = await cache.before_dispatch(parent, QUERY, LOGGER)
    assert outcome == cache.DISPATCH and out is parent


# ---------------------------------------------------------------------------
# materialize
# ---------------------------------------------------------------------------


def _source_tree(db, with_intermediate_merge=True):
    source_pk, merged_pk, ara_pk, kp_pk, mid_pk = (uuid.uuid4() for _ in range(5))
    source = row(
        pk=source_pk,
        status="D",
        code=200,
        merged_version=merged_pk,
        result_count=2,
        result_stat={"x": 1},
        params={"query_type": "standard", "stats": {"results": 2}},
    )
    children = [
        dict(
            row(pk=ara_pk, actor=7, ref=source_pk, status="D", code=200),
            agent_name="ara-aragorn",
            inforesid="infores:aragorn",
            result_count=2,
            url="http://aragorn",
            name="aragorn",
        ),
        dict(
            row(pk=kp_pk, actor=8, ref=source_pk, status="E", code=598),
            agent_name="ara-arax",
            inforesid="infores:arax",
        ),
    ]
    if with_intermediate_merge:
        children.append(
            dict(
                row(pk=mid_pk, actor=3, ref=source_pk, status="D", code=200),
                agent_name="ars-ars-agent",
                inforesid="infores:ars",
                result_count=1,
            )
        )
    children.append(
        dict(
            row(pk=merged_pk, actor=3, ref=source_pk, status="D", code=200),
            agent_name="ars-ars-agent",
            inforesid="infores:ars",
            result_count=2,
            result_stat={"x": 1},
        )
    )
    db["get_message_row"].return_value = source
    db["get_children"].return_value = children
    payload = {
        "message": {
            "query_graph": json.loads(json.dumps(QUERY["message"]["query_graph"])),
            "results": [{"node_bindings": {"n0": [], "n1": []}, "analyses": [{"edge_bindings": {"e": []}}]}],
        },
        "logs": [],
    }
    db["load_message_data"].side_effect = lambda pk, *a: json.loads(json.dumps(payload))
    return source, merged_pk, ara_pk, kp_pk


async def test_materialize_copies_children_and_merged(db):
    source, merged_pk, ara_pk, kp_pk = _source_tree(db)
    parent = row(params={"query_type": "standard"})
    _, source_map = cache.canonical_graph(QUERY["message"]["query_graph"])
    caller_qg = {
        "nodes": {"dz": {"ids": ["MONDO:0005148"]}, "cx": {}},
        "edges": {"rel": {"subject": "cx", "object": "dz"}},
    }
    _, caller_map = cache.canonical_graph(caller_qg)
    e = entry(source["id"], label_map=source_map)
    out = await cache.materialize(parent, e, caller_map, LOGGER)
    assert out is not None
    # three rows created: two ARA children + the final merge (intermediate skipped)
    created = [c.kwargs for c in db["create_message"].await_args_list]
    assert [c["actor_id"] for c in created] == [7, 8, 3]
    assert all(c["ref"] == parent["id"] for c in created)
    assert created[0]["name"] == "aragorn"
    # exact status/code restored on each copy, incl. the 598
    child_updates = [c.kwargs for c in db["update_message"].await_args_list if c.kwargs.get("skip_coercion")]
    assert [(u["status"], u["code"]) for u in child_updates] == [("D", 200), ("E", 598), ("D", 200)]
    assert child_updates[0]["url"] == "http://aragorn"
    assert child_updates[0]["result_count"] == 2
    # payloads rewritten to caller labels; only the merged one gets the log line
    saved = [c.args[1] for c in db["save_message_data"].await_args_list]
    assert len(saved) == 3
    for p in saved:
        assert set(p["message"]["query_graph"]["nodes"]) == {"dz", "cx"}
        assert set(p["message"]["results"][0]["node_bindings"]) == {"dz", "cx"}
        assert set(p["message"]["results"][0]["analyses"][0]["edge_bindings"]) == {"rel"}
    assert saved[0]["logs"] == [] and saved[1]["logs"] == []
    assert "Served from ARS response cache" in saved[2]["logs"][0]["message"]
    # parent flipped to Done, pointing at the copied merge, carrying stats
    parent_update = db["update_message"].await_args_list[-1]
    assert parent_update.args[0] == parent["id"]
    kw = parent_update.kwargs
    assert kw["status"] == "D" and kw["code"] == 200
    assert kw["merged_version"] == str(db["created_rows"][2]["id"])
    assert out["merged_version"] == kw["merged_version"]
    assert kw["merged_versions_list"] == [[kw["merged_version"], "ars"]]
    assert kw["result_count"] == 2 and kw["result_stat"] == {"x": 1}
    assert kw["params"]["stats"] == {"results": 2}
    assert kw["params"]["query_type"] == "standard"
    assert kw["params"]["cache"]["role"] == "hit"
    assert kw["params"]["cache"]["source_pk"] == str(source["id"])
    assert db["persist_data_copy"].await_count == 4  # 3 copies + parent
    db["notify"].assert_awaited_once()
    db["record_cache_hit"].assert_awaited_once_with(1, KEY)
    db["delete_children"].assert_not_awaited()


async def test_materialize_identity_labels_leave_graph_alone(db):
    source, *_ = _source_tree(db, with_intermediate_merge=False)
    _, source_map = cache.canonical_graph(QUERY["message"]["query_graph"])
    out = await cache.materialize(row(), entry(source["id"], label_map=source_map), CALLER_MAP, LOGGER)
    assert out is not None
    saved = [c.args[1] for c in db["save_message_data"].await_args_list]
    assert set(saved[-1]["message"]["query_graph"]["nodes"]) == {"n0", "n1"}


@pytest.mark.parametrize(
    "breakage",
    ["missing_source", "source_running", "no_merged_version", "merged_not_child", "no_payload"],
)
async def test_materialize_broken_entry_is_dropped(db, breakage):
    source, merged_pk, *_ = _source_tree(db)
    if breakage == "missing_source":
        db["get_message_row"].return_value = None
    elif breakage == "source_running":
        source["status"] = "R"
    elif breakage == "no_merged_version":
        source["merged_version"] = None
    elif breakage == "merged_not_child":
        db["get_children"].return_value = [c for c in db["get_children"].return_value if c["id"] != merged_pk]
    elif breakage == "no_payload":
        db["load_message_data"].side_effect = None
        db["load_message_data"].return_value = None
    parent = row()
    out = await cache.materialize(parent, entry(source["id"]), CALLER_MAP, LOGGER)
    assert out is None
    db["delete_cache_entry"].assert_awaited_once_with(1, KEY)
    db["delete_children"].assert_awaited_once_with(parent["id"])
    db["create_message"].assert_not_awaited()


async def test_materialize_transient_failure_keeps_entry_and_cleans_up(db):
    source, *_ = _source_tree(db)
    db["save_message_data"].side_effect = RuntimeError("redis hiccup")
    parent = row()
    out = await cache.materialize(parent, entry(source["id"]), CALLER_MAP, LOGGER)
    assert out is None
    db["delete_cache_entry"].assert_not_awaited()
    db["delete_children"].assert_awaited_once_with(parent["id"])


# ---------------------------------------------------------------------------
# completion hook / fail-over
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
    for role in (None, "hit", "follower", "bypass", "uncached"):
        params = {"query_type": "standard"}
        if role:
            params["cache"] = {"role": role, "key": KEY, "generation": 1}
        await cache.on_parent_complete(row(status="D", params=params), LOGGER)
    db["get_children"].assert_not_awaited()
    db["mark_cache_entry_ready"].assert_not_awaited()


async def test_leader_publishes_entry_and_resolves_waiters(db, mocker):
    merged_pk = uuid.uuid4()
    leader = _leader(merged_version=merged_pk)
    db["get_children"].return_value = [_ara("D", 3), _merge(merged_pk, 3)]
    db["load_message_data"].return_value = QUERY
    ready = entry(leader["id"], label_map={"nodes": {}})
    db["mark_cache_entry_ready"].return_value = ready
    w1, w2 = row(), row()
    db["get_cache_waiters"].return_value = [
        {"parent_pk": w1["id"], "leader_pk": leader["id"], "generation": 1, "cache_key": KEY},
        {"parent_pk": w2["id"], "leader_pk": leader["id"], "generation": 1, "cache_key": KEY},
    ]
    db["get_message_row"].side_effect = lambda pk: {w1["id"]: w1, w2["id"]: w2}.get(pk)
    mat = mocker.patch.object(
        cache, "materialize", new_callable=AsyncMock, side_effect=[w1, None]
    )
    await cache.on_parent_complete(leader, LOGGER)
    _, label_map = cache.cache_key(QUERY)
    db["mark_cache_entry_ready"].assert_awaited_once_with(leader["id"], label_map)
    assert mat.await_count == 2
    assert mat.await_args_list[0].args[1] is ready
    assert mat.await_args_list[0].args[2] == label_map  # waiter's own caller map
    # both were claimed (delete-first); the failed one is put back for the sweep
    assert [c.args[0] for c in db["claim_cache_waiter"].await_args_list] == [w1["id"], w2["id"]]
    db["add_cache_waiter"].assert_awaited_once_with(w2["id"], leader["id"], 1, KEY)


async def test_waiter_already_claimed_elsewhere_is_skipped(db, mocker):
    """Two completion hooks racing on one leader: the second claim fails and
    that resolver does not copy."""
    merged_pk = uuid.uuid4()
    leader = _leader(merged_version=merged_pk)
    db["get_children"].return_value = [_ara("D", 1), _merge(merged_pk, 1)]
    db["load_message_data"].return_value = QUERY
    db["mark_cache_entry_ready"].return_value = entry(leader["id"])
    db["get_cache_waiters"].return_value = [
        {"parent_pk": uuid.uuid4(), "leader_pk": leader["id"], "generation": 1, "cache_key": KEY}
    ]
    db["claim_cache_waiter"].return_value = False
    mat = mocker.patch.object(cache, "materialize", new_callable=AsyncMock)
    await cache.on_parent_complete(leader, LOGGER)
    mat.assert_not_awaited()


async def test_leader_lost_entry_still_answers_waiters(db, mocker):
    merged_pk = uuid.uuid4()
    leader = _leader(merged_version=merged_pk)
    db["get_children"].return_value = [_ara("D", 1), _merge(merged_pk, 1)]
    db["load_message_data"].return_value = QUERY
    db["mark_cache_entry_ready"].return_value = None
    w = row()
    db["get_cache_waiters"].return_value = [
        {"parent_pk": w["id"], "leader_pk": leader["id"], "generation": 1, "cache_key": KEY}
    ]
    db["get_message_row"].return_value = w
    mat = mocker.patch.object(cache, "materialize", new_callable=AsyncMock, return_value=w)
    await cache.on_parent_complete(leader, LOGGER)
    used = mat.await_args.args[1]
    assert used["source_pk"] == leader["id"] and used["cache_key"] == KEY


async def test_leader_empty_with_errors_is_not_cached_and_fails_over(db, mocker):
    merged_pk = uuid.uuid4()
    leader = _leader(merged_version=merged_pk)
    db["get_children"].return_value = [_ara("E"), _ara("D", 0), _merge(merged_pk, 0)]
    fo = mocker.patch.object(cache, "fail_over", new_callable=AsyncMock)
    await cache.on_parent_complete(leader, LOGGER)
    fo.assert_awaited_once_with(leader["id"], LOGGER)
    db["mark_cache_entry_ready"].assert_not_awaited()


async def test_leader_empty_without_errors_is_cached(db):
    merged_pk = uuid.uuid4()
    leader = _leader(merged_version=merged_pk)
    db["get_children"].return_value = [_ara("D", 0), _merge(merged_pk, 0)]
    db["load_message_data"].return_value = QUERY
    db["mark_cache_entry_ready"].return_value = entry(leader["id"])
    await cache.on_parent_complete(leader, LOGGER)
    db["mark_cache_entry_ready"].assert_awaited_once()


async def test_partial_results_cached_unless_store_partial_off(db, monkeypatch, mocker):
    merged_pk = uuid.uuid4()
    leader = _leader(merged_version=merged_pk)
    db["get_children"].return_value = [_ara("E"), _ara("D", 5), _merge(merged_pk, 5)]
    db["load_message_data"].return_value = QUERY
    db["mark_cache_entry_ready"].return_value = entry(leader["id"])
    await cache.on_parent_complete(leader, LOGGER)
    db["mark_cache_entry_ready"].assert_awaited_once()

    db["mark_cache_entry_ready"].reset_mock()
    monkeypatch.setattr(settings, "ars_cache_store_partial", False)
    fo = mocker.patch.object(cache, "fail_over", new_callable=AsyncMock)
    await cache.on_parent_complete(leader, LOGGER)
    db["mark_cache_entry_ready"].assert_not_awaited()
    fo.assert_awaited_once()


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


async def test_fail_over_without_waiters_drops_pending_entry(db):
    leader_pk = uuid.uuid4()
    assert await cache.fail_over(leader_pk, LOGGER) is None
    db["delete_pending_cache_entry"].assert_awaited_once_with(leader_pk)
    db["add_task"].assert_not_awaited()


async def test_fail_over_promotes_oldest_waiter(db):
    leader_pk = uuid.uuid4()
    w1, w2 = row(), row()
    db["get_cache_waiters"].return_value = [
        {"parent_pk": w1["id"], "leader_pk": leader_pk, "generation": 1, "cache_key": KEY},
        {"parent_pk": w2["id"], "leader_pk": leader_pk, "generation": 1, "cache_key": KEY},
    ]
    db["repoint_pending_cache_entry"].return_value = True
    db["get_message_row"].return_value = w1
    new_pk = await cache.fail_over(leader_pk, LOGGER)
    assert new_pk == str(w1["id"])
    db["repoint_pending_cache_entry"].assert_awaited_once_with(leader_pk, w1["id"])
    db["repoint_cache_waiters"].assert_awaited_once_with(leader_pk, w1["id"])
    params = db["update_message"].await_args.kwargs["params"]
    assert params["cache"]["role"] == "leader"
    assert params["cache"]["promoted_from"] == str(leader_pk)
    task = db["add_task"].await_args
    assert task.args[0] == "ars.fanout"
    assert task.args[1]["parent_pk"] == str(w1["id"])
    assert task.args[1]["query_id"] == str(w1["id"])


async def test_fail_over_recreates_vanished_entry(db):
    leader_pk = uuid.uuid4()
    w1 = row()
    db["get_cache_waiters"].return_value = [
        {"parent_pk": w1["id"], "leader_pk": leader_pk, "generation": 1, "cache_key": KEY}
    ]
    db["repoint_pending_cache_entry"].return_value = False
    db["get_message_row"].return_value = w1
    await cache.fail_over(leader_pk, LOGGER)
    db["claim_or_get_cache_entry"].assert_awaited_once_with(1, KEY, w1["id"])


async def test_on_parent_failed_only_for_leaders(db, mocker):
    fo = mocker.patch.object(cache, "fail_over", new_callable=AsyncMock)
    await cache.on_parent_failed(_leader(role="follower", status="E"), LOGGER)
    fo.assert_not_awaited()
    leader = _leader(status="E")
    await cache.on_parent_failed(leader, LOGGER)
    fo.assert_awaited_once_with(leader["id"], LOGGER)


# ---------------------------------------------------------------------------
# repair sweep / invalidation
# ---------------------------------------------------------------------------


async def test_repair_sweep_disabled(db, monkeypatch):
    monkeypatch.setattr(settings, "ars_cache_enabled", False)
    assert await cache.repair_sweep(LOGGER) == {"stale_pending": 0, "stuck_waiters": 0, "purged": 0}
    db["get_stale_pending_cache_entries"].assert_not_awaited()


async def test_repair_sweep_finishes_done_leader_and_fails_over_others(db, mocker):
    done_leader = _leader()
    stuck_pk, gone_pk = uuid.uuid4(), uuid.uuid4()
    db["get_stale_pending_cache_entries"].return_value = [
        dict(entry(done_leader["id"], "pending"), leader_status="D"),
        dict(entry(stuck_pk, "pending"), leader_status="R"),
        dict(entry(gone_pk, "pending"), leader_status=None),
    ]
    db["get_message_row"].return_value = done_leader
    complete = mocker.patch.object(cache, "on_parent_complete", new_callable=AsyncMock)
    fo = mocker.patch.object(cache, "fail_over", new_callable=AsyncMock)
    counts = await cache.repair_sweep(LOGGER)
    assert counts["stale_pending"] == 3
    complete.assert_awaited_once_with(done_leader, LOGGER)
    assert [c.args[0] for c in fo.await_args_list] == [stuck_pk, gone_pk]
    db["get_stale_pending_cache_entries"].assert_awaited_once_with(settings.ars_cache_pending_max_sec)


async def test_repair_sweep_recopies_stuck_waiter(db, mocker):
    w = row()
    e = entry(uuid.uuid4())
    db["get_stuck_cache_waiters"].return_value = [
        {"parent_pk": w["id"], "leader_pk": e["source_pk"], "generation": 1, "cache_key": KEY, "entry": e}
    ]
    db["get_message_row"].return_value = w
    db["load_message_data"].return_value = QUERY
    mat = mocker.patch.object(cache, "materialize", new_callable=AsyncMock, return_value=w)
    counts = await cache.repair_sweep(LOGGER)
    assert counts["stuck_waiters"] == 1
    db["delete_children"].assert_awaited_once_with(w["id"])
    assert mat.await_args.args[1] is e and mat.await_args.args[2] == CALLER_MAP
    db["claim_cache_waiter"].assert_awaited_once_with(w["id"])
    db["add_cache_waiter"].assert_not_awaited()


async def test_repair_sweep_purges_superseded_generations(db):
    db["get_cache_generation"].return_value = 4
    db["purge_stale_cache_entries"].return_value = 12
    counts = await cache.repair_sweep(LOGGER)
    assert counts["purged"] == 12
    db["purge_stale_cache_entries"].assert_awaited_once_with(4, settings.ars_cache_stale_grace_sec)


async def test_repair_sweep_isolates_failures(db, mocker):
    db["get_stale_pending_cache_entries"].return_value = [
        dict(entry(uuid.uuid4(), "pending"), leader_status="R")
    ]
    mocker.patch.object(cache, "fail_over", new_callable=AsyncMock, side_effect=RuntimeError("x"))
    db["purge_stale_cache_entries"].side_effect = RuntimeError("y")
    counts = await cache.repair_sweep(LOGGER)  # no raise
    assert counts["stale_pending"] == 1 and counts["purged"] == 0


async def test_invalidate_all_bumps_generation(mocker):
    bump = mocker.patch.object(cache.ars_db, "bump_cache_generation", new_callable=AsyncMock, return_value=7)
    assert await cache.invalidate_all("new KG") == 7
    bump.assert_awaited_once_with("new KG")

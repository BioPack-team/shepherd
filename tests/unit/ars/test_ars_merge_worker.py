"""Parity tests for the ars_merge worker.

Upstream reference: NCATSTranslator/Relay @ 3e65975 utils.py
merge_and_post_process + merge_received + post_process: lock, merge-child
creation, fold into the running merged_version, parent bookkeeping
(merged_version, merged_versions_list append, params.stats), the
merged_version_begun notification, then post-processing (blocklist ->
scrub -> annotate -> appraise_confidence -> stats, with the exact failure
codes: 444 for the cleanup stages and the stat calc, confidence failures
only logged), the merged_version_available notification and the parent
completion check.

Work comes from the merge-ready index (real, on fakeredis here): the worker
holding the parent's lock drains it in arrival order, one merged version per
child; a loser acks. The fold + post-process run in a pool child; the tests
stand the pool call in with an outcome, and exercise the child function
itself against fakeredis blobs with the annotator package mocked.
"""

import asyncio
import datetime
import json
import logging
import pathlib
import uuid
from unittest.mock import AsyncMock

import pytest

import shepherd_utils.ars.db as ars_db
import shepherd_utils.broker as broker_mod
from workers.ars_merge import worker as merge_worker

LOGGER = logging.getLogger(__name__)
UTC = datetime.timezone.utc
TS = datetime.datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)


def load_corpus(name):
    return json.loads(pathlib.Path(f"tests/fixtures/ars_corpus/{name}").read_text())


def _outcome(**overrides):
    out = {
        "stats": {"results": 2, "knowledge_graph_nodes": 4},
        "status": "D",
        "code": 200,
        "result_count": 2,
        "result_stat": {"mean": 0.5},
        "logs": [(logging.INFO, "child says hi")],
    }
    out.update(overrides)
    return out


@pytest.fixture
def env(mocker, redis_mock):
    parent_pk = uuid.uuid4()
    child_pk = uuid.uuid4()
    child2_pk = uuid.uuid4()
    rows = {
        str(parent_pk): {
            "id": parent_pk,
            "status": "R",
            "code": 202,
            "agent": "ars-default-agent",
            "ref": None,
            "merged_version": None,
            "merged_versions_list": None,
            "params": {"query_type": "standard"},
            "result_count": None,
        },
        str(child_pk): {
            "id": child_pk,
            "status": "D",
            "code": 200,
            "agent": "ara-shepherd-aragorn",
            "ref": parent_pk,
            "result_count": 2,
        },
        str(child2_pk): {
            "id": child2_pk,
            "status": "D",
            "code": 200,
            "agent": "ara-shepherd-arax",
            "ref": parent_pk,
            "result_count": 1,
        },
    }
    merge_pks = []

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    def _create(**kw):
        pk = uuid.uuid4()
        merge_pks.append(pk)
        rows[str(pk)] = {
            "id": pk,
            "status": "R",
            "code": 202,
            "agent": kw["agent"],
            "ref": kw.get("ref"),
            "result_count": None,
            "updated_at": TS,
        }
        return dict(rows[str(pk)])

    def _update(pk, **kw):
        row = rows.setdefault(str(pk), {"id": pk})
        row.update({k: v for k, v in kw.items() if k != "skip_coercion"})
        return dict(row)

    return {
        "parent_pk": parent_pk,
        "child_pk": child_pk,
        "child2_pk": child2_pk,
        "rows": rows,
        "merge_pks": merge_pks,
        "get_message_row": _patch(
            "get_message_row", side_effect=lambda pk: rows.get(str(pk))
        ),
        "create_message": _patch("create_message", side_effect=_create),
        "update_message": _patch("update_message", side_effect=_update),
        "persist_data_copy": _patch("persist_data_copy"),
        "notify": mocker.patch.object(
            merge_worker, "notify_subscribers", new_callable=AsyncMock
        ),
        "completion": mocker.patch.object(
            merge_worker.lifecycle, "check_parent_completion", new_callable=AsyncMock
        ),
        "run_merge": mocker.patch.object(
            merge_worker,
            "_run_merge_in_pool",
            new_callable=AsyncMock,
            return_value=_outcome(),
        ),
        # the unlock is a Lua script fakeredis cannot run; try_lock itself
        # (a plain SET NX) is real
        "remove_lock": mocker.patch.object(
            merge_worker, "remove_lock", new_callable=AsyncMock
        ),
    }


def _task(env, child_pk=None):
    return [
        "tid",
        {
            "parent_pk": str(env["parent_pk"]),
            "child_pk": str(child_pk or env["child_pk"]),
            "agent_name": "ara-shepherd-aragorn",
            "query_id": str(env["parent_pk"]),
            "log_level": "20",
            "otel": "{}",
        },
    ]


async def _ready(env, *children):
    for pk in children:
        await ars_db.add_ready_child(env["parent_pk"], pk, LOGGER)
        await asyncio.sleep(0.002)  # distinct arrival stamps


async def _merge_tasks():
    from shepherd_utils.broker import get_task

    tasks = []
    while True:
        t = await get_task("ars.merge", "consumer", "t", LOGGER)
        if t is None:
            return tasks
        tasks.append(t)


def _updates_for(env, pk):
    return [
        c.kwargs
        for c in env["update_message"].await_args_list
        if c.args and str(c.args[0]) == str(pk)
    ]


def _events(env):
    return [c.args[1]["event_type"] for c in env["notify"].await_args_list]


async def test_first_merge(env, redis_mock, caplog):
    await _ready(env, env["child_pk"])
    with caplog.at_level(logging.INFO, logger=LOGGER.name):
        await merge_worker.ars_merge(_task(env), LOGGER)

    # merge child created under the ars agent, ref = parent, then the fold
    # + post-process for it in the pool
    kw = env["create_message"].await_args.kwargs
    assert kw["agent"] == "ars-ars-agent"
    assert str(kw["ref"]) == str(env["parent_pk"])
    (merge_pk,) = env["merge_pks"]
    args = env["run_merge"].await_args.args
    assert args[0] is None  # first merge: no current merged version
    assert str(args[1]) == str(env["child_pk"])
    assert str(args[2]) == str(merge_pk)
    assert args[3] == "ara-shepherd-aragorn"

    # parent bookkeeping in one update
    pupdate = next(
        u for u in _updates_for(env, env["parent_pk"]) if "merged_version" in u
    )
    assert str(pupdate["merged_version"]) == str(merge_pk)
    assert pupdate["merged_versions_list"] == [[str(merge_pk), "ara-shepherd-aragorn"]]
    assert pupdate["params"]["stats"]["results"] == 2
    assert pupdate["merge_semaphore"] is False

    # the merged child's final state, from the post-process outcome
    final = next(u for u in _updates_for(env, merge_pk) if "status" in u)
    assert final["status"] == "D"
    assert final["code"] == 200
    assert final["result_count"] == 2
    assert final["result_stat"] == {"mean": 0.5}
    env["persist_data_copy"].assert_awaited_once()
    # the merge's result count is carried up to the parent
    assert {"result_count": 2} in _updates_for(env, env["parent_pk"])

    # begun, then available, then the completion check
    assert _events(env) == ["merged_version_begun", "merged_version_available"]
    begun = env["notify"].await_args_list[0].args[1]
    assert begun["complete"] is False
    assert begun["merged_versions_list"] == [[str(merge_pk), "ara-shepherd-aragorn"]]
    available = env["notify"].await_args_list[1].args[1]
    assert available["merged_version"] == str(merge_pk)
    assert available["stats"] == {"results": 2, "knowledge_graph_nodes": 4}
    env["completion"].assert_awaited_once()

    # the child is drained from the index, the lock released, and the pool
    # child's stage logs replayed into the query's logger
    assert await ars_db.get_ready_children(env["parent_pk"], LOGGER) == []
    env["remove_lock"].assert_awaited_once()
    assert env["remove_lock"].await_args.args[0] == merge_worker._lock_key(
        env["parent_pk"]
    )
    assert "child says hi" in caplog.text
    assert await _merge_tasks() == []


async def test_second_merge_uses_current(env, redis_mock):
    prev = uuid.uuid4()
    env["rows"][str(env["parent_pk"])]["merged_version"] = prev
    env["rows"][str(env["parent_pk"])]["merged_versions_list"] = [
        [str(prev), "ara-shepherd-arax"]
    ]
    await _ready(env, env["child_pk"])
    await merge_worker.ars_merge(_task(env), LOGGER)
    args = env["run_merge"].await_args.args
    assert str(args[0]) == str(prev)
    pupdate = next(
        u for u in _updates_for(env, env["parent_pk"]) if "merged_version" in u
    )
    (merge_pk,) = env["merge_pks"]
    assert pupdate["merged_versions_list"] == [
        [str(prev), "ara-shepherd-arax"],
        [str(merge_pk), "ara-shepherd-aragorn"],
    ]


async def test_drain_folds_every_ready_child_in_arrival_order(env, redis_mock):
    """Two children ready at once: the lock holder folds both, oldest first,
    each into its own merged version chained off the previous one, with the
    full begun/available/completion sequence per version."""
    await _ready(env, env["child_pk"], env["child2_pk"])
    await merge_worker.ars_merge(_task(env), LOGGER)

    assert env["create_message"].await_count == 2
    first, second = env["merge_pks"]
    calls = env["run_merge"].await_args_list
    assert [str(c.args[1]) for c in calls] == [
        str(env["child_pk"]),
        str(env["child2_pk"]),
    ]
    assert calls[0].args[0] is None
    assert str(calls[1].args[0]) == str(first)  # chained off the first version
    assert calls[1].args[3] == "ara-shepherd-arax"
    assert env["rows"][str(env["parent_pk"])]["merged_versions_list"] == [
        [str(first), "ara-shepherd-aragorn"],
        [str(second), "ara-shepherd-arax"],
    ]
    assert _events(env) == [
        "merged_version_begun",
        "merged_version_available",
        "merged_version_begun",
        "merged_version_available",
    ]
    assert env["completion"].await_count == 2
    assert await ars_db.get_ready_children(env["parent_pk"], LOGGER) == []
    assert await _merge_tasks() == []


async def test_lock_busy_acks_without_reenqueue(env, redis_mock):
    """A worker that loses the parent's lock does nothing: its child is in
    the index and the holder drains it. No re-enqueue, no wait."""
    await _ready(env, env["child_pk"])
    assert await broker_mod.try_lock(
        merge_worker._lock_key(env["parent_pk"]), "other-worker", LOGGER
    )
    await merge_worker.ars_merge(_task(env), LOGGER)
    env["create_message"].assert_not_awaited()
    env["run_merge"].assert_not_awaited()
    assert await _merge_tasks() == []
    # still there for the holder
    assert await ars_db.get_ready_children(env["parent_pk"], LOGGER) == [
        str(env["child_pk"])
    ]


async def test_child_arriving_after_the_final_read_gets_one_wake(
    env, mocker, redis_mock
):
    """A child recorded after the holder's last empty read but before the
    lock was released has a wake task that found the lock held and acked;
    the holder's post-release recheck kicks exactly one new wake for it."""
    seen = [[str(env["child_pk"])], [], [str(env["child2_pk"])]]
    mocker.patch.object(
        ars_db,
        "get_ready_children",
        new_callable=AsyncMock,
        side_effect=lambda pk, logger: seen.pop(0) if seen else [],
    )
    await merge_worker.ars_merge(_task(env), LOGGER)
    assert env["run_merge"].await_count == 1
    tasks = await _merge_tasks()
    assert len(tasks) == 1
    assert tasks[0][1]["parent_pk"] == str(env["parent_pk"])
    assert "_started_at" not in tasks[0][1]


async def test_merge_failure_leaves_merge_child_running(env, redis_mock):
    """Upstream merge_received swallows the failure and returns {}; the shell
    merge child stays Running (the 8-minute watchdog eventually 598s it),
    the semaphore is released, and the drain goes on to the next child."""
    await _ready(env, env["child_pk"], env["child2_pk"])

    async def _run(current_pk, child_pk, *a):
        if str(child_pk) == str(env["child_pk"]):
            raise RuntimeError("merge exploded")
        return _outcome(result_count=1)

    env["run_merge"].side_effect = _run
    await merge_worker.ars_merge(_task(env), LOGGER)

    failed_shell, good = env["merge_pks"]
    # no final state for the failed shell, no bookkeeping pointing at it
    assert not any("status" in u for u in _updates_for(env, failed_shell))
    assert env["rows"][str(env["parent_pk"])]["merged_version"] == str(good)
    assert env["rows"][str(env["parent_pk"])]["merge_semaphore"] is False
    assert _events(env) == ["merged_version_begun", "merged_version_available"]
    env["completion"].assert_awaited_once()
    # both attempted, both cleared: a poison child cannot wedge the drain
    assert await ars_db.get_ready_children(env["parent_pk"], LOGGER) == []


async def test_no_results_leaves_the_parent_count_alone(env, redis_mock):
    """A merged message with no results must not stamp a count on the
    parent -- there is nothing to report yet."""
    env["run_merge"].return_value = _outcome(result_count=None, result_stat=None)
    await _ready(env, env["child_pk"])
    await merge_worker.ars_merge(_task(env), LOGGER)
    assert {"result_count": None} not in _updates_for(env, env["parent_pk"])
    assert not any(
        "result_count" in u and "merged_version" not in u
        for u in _updates_for(env, env["parent_pk"])
    )
    (merge_pk,) = env["merge_pks"]
    final = next(u for u in _updates_for(env, merge_pk) if "status" in u)
    assert "result_count" not in final


async def test_postprocess_failure_codes_land_on_the_merged_child(env, redis_mock):
    """The pool child reports upstream's sticky stage codes; the row gets
    them, the notifications and completion check still go out."""
    env["run_merge"].return_value = _outcome(status="E", code=444)
    await _ready(env, env["child_pk"])
    await merge_worker.ars_merge(_task(env), LOGGER)
    (merge_pk,) = env["merge_pks"]
    final = next(u for u in _updates_for(env, merge_pk) if "status" in u)
    assert final["status"] == "E"
    assert final["code"] == 444
    assert _events(env) == ["merged_version_begun", "merged_version_available"]
    env["completion"].assert_awaited_once()


# ---------------------------------------------------------------------------
# pool side: the fold (golden-backed) and the post-process stages
# ---------------------------------------------------------------------------


@pytest.fixture
def sync_blobs(monkeypatch):
    import fakeredis

    import shepherd_utils.db as shepherd_db

    sync_redis = fakeredis.FakeRedis()
    monkeypatch.setattr(shepherd_db, "_sync_data_db_client", sync_redis)
    return shepherd_db


def test_merge_in_child_folds_messages(sync_blobs):
    """The pool-side merge against real (fake) redis blobs matches the
    golden-tested mergeMessages path."""
    shepherd_db = sync_blobs
    aragorn = load_corpus("response_aragorn.json")
    arax = load_corpus("response_arax.json")
    child_pk = "11111111-1111-1111-1111-111111111111"
    current_pk = "22222222-2222-2222-2222-222222222222"
    new_pk = "33333333-3333-3333-3333-333333333333"

    shepherd_db.save_message_sync(child_pk, aragorn)
    stats = merge_worker.merge_in_child(None, child_pk, current_pk)
    first = shepherd_db.get_message_sync(current_pk)
    assert first["message"]["results"] == aragorn["message"]["results"]
    assert stats["results"] == 2

    shepherd_db.save_message_sync(child_pk, arax)
    stats = merge_worker.merge_in_child(current_pk, child_pk, new_pk)
    merged = shepherd_db.get_message_sync(new_pk)
    assert stats["results"] == 3
    assert set(merged["message"]["knowledge_graph"]["edges"].keys()) == {
        "e1",
        "e2",
        "e3",
        "e9",
    }


ANNOTATIONS = {
    "MONDO:0005148": {"disease_info": {"mondo": "0005148"}},
    "CHEBI:6801": [{"notfound": True}],
    "NCBIGene:5468": {"gene_info": {"symbol": "PPARG"}},
}


@pytest.fixture
def bt_annotator(mocker):
    """The in-process biothings_annotator package, mocked at the Annotator
    class (upstream uses the same package; no HTTP is involved)."""
    inst = mocker.MagicMock()
    inst.annotate_curie_list = AsyncMock(return_value=ANNOTATIONS)
    mocker.patch.object(merge_worker.annotator, "Annotator", return_value=inst)
    # any HTTP during post-process is a regression: the Appraiser and the
    # annotator API transport are both gone
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=AsyncMock,
        side_effect=AssertionError("postprocess made an HTTP call"),
    )
    return inst


def _expected_confidence(result):
    product = 1
    for analysis in result.get("analyses") or []:
        if analysis.get("score") is not None:
            product = product * (1 - analysis["score"])
    return 1 - product


def _merged_row():
    return {"id": str(uuid.uuid4()), "code": 202, "updated_at": TS}


async def test_postprocess_happy_path(bt_annotator):
    data = load_corpus("response_aragorn.json")
    expected_confidences = [_expected_confidence(r) for r in data["message"]["results"]]

    outcome = await merge_worker.postprocess_message(
        data, _merged_row(), "ara-shepherd-aragorn", LOGGER
    )

    # the package was asked to annotate exactly the unannotated valid curies
    (curie_list,) = bt_annotator.annotate_curie_list.await_args.args
    assert sorted(curie_list) == ["CHEBI:6801", "MONDO:0005148", "NCBIGene:5468"]

    # merged child -> D/200 with result_count + result_stat
    assert outcome["status"] == "D"
    assert outcome["code"] == 200
    assert outcome["result_count"] == 2
    assert outcome["result_stat"]

    # ordering_components computed locally by appraise_confidence; no sugeno
    results = data["message"]["results"]
    for result, confidence in zip(results, expected_confidences):
        assert result["ordering_components"] == {
            "confidence": confidence,
            "clinical_evidence": 0.0,
            "novelty": 0.0,
        }
        assert "sugeno" not in result
        assert "rank" not in result
    nodes = data["message"]["knowledge_graph"]["nodes"]
    annotated = [
        a
        for a in nodes["MONDO:0005148"]["attributes"]
        if a.get("attribute_type_id") == "biothings_annotations"
    ]
    assert len(annotated) == 1
    assert annotated[0]["value"] == ANNOTATIONS["MONDO:0005148"]
    # notfound entries ([{"notfound": true}]) are skipped
    assert not any(
        a.get("attribute_type_id") == "biothings_annotations"
        for a in nodes["CHEBI:6801"]["attributes"]
    )


async def test_postprocess_confidence_failure_only_logged(bt_annotator, mocker):
    """appraise_confidence failures are logged and swallowed: the message
    still completes D/200 (upstream post_process @ 3e65975)."""
    mocker.patch.object(
        merge_worker, "appraise_confidence", side_effect=RuntimeError("confidence boom")
    )
    data = load_corpus("response_aragorn.json")
    outcome = await merge_worker.postprocess_message(
        data, _merged_row(), "ara-shepherd-aragorn", LOGGER
    )
    assert outcome["status"] == "D"
    assert outcome["code"] == 200
    assert outcome["result_count"] == 2


async def test_postprocess_stat_calc_failure_is_444(bt_annotator, mocker):
    """A ScoreStatCalc/result-count failure marks the merged child E/444 and
    skips the 202->200 flip; the data carries the error log entries."""
    mocker.patch.object(
        merge_worker, "ScoreStatCalc", side_effect=RuntimeError("stat boom")
    )
    data = load_corpus("response_aragorn.json")
    outcome = await merge_worker.postprocess_message(
        data, _merged_row(), "ara-shepherd-aragorn", LOGGER
    )
    assert outcome["status"] == "E"
    assert outcome["code"] == 444
    # len(results) was assigned before the stat calc raised, as upstream
    assert outcome["result_count"] == 2
    assert any(
        entry["message"] == "Error in score stat calculation"
        for entry in data.get("logs", [])
    )


async def test_postprocess_empty_results_still_completes(bt_annotator):
    """No results: the count/stat/confidence block is skipped entirely and
    the 202 shell still flips to D/200."""
    data = load_corpus("response_aragorn.json")
    data["message"]["results"] = []
    outcome = await merge_worker.postprocess_message(
        data, _merged_row(), "ara-shepherd-aragorn", LOGGER
    )
    assert outcome["status"] == "D"
    assert outcome["code"] == 200
    assert outcome["result_count"] is None


async def test_postprocess_annotator_failure_is_444(mocker):
    """A package-level annotation failure marks the merged child E/444 and
    the 444 sticks through the successful later stages, as upstream."""
    inst = mocker.MagicMock()
    inst.annotate_curie_list = AsyncMock(side_effect=RuntimeError("annotator down"))
    mocker.patch.object(merge_worker.annotator, "Annotator", return_value=inst)
    data = load_corpus("response_aragorn.json")
    outcome = await merge_worker.postprocess_message(
        data, _merged_row(), "ara-shepherd-aragorn", LOGGER
    )
    assert outcome["status"] == "E"
    assert outcome["code"] == 444
    assert outcome["result_count"] == 2
    assert any(
        entry["message"].startswith("node annotation internal error")
        for entry in data.get("logs", [])
    )


def test_merge_and_postprocess_in_child(sync_blobs, bt_annotator):
    """The production pool call end to end against fake redis: the fold is
    saved, then post-processed in place and saved again; the outcome, the
    stats, and the child's stage logs come back."""
    shepherd_db = sync_blobs
    child_pk = "11111111-1111-1111-1111-111111111111"
    new_pk = "33333333-3333-3333-3333-333333333333"
    shepherd_db.save_message_sync(child_pk, load_corpus("response_aragorn.json"))

    outcome = merge_worker.merge_and_postprocess_in_child(
        None, child_pk, new_pk, "ara-shepherd-aragorn", TS.isoformat()
    )

    assert outcome["status"] == "D"
    assert outcome["code"] == 200
    assert outcome["result_count"] == 2
    assert outcome["stats"]["results"] == 2
    assert any("annotating" in message for _, message in outcome["logs"])
    merged = shepherd_db.get_message_sync(new_pk)
    assert all("ordering_components" in r for r in merged["message"]["results"])
    nodes = merged["message"]["knowledge_graph"]["nodes"]
    assert any(
        a.get("attribute_type_id") == "biothings_annotations"
        for a in nodes["MONDO:0005148"]["attributes"]
    )


def test_merge_and_postprocess_in_child_final_save_failure_is_422(
    sync_blobs, bt_annotator, mocker
):
    shepherd_db = sync_blobs
    child_pk = "11111111-1111-1111-1111-111111111111"
    shepherd_db.save_message_sync(child_pk, load_corpus("response_aragorn.json"))
    real_save = merge_worker.save_message_sync
    calls = {"n": 0}

    def _flaky(pk, payload):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("redis full")
        real_save(pk, payload)

    mocker.patch.object(merge_worker, "save_message_sync", side_effect=_flaky)
    outcome = merge_worker.merge_and_postprocess_in_child(
        None, child_pk, str(uuid.uuid4()), "ara-shepherd-aragorn", None
    )
    assert outcome["status"] == "E"
    assert outcome["code"] == 422

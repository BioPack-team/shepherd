"""Parity tests for the ars_premerge worker.

Upstream reference: NCATSTranslator/Relay @ 3e65975 api.py (the result
callback view): intake guards and counts, pre_merge_process ->
remove_phantom_support_graphs -> validate over the payload, the D/200 (or
header-status) flip and merge hand-off on success, E/422 + the
ara_failed_validation notification on invalid TRAPI, the generic-handler
E/500 with its "Internal ARS Server Error" log entry on a crash, and the
parent completion check after every terminal outcome.

The premerge stages run in a process pool that reads and writes the child's
blob by pk with the sync Redis client; the tests stand those two calls in
(``get_message_sync`` / ``save_message_sync`` on the worker module) and the
pool indirection falls back to a worker thread, so the stages themselves run
for real over the corpus.
"""

import json
import logging
import pathlib
import uuid
from unittest.mock import AsyncMock

import pytest

import shepherd_utils.ars.annotate as annotate_mod
import shepherd_utils.ars.db as ars_db
from shepherd_utils.config import settings
from workers.ars_premerge import worker as pm

LOGGER = logging.getLogger(__name__)


ANNOTATIONS = {
    "MONDO:0005148": {"disease_info": {"mondo": "0005148"}},
    "CHEBI:6801": [{"notfound": True}],
    "NCBIGene:5468": {"gene_info": {"symbol": "PPARG"}},
}


def _annotated_curies(payload):
    return {
        curie
        for curie, node in payload["message"]["knowledge_graph"]["nodes"].items()
        if any(
            a.get("attribute_type_id") == "biothings_annotations"
            for a in node.get("attributes") or []
        )
    }


def load_corpus(name):
    return json.loads(pathlib.Path(f"tests/fixtures/ars_corpus/{name}").read_text())


@pytest.fixture
def env(mocker, redis_mock):
    parent_pk = uuid.uuid4()
    child_pk = uuid.uuid4()
    child_row = {
        "id": child_pk,
        "status": "R",
        "code": 202,
        "agent": "ara-shepherd-aragorn",
        "ref": parent_pk,
        "result_count": 2,
        "params": {"query_type": "standard"},
    }
    parent_row = {
        "id": parent_pk,
        "status": "R",
        "code": 202,
        "agent": "ars-default-agent",
        "result_count": None,
    }
    data = load_corpus("response_aragorn.json")

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    rows = {str(child_pk): child_row, str(parent_pk): parent_row}
    # the blob store as the pool child sees it
    blobs = {str(child_pk): data}
    saved = []

    def _get_sync(pk):
        if pk not in blobs:
            raise KeyError(pk)
        return blobs[pk]

    def _save_sync(pk, payload):
        blobs[pk] = payload
        saved.append((pk, payload))

    mocker.patch.object(pm, "get_message_sync", side_effect=_get_sync)
    mocker.patch.object(pm, "save_message_sync", side_effect=_save_sync)
    # the in-process biothings_annotator package (ars_annotation_mode
    # "premerge" runs it here); never the live BioThings APIs
    annotator = mocker.MagicMock()
    annotator.annotate_curie_list = AsyncMock(return_value=ANNOTATIONS)
    mocker.patch.object(annotate_mod.annotator, "Annotator", return_value=annotator)
    return {
        "annotator": annotator,
        "parent_pk": parent_pk,
        "child_pk": child_pk,
        "child_row": child_row,
        "data": data,
        "saved": saved,
        "get_message_row": _patch(
            "get_message_row", side_effect=lambda pk: rows.get(str(pk))
        ),
        "update_message": _patch(
            "update_message",
            side_effect=lambda pk, **kw: {
                **rows.get(str(pk), {}),
                **{k: v for k, v in kw.items() if k != "skip_coercion"},
            },
        ),
        "load_message_data": _patch("load_message_data", return_value=data),
        "save_message_data": _patch("save_message_data"),
        "persist_data_copy": _patch("persist_data_copy"),
        "notify": mocker.patch.object(pm, "notify_subscribers", new_callable=AsyncMock),
        "completion": mocker.patch.object(
            pm.lifecycle, "check_parent_completion", new_callable=AsyncMock
        ),
    }


def _task(env, status="D", agent="ara-shepherd-aragorn"):
    return [
        "tid",
        {
            "child_pk": str(env["child_pk"]),
            "parent_pk": str(env["parent_pk"]),
            "agent_name": agent,
            "inforesid": "infores:shepherd-aragorn",
            "status": status,
            "query_id": str(env["parent_pk"]),
            "log_level": "20",
            "otel": '{"traceparent": "00-sub"}',
        },
    ]


async def _merge_tasks():
    from shepherd_utils.broker import get_task

    tasks = []
    while True:
        t = await get_task("ars.merge", "consumer", "t", LOGGER)
        if t is None:
            return tasks
        tasks.append(t)


async def _ready(env):
    return await ars_db.get_ready_children(env["parent_pk"], LOGGER)


def _final_status_update(env):
    return next(
        c.kwargs for c in env["update_message"].await_args_list if "status" in c.kwargs
    )


async def test_premerge_happy_path(env, redis_mock):
    await pm.ars_premerge(_task(env), LOGGER)

    # the pool child wrote the premerged payload back (normalize_scores ran)
    pk, payload = env["saved"][-1]
    assert pk == str(env["child_pk"])
    assert "normalized_score" in payload["message"]["results"][0]
    # ...and nothing large went through the async save in this process
    env["save_message_data"].assert_not_awaited()

    # child flipped to D/200
    final = _final_status_update(env)
    assert final["status"] == "D"
    assert final["code"] == 200
    env["persist_data_copy"].assert_awaited()
    env["completion"].assert_awaited_once()

    # the child is merge-ready, and one wake task went out in the query's trace
    assert await _ready(env) == [str(env["child_pk"])]
    tasks = await _merge_tasks()
    assert len(tasks) == 1
    assert tasks[0][1]["parent_pk"] == str(env["parent_pk"])
    assert tasks[0][1]["child_pk"] == str(env["child_pk"])
    assert tasks[0][1]["otel"] == '{"traceparent": "00-sub"}'


async def test_premerge_status_header_override(env, redis_mock):
    """The callback's tr_ars.message.status header value rides the task and
    lands on the child, as upstream applied it at the end of its view."""
    await pm.ars_premerge(_task(env, status="S"), LOGGER)
    assert _final_status_update(env)["status"] == "S"


async def test_premerge_validation_failure_is_422(env, redis_mock):
    del env["data"]["message"]["results"][0]["node_bindings"]

    await pm.ars_premerge(_task(env), LOGGER)

    final = _final_status_update(env)
    assert final["status"] == "E"
    assert final["code"] == 422
    # the premerged payload is still saved, as upstream saved it on this
    # branch too
    assert env["saved"][-1][0] == str(env["child_pk"])
    fields = env["notify"].await_args.args[1]
    assert fields["event_type"] == "ara_failed_validation"
    assert fields["ara_name"] == "infores:shepherd-aragorn"
    assert fields["child_uuid"] == str(env["child_pk"])
    assert fields["ara_n_results"] == 2
    env["completion"].assert_awaited_once()
    assert await _merge_tasks() == []
    assert await _ready(env) == []


async def test_premerge_validate_false_skips_validation(env, redis_mock):
    """params.validate == False skips phantom removal + validation, exactly
    like upstream's callback."""
    env["child_row"]["params"] = {"query_type": "standard", "validate": False}
    del env["data"]["message"]["results"][0]["node_bindings"]  # would fail

    await pm.ars_premerge(_task(env), LOGGER)
    assert _final_status_update(env)["status"] == "D"
    assert len(await _merge_tasks()) == 1


async def test_premerge_crash_is_500_with_log_entry(env, mocker, redis_mock):
    """A premerge failure reproduces upstream's generic callback handler:
    E/500 and the 'Internal ARS Server Error' log entry in the payload. The
    failure happens in the pool child, so the payload is loaded here to
    carry the entry."""
    mocker.patch.object(pm, "pre_merge_process", side_effect=RuntimeError("scrub boom"))
    await pm.ars_premerge(_task(env), LOGGER)

    final = _final_status_update(env)
    assert final["status"] == "E"
    assert final["code"] == 500
    saved = env["save_message_data"].await_args_list[-1].args[1]
    assert any(
        entry["message"] == "Internal ARS Server Error"
        for entry in saved.get("logs", [])
    )
    env["completion"].assert_awaited_once()
    assert await _merge_tasks() == []
    assert await _ready(env) == []


async def test_premerge_missing_blob_is_500(env, redis_mock):
    """The child's blob is gone from the store: the same generic-handler
    E/500, from the pool child's KeyError."""
    env["child_row"]["id"] = env["child_pk"]
    task = _task(env)
    task[1]["child_pk"] = str(uuid.uuid4())
    env["get_message_row"].side_effect = lambda pk: env["child_row"]
    await pm.ars_premerge(task, LOGGER)
    final = _final_status_update(env)
    assert final["status"] == "E"
    assert final["code"] == 500


async def test_premerge_non_ara_agent_no_merge(env, redis_mock):
    """A non-ARA agent premerges and goes terminal but never hands off to
    the merge, exactly like upstream's agent_name.startswith('ara-') guard."""
    await pm.ars_premerge(_task(env, agent="kp-genetics"), LOGGER)
    assert _final_status_update(env)["status"] == "D"
    assert await _merge_tasks() == []
    assert await _ready(env) == []


async def test_premerge_handoff_failure_fails_the_child(env, mocker, redis_mock):
    """A validated child that cannot be recorded as merge-ready would leave
    its parent waiting forever on a merge that never comes (parents are
    watchdog-exempt), so the child is failed E/500 instead and the
    completion check runs."""
    mocker.patch.object(
        ars_db,
        "add_ready_child",
        new_callable=AsyncMock,
        side_effect=RuntimeError("redis down"),
    )
    await pm.ars_premerge(_task(env), LOGGER)
    final = _final_status_update(env)
    assert final["status"] == "E"
    assert final["code"] == 500
    env["completion"].assert_awaited_once()
    assert await _merge_tasks() == []


# ---------------------------------------------------------------------------
# intake: finish_query hands an ARA's response over via the queue. The intake
# reproduces the upstream callback view's state machine (guards, counts, the
# ara_response_complete notification), then falls straight into premerge.
# ---------------------------------------------------------------------------


def _intake_task(env, response_id="resp1"):
    return [
        "tid",
        {
            "intake_child_pk": str(env["child_pk"]),
            "response_id": response_id,
            "query_id": "q1",
            "log_level": "20",
            "otel": '{"traceparent": "00-fin"}',
        },
    ]


@pytest.fixture
def intake(env, mocker):
    """Arm the blob-store mocks for a broker delivery."""
    env["child_row"]["result_count"] = None
    logs = [{"message": "ara log line", "level": "INFO"}]
    return {
        "get_message": mocker.patch.object(
            pm, "get_message", new_callable=AsyncMock, return_value=env["data"]
        ),
        "get_logs": mocker.patch.object(
            pm, "get_logs", new_callable=AsyncMock, return_value=logs
        ),
        "logs": logs,
    }


async def test_intake_happy_path_premerges_and_merges(env, intake, redis_mock):
    await pm.ars_premerge(_intake_task(env), LOGGER)

    # ara_response_complete went out, as the upstream callback view sent it,
    # naming the ARA by the infores its agent stands for
    notified = env["notify"].await_args_list[0].args[1]
    assert notified["event_type"] == "ara_response_complete"
    assert notified["ara_name"] == "infores:shepherd-aragorn"
    assert notified["child_uuid"] == str(env["child_pk"])
    assert notified["ara_n_results"] == 2

    # the raw payload was saved with the query's logs spliced in, and the
    # counts recorded before premerge -- exactly the server intake's order
    first_save = env["save_message_data"].await_args_list[0].args[1]
    assert first_save["logs"][-len(intake["logs"]) :] == intake["logs"]
    counts = next(
        c.kwargs
        for c in env["update_message"].await_args_list
        if "result_count" in c.kwargs
    )
    assert counts["result_count"] == 2
    assert counts["result_stat"]

    # then premerge ran in the pool child: normalized payload, D/200, merge
    _, last_save = env["saved"][-1]
    assert "normalized_score" in last_save["message"]["results"][0]
    final = _final_status_update(env)
    assert final["status"] == "D"
    assert final["code"] == 200
    env["completion"].assert_awaited_once()

    assert await _ready(env) == [str(env["child_pk"])]
    tasks = await _merge_tasks()
    assert len(tasks) == 1
    assert tasks[0][1]["parent_pk"] == str(env["parent_pk"])
    assert tasks[0][1]["agent_name"] == "ara-shepherd-aragorn"
    assert tasks[0][1]["otel"] == '{"traceparent": "00-fin"}'


async def test_intake_empty_results_terminal_without_merge(env, intake, redis_mock):
    """results=[] completes the child D/200 with no count recorded and no
    premerge/merge, matching the callback endpoint's no-results branch."""
    env["data"]["message"]["results"] = []
    intake["get_message"].return_value = env["data"]

    await pm.ars_premerge(_intake_task(env), LOGGER)
    final = _final_status_update(env)
    assert final["status"] == "D"
    assert final["code"] == 200
    assert "result_count" not in final
    env["completion"].assert_awaited_once()
    assert await _merge_tasks() == []
    assert env["saved"] == []


async def test_intake_missing_results_zeroes_count(env, intake, redis_mock):
    del env["data"]["message"]["results"]
    intake["get_message"].return_value = env["data"]

    await pm.ars_premerge(_intake_task(env), LOGGER)
    final = _final_status_update(env)
    assert final["status"] == "D"
    assert final["result_count"] == 0
    assert await _merge_tasks() == []


async def test_intake_duplicate_done_child_skips(env, intake, redis_mock):
    """A child already Done is left alone (the endpoint's dup-200 guard);
    the notification still goes out first, as it does upstream."""
    env["child_row"]["status"] = "D"
    await pm.ars_premerge(_intake_task(env), LOGGER)
    assert env["notify"].await_args_list[0].args[1]["event_type"] == (
        "ara_response_complete"
    )
    env["update_message"].assert_not_awaited()
    env["save_message_data"].assert_not_awaited()
    assert await _merge_tasks() == []


async def test_intake_repeated_results_skips(env, intake, redis_mock):
    """A child that already has results rejects the update (the 409 guard)."""
    env["child_row"]["result_count"] = 5
    await pm.ars_premerge(_intake_task(env), LOGGER)
    env["update_message"].assert_not_awaited()
    assert await _merge_tasks() == []


async def test_intake_errored_child_skips(env, intake, redis_mock):
    """A child already errored rejects the response (the 400 guard)."""
    env["child_row"]["status"] = "E"
    await pm.ars_premerge(_intake_task(env), LOGGER)
    env["update_message"].assert_not_awaited()
    assert await _merge_tasks() == []


async def test_intake_missing_blob_leaves_child_running(env, intake, redis_mock):
    """No stored response = the callback never arrived: the child stays R
    for the watchdog, and nothing is notified or saved."""
    intake["get_message"].side_effect = KeyError("gone")
    await pm.ars_premerge(_intake_task(env), LOGGER)
    env["notify"].assert_not_awaited()
    env["update_message"].assert_not_awaited()
    env["completion"].assert_not_awaited()


async def test_intake_crash_is_500_with_log_entry(env, intake, mocker, redis_mock):
    """An intake failure reproduces the upstream view's generic handler:
    E/500 with the 'Internal ARS Server Error' log entry."""
    env["notify"].side_effect = RuntimeError("boom")
    await pm.ars_premerge(_intake_task(env), LOGGER)
    final = _final_status_update(env)
    assert final["status"] == "E"
    assert final["code"] == 500
    saved = env["save_message_data"].await_args_list[-1].args[1]
    assert any(
        entry["message"] == "Internal ARS Server Error"
        for entry in saved.get("logs", [])
    )
    env["completion"].assert_awaited_once()
    assert await _merge_tasks() == []


# ---------------------------------------------------------------------------
# ars_annotation_mode: annotation in premerge, off the merge lock
# ---------------------------------------------------------------------------


async def test_premerge_mode_annotates_the_validated_response(
    env, monkeypatch, redis_mock
):
    monkeypatch.setattr(settings, "ars_annotation_mode", "premerge")
    await pm.ars_premerge(_task(env), LOGGER)

    (curies,) = env["annotator"].annotate_curie_list.await_args.args
    assert sorted(curies) == ["CHEBI:6801", "MONDO:0005148", "NCBIGene:5468"]
    # annotations ride the saved response to the merge; notfound stays bare
    payload = env["saved"][-1][1]
    assert _annotated_curies(payload) == {"MONDO:0005148", "NCBIGene:5468"}
    assert _final_status_update(env)["status"] == "D"
    assert await _ready(env) == [str(env["child_pk"])]


@pytest.mark.parametrize("mode", ["merge", "off"])
async def test_other_modes_do_not_annotate_in_premerge(
    env, monkeypatch, redis_mock, mode
):
    monkeypatch.setattr(settings, "ars_annotation_mode", mode)
    await pm.ars_premerge(_task(env), LOGGER)
    env["annotator"].annotate_curie_list.assert_not_awaited()
    assert _annotated_curies(env["saved"][-1][1]) == set()


async def test_premerge_annotation_failure_still_merges(env, monkeypatch, redis_mock):
    """A failed annotation is noted on the response, which still goes D/200
    and merge-ready: its nodes just merge unannotated."""
    monkeypatch.setattr(settings, "ars_annotation_mode", "premerge")
    env["annotator"].annotate_curie_list.side_effect = RuntimeError("annotator down")
    await pm.ars_premerge(_task(env), LOGGER)

    payload = env["saved"][-1][1]
    assert _annotated_curies(payload) == set()
    assert any(
        entry["message"].startswith("node annotation internal error")
        for entry in payload.get("logs", [])
    )
    final = _final_status_update(env)
    assert final["status"] == "D"
    assert final["code"] == 200
    assert await _ready(env) == [str(env["child_pk"])]


async def test_premerge_mode_skips_invalid_and_non_ara_responses(
    env, monkeypatch, redis_mock
):
    monkeypatch.setattr(settings, "ars_annotation_mode", "premerge")
    await pm.ars_premerge(_task(env, agent="kp-genetics"), LOGGER)
    del env["data"]["message"]["results"][0]["node_bindings"]
    await pm.ars_premerge(_task(env), LOGGER)
    env["annotator"].annotate_curie_list.assert_not_awaited()

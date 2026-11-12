"""Parity tests for the ars_premerge worker.

Upstream reference: NCATSTranslator/Relay @ 3e65975 api.py (the result
callback view): intake guards and counts, pre_merge_process ->
remove_phantom_support_graphs -> validate over the payload (deviation 21:
the ARA's infores is reported rather than stamped, and stripped nulls and
removed phantom support graphs are reported too), the D/200 (or
header-status) flip and merge hand-off on success, E/422 + the
ara_failed_validation notification on invalid TRAPI, the generic-handler
E/500 with its "Internal ARS Server Error" log entry on a crash, and the
parent completion check after every terminal outcome.

The premerge stages run in a process pool that reads and writes the blobs
by pk with the sync Redis client; the tests stand those two calls in
(``get_message_sync`` / ``save_message_sync`` on the worker module) and the
pool indirection falls back to a worker thread, so the stages themselves run
for real over the corpus.
"""

import copy
import json
import logging
import pathlib
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import shepherd_utils.ars.annotate as annotate_mod
import shepherd_utils.ars.db as ars_db
from shepherd_utils import shared
from shepherd_utils.config import settings
from shepherd_utils.db import get_logs
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

    # the pool child saved the premerged result (normalize_scores ran) as
    # the merge's working copy -- never over the child's own entry, which is
    # its ARA's response
    ((pk, payload),) = env["saved"]
    assert pk == ars_db.premerged_key(env["child_pk"])
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
    # nothing is saved: the premerged form only exists for the merge to
    # read, and this response is not going to it
    assert env["saved"] == []
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


def _has_null(obj):
    if isinstance(obj, dict):
        return any(v is None or _has_null(v) for v in obj.values())
    if isinstance(obj, list):
        return any(_has_null(v) for v in obj)
    return False


async def test_premerge_reads_explicit_nulls_as_absent(env, redis_mock):
    """TRAPI 2.0 has no nullable members. A null on an optional member is
    read as absent -- the response still validates -- and is stripped from
    the saved payload, so the merge (and everything the ARS serves) never
    carries one. Nulls inside a free-form attribute value are data."""
    data = env["data"]
    data["logs"] = None
    data["message"]["knowledge_graph"]["nodes"]["CHEBI:6801"]["is_set"] = None
    data["message"]["knowledge_graph"]["edges"]["e1"]["qualifiers"] = None
    data["message"]["results"][0]["analyses"][0]["scoring_method"] = None
    data["message"]["knowledge_graph"]["edges"]["e1"]["attributes"].append(
        {"attribute_type_id": "biolink:has_evidence", "value": {"x": None}}
    )

    await pm.ars_premerge(_task(env), LOGGER)

    final = _final_status_update(env)
    assert (final["status"], final["code"]) == ("D", 200)
    _, payload = env["saved"][-1]
    edge = payload["message"]["knowledge_graph"]["edges"]["e1"]
    assert edge["attributes"][-1]["value"] == {"x": None}
    edge["attributes"].pop()
    assert not _has_null(payload)


@pytest.mark.parametrize(
    "mutate",
    [
        # 2.0 required Edge members (1.5 carried these as attributes)
        lambda d: d["message"]["knowledge_graph"]["edges"]["e1"].pop("knowledge_level"),
        lambda d: d["message"]["knowledge_graph"]["edges"]["e1"].pop("agent_type"),
        # a 1.x-shaped node binding
        lambda d: d["message"]["results"][0]["node_bindings"].update(
            sn=[{"id": "MONDO:0005148"}]
        ),
        # forbidden empties (minItems 1)
        lambda d: d["message"]["results"][0].update(analyses=[]),
        lambda d: d["message"]["results"][0]["node_bindings"]["sn"].update(ids=[]),
        # a null on a REQUIRED member reads as missing
        lambda d: d["message"]["knowledge_graph"]["edges"]["e1"].update(predicate=None),
        # Edge is additionalProperties: false
        lambda d: d["message"]["knowledge_graph"]["edges"]["e1"].update(extra=1),
    ],
    ids=[
        "edge_missing_knowledge_level",
        "edge_missing_agent_type",
        "trapi1_node_binding",
        "empty_analyses",
        "empty_binding_ids",
        "null_predicate",
        "edge_extra_member",
    ],
)
async def test_premerge_non_trapi2_response_is_422(env, redis_mock, mutate):
    mutate(env["data"])
    await pm.ars_premerge(_task(env), LOGGER)
    final = _final_status_update(env)
    assert (final["status"], final["code"]) == ("E", 422)
    assert await _merge_tasks() == []


async def test_intake_puts_no_query_logs_in_the_child_payload(env, intake, redis_mock):
    """The premerged copy is the merge's working copy: the query's logs stay
    in the query's log list (the merge folds only the message, and a child
    is served with them from its ARA's response), so the intake no longer
    splices them in -- and never ends up with the ``logs: []`` TRAPI 2.0
    forbids."""
    await pm.ars_premerge(_intake_task(env), LOGGER)
    ((pk, payload),) = env["saved"]
    assert pk == ars_db.premerged_key(env["child_pk"])
    assert "logs" not in payload
    final = _final_status_update(env)
    assert (final["status"], final["code"]) == ("D", 200)


async def test_premerge_crash_is_500_with_log_entry(env, mocker, redis_mock, caplog):
    """A premerge failure ends the child E/500, as upstream's generic
    callback handler did. Upstream also wrote an 'Internal ARS Server Error'
    entry into the payload; the child's payload is its ARA's response, which
    the ARS never writes, so the entry goes to the ARS query's logs."""
    mocker.patch.object(pm, "pre_merge_process", side_effect=RuntimeError("scrub boom"))
    with caplog.at_level(logging.ERROR):
        await pm.ars_premerge(_task(env), LOGGER)

    final = _final_status_update(env)
    assert final["status"] == "E"
    assert final["code"] == 500
    assert "Internal ARS Server Error" in caplog.text
    # nothing written under the child, and nothing saved for the merge
    env["save_message_data"].assert_not_awaited()
    assert env["saved"] == []
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
    # a response as it is stored: the query's logs are kept apart from it
    # (shepherd_utils.trapi.prepare_stored_response)
    env["data"].pop("logs", None)
    # The ARA's response is read in the pool child, through the sync blob
    # store (#217): serve it from there, and the child blobs as before.
    fetch = mocker.MagicMock(return_value=env["data"])
    child_blobs = pm.get_message_sync.side_effect

    def _get_sync(pk):
        if pk.startswith("resp"):
            return fetch(pk)
        return child_blobs(pk)

    pm.get_message_sync.side_effect = _get_sync
    return {"get_message": fetch}


async def test_intake_happy_path_premerges_and_merges(env, intake, redis_mock):
    await pm.ars_premerge(_intake_task(env), LOGGER)

    # ara_response_complete went out, as the upstream callback view sent it,
    # naming the ARA by the infores its agent stands for
    notified = env["notify"].await_args_list[0].args[1]
    assert notified["event_type"] == "ara_response_complete"
    assert notified["ara_name"] == "infores:shepherd-aragorn"
    assert notified["child_uuid"] == str(env["child_pk"])
    assert notified["ara_n_results"] == 2

    # one pool call: the response was loaded once, and its premerged form
    # saved once as the merge's working copy; the response itself (the
    # child's own entry) is not written
    intake["get_message"].assert_called_once_with("resp1")
    ((pk, payload),) = env["saved"]
    assert pk == ars_db.premerged_key(env["child_pk"])
    assert "normalized_score" in payload["message"]["results"][0]
    # the counts are recorded before the child is handed on
    counts = next(
        c.kwargs
        for c in env["update_message"].await_args_list
        if "result_count" in c.kwargs
    )
    assert counts["result_count"] == 2
    assert counts["result_stat"]

    # D/200, merge
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
    # nothing is saved: the child's payload is the ARA's response itself,
    # already in its entry, and nothing goes to the merge
    assert env["saved"] == []


@pytest.mark.parametrize(
    "ara_status,code",
    [
        # the ARA's worker said it doesn't do this kind of query (BTE on
        # Pathfinder): what an HTTP ARA answering 501 is to upstream
        ("ERROR 501", 501),
        # a plain operation failure: what an ARA answering 500 is
        ("ERROR", 500),
    ],
)
async def test_intake_failed_ara_query_errors_the_child(
    env, intake, redis_mock, ara_status, code
):
    """A failed ARA query used to land as a clean D/200 with no results, so
    the ARS never said the ARA had failed, let alone how."""
    task = _intake_task(env)
    task[1]["ara_status"] = ara_status
    task[1]["ara_description"] = "BTE does not support Pathfinder type queries."

    await pm.ars_premerge(task, LOGGER)
    final = _final_status_update(env)
    assert final["status"] == "E"
    assert final["code"] == code
    env["completion"].assert_awaited_once()
    # nothing is premerged or merged from a failed query; the child's
    # payload is the ARA's response itself, which the ARS does not write
    assert await _merge_tasks() == []
    assert env["saved"] == []


async def test_intake_timed_out_ara_query_still_delivers(env, intake, redis_mock):
    """Only a failed operation errors the child: a TIMEOUT query's partial
    results are delivered as before."""
    env["data"]["message"]["results"] = []
    intake["get_message"].return_value = env["data"]
    task = _intake_task(env)
    task[1]["ara_status"] = "TIMEOUT"

    await pm.ars_premerge(task, LOGGER)
    final = _final_status_update(env)
    assert (final["status"], final["code"]) == ("D", 200)


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
    assert env["saved"] == []
    assert await _merge_tasks() == []


async def test_intake_redelivered_before_the_merge_resumes_at_premerge(
    env, intake, redis_mock
):
    """C8-6 (#214): intake records the count before premerge, so a redelivery
    after that point (the worker died) used to hit the repeated-results guard:
    the child stayed Running until the watchdog and its results never merged.
    It now resumes from the stored response."""
    env["child_row"]["result_count"] = 2
    await pm.ars_premerge(_intake_task(env), LOGGER)

    # premerged again from the ARA's own response (sealed, so exactly as
    # sent, whatever the dead worker had got to) and saved once for the merge
    ((pk, payload),) = env["saved"]
    assert pk == ars_db.premerged_key(env["child_pk"])
    assert "normalized_score" in payload["message"]["results"][0]
    final = _final_status_update(env)
    assert final["status"] == "D"
    assert final["code"] == 200
    assert await _ready(env) == [str(env["child_pk"])]
    assert len(await _merge_tasks()) == 1
    env["completion"].assert_awaited_once()


async def test_intake_redelivered_after_the_hand_off_only_finishes_status(
    env, intake, redis_mock
):
    """Already waiting in the merge-ready index: not premerged or merged again."""
    env["child_row"]["result_count"] = 2
    await ars_db.add_ready_child(env["parent_pk"], env["child_pk"], LOGGER)

    await pm.ars_premerge(_intake_task(env), LOGGER)

    final = _final_status_update(env)
    assert final["status"] == "D"
    env["save_message_data"].assert_not_awaited()
    assert env["saved"] == []
    assert await _merge_tasks() == []
    env["completion"].assert_awaited_once()


async def test_intake_redelivered_after_the_merge_only_finishes_status(
    env, intake, redis_mock, mocker
):
    """Already folded in (the parent's merged_versions_list names the agent)."""
    env["child_row"]["result_count"] = 2
    rows = {
        str(env["child_pk"]): env["child_row"],
        str(env["parent_pk"]): {
            "id": env["parent_pk"],
            "status": "R",
            "agent": "ars-default-agent",
            "merged_versions_list": [[str(uuid.uuid4()), "ara-shepherd-aragorn"]],
        },
    }
    env["get_message_row"].side_effect = lambda pk: rows.get(str(pk))

    await pm.ars_premerge(_intake_task(env), LOGGER)

    assert _final_status_update(env)["status"] == "D"
    env["save_message_data"].assert_not_awaited()
    assert env["saved"] == []
    assert await _merge_tasks() == []


async def test_intake_terminal_child_with_results_skips(env, intake, redis_mock):
    """A child that already has results and is no longer Running is left alone
    (the 409 guard)."""
    env["child_row"]["result_count"] = 5
    env["child_row"]["status"] = "S"
    await pm.ars_premerge(_intake_task(env), LOGGER)
    env["update_message"].assert_not_awaited()
    assert await _merge_tasks() == []


async def test_intake_errored_child_skips(env, intake, redis_mock):
    """A child already errored rejects the response (the 400 guard)."""
    env["child_row"]["status"] = "E"
    await pm.ars_premerge(_intake_task(env), LOGGER)
    env["update_message"].assert_not_awaited()
    assert await _merge_tasks() == []


async def test_intake_missing_blob_fails_the_child(env, intake, redis_mock):
    """A finished response whose blob is gone (e.g. lost in a Redis restart)
    will never be re-delivered: the child fails E/500 right away and the
    parent completion check runs, instead of the child sitting Running until
    the watchdog times it out."""
    intake["get_message"].side_effect = KeyError("gone")
    await pm.ars_premerge(_intake_task(env), LOGGER)
    env["notify"].assert_not_awaited()
    final = _final_status_update(env)
    assert final["status"] == "E"
    assert final["code"] == 500
    env["save_message_data"].assert_not_awaited()
    env["completion"].assert_awaited_once_with(env["parent_pk"], LOGGER)
    assert await _merge_tasks() == []


@pytest.mark.parametrize("status", ["D", "E"])
async def test_intake_missing_blob_leaves_terminal_child_alone(
    env, intake, redis_mock, status
):
    """A child the watchdog (or an earlier delivery) already finished is not
    rewritten when its response blob turns out to be missing."""
    env["child_row"]["status"] = status
    intake["get_message"].side_effect = KeyError("gone")
    await pm.ars_premerge(_intake_task(env), LOGGER)
    env["update_message"].assert_not_awaited()
    env["save_message_data"].assert_not_awaited()
    env["completion"].assert_not_awaited()


async def test_intake_crash_is_500_with_log_entry(
    env, intake, mocker, redis_mock, caplog
):
    """An intake failure ends the child E/500, as the upstream view's
    generic handler did; the 'Internal ARS Server Error' entry goes to the
    ARS query's logs, not into the child's payload."""
    env["notify"].side_effect = RuntimeError("boom")
    with caplog.at_level(logging.ERROR):
        await pm.ars_premerge(_intake_task(env), LOGGER)
    final = _final_status_update(env)
    assert final["status"] == "E"
    assert final["code"] == 500
    assert "Internal ARS Server Error" in caplog.text
    env["save_message_data"].assert_not_awaited()
    env["completion"].assert_awaited_once()
    assert await _merge_tasks() == []


async def test_intake_premerge_crash_still_notifies_and_counts_first(
    env, intake, mocker, redis_mock
):
    """Premerge now runs in the intake's pool call, but a failure in it
    keeps the shape it had as a pool call of its own: the
    ara_response_complete notification and the counts first, then E/500.
    Nothing is saved for the merge."""
    mocker.patch.object(pm, "pre_merge_process", side_effect=RuntimeError("boom"))
    await pm.ars_premerge(_intake_task(env), LOGGER)

    notified = env["notify"].await_args_list[0].args[1]
    assert notified["event_type"] == "ara_response_complete"
    assert notified["ara_n_results"] == 2
    kwargs = [c.kwargs for c in env["update_message"].await_args_list]
    assert kwargs[0]["result_count"] == 2
    assert (kwargs[-1]["status"], kwargs[-1]["code"]) == ("E", 500)
    assert env["saved"] == []
    env["save_message_data"].assert_not_awaited()
    env["completion"].assert_awaited_once()
    assert await _merge_tasks() == []


# ---------------------------------------------------------------------------
# redelivery: premerging the same payload twice changes nothing
# ---------------------------------------------------------------------------


async def test_premerging_a_payload_twice_gives_the_same_payload(
    env, monkeypatch, redis_mock
):
    """A task redelivered after its worker died may premerge a payload that
    was already premerged (a task naming a child_pk works on the stored
    blob). Every stage is idempotent now that nothing is stamped on edges:
    the second pass saves exactly what the first did."""
    monkeypatch.setattr(settings, "ars_annotation_mode", "premerge")
    # the annotator answers for the curies it is asked about (the second
    # pass asks only for the one the first found nothing for)
    env["annotator"].annotate_curie_list.side_effect = lambda curies: {
        c: ANNOTATIONS[c] for c in curies if c in ANNOTATIONS
    }
    env["data"]["message"]["knowledge_graph"]["nodes"]["CHEBI:6801"]["is_set"] = None
    await pm.ars_premerge(_task(env), LOGGER)
    once = copy.deepcopy(env["saved"][-1][1])

    await pm.ars_premerge(_task(env), LOGGER)
    assert env["saved"][-1][1] == once
    assert _final_status_update(env)["status"] == "D"


async def test_a_redelivered_intake_saves_the_same_payload(env, intake, redis_mock):
    """A redelivered delivery premerges the ARA's response again, from
    scratch, and lands on the payload the first delivery saved."""
    pristine = copy.deepcopy(env["data"])
    await pm.ars_premerge(_intake_task(env), LOGGER)
    first = copy.deepcopy(env["saved"][-1][1])

    # the worker died after recording the counts, before the hand-off
    env["child_row"]["result_count"] = 2
    await ars_db.clear_ready_child(env["parent_pk"], env["child_pk"], LOGGER)
    intake["get_message"].return_value = pristine
    await pm.ars_premerge(_intake_task(env), LOGGER)
    assert len(env["saved"]) == 2
    assert env["saved"][-1][1] == first


# ---------------------------------------------------------------------------
# premerge reports (deviation 21): nulls stripped, infores missing, phantom
# support graphs removed -- each logged once on the query, from the pool child
# ---------------------------------------------------------------------------

ARA_INFORES = "infores:shepherd-aragorn"


def _name_the_ara_on_every_edge(data):
    for edge in data["message"]["knowledge_graph"]["edges"].values():
        if not any(s["resource_id"] == ARA_INFORES for s in edge["sources"]):
            edge["sources"].append(
                {
                    "resource_id": ARA_INFORES,
                    "resource_role": "aggregator_knowledge_source",
                }
            )


async def _query_logs(env, task, mocker):
    """Run ``task`` the way the poll loop does (a task logger with a query
    log handler, flushed by process_task) and return the ARS query's stored
    logs."""
    mocker.patch.object(pm, "mark_task_as_complete", new_callable=AsyncMock)
    _, task_logger = shared._build_task_context(pm.STREAM, "c1", task, logging.INFO)
    await pm.process_task(task, None, task_logger, MagicMock())
    return await get_logs(str(env["parent_pk"]), LOGGER)


def _reports(logs, needle):
    return [e for e in logs if needle in e["message"]]


@pytest.mark.parametrize("delivery", [True, False], ids=["intake", "child_task"])
async def test_a_clean_response_reports_nothing(
    env, intake, mocker, redis_mock, delivery
):
    _name_the_ara_on_every_edge(env["data"])
    task = _intake_task(env) if delivery else _task(env)
    logs = await _query_logs(env, task, mocker)

    assert _reports(logs, "ARA ara-shepherd-aragorn") == []
    assert not [e for e in logs if e["level"] in ("WARNING", "ERROR")]
    assert (_final_status_update(env)["status"]) == "D"


async def test_stripped_nulls_are_reported_as_a_warning(
    env, intake, mocker, redis_mock
):
    _name_the_ara_on_every_edge(env["data"])
    kg = env["data"]["message"]["knowledge_graph"]
    kg["nodes"]["CHEBI:6801"]["is_set"] = None
    kg["edges"]["e1"]["qualifiers"] = None
    env["data"]["message"]["results"][0]["analyses"][0]["scoring_method"] = None

    logs = await _query_logs(env, _intake_task(env), mocker)

    (entry,) = _reports(logs, "explicit null")
    assert entry["level"] == "WARNING"
    for part in ("ara-shepherd-aragorn", ARA_INFORES, str(env["child_pk"]), "3 "):
        assert part in entry["message"]
    # still stripped: the response validates and merges without them
    assert not _has_null(env["saved"][-1][1])
    assert _final_status_update(env)["status"] == "D"
    assert _reports(logs, "do not name") == []
    assert _reports(logs, "support graphs") == []


async def test_edges_without_the_ara_infores_are_reported_not_stamped(
    env, mocker, redis_mock
):
    """Upstream appended the ARA's infores to every edge whose sources
    lacked it. The port leaves the sources as the ARA sent them and warns,
    naming the edges. (The corpus edges name infores:aragorn, not the
    hosted ARA's infores:shepherd-aragorn.)"""
    sent = copy.deepcopy(env["data"]["message"]["knowledge_graph"]["edges"])
    logs = await _query_logs(env, _task(env), mocker)

    (entry,) = _reports(logs, "do not name")
    assert entry["level"] == "WARNING"
    for part in (
        "ara-shepherd-aragorn",
        ARA_INFORES,
        str(env["child_pk"]),
        "3 knowledge graph edge(s)",
        "e1, e2, e3",
    ):
        assert part in entry["message"]
    edges = env["saved"][-1][1]["message"]["knowledge_graph"]["edges"]
    assert {k: e["sources"] for k, e in edges.items()} == {
        k: e["sources"] for k, e in sent.items()
    }
    # still valid, still merged
    assert _final_status_update(env)["status"] == "D"
    assert await _ready(env) == [str(env["child_pk"])]


async def test_phantom_support_graphs_are_removed_and_reported_as_an_error(
    env, intake, mocker, redis_mock
):
    _name_the_ara_on_every_edge(env["data"])
    edges = env["data"]["message"]["knowledge_graph"]["edges"]
    edges["e3"]["attributes"] = [
        {"attribute_type_id": "biolink:support_graphs", "value": ["aux9"]}
    ]

    logs = await _query_logs(env, _intake_task(env), mocker)

    (entry,) = _reports(logs, "support graphs")
    assert entry["level"] == "ERROR"
    for part in (
        "ara-shepherd-aragorn",
        ARA_INFORES,
        str(env["child_pk"]),
        "1 edge(s)",
        "e3",
        "aux9",
    ):
        assert part in entry["message"]
    # removed, so the response still merges; the intact reference stays
    saved = env["saved"][-1][1]["message"]["knowledge_graph"]["edges"]
    assert saved["e3"]["attributes"] == []
    assert saved["e2"]["attributes"][0]["value"] == ["aux1"]
    assert _final_status_update(env)["status"] == "D"
    assert await _ready(env) == [str(env["child_pk"])]


def test_report_examples_are_capped():
    assert pm._examples(["a", "b"]) == "a, b"
    assert pm._examples([f"e{i}" for i in range(9)]) == "e0, e1, e2, e3, e4, ..."


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


# --- #252 stage 2: which children keep a durable copy ------------------------


@pytest.fixture
def cold_store(env, tmp_path, monkeypatch):
    """A cold store is configured."""
    monkeypatch.setattr(settings, "ars_payload_dir", str(tmp_path))


async def test_a_premerged_child_keeps_no_durable_copy(env, cold_store, redis_mock):
    """Its ARA's answer is in the archive finish_query wrote under the
    child's own id, and the child is served from that once Redis lets go."""
    await pm.ars_premerge(_task(env), LOGGER)

    assert _final_status_update(env)["status"] == "D"
    env["persist_data_copy"].assert_not_awaited()
    env["completion"].assert_awaited_once()


async def test_a_child_whose_response_was_lost_keeps_its_copy(
    env, intake, cold_store, redis_mock
):
    """Nothing reached the ARS, so there may be no archive to point at: the
    child's own payload (its query and the error) is all there is."""
    intake["get_message"].side_effect = KeyError("gone")

    await pm.ars_premerge(_intake_task(env), LOGGER)

    assert _final_status_update(env)["status"] == "E"
    env["persist_data_copy"].assert_awaited_once_with(env["child_pk"], LOGGER)

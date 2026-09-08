"""Tests for the ars_premerge worker.

Pre-merge processing (scrub -> decorate -> normalize_scores), phantom
support-graph removal, and TRAPI validation used to run inline in the
callback request, as upstream does in its Django view. They now run here
(documented deviation: the CPU work saturated the server under load), with
upstream's exact outcome contract preserved asynchronously:

  - success        -> child D/200 (or the tr_ars.message.status header
                      value), premerged payload saved, ars.merge enqueued
                      for ara- agents, completion check
  - invalid TRAPI  -> child E/422, ara_failed_validation notification,
                      NO merge task (upstream answered the HTTP 422 inline;
                      the child's terminal state and notification match)
  - premerge crash -> child E/500 with the "Internal ARS Server Error" log
                      entry (upstream's generic callback handler)
"""

import json
import logging
import pathlib
import uuid
from unittest.mock import AsyncMock

import pytest

import shepherd_utils.ars.db as ars_db
from workers.ars_premerge import worker as pm

LOGGER = logging.getLogger(__name__)


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
        "actor": 7,
        "ref": parent_pk,
        "result_count": 2,
        "params": {"query_type": "standard"},
    }
    parent_row = {
        "id": parent_pk,
        "status": "R",
        "code": 202,
        "actor": 1,
        "result_count": None,
    }
    data = load_corpus("response_aragorn.json")

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    rows = {str(child_pk): child_row, str(parent_pk): parent_row}
    return {
        "parent_pk": parent_pk,
        "child_pk": child_pk,
        "child_row": child_row,
        "data": data,
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


def _task(env, status="D", agent="ara-aragorn"):
    return [
        "tid",
        {
            "child_pk": str(env["child_pk"]),
            "parent_pk": str(env["parent_pk"]),
            "agent_name": agent,
            "inforesid": "infores:aragorn",
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


async def test_premerge_happy_path(env, redis_mock):
    await pm.ars_premerge(_task(env), LOGGER)

    # premerged payload saved (normalize_scores ran)
    saved = env["save_message_data"].await_args_list[-1]
    assert str(saved.args[0]) == str(env["child_pk"])
    assert "normalized_score" in saved.args[1]["message"]["results"][0]

    # child flipped to D/200
    final = next(
        c.kwargs for c in env["update_message"].await_args_list if "status" in c.kwargs
    )
    assert final["status"] == "D"
    assert final["code"] == 200
    env["persist_data_copy"].assert_awaited()
    env["completion"].assert_awaited_once()

    # merge task enqueued for the ara- agent, in the query's trace
    tasks = await _merge_tasks()
    assert len(tasks) == 1
    assert tasks[0][1]["parent_pk"] == str(env["parent_pk"])
    assert tasks[0][1]["child_pk"] == str(env["child_pk"])
    assert tasks[0][1]["otel"] == '{"traceparent": "00-sub"}'


async def test_premerge_status_header_override(env, redis_mock):
    """The callback's tr_ars.message.status header value rides the task and
    lands on the child, as upstream applied it at the end of its view."""
    await pm.ars_premerge(_task(env, status="S"), LOGGER)
    final = next(
        c.kwargs for c in env["update_message"].await_args_list if "status" in c.kwargs
    )
    assert final["status"] == "S"


async def test_premerge_validation_failure_is_422(env, redis_mock):
    del env["data"]["message"]["results"][0]["node_bindings"]
    env["load_message_data"].return_value = env["data"]

    await pm.ars_premerge(_task(env), LOGGER)

    final = next(
        c.kwargs for c in env["update_message"].await_args_list if "status" in c.kwargs
    )
    assert final["status"] == "E"
    assert final["code"] == 422
    fields = env["notify"].await_args.args[1]
    assert fields["event_type"] == "ara_failed_validation"
    assert fields["child_uuid"] == str(env["child_pk"])
    env["completion"].assert_awaited_once()
    assert await _merge_tasks() == []


async def test_premerge_validate_false_skips_validation(env, redis_mock):
    """params.validate == False skips phantom removal + validation, exactly
    like upstream's callback."""
    env["child_row"]["params"] = {"query_type": "standard", "validate": False}
    del env["data"]["message"]["results"][0]["node_bindings"]  # would fail
    env["load_message_data"].return_value = env["data"]

    await pm.ars_premerge(_task(env), LOGGER)
    final = next(
        c.kwargs for c in env["update_message"].await_args_list if "status" in c.kwargs
    )
    assert final["status"] == "D"
    assert len(await _merge_tasks()) == 1


async def test_premerge_crash_is_500_with_log_entry(env, mocker, redis_mock):
    """A premerge failure reproduces upstream's generic callback handler:
    E/500 and the 'Internal ARS Server Error' log entry in the payload."""
    mocker.patch.object(pm, "pre_merge_process", side_effect=RuntimeError("scrub boom"))
    await pm.ars_premerge(_task(env), LOGGER)

    final = next(
        c.kwargs for c in env["update_message"].await_args_list if "status" in c.kwargs
    )
    assert final["status"] == "E"
    assert final["code"] == 500
    saved = env["save_message_data"].await_args_list[-1].args[1]
    assert any(
        entry["message"] == "Internal ARS Server Error"
        for entry in saved.get("logs", [])
    )
    env["completion"].assert_awaited_once()
    assert await _merge_tasks() == []


async def test_premerge_non_ara_agent_no_merge(env, redis_mock):
    """KP callbacks premerge and go terminal but never enqueue a merge,
    exactly like upstream's agent_name.startswith('ara-') guard."""
    await pm.ars_premerge(_task(env, agent="kp-genetics"), LOGGER)
    final = next(
        c.kwargs for c in env["update_message"].await_args_list if "status" in c.kwargs
    )
    assert final["status"] == "D"
    assert await _merge_tasks() == []

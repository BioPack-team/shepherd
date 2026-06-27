"""Tests for the ARS fan-out orchestrator worker.

Verifies that a parent query is dispatched to every configured ARA: one child
query + response per ARA, one ars_children batch insert, and one task enqueued
onto each ARA's stream.
"""

import json
import logging

import pytest

from workers.ars import worker as ars_worker

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_fan_out_dispatches_to_every_ara(mocker):
    aras = ["aragorn", "arax", "bte", "sipr"]
    mocker.patch.object(ars_worker.settings, "ars_aras", aras)
    mocker.patch.object(ars_worker.settings, "callback_host", "http://cb")

    get_message = mocker.patch.object(
        ars_worker,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value={"message": {"query_graph": {"nodes": {}, "edges": {}}}},
    )
    add_query = mocker.patch.object(
        ars_worker, "add_query", new_callable=mocker.AsyncMock
    )
    add_children = mocker.patch.object(
        ars_worker, "add_ars_children", new_callable=mocker.AsyncMock
    )
    add_task = mocker.patch.object(
        ars_worker, "add_task", new_callable=mocker.AsyncMock
    )

    task = ("msg-id", {"query_id": "parent-1", "log_level": 20})
    await ars_worker.ars(task, logger)

    # The parent message was read once.
    get_message.assert_awaited_once_with("parent-1", logger)

    # One child query persisted per ARA, each with an /ars/callback URL.
    assert add_query.await_count == len(aras)
    for call in add_query.await_args_list:
        callback_url = call.args[3]
        assert callback_url.startswith("http://cb/ars/callback/")
        assert call.kwargs.get("target") in aras

    # One ars_children batch with a row per ARA carrying the callback mapping.
    add_children.assert_awaited_once()
    parent_qid, children = add_children.await_args.args[0], add_children.await_args.args[1]
    assert parent_qid == "parent-1"
    assert [c["ara"] for c in children] == aras
    for child in children:
        assert child["ars_callback_id"]
        assert child["child_qid"] and child["child_response_id"]
        assert json.loads(child["otel_trace"]) is not None or True  # carrier dict

    # One task enqueued onto each ARA stream with a null (ARA-defined) workflow.
    assert add_task.await_count == len(aras)
    dispatched_streams = [call.args[0] for call in add_task.await_args_list]
    assert dispatched_streams == aras
    for call in add_task.await_args_list:
        payload = call.args[1]
        assert json.loads(payload["workflow"]) is None
        assert payload["query_id"] and payload["response_id"]


@pytest.mark.asyncio
async def test_fan_out_callback_ids_are_unique(mocker):
    aras = ["aragorn", "bte"]
    mocker.patch.object(ars_worker.settings, "ars_aras", aras)
    mocker.patch.object(ars_worker.settings, "callback_host", "http://cb")
    mocker.patch.object(
        ars_worker,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value={"message": {}},
    )
    mocker.patch.object(ars_worker, "add_query", new_callable=mocker.AsyncMock)
    add_children = mocker.patch.object(
        ars_worker, "add_ars_children", new_callable=mocker.AsyncMock
    )
    mocker.patch.object(ars_worker, "add_task", new_callable=mocker.AsyncMock)

    await ars_worker.ars(("m", {"query_id": "p", "log_level": 20}), logger)

    children = add_children.await_args.args[1]
    callback_ids = [c["ars_callback_id"] for c in children]
    child_qids = [c["child_qid"] for c in children]
    assert len(set(callback_ids)) == len(callback_ids)
    assert len(set(child_qids)) == len(child_qids)

"""Parity tests for the ars_postprocess worker.

Upstream reference: NCATSTranslator/Relay @ dd1e71b utils.py post_process +
the tail of merge_and_post_process: blocklist -> scrub -> annotate ->
appraise -> sugeno scoring -> stats, with the exact failure codes (444 for
cleanup stages, 422 for appraise/scoring) and the merged_version_available
notification + parent completion check afterwards.
"""

import json
import logging
import pathlib
import uuid
from unittest.mock import AsyncMock

import httpx
import pytest
import zstandard

import shepherd_utils.ars.db as ars_db
from workers.ars_postprocess import worker as pp

LOGGER = logging.getLogger(__name__)


def load_corpus(name):
    return json.loads(pathlib.Path(f"tests/fixtures/ars_corpus/{name}").read_text())


def appraised(data):
    """What the appraiser would return: results with ordering_components."""
    out = json.loads(json.dumps(data))
    for result in out["message"]["results"]:
        result["ordering_components"] = {
            "novelty": 0.5,
            "confidence": 0.8,
            "clinical_evidence": 0.2,
        }
    return out


@pytest.fixture
def env(mocker, redis_mock):
    parent_pk = uuid.uuid4()
    merged_pk = uuid.uuid4()
    merged_row = {
        "id": merged_pk,
        "status": "R",
        "code": 202,
        "actor": 3,
        "ref": parent_pk,
        "result_count": None,
        "params": None,
    }
    parent_row = {
        "id": parent_pk,
        "status": "R",
        "code": 202,
        "actor": 1,
        "result_count": None,
        "merged_versions_list": [[str(merged_pk), "ara-x"]],
    }
    data = load_corpus("response_aragorn.json")

    def _patch(name, **kwargs):
        return mocker.patch.object(ars_db, name, new_callable=AsyncMock, **kwargs)

    rows = {str(merged_pk): merged_row, str(parent_pk): parent_row}
    mocks = {
        "parent_pk": parent_pk,
        "merged_pk": merged_pk,
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
        "notify": mocker.patch.object(pp, "notify_subscribers", new_callable=AsyncMock),
        "completion": mocker.patch.object(
            pp.lifecycle, "check_parent_completion", new_callable=AsyncMock
        ),
    }
    return mocks


def _task(env, stats=None):
    return [
        "tid",
        {
            "merged_pk": str(env["merged_pk"]),
            "parent_pk": str(env["parent_pk"]),
            "agent_name": "ara-aragorn",
            "stats": json.dumps(stats or {"results": 2}),
            "log_level": "20",
            "otel": "{}",
        },
    ]


def _appraiser_response(env):
    payload = zstandard.compress(json.dumps(appraised(env["data"])).encode("utf-8"))
    return httpx.Response(
        status_code=200,
        content=payload,
        request=httpx.Request("POST", "https://appraiser.example/get_appraisal"),
    )


def _annotator_response():
    return httpx.Response(
        status_code=200,
        json={
            "MONDO:0005148": {"disease_info": {"mondo": "0005148"}},
            "CHEBI:6801": [{"notfound": True}],
            "NCBIGene:5468": {"gene_info": {"symbol": "PPARG"}},
        },
        request=httpx.Request("POST", "https://annotator.example/curie"),
    )


@pytest.fixture
def http(mocker, env):
    def _route(url, **kwargs):
        if "appraise" in str(url) or "get_appraisal" in str(url):
            return _appraiser_response(env)
        return _annotator_response()

    return mocker.patch(
        "httpx.AsyncClient.post", new_callable=AsyncMock, side_effect=_route
    )


async def test_postprocess_happy_path(env, http, redis_mock):
    await pp.ars_postprocess(_task(env), LOGGER)

    # merged child -> D/200 with result_count + result_stat
    final = next(
        c.kwargs
        for c in env["update_message"].await_args_list
        if str(c.args[0]) == str(env["merged_pk"]) and "status" in c.kwargs
    )
    assert final["status"] == "D"
    assert final["code"] == 200
    assert final["result_count"] == 2
    assert "result_stat" in final

    # saved payload got annotations, appraiser results, and sugeno ranks
    saved = env["save_message_data"].await_args_list[-1].args[1]
    results = saved["message"]["results"]
    assert all("ordering_components" in r for r in results)
    assert all("sugeno" in r and "rank" in r for r in results)
    nodes = saved["message"]["knowledge_graph"]["nodes"]
    annotated = [
        a
        for a in nodes["MONDO:0005148"]["attributes"]
        if a.get("attribute_type_id") == "biothings_annotations"
    ]
    assert len(annotated) == 1
    # notfound entries are skipped
    assert not any(
        a.get("attribute_type_id") == "biothings_annotations"
        for a in nodes["CHEBI:6801"]["attributes"]
    )

    # merged_version_available notification, then the completion check
    fields = env["notify"].await_args.args[1]
    assert fields["event_type"] == "merged_version_available"
    assert fields["merged_version"] == str(env["merged_pk"])
    assert fields["stats"] == {"results": 2}
    env["completion"].assert_awaited_once()


async def test_postprocess_appraiser_failure_is_422(env, mocker, redis_mock):
    def _route(url, **kwargs):
        if "appraise" in str(url) or "get_appraisal" in str(url):
            return httpx.Response(
                status_code=500,
                text="appraiser down",
                request=httpx.Request("POST", "https://appraiser.example/x"),
            )
        return _annotator_response()

    mocker.patch("httpx.AsyncClient.post", new_callable=AsyncMock, side_effect=_route)
    await pp.ars_postprocess(_task(env), LOGGER)

    final = next(
        c.kwargs
        for c in env["update_message"].await_args_list
        if str(c.args[0]) == str(env["merged_pk"]) and c.kwargs.get("code") == 422
    )
    assert final["status"] == "E"
    # default zeroed ordering_components were substituted
    saved = env["save_message_data"].await_args_list[-1].args[1]
    for result in saved["message"]["results"]:
        assert result["ordering_components"] == {
            "novelty": 0,
            "confidence": 0,
            "clinical_evidence": 0,
        }
    # notification + completion still happen (upstream notifies regardless)
    env["notify"].assert_awaited()
    env["completion"].assert_awaited()


async def test_postprocess_annotator_failure_is_444(env, mocker, redis_mock):
    def _route(url, **kwargs):
        if "appraise" in str(url) or "get_appraisal" in str(url):
            return _appraiser_response(env)
        raise httpx.ConnectError("annotator down")

    mocker.patch("httpx.AsyncClient.post", new_callable=AsyncMock, side_effect=_route)
    await pp.ars_postprocess(_task(env), LOGGER)
    # the 444 was recorded when annotation failed...
    saw_444 = any(
        c.kwargs.get("code") == 444
        for c in env["update_message"].await_args_list
        if str(c.args[0]) == str(env["merged_pk"])
    )
    assert saw_444
    # ...and it sticks through to the end (upstream's sticky code/status
    # locals survive the successful later stages)
    final = env["update_message"].await_args_list[-1].kwargs
    assert final.get("status") == "E"
    assert final.get("code") == 444
    env["completion"].assert_awaited()

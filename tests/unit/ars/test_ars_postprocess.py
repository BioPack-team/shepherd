"""Parity tests for the ars_postprocess worker.

Upstream reference: NCATSTranslator/Relay @ 3e65975 utils.py post_process +
the tail of merge_and_post_process: blocklist -> scrub -> annotate ->
appraise_confidence -> stats, with the exact failure codes (444 for the
cleanup stages and the stat calc; confidence failures are only logged) and
the merged_version_available notification + parent completion check
afterwards. The external Appraiser call and the Sugeno scoring pass were
removed upstream (Relay PRs #884/#883).
"""

import json
import logging
import pathlib
import uuid
from unittest.mock import AsyncMock

import pytest

import shepherd_utils.ars.db as ars_db
from workers.ars_postprocess import worker as pp

LOGGER = logging.getLogger(__name__)


def load_corpus(name):
    return json.loads(pathlib.Path(f"tests/fixtures/ars_corpus/{name}").read_text())


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


ANNOTATIONS = {
    "MONDO:0005148": {"disease_info": {"mondo": "0005148"}},
    "CHEBI:6801": [{"notfound": True}],
    "NCBIGene:5468": {"gene_info": {"symbol": "PPARG"}},
}


@pytest.fixture
def bt_annotator(mocker, env):
    """The in-process biothings_annotator package, mocked at the Annotator
    class (upstream uses the same package; no HTTP is involved)."""
    inst = mocker.MagicMock()
    inst.annotate_curie_list = AsyncMock(return_value=ANNOTATIONS)
    mocker.patch.object(pp.annotator, "Annotator", return_value=inst)
    # any HTTP during postprocess is a regression: the Appraiser and the
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


async def test_postprocess_happy_path(env, bt_annotator, redis_mock):
    expected_confidences = [
        _expected_confidence(r) for r in env["data"]["message"]["results"]
    ]

    await pp.ars_postprocess(_task(env), LOGGER)

    # the package was asked to annotate exactly the unannotated valid curies
    (curie_list,) = bt_annotator.annotate_curie_list.await_args.args
    assert sorted(curie_list) == ["CHEBI:6801", "MONDO:0005148", "NCBIGene:5468"]

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

    # ordering_components computed locally by appraise_confidence; no sugeno
    saved = env["save_message_data"].await_args_list[-1].args[1]
    results = saved["message"]["results"]
    for result, confidence in zip(results, expected_confidences):
        assert result["ordering_components"] == {
            "confidence": confidence,
            "clinical_evidence": 0.0,
            "novelty": 0.0,
        }
        assert "sugeno" not in result
        assert "rank" not in result
    nodes = saved["message"]["knowledge_graph"]["nodes"]
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

    # merged_version_available notification, then the completion check
    fields = env["notify"].await_args.args[1]
    assert fields["event_type"] == "merged_version_available"
    assert fields["merged_version"] == str(env["merged_pk"])
    assert fields["stats"] == {"results": 2}
    env["completion"].assert_awaited_once()


async def test_postprocess_confidence_failure_only_logged(
    env, bt_annotator, mocker, redis_mock
):
    """appraise_confidence failures are logged and swallowed: the message
    still completes D/200 (upstream post_process @ 3e65975)."""
    mocker.patch.object(
        pp, "appraise_confidence", side_effect=RuntimeError("confidence boom")
    )
    await pp.ars_postprocess(_task(env), LOGGER)

    final = next(
        c.kwargs
        for c in env["update_message"].await_args_list
        if str(c.args[0]) == str(env["merged_pk"]) and "status" in c.kwargs
    )
    assert final["status"] == "D"
    assert final["code"] == 200
    assert final["result_count"] == 2
    env["notify"].assert_awaited()
    env["completion"].assert_awaited()


async def test_postprocess_stat_calc_failure_is_444(
    env, bt_annotator, mocker, redis_mock
):
    """A ScoreStatCalc/result-count failure marks the merged child E/444 and
    skips the 202->200 flip, but the data (with its error log entries),
    notification, and completion check still go out."""
    mocker.patch.object(pp, "ScoreStatCalc", side_effect=RuntimeError("stat boom"))
    await pp.ars_postprocess(_task(env), LOGGER)

    final = next(
        c.kwargs
        for c in env["update_message"].await_args_list
        if str(c.args[0]) == str(env["merged_pk"]) and "status" in c.kwargs
    )
    assert final["status"] == "E"
    assert final["code"] == 444
    # len(results) was assigned before the stat calc raised, as upstream
    assert final["result_count"] == 2
    saved = env["save_message_data"].await_args_list[-1].args[1]
    assert any(
        entry["message"] == "Error in score stat calculation"
        for entry in saved.get("logs", [])
    )
    env["notify"].assert_awaited()
    env["completion"].assert_awaited()


async def test_postprocess_empty_results_still_completes(env, bt_annotator, redis_mock):
    """No results: the count/stat/confidence block is skipped entirely and
    the 202 shell still flips to D/200."""
    env["data"]["message"]["results"] = []
    env["load_message_data"].return_value = env["data"]

    await pp.ars_postprocess(_task(env), LOGGER)

    final = next(
        c.kwargs
        for c in env["update_message"].await_args_list
        if str(c.args[0]) == str(env["merged_pk"]) and "status" in c.kwargs
    )
    assert final["status"] == "D"
    assert final["code"] == 200
    assert "result_count" not in final
    env["completion"].assert_awaited()


async def test_postprocess_annotator_failure_is_444(env, mocker, redis_mock):
    """A package-level annotation failure marks the merged child E/444 and
    the 444 sticks through the successful later stages, as upstream."""
    inst = mocker.MagicMock()
    inst.annotate_curie_list = AsyncMock(side_effect=RuntimeError("annotator down"))
    mocker.patch.object(pp.annotator, "Annotator", return_value=inst)
    await pp.ars_postprocess(_task(env), LOGGER)

    saw_444 = any(
        c.kwargs.get("code") == 444
        for c in env["update_message"].await_args_list
        if str(c.args[0]) == str(env["merged_pk"])
    )
    assert saw_444
    final = env["update_message"].await_args_list[-1].kwargs
    assert final.get("status") == "E"
    assert final.get("code") == 444
    env["completion"].assert_awaited()

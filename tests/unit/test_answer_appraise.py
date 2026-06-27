"""Tests for the answer appraiser worker."""

import logging

import pytest

from workers.answer_appraise.worker import (
    DEFAULT_ORDERING_COMPONENTS,
    appraise,
    apply_defaults,
    normalize_scores,
)

logger = logging.getLogger(__name__)


def _results(scores):
    return [{"analyses": [{"score": s}]} for s in scores]


# --- normalize_scores -----------------------------------------------------


def test_normalize_scores_percentile_ranks():
    results = _results([0.1, 0.5, 0.9])
    normalize_scores(results)
    norms = [r["normalized_score"] for r in results]
    assert norms == [0.0, 50.0, 100.0]


def test_normalize_scores_single_result():
    results = _results([0.4])
    normalize_scores(results)
    assert results[0]["normalized_score"] == 100.0
    results = _results([0])
    normalize_scores(results)
    assert results[0]["normalized_score"] == 0.0


def test_normalize_scores_handles_ties():
    results = _results([0.5, 0.5, 0.9])
    normalize_scores(results)
    # rankdata average of the two ties is 1.5 -> (1.5-1)/2*100 = 25
    assert results[0]["normalized_score"] == 25.0
    assert results[1]["normalized_score"] == 25.0
    assert results[2]["normalized_score"] == 100.0


# --- apply_defaults / appraise --------------------------------------------


def test_apply_defaults_sets_missing_components():
    results = [{"ordering_components": {"novelty": 5}}, {}]
    apply_defaults(results)
    # Existing components untouched, missing ones filled.
    assert results[0]["ordering_components"] == {"novelty": 5}
    assert results[1]["ordering_components"] == DEFAULT_ORDERING_COMPONENTS


@pytest.mark.asyncio
async def test_appraise_merges_ordering_components(mocker):
    message = {"message": {"results": _results([0.1, 0.2])}}
    appraised = {
        "message": {
            "results": [
                {"ordering_components": {"novelty": 1, "confidence": 2}},
                {"ordering_components": {"novelty": 3, "confidence": 4}},
            ]
        }
    }
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(
            json=mocker.Mock(return_value=appraised), raise_for_status=mocker.Mock()
        ),
    )
    await appraise(message, logger)
    results = message["message"]["results"]
    assert results[0]["ordering_components"] == {"novelty": 1, "confidence": 2}
    assert results[1]["ordering_components"] == {"novelty": 3, "confidence": 4}


@pytest.mark.asyncio
async def test_appraise_defaults_on_failure(mocker):
    message = {"message": {"results": _results([0.1, 0.2])}}
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        side_effect=Exception("appraiser down"),
    )
    await appraise(message, logger)
    for result in message["message"]["results"]:
        assert result["ordering_components"] == DEFAULT_ORDERING_COMPONENTS


@pytest.mark.asyncio
async def test_appraise_noop_when_no_results(mocker):
    post = mocker.patch("httpx.AsyncClient.post", new_callable=mocker.AsyncMock)
    await appraise({"message": {"results": []}}, logger)
    post.assert_not_called()

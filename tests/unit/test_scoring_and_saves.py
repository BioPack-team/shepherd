"""Scoring and saving must not fail silently (#206).

The ARAGORN ranker scored answers in one loop, so one it couldn't handle left
every later answer unscored -- saved as if nothing had happened, then sorted
as score 0. And a response that couldn't be saved was logged and dropped,
letting the workflow carry on from the previous response.
"""

import logging
from unittest.mock import AsyncMock

import pytest

from shepherd_utils import db

logger = logging.getLogger(__name__)


def _ranker_message(include_bad=True):
    """Answers to a one-hop query; the first is missing a binding for n1."""
    source = [{"resource_id": "infores:a", "resource_role": "primary_knowledge_source"}]
    analysis = {"resource_id": "infores:t", "edge_bindings": {"e0": {"ids": ["k1"]}}}
    results = [
        {
            "node_bindings": {"n0": {"ids": ["X:A"]}, "n1": {"ids": ["X:B"]}},
            "analyses": [dict(analysis)],
        }
    ]
    if include_bad:
        results.insert(
            0,
            {"node_bindings": {"n0": {"ids": ["X:A"]}}, "analyses": [dict(analysis)]},
        )
    return {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": ["X:A"]}, "n1": {}},
                "edges": {"e0": {"subject": "n0", "object": "n1"}},
            },
            "knowledge_graph": {
                "nodes": {
                    "X:A": {"categories": ["biolink:NamedThing"]},
                    "X:B": {"categories": ["biolink:NamedThing"]},
                },
                "edges": {
                    "k1": {
                        "subject": "X:A",
                        "object": "X:B",
                        "predicate": "biolink:related_to",
                        "sources": source,
                    }
                },
            },
            "results": results,
        }
    }


def _score(monkeypatch, message):
    from workers.aragorn_score import worker as score

    saved = {}
    monkeypatch.setattr(score, "get_message_sync", lambda rid: message)
    monkeypatch.setattr(score, "save_response_sync", lambda rid, m: saved.update(m))
    score.aragorn_score("r1", logging.INFO)
    return saved["message"]["results"]


def test_one_unscorable_answer_does_not_stop_the_rest(monkeypatch):
    results = _score(monkeypatch, _ranker_message())

    good = [r for r in results if "n1" in r["node_bindings"]][0]
    bad = [r for r in results if "n1" not in r["node_bindings"]][0]
    assert good["analyses"][0]["score"] > 0
    assert bad["analyses"][0]["score"] == 0


def test_scores_are_unchanged_when_every_answer_scores(monkeypatch):
    """The same well-formed answer scores the same with or without a bad
    neighbour."""
    alone = _score(monkeypatch, _ranker_message(include_bad=False))
    mixed = _score(monkeypatch, _ranker_message())
    good = [r for r in mixed if "n1" in r["node_bindings"]][0]
    assert good["analyses"][0]["score"] == alone[0]["analyses"][0]["score"]


@pytest.fixture
def store_down(redis_mock, monkeypatch):
    async def _set(*args, **kwargs):
        raise ConnectionError("redis unavailable")

    monkeypatch.setattr(redis_mock["data"], "set", _set)
    monkeypatch.setattr(db.asyncio, "sleep", AsyncMock())


async def test_save_response_raises_when_the_store_rejects_it(store_down):
    with pytest.raises(ConnectionError):
        await db.save_response("r1", {"message": {}}, logger)


async def test_save_response_can_still_be_best_effort(store_down):
    await db.save_response("r1", {"message": {}}, logger, raise_on_failure=False)


async def test_the_too_large_tombstone_stays_best_effort(store_down, monkeypatch):
    """Failing a query as too large must not itself fail on a store blip: the
    marker is what later stages check, and finish_query delivers the
    response returned here."""
    from shepherd_utils import response_limit

    monkeypatch.setattr(
        db, "get_message", AsyncMock(return_value={"message": {"query_graph": {}}})
    )
    response = await response_limit.write_too_large_response(
        "q1", "r1", "too big", logger
    )
    assert response["status"] == response_limit.TOO_LARGE_RESPONSE_STATUS

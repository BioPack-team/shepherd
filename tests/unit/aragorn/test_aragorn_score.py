import copy
import json
import logging

import pytest

from shepherd_utils.db import get_message
from tests.helpers.generate_messages import response_1
from workers.aragorn_score.worker import aragorn_score


@pytest.mark.asyncio
async def test_aragorn_ranker(redis_mock, mocker):
    """Test that Aragorn Ranker returns the correct score."""
    mock_callback_response = mocker.patch(
        "workers.aragorn_score.worker.get_message_sync"
    )
    mock_callback_response.return_value = copy.deepcopy(response_1)
    logger = logging.getLogger(__name__)

    message = aragorn_score(
        copy.deepcopy(response_1),
        logger,
    )

    assert len(message["message"]["results"]) == 2
    assert "score" in message["message"]["results"][0]["analyses"][0]
    assert len(message["message"]["results"][0]["analyses"]) == 1
    assert isinstance(message["message"]["results"][0]["analyses"][0]["score"], float)
    assert message["message"]["results"][0]["analyses"][0]["score"] > 0.063
    assert message["message"]["results"][0]["analyses"][0]["score"] < 0.064


def test_run_score_from_db_loads_scores_and_saves(mocker):
    """The process-pool entrypoint reads by id, scores, and writes back.

    Only the ``response_id`` crosses into the worker: it loads via
    ``get_message_sync``, scores, and persists with ``save_message_sync`` -- the
    large payload never has to be passed in or returned across the boundary.
    """
    from workers.aragorn_score.worker import run_score_from_db

    mocker.patch(
        "workers.aragorn_score.worker.get_message_sync",
        return_value=copy.deepcopy(response_1),
    )
    save = mocker.patch("workers.aragorn_score.worker.save_message_sync")
    logger = logging.getLogger(__name__)

    run_score_from_db("resp-1", logger)

    save.assert_called_once()
    saved_id, saved_message = save.call_args.args
    assert saved_id == "resp-1"
    assert saved_message["message"]["results"][0]["analyses"][0]["score"] > 0.063


def test_run_score_from_db_saves_unchanged_when_no_results(mocker):
    """When there are no results to score, the message is saved unchanged.

    ``aragorn_score`` returns ``None`` in that case; the entrypoint must fall
    back to persisting the message it loaded rather than saving ``None``.
    """
    from workers.aragorn_score.worker import run_score_from_db

    no_results = {"message": {"results": None}}
    mocker.patch(
        "workers.aragorn_score.worker.get_message_sync",
        return_value=copy.deepcopy(no_results),
    )
    save = mocker.patch("workers.aragorn_score.worker.save_message_sync")
    logger = logging.getLogger(__name__)

    run_score_from_db("resp-2", logger)

    # The loaded message is saved back (not None); scoring was a no-op because
    # there were no results. (aragorn_score also initializes an empty logs list.)
    save.assert_called_once()
    saved_id, saved_message = save.call_args.args
    assert saved_id == "resp-2"
    assert saved_message is not None
    assert saved_message["message"]["results"] is None

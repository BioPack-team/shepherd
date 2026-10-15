import copy
import logging

from tests.helpers.generate_messages import response_1
from workers.aragorn_score.worker import aragorn_score


def test_aragorn_ranker_loads_scores_and_saves(mocker):
    """The process-pool entrypoint reads by id, scores, and writes back.

    Only the ``response_id`` crosses into the worker: ``aragorn_score`` loads the
    message via ``get_message_sync``, scores it, and persists it with
    ``save_message_sync`` -- the large payload never has to be passed in or
    returned across the process boundary.
    """
    mocker.patch(
        "workers.aragorn_score.worker.get_message_sync",
        return_value=copy.deepcopy(response_1),
    )
    save = mocker.patch("workers.aragorn_score.worker.save_message_sync")
    logger = logging.getLogger(__name__)

    aragorn_score("resp-1", logger)

    save.assert_called_once()
    saved_id, message = save.call_args.args
    assert saved_id == "resp-1"
    assert len(message["message"]["results"]) == 2
    assert "score" in message["message"]["results"][0]["analyses"][0]
    assert len(message["message"]["results"][0]["analyses"]) == 1
    assert isinstance(message["message"]["results"][0]["analyses"][0]["score"], float)
    assert message["message"]["results"][0]["analyses"][0]["score"] > 0.063
    assert message["message"]["results"][0]["analyses"][0]["score"] < 0.064


def test_aragorn_score_saves_unchanged_when_no_results(mocker):
    """When there are no results to score, the message is saved back unchanged.

    Scoring is a no-op in that case; the entrypoint must still persist the
    message it loaded rather than dropping it.
    """
    no_results = {"message": {"results": None}}
    mocker.patch(
        "workers.aragorn_score.worker.get_message_sync",
        return_value=copy.deepcopy(no_results),
    )
    save = mocker.patch("workers.aragorn_score.worker.save_message_sync")
    logger = logging.getLogger(__name__)

    aragorn_score("resp-2", logger)

    # The loaded message is saved back (not None); scoring was skipped because
    # there were no results. (aragorn_score also initializes an empty logs list.)
    save.assert_called_once()
    saved_id, saved_message = save.call_args.args
    assert saved_id == "resp-2"
    assert saved_message is not None
    assert saved_message["message"]["results"] is None

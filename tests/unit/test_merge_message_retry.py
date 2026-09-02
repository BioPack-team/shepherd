"""Regression guards on the merge_message failure path.

A merge that raises used to re-enqueue its wake task immediately and
unconditionally. That is right for a transient failure, but a batch that fails
*deterministically* -- e.g. a callback node missing an optional TRAPI field,
which raised ``KeyError: 'name'`` out of ``merge_kgraph`` -- fails again on
every retry. Observed in production as one query logging the same traceback
dozens of times a second for as long as it lived, never finishing.

So the failure path now backs off between attempts and, after
``merge_max_attempts`` consecutive failures, discards the batch it can't merge
so the query gets on with the rest.
"""

import logging

import pytest

from shepherd_utils.config import settings
from workers.merge_message.worker import (
    MERGE_ATTEMPT_FIELD,
    STREAM,
    _handle_merge_failure,
)

logger = logging.getLogger(__name__)


def _task(attempt=None):
    fields = {
        "query_id": "q1",
        "response_id": "rid",
        "callback_id": "cb1",
        "target": "aragorn",
        "_started_at": "123",
    }
    if attempt is not None:
        fields[MERGE_ATTEMPT_FIELD] = str(attempt)
    return ("1-1", fields)


@pytest.fixture(autouse=True)
def _no_sleep(mocker):
    """The backoff itself is asserted on; don't actually wait it out."""
    return mocker.patch(
        "workers.merge_message.worker.asyncio.sleep", new=mocker.AsyncMock()
    )


async def test_first_failure_backs_off_and_carries_the_attempt_count(mocker):
    add_task = mocker.patch(
        "workers.merge_message.worker.add_task", new=mocker.AsyncMock()
    )
    sleep = mocker.patch(
        "workers.merge_message.worker.asyncio.sleep", new=mocker.AsyncMock()
    )

    await _handle_merge_failure(_task(), "rid", ["cb1"], logger)

    sleep.assert_awaited_once_with(settings.merge_retry_backoff)
    stream, fields, _ = add_task.await_args.args
    assert stream == STREAM
    assert fields[MERGE_ATTEMPT_FIELD] == "1"
    # The re-enqueued task is otherwise the one we got, minus the bookkeeping
    # field the broker adds.
    assert "_started_at" not in fields
    assert fields["callback_id"] == "cb1"


async def test_backoff_grows_and_is_capped(mocker):
    """Successive failures wait longer, up to merge_retry_backoff_max."""
    mocker.patch("workers.merge_message.worker.add_task", new=mocker.AsyncMock())
    sleep = mocker.patch(
        "workers.merge_message.worker.asyncio.sleep", new=mocker.AsyncMock()
    )
    mocker.patch.object(settings, "merge_max_attempts", 0)  # breaker off

    waits = []
    for attempt in (1, 2, 3, 20):
        sleep.reset_mock()
        await _handle_merge_failure(_task(attempt), "rid", ["cb1"], logger)
        waits.append(sleep.await_args.args[0])

    assert waits[0] < waits[1] < waits[2]
    assert waits[-1] == settings.merge_retry_backoff_max
    assert all(w <= settings.merge_retry_backoff_max for w in waits)


async def test_batch_is_discarded_after_max_attempts(mocker):
    add_task = mocker.patch(
        "workers.merge_message.worker.add_task", new=mocker.AsyncMock()
    )
    clear = mocker.patch(
        "workers.merge_message.worker.clear_ready_callback", new=mocker.AsyncMock()
    )
    remove = mocker.patch(
        "workers.merge_message.worker.remove_callback_id", new=mocker.AsyncMock()
    )
    sleep = mocker.patch(
        "workers.merge_message.worker.asyncio.sleep", new=mocker.AsyncMock()
    )

    last = settings.merge_max_attempts - 1
    await _handle_merge_failure(_task(last), "rid", ["cb1", "cb2"], logger)

    # The unmergeable callbacks are dropped from the ready index and the
    # callbacks table, so the next drain pass can't pick them up again.
    assert {c.args[1] for c in clear.await_args_list} == {"cb1", "cb2"}
    assert {c.args[0] for c in remove.await_args_list} == {"cb1", "cb2"}
    # No point sleeping: this batch isn't being retried.
    sleep.assert_not_awaited()
    # A wake task still goes back so anything else ready still gets merged --
    # with the counter reset, since the poison batch is gone.
    _, fields, _ = add_task.await_args.args
    assert MERGE_ATTEMPT_FIELD not in fields


async def test_breaker_does_not_fire_with_nothing_to_discard(mocker):
    """A failure before any batch was read (get_ready_callbacks itself raising)
    has nothing to drop, so it just retries."""
    add_task = mocker.patch(
        "workers.merge_message.worker.add_task", new=mocker.AsyncMock()
    )
    clear = mocker.patch(
        "workers.merge_message.worker.clear_ready_callback", new=mocker.AsyncMock()
    )

    await _handle_merge_failure(
        _task(settings.merge_max_attempts + 5), "rid", [], logger
    )

    clear.assert_not_awaited()
    _, fields, _ = add_task.await_args.args
    assert fields[MERGE_ATTEMPT_FIELD] == str(settings.merge_max_attempts + 6)


async def test_garbled_attempt_field_does_not_break_the_retry(mocker):
    add_task = mocker.patch(
        "workers.merge_message.worker.add_task", new=mocker.AsyncMock()
    )
    task = _task()
    task[1][MERGE_ATTEMPT_FIELD] = "not-a-number"

    await _handle_merge_failure(task, "rid", ["cb1"], logger)

    _, fields, _ = add_task.await_args.args
    assert fields[MERGE_ATTEMPT_FIELD] == "1"

"""Tests for the whole-query timeout budget.

Queries used to run to completion however long ago they were submitted, even
though the ARS and the sync endpoint stop waiting after ~5 minutes. These cover
the deadline stamped at intake, its propagation from operation to operation, and
the wrap-up a worker does instead of running an operation whose query has
outlived its budget.
"""

import asyncio
import logging
import time

import pytest

from shepherd_server import base_routes
from shepherd_utils import shared
from shepherd_utils.config import settings
from shepherd_utils.logger import attach_query_handler
from shepherd_utils.task_deadline import (
    DEADLINE_FIELD,
    TIMEOUT_STATUS,
    carry_deadline,
    deadline_field,
    get_deadline,
    is_expired,
    query_budget,
    query_deadline,
    seconds_overdue,
)

logger = logging.getLogger(__name__)


# --- budget / deadline arithmetic -------------------------------------------


def test_query_budget_defaults_to_configured_value(monkeypatch):
    monkeypatch.setattr(settings, "query_timeout_sec", 300.0)
    assert query_budget() == 300.0
    assert query_budget({}) == 300.0
    assert query_budget({"parameters": None}) == 300.0


def test_query_budget_honors_a_client_asking_to_wait_longer(monkeypatch):
    """A caller that explicitly waits longer than the fleet budget isn't cut
    short partway through its own timeout."""
    monkeypatch.setattr(settings, "query_timeout_sec", 300.0)
    assert query_budget({"parameters": {"timeout": 600}}) == 600.0


def test_query_budget_ignores_a_shorter_or_unusable_client_timeout(monkeypatch):
    monkeypatch.setattr(settings, "query_timeout_sec", 300.0)
    # aragorn's lookup timeout rides on the same parameter and is shorter than
    # the whole-query budget; it must not shrink it.
    assert query_budget({"parameters": {"timeout": 210}}) == 300.0
    assert query_budget({"parameters": {"timeout": "soon"}}) == 300.0
    assert query_budget({"parameters": {"timeout": None}}) == 300.0


def test_query_budget_disabled_returns_zero(monkeypatch):
    monkeypatch.setattr(settings, "query_timeout_sec", 0.0)
    # Even a client-supplied timeout can't re-enable a disabled budget.
    assert query_budget({"parameters": {"timeout": 600}}) == 0.0


def test_query_deadline_is_start_plus_budget(monkeypatch):
    monkeypatch.setattr(settings, "query_timeout_sec", 300.0)
    assert query_deadline({}, start=1000.0) == 1300.0


def test_query_deadline_is_none_when_disabled(monkeypatch):
    monkeypatch.setattr(settings, "query_timeout_sec", 0.0)
    assert query_deadline({}, start=1000.0) is None


# --- payload field round trip -----------------------------------------------


def test_deadline_field_round_trips_through_a_payload():
    fields = deadline_field(1234.5678)
    # Redis stream values are strings; millisecond precision is plenty.
    assert fields == {DEADLINE_FIELD: "1234.568"}
    assert get_deadline(fields) == pytest.approx(1234.568)


def test_deadline_field_is_empty_without_a_deadline():
    assert deadline_field(None) == {}
    assert carry_deadline({"query_id": "q1"}) == {}


def test_carry_deadline_propagates_an_existing_deadline():
    assert carry_deadline({DEADLINE_FIELD: "999.5"}) == {DEADLINE_FIELD: "999.500"}


def test_get_deadline_tolerates_junk():
    assert get_deadline({DEADLINE_FIELD: ""}) is None
    assert get_deadline({DEADLINE_FIELD: "never"}) is None
    assert get_deadline(None) is None
    assert get_deadline("not-a-mapping") is None


def test_overdue_only_counts_time_past_the_deadline():
    now = 1000.0
    assert seconds_overdue({DEADLINE_FIELD: "1100"}, now=now) == 0.0
    assert seconds_overdue({DEADLINE_FIELD: "1000"}, now=now) == 0.0
    assert seconds_overdue({DEADLINE_FIELD: "940"}, now=now) == 60.0


def test_a_task_without_a_deadline_never_expires():
    """Fail open: payloads from an older server keep running as before."""
    assert is_expired({"query_id": "q1"}) is False
    assert is_expired({DEADLINE_FIELD: "junk"}) is False
    assert is_expired({DEADLINE_FIELD: str(time.time() + 60)}) is False
    assert is_expired({DEADLINE_FIELD: str(time.time() - 1)}) is True


# --- the wrap-up an expired task triggers -----------------------------------


def _expired_task(msg_id="42-0"):
    return (
        msg_id,
        {
            "query_id": "q1",
            "response_id": "r1",
            "workflow": '[{"id": "aragorn.score"}]',
            "log_level": 20,
            "otel": "{}",
            "metadata": "{}",
            DEADLINE_FIELD: f"{time.time() - 30:.3f}",
        },
    )


@pytest.fixture
def wrap_up_spy(monkeypatch):
    """Capture the broker/db calls the expiry path makes, in order."""
    calls = {"added": [], "acked": [], "saved_logs": [], "order": []}

    async def _fake_add_task(queue, payload, _logger):
        calls["added"].append((queue, payload))
        calls["order"].append("add_task")

    async def _fake_mark_complete(stream, group, msg_id, _logger, retries=0):
        calls["acked"].append((stream, group, msg_id))
        calls["order"].append("ack")

    async def _fake_save_logs(response_id, _logger):
        calls["saved_logs"].append(response_id)
        calls["order"].append("save_logs")

    monkeypatch.setattr(shared, "add_task", _fake_add_task)
    monkeypatch.setattr(shared, "mark_task_as_complete", _fake_mark_complete)
    monkeypatch.setattr(shared, "save_logs", _fake_save_logs)
    return calls


@pytest.mark.asyncio
async def test_expire_task_finishes_the_query_instead_of_running_it(wrap_up_spy):
    """The operation is skipped, but the query still ends the ordinary way:
    routed to finish_query (which settles Postgres, reaps callbacks and delivers
    what was gathered) and its message cleared from the stream."""
    task = _expired_task()
    await shared._expire_task("aragorn.score", "consumer", task, logger, 30.0)

    assert wrap_up_spy["acked"] == [("aragorn.score", "consumer", "42-0")]
    assert len(wrap_up_spy["added"]) == 1
    queue, payload = wrap_up_spy["added"][0]
    assert queue == "finish_query"
    assert payload["status"] == TIMEOUT_STATUS
    assert payload["query_id"] == "q1"
    assert payload["response_id"] == "r1"
    # No further operations: the remaining workflow is dropped.
    assert payload["workflow"] == "[]"
    # The deadline rides along so finish_query's own bookkeeping sees it.
    assert payload[DEADLINE_FIELD] == task[1][DEADLINE_FIELD]


@pytest.mark.asyncio
async def test_expire_task_saves_its_explanation_before_handing_off(wrap_up_spy):
    """finish_query reads the query's logs into the response it delivers, and
    can pick the task up immediately -- so the timeout note has to be persisted
    before the hand-off, not after."""
    task_logger = logging.getLogger("shepherd.test.expiry.q1")
    attach_query_handler(task_logger)

    await shared._expire_task(
        "aragorn.score", "consumer", _expired_task(), task_logger, 30.0
    )

    # Flushed before the hand-off, then again to clear what the hand-off itself
    # logged (nothing else runs for this query to drain it later).
    assert wrap_up_spy["saved_logs"] == ["r1", "r1"]
    assert wrap_up_spy["order"] == ["save_logs", "add_task", "ack", "save_logs"]


@pytest.mark.asyncio
async def test_expire_task_still_wraps_up_when_logs_cannot_be_saved(
    monkeypatch, wrap_up_spy
):
    """A Redis hiccup while flushing logs must not strand the query."""

    async def _boom(response_id, _logger):
        raise RuntimeError("redis down")

    monkeypatch.setattr(shared, "save_logs", _boom)
    await shared._expire_task("aragorn.score", "consumer", _expired_task(), logger, 1.0)

    assert wrap_up_spy["acked"] == [("aragorn.score", "consumer", "42-0")]
    assert wrap_up_spy["added"][0][0] == "finish_query"


# --- which tasks the check applies to ---------------------------------------


@pytest.mark.asyncio
async def test_handled_as_expired_skips_a_task_within_its_budget(wrap_up_spy):
    task = (
        "1-0",
        {"query_id": "q1", "response_id": "r1", DEADLINE_FIELD: str(time.time() + 60)},
    )
    assert (
        await shared._handled_as_expired("aragorn.score", "consumer", task, logger)
        is False
    )
    assert wrap_up_spy["added"] == []
    assert wrap_up_spy["acked"] == []


@pytest.mark.asyncio
async def test_handled_as_expired_skips_a_task_without_a_deadline(wrap_up_spy):
    task = ("1-0", {"query_id": "q1", "response_id": "r1"})
    assert (
        await shared._handled_as_expired("aragorn.score", "consumer", task, logger)
        is False
    )
    assert wrap_up_spy["added"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", ["finish_query", "merge_message"])
async def test_exempt_streams_are_never_expired(stream, wrap_up_spy):
    """finish_query IS the wrap-up -- expiring it would leave the query's state
    unset forever -- and merge_message folds callbacks upstream already paid for.
    """
    assert (
        await shared._handled_as_expired(stream, "consumer", _expired_task(), logger)
        is False
    )
    assert wrap_up_spy["added"] == []
    assert wrap_up_spy["acked"] == []


@pytest.mark.asyncio
async def test_handled_as_expired_wraps_up_an_overdue_task(wrap_up_spy):
    assert (
        await shared._handled_as_expired(
            "aragorn.score", "consumer", _expired_task(), logger
        )
        is True
    )
    assert wrap_up_spy["added"][0][1]["status"] == TIMEOUT_STATUS


# --- propagation from operation to operation --------------------------------


@pytest.mark.asyncio
async def test_wrap_up_task_passes_the_deadline_to_the_next_operation(wrap_up_spy):
    """The budget is measured from intake, so it must not restart each hop."""
    task = (
        "7-0",
        {
            "query_id": "q1",
            "response_id": "r1",
            "workflow": '[{"id": "aragorn.score"}, {"id": "sort_results_score"}]',
            "otel": "{}",
            "metadata": "{}",
            DEADLINE_FIELD: "1700.000",
        },
    )
    await shared.wrap_up_task("aragorn.score", "consumer", task, logger)

    queue, payload = wrap_up_spy["added"][0]
    assert queue == "sort_results_score"
    assert payload[DEADLINE_FIELD] == "1700.000"


@pytest.mark.asyncio
async def test_handle_task_failure_passes_the_deadline_along(wrap_up_spy):
    task = (
        "8-0",
        {
            "query_id": "q1",
            "response_id": "r1",
            "otel": "{}",
            "metadata": "{}",
            DEADLINE_FIELD: "1700.000",
        },
    )
    await shared.handle_task_failure("aragorn.score", "consumer", task, logger)

    queue, payload = wrap_up_spy["added"][0]
    assert queue == "finish_query"
    assert payload["status"] == "ERROR"
    assert payload[DEADLINE_FIELD] == "1700.000"


@pytest.mark.asyncio
async def test_terminate_task_still_defaults_to_an_error_status(wrap_up_spy):
    """The poison-pill/unprocessable paths keep their ERROR status."""
    await shared._terminate_task(
        "aragorn.score", "consumer", _expired_task(), logger, "poison pill"
    )
    assert wrap_up_spy["added"][0][1]["status"] == "ERROR"


# --- intake stamps the deadline ---------------------------------------------


@pytest.mark.asyncio
async def test_run_query_stamps_the_deadline_on_the_first_task(monkeypatch):
    monkeypatch.setattr(settings, "query_timeout_sec", 300.0)
    added = []

    async def _fake_add_query(*args, **kwargs):
        return None

    async def _fake_add_task(queue, payload, _logger):
        added.append((queue, payload))

    monkeypatch.setattr(base_routes, "add_query", _fake_add_query)
    monkeypatch.setattr(base_routes, "add_task", _fake_add_task)

    before = time.time()
    await base_routes.run_query("aragorn", {"message": {}})

    _, payload = added[0]
    assert get_deadline(payload) == pytest.approx(before + 300.0, abs=5)


@pytest.mark.asyncio
async def test_run_query_leaves_the_task_unstamped_when_disabled(monkeypatch):
    """With the budget off, tasks look exactly as they did before -- and so
    never expire."""
    monkeypatch.setattr(settings, "query_timeout_sec", 0.0)
    added = []

    async def _fake_add_query(*args, **kwargs):
        return None

    async def _fake_add_task(queue, payload, _logger):
        added.append((queue, payload))

    monkeypatch.setattr(base_routes, "add_query", _fake_add_query)
    monkeypatch.setattr(base_routes, "add_task", _fake_add_task)

    await base_routes.run_query("aragorn", {"message": {}})
    assert DEADLINE_FIELD not in added[0][1]


# --- get_tasks integration --------------------------------------------------


async def _async_noop(*args, **kwargs):
    return None


async def _no_reclaim(*args, **kwargs):
    return []


class _FakeHeartbeat:
    def __init__(self):
        self.marked = False
        self.stopped = False

    async def mark_clean_shutdown(self):
        self.marked = True

    async def stop(self):
        self.stopped = True


@pytest.fixture(autouse=True)
def _reset_shutdown_state():
    shared._shutdown = asyncio.Event()
    shared._signal_handlers_installed = False
    shared._active_heartbeat = None
    yield
    shared._shutdown = asyncio.Event()
    shared._signal_handlers_installed = False
    shared._active_heartbeat = None


def _stub_worker_startup(monkeypatch):
    fake_hb = _FakeHeartbeat()

    class _HBFactory:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            return fake_hb

    monkeypatch.setattr(shared, "initialize_db", _async_noop)
    monkeypatch.setattr(shared, "Heartbeat", _HBFactory)
    monkeypatch.setattr(shared, "install_shutdown_handlers", lambda hb=None: None)
    monkeypatch.setattr(settings, "broker_unhealthy_exit_sec", 0.0)
    shared._active_heartbeat = fake_hb
    return fake_hb


@pytest.mark.asyncio
async def test_get_tasks_does_not_hand_an_expired_task_to_the_worker(
    monkeypatch, wrap_up_spy
):
    """The whole point: a stale task is wrapped up rather than worked on."""
    _stub_worker_startup(monkeypatch)
    task = _expired_task("500-0")

    async def _fake_get_task(*args, **kwargs):
        # One delivery, then ask the loop to drain and exit.
        shared._request_shutdown()
        return task

    monkeypatch.setattr(shared, "get_task", _fake_get_task)
    monkeypatch.setattr(shared, "reclaim_orphaned", _no_reclaim)

    yielded = []
    with pytest.raises(SystemExit) as exc:
        async for item in shared.get_tasks("aragorn.score", "consumer", "cid", 8):
            yielded.append(item)

    assert exc.value.code == 0
    assert yielded == []
    assert wrap_up_spy["acked"] == [("aragorn.score", "consumer", "500-0")]
    assert wrap_up_spy["added"][0][0] == "finish_query"
    assert wrap_up_spy["added"][0][1]["status"] == TIMEOUT_STATUS


@pytest.mark.asyncio
async def test_get_tasks_still_hands_over_a_task_within_budget(
    monkeypatch, wrap_up_spy
):
    _stub_worker_startup(monkeypatch)
    task = (
        "501-0",
        {
            "query_id": "q1",
            "response_id": "r1",
            "workflow": "[]",
            "otel": "{}",
            "metadata": "{}",
            DEADLINE_FIELD: f"{time.time() + 120:.3f}",
        },
    )

    async def _fake_get_task(*args, **kwargs):
        shared._request_shutdown()
        return task

    monkeypatch.setattr(shared, "get_task", _fake_get_task)
    monkeypatch.setattr(shared, "reclaim_orphaned", _no_reclaim)

    yielded = []
    with pytest.raises(SystemExit):
        async for item in shared.get_tasks("aragorn.score", "consumer", "cid", 8):
            yielded.append(item)
            item[3].release()

    assert [t[0][0] for t in yielded] == ["501-0"]
    assert wrap_up_spy["added"] == []


@pytest.mark.asyncio
async def test_get_tasks_expires_a_reclaimed_task(monkeypatch, wrap_up_spy):
    """A message stranded in a dead consumer's PEL is the likeliest way a task
    outlives its query's budget."""
    _stub_worker_startup(monkeypatch)
    monkeypatch.setattr(settings, "max_task_deliveries", 0)  # breaker off
    stale = _expired_task("502-0")

    async def _fake_reclaim(stream, group, consumer, _logger, **kwargs):
        shared._request_shutdown()
        return [stale]

    monkeypatch.setattr(shared, "reclaim_orphaned", _fake_reclaim)
    monkeypatch.setattr(shared, "get_task", _async_noop)

    yielded = []
    with pytest.raises(SystemExit):
        async for item in shared.get_tasks("aragorn.score", "consumer", "cid", 8):
            yielded.append(item)

    assert yielded == []
    assert wrap_up_spy["acked"] == [("aragorn.score", "consumer", "502-0")]
    assert wrap_up_spy["added"][0][1]["status"] == TIMEOUT_STATUS


class _NonBlockingSlots(shared.TaskSlots):
    """TaskSlots that refuses to wait for a permit.

    A leaked permit makes ``get_tasks`` block forever on its next poll, which as
    a test failure mode is a hung suite. Turning the wait into an immediate
    error keeps the regression loud and fast.
    """

    async def acquire(self):
        assert (
            not self._sem.locked()
        ), "get_tasks blocked waiting for a permit that was never released"
        await super().acquire()


@pytest.mark.asyncio
async def test_expired_task_does_not_leak_a_concurrency_slot(monkeypatch, wrap_up_spy):
    """Expiring must free the slot the poll reserved, or a worker fed stale
    tasks would wedge with every permit held."""
    _stub_worker_startup(monkeypatch)
    monkeypatch.setattr(shared, "TaskSlots", _NonBlockingSlots)
    seen = []

    async def _fake_get_task(*args, **kwargs):
        seen.append(1)
        if len(seen) >= 3:
            shared._request_shutdown()
        return _expired_task(f"60{len(seen)}-0")

    monkeypatch.setattr(shared, "get_task", _fake_get_task)
    monkeypatch.setattr(shared, "reclaim_orphaned", _no_reclaim)

    # A task limit of 1: every poll after the first needs the previous task's
    # permit back.
    with pytest.raises(SystemExit):
        async for _ in shared.get_tasks("aragorn.score", "consumer", "cid", 1):
            pass

    assert len(seen) == 3
    assert len(wrap_up_spy["added"]) == 3

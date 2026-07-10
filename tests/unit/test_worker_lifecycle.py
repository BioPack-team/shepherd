"""Tests for the worker graceful-shutdown / drain machinery in
``shepherd_utils.shared`` and the clean-shutdown marker on
``shepherd_utils.heartbeat.Heartbeat``.

These cover the pieces added so that a SIGTERM (which Kubernetes sends on every
rollout, scale-down and node drain) stops the worker pulling new work, drains
in-flight tasks within a bounded window, writes a clean-shutdown marker, then
exits -- plus the ``TASK_LIMIT`` env override that lets ops tune concurrency
per Deployment.
"""

import asyncio
import logging

import pytest

from shepherd_utils import heartbeat as heartbeat_module
from shepherd_utils import shared
from shepherd_utils.heartbeat import Heartbeat, shutdown_key
from shepherd_utils.config import settings

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def _reset_shutdown_state():
    """Keep the module-level shutdown flag from leaking between tests."""
    shared._shutdown = asyncio.Event()
    shared._signal_handlers_installed = False
    shared._active_heartbeat = None
    yield
    shared._shutdown = asyncio.Event()
    shared._signal_handlers_installed = False
    shared._active_heartbeat = None


# --- TASK_LIMIT env override ------------------------------------------------


def test_resolve_task_limit_uses_default_without_env(monkeypatch):
    monkeypatch.delenv("TASK_LIMIT", raising=False)
    assert shared._resolve_task_limit("finish_query", 100, logger) == 100


def test_resolve_task_limit_honors_env_override(monkeypatch):
    monkeypatch.setenv("TASK_LIMIT", "32")
    assert shared._resolve_task_limit("finish_query", 100, logger) == 32


def test_resolve_task_limit_ignores_non_integer(monkeypatch):
    monkeypatch.setenv("TASK_LIMIT", "not-a-number")
    assert shared._resolve_task_limit("finish_query", 100, logger) == 100


def test_resolve_task_limit_ignores_non_positive(monkeypatch):
    monkeypatch.setenv("TASK_LIMIT", "0")
    assert shared._resolve_task_limit("finish_query", 100, logger) == 100


# --- drain and exit ---------------------------------------------------------


class _FakeHeartbeat:
    def __init__(self):
        self.marked = False
        self.stopped = False

    async def mark_clean_shutdown(self):
        self.marked = True

    async def stop(self):
        self.stopped = True


@pytest.mark.asyncio
async def test_drain_and_exit_drains_then_exits_zero(monkeypatch):
    """With no task holding a permit, drain completes immediately and the
    process exits 0 after writing the clean-shutdown marker."""
    hb = _FakeHeartbeat()
    shared._active_heartbeat = hb
    limiter = asyncio.Semaphore(4)

    with pytest.raises(SystemExit) as exc:
        await shared._drain_and_exit(limiter, 4, logger)

    assert exc.value.code == 0
    assert hb.marked is True
    assert hb.stopped is True


@pytest.mark.asyncio
async def test_drain_and_exit_waits_for_inflight_permit(monkeypatch):
    """A held permit (an in-flight task) is awaited; once released, drain
    completes and the process exits."""
    hb = _FakeHeartbeat()
    shared._active_heartbeat = hb
    limiter = asyncio.Semaphore(2)
    # Simulate one in-flight task holding a permit.
    await limiter.acquire()

    async def _release_soon():
        await asyncio.sleep(0.02)
        limiter.release()

    monkeypatch.setattr(settings, "worker_drain_timeout_sec", 1.0)
    releaser = asyncio.create_task(_release_soon())

    with pytest.raises(SystemExit) as exc:
        await shared._drain_and_exit(limiter, 2, logger)

    assert exc.value.code == 0
    assert hb.marked is True
    await releaser


@pytest.mark.asyncio
async def test_drain_and_exit_times_out_but_still_exits(monkeypatch):
    """If an in-flight task never finishes, drain times out yet still exits so
    the orchestrator's terminationGracePeriod isn't blocked indefinitely."""
    hb = _FakeHeartbeat()
    shared._active_heartbeat = hb
    limiter = asyncio.Semaphore(2)
    await limiter.acquire()  # never released

    monkeypatch.setattr(settings, "worker_drain_timeout_sec", 0.05)

    with pytest.raises(SystemExit) as exc:
        await shared._drain_and_exit(limiter, 2, logger)

    assert exc.value.code == 0
    # Marker is still written even on a timed-out drain.
    assert hb.marked is True


# --- broker liveness self-exit ----------------------------------------------


def test_exit_if_broker_wedged_stays_quiet_when_healthy(monkeypatch):
    """A recently-successful broker read must never trigger the self-exit."""
    monkeypatch.setattr(settings, "broker_unhealthy_exit_sec", 300.0)
    monkeypatch.setattr(shared.broker_health, "seconds_since_success", lambda: 5.0)
    # Should simply return, not raise SystemExit.
    shared._exit_if_broker_wedged("aragorn.score", logger)


def test_exit_if_broker_wedged_exits_after_threshold(monkeypatch):
    """Once the broker has been unreachable past the window, the worker exits
    non-zero so Kubernetes reschedules it."""
    monkeypatch.setattr(settings, "broker_unhealthy_exit_sec", 300.0)
    monkeypatch.setattr(shared.broker_health, "seconds_since_success", lambda: 301.0)

    with pytest.raises(SystemExit) as exc:
        shared._exit_if_broker_wedged("aragorn.score", logger)
    assert exc.value.code == 1


def test_exit_if_broker_wedged_disabled_with_zero(monkeypatch):
    """A zero threshold disables the self-exit even when the broker is long gone
    -- an escape hatch so ops can turn the behavior off without a code change."""
    monkeypatch.setattr(settings, "broker_unhealthy_exit_sec", 0.0)
    monkeypatch.setattr(
        shared.broker_health, "seconds_since_success", lambda: 100_000.0
    )
    shared._exit_if_broker_wedged("aragorn.score", logger)


# --- clean shutdown marker --------------------------------------------------


@pytest.mark.asyncio
async def test_mark_clean_shutdown_writes_marker(redis_mock, monkeypatch):
    """``mark_clean_shutdown`` writes the shutdown marker key the monitor reads
    to classify a clean scale-down."""
    # heartbeat binds broker_client at import; point it at the fake broker.
    monkeypatch.setattr(heartbeat_module, "broker_client", redis_mock["broker"])
    hb = Heartbeat("finish_query", "abc123", 100, manage_signals=False)

    await hb.mark_clean_shutdown()

    raw = await redis_mock["broker"].get(shutdown_key("finish_query", "abc123"))
    assert raw is not None


def test_heartbeat_manage_signals_flag_defaults_true():
    assert Heartbeat("s", "c", 1).manage_signals is True
    assert Heartbeat("s", "c", 1, manage_signals=False).manage_signals is False


# --- resource / in-flight reporting -----------------------------------------


def test_heartbeat_in_flight_from_semaphore():
    """``_in_flight`` = task_limit minus the semaphore's free permits."""
    limiter = asyncio.Semaphore(10)
    hb = Heartbeat("merge_message", "c1", 10, manage_signals=False, limiter=limiter)
    assert hb._in_flight() == 0


@pytest.mark.asyncio
async def test_heartbeat_in_flight_tracks_acquired_permits():
    limiter = asyncio.Semaphore(10)
    await limiter.acquire()
    await limiter.acquire()
    hb = Heartbeat("merge_message", "c1", 10, manage_signals=False, limiter=limiter)
    assert hb._in_flight() == 2


def test_heartbeat_in_flight_none_without_limiter():
    """Workers/tests that don't pass a limiter simply omit the count."""
    hb = Heartbeat("merge_message", "c1", 10, manage_signals=False)
    assert hb._in_flight() is None


@pytest.mark.asyncio
async def test_heartbeat_ping_payload_includes_resources(redis_mock, monkeypatch):
    """A ping writes in-flight, RSS and CPU fields for the monitor to surface."""
    import json

    from shepherd_utils.heartbeat import heartbeat_key

    monkeypatch.setattr(heartbeat_module, "broker_client", redis_mock["broker"])
    limiter = asyncio.Semaphore(8)
    await limiter.acquire()
    hb = Heartbeat("merge_message", "abc", 8, manage_signals=False, limiter=limiter)

    await hb._ping()

    raw = await redis_mock["broker"].get(heartbeat_key("merge_message", "abc"))
    payload = json.loads(raw)
    assert payload["in_flight"] == 1
    assert payload["task_limit"] == 8
    # Keys are always present; values are best-effort (None off Linux / no /proc).
    assert "rss_bytes" in payload
    assert "cpu_pct" in payload
    # The very first ping has no prior CPU sample to diff against.
    assert payload["cpu_pct"] is None


def test_heartbeat_cpu_pct_computes_after_two_samples(monkeypatch):
    """CPU% is a delta between consecutive samples, so it lands on the 2nd read."""
    # Build the heartbeat before patching the clock so its ``started_at`` read
    # doesn't consume one of our scripted wall-clock samples.
    hb = Heartbeat("s", "c", 1, manage_signals=False)
    samples = iter([1.0, 1.5])  # +0.5 cpu-seconds between pings
    walls = iter([100.0, 101.0])  # over 1.0 wall-second -> 50% of a core
    monkeypatch.setattr(heartbeat_module, "_read_cpu_seconds", lambda: next(samples))
    monkeypatch.setattr(heartbeat_module.time, "time", lambda: next(walls))

    assert hb._cpu_pct() is None  # first sample: nothing to diff
    assert hb._cpu_pct() == 50.0


# --- get_tasks integration: shutdown short-circuits the poll loop -----------


@pytest.mark.asyncio
async def test_get_tasks_exits_when_shutdown_already_requested(monkeypatch):
    """If shutdown is requested, get_tasks must not yield any task -- it drains
    and exits the process instead of fetching new work."""
    monkeypatch.setattr(shared, "initialize_db", _async_noop)

    fake_hb = _FakeHeartbeat()

    class _HBFactory:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            return fake_hb

    monkeypatch.setattr(shared, "Heartbeat", _HBFactory)
    monkeypatch.setattr(shared, "install_shutdown_handlers", lambda hb=None: None)

    # Request shutdown before iterating so the first loop turn drains+exits.
    shared._active_heartbeat = fake_hb
    shared._request_shutdown()

    yielded = []
    with pytest.raises(SystemExit) as exc:
        async for task in shared.get_tasks("finish_query", "consumer", "cid", 8):
            yielded.append(task)

    assert exc.value.code == 0
    assert yielded == []
    assert fake_hb.marked is True


async def _async_noop(*args, **kwargs):
    return None

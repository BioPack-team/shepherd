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

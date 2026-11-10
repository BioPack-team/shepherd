"""Push "something finished" instead of polling for it (#221).

Waiters used to poll on a fixed timer: every sync ``/query`` read Postgres
every 0.5s for up to its whole timeout, every ARAX stream read it every 0.2s,
and every lookup worker read its query's callback rows every second. A few
dozen waiters made a steady load that competed with intake for pooled
connections.

Now the code that changes the state publishes a signal on Redis pub/sub
(``signal``), and a waiter (``wait_for``) re-checks the state when one
arrives. Pub/sub can drop a message (a listener reconnecting, Redis
restarting), so the waiter still re-checks on its own, backing off from
``MIN_INTERVAL_SEC`` to ``MAX_INTERVAL_SEC``: a signal only makes the common
case immediate, and the state is always read from where it lives.

One pattern-subscribed listener per process serves every waiter in it, so this
costs one Redis connection per process, not one per waiter.
"""

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Set

LOGGER = logging.getLogger("shepherd.completion")

CHANNEL_PREFIX = "shepherd:signal:"
#: Kinds of signal: a query reached COMPLETED; a query's callback rows changed.
DONE = "done"
CALLBACKS = "callbacks"

MIN_INTERVAL_SEC = 0.5
MAX_INTERVAL_SEC = 5.0


def channel(kind: str, query_id: str) -> str:
    return f"{CHANNEL_PREFIX}{kind}:{query_id}"


def _client():
    # Looked up at call time, so tests that swap the broker client are seen.
    from . import broker

    return broker.broker_client


async def signal(kind: str, query_id: Optional[str]) -> None:
    """Tell any waiter on ``query_id`` that its ``kind`` of state changed.

    Best effort: a waiter re-checks on its own regardless.
    """
    if not query_id:
        return
    try:
        await _client().publish(channel(kind, str(query_id)), "1")
    except Exception as e:
        LOGGER.debug(f"Could not publish {kind} for {query_id}: {e}")


class _Listener:
    """The process's one subscription, fanning signals out to waiters."""

    def __init__(self) -> None:
        self._waiters: Dict[str, Set[asyncio.Event]] = {}
        self._task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._subscribed: Optional[asyncio.Event] = None

    def register(self, name: str) -> asyncio.Event:
        event = asyncio.Event()
        self._waiters.setdefault(name, set()).add(event)
        return event

    def unregister(self, name: str, event: asyncio.Event) -> None:
        events = self._waiters.get(name)
        if events is not None:
            events.discard(event)
            if not events:
                del self._waiters[name]

    async def ensure_running(self) -> None:
        """Start the subscription on this loop if it isn't running.

        Not awaited until subscribed: a signal sent before the subscription
        takes is only a lost wake-up, which the waiter's own re-check covers
        within ``MIN_INTERVAL_SEC``.
        """
        loop = asyncio.get_running_loop()
        if self._task is None or self._task.done() or self._loop is not loop:
            self._loop = loop
            self._subscribed = asyncio.Event()
            self._task = loop.create_task(self._run())

    async def _run(self) -> None:
        backoff = 1.0
        while True:
            pubsub = None
            try:
                pubsub = _client().pubsub()
                await pubsub.psubscribe(f"{CHANNEL_PREFIX}*")
                self._subscribed.set()
                backoff = 1.0
                while True:
                    message = await pubsub.get_message(
                        ignore_subscribe_messages=True, timeout=1.0
                    )
                    if message is None:
                        continue
                    name = message.get("channel")
                    if isinstance(name, bytes):
                        name = name.decode()
                    for event in self._waiters.get(name, ()):
                        event.set()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                LOGGER.debug(f"Completion listener reconnecting: {e}")
                self._subscribed.clear()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            finally:
                if pubsub is not None:
                    try:
                        await pubsub.aclose()
                    except Exception:
                        pass


_listener = _Listener()


async def wait_for(
    kind: str,
    query_id: str,
    check: Callable[[], Awaitable[Any]],
    timeout: float,
    min_interval: Optional[float] = None,
    max_interval: Optional[float] = None,
) -> Any:
    """Await ``check()`` returning something other than None, for ``timeout``.

    ``check`` runs at once, again whenever ``kind`` is signalled for
    ``query_id``, and otherwise on a backoff from ``min_interval`` to
    ``max_interval``. Returns its first non-None result, or None on timeout.
    """
    min_interval = MIN_INTERVAL_SEC if min_interval is None else min_interval
    max_interval = MAX_INTERVAL_SEC if max_interval is None else max_interval
    max_interval = max(max_interval, min_interval)
    name = channel(kind, str(query_id))
    event = _listener.register(name)
    try:
        await _listener.ensure_running()
        deadline = time.monotonic() + timeout
        interval = min_interval
        while True:
            # Cleared before the check, so a signal for a change the check
            # just missed still wakes the wait below.
            event.clear()
            result = await check()
            if result is not None:
                return result
            left = deadline - time.monotonic()
            if left <= 0:
                return None
            try:
                await asyncio.wait_for(event.wait(), min(interval, left))
                interval = min_interval
            except asyncio.TimeoutError:
                interval = min(interval * 2, max_interval)
    finally:
        _listener.unregister(name, event)


def signalled(kind: str, query_id: str) -> asyncio.Event:
    """An event set whenever ``kind`` is signalled for ``query_id``.

    For a caller with its own loop (it must ``release`` the event and call
    ``ensure_listening`` first). Pair with ``release``.
    """
    return _listener.register(channel(kind, str(query_id)))


def release(kind: str, query_id: str, event: asyncio.Event) -> None:
    _listener.unregister(channel(kind, str(query_id)), event)


async def ensure_listening() -> None:
    await _listener.ensure_running()

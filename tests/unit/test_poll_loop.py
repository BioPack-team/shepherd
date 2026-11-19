"""The shared worker poll loop (#223 C5-4).

Every worker had its own copy of the loop, and they had drifted: most fired
``asyncio.create_task`` without keeping a reference (the event loop holds only
a weak one, so a running task could be garbage-collected mid-flight), and
their ``except Exception``/``except BaseException`` arms swallowed
``CancelledError`` and went back to polling, so cancelling a worker never
stopped it.
"""

import asyncio
import gc
import logging

from shepherd_utils import shared


class _Limiter:
    def release(self):
        pass


def _feed(count, then_block=True):
    async def _get_tasks(stream, group, consumer, limit):
        for i in range(count):
            yield (f"{i}-0", {}), None, logging.getLogger("t"), _Limiter()
        if then_block:
            await asyncio.sleep(3600)

    return _get_tasks


async def test_cancelling_the_loop_stops_it_and_its_tasks(monkeypatch):
    monkeypatch.setattr(shared, "get_tasks", _feed(2))
    started, cancelled = [], []
    stopped = []

    async def handler(task, ctx, logger, limiter):
        started.append(task[0])
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.append(task[0])
            raise

    loop_task = asyncio.create_task(
        shared.run_poll_loop(
            "s", "g", "c", 2, handler, on_stop=lambda: stopped.append(1)
        )
    )
    while len(started) < 2:
        await asyncio.sleep(0)
    loop_task.cancel()
    await asyncio.wait_for(loop_task, timeout=2)  # returns: it doesn't poll on

    await asyncio.sleep(0)
    assert sorted(cancelled) == ["0-0", "1-0"]
    assert stopped == [1]


async def test_an_async_on_stop_is_awaited(monkeypatch):
    monkeypatch.setattr(shared, "get_tasks", _feed(0))
    stopped = []

    async def on_stop():
        await asyncio.sleep(0)
        stopped.append(1)

    async def handler(*args):
        pass

    loop_task = asyncio.create_task(
        shared.run_poll_loop("s", "g", "c", 1, handler, on_stop=on_stop)
    )
    await asyncio.sleep(0.01)
    loop_task.cancel()
    await asyncio.wait_for(loop_task, timeout=2)

    assert stopped == [1]


async def test_running_tasks_survive_garbage_collection(monkeypatch):
    monkeypatch.setattr(shared, "get_tasks", _feed(1))
    release = asyncio.Event()
    finished = []

    async def handler(task, ctx, logger, limiter):
        await release.wait()
        finished.append(task[0])

    loop_task = asyncio.create_task(shared.run_poll_loop("s", "g", "c", 1, handler))
    await asyncio.sleep(0.01)
    gc.collect()
    release.set()
    await asyncio.sleep(0.01)
    loop_task.cancel()
    await asyncio.wait_for(loop_task, timeout=2)

    assert finished == ["0-0"]


async def test_a_polling_error_is_logged_and_polling_resumes(monkeypatch):
    calls = []

    async def _get_tasks(stream, group, consumer, limit):
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("redis went away")
        yield ("0-0", {}), None, logging.getLogger("t"), _Limiter()
        await asyncio.Event().wait()

    real_sleep = asyncio.sleep

    async def _no_backoff(delay, *args, **kwargs):
        await real_sleep(0)

    monkeypatch.setattr(shared, "get_tasks", _get_tasks)
    monkeypatch.setattr(shared.asyncio, "sleep", _no_backoff)
    handled = []

    async def handler(task, *args):
        handled.append(task[0])

    loop_task = asyncio.create_task(shared.run_poll_loop("s", "g", "c", 1, handler))
    for _ in range(50):
        if handled:
            break
        await real_sleep(0)
    loop_task.cancel()
    await asyncio.wait_for(loop_task, timeout=2)

    assert len(calls) == 2
    assert handled == ["0-0"]

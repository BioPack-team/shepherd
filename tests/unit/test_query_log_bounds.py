"""Tests for the two bounds on per-query log growth.

One production query ended up with 781k log entries and a 1GB log list: a
merge that failed deterministically was retried immediately and unboundedly,
and every iteration appended its traceback. Every reader of that list then
loaded the whole thing into several GB of memory, and the worker's own
parent process quietly kept a copy of every record via a log handler that
was never drained. The list is now capped on append, and the leaking handler
is gone.
"""

import logging

import pytest

from shepherd_utils import shared
from shepherd_utils.config import settings
from shepherd_utils.db import _append_logs, get_logs
from shepherd_utils.logger import attach_query_handler, get_query_handler

logger = logging.getLogger(__name__)


def _entries(start, n):
    return [
        {"message": f"m{i}", "timestamp": f"t{i:06d}", "level": "INFO"}
        for i in range(start, start + n)
    ]


async def test_append_logs_keeps_only_the_newest_entries(redis_mock, monkeypatch):
    monkeypatch.setattr(settings, "query_max_log_entries", 10)
    await _append_logs("r", _entries(0, 8))
    await _append_logs("r", _entries(8, 7))
    logs = await get_logs("r", logger)
    assert [e["message"] for e in logs] == [f"m{i}" for i in range(5, 15)]


async def test_append_logs_cap_is_applied_within_a_single_flush(
    redis_mock, monkeypatch
):
    monkeypatch.setattr(settings, "query_max_log_entries", 3)
    await _append_logs("r", _entries(0, 50))
    logs = await get_logs("r", logger)
    assert [e["message"] for e in logs] == ["m47", "m48", "m49"]


async def test_append_logs_cap_can_be_disabled(redis_mock, monkeypatch):
    monkeypatch.setattr(settings, "query_max_log_entries", 0)
    await _append_logs("r", _entries(0, 50))
    assert len(await get_logs("r", logger)) == 50


def test_worker_level_logger_does_not_retain_task_records():
    """The per-task loggers are children of the worker-level one, and logging
    propagates their records to every ancestor's handlers. A query handler on
    the worker-level logger therefore collected every record of every task
    the worker ever ran and nothing drained it. get_tasks must not attach one."""
    # get_tasks is an async generator; assert on its source rather than driving
    # the broker: the worker logger must be created without a query handler.
    import inspect

    body = inspect.getsource(shared.get_tasks)
    assert "attach_query_handler(worker_logger)" not in body

    # And the mechanism the leak relied on, so the assertion above stays
    # meaningful: a record on a child logger does reach the parent's handler.
    parent = logging.getLogger("shepherd.leaktest.parent")
    parent.setLevel(logging.DEBUG)
    attach_query_handler(parent)
    child = logging.getLogger("shepherd.leaktest.parent.q1")
    child.setLevel(logging.DEBUG)
    attach_query_handler(child)
    child.debug("hello")
    get_query_handler(child).drain()
    assert len(get_query_handler(parent).log_queue) == 1
    parent.removeHandler(get_query_handler(parent))

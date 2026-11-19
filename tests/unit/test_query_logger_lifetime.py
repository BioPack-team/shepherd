"""Per-query loggers must not live for the whole process (#216).

``logging.getLogger`` registers every name it sees with the logging manager
forever. Workers named a logger per query, and the server one per query and
one per callback, each with a query log handler and its queue, so a pod's
memory grew with every query it served.
"""

import gc
import logging
import weakref

from shepherd_server import base_routes
from shepherd_utils import shared
from shepherd_utils.db import get_logs, save_logs
from shepherd_utils.logger import get_query_handler


def _registered():
    return len(logging.Logger.manager.loggerDict)


def _task(query_id):
    return ("1-0", {"query_id": query_id, "response_id": f"r-{query_id}", "otel": "{}"})


def test_worker_task_loggers_are_not_kept():
    shared._build_task_context("aragorn.score", "c1", _task("warmup"), logging.INFO)
    before = _registered()

    for i in range(200):
        shared._build_task_context("aragorn.score", "c1", _task(f"q{i}"), logging.INFO)

    assert _registered() == before


def test_a_finished_tasks_logger_and_handler_are_freed():
    _, task_logger = shared._build_task_context(
        "aragorn.score", "c1", _task("q-free"), logging.INFO
    )
    handler = weakref.ref(get_query_handler(task_logger))
    task_logger = None
    gc.collect()
    assert handler() is None


async def test_server_query_loggers_are_not_kept(monkeypatch):
    async def _noop(*args, **kwargs):
        return None

    monkeypatch.setattr(base_routes, "add_query", _noop)
    monkeypatch.setattr(base_routes, "add_task", _noop)
    await base_routes.run_query("aragorn", {"message": {}})
    before = _registered()

    for _ in range(50):
        await base_routes.run_query("aragorn", {"message": {}})

    assert _registered() == before


async def test_task_logs_still_reach_the_query_and_the_console(redis_mock, caplog):
    """Unregistered, the logger still feeds save_logs and propagates."""
    _, task_logger = shared._build_task_context(
        "aragorn.score", "c1", _task("q-logs"), logging.INFO
    )
    with caplog.at_level(logging.INFO, logger="shepherd"):
        task_logger.info("hello from the task")
    await save_logs("r-q-logs", task_logger)

    stored = await get_logs("r-q-logs", task_logger)
    assert any(entry["message"] == "hello from the task" for entry in stored)
    assert "hello from the task" in caplog.text


def test_a_level_set_after_logging_takes_effect():
    """The callback handler sets its logger's level once it has read the
    body; an unregistered logger must not keep a stale level cache."""
    from shepherd_utils.logger import transient_logger

    task_logger = transient_logger("shepherd.cb-level", logging.getLogger("shepherd"))
    logging.getLogger("shepherd").setLevel(logging.INFO)
    assert not task_logger.isEnabledFor(logging.DEBUG)
    task_logger.setLevel(logging.DEBUG)
    assert task_logger.isEnabledFor(logging.DEBUG)

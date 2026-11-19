"""Every pool worker bounds its task by its query's remaining budget (#245).

The bound itself is ProcessPoolManager.run_within_budget (tested in
test_process_pool.py); this checks each worker goes through it with its task.
"""

import asyncio
import importlib
import logging
import pickle

import pytest

from shepherd_utils.logger import transient_logger

logger = logging.getLogger(__name__)


class _Pool:
    def __init__(self):
        self.calls = []

    async def run_within_budget(self, loop, task_fields, step, fn, *args):
        self.calls.append((task_fields, step, fn))
        self.args = args
        return []


class _Limiter:
    def release(self):
        pass


@pytest.mark.parametrize(
    "module,task_fn,setup",
    [
        ("workers.aragorn_score.worker", "aragorn_score", None),
        # these two import what only their images install; their own test
        # modules stub it (torch) or put it on the path (ranker) at import
        ("workers.arax_rank.worker", "arax_rank_task", "tests.unit.test_arax_rank"),
        (
            "workers.score_paths.worker",
            "score_paths_task",
            "tests.unit.test_score_paths",
        ),
        ("workers.arax_pathfinder.worker", "arax_pathfinder_task", None),
        ("workers.aragorn_omnicorp.worker", "aragorn_omnicorp", None),
    ],
)
async def test_the_worker_runs_within_its_querys_budget(
    module, task_fn, setup, monkeypatch
):
    from shepherd_utils import shared

    if setup:
        importlib.import_module(setup)
    worker = importlib.import_module(module)

    async def _noop(*args, **kwargs):
        return None

    monkeypatch.setattr(shared, "wrap_up_task", _noop)
    monkeypatch.setattr(shared, "_guard_response_size", _noop)
    failures = []

    async def _failed(*args, error=None, **kwargs):
        failures.append(error)

    monkeypatch.setattr(shared, "handle_task_failure", _failed)
    pool = _Pool()
    task = (
        "1-0",
        {"query_id": "q", "response_id": "r", "query_deadline": "1", "otel": "{}"},
    )

    # A task's real logger: unregistered, so it can't be pickled into a child.
    task_logger = transient_logger("shepherd.test.task", logger)

    await worker.process_task(
        task, None, task_logger, _Limiter(), asyncio.get_running_loop(), pool
    )

    assert failures == []
    ((task_fields, step, fn),) = pool.calls
    assert task_fields is task[1]
    assert step
    assert fn is getattr(worker, task_fn)
    # everything handed to the child has to survive the process boundary
    pickle.dumps(pool.args)

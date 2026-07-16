"""Regression guard: CPU-bound workers must dispatch tasks concurrently.

``arax_rank`` and ``aragorn_score`` previously awaited ``process_task`` inline
inside their ``poll_for_tasks`` ``async for`` body, which serialized every task
through their ProcessPoolExecutor (only one task ever ran at a time -- the same
bug fixed in ``merge_message``). The corrected code dispatches each task with
``asyncio.create_task(process_task(...))``, matching the ``filter_results_top_n``
template.

``poll_for_tasks`` is an unbounded ``while True`` loop whose ``CancelledError``
handler intentionally does not return (the shared worker template), so it can't
be run to completion in a unit test; and ``arax_rank`` isn't importable outside
its container. So we pin the dispatch shape statically on the source of
``poll_for_tasks``: it must wrap ``process_task`` in ``asyncio.create_task`` and
must not ``await`` it inline.
"""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

WORKER_FILES = [
    "workers/arax_rank/worker.py",
    "workers/aragorn_score/worker.py",
]


def _poll_for_tasks_source(worker_file: str) -> str:
    text = (REPO_ROOT / worker_file).read_text()
    start = text.index("async def poll_for_tasks")
    # poll_for_tasks is the last definition before the __main__ guard.
    end = text.index('if __name__ == "__main__"', start)
    return text[start:end]


@pytest.mark.parametrize("worker_file", WORKER_FILES)
def test_poll_for_tasks_dispatches_concurrently(worker_file):
    src = _poll_for_tasks_source(worker_file)

    # Correct pattern: dispatch each task concurrently.
    assert "asyncio.create_task(" in src, (
        f"{worker_file}: poll_for_tasks must dispatch tasks with "
        "asyncio.create_task(process_task(...)); found no create_task."
    )
    # Serial bug shape: awaiting process_task inline in the loop body.
    assert "await process_task(" not in src, (
        f"{worker_file}: poll_for_tasks awaits process_task inline, which "
        "serializes every task through the process pool (the merge_message bug). "
        "Dispatch with asyncio.create_task(process_task(...)) instead."
    )

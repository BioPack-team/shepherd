"""Regression guards: how CPU-bound workers must run their heavy work.

Two invariants, both learned the hard way, both pinned statically here.

**Dispatch concurrently.** ``arax_rank`` and ``aragorn_score`` previously
awaited ``process_task`` inline inside their ``poll_for_tasks`` ``async for``
body, which serialized every task through their ProcessPoolExecutor (only one
task ever ran at a time -- the same bug fixed in ``merge_message``). The
corrected code dispatches each task with ``asyncio.create_task(process_task
(...))``, matching the ``filter_results_top_n`` template.

**Offload to a process pool, not a thread pool.** These workers' hot loops are
mostly pure Python, so in a ``ThreadPoolExecutor`` they hold the GIL and starve
the event loop the heartbeat pings from. Once the heartbeat goes stale past
``HEARTBEAT_TTL_SEC`` a peer stops treating the worker as alive and reclaims its
in-flight tasks; past ``worker_loop_stall_exit_sec`` the loop watchdog restarts
the pod outright. ``arax_pathfinder`` hit exactly this on ``asyncio.to_thread``
and ``score_paths`` on a ``ThreadPoolExecutor``; both now use
``ProcessPoolManager``, which also brings the OOM self-heal and per-task timeout.

``poll_for_tasks`` is an unbounded ``while True`` loop whose ``CancelledError``
handler intentionally does not return (the shared worker template), so it can't
be run to completion in a unit test; and these workers aren't importable outside
their containers. So both invariants are pinned on the worker sources instead.
"""

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

WORKER_FILES = [
    "workers/arax_rank/worker.py",
    "workers/aragorn_score/worker.py",
    "workers/arax_pathfinder/worker.py",
    "workers/score_paths/worker.py",
]


def _worker_source(worker_file: str) -> str:
    return (REPO_ROOT / worker_file).read_text()


def _poll_for_tasks_source(worker_file: str) -> str:
    text = _worker_source(worker_file)
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


@pytest.mark.parametrize("worker_file", WORKER_FILES)
def test_cpu_bound_work_runs_in_a_process_pool(worker_file):
    src = _worker_source(worker_file)

    assert "ProcessPoolManager" in src, (
        f"{worker_file}: CPU-bound work must be offloaded with "
        "ProcessPoolManager; found no reference to it."
    )
    # Thread-pool bug shape: heavy pure-Python work sharing the GIL with the
    # event loop, which starves the heartbeat and gets the worker's tasks
    # reclaimed while it is still very much alive.
    # Matched on the call, not the bare name, so the docstrings explaining why
    # these workers moved off a thread pool don't trip their own guard.
    assert "ThreadPoolExecutor(" not in src, (
        f"{worker_file}: offloads to a ThreadPoolExecutor, whose threads hold "
        "the GIL against the event loop and can starve the heartbeat past "
        "HEARTBEAT_TTL_SEC. Use ProcessPoolManager instead."
    )
    assert "asyncio.to_thread(" not in src, (
        f"{worker_file}: offloads with asyncio.to_thread, which has the same "
        "GIL/heartbeat problem as a ThreadPoolExecutor. Use ProcessPoolManager."
    )

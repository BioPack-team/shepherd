"""Workers that ACK directly (not via wrap_up_task) still report latency.

Without this they never appeared in the monitor's per-worker latency charts:
only wrap_up_task / handle_task_failure pushed a task duration.
"""

import asyncio
import importlib
import logging
import time
from unittest.mock import AsyncMock

import pytest

from shepherd_utils import shared

WORKERS = [
    ("workers.finish_query.worker", "finish_query", "finish_query"),
    ("workers.ars_fanout.worker", "ars_fanout", "ars.fanout"),
    ("workers.ars_merge.worker", "ars_merge", "ars.merge"),
    ("workers.ars_notify.worker", "ars_notify", "ars.notify"),
    ("workers.ars_premerge.worker", "ars_premerge", "ars.premerge"),
]


@pytest.mark.parametrize("module_name,fn_name,stream", WORKERS)
@pytest.mark.parametrize("fails", [False, True])
async def test_process_task_records_duration(
    monkeypatch, module_name, fn_name, stream, fails
):
    mod = importlib.import_module(module_name)
    work = AsyncMock(side_effect=RuntimeError("boom") if fails else None)
    monkeypatch.setattr(mod, fn_name, work)
    monkeypatch.setattr(mod, "mark_task_as_complete", AsyncMock())
    monkeypatch.setattr(mod, "save_logs", AsyncMock(), raising=False)
    for name in ("fail_unfanned_parent", "cleanup_callbacks"):
        if hasattr(mod, name):
            monkeypatch.setattr(mod, name, AsyncMock())
    recorded = AsyncMock()
    monkeypatch.setattr(shared, "_record_task_duration", recorded)

    started = str(time.time() - 2)
    task = ("1-0", {"_started_at": started, "query_id": "q", "response_id": "r"})
    await mod.process_task(task, None, logging.getLogger("t"), asyncio.Semaphore(1))

    mod.mark_task_as_complete.assert_awaited_once()
    recorded.assert_awaited_once()
    assert recorded.await_args.args[:2] == (stream, started)

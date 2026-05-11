"""Tests for the ``process_task`` wrappers across multiple workers.

The pattern is repeated across most workers: call the worker's main async
function, then ``wrap_up_task`` on success or ``handle_task_failure`` on
exception. We verify the cancellation and failure branches are wired up
correctly.

Workers covered:

- ``filter_kgraph_orphans``
- ``filter_results_top_n``
- ``filter_analyses_top_n``
- ``sort_results_score``
- ``example_ara``
- ``example_score``
- ``example_lookup``
- ``aragorn``
- ``aragorn_pathfinder``
- ``bte``
"""

import asyncio
import json
import logging

import pytest

logger = logging.getLogger(__name__)


def _make_task(stream_id):
    return [
        "msg-id",
        {
            "query_id": "qid",
            "response_id": "rid",
            "workflow": json.dumps([{"id": stream_id}]),
            "log_level": "20",
            "otel": "{}",
        },
    ]


class _Limiter:
    """Stand-in for the ``asyncio.Semaphore`` ``process_task`` releases.

    We just record whether ``release()`` was called.
    """

    def __init__(self):
        self.released = False

    def release(self):
        self.released = True


# --- filter_kgraph_orphans ------------------------------------------------


@pytest.mark.asyncio
async def test_filter_kgraph_orphans_process_task_happy_path(redis_mock, mocker):
    from workers.filter_kgraph_orphans import worker as fko

    mocker.patch.object(fko, "do_filter_kgraph_orphans", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(fko, "wrap_up_task", new_callable=mocker.AsyncMock)

    limiter = _Limiter()
    await fko.process_task(_make_task("filter_kgraph_orphans"), None, logger, limiter)
    assert mock_wrap.called
    assert limiter.released


@pytest.mark.asyncio
async def test_filter_kgraph_orphans_process_task_failure_routes_to_failure_handler(
    redis_mock, mocker
):
    from workers.filter_kgraph_orphans import worker as fko

    mocker.patch.object(
        fko,
        "do_filter_kgraph_orphans",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("kaboom"),
    )
    mock_failure = mocker.patch.object(
        fko, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await fko.process_task(_make_task("filter_kgraph_orphans"), None, logger, limiter)
    assert mock_failure.called
    assert limiter.released


@pytest.mark.asyncio
async def test_filter_kgraph_orphans_process_task_cancellation_does_not_route_failure(
    redis_mock, mocker
):
    """A CancelledError should be logged but not routed to handle_task_failure."""
    from workers.filter_kgraph_orphans import worker as fko

    mocker.patch.object(
        fko,
        "do_filter_kgraph_orphans",
        new_callable=mocker.AsyncMock,
        side_effect=asyncio.CancelledError,
    )
    mock_failure = mocker.patch.object(
        fko, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await fko.process_task(_make_task("filter_kgraph_orphans"), None, logger, limiter)
    assert not mock_failure.called
    assert limiter.released


# --- filter_results_top_n -------------------------------------------------


@pytest.mark.asyncio
async def test_filter_results_top_n_process_task_happy_path(redis_mock, mocker):
    from workers.filter_results_top_n import worker as frt

    mocker.patch.object(frt, "filter_results_top_n", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(frt, "wrap_up_task", new_callable=mocker.AsyncMock)

    limiter = _Limiter()
    await frt.process_task(_make_task("filter_results_top_n"), None, logger, limiter)
    assert mock_wrap.called


@pytest.mark.asyncio
async def test_filter_results_top_n_process_task_failure(redis_mock, mocker):
    from workers.filter_results_top_n import worker as frt

    mocker.patch.object(
        frt,
        "filter_results_top_n",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("oops"),
    )
    mock_failure = mocker.patch.object(
        frt, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await frt.process_task(_make_task("filter_results_top_n"), None, logger, limiter)
    assert mock_failure.called


# --- filter_analyses_top_n ------------------------------------------------


@pytest.mark.asyncio
async def test_filter_analyses_top_n_process_task_happy_path(redis_mock, mocker):
    from workers.filter_analyses_top_n import worker as fan

    mocker.patch.object(fan, "filter_analyses_top_n", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(fan, "wrap_up_task", new_callable=mocker.AsyncMock)

    limiter = _Limiter()
    await fan.process_task(_make_task("filter_analyses_top_n"), None, logger, limiter)
    assert mock_wrap.called


@pytest.mark.asyncio
async def test_filter_analyses_top_n_process_task_failure(redis_mock, mocker):
    from workers.filter_analyses_top_n import worker as fan

    mocker.patch.object(
        fan,
        "filter_analyses_top_n",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("nope"),
    )
    mock_failure = mocker.patch.object(
        fan, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await fan.process_task(_make_task("filter_analyses_top_n"), None, logger, limiter)
    assert mock_failure.called


# --- sort_results_score ---------------------------------------------------


@pytest.mark.asyncio
async def test_sort_results_score_process_task_happy_path(redis_mock, mocker):
    from workers.sort_results_score import worker as srs

    mocker.patch.object(srs, "sort_results_score", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(srs, "wrap_up_task", new_callable=mocker.AsyncMock)

    limiter = _Limiter()
    await srs.process_task(_make_task("sort_results_score"), None, logger, limiter)
    assert mock_wrap.called


@pytest.mark.asyncio
async def test_sort_results_score_process_task_failure(redis_mock, mocker):
    from workers.sort_results_score import worker as srs

    mocker.patch.object(
        srs,
        "sort_results_score",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("nope"),
    )
    mock_failure = mocker.patch.object(
        srs, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await srs.process_task(_make_task("sort_results_score"), None, logger, limiter)
    assert mock_failure.called


# --- example_ara ----------------------------------------------------------


@pytest.mark.asyncio
async def test_example_ara_process_task_happy_path(redis_mock, mocker):
    from workers.example_ara import worker as eara

    mocker.patch.object(eara, "example_ara", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(eara, "wrap_up_task", new_callable=mocker.AsyncMock)

    limiter = _Limiter()
    await eara.process_task(_make_task("example"), None, logger, limiter)
    assert mock_wrap.called


@pytest.mark.asyncio
async def test_example_ara_process_task_failure(redis_mock, mocker):
    from workers.example_ara import worker as eara

    mocker.patch.object(
        eara,
        "example_ara",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("nope"),
    )
    mock_failure = mocker.patch.object(
        eara, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await eara.process_task(_make_task("example"), None, logger, limiter)
    assert mock_failure.called


# --- example_score --------------------------------------------------------


@pytest.mark.asyncio
async def test_example_score_process_task_happy_path(redis_mock, mocker):
    from workers.example_score import worker as escore

    mocker.patch.object(escore, "example_score", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(
        escore, "wrap_up_task", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await escore.process_task(_make_task("example.score"), None, logger, limiter)
    assert mock_wrap.called


@pytest.mark.asyncio
async def test_example_score_process_task_failure(redis_mock, mocker):
    from workers.example_score import worker as escore

    mocker.patch.object(
        escore,
        "example_score",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("nope"),
    )
    mock_failure = mocker.patch.object(
        escore, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await escore.process_task(_make_task("example.score"), None, logger, limiter)
    assert mock_failure.called


# --- example_lookup -------------------------------------------------------


@pytest.mark.asyncio
async def test_example_lookup_process_task_happy_path(redis_mock, mocker):
    from workers.example_lookup import worker as elookup

    mocker.patch.object(elookup, "example_lookup", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(
        elookup, "wrap_up_task", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await elookup.process_task(_make_task("example.lookup"), None, logger, limiter)
    assert mock_wrap.called


@pytest.mark.asyncio
async def test_example_lookup_process_task_failure(redis_mock, mocker):
    from workers.example_lookup import worker as elookup

    mocker.patch.object(
        elookup,
        "example_lookup",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("nope"),
    )
    mock_failure = mocker.patch.object(
        elookup, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await elookup.process_task(_make_task("example.lookup"), None, logger, limiter)
    assert mock_failure.called


# --- aragorn -------------------------------------------------------------


@pytest.mark.asyncio
async def test_aragorn_process_task_happy_path(redis_mock, mocker):
    from workers.aragorn import worker as ar

    mocker.patch.object(ar, "aragorn", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(ar, "wrap_up_task", new_callable=mocker.AsyncMock)

    limiter = _Limiter()
    await ar.process_task(_make_task("aragorn"), None, logger, limiter)
    assert mock_wrap.called


@pytest.mark.asyncio
async def test_aragorn_process_task_failure(redis_mock, mocker):
    from workers.aragorn import worker as ar

    mocker.patch.object(
        ar,
        "aragorn",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("nope"),
    )
    mock_failure = mocker.patch.object(
        ar, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await ar.process_task(_make_task("aragorn"), None, logger, limiter)
    assert mock_failure.called


# --- aragorn_pathfinder --------------------------------------------------


@pytest.mark.asyncio
async def test_aragorn_pathfinder_process_task_happy_path(redis_mock, mocker):
    from workers.aragorn_pathfinder import worker as apf

    mocker.patch.object(apf, "shadowfax", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(apf, "wrap_up_task", new_callable=mocker.AsyncMock)

    limiter = _Limiter()
    await apf.process_task(_make_task("aragorn.pathfinder"), None, logger, limiter)
    assert mock_wrap.called


@pytest.mark.asyncio
async def test_aragorn_pathfinder_process_task_failure(redis_mock, mocker):
    from workers.aragorn_pathfinder import worker as apf

    mocker.patch.object(
        apf,
        "shadowfax",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("nope"),
    )
    mock_failure = mocker.patch.object(
        apf, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await apf.process_task(_make_task("aragorn.pathfinder"), None, logger, limiter)
    assert mock_failure.called


# --- bte -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_bte_process_task_happy_path(redis_mock, mocker):
    from workers.bte import worker as bte

    mocker.patch.object(bte, "bte", new_callable=mocker.AsyncMock)
    mock_wrap = mocker.patch.object(bte, "wrap_up_task", new_callable=mocker.AsyncMock)

    limiter = _Limiter()
    await bte.process_task(_make_task("bte"), None, logger, limiter)
    assert mock_wrap.called


@pytest.mark.asyncio
async def test_bte_process_task_failure(redis_mock, mocker):
    from workers.bte import worker as bte

    mocker.patch.object(
        bte,
        "bte",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("nope"),
    )
    mock_failure = mocker.patch.object(
        bte, "handle_task_failure", new_callable=mocker.AsyncMock
    )

    limiter = _Limiter()
    await bte.process_task(_make_task("bte"), None, logger, limiter)
    assert mock_failure.called


# --- wrap_up_task failures shouldn't escape -------------------------------


@pytest.mark.asyncio
async def test_process_task_swallows_wrap_up_failures(redis_mock, mocker):
    """The pattern is: try/except wrap_up_task — failures get logged but
    don't escape the wrapper. Verify on filter_kgraph_orphans."""
    from workers.filter_kgraph_orphans import worker as fko

    mocker.patch.object(fko, "do_filter_kgraph_orphans", new_callable=mocker.AsyncMock)
    mocker.patch.object(
        fko,
        "wrap_up_task",
        new_callable=mocker.AsyncMock,
        side_effect=RuntimeError("redis dropped"),
    )
    limiter = _Limiter()
    # Should not raise; logged and limiter still released.
    await fko.process_task(_make_task("filter_kgraph_orphans"), None, logger, limiter)
    assert limiter.released

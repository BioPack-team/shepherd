"""The shared lookup submission (#223 C6-3, C9-5)."""

import asyncio
import builtins
import copy
import json
import logging

import pytest

from shepherd_utils import lookups
from shepherd_utils.query_errors import QueryError
from tests.helpers.generate_messages import creative_query
from workers.aragorn_lookup import worker as aragorn_lookup
from workers.bte_lookup import worker as bte_lookup

logger = logging.getLogger(__name__)

PLAIN_QUERY = {
    "message": {
        "query_graph": {
            "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
            "edges": {"e0": {"subject": "a", "object": "b"}},
        }
    },
    "parameters": {"timeout": 5},
}

WORKERS = {
    "aragorn": aragorn_lookup.aragorn_lookup,
    "bte": bte_lookup.bte_lookup,
}


def _task():
    return ["1-0", {"query_id": "qid", "response_id": "rid", "otel": "{}"}]


@pytest.fixture
def plain(mocker, request):
    module = {"aragorn": aragorn_lookup, "bte": bte_lookup}[request.param]
    mocker.patch.object(
        module,
        "get_message",
        new_callable=mocker.AsyncMock,
        side_effect=lambda *a, **k: copy.deepcopy(PLAIN_QUERY),
    )
    registered = mocker.patch.object(
        lookups, "add_callback_id", new_callable=mocker.AsyncMock
    )
    removed = mocker.patch.object(
        lookups, "remove_callback_id", new_callable=mocker.AsyncMock
    )
    running = mocker.patch.object(
        lookups, "get_running_callbacks", new_callable=mocker.AsyncMock
    )
    return {
        "run": WORKERS[request.param],
        "ara": request.param,
        "registered": registered,
        "removed": removed,
        "running": running,
    }


# --- C6-3: the plain (non-inferred) lookup ------------------------------------


@pytest.mark.parametrize("plain", sorted(WORKERS), indirect=True)
async def test_a_rejected_plain_lookup_fails_instead_of_waiting(plain, mocker):
    """The plain lookup's POST used to go out with no error handling: a
    rejection left its callback registered, and the query waited out its whole
    lookup timeout for a callback that was never coming, then finished as an
    empty -- but successful -- answer."""
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=503),
    )

    with pytest.raises(QueryError, match="HTTP 503") as e:
        await plain["run"](_task(), logger)

    assert e.value.status_code == 502
    ((callback_id, _),) = [c.args for c in plain["removed"].await_args_list]
    assert plain["registered"].await_args.args[1] == callback_id
    plain["running"].assert_not_awaited()


@pytest.mark.parametrize("plain", sorted(WORKERS), indirect=True)
async def test_an_unreachable_retrieval_service_fails_the_plain_lookup(plain, mocker):
    import httpx

    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        side_effect=httpx.ConnectError("refused"),
    )

    with pytest.raises(QueryError, match="ConnectError: refused"):
        await plain["run"](_task(), logger)

    plain["removed"].assert_awaited_once()


@pytest.mark.parametrize("plain", sorted(WORKERS), indirect=True)
async def test_an_unacknowledged_plain_lookup_is_still_waited_for(plain, mocker):
    """The service got it and may still call back, as for the expanded ones."""
    import httpx

    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        side_effect=httpx.ReadTimeout(""),
    )
    plain["running"].side_effect = [["cb"], []]
    mocker.patch.object(lookups, "abandon_lookup_if_data_lost", mocker.AsyncMock())
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)

    await plain["run"](_task(), logger)

    plain["removed"].assert_not_awaited()
    assert plain["running"].await_count == 2


@pytest.mark.parametrize("plain", sorted(WORKERS), indirect=True)
async def test_the_plain_lookup_carries_its_own_trace_and_callback_route(plain, mocker):
    post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    plain["running"].return_value = []

    await plain["run"](_task(), logger)

    sent = post.await_args.kwargs["json"]
    callback_id = plain["registered"].await_args.args[1]
    assert sent["callback"].endswith(f"/{plain['ara']}/callback/{callback_id}")
    # the submission's trace context, not the task's: a JSON carrier
    assert isinstance(json.loads(plain["registered"].await_args.args[2]), dict)


# --- submit_lookups -------------------------------------------------------------


async def test_one_accepted_lookup_is_enough_to_wait(mocker):
    mocker.patch.object(lookups, "add_callback_id", mocker.AsyncMock())
    mocker.patch.object(lookups, "remove_callback_id", mocker.AsyncMock())
    codes = iter([500, 200, 500])
    mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        side_effect=lambda *a, **k: mocker.Mock(status_code=next(codes)),
    )

    responses = await lookups.submit_lookups([{}, {}, {}], "qid", "aragorn", logger)

    assert sorted(r.success for r in responses) == [False, False, True]
    assert lookups.remove_callback_id.await_count == 2


async def test_nothing_to_submit_is_not_a_failure():
    assert await lookups.submit_lookups([], "qid", "bte", logger) == []


async def test_the_callback_wait_can_be_cancelled(mocker):
    """aragorn_pathfinder's own copy of the wait swallowed CancelledError, so
    a cancelled query went on to build paths from whatever had arrived."""
    started = asyncio.Event()

    async def _wait(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    mocker.patch.object(lookups, "wait_for_lookups", _wait)
    waiting = asyncio.create_task(lookups.wait_for_callbacks("q", "r", 60, logger))
    await started.wait()
    waiting.cancel()

    with pytest.raises(asyncio.CancelledError):
        await waiting


# --- C9-5: bte's templates are read once ------------------------------------------


def test_bte_templates_are_read_once_per_process(monkeypatch):
    for cached in (
        bte_lookup._template_groups,
        bte_lookup._template_paths,
        bte_lookup._template,
    ):
        cached.cache_clear()
    opened = []
    real_open = builtins.open

    def _counting_open(path, *args, **kwargs):
        if "bte_lookup" in str(path):
            opened.append(str(path))
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", _counting_open)
    query = copy.deepcopy(creative_query)
    query["message"]["query_graph"]["nodes"]["SN"]["categories"] = ["biolink:Drug"]
    query["message"]["query_graph"]["nodes"]["ON"]["categories"] = ["biolink:Disease"]
    query["parameters"] = {}
    query["submitter"] = "test"

    first = bte_lookup.expand_bte_query(copy.deepcopy(query), logger)
    read_once = len(opened)
    for _ in range(3):
        again = bte_lookup.expand_bte_query(copy.deepcopy(query), logger)

    assert len(first) > 1 and again == first
    assert read_once > 1
    assert len(opened) == read_once

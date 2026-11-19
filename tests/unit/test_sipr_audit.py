"""SIPR neighborhood search, qnode ids, failures and timing (#215)."""

import asyncio
import json
import logging
import threading
import time

import httpx
import pytest

from shepherd_utils import reclaim
from shepherd_utils.query_errors import QueryError
from shepherd_utils.task_deadline import DEADLINE_FIELD
from workers.sipr import worker as sipr_worker

logger = logging.getLogger(__name__)


def _response(edges):
    """A retrieval response with ``edges`` as (subject, object) pairs."""
    nodes, kg_edges = {}, {}
    for i, (subject, obj) in enumerate(edges):
        nodes[subject] = {"categories": ["biolink:NamedThing"], "name": subject}
        nodes[obj] = {"categories": ["biolink:NamedThing"], "name": obj}
        kg_edges[f"e{i}"] = {
            "subject": subject,
            "object": obj,
            "predicate": "biolink:treats",
        }
    return {"message": {"knowledge_graph": {"nodes": nodes, "edges": kg_edges}}}


def _query_ids(query):
    return query["message"]["query_graph"]["nodes"]["n0"]["ids"]


# --- C9-1: every batch's neighborhood is searched --------------------------


async def test_the_next_hop_expands_candidates_from_every_batch(mocker):
    """150 inputs make two batches. Batch 1's curies all point at one hub; the
    hub must be expanded on the next hop. Each batch used to overwrite the
    candidates, so only the last batch's nodes went on."""
    inputs = [f"MONDO:{i:04d}" for i in range(150)]
    first, second = inputs[:100], inputs[100:]
    queried = []

    async def _retrieve(query, _logger, *args, **kwargs):
        ids = _query_ids(query)
        queried.append(list(ids))
        if ids == first:
            return _response([(curie, "HUB:1") for curie in ids])
        if ids == second:
            return _response([(curie, f"LEAF:{curie}") for curie in ids])
        return _response([])

    mocker.patch.object(sipr_worker, "run_trapi", side_effect=_retrieve)

    await sipr_worker.get_neighborhood(inputs, 2, logger)

    assert any("HUB:1" in ids for ids in queried[2:])


async def test_each_batch_is_personalized_on_its_own_curies(mocker):
    inputs = [f"MONDO:{i:04d}" for i in range(150)]
    targets = []
    real = sipr_worker.distribute_weights

    def _spy(responses, target_nodes, _logger):
        targets.append(list(target_nodes))
        return real(responses, target_nodes, _logger)

    async def _retrieve(query, _logger, *args, **kwargs):
        return _response([(curie, f"N:{curie}") for curie in _query_ids(query)])

    mocker.patch.object(sipr_worker, "distribute_weights", side_effect=_spy)
    mocker.patch.object(sipr_worker, "run_trapi", side_effect=_retrieve)

    await sipr_worker.get_neighborhood(inputs, 1, logger)

    assert sorted(len(t) for t in targets) == [50, 100]


# --- C9-2: failures are not empty successes --------------------------------


async def test_a_retrieval_failure_raises(mocker):
    mocker.patch.object(
        httpx.AsyncClient, "post", side_effect=httpx.ConnectError("retrieval down")
    )
    with pytest.raises(httpx.ConnectError):
        await sipr_worker.run_trapi(sipr_worker.write_trapi(["MONDO:1"], 0), logger)


def _task(deadline=None):
    fields = {
        "query_id": "qid",
        "response_id": "rid",
        "workflow": json.dumps([{"id": "sipr"}]),
        "log_level": "20",
        "otel": "{}",
    }
    if deadline is not None:
        fields[DEADLINE_FIELD] = f"{deadline:.3f}"
    return ["mid", fields]


def _set_query(set_node="SN", answer_node="ON", edge_id="e0"):
    return {
        "message": {
            "query_graph": {
                "nodes": {
                    set_node: {
                        "ids": ["MONDO:0001", "MONDO:0002"],
                        "set_interpretation": "MANY",
                    },
                    answer_node: {"categories": ["biolink:NamedThing"]},
                },
                "edges": {edge_id: {"subject": set_node, "object": answer_node}},
            }
        }
    }


@pytest.mark.parametrize(
    "query_graph",
    [
        # no set node
        {
            "nodes": {"a": {"ids": ["X:1"]}, "b": {}},
            "edges": {"e": {"subject": "a", "object": "b"}},
        },
        # more than one hop
        {
            "nodes": {
                "a": {"ids": ["X:1"], "set_interpretation": "MANY"},
                "b": {},
                "c": {},
            },
            "edges": {
                "e": {"subject": "a", "object": "b"},
                "f": {"subject": "b", "object": "c"},
            },
        },
    ],
)
async def test_a_query_sipr_does_not_do_fails_with_501(mocker, query_graph):
    mocker.patch.object(
        sipr_worker,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value={"message": {"query_graph": query_graph}},
    )
    save = mocker.patch.object(
        sipr_worker, "save_response", new_callable=mocker.AsyncMock
    )
    with pytest.raises(QueryError) as raised:
        await sipr_worker.sipr(_task(), logger)
    assert raised.value.status_code == 501
    save.assert_not_awaited()


# --- C9-3: qnode ids come from the query graph -----------------------------


async def test_results_bind_the_clients_own_qnode_ids(mocker):
    mocker.patch.object(
        sipr_worker,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value=_set_query("inputs", "answers", "related"),
    )
    mocker.patch.object(
        sipr_worker,
        "get_neighborhood",
        new_callable=mocker.AsyncMock,
        return_value=[
            _response([("MONDO:0001", "CHEBI:1"), ("MONDO:0002", "CHEBI:1")])
        ],
    )
    save = mocker.patch.object(
        sipr_worker, "save_response", new_callable=mocker.AsyncMock
    )

    await sipr_worker.sipr(_task(), logger)

    results = save.await_args.args[1]["message"]["results"]
    assert results
    for result in results:
        assert set(result["node_bindings"]) == {"inputs", "answers"}
        assert set(result["analyses"][0]["edge_bindings"]) == {"related"}


# --- C9-4: timing ------------------------------------------------------------


async def test_batches_run_concurrently(mocker):
    inputs = [f"MONDO:{i:04d}" for i in range(300)]
    running = 0
    peak = 0

    async def _retrieve(query, _logger, *args, **kwargs):
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.1)
        running -= 1
        return _response([])

    mocker.patch.object(sipr_worker, "run_trapi", side_effect=_retrieve)

    await sipr_worker.get_neighborhood(inputs, 1, logger)

    assert peak > 1


async def test_retrieval_timeouts_come_from_the_query_deadline(mocker):
    timeouts = []

    async def _retrieve(query, _logger, timeout):
        timeouts.append((timeout, query["parameters"]["timeout"]))
        return _response([])

    mocker.patch.object(sipr_worker, "run_trapi", side_effect=_retrieve)

    await sipr_worker.get_neighborhood(
        ["MONDO:1"], 1, logger, deadline=time.time() + 30
    )

    ((timeout, sent),) = timeouts
    assert 0 < timeout <= 30
    assert sent <= 30


async def test_a_query_past_its_deadline_is_not_retrieved(mocker):
    retrieve = mocker.patch.object(
        sipr_worker, "run_trapi", new_callable=mocker.AsyncMock
    )
    with pytest.raises(QueryError) as raised:
        await sipr_worker.get_neighborhood(
            ["MONDO:1"], 1, logger, deadline=time.time() - 1
        )
    assert raised.value.status_code == 504
    retrieve.assert_not_awaited()


async def test_pagerank_runs_off_the_event_loop(mocker):
    loop_thread = threading.get_ident()
    threads = []
    real = sipr_worker.distribute_weights

    def _spy(*args):
        threads.append(threading.get_ident())
        return real(*args)

    mocker.patch.object(sipr_worker, "distribute_weights", side_effect=_spy)
    mocker.patch.object(
        sipr_worker,
        "get_message",
        new_callable=mocker.AsyncMock,
        return_value=_set_query(),
    )
    mocker.patch.object(
        sipr_worker,
        "get_neighborhood",
        new_callable=mocker.AsyncMock,
        return_value=[_response([("MONDO:0001", "CHEBI:1")])],
    )
    mocker.patch.object(sipr_worker, "save_response", new_callable=mocker.AsyncMock)

    await sipr_worker.sipr(_task(), logger)

    assert threads and loop_thread not in threads


def test_sipr_has_a_reclaim_floor_above_the_query_budget():
    assert reclaim.min_idle_sec_for("sipr") > 300

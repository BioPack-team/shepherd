"""merge_message folds a pass once, and its output doesn't depend on batching
(#218).

ARAGORN's reference merge (``combine_messages``) takes every rule's results
at once: one inferred edge per answer, supported by every rule's aux graph.
The worker folds callbacks as they arrive instead, and every fold used to
re-merge the whole accumulated response and mint a fresh inferred edge per
answer, leaving the earlier ones bound too. An answer supported by k rule
callbacks ended up with up to k identical inferred edges, depending on how
callbacks happened to be batched.
"""

import copy
import logging

from shepherd_utils.db import get_message_sync, save_message_sync
from shepherd_utils.trapi import binding_ids
from workers.merge_message import worker as merge

logger = logging.getLogger(__name__)

TARGET = "aragorn"
SHEPHERD = f"infores:shepherd-{TARGET}"

ORIGINAL_QG = {
    "nodes": {
        "n0": {"ids": ["MONDO:1"]},
        "n1": {"categories": ["biolink:ChemicalEntity"]},
    },
    "edges": {
        "e0": {"subject": "n0", "object": "n1", "predicates": ["biolink:related_to"]},
    },
}


def _node(curie):
    return {"categories": ["biolink:NamedThing"], "name": curie}


def _edge(subject, obj):
    return {
        "subject": subject,
        "object": obj,
        "predicate": "biolink:related_to",
        "sources": [
            {"resource_id": "infores:kp", "resource_role": "primary_knowledge_source"}
        ],
    }


def _rule_callback(rule, answer, via):
    """A creative rule's callback: MONDO:1 -> via -> answer."""
    first, second = f"{rule}-a", f"{rule}-b"
    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"ids": ["MONDO:1"]},
                    "n1": {"categories": ["biolink:ChemicalEntity"]},
                    "x": {},
                },
                "edges": {
                    "r0": {"subject": "n0", "object": "x"},
                    "r1": {"subject": "x", "object": "n1"},
                },
            },
            "knowledge_graph": {
                "nodes": {c: _node(c) for c in ("MONDO:1", via, answer)},
                "edges": {
                    first: _edge("MONDO:1", via),
                    second: _edge(via, answer),
                },
            },
            "results": [
                {
                    "node_bindings": {
                        "n0": {"ids": ["MONDO:1"]},
                        "n1": {"ids": [answer]},
                        "x": {"ids": [via]},
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:retriever",
                            "edge_bindings": {
                                "r0": {"ids": [first]},
                                "r1": {"ids": [second]},
                            },
                        }
                    ],
                }
            ],
        }
    }


def _empty_response():
    return {
        "message": {
            "query_graph": copy.deepcopy(ORIGINAL_QG),
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "results": [],
        }
    }


def _inferred_edges(response, answer):
    """The Shepherd-inferred edges bound for ``answer``'s result."""
    kg_edges = response["message"]["knowledge_graph"]["edges"]
    (result,) = [
        r
        for r in response["message"]["results"]
        if binding_ids(r["node_bindings"]["n1"]) == [answer]
    ]
    bound = [
        edge_id
        for analysis in result["analyses"]
        for edge_id in binding_ids(analysis["edge_bindings"].get("e0", {}))
    ]
    return [
        kg_edges[edge_id]
        for edge_id in bound
        if any(s["resource_id"] == SHEPHERD for s in kg_edges[edge_id]["sources"])
    ]


def _support(edge):
    (attribute,) = [
        a
        for a in edge["attributes"]
        if a["attribute_type_id"] == "biolink:support_graphs"
    ]
    return attribute["value"]


def test_rule_callbacks_folded_one_at_a_time_share_one_inferred_edge():
    acc = _empty_response()
    for rule, via in (("rule1", "G:1"), ("rule2", "G:2")):
        acc = merge.merge_messages(
            TARGET, ORIGINAL_QG, acc, _rule_callback(rule, "CHEBI:X", via), logger
        )

    (inferred,) = _inferred_edges(acc, "CHEBI:X")
    assert len(_support(inferred)) == 2
    aux = acc["message"]["auxiliary_graphs"]
    supported = {frozenset(aux[a]["edges"]) for a in _support(inferred)}
    assert supported == {
        frozenset({"rule1-a", "rule1-b"}),
        frozenset({"rule2-a", "rule2-b"}),
    }


def test_the_same_rule_result_twice_adds_no_second_support_graph():
    acc = _empty_response()
    for _ in range(2):
        acc = merge.merge_messages(
            TARGET, ORIGINAL_QG, acc, _rule_callback("rule1", "CHEBI:X", "G:1"), logger
        )

    (inferred,) = _inferred_edges(acc, "CHEBI:X")
    assert len(_support(inferred)) == 1


def _patch_sync_store(mocker):
    storage = {}
    client = mocker.Mock()
    client.set.side_effect = lambda key, blob, ex=None: storage.__setitem__(key, blob)
    client.get.side_effect = lambda key: storage.get(key)
    mocker.patch("shepherd_utils.db._get_sync_data_db", return_value=client)


def _shape(response):
    """Batching-independent shape: per answer, its inferred-edge count and the
    edge sets of their support graphs."""
    aux = response["message"].get("auxiliary_graphs") or {}
    shape = {}
    for result in response["message"]["results"]:
        (answer,) = binding_ids(result["node_bindings"]["n1"])
        inferred = _inferred_edges(response, answer)
        shape[answer] = (
            len(inferred),
            sorted(
                sorted(aux[a]["edges"]) for edge in inferred for a in _support(edge)
            ),
        )
    return shape


def _fold(mocker, batches):
    _patch_sync_store(mocker)
    save_message_sync("qid", {"message": {"query_graph": copy.deepcopy(ORIGINAL_QG)}})
    save_message_sync("rid", _empty_response())
    callbacks = {
        "c1": _rule_callback("rule1", "CHEBI:X", "G:1"),
        "c2": _rule_callback("rule2", "CHEBI:X", "G:2"),
        "c3": _rule_callback("rule3", "CHEBI:Y", "G:3"),
    }
    for callback_id, callback in callbacks.items():
        save_message_sync(callback_id, copy.deepcopy(callback))
    for batch in batches:
        merge.merge_messages_by_ids(TARGET, "qid", "rid", batch)
    return get_message_sync("rid")


def test_the_merged_answer_does_not_depend_on_batching(mocker):
    one_pass = _fold(mocker, [["c1", "c2", "c3"]])
    one_by_one = _fold(mocker, [["c1"], ["c2"], ["c3"]])

    assert _shape(one_pass) == _shape(one_by_one)
    assert _shape(one_pass)["CHEBI:X"][0] == 1


def test_a_pass_groups_and_merges_results_once(mocker):
    """C4-4: every fold used to rebuild the knowledge graph and regroup every
    accumulated result; a pass now does that once, whatever its size."""
    grouped = mocker.spy(merge, "group_results_by_qnode")

    _fold(mocker, [["c1", "c2", "c3"]])

    assert grouped.call_count == 1


# --- no idle wait before the first pass ---------------------------------------


async def test_a_pass_starts_at_once_and_late_callbacks_join_the_next(
    redis_mock, monkeypatch
):
    """The lock holder merges what is ready straight away instead of waiting
    for more callbacks; one that lands during a pass is folded by the next."""
    import asyncio
    from unittest.mock import AsyncMock

    from shepherd_utils import db

    response_id = "resp-eager"
    await redis_mock["data"].set("q-eager", b"x")
    await redis_mock["data"].set(response_id, b"x")
    await db.add_ready_callback(response_id, "cb-1", logger)

    lock = redis_mock["lock"]

    async def _remove_lock(key, consumer, _logger):
        if await lock.get(key) == consumer:
            await lock.delete(key)

    async def _refresh_lock(key, consumer, ttl_ms, _logger):
        return await lock.get(key) == consumer

    monkeypatch.setattr(merge, "remove_lock", _remove_lock)
    monkeypatch.setattr(merge, "refresh_lock", _refresh_lock)
    monkeypatch.setattr(merge, "remove_callback_id", AsyncMock())
    monkeypatch.setattr(merge, "resolve_pool_workers", lambda limit, lg: 1)
    monkeypatch.setattr(
        merge,
        "get_existing_callback_ids",
        AsyncMock(return_value={"cb-1", "cb-2"}),
    )
    passes = []
    started = []
    done = asyncio.get_running_loop().create_future()
    loop = asyncio.get_running_loop()

    class _Pool:
        def __init__(self, *args, **kwargs):
            pass

        async def run(self, loop_, fn, target, query_id, rid, ready, level):
            started.append(loop.time())
            passes.append(list(ready))
            if len(passes) == 1:
                # A callback arrives while the first pass is running.
                await db.add_ready_callback(response_id, "cb-2", logger)
            return list(ready), []

        def shutdown(self):
            pass

    class _Limiter:
        def release(self):
            if not done.done():
                done.set_result(None)

    async def _one_task(stream, group, consumer, limit):
        yield (
            "1-0",
            {
                "query_id": "q-eager",
                "response_id": response_id,
                "callback_id": "cb-1",
                "target": "aragorn",
                "log_level": "20",
                "otel": "{}",
                "metadata": "{}",
            },
        ), None, logging.getLogger("shepherd.test.eager"), _Limiter()
        await asyncio.sleep(3600)

    from shepherd_utils import shared

    monkeypatch.setattr(shared, "get_tasks", _one_task)
    monkeypatch.setattr(merge, "ProcessPoolManager", _Pool)
    t0 = loop.time()
    poller = asyncio.create_task(merge.poll_for_tasks())
    try:
        await asyncio.wait_for(done, timeout=10)
    finally:
        poller.cancel()
        try:
            await poller
        except BaseException:
            pass

    assert passes == [["cb-1"], ["cb-2"]]
    # No debounce: the first pass starts well inside the old 1s wait.
    assert started[0] - t0 < 0.5


# --- C4-8 ----------------------------------------------------------------------


async def test_batch_sizes_are_probed_in_one_round_trip(redis_mock, monkeypatch):
    from shepherd_utils import db
    from shepherd_utils.config import settings

    monkeypatch.setattr(settings, "max_response_size", "1G")
    for key in ("acc", "c1", "c2", "c3"):
        await db.save_message(key, {"message": {"pad": key * 100}}, logger)
    calls = []
    real = redis_mock["data"].getrange

    async def _counted(*args, **kwargs):
        calls.append(args[0])
        return await real(*args, **kwargs)

    monkeypatch.setattr(redis_mock["data"], "getrange", _counted)

    batch, reason = await merge._fit_batch_to_budget("acc", ["c1", "c2", "c3"], logger)

    assert (batch, reason) == (["c1", "c2", "c3"], None)
    # only the accumulator's own probe goes out on its own
    assert calls == ["acc"]


def test_promiscuous_results_are_dropped_and_the_rest_kept_in_order():
    results = [
        {"node_bindings": {"c": {"ids": ["HUB"]}}, "i": i} for i in range(12)
    ] + [{"node_bindings": {"c": {"ids": [f"N{i}"]}}, "i": 100 + i} for i in range(3)]
    response = {"message": {"results": results}}

    merge.remove_promiscuous_knode_results(10, "c", response)

    assert [r["i"] for r in response["message"]["results"]] == [100, 101, 102]

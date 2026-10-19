"""Tests for aragorn.pathfinder's path finding and building.

``search_paths`` is checked against a brute-force enumeration over small
graphs written as the same CSR the worker memory-maps; ``edge_lookups`` and
``build_paths`` are pure; ``build_paths_task`` and an on-backbone
``shadowfax`` run with Redis and the outbound calls mocked.
"""

import itertools
import json
import logging
import random
from math import inf, isclose

import numpy as np
import pytest
from translator_tom.v2_0 import AsyncQuery

from shepherd_utils import data_download
from shepherd_utils.config import settings
from shepherd_utils.data_download import ensure_pathfinder_search_graph
from workers.aragorn_pathfinder import worker
from workers.aragorn_pathfinder.worker import (
    build_paths,
    build_paths_task,
    cheapest_legs,
    edge_lookups,
    load_search_graph,
    one_hop,
    search_paths,
    shadowfax,
)

logger = logging.getLogger(__name__)


# --- helpers -----------------------------------------------------------------


def write_graph(directory, num_nodes, edges):
    """Write an undirected weighted graph as the worker's CSR .npy files and
    return the adjacency it encodes."""
    adjacency = {node: {} for node in range(num_nodes)}
    for u, v, w in edges:
        adjacency[u][v] = w
        adjacency[v][u] = w
    offsets, neighbors, weights = [0], [], []
    for node in range(num_nodes):
        for other, w in sorted(adjacency[node].items()):
            neighbors.append(other)
            weights.append(w)
        offsets.append(len(neighbors))
    directory.mkdir(parents=True, exist_ok=True)
    np.save(directory / "offsets.npy", np.array(offsets, dtype=np.int64))
    np.save(directory / "neighbors.npy", np.array(neighbors, dtype=np.int32))
    np.save(directory / "weights.npy", np.array(weights, dtype=np.float32))
    return adjacency


def random_graph(directory, num_nodes=30, num_edges=110, seed=0):
    rng = random.Random(seed)
    edges = []
    for _ in range(num_edges):
        u, v = rng.sample(range(num_nodes), 2)
        edges.append((u, v, rng.random()))
    return write_graph(directory, num_nodes, edges)


def weight(adjacency, u, v):
    return float(np.float32(adjacency[u][v]))


def path_cost(adjacency, path):
    return sum(weight(adjacency, u, v) for u, v in zip(path, path[1:]))


def centres(path):
    """The nodes a path is centred on: its middle, or both intermediates of a
    three-hop path."""
    if len(path) == 4:
        return [path[1], path[2]]
    return [path[len(path) // 2]]


def brute_force(adjacency, source, target):
    """Every centre's cheapest loopless path of two to four hops."""
    best = {}

    def walk(path):
        if path[-1] == target:
            if len(path) >= 3:
                cost = path_cost(adjacency, path)
                for centre in centres(path):
                    if centre not in best or cost < best[centre]:
                        best[centre] = cost
            return
        if len(path) == 5:
            return
        for node in adjacency[path[-1]]:
            if node not in path:
                walk(path + [node])

    walk([source])
    return best


@pytest.fixture
def graph_dir(tmp_path):
    yield tmp_path / "graph"
    worker._search_graphs.clear()


# --- search ------------------------------------------------------------------


def test_load_search_graph_maps_the_csr_once(graph_dir):
    write_graph(graph_dir, 3, [(0, 1, 0.5), (1, 2, 0.25)])

    graph = load_search_graph(str(graph_dir))

    assert load_search_graph(str(graph_dir)) is graph
    offsets, neighbors, weights = graph
    assert offsets.tolist() == [0, 1, 3, 4]
    assert neighbors.tolist() == [1, 0, 2, 1]
    assert isinstance(weights, np.memmap)


def test_cheapest_legs(graph_dir):
    #   0 -1- 1 -1- 3
    #   0 -2- 2 -2- 3      0 -9- 4 (the far end)
    #   1 -5- 2            3 -1- 4
    write_graph(
        graph_dir,
        5,
        [(0, 1, 1), (1, 3, 1), (0, 2, 2), (2, 3, 2), (1, 2, 5), (0, 4, 9), (3, 4, 1)],
    )
    one, two, via = cheapest_legs(load_search_graph(str(graph_dir)), 0, 4)

    # one-hop legs: never to the start or the far end
    assert one.tolist() == [inf, 1, 2, inf, inf]
    # two-hop legs to 3: through 1 (cost 2) then through 2 (cost 4)
    assert two[0][3] == 2 and via[0][3] == 1
    assert two[1][3] == 4 and via[1][3] == 2
    # 0 -> 1 -> 2 (cost 6) and 0 -> 2 -> 1 (cost 7)
    assert two[0][2] == 6 and via[0][2] == 1
    assert two[0][1] == 7 and via[0][1] == 2
    # no leg passes through or ends at the far end, or returns to the start
    assert two[0][4] == inf and two[0][0] == inf
    assert via[1][1] == -1


def test_search_paths_matches_brute_force(graph_dir):
    adjacency = random_graph(graph_dir)

    checked = 0
    for source, target in itertools.permutations(range(8), 2):
        paths, _ = search_paths(str(graph_dir), source, target, 10**6)
        expected = brute_force(adjacency, source, target)

        found = {}
        for path in paths:
            assert path[0] == source and path[-1] == target
            assert 3 <= len(path) <= 5
            assert len(set(path)) == len(path), f"{path} has a loop"
            assert all(v in adjacency[u] for u, v in zip(path, path[1:]))
            for centre in centres(path):
                found.setdefault(centre, path_cost(adjacency, path))
        # every centre a loopless path has, at the cheapest such path's cost
        assert found.keys() == expected.keys()
        for centre, cost in expected.items():
            assert isclose(found[centre], cost, rel_tol=1e-6), centre
        checked += len(paths)
    assert checked > 100


def test_search_paths_come_out_cheapest_first_and_once(graph_dir):
    adjacency = random_graph(graph_dir, seed=1)

    paths, middles = search_paths(str(graph_dir), 0, 1, 10**6)

    costs = [path_cost(adjacency, path) for path in paths]
    assert all(a <= b + 1e-6 for a, b in zip(costs, costs[1:]))
    assert len(set(paths)) == len(paths)
    assert middles >= len(paths)


def test_search_paths_stops_at_max_paths(graph_dir):
    random_graph(graph_dir, seed=2)

    everything, _ = search_paths(str(graph_dir), 0, 1, 10**6)
    first, _ = search_paths(str(graph_dir), 0, 1, 5)

    assert len(everything) > 5
    assert first == everything[:5]


def test_search_paths_between_unconnected_nodes_is_empty(graph_dir):
    write_graph(graph_dir, 4, [(0, 1, 1.0), (2, 3, 1.0)])

    assert search_paths(str(graph_dir), 0, 3, 10)[0] == []


def test_search_paths_skips_unusable_edges(graph_dir):
    # the only route through 2 has an infinite weight
    write_graph(graph_dir, 4, [(0, 1, 1), (1, 3, 1), (0, 2, inf), (2, 3, 1)])

    paths, _ = search_paths(str(graph_dir), 0, 3, 10)

    assert paths == [(0, 1, 3)]


def test_search_paths_gives_up_at_the_deadline(graph_dir, mocker):
    random_graph(graph_dir, seed=3)
    mocker.patch.object(worker, "SEARCH_BUDGET_SEC", -1.0)

    paths, middles = search_paths(str(graph_dir), 0, 1, 10)

    assert paths == []
    assert middles > 0


# --- lookups -----------------------------------------------------------------


def query_ids(lookup):
    nodes = lookup["message"]["query_graph"]["nodes"]
    return nodes["n0"].get("ids"), nodes["n1"].get("ids")


def test_one_hop():
    lookup = one_hop(["A"], None, {"timeout": 5}, True)

    assert lookup == {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": ["A"]}, "n1": {}},
                "edges": {
                    "e0": {
                        "subject": "n0",
                        "object": "n1",
                        "predicates": ["biolink:related_to"],
                    }
                },
            }
        },
        "parameters": {"timeout": 5, "subclass": True},
    }


def test_edge_lookups_without_paths_is_the_direct_edge():
    lookups = edge_lookups([], "S", "T", {"timeout": 5})

    assert len(lookups) == 1
    assert query_ids(lookups[0]) == (["S"], ["T"])
    assert lookups[0]["parameters"] == {"timeout": 5, "subclass": True}


def test_edge_lookups_cover_every_hop():
    paths = [
        ["S", "M", "T"],
        ["S", "A", "B", "T"],
        ["S", "A", "M", "C", "T"],
        ["S", "D", "M", "C", "T"],
    ]

    lookups = edge_lookups(paths, "S", "T", {"timeout": 5})

    # the endpoints are expanded to their subclasses, against anything
    assert [query_ids(lookup) for lookup in lookups[:3]] == [
        (["S"], ["T"]),
        (["S"], None),
        (None, ["T"]),
    ]
    assert all(lookup["parameters"]["subclass"] for lookup in lookups[:3])
    # the hops between intermediates are one exact lookup
    assert len(lookups) == 4
    near, far = query_ids(lookups[3])
    assert near == ["A", "C", "D"]
    assert far == ["B", "M"]
    # held to the hops the paths take, not every near and far combination
    assert lookups[3]["parameters"] == {
        "timeout": 5,
        "subclass": False,
        "pairs": [["A", "B"], ["A", "M"], ["C", "M"], ["D", "M"]],
    }
    for u, v in lookups[3]["parameters"]["pairs"]:
        assert u in near and v in far
    # the endpoint lookups are not restricted to pairs
    assert all("pairs" not in lookup["parameters"] for lookup in lookups[:3])


def test_edge_lookups_for_two_hop_paths_need_no_interior_lookup():
    lookups = edge_lookups([["S", "M", "T"], ["S", "N", "T"]], "S", "T", {})

    assert [query_ids(lookup) for lookup in lookups] == [
        (["S"], ["T"]),
        (["S"], None),
        (None, ["T"]),
    ]


# --- building paths ----------------------------------------------------------


def edge(subject, obj, predicate="biolink:related_to"):
    return {"subject": subject, "object": obj, "predicate": predicate}


def node(*categories):
    return {"categories": list(categories)}


def make_plan(node_paths, category=None):
    return {
        "source": "S",
        "target": "T",
        "node_paths": node_paths,
        "path_key": "p0",
        "subject_key": "n0",
        "object_key": "n1",
        "intermediate_category": category,
    }


def make_message():
    return {
        "query_graph": {},
        "knowledge_graph": {
            "nodes": {
                "S": node("biolink:Disease"),
                "T": node("biolink:Disease"),
                "A": node("biolink:Gene"),
                "B": node("biolink:ChemicalEntity"),
                "M": node("biolink:Gene"),
            },
            "edges": {
                "st": edge("S", "T"),
                "sa": edge("S", "A"),
                "as": edge("A", "S", "biolink:affects"),
                "ab": edge("B", "A"),
                "bt": edge("B", "T"),
                "sm": edge("S", "M"),
                "m_sub": edge("M", "T", "biolink:subclass_of"),
                "sb": edge("S", "B"),
            },
        },
        "auxiliary_graphs": {"support": {"edges": ["sa"]}},
        "results": [{"stale": True}],
    }


def bound_paths(message):
    """{aux id: sorted edge ids} for each path analysis."""
    (result,) = message["results"]
    out = {}
    for analysis in result["analyses"]:
        assert analysis["resource_id"] == "infores:shepherd-aragorn"
        (aux_id,) = analysis["path_bindings"]["p0"]["ids"]
        out[aux_id] = sorted(message["auxiliary_graphs"][aux_id]["edges"])
    return out


def test_build_paths():
    message = make_message()
    plan = make_plan([("S", "A", "B", "T"), ("S", "M", "T"), ("S", "B", "T")])

    build_paths(message, plan, logger)

    (result,) = message["results"]
    assert result["node_bindings"] == {
        "n0": {"ids": ["S"]},
        "n1": {"ids": ["T"]},
    }
    # the direct edge first, then every edge of each hop, either way round; the
    # path through M has only a subclass_of edge to T and is dropped
    assert bound_paths(message) == {
        "p_0": ["st"],
        "p_1": ["ab", "as", "bt", "sa"],
        "p_3": ["bt", "sb"],
    }
    # the lookups' own aux graphs stay
    assert message["auxiliary_graphs"]["support"] == {"edges": ["sa"]}


def test_build_paths_keeps_one_path_per_set_of_intermediates():
    message = make_message()

    build_paths(message, make_plan([("S", "B", "T"), ("S", "B", "T")]), logger)

    assert list(bound_paths(message)) == ["p_0", "p_1"]


def test_build_paths_without_a_direct_edge():
    message = make_message()
    del message["knowledge_graph"]["edges"]["st"]

    build_paths(message, make_plan([("S", "B", "T")]), logger)

    assert bound_paths(message) == {"p_1": ["bt", "sb"]}


@pytest.mark.parametrize(
    "category, expected",
    [
        ("biolink:Gene", ["p_1"]),
        ("biolink:ChemicalEntity", ["p_1", "p_2"]),
        ("biolink:NamedThing", ["p_0", "p_1", "p_2"]),
        (None, ["p_0", "p_1", "p_2"]),
    ],
)
def test_build_paths_requires_the_intermediate_category(category, expected):
    message = make_message()
    plan = make_plan([("S", "A", "B", "T"), ("S", "B", "T")], category)

    build_paths(message, plan, logger)

    assert list(bound_paths(message)) == expected


def test_build_paths_with_nothing_to_bind():
    message = make_message()
    message["knowledge_graph"]["edges"] = {}

    build_paths(message, make_plan([("S", "A", "B", "T")]), logger)

    assert message["results"] == []
    assert message["auxiliary_graphs"] == {"support": {"edges": ["sa"]}}


def test_build_paths_on_an_empty_message():
    message = {}

    build_paths(message, make_plan([("S", "A", "T")]), logger)

    assert message == {"knowledge_graph": {"nodes": {}, "edges": {}}, "results": []}


def test_build_paths_task_saves_the_paths_without_orphans(mocker):
    stored = {"message": make_message()}
    stored["message"]["auxiliary_graphs"] = {}
    mocker.patch.object(worker, "get_message_sync", return_value=stored)
    save = mocker.patch.object(worker, "save_response_sync")

    entries = build_paths_task("rid", make_plan([("S", "B", "T")]), logging.INFO)

    save.assert_called_once()
    response_id, response = save.call_args.args
    assert response_id == "rid"
    assert bound_paths(response["message"]) == {"p_0": ["st"], "p_1": ["bt", "sb"]}
    # edges and nodes no path uses are gone
    assert set(response["message"]["knowledge_graph"]["edges"]) == {"st", "sb", "bt"}
    assert set(response["message"]["knowledge_graph"]["nodes"]) == {"S", "B", "T"}
    assert any("Built 2 paths" in json.dumps(entry) for entry in entries)


def test_build_paths_takes_an_inferred_endpoint_edge_and_keeps_its_support(mocker):
    """Gandalf expands an endpoint to its subclass children and returns a
    child's edge as an inferred edge on the endpoint, supported by the child's
    own edge and its subclass_of edge. The inferred edge is the hop; the
    child's edge is not, and the support survives the orphan filter."""
    message = {
        "knowledge_graph": {
            "nodes": {
                "S": node("biolink:Disease"),
                "C": node("biolink:Disease"),
                "M": node("biolink:Gene"),
                "T": node("biolink:Disease"),
            },
            "edges": {
                "cm": edge("C", "M", "biolink:affects"),
                "cs": edge("C", "S", "biolink:subclass_of"),
                "cm_cs": {
                    **edge("S", "M", "biolink:affects"),
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:support_graphs",
                            "value": ["support_cm_cs"],
                        }
                    ],
                },
                "mt": edge("M", "T"),
            },
        },
        "auxiliary_graphs": {"support_cm_cs": {"edges": ["cm", "cs"]}},
        "results": [],
    }
    mocker.patch.object(worker, "get_message_sync", return_value={"message": message})
    save = mocker.patch.object(worker, "save_response_sync")

    build_paths_task("rid", make_plan([("S", "M", "T")]), logging.INFO)

    saved = save.call_args.args[1]["message"]
    assert bound_paths(saved) == {"p_1": ["cm_cs", "mt"]}
    assert saved["auxiliary_graphs"]["support_cm_cs"] == {"edges": ["cm", "cs"]}
    assert set(saved["knowledge_graph"]["edges"]) == {"cm_cs", "mt", "cm", "cs"}
    assert set(saved["knowledge_graph"]["nodes"]) == {"S", "C", "M", "T"}


def test_build_paths_says_where_the_dropped_paths_lack_a_hop(caplog):
    """A path whose hop came back with no edges is dropped, counted by where
    the hop is, and the first such hop is named with whether its nodes came
    back at all."""
    message = make_message()
    plan = make_plan(
        [
            ("S", "X", "T"),  # no S - X edge: at the source
            ("S", "A", "X", "T"),  # no A - X edge: between intermediates
            ("S", "M", "T"),  # M - T only subclass_of: at the target
        ]
    )

    with caplog.at_level(logging.INFO):
        build_paths(message, plan, logger)

    assert bound_paths(message) == {"p_0": ["st"]}
    assert (
        "dropped 3 with a hop that has no edges {'at the source': 1, "
        "'between intermediates': 1, 'at the target': 1}"
    ) in caplog.text
    assert "First hop with no edges: S (in the edges) - X (not in the edges)" in (
        caplog.text
    )


# --- shadowfax over the backbone ---------------------------------------------


class InlinePool:
    """Runs the search for real and records the path build it is handed,
    running that too when asked."""

    def __init__(self, build=False):
        self.build = build
        self.builds = []

    async def run(self, loop, fn, *args):
        if fn is search_paths:
            return fn(*args)
        self.builds.append(args)
        return fn(*args) if self.build else []


def backbone_query(constraints=None):
    query = {
        "message": {
            "query_graph": {
                "nodes": {"n0": {"ids": ["X:S"]}, "n1": {"ids": ["X:T"]}},
                "paths": {"p0": {"subject": "n0", "object": "n1"}},
            }
        },
        "parameters": {"timeout": 5},
    }
    if constraints is not None:
        query["message"]["query_graph"]["paths"]["p0"]["constraints"] = constraints
    return query


def backbone_merged_message():
    """The response once every lookup over the backbone is merged: A is the
    only gene, and there is no direct edge."""
    return {
        "knowledge_graph": {
            "nodes": {
                "X:S": node("biolink:Disease"),
                "X:A": node("biolink:Gene"),
                "X:B": node("biolink:ChemicalEntity"),
                "X:M": node("biolink:Disease"),
                "X:T": node("biolink:Disease"),
            },
            "edges": {
                "sa": edge("X:S", "X:A", "biolink:affects"),
                "ab": edge("X:A", "X:B"),
                "bt": edge("X:B", "X:T"),
                "sm": edge("X:S", "X:M"),
                "mt": edge("X:M", "X:T"),
            },
        },
        "results": [],
    }


async def run_on_backbone(mocker, graph_dir, query, pool):
    """``shadowfax`` on ``query`` over the backbone S - A - B - T, S - M - T,
    with Redis and the outbound calls mocked. Returns its metadata, the
    lookups it sent, the callback registrations and the response it saved
    (None unless the pool runs the build)."""
    write_graph(
        graph_dir,
        5,
        [(0, 1, 1), (1, 2, 1), (2, 3, 1), (0, 4, 1), (4, 3, 1)],
    )
    curies = ["X:S", "X:A", "X:B", "X:T", "X:M"]
    mocker.patch.object(worker, "SEARCH_GRAPH_DIR", str(graph_dir))
    mocker.patch.object(worker, "NODE_CURIES", curies)
    mocker.patch.object(worker, "NODE_INDEX", {c: i for i, c in enumerate(curies)})
    mocker.patch.object(worker, "POOL", pool)
    mocker.patch.object(
        worker, "get_message", new_callable=mocker.AsyncMock, return_value=query
    )
    add_callback = mocker.patch.object(
        worker, "add_callback_id", new_callable=mocker.AsyncMock
    )
    mocker.patch.object(
        worker,
        "get_running_callbacks",
        new_callable=mocker.AsyncMock,
        return_value=[],
    )
    mocker.patch.object(
        worker,
        "get_message_sync",
        return_value={"message": backbone_merged_message()},
    )
    save = mocker.patch.object(worker, "save_response_sync")
    post = mocker.patch(
        "httpx.AsyncClient.post",
        new_callable=mocker.AsyncMock,
        return_value=mocker.Mock(status_code=200),
    )
    task = [
        "1-0",
        {
            "query_id": "qid",
            "response_id": "rid",
            "workflow": "[]",
            "log_level": "20",
            "otel": "{}",
            "metadata": "{}",
        },
    ]

    metadata = await shadowfax(task, logger)

    lookups = [call.kwargs["json"] for call in post.call_args_list]
    saved = save.call_args.args[1]["message"] if save.called else None
    return metadata, lookups, add_callback, saved


async def test_shadowfax_searches_the_backbone(redis_mock, mocker, graph_dir):
    pool = InlinePool()

    metadata, lookups, _, _ = await run_on_backbone(
        mocker, graph_dir, backbone_query(), pool
    )

    assert json.loads(metadata) == {"rehydrate": True}
    sent = [query_ids(lookup) for lookup in lookups]
    assert sorted(sent, key=str) == sorted(
        [
            (["X:S"], ["X:T"]),
            (["X:S"], None),
            (None, ["X:T"]),
            (["X:A"], ["X:B"]),
        ],
        key=str,
    )
    (build,) = pool.builds
    response_id, plan, _ = build
    assert response_id == "rid"
    assert sorted(plan["node_paths"]) == [
        ["X:S", "X:A", "X:B", "X:T"],
        ["X:S", "X:M", "X:T"],
    ]
    assert plan["source"] == "X:S" and plan["target"] == "X:T"


async def test_shadowfax_on_the_backbone_sends_valid_lookups_and_builds_the_paths(
    redis_mock, mocker, graph_dir
):
    """Every lookup a query on the backbone sends -- the open endpoint lookups
    and the interior one pinning many ids -- is a valid TRAPI 2.0 query with
    the worker's parameters and its own callback, and once they are merged
    the paths are built and saved."""
    metadata, lookups, add_callback, saved = await run_on_backbone(
        mocker, graph_dir, backbone_query(), InlinePool(build=True)
    )

    assert json.loads(metadata) == {"rehydrate": True}
    assert len(lookups) == 4
    for lookup in lookups:
        AsyncQuery.from_dict(lookup)
        assert "log_level" not in lookup
        parameters = {
            k: v
            for k, v in lookup["parameters"].items()
            if k not in ("subclass", "pairs")
        }
        assert parameters == {
            "timeout": 5,
            "tiers": [settings.default_data_tier],
            "tier_fallback": False,
            "dehydrated": True,
            "kg_only": True,
        }
    assert [
        lookup["parameters"]["pairs"]
        for lookup in lookups
        if "pairs" in lookup["parameters"]
    ] == [[["X:A", "X:B"]]]
    assert add_callback.await_count == len(lookups)
    callbacks = {lookup["callback"] for lookup in lookups}
    assert len(callbacks) == len(lookups)
    assert sorted(bound_paths(saved).values()) == [
        ["ab", "bt", "sa"],
        ["mt", "sm"],
    ]


async def test_shadowfax_keeps_to_the_intermediate_category(
    redis_mock, mocker, graph_dir
):
    """A required_intermediate_categories constraint reaches the path build,
    and only paths through a node of that category are kept."""
    pool = InlinePool(build=True)
    query = backbone_query(
        constraints=[{"required_intermediate_categories": ["biolink:Gene"]}]
    )

    _, lookups, _, saved = await run_on_backbone(mocker, graph_dir, query, pool)

    for lookup in lookups:
        AsyncQuery.from_dict(lookup)
    (build,) = pool.builds
    assert build[1]["intermediate_category"] == "biolink:Gene"
    # the path through A, the gene; not the one through M
    assert list(bound_paths(saved).values()) == [["ab", "bt", "sa"]]


# --- the search graph download ----------------------------------------------


def test_ensure_pathfinder_search_graph_wires_settings(tmp_path, mocker):
    ensure = mocker.patch.object(data_download, "ensure_lmdb_dataset")
    mocker.patch.object(
        data_download.settings, "pathfinder_search_graph_dir", str(tmp_path)
    )
    mocker.patch.object(
        data_download.settings, "pathfinder_search_graph_url", "http://x/g.tar.gz"
    )

    ensure_pathfinder_search_graph(logger)

    ensure.assert_called_once_with(
        name="aragorn_pathfinder",
        target_dir=str(tmp_path),
        required_files=["offsets.npy", "neighbors.npy", "weights.npy", "nodes.txt"],
        url="http://x/g.tar.gz",
        logger=logger,
    )

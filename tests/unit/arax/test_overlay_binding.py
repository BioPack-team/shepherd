"""Binding overlay edges to results in one pass (D-28) binds exactly what one
call per edge did.

fisher_exact_test and overlay_clinical_info used to call
update_results_with_overlay_edge once per new virtual edge, each call walking
every result. They now collect their edges and call
update_results_with_overlay_edge_list once. These tests compare the two on the
same messages, down to the order of each edge binding's ids, including edges
that share a node pair.

TRAPI 2.0: an analysis has one EdgeBinding per qedge, so an overlay edge's key
is unioned into that binding's ids (1.x appended an EdgeBinding to the qedge's
list); the tests at the end pin that.
"""

import copy
import random
import time

import pytest

from shepherd_utils.arax.Overlay import overlay_utilities as ou
from shepherd_utils.arax.openapi_server.models.analysis import Analysis
from shepherd_utils.arax.openapi_server.models.edge_binding import EdgeBinding
from shepherd_utils.arax.openapi_server.models.message import Message
from shepherd_utils.arax.openapi_server.models.node_binding import NodeBinding
from shepherd_utils.arax.openapi_server.models.q_edge import QEdge
from shepherd_utils.arax.openapi_server.models.q_node import QNode
from shepherd_utils.arax.openapi_server.models.query_graph import QueryGraph
from shepherd_utils.arax.openapi_server.models.result import Result
from translator_tom import Result as Result_2_0


class Log:
    def __init__(self):
        self.lines = []

    def warning(self, message, **_):
        self.lines.append(("WARNING", message))

    def error(self, message, **_):
        self.lines.append(("ERROR", message))

    def debug(self, *_, **__):
        pass

    def info(self, *_, **__):
        pass


def make_message(rng, n_results, n_genes, n_diseases):
    qg = QueryGraph(
        nodes={key: QNode() for key in ("n0", "n1", "n2")},
        edges={
            "e0": QEdge(subject="n0", object="n1"),
            "e1": QEdge(subject="n1", object="n2"),
            "F1": QEdge(subject="n1", object="n2"),
        },
    )
    results = []
    for i in range(n_results):
        genes = rng.sample(range(n_genes), rng.randint(1, 3))
        diseases = rng.sample(range(n_diseases), rng.randint(1, 2))
        analyses = [
            Analysis(
                resource_id=resource_id,
                edge_bindings={
                    "e0": EdgeBinding(ids=[f"a{i}"]),
                    "e1": EdgeBinding(ids=[f"b{i}"]),
                },
            )
            for resource_id in ("infores:arax", "infores:other")
        ]
        results.append(
            Result(
                node_bindings={
                    "n0": NodeBinding(ids=["C:1"]),
                    "n1": NodeBinding(ids=[f"G:{g}" for g in genes]),
                    "n2": NodeBinding(ids=[f"D:{d}" for d in diseases]),
                },
                analyses=analyses,
            )
        )
    return Message(query_graph=qg, results=results)


def bindings(message):
    return [
        [
            (analysis.resource_id, qedge_key, list(edge_binding.ids))
            for analysis in result.analyses
            for qedge_key, edge_binding in analysis.edge_bindings.items()
        ]
        for result in message.results
    ]


def overlay_edges(rng, n_edges, n_genes, n_diseases, repeat_pairs):
    kedges = []
    for k in range(n_edges):
        if repeat_pairs and kedges and rng.random() < 0.2:
            pair = rng.choice(kedges)[0]  # a second edge for a pair already seen
        else:
            pair = (f"G:{rng.randrange(n_genes)}", f"D:{rng.randrange(n_diseases)}")
        kedges.append((pair, f"F1_{k}"))
    # also a node pair the other way round, and curies no result has
    kedges.append((("D:0", "G:0"), "F1_reversed"))
    kedges.append((("G:missing", "D:0"), "F1_unbound"))
    return kedges


@pytest.mark.parametrize("repeat_pairs", [False, True])
@pytest.mark.parametrize("seed", range(5))
def test_list_binds_what_one_call_per_edge_did(seed, repeat_pairs):
    rng = random.Random(seed)
    message = make_message(rng, n_results=300, n_genes=40, n_diseases=15)
    kedges = overlay_edges(rng, 400, 40, 15, repeat_pairs)

    one_by_one = copy.deepcopy(message)
    log_one_by_one = Log()
    for (subject_key, object_key), kedge_key in kedges:
        ou.update_results_with_overlay_edge(
            subject_knode_key=subject_key,
            object_knode_key=object_key,
            kedge_key=kedge_key,
            message=one_by_one,
            log=log_one_by_one,
        )
    batched = copy.deepcopy(message)
    log_batched = Log()
    ou.update_results_with_overlay_edge_list(kedges, batched, log_batched)

    assert bindings(batched) == bindings(one_by_one)
    assert bindings(batched) != bindings(message)  # something was bound
    assert log_batched.lines == log_one_by_one.lines == []


def test_empty_list_and_no_results_do_nothing():
    rng = random.Random(0)
    message = make_message(rng, n_results=10, n_genes=5, n_diseases=3)
    before = bindings(message)
    ou.update_results_with_overlay_edge_list([], message, Log())
    assert bindings(message) == before
    no_results = Message(query_graph=message.query_graph, results=[])
    ou.update_results_with_overlay_edge_list(
        [(("G:0", "D:0"), "F1_0")], no_results, Log()
    )
    assert no_results.results == []


def test_one_pass_is_fast_on_a_large_answer():
    """The case D-28 was found on: thousands of edges over 22k results."""
    rng = random.Random(1)
    message = make_message(rng, n_results=22000, n_genes=300, n_diseases=70)
    kedges = overlay_edges(rng, 10000, 300, 70, repeat_pairs=True)
    start = time.perf_counter()
    ou.update_results_with_overlay_edge_list(kedges, message, Log())
    # one call per edge took about 46 minutes here
    assert time.perf_counter() - start < 10


def _one_result_message():
    qg = QueryGraph(
        nodes={key: QNode() for key in ("n0", "n1", "n2")},
        edges={
            "e0": QEdge(subject="n0", object="n1"),
            "e1": QEdge(subject="n1", object="n2"),
        },
    )
    result = Result(
        node_bindings={
            "n0": NodeBinding(ids=["C:1"]),
            "n1": NodeBinding(ids=["G:1", "G:2"]),
            "n2": NodeBinding(ids=["D:1"]),
        },
        analyses=[
            Analysis(
                resource_id="infores:arax",
                edge_bindings={
                    "e0": EdgeBinding(ids=["a0"]),
                    "e1": EdgeBinding(ids=["b0"]),
                },
            ),
            Analysis(
                resource_id="infores:other",
                edge_bindings={"e1": EdgeBinding(ids=["x0"])},
            ),
        ],
    )
    return Message(query_graph=qg, results=[result])


def test_overlay_edges_are_unioned_into_the_one_edge_binding_per_qedge():
    message = _one_result_message()
    arax, other = message.results[0].analyses
    e1_binding = arax.edge_bindings["e1"]
    ou.update_results_with_overlay_edge_list(
        [
            (("G:1", "D:1"), "N1_0"),
            (("D:1", "G:2"), "N1_1"),  # the other way round binds too
            (("G:1", "D:1"), "N1_0"),  # already bound: not repeated
            (("G:3", "D:1"), "N1_2"),  # no result binds G:3
            (("C:1", "G:2"), "N1_3"),  # covers e0 only
        ],
        message,
        Log(),
    )
    # still one EdgeBinding object per qedge, its ids grown in creation order
    assert arax.edge_bindings["e1"] is e1_binding
    assert arax.edge_bindings["e1"].ids == ["b0", "N1_0", "N1_1"]
    assert arax.edge_bindings["e0"].ids == ["a0", "N1_3"]
    # analyses of other reasoners are left alone
    assert other.edge_bindings["e1"].ids == ["x0"]
    # and the result is valid TRAPI 2.0
    assert Result_2_0.from_dict(message.results[0].to_dict())


def test_results_without_analyses_or_edge_bindings_are_skipped():
    message = _one_result_message()
    message.results[0].analyses[1].edge_bindings = None  # a path-only analysis
    message.results.append(
        Result(node_bindings={"n1": NodeBinding(ids=["G:1"])})  # no analyses
    )
    log = Log()
    ou.update_results_with_overlay_edge_list([(("G:1", "D:1"), "N1_0")], message, log)
    assert message.results[0].analyses[0].edge_bindings["e1"].ids == ["b0", "N1_0"]
    assert message.results[1].analyses is None
    assert log.lines == []

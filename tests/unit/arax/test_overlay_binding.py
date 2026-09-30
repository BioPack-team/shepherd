"""Binding overlay edges to results in one pass (D-28) binds exactly what one
call per edge did.

fisher_exact_test and overlay_clinical_info used to call
update_results_with_overlay_edge once per new virtual edge, each call walking
every result. They now collect their edges and call
update_results_with_overlay_edge_list once. These tests compare the two on the
same messages, down to the order of each edge binding list, including edges
that share a node pair.
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
                    "e0": [EdgeBinding(id=f"a{i}", attributes=[])],
                    "e1": [EdgeBinding(id=f"b{i}", attributes=[])],
                },
            )
            for resource_id in ("infores:arax", "infores:other")
        ]
        results.append(
            Result(
                node_bindings={
                    "n0": [NodeBinding(id="C:1", attributes=[])],
                    "n1": [NodeBinding(id=f"G:{g}", attributes=[]) for g in genes],
                    "n2": [NodeBinding(id=f"D:{d}", attributes=[]) for d in diseases],
                },
                analyses=analyses,
            )
        )
    return Message(query_graph=qg, results=results)


def bindings(message):
    return [
        [
            (analysis.resource_id, qedge_key, [b.id for b in edge_bindings])
            for analysis in result.analyses
            for qedge_key, edge_bindings in analysis.edge_bindings.items()
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

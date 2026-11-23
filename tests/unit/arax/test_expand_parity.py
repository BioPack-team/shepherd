"""Expand parity: the Shepherd port vs. goldens recorded from upstream ARAX.

``expand_parity/run_upstream.py`` ran each case through ARAX's own Expand
(RTXteam/RTX @ 9485431, TRAPI 1.6) against a mock Retriever and recorded
everything observable: KG, QG, aux graphs, the in-memory
qnode_keys/qedge_keys/query_ids/filled annotations, excluded-edge info, the
query plan, INFO+ logs, and every request body sent to the KP
(``goldens_trapi16.json.gz``). The port speaks TRAPI 2.0: it runs the 2.0
cases in ``expand_parity/cases.py`` against the mock Retriever's 2.0 mode, and
must produce ``goldens.json.gz``, the TRAPI 2.0 translation of that record
(``trapi2_goldens.py``, which lists its rules), identically.

It runs the port in a subprocess with PYTHONHASHSEED=0, because ARAX builds
several lists from sets (and so do the goldens); Shepherd's Dockerfiles pin the
same seed.
"""

import gzip
import json
import math
import os
import subprocess
import sys

import pytest

HERE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "expand_parity")
sys.path.insert(0, HERE)
from cases import CASES  # noqa: E402

sys.path.insert(0, os.path.dirname(HERE))
from trapi2_goldens import (  # noqa: E402
    QUERY_ID_LOST,
    golden_paths,
    query_id_lost_pairs,
    read_json_gz,
)

FIELDS = [
    "exception",
    "status",
    "error_code",
    "qg",
    "qg_filled",
    "kg",
    "node_qnode_keys",
    "node_query_ids",
    "edge_qedge_keys",
    "aux",
    "kryptonite",
    "plan",
    "logs",
    "requests",
]
# Intended differences from upstream, by case: the fields exempt from comparison.
EXPECTED_DIFFERENCES = {
    # DEC-23: single-node (edge-less) expand is rejected, as in upstream's
    # trapi2.0 (Retriever needs an edge); upstream 9485431 sent it to rtx-kg2.
    # See test_single_node_is_rejected
    "single_node": set(FIELDS),
}
# TRAPI 2.0 (H5 in trapi2_goldens.py): these cases bind a subclass child to a
# qnode with several ids, and upstream learned which one it fulfils from
# NodeBinding.query_id, which 2.0 removed. Without it ARAX keeps no parent for
# the child, so it adds no subclass_of edge for it, and everything built on
# that differs. What 2.0 can still carry is compared, and
# test_subclass_parent_is_not_expressible_in_trapi2 pins the difference.
# The KP requests and the plan are compared where the case sends one request
# (it is built before any answer); later requests depend on the KG.
for _case in QUERY_ID_LOST["expand_parity"]:
    EXPECTED_DIFFERENCES.setdefault(_case, set()).update(
        {
            "qg",
            "qg_filled",
            "kg",
            "node_qnode_keys",
            "node_query_ids",
            "edge_qedge_keys",
            "aux",
            "kryptonite",
            "logs",
        }
    )
for _case in ("kryptonite", "prune_threshold", "three_hop"):
    EXPECTED_DIFFERENCES[_case].update({"requests", "plan"})
# Upstream trapi2.0 (2add4986, sync item 14): only the support graphs (and
# their edges and nodes) that edges still in the KG reach are kept; upstream
# 9485431 kept every one Retriever sent, also those of pruned edges. See
# test_only_reachable_support_graphs_are_kept
SUPPORT_GRAPH_PRUNED = ("prune_threshold_option",)
for _case in SUPPORT_GRAPH_PRUNED:
    EXPECTED_DIFFERENCES.setdefault(_case, set()).update(
        {"kg", "aux", "edge_qedge_keys", "node_qnode_keys", "logs"}
    )


def _sort_ids(obj):
    """The order of pinned curies sent to Retriever is set-derived upstream
    (``get_canonical_curies_list`` returns ``list(set(...))``), which the port
    skips under DEC-9, so compare those lists order-insensitively."""
    if isinstance(obj, dict):
        return {
            k: (sorted(v) if k == "ids" and isinstance(v, list) else _sort_ids(v))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_sort_ids(i) for i in obj]
    return obj


def _first_diff(x, y, path=""):
    """None if equal (NaN == NaN), else a short description of the first difference."""
    if (
        isinstance(x, float)
        and isinstance(y, float)
        and math.isnan(x)
        and math.isnan(y)
    ):
        return None
    if type(x) is not type(y):
        return f"{path}: {x!r:.300} != {y!r:.300}"
    if isinstance(x, dict):
        for k in sorted(set(x) | set(y), key=str):
            if k not in x or k not in y:
                side = "port" if k not in x else "upstream"
                return f"{path}.{k}: only in {side}"
            d = _first_diff(x[k], y[k], f"{path}.{k}")
            if d:
                return d
        return None
    if isinstance(x, list):
        if len(x) != len(y):
            return f"{path}: length {len(x)} (upstream) != {len(y)} (port)"
        for i, (xi, yi) in enumerate(zip(x, y)):
            d = _first_diff(xi, yi, f"{path}[{i}]")
            if d:
                return d
        return None
    return None if x == y else f"{path}: {x!r:.300} != {y!r:.300}"


@pytest.fixture(scope="module")
def outputs(tmp_path_factory):
    out_path = tmp_path_factory.mktemp("expand_parity") / "port.json"
    env = dict(os.environ, PYTHONHASHSEED="0")
    proc = subprocess.run(
        [sys.executable, os.path.join(HERE, "run_port.py"), str(out_path)],
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    with open(out_path) as f:
        port = json.load(f)
    with gzip.open(os.path.join(HERE, "goldens.json.gz"), "rt") as f:
        upstream = json.load(f)
    return upstream, port


@pytest.mark.parametrize("case", [c[0] for c in CASES])
def test_expand_matches_upstream(outputs, case):
    upstream, port = outputs
    exempt = EXPECTED_DIFFERENCES.get(case, set())
    for field in FIELDS:
        if field in exempt:
            continue
        want, got = upstream[case][field], port[case][field]
        if field in ("requests", "plan"):
            want, got = _sort_ids(want), _sort_ids(got)
        diff = _first_diff(want, got, field)
        assert diff is None, f"{case}: differs from upstream ARAX at {diff}"


@pytest.mark.parametrize("case", ["single_node", "kp_forwarded_single_node"])
def test_single_node_is_rejected(outputs, case):
    """DEC-23: an orphan qnode (expand with no edge_key, or node_key) is not
    expanded on its own -- with or without a kp list -- and nothing is sent."""
    _, port = outputs
    rec = port[case] if case in port else port["_port_only"][case]
    assert rec["status"] == "ERROR"
    assert rec["error_code"] == "InvalidQuery"
    assert rec["requests"] == []


@pytest.mark.parametrize(
    "case,expected_kp",
    [
        ("kp_forwarded", ["infores:ctd", "infores:drugbank", "infores:ctd"]),
        ("kp_forwarded_single", ["infores:drugbank"]),
    ],
)
def test_kp_list_is_forwarded_to_retriever(outputs, case, expected_kp):
    """DEC-10: the kp list goes to Retriever as parameters.kp, as given."""
    _, port = outputs
    rec = port["_port_only"][case]
    assert rec["status"] == "OK"
    (request,) = rec["requests"]
    assert request["body"]["parameters"] == {"tiers": [0], "kp": expected_kp}


def test_kp_list_leaves_every_other_kp_skipped(outputs):
    """DEC-12: with a kp list, only Retriever is queried; every other KP is Skipped."""
    _, port = outputs
    plan = port["_port_only"]["kp_forwarded"]["plan"]["e0"]
    # "Warning" when ARAX dropped a NaN-valued attribute from the answer
    assert plan["infores:retriever"]["status"] in ("Done", "Warning")
    assert {
        kp: v["status"]
        for kp, v in plan.items()
        if kp not in ("edge_properties", "infores:retriever")
    } == {
        "infores:rtx-kg2": "Skipped",
        "infores:spoke": "Skipped",
    }


@pytest.mark.parametrize("case", QUERY_ID_LOST["expand_parity"])
def test_subclass_parent_is_not_expressible_in_trapi2(outputs, case):
    """TRAPI 2.0 has no NodeBinding.query_id: a subclass child that Retriever
    bound to a qnode with several ids (in any of the case's queries) has no
    recorded parent in the port, where upstream had the one Retriever named;
    every other KG node keeps upstream's query_ids."""
    upstream, port = outputs
    lost = query_id_lost_pairs(
        read_json_gz(golden_paths("expand_parity")[0])[case]["requests"]
    )
    assert lost, "the case no longer exercises a lost query_id"
    want, got = upstream[case]["node_query_ids"], port[case]["node_query_ids"]
    for node, parents in lost.items():
        if node in want:  # (a node may be pruned later)
            assert set(parents) <= set(want[node]), node
        assert not set(got.get(node) or []) & set(parents), node
    for node, parents in want.items():
        if node in got and node not in lost:
            assert got[node] == parents, node


def _support_graph_ids(edge):
    return [
        aux_id
        for a in edge.get("attributes") or []
        if a.get("attribute_type_id") == "biolink:support_graphs"
        for aux_id in a["value"]
    ]


@pytest.mark.parametrize("case", SUPPORT_GRAPH_PRUNED)
def test_only_reachable_support_graphs_are_kept(outputs, case):
    """Upstream trapi2.0 (2add4986): the port's KG and aux graphs are upstream
    9485431's minus the support graphs no bound edge reaches (transitively),
    and the support edges and nodes only those held."""
    upstream, port = outputs
    want, got = upstream[case], port[case]
    edges, aux = want["kg"]["edges"], want["aux"]
    kept_edges = {k for k, q in want["edge_qedge_keys"].items() if q}
    kept_aux, todo = set(), list(kept_edges)
    while todo:
        for aux_id in _support_graph_ids(edges[todo.pop()]):
            if aux_id in aux and aux_id not in kept_aux:
                kept_aux.add(aux_id)
                new = set(aux[aux_id]["edges"]) - kept_edges
                kept_edges |= new
                todo.extend(new)
    assert kept_aux < set(aux), "the case no longer prunes a support graph"
    kept_nodes = {k for k, q in want["node_qnode_keys"].items() if q} | {
        n for k in kept_edges for n in (edges[k]["subject"], edges[k]["object"])
    }
    assert got["aux"] == {k: aux[k] for k in kept_aux}
    assert got["kg"]["edges"] == {k: edges[k] for k in kept_edges}
    assert got["kg"]["nodes"] == {k: want["kg"]["nodes"][k] for k in kept_nodes}


def _all_records(port):
    yield from ((c, r) for c, r in port.items() if c != "_port_only")
    yield from port["_port_only"].items()


def test_support_graphs_are_intact_in_the_kg(outputs):
    """Upstream trapi2.0's test_issue_2662: every aux graph Expand returns is
    referenced by a KG edge, and all its edges, and their nodes, are in the KG."""
    _, port = outputs
    for case, rec in _all_records(port):
        kg = rec["kg"] or {"nodes": {}, "edges": {}}
        referenced = {
            aux_id for e in kg["edges"].values() for aux_id in _support_graph_ids(e)
        }
        for aux_id, aux_graph in rec["aux"].items():
            assert aux_id in referenced, (case, aux_id)
            assert aux_graph["edges"], (case, aux_id)
            for edge_key in aux_graph["edges"]:
                assert edge_key in kg["edges"], (case, aux_id, edge_key)
                edge = kg["edges"][edge_key]
                assert edge["subject"] in kg["nodes"], (case, edge_key)
                assert edge["object"] in kg["nodes"], (case, edge_key)


def _e0_counts(rec):
    """e0's edge count in Retriever's answer, and after Expand."""
    (answered,) = [
        t
        for _, t in rec["logs"]
        if t.startswith("infores:retriever: Query for edge e0")
    ]
    (final,) = [t for _, t in rec["logs"] if t.startswith("After Expand")]
    return tuple(int(t.split("(e0: ")[1].split(",")[0]) for t in (answered, final))


def test_sources_allow_queries_retriever_and_filters_edges(outputs):
    """Upstream trapi2.0 (2f78aa95, sync items 9-10): a constraints.sources
    ALLOW list does not drop Retriever (it names knowledge sources an
    aggregator provides): Retriever is queried, and every returned edge from no
    allowed source is removed, not just the first."""
    _, port = outputs
    rec = port["_port_only"]["ks_sources_allow"]
    assert rec["status"] == "OK"
    (request,) = rec["requests"]
    assert request["body"]["message"]["query_graph"]["edges"]["e0"]["constraints"] == {
        "sources": {"behavior": "ALLOW", "values": ["infores:ctd"]}
    }
    answered, kept = _e0_counts(rec)
    assert kept and answered - kept >= 2
    for edge_key, qedge_keys in rec["edge_qedge_keys"].items():
        if "e0" in qedge_keys:
            sources = {
                s["resource_id"] for s in rec["kg"]["edges"][edge_key]["sources"]
            }
            assert "infores:ctd" in sources, edge_key


def test_evidence_count_is_an_arax_post_filter(outputs):
    """Upstream trapi2.0 (2f78aa95, sync items 11-12): a biolink:evidence_count
    constraint is supported, not sent to Retriever (the emptied constraints
    object goes with it), and an edge without the attribute fails it."""
    _, port = outputs
    rec = port["_port_only"]["evidence_count_constraint"]
    assert rec["status"] == "OK"
    (request,) = rec["requests"]
    assert "constraints" not in request["body"]["message"]["query_graph"]["edges"]["e0"]
    assert (
        "INFO",
        "Removed 6 edges fulfilling e0 that don't satisfy its attribute constraints "
        "(biolink:evidence_count > 5); 0 remain",
    ) in [tuple(line) for line in rec["logs"]]
    assert not any("e0" in q for q in rec["edge_qedge_keys"].values())


def test_agent_type_constraint_is_widened_with_its_subtypes(outputs):
    """Upstream trapi2.0 (2f78aa95, sync item 12): Retriever is sent the
    agent_type values' Biolink AgentTypeEnum subtypes too."""
    _, port = outputs
    (request,) = port["_port_only"]["agent_type_constraint"]["requests"]
    constraints = request["body"]["message"]["query_graph"]["edges"]["e0"][
        "constraints"
    ]
    assert constraints == {
        "agent_type": {
            "behavior": "ALLOW",
            "values": [
                "automated_agent",
                "data_analysis_pipeline",
                "computational_model",
                "text_mining_agent",
                "image_processing_agent",
            ],
        }
    }

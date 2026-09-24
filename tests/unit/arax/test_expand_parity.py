"""Expand parity: the Shepherd port vs. goldens recorded from upstream ARAX.

``expand_parity/run_upstream.py`` ran each case in ``expand_parity/cases.py``
through ARAX's own Expand (RTXteam/RTX @ 9485431) against a mock Retriever and
recorded everything observable: KG, QG, aux graphs, the in-memory
qnode_keys/qedge_keys/query_ids/filled annotations, excluded-edge info, the
query plan, INFO+ logs, and every request body sent to the KP. This test runs
the same cases through the port and requires identical results.

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
    # DEC-4: single-node queries go to Retriever (so they carry
    # parameters.tiers, and log setting it) instead of infores:rtx-kg2
    "single_node": {"requests", "logs"},
}


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


def test_single_node_goes_to_retriever(outputs):
    _, port = outputs
    (request,) = port["single_node"]["requests"]
    assert request["path"] == "/query"
    assert request["body"]["parameters"] == {"tiers": [0]}


@pytest.mark.parametrize(
    "case,expected_kp",
    [
        ("kp_forwarded", ["infores:ctd", "infores:drugbank", "infores:ctd"]),
        ("kp_forwarded_single", ["infores:drugbank"]),
        ("kp_forwarded_single_node", ["infores:rtx-kg2"]),
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

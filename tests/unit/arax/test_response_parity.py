"""/arax/response/{id} parity: the port's lookup vs. upstream's ResponseCache.get_response.

``response_parity/run_upstream.py`` ran every case through ARAX's own
get_response (RTXteam/RTX @ 9485431), with its storage and the ARS reached
through the same case data the port's fetchers get, and a deterministic
stand-in for reasoner-validator on both sides (the package is the same version
for both, and needs the network). This runs the cases through the port and
requires identical results.
"""

import gzip
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
RP = os.path.join(HERE, "response_parity")
sys.path.insert(0, RP)
sys.path.insert(0, HERE)
from response_cases import CASES  # noqa: E402
from test_expand_parity import _first_diff  # noqa: E402

# Cases whose difference is intended, checked separately below
EXPECTED_DIFFERENCES = {"ars_child_shepherd_agent"}


@pytest.fixture(scope="module")
def outputs(tmp_path_factory):
    out_path = tmp_path_factory.mktemp("response_parity") / "port.json"
    proc = subprocess.run(
        [sys.executable, os.path.join(RP, "run_port.py"), str(out_path)],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    with open(out_path) as f:
        port = json.load(f)
    with gzip.open(os.path.join(RP, "goldens.json.gz"), "rt") as f:
        upstream = json.load(f)
    return upstream, port


@pytest.mark.parametrize(
    "case", [c[0] for c in CASES if c[0] not in EXPECTED_DIFFERENCES]
)
def test_response_matches_upstream(outputs, case):
    upstream, port = outputs
    diff = _first_diff(upstream[case], port[case], case)
    assert diff is None, f"differs from upstream ARAX at {diff}"


def test_shepherd_ars_agent_names_label_results(outputs):
    """Results from Shepherd's ARS children get the ARA's name, as upstream
    labels the real ARS's (whose agents are named ara-arax etc.)."""
    upstream, port = outputs
    (up,) = upstream["ars_child_shepherd_agent"]
    (got,) = port["ars_child_shepherd_agent"]
    assert [r.get("resource_id") for r in up["message"]["results"]] == [
        None,
        "infores:other",
    ]
    assert [r["resource_id"] for r in got["message"]["results"]] == [
        "ARAX",
        "infores:other",
    ]
    for r in up["message"]["results"]:
        r.pop("resource_id", None)
    for r in got["message"]["results"]:
        r.pop("resource_id", None)
    assert _first_diff(up, got, "rest") is None

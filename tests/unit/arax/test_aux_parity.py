"""/arax/meta_knowledge_graph (AUX-01, DEC-11) and /arax/rtxcomplete/nodeslike
(AUX-02) parity with upstream ARAX.

``aux_parity/run_upstream.py`` ran upstream's KnowledgeSourceMetadata (with
PloverDB returning each case's base meta-KG and no KP info cache, so its merge
step leaves it unchanged), its hourly refresh function, and its autocomplete
lookup over a small terms database; the port, given the same meta-KG as
Retriever's, must produce the same full and simple meta-KGs, backups
fallback included, and the same autocomplete answers in the same order (the
fragment cache carries over between lookups).
"""

import gzip
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
AP = os.path.join(HERE, "aux_parity")
sys.path.insert(0, HERE)
from test_expand_parity import _first_diff  # noqa: E402


@pytest.fixture(scope="module")
def outputs(tmp_path_factory):
    out_path = tmp_path_factory.mktemp("aux_parity") / "port.json"
    proc = subprocess.run(
        [sys.executable, os.path.join(AP, "run_port.py"), str(out_path)],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    with open(out_path) as f:
        port = json.load(f)
    with gzip.open(os.path.join(AP, "goldens.json.gz"), "rt") as f:
        upstream = json.load(f)
    return upstream, port


@pytest.mark.parametrize(
    "case",
    [
        "full_then_simple",
        "simple_first",
        "fetch_fails_no_backup",
        "fetch_fails_uses_backup",
        "_refresh",
    ],
)
def test_meta_kg_matches_upstream(outputs, case):
    upstream, port = outputs
    diff = _first_diff(upstream["meta_kg"][case], port["meta_kg"][case], case)
    assert diff is None, f"differs from upstream ARAX at {diff}"


def test_autocomplete_matches_upstream(outputs):
    upstream, port = outputs
    diff = _first_diff(upstream["autocomplete"], port["autocomplete"], "nodeslike")
    assert diff is None, f"differs from upstream ARAX at {diff}"

"""End-to-end ARAXQuery parity: the Shepherd port vs. goldens from upstream ARAX.

``query_parity/run_upstream.py`` ran each case in ``query_parity/query_cases.py``
through ARAX's own ARAXQuery.query() (RTXteam/RTX @ 9485431) against the Expand
parity mock Retriever, with small synthetic data files (tier0 overlay,
curie_to_pmids, COHD, ExplainableDTD, FDA drugs) and recorded the status, the
final envelope (minus its id, datetime and logs), the INFO+ log and every
request sent to the KP. This test runs the same cases through the port and
requires identical results, apart from the intended differences below.

Like the Expand parity test it runs the port in a subprocess with
PYTHONHASHSEED=0.
"""

import gzip
import json
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
QP = os.path.join(HERE, "query_parity")
sys.path.insert(0, QP)
sys.path.insert(0, HERE)
from query_cases import CASES  # noqa: E402
from test_expand_parity import _first_diff, _sort_ids  # noqa: E402

FIELDS = [
    "exception",
    "status",
    "error_code",
    "http_status",
    "message",
    "envelope",
    "logs",
    "requests",
]
# Log lines upstream writes and the port does not, because the port has no KP
# cache (DEC-3): Expand stores the xDTD result in the cache after inferring.
NO_CACHE_LOGS = {"Storing result in the cache", "Stored result in the cache."}
# overlay_exposures_data is removed (E-8), so it is not listed as allowable
REMOVED_OVERLAY = "'overlay_exposures_data', "
# Fields exempt from comparison, by case
EXPECTED_DIFFERENCES = {
    # DEC-4: single-node queries go to Retriever (so they carry
    # parameters.tiers, and log setting it) instead of infores:rtx-kg2
    "tpl_one_node": {"requests", "logs"},
    # E-4 / E-6: the legacy `filter` command is removed; see
    # test_removed_filter_command_is_unrecognized
    "dsl_removed_filter_command": {"error_code", "message", "logs", "envelope"},
}


def _upstream_view(field, value):
    """Upstream's value with the intended, line-level differences applied."""
    if field == "logs":
        value = [x for x in value if x[2] not in NO_CACHE_LOGS]
    return json.loads(json.dumps(value).replace(REMOVED_OVERLAY, ""))


@pytest.fixture(scope="module")
def outputs(tmp_path_factory):
    out_path = tmp_path_factory.mktemp("query_parity") / "port.json"
    env = dict(os.environ, PYTHONHASHSEED="0")
    proc = subprocess.run(
        [sys.executable, os.path.join(QP, "run_port.py"), str(out_path)],
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    with open(out_path) as f:
        port = json.load(f)
    with gzip.open(os.path.join(QP, "goldens.json.gz"), "rt") as f:
        upstream = json.load(f)
    return upstream, port


@pytest.mark.parametrize("case", [c[0] for c in CASES])
def test_query_matches_upstream(outputs, case):
    upstream, port = outputs
    exempt = EXPECTED_DIFFERENCES.get(case, set())
    for field in FIELDS:
        if field in exempt:
            continue
        want, got = _upstream_view(field, upstream[case][field]), port[case][field]
        if field in ("requests", "envelope"):
            want, got = _sort_ids(want), _sort_ids(got)
        diff = _first_diff(want, got, field)
        assert diff is None, f"{case}: differs from upstream ARAX at {diff}"


def test_removed_filter_command_is_unrecognized(outputs):
    _, port = outputs
    rec = port["dsl_removed_filter_command"]
    assert rec["status"] == "ERROR"
    assert rec["error_code"] == "UnrecognizedCommand"
    assert rec["message"] == "Unrecognized command filter"


def test_stored_response_url_is_shepherds(outputs):
    """DEC-3: envelope.id is where Shepherd serves the stored response."""
    upstream, port = outputs
    stored = [n for n, rec in upstream.items() if rec["envelope_id"]]
    assert stored
    for name in stored:
        assert port[name]["envelope_id"] == "http://shepherd.test/arax/response/R1"
        assert (
            "INFO",
            "",
            "Result was stored with id R1. It can be viewed at URL",
        ) in [tuple(x) for x in port[name]["logs"]]

"""End-to-end ARAXQuery parity: the Shepherd port vs. goldens from upstream ARAX.

``query_parity/run_upstream.py`` ran each case through ARAX's own
ARAXQuery.query() (RTXteam/RTX @ 9485431, TRAPI 1.6) against the Expand parity
mock Retriever, with small synthetic data files (tier0 overlay,
curie_to_pmids, COHD, ExplainableDTD, FDA drugs) and recorded the status, the
final envelope (minus its id, datetime and logs), the INFO+ log and every
request sent to the KP (``goldens_trapi16.json.gz``). The port speaks TRAPI
2.0: it runs the 2.0 cases in ``query_parity/query_cases.py`` against the mock
Retriever's 2.0 mode and must produce ``goldens.json.gz``, the TRAPI 2.0
translation of that record (``trapi2_goldens.py``, which lists its rules),
apart from the intended differences below.

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
from trapi2_goldens import QUERY_ID_LOST  # noqa: E402

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
    # E-4: overlay_connect_knodes no longer emits the disabled
    # predict_drug_treats_disease overlay (which fails the plan upstream), so
    # the plan runs on to the next overlay; see
    # test_connect_knodes_translation_is_upstreams_minus_the_disabled_action
    "wf_connect_knodes": {"error_code", "message", "logs", "envelope", "status"},
    # DEC-10: a user-specified KP list (fill's allowlist) is forwarded to
    # Retriever as parameters.kp instead of being checked against SmartAPI
    # metadata, which in upstream rejects the query
    "wf_fill_qedge_keys_and_allowlist": {
        "error_code",
        "message",
        "logs",
        "envelope",
        "status",
        "http_status",
        "requests",
    },
    # DEC-9: no curie-prefix conversion before the KP query, so one fewer
    # "NodeSynonymizer did not recognize" warning for an unknown id; the KP
    # request itself is identical
    "trapi_set_interpretation_all": {"logs"},
}
# TRAPI 2.0 (H5 in trapi2_goldens.py): a subclass child bound to a qnode with
# several ids has no NodeBinding.query_id in 2.0, so ARAX records no parent
# for it and adds no subclass_of edge; the results built on that differ. The
# KP request (the case sends one, before any answer) and the outcome are
# compared; test_expand_parity pins the lost parent itself.
for _case in QUERY_ID_LOST["query_parity"]:
    EXPECTED_DIFFERENCES.setdefault(_case, set()).update(
        {"message", "envelope", "logs"}
    )
# Cases that cannot pass on TRAPI 2.0 yet, as strict xfails
XFAIL = {
    # connect(action=xcrg) runs the catrax-xcrg package, which speaks TRAPI
    # 1.x internally and has no 2.0 release; converting at its boundary would
    # be a runtime 1.x<->2.0 conversion, which Shepherd does not do
    "mvp2_xcrg_route": "catrax-xcrg has no TRAPI 2.0 release",
}


def _upstream_view(field, value):
    """Upstream's value with the intended, line-level differences applied."""
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


@pytest.mark.parametrize(
    "case",
    [
        (
            pytest.param(c[0], marks=pytest.mark.xfail(strict=True, reason=XFAIL[c[0]]))
            if c[0] in XFAIL
            else c[0]
        )
        for c in CASES
    ],
)
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


def test_set_interpretation_differs_only_by_the_curie_conversion_warning(outputs):
    """DEC-9: the one log line upstream writes before converting curie prefixes
    for the KP; everything else, and the KP request, is upstream's."""
    upstream, port = outputs
    case = "trapi_set_interpretation_all"
    warning = ["WARNING", "", "NodeSynonymizer did not recognize: {'uuid:set1'}"]
    want = list(upstream[case]["logs"])
    want.remove(warning)  # the first of upstream's two
    got = [
        x
        for x in port[case]["logs"]
        if x[2] != "loading blocklist file of overly general concept nodes"
    ]
    want = [
        x
        for x in want
        if x[2] != "loading blocklist file of overly general concept nodes"
    ]
    assert got == want
    assert port[case]["requests"] == upstream[case]["requests"]


def test_fill_allowlist_is_forwarded_to_retriever(outputs):
    """DEC-10: fill's allowlist becomes Retriever's parameters.kp."""
    _, port = outputs
    rec = port["wf_fill_qedge_keys_and_allowlist"]
    assert rec["status"] == "OK"
    (forwarded,) = [
        r for r in rec["requests"] if (r["body"].get("parameters") or {}).get("kp")
    ]
    assert forwarded["body"]["parameters"]["kp"] == ["infores:retriever"]


def test_connect_knodes_translation_is_upstreams_minus_the_disabled_action(outputs):
    """E-4: upstream's plan fails on predict_drug_treats_disease; the port's runs
    the same actions without it."""
    upstream, port = outputs
    case = "wf_connect_knodes"

    def actions(rec):
        return rec["envelope"]["operations"]["actions"]

    want = [
        a for a in actions(upstream[case]) if "predict_drug_treats_disease" not in a
    ]
    assert len(want) == len(actions(upstream[case])) - 1
    assert actions(port[case]) == want

"""The TRAPI 2.0 parity goldens and inputs are exactly the translation of
upstream's recorded TRAPI 1.6 ones (trapi2_goldens.py), and valid TRAPI 2.0.

- each suite's goldens.json.gz is trapi2_goldens' translation of its recorded
  goldens_trapi16.json.gz (re-run ``python trapi2_goldens.py`` after
  re-recording);
- the 2.0 cases the port runs are the translation of the recorded 1.6 inputs
  (inputs_trapi16.json.gz) upstream ran;
- the mock Retriever's 2.0 answer to every recorded request is the
  translation of its 1.6 answer, and valid 2.0;
- every TRAPI object in them passes TOM 2.0 validation, apart from the few
  deliberately invalid inputs named below.
"""

import json
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
for sub in ("", "expand_parity", "query_parity", "response_parity"):
    sys.path.insert(0, os.path.join(HERE, sub))

import trapi2_goldens as T  # noqa: E402

pytest.importorskip("translator_tom")
from translator_tom import v2_0  # noqa: E402


def _json(obj):
    return json.loads(json.dumps(obj, sort_keys=True))


def _deep(obj):
    """Case data compared as JSON: bytes and JSON text parsed, keys as strings."""
    if isinstance(obj, bytes):
        obj = obj.decode()
    if isinstance(obj, str) and obj[:1] in "{[":
        try:
            return _deep(json.loads(obj))
        except ValueError:
            return obj
    if isinstance(obj, (list, tuple)):
        return [_deep(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _deep(v) for k, v in obj.items()}
    return obj


@pytest.mark.parametrize("suite", T.SUITES)
def test_goldens_are_the_translation_of_the_upstream_record(suite):
    recorded = T.read_json_gz(T.golden_paths(suite)[1])
    assert recorded == _json(
        T.translate(suite)
    ), f"{suite}/goldens.json.gz is stale: run python tests/unit/arax/trapi2_goldens.py"


@pytest.mark.parametrize("suite", T.SUITES)
def test_goldens_are_valid_trapi2(suite):
    goldens = T.read_json_gz(T.golden_paths(suite)[1])
    assert T.validation_errors(suite, goldens) == []


def test_expand_cases_are_the_translation_of_the_recorded_inputs():
    import cases

    recorded = T.load_inputs16("expand_parity")
    for current, old in (
        (cases.CASES, recorded["CASES"]),
        (cases.PORT_ONLY_CASES, recorded["PORT_ONLY_CASES"]),
    ):
        assert [c[0] for c in current] == [c[0] for c in old]
        for case, old_case in zip(current, old):
            assert _json(case) == _json(T.expand_case(old_case)), case[0]
            v2_0.QueryGraph.from_dict(case[1])


# Deliberately invalid TRAPI 2.0 inputs: what ARAX does with them is the test
INVALID_QUERY_CASES = {
    "workflow_unknown_op": "an operation TRAPI does not define",
    "wf_fill_qedge_keys_and_allowlist": "fill with only ARAX's qedge_keys first",
}


def test_query_cases_are_the_translation_of_the_recorded_inputs():
    import query_cases

    old = T.load_inputs16("query_parity")["CASES"]
    assert [c[0] for c in query_cases.CASES] == [c[0] for c in old]
    for case, old_case in zip(query_cases.CASES, old):
        assert _json(case) == _json(T.query_case(old_case)), case[0]
        name, query = case
        if "message" in query and name not in INVALID_QUERY_CASES:
            v2_0.Query.from_dict(query)
    for name in INVALID_QUERY_CASES:
        query = dict(query_cases.CASES)[name]
        with pytest.raises(Exception):
            v2_0.Query.from_dict(query)


def test_response_cases_are_the_translation_of_the_recorded_inputs():
    import response_cases

    old = T.load_inputs16("response_parity")["CASES"]
    assert [c[0] for c in response_cases.CASES] == [c[0] for c in old]
    for case, old_case in zip(response_cases.CASES, old):
        assert _deep(case) == _deep(T.response_case(old_case)), case[0]


def _recorded_requests():
    for suite in ("expand_parity", "query_parity"):
        for case, rec in T.read_json_gz(T.golden_paths(suite)[0]).items():
            for i, request in enumerate(rec["requests"]):
                yield f"{suite}.{case}[{i}]", request["body"]


def test_mock_retriever_answers_are_the_translation_of_its_recorded_answers():
    import mock_retriever

    n = 0
    for where, body in _recorded_requests():
        code16, answer16 = mock_retriever.answer(body, trapi="1.6")
        code20, answer20 = mock_retriever.answer(T.query(body), trapi="2.0")
        assert code20 == code16, where
        if code16 != 200:
            assert answer20 == answer16, where
            continue
        want = T.response(answer16, produced=False)
        assert _json(answer20) == _json(want), where
        v2_0.Response.from_dict(answer20)
        n += 1
    assert n > 100


def test_mock_retriever_serves_trapi2_by_default():
    import mock_retriever

    assert mock_retriever.TRAPI == "2.0"


@pytest.mark.parametrize("suite", ["expand_parity", "query_parity"])
def test_query_id_lost_cases_are_the_ones_listed(suite):
    assert tuple(T.query_id_lost_cases(suite)) == T.QUERY_ID_LOST[suite]


def test_arax_made_edges_get_their_2_0_levels():
    """H9: the overlay edges upstream made without KL/AT get the port's values."""
    old = T.read_json_gz(T.golden_paths("query_parity")[0])
    new = T.read_json_gz(T.golden_paths("query_parity")[1])
    seen = set()
    for case, rec in old.items():
        edges = (rec["envelope"]["message"].get("knowledge_graph") or {}).get(
            "edges"
        ) or {}
        new_kg = new[case]["envelope"]["message"].get("knowledge_graph") or {}
        new_edges = new_kg.get("edges") or {}
        for key, edge in edges.items():
            levels = T.arax_edge_levels(edge)
            if levels is None:
                continue
            seen.add(edge["predicate"])
            got = new_edges[key]
            assert (got["knowledge_level"], got["agent_type"]) == levels, key
    assert seen == {p for p, _ in T.ARAX_EDGE_LEVELS}


def test_envelopes_declare_trapi_2():
    new = T.read_json_gz(T.golden_paths("query_parity")[1])
    assert {rec["envelope"]["schema_version"] for rec in new.values()} == {"2.0.0"}

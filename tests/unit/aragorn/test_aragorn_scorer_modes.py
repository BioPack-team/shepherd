"""The aragorn.score worker's scorer choice (ARAGORN_SCORER): python, rust
(the aragorn_score_rs module, with a fallback to python) and shadow (python
saves, rust is compared). Tests that run the Rust scorer skip when the module
isn't built (see workers/aragorn_score/rust)."""

import copy
import importlib.util
import json
import logging
from pathlib import Path

import pytest

from shepherd_utils.db import decode_message, encode_message
from tests.helpers.generate_messages import response_1
from workers.aragorn_score import worker
from workers.aragorn_score.worker import Ranker, aragorn_score, compare_scores

needs_rust = pytest.mark.skipif(
    worker.aragorn_score_rs is None, reason="aragorn_score_rs is not built"
)

ROOT = Path(__file__).resolve().parents[3]


def _gen_message(**kwargs):
    """benchmarks/aragorn_score/gen_message.py, which isn't a package."""
    spec = importlib.util.spec_from_file_location(
        "gen_message", ROOT / "benchmarks" / "aragorn_score" / "gen_message.py"
    )
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    quirks = kwargs.pop("quirks", False)
    cycles = kwargs.pop("cycles", False)
    message = gen.make_message(**kwargs)
    if cycles:
        gen.add_cycles(message, seed=kwargs.get("seed", 0))
    if quirks:
        gen.add_quirks(message)
    return {"message": message}


class Store:
    """Stands in for the data db: one stored response, as a blob."""

    def __init__(self, mocker, response):
        self.blob = encode_message(response)
        self.saved_blob = None
        self.saved_response = None
        mocker.patch.object(worker, "get_blob_sync", side_effect=lambda rid: self.blob)
        mocker.patch.object(
            worker,
            "get_message_sync",
            side_effect=lambda rid: decode_message(self.blob),
        )
        mocker.patch.object(worker, "save_blob_sync", side_effect=self._save_blob)
        mocker.patch.object(
            worker, "save_response_sync", side_effect=self._save_response
        )

    def _save_blob(self, rid, blob):
        self.saved_blob = blob

    def _save_response(self, rid, response):
        self.saved_response = copy.deepcopy(response)


def python_scores(response):
    """Each result's analysis scores, from the Python Ranker on a copy."""
    message = copy.deepcopy(response["message"])
    results = message["results"]
    Ranker(message, logging.getLogger(__name__)).rank(results)
    return [[a.get("score") for a in r.get("analyses") or []] for r in results]


def scores_of(response):
    """Each result's analysis scores, keyed by the result's content."""
    out = {}
    for r in response["message"]["results"] or []:
        key = json.dumps(r["node_bindings"], sort_keys=True)
        out.setdefault(key, []).append(
            [a.get("score") for a in r.get("analyses") or []]
        )
    return out


@pytest.fixture(autouse=True)
def _reset_warnings():
    worker._warned.clear()


def test_python_is_the_default_and_never_calls_rust(mocker, monkeypatch):
    monkeypatch.setattr(worker.settings, "aragorn_scorer", "python")
    rust = mocker.patch.object(worker, "aragorn_score_rs")
    store = Store(mocker, copy.deepcopy(response_1))

    aragorn_score("r", logging.INFO)

    rust.score_blob.assert_not_called()
    assert store.saved_blob is None
    assert store.saved_response["message"]["results"][0]["analyses"][0]["score"] > 0


def test_unknown_scorer_warns_and_runs_python(mocker, monkeypatch, caplog):
    monkeypatch.setattr(worker.settings, "aragorn_scorer", "fortran")
    store = Store(mocker, copy.deepcopy(response_1))

    with caplog.at_level(logging.WARNING):
        aragorn_score("r", logging.INFO)

    assert store.saved_response is not None
    assert "Unknown ARAGORN_SCORER 'fortran'" in caplog.text


def test_rust_without_the_module_runs_python(mocker, monkeypatch, caplog):
    monkeypatch.setattr(worker.settings, "aragorn_scorer", "rust")
    monkeypatch.setattr(worker, "aragorn_score_rs", None)
    store = Store(mocker, copy.deepcopy(response_1))

    with caplog.at_level(logging.WARNING):
        aragorn_score("r", logging.INFO)

    assert store.saved_response is not None
    assert store.saved_blob is None
    assert "isn't installed" in caplog.text


def test_rust_failure_falls_back_to_python(mocker, monkeypatch):
    monkeypatch.setattr(worker.settings, "aragorn_scorer", "rust")
    rust = mocker.patch.object(worker, "aragorn_score_rs")
    rust.score_blob.side_effect = RuntimeError("boom")
    store = Store(mocker, copy.deepcopy(response_1))

    entries = aragorn_score("r", logging.INFO)

    assert store.saved_blob is None
    assert store.saved_response["message"]["results"][0]["analyses"][0]["score"] > 0
    assert any("could not score this response (boom)" in str(e) for e in entries)


@needs_rust
@pytest.mark.parametrize(
    "response",
    [
        pytest.param(lambda: copy.deepcopy(response_1), id="fixture"),
        pytest.param(lambda: _gen_message(n_results=60, seed=3), id="synthetic"),
        pytest.param(
            lambda: _gen_message(n_results=40, seed=5, nested=True, quirks=True),
            id="quirks",
        ),
        pytest.param(
            lambda: _gen_message(n_results=60, seed=7, cycles=True),
            id="support-cycles",
        ),
    ],
)
def test_rust_scores_like_python(mocker, monkeypatch, response):
    monkeypatch.setattr(worker.settings, "aragorn_scorer", "rust")
    response = response()
    expected = python_scores(response)
    store = Store(mocker, response)

    aragorn_score("r", logging.INFO)

    assert store.saved_response is None, "the Python scorer should not have run"
    saved = decode_message(store.saved_blob)
    # Everything but the results is untouched.
    assert {k: v for k, v in saved["message"].items() if k != "results"} == {
        k: v for k, v in response["message"].items() if k != "results"
    }
    # Same scores, result by result (matched by content: near-ties may order
    # differently).
    python_by_result = {}
    for r, s in zip(response["message"]["results"], expected):
        python_by_result.setdefault(
            json.dumps(r["node_bindings"], sort_keys=True), []
        ).append(s)
    rust_by_result = scores_of(saved)
    assert python_by_result.keys() == rust_by_result.keys()
    for key, py in python_by_result.items():
        for a, b in zip(py, rust_by_result[key]):
            assert b == pytest.approx(a, rel=1e-9, abs=1e-12)
    # Ranked ascending by best score.
    best = [
        max(s for s in row)
        for row in (
            [a["score"] for a in r["analyses"]] for r in saved["message"]["results"]
        )
    ]
    assert all(x <= y + 1e-12 for x, y in zip(best, best[1:]))


@needs_rust
def test_rust_saves_unchanged_without_results(mocker, monkeypatch):
    monkeypatch.setattr(worker.settings, "aragorn_scorer", "rust")
    response = {"message": {"results": None}}
    store = Store(mocker, response)

    aragorn_score("r", logging.INFO)

    assert decode_message(store.saved_blob) == response


@needs_rust
def test_shadow_saves_python_and_logs_the_comparison(mocker, monkeypatch, caplog):
    monkeypatch.setattr(worker.settings, "aragorn_scorer", "shadow")
    response = _gen_message(n_results=30, seed=1)
    store = Store(mocker, response)

    with caplog.at_level(logging.INFO, logger="shepherd.aragorn.score"):
        aragorn_score("r-shadow", logging.INFO)

    assert store.saved_blob is None, "shadow mode saves Python's response only"
    assert store.saved_response is not None
    lines = [
        r.getMessage()
        for r in caplog.records
        if "aragorn.score shadow" in r.getMessage()
    ]
    assert len(lines) == 1
    summary = json.loads(lines[0].split("aragorn.score shadow ", 1)[1])
    assert summary["response_id"] == "r-shadow"
    assert summary["agree"] is True
    assert summary["analyses"] == 30
    assert summary["beyond_tol"] == 0
    assert summary["max_rel_diff"] < 1e-9
    assert summary["python_rank_ms"] > 0 and summary["rust_total_ms"] > 0
    assert summary["support_cycles"] == 0 and summary["largest_support_cycle"] == 0


@needs_rust
def test_shadow_survives_a_rust_failure(mocker, monkeypatch, caplog):
    monkeypatch.setattr(worker.settings, "aragorn_scorer", "shadow")
    mocker.patch.object(
        worker.aragorn_score_rs, "score_blob", side_effect=RuntimeError("boom")
    )
    store = Store(mocker, copy.deepcopy(response_1))

    with caplog.at_level(logging.WARNING):
        aragorn_score("r", logging.INFO)

    assert store.saved_response is not None
    assert "the Rust scorer failed: boom" in caplog.text


# --- compare_scores -----------------------------------------------------------


def test_compare_identical():
    out = compare_scores([[0.5], [0.25, 0.1]], [1, 0], [[0.5], [0.25, 0.1]], [1, 0])
    assert out["agree"] and out["beyond_tol"] == 0 and out["results_moved"] == 0
    assert out["top10_overlap"] == 2 and out["top10_of"] == 2


def test_compare_rounding_is_agreement_but_a_real_difference_is_not():
    rounding = compare_scores([[0.5]], [0], [[0.5 + 1e-14]], [0])
    assert rounding["agree"] and rounding["max_rel_diff"] < 1e-12

    off = compare_scores([[0.5], [0.2]], [1, 0], [[0.5], [0.3]], [0, 1])
    assert not off["agree"]
    assert off["beyond_tol"] == 1
    assert off["worst"] == {"result": 1, "analysis": 0, "python": 0.2, "rust": 0.3}
    assert off["results_moved"] == 2


def test_compare_shape_and_type_mismatches_disagree():
    assert not compare_scores([[0.5]], [0], [[0.5, 0.1]], [0])["agree"]
    assert not compare_scores([[0.5]], [0], [], [])["agree"]
    assert not compare_scores([[None]], [0], [[0]], [0])["agree"]
    assert compare_scores([[0]], [0], [[0.0]], [0])["agree"]


@needs_rust
def test_shadow_saves_the_input_of_a_disagreement(
    mocker, monkeypatch, tmp_path, caplog
):
    monkeypatch.setattr(worker.settings, "aragorn_scorer", "shadow")
    monkeypatch.setattr(worker.settings, "aragorn_shadow_dump_dir", str(tmp_path))
    monkeypatch.setattr(worker, "_shadow_dumps", 0)
    response = _gen_message(n_results=20, seed=2)
    store = Store(mocker, response)
    real = worker.aragorn_score_rs.score_blob

    def skewed(blob, **kw):
        out, info = real(blob, **kw)
        info["scores"][0][0] = info["scores"][0][0] / 2  # a disagreement
        return out, info

    mocker.patch.object(worker.aragorn_score_rs, "score_blob", side_effect=skewed)
    with caplog.at_level(logging.WARNING, logger="shepherd.aragorn.score"):
        aragorn_score("r/dis agree", logging.INFO)

    saved = tmp_path / "r_dis_agree.json.zst"
    assert saved.read_bytes() == store.blob
    line = next(
        r.getMessage()
        for r in caplog.records
        if "aragorn.score shadow" in r.getMessage()
    )
    summary = json.loads(line.split("aragorn.score shadow ", 1)[1])
    assert summary["agree"] is False
    assert summary["saved_input"] == str(saved)


@needs_rust
def test_shadow_saves_nothing_when_they_agree(mocker, monkeypatch, tmp_path):
    monkeypatch.setattr(worker.settings, "aragorn_scorer", "shadow")
    monkeypatch.setattr(worker.settings, "aragorn_shadow_dump_dir", str(tmp_path))
    Store(mocker, _gen_message(n_results=20, seed=2))

    aragorn_score("r", logging.INFO)

    assert list(tmp_path.iterdir()) == []

"""shepherd_utils.ars.ranker_fusion: configuration and source matching.

The fusion itself is pinned against upstream's own output in
test_golden_parity.py (test_ranker_fusion_parity); these cover what the
goldens can't: reading ARS_RRF_* from the environment, with upstream's
fall-backs, and naming a ranker by infores or agent name.
"""

import logging

import pytest

from shepherd_utils.ars import ranker_fusion

ARAGORN = "infores:shepherd-aragorn"
ARAX = "infores:shepherd-arax"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for var in ("ARS_RRF_ENABLED", "ARS_RRF_ARAGORN_WEIGHT", "ARS_RRF_C"):
        monkeypatch.delenv(var, raising=False)


def test_defaults():
    assert ranker_fusion.get_config() == {
        "enabled": True,
        "weights": {ARAGORN: 0.9, ARAX: 0.1},
        "c_value": 40,
    }


@pytest.mark.parametrize(
    "value,enabled", [("true", True), (" TRUE ", True), ("false", False), ("1", False)]
)
def test_enabled_only_by_true(monkeypatch, value, enabled):
    monkeypatch.setenv("ARS_RRF_ENABLED", value)
    assert ranker_fusion.get_config()["enabled"] is enabled


def test_aragorn_weight_sets_both(monkeypatch):
    monkeypatch.setenv("ARS_RRF_ARAGORN_WEIGHT", "0.3")
    assert ranker_fusion.get_config()["weights"] == {ARAGORN: 0.3, ARAX: 0.7}


@pytest.mark.parametrize("value", ["1.5", "-0.1", "nan", "inf", "heavy"])
def test_invalid_aragorn_weight_falls_back(monkeypatch, caplog, value):
    monkeypatch.setenv("ARS_RRF_ARAGORN_WEIGHT", value)
    with caplog.at_level(logging.WARNING):
        assert ranker_fusion.get_config()["weights"] == {ARAGORN: 0.9, ARAX: 0.1}
    assert "ARS_RRF_ARAGORN_WEIGHT" in caplog.text


@pytest.mark.parametrize("value,c", [("0", 0), ("60", 60), ("-1", 40), ("4.5", 40)])
def test_c_value(monkeypatch, value, c):
    monkeypatch.setenv("ARS_RRF_C", value)
    assert ranker_fusion.get_config()["c_value"] == c


@pytest.mark.parametrize(
    "inforesid,agent,source",
    [
        (ARAGORN, None, ARAGORN),
        (" Infores:Shepherd-ARAX ", None, ARAX),
        (None, "ara-shepherd-aragorn", ARAGORN),
        ("", "ARA-shepherd-arax", ARAX),
        ("infores:shepherd-bte", "ara-shepherd-bte", None),
        (None, "ars-ars-agent", None),
        (None, None, None),
    ],
)
def test_configured_source(inforesid, agent, source):
    weights = {ARAGORN: 0.9, ARAX: 0.1}
    assert ranker_fusion.configured_source(inforesid, agent, weights) == source

"""The ARAX port's Node Normalizer switch (shepherd_utils/arax_normalization.py):
on, off (CURIEs taken as already normalized) and compare."""

from types import SimpleNamespace

import pytest

from shepherd_utils import arax_normalization as AN
from shepherd_utils.arax.NodeSynonymizer.node_synonymizer import NodeSynonymizer
from shepherd_utils.config import settings

# What the Node Normalizer answers for a drug given by a non-preferred id
NODENORM = {
    "MESH:D000068877": {
        "id": {"identifier": "CHEBI:45783", "label": "imatinib"},
        "equivalent_identifiers": [
            {"identifier": "CHEBI:45783"},
            {"identifier": "MESH:D000068877"},
        ],
        "type": ["biolink:SmallMolecule", "biolink:ChemicalEntity"],
    },
    "NCBIGene:25": {
        "id": {"identifier": "NCBIGene:25", "label": "ABL1"},
        "equivalent_identifiers": [{"identifier": "NCBIGene:25"}],
        "type": ["biolink:Gene", "biolink:BiologicalEntity"],
    },
}


@pytest.fixture
def synonymizer(monkeypatch):
    calls = []

    def nodenorm(self, curies):
        calls.append(list(curies))
        return {c: NODENORM.get(c) for c in curies}

    monkeypatch.setattr(NodeSynonymizer, "_call_normalizer_api_uncompared", nodenorm)
    yield NodeSynonymizer(), calls
    AN.reset()
    AN._mode = "on"


def _query(mode=None):
    return {"parameters": {AN.PARAMETER: mode}} if mode else {}


def test_the_query_parameter_overrides_the_setting(monkeypatch):
    monkeypatch.setattr(settings, "arax_node_normalization", "off")
    assert AN.resolve_mode(_query()) == "off"
    assert AN.resolve_mode(_query("compare")) == "compare"
    assert AN.resolve_mode(_query("nonsense")) == "off"
    monkeypatch.setattr(settings, "arax_node_normalization", "nonsense")
    assert AN.resolve_mode(_query()) == "on"


def test_the_mode_lasts_only_for_the_query():
    with AN.query_mode(_query("off")) as mode:
        assert mode == AN.mode() == "off"
    assert AN.mode() == "on"


def test_on_asks_the_node_normalizer(synonymizer):
    syn, calls = synonymizer
    with AN.query_mode(_query("on")):
        result = syn.get_canonical_curies(["MESH:D000068877"])
        assert AN.report() is None
    assert result["MESH:D000068877"]["preferred_curie"] == "CHEBI:45783"
    assert calls == [["MESH:D000068877"]]


def test_off_takes_curies_as_normalized_with_the_kps_names_and_categories(
    synonymizer,
):
    syn, calls = synonymizer
    with AN.query_mode(_query("off")):
        AN.remember_kp_nodes(
            {
                "NCBIGene:25": SimpleNamespace(
                    name="ABL1",
                    categories=["biolink:BiologicalEntity", "biolink:Gene"],
                )
            }
        )
        canonical = syn.get_canonical_curies(["NCBIGene:25", "MESH:D000068877"])
        equivalents = syn.get_equivalent_nodes(["MESH:D000068877"])
        report = AN.report()
    assert calls == []  # no Node Normalizer request
    assert canonical["NCBIGene:25"]["preferred_curie"] == "NCBIGene:25"
    assert canonical["NCBIGene:25"]["preferred_name"] == "ABL1"
    # the most specific of the KP's categories, as the Node Normalizer's first
    assert canonical["NCBIGene:25"]["preferred_category"].endswith("Gene")
    assert canonical["MESH:D000068877"]["preferred_curie"] == "MESH:D000068877"
    assert canonical["MESH:D000068877"]["preferred_category"] is None
    assert equivalents == {"MESH:D000068877": ["MESH:D000068877"]}
    assert report["mode"] == "off"
    assert report["totals"] == {"calls": 2, "curies": 3}


def test_compare_answers_as_on_and_records_what_off_would_change(synonymizer):
    syn, calls = synonymizer
    with AN.query_mode(_query("compare")):
        AN.remember_kp_nodes(
            {"NCBIGene:25": SimpleNamespace(name="ABL1", categories=["biolink:Gene"])}
        )
        canonical = syn.get_canonical_curies(["NCBIGene:25", "MESH:D000068877"])
        report = AN.report()
    assert canonical["MESH:D000068877"]["preferred_curie"] == "CHEBI:45783"
    assert len(calls) == 1
    totals = report["totals"]
    # NCBIGene:25 is the same either way; the MESH id would keep its own id,
    # lose its category and name, and has another equivalent id
    assert (totals["curies"], totals["id"], totals["no_category"]) == (2, 1, 1)
    assert (totals["name"], totals["equivalents"], totals["category"]) == (1, 1, 0)
    (site,) = report["sites"]
    assert "test_arax_normalization.py" in site
    (example,) = report["sites"][site]["examples"]["id"]
    assert example["curie"] == "MESH:D000068877"
    assert example["normalized"] == "CHEBI:45783"


def test_compare_counts_curies_the_node_normalizer_doesnt_know(synonymizer):
    syn, _ = synonymizer
    with AN.query_mode(_query("compare")):
        syn.get_canonical_curies(["FAKE:1"])
        assert AN.report()["totals"]["unknown"] == 1


def test_kp_nodes_are_only_kept_off_or_compare():
    node = SimpleNamespace(name="ABL1", categories=["biolink:Gene"])
    with AN.query_mode(_query("on")):
        AN.remember_kp_nodes({"NCBIGene:25": node})
        assert AN._kp_nodes == {}
    with AN.query_mode(_query("compare")):
        AN.remember_kp_nodes({"NCBIGene:25": node})
        assert AN._kp_nodes["NCBIGene:25"]["name"] == "ABL1"
    with AN.query_mode(_query("off")):
        assert AN._kp_nodes == {}  # a new query starts afresh

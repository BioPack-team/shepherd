"""Parity tests for the ARAX ranker port (workers/arax_rank).

Expected values come from running the real ARAX ranker
(RTXteam/RTX @ 9485431, ARAX_ranker.py) on the same inputs. See
docs/ARAX_PORT_BASELINE.md (DEC-1, DEC-8, RNK-*): ARAX's quirks are
reproduced on purpose.
"""

import copy
import logging
import os
import sys

import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "..", "workers", "arax_rank")
)

from ranker import ARAXRanker, arax_rank  # noqa: E402
from workers.arax_rank.worker import rank_message  # noqa: E402

logger = logging.getLogger(__name__)


def kg_key(kp, subject, obj, source):
    """An ARAX Expand-style KG edge key (primary knowledge source last)."""
    return f"{kp}:{subject}--biolink:treats--None--None--None--{obj}--{source}"


DRUGBANK = kg_key("infores:retriever", "CHEBI:1", "MONDO:1", "infores:drugbank")
DRUGBANK_MANUAL = kg_key("infores:rtx-kg2", "CHEBI:1", "MONDO:1", "infores:drugbank")
SEMMED_7 = kg_key("infores:retriever", "CHEBI:2", "MONDO:1", "infores:semmeddb")
CTD_CONF = kg_key("infores:retriever", "CHEBI:3", "MONDO:1", "infores:ctd")
SEMMED_1 = kg_key("infores:retriever", "CHEBI:4", "MONDO:1", "infores:semmeddb")


def ngd_edge(subject, value):
    return {
        "subject": subject,
        "object": "MONDO:1",
        "predicate": "biolink:occurs_together_in_literature_with",
        "attributes": [
            {
                "attribute_type_id": "EDAM-DATA:2526",
                "original_attribute_name": "normalized_google_distance",
                "value": value,
            }
        ],
    }


def treats_edge(subject, attributes):
    return {
        "subject": subject,
        "object": "MONDO:1",
        "predicate": "biolink:treats",
        "attributes": attributes,
    }


def golden_envelope():
    edges = {
        DRUGBANK: treats_edge("CHEBI:1", []),
        DRUGBANK_MANUAL: treats_edge(
            "CHEBI:1",
            [{"attribute_type_id": "biolink:agent_type", "value": "manual_agent"}],
        ),
        SEMMED_7: treats_edge(
            "CHEBI:2",
            [
                {
                    "attribute_type_id": "biolink:publications",
                    "value": [f"PMID:{i}" for i in range(1, 8)],
                }
            ],
        ),
        CTD_CONF: treats_edge(
            "CHEBI:3",
            [
                {
                    "attribute_type_id": "EDAM:x",
                    "original_attribute_name": "confidence",
                    "value": 0.2,
                }
            ],
        ),
        SEMMED_1: treats_edge(
            "CHEBI:4",
            [{"attribute_type_id": "biolink:publications", "value": "PMID:9"}],
        ),
        "N1_1": ngd_edge("CHEBI:1", "0.35"),
        "N1_2": ngd_edge("CHEBI:2", "no value!"),
        "N1_3": ngd_edge("CHEBI:3", "0.9"),
        "N1_4": ngd_edge("CHEBI:4", "inf"),
    }

    def result(i, treats_keys):
        return {
            "node_bindings": {
                "n0": [{"id": "MONDO:1", "attributes": []}],
                "n1": [{"id": f"CHEBI:{i}", "attributes": []}],
            },
            "analyses": [
                {
                    "resource_id": "infores:arax",
                    "edge_bindings": {
                        "e0": [{"id": key, "attributes": []} for key in treats_keys],
                        "N1": [{"id": f"N1_{i}", "attributes": []}],
                    },
                }
            ],
            "essence": f"drug {i}",
            "essence_category": "['biolink:ChemicalEntity']",
        }

    return {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {"ids": ["MONDO:1"]},
                    "n1": {"categories": ["biolink:ChemicalEntity"]},
                },
                "edges": {
                    "e0": {
                        "subject": "n1",
                        "object": "n0",
                        "predicates": ["biolink:treats"],
                    },
                    "N1": {"subject": "n1", "object": "n0"},
                },
            },
            "knowledge_graph": {"nodes": {}, "edges": edges},
            "results": [
                result(4, [SEMMED_1]),
                result(3, [CTD_CONF]),
                result(2, [SEMMED_7]),
                result(1, [DRUGBANK, DRUGBANK_MANUAL]),
            ],
        }
    }


def test_golden_matches_arax():
    """Order, scores, row_data and table_column_names match ARAX exactly."""
    envelope = arax_rank(golden_envelope(), logger)
    results = envelope["message"]["results"]

    assert [r["essence"] for r in results] == ["drug 1", "drug 2", "drug 4", "drug 3"]
    assert [r["analyses"][0]["score"] for r in results] == [1.0, 0.75, 0.5, 0.25]
    assert [r["row_data"] for r in results] == [
        [1.0, "drug 1", "['biolink:ChemicalEntity']"],
        [0.75, "drug 2", "['biolink:ChemicalEntity']"],
        [0.5, "drug 4", "['biolink:ChemicalEntity']"],
        [0.25, "drug 3", "['biolink:ChemicalEntity']"],
    ]
    assert envelope["table_column_names"] == ["score", "essence", "essence_category"]
    assert all(type(r["analyses"][0]["score"]) is float for r in results)


def test_golden_edge_confidences_match_arax():
    ranker = ARAXRanker()
    ranker.aggregate_scores_dmk(golden_envelope(), logger)
    expected = {
        DRUGBANK: 0.99,
        DRUGBANK_MANUAL: 0.9,
        SEMMED_7: 0.8719766599592949,
        CTD_CONF: 0.2,
        SEMMED_1: 0.503024543592212,
        "N1_1": 0.7237204280807125,
        "N1_2": 0.7964029814712471,
        "N1_3": 0.05037868484559719,
        "N1_4": 0,
    }
    assert ranker.edge_confidences == pytest.approx(expected, rel=1e-12, abs=0)


def test_confidences_are_not_written_to_the_kg():
    """ARAX keeps edge confidence off the serialized edge; so does the port."""
    envelope = arax_rank(golden_envelope(), logger)
    for edge in envelope["message"]["knowledge_graph"]["edges"].values():
        assert "confidence" not in edge


def test_no_value_is_rewritten_to_zero_on_the_edge():
    envelope = arax_rank(golden_envelope(), logger)
    attr = envelope["message"]["knowledge_graph"]["edges"]["N1_2"]["attributes"][0]
    assert attr["value"] == 0


@pytest.mark.parametrize(
    "key,expected",
    [
        # the data source is the LAST "--" segment of the key (C-2)
        (kg_key("infores:rtx-kg2", "A:1", "B:1", "infores:drugbank"), 0.99),
        (kg_key("infores:rtx-kg2", "A:1", "B:1", "infores:semmeddb"), 0.5),
        (kg_key("infores:rtx-kg2", "A:1", "B:1", "infores:drugcentral"), 0.93),
        (kg_key("infores:rtx-kg2", "A:1", "B:1", "infores:ctd"), 0.5),
        # no infores in the last segment: virtual/inferred edge, no base score
        ("N1_3", 0),
        ("creative_DTD_prediction_0", 0),
    ],
)
def test_base_weight_from_last_key_segment(key, expected):
    edge = {"attributes": [], "sources": [{"resource_id": "infores:drugbank"}]}
    assert ARAXRanker().edge_attribute_score_combiner(key, edge) == expected


def test_score_stats_falsy_zero_quirk():
    """ARAX treats a recorded 0 minimum as unset, so later values replace it."""
    envelope = golden_envelope()
    edges = envelope["message"]["knowledge_graph"]["edges"]
    for key, value in (("N1_1", 0.0), ("N1_2", 0.4), ("N1_3", 0.2)):
        edges[key]["attributes"][0]["value"] = value
    ranker = ARAXRanker()
    ranker.aggregate_scores_dmk(envelope, logger)
    stats = ranker.score_stats["normalized_google_distance"]
    assert stats == {"minimum": 0.2, "maximum": 0.4}


def test_ties_are_broken_in_descending_steps():
    envelope = golden_envelope()
    # make every result identical in shape and confidence
    edges = envelope["message"]["knowledge_graph"]["edges"]
    for edge in edges.values():
        edge["attributes"] = []
    for result in envelope["message"]["results"]:
        result["analyses"][0]["edge_bindings"]["e0"] = [{"id": DRUGBANK}]
    envelope = arax_rank(envelope, logger)
    scores = [r["analyses"][0]["score"] for r in envelope["message"]["results"]]
    assert scores == [1.0, 0.999, 0.998, 0.997]


def test_empty_results_still_set_table_column_names():
    envelope = golden_envelope()
    envelope["message"]["results"] = []
    envelope = arax_rank(envelope, logger)
    assert envelope["message"]["results"] == []
    assert envelope["table_column_names"] == ["score", "essence", "essence_category"]


def test_empty_semmeddb_publications_raise_like_arax():
    envelope = golden_envelope()
    edges = envelope["message"]["knowledge_graph"]["edges"]
    edges[SEMMED_7]["attributes"][0]["value"] = []
    with pytest.raises(UnboundLocalError):
        arax_rank(envelope, logger)


def test_dangling_edge_binding_raises_like_arax():
    envelope = golden_envelope()
    envelope["message"]["results"][0]["analyses"][0]["edge_bindings"]["e0"] = [
        {"id": "not-in-kg"}
    ]
    with pytest.raises(KeyError):
        arax_rank(envelope, logger)


def test_result_without_analysis_raises_like_arax():
    envelope = golden_envelope()
    envelope["message"]["results"][0]["analyses"] = []
    with pytest.raises(IndexError):
        arax_rank(envelope, logger)


def test_worker_propagates_ranking_errors():
    """The worker no longer swallows errors and returns the unranked message."""
    envelope = golden_envelope()
    envelope["message"]["results"][0]["analyses"] = []
    with pytest.raises(IndexError):
        rank_message(copy.deepcopy(envelope), logger)


def test_worker_ranks_and_stringifies_log_timestamps():
    envelope = golden_envelope()
    envelope["logs"] = [{"timestamp": 1234, "message": "x"}]
    ranked = rank_message(envelope, logger)
    assert ranked["logs"][0]["timestamp"] == "1234"
    assert ranked["message"]["results"][0]["essence"] == "drug 1"

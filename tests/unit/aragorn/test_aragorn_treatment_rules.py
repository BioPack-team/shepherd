"""Keep shared-regulator analogies out of treatment query expansions."""

import copy
import json
import logging
from pathlib import Path

import pytest

from tests.helpers.generate_messages import creative_query
from workers.aragorn_lookup import worker


def _uses_shared_regulator(query_graph, candidate, disease):
    """Recognize I -> A, I -> E, E -> D without relying on template node names."""
    edges = query_graph["edges"].values()
    candidate_regulators = {
        edge["subject"]
        for edge in edges
        if edge["object"] == candidate
        and "biolink:affects" in edge.get("predicates", [])
    }
    for treatment in edges:
        if (
            treatment["object"] != disease
            or treatment["subject"] == candidate
            or not {
                "biolink:treats",
                "biolink:treats_or_applied_or_studied_to_treat",
            }.intersection(treatment.get("predicates", []))
        ):
            continue
        treatment_regulators = {
            edge["subject"]
            for edge in edges
            if edge["object"] == treatment["subject"]
            and "biolink:affects" in edge.get("predicates", [])
        }
        if candidate_regulators & treatment_regulators:
            return True
    return False


def test_treatment_rules_exclude_shared_regulator_analogies():
    rules_path = (
        Path(worker.__file__).parent / "rules_with_types_cleaned_finalized.json"
    )
    rules = json.loads(rules_path.read_text())
    treatment_rules = rules[json.dumps({"predicate": "biolink:treats"})]

    # Both co-increase and co-decrease variants make the same weak analogy.
    rejected = [
        rule["Rule"]
        for rule in treatment_rules
        if _uses_shared_regulator(rule["template"]["query_graph"], "$source", "$target")
    ]
    assert rejected == []


@pytest.mark.parametrize("pinned_node", ["SN", "ON"], ids=["chemical", "disease"])
def test_treatment_expansion_preserves_lookup_without_shared_regulator(pinned_node):
    message = copy.deepcopy(creative_query)
    nodes = message["message"]["query_graph"]["nodes"]
    nodes["ON"].pop("ids")
    nodes[pinned_node]["ids"] = [
        "CHEBI:63115" if pinned_node == "SN" else "MONDO:0005301"
    ]
    message["parameters"] = {"timeout": 60, "tiers": [0]}
    message["submitter"] = "test"
    original = copy.deepcopy(message)

    expanded = worker.expand_aragorn_query(message, logging.getLogger(__name__))

    # Eleven retained templates plus the original direct treatment lookup.
    assert len(expanded) == 12
    direct_query = copy.deepcopy(original["message"]["query_graph"])
    direct_query["edges"]["e0"].pop("knowledge_type")
    assert expanded[0]["message"]["query_graph"] == direct_query
    unpinned_node = "ON" if pinned_node == "SN" else "SN"
    for lookup in expanded:
        query_graph = lookup["message"]["query_graph"]
        assert not _uses_shared_regulator(query_graph, "SN", "ON")
        assert query_graph["nodes"][pinned_node]["ids"] == nodes[pinned_node]["ids"]
        assert "ids" not in query_graph["nodes"][unpinned_node]
        assert lookup["parameters"] == original["parameters"]
        assert lookup["submitter"] == original["submitter"]
    assert message == original

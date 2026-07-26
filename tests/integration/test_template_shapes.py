"""Execute every rendered query template through ``gandalf.search.lookup``.

The unit tests check that templates render to the query graphs we intend. They
cannot check that Gandalf *accepts* those graphs, because Gandalf is a separate
service -- and a template that renders beautifully but is rejected at lookup is
worse than no template. This closes that gap: it builds a small fixture graph
carrying one instance of every shape the portfolio asks for, renders each
template exactly as the Aragorn lookup worker does (the caller's qnode keys,
the caller's broader pinned category), and executes it.

It needs a Gandalf checkout importable, so it skips by default and runs when
one is on the path::

    PYTHONPATH=/path/to/gandalf pytest tests/integration/test_template_shapes.py

Run it after changing any template's shape.
"""

import json
import os
import tempfile

import pytest

os.environ.setdefault("GANDALF_SKIP_PRELOAD", "true")
os.environ.setdefault("GANDALF_OTEL_ENABLED", "false")

build_graph_from_jsonl = pytest.importorskip(
    "gandalf.loader", reason="needs a Gandalf checkout on the path"
).build_graph_from_jsonl
lookup = pytest.importorskip("gandalf.search.lookup").lookup

from workers.aragorn_lookup.query_templates import TEMPLATES  # noqa: E402

DISEASE = "MONDO:0004979"
OTHER_DISEASE = "MONDO:0009999"
PROTEIN_A = "UniProtKB:P00001"
PROTEIN_B = "UniProtKB:P00002"
PARTNER = "UniProtKB:P00003"
CAUSAL = "UniProtKB:P00004"
SM = "CHEBI:100001"
DRUG = "CHEBI:200001"
PATHWAY = "REACT:R-HSA-1"
PHENOTYPE = "HP:0001250"

NODES = [
    (DISEASE, "Asthma", ["biolink:Disease"]),
    (OTHER_DISEASE, "Other disease", ["biolink:Disease"]),
    (PROTEIN_A, "Protein A", ["biolink:Protein"]),
    (PROTEIN_B, "Protein B", ["biolink:Protein"]),
    (PARTNER, "Partner", ["biolink:Protein"]),
    (CAUSAL, "Causal protein", ["biolink:Protein"]),
    (SM, "Small molecule", ["biolink:SmallMolecule"]),
    (DRUG, "A drug", ["biolink:Drug"]),
    (PATHWAY, "Pathway", ["biolink:Pathway"]),
    (PHENOTYPE, "Seizure", ["biolink:PhenotypicFeature"]),
]

DECREASED = [
    ("biolink:object_direction_qualifier", "decreased"),
    ("biolink:qualified_predicate", "biolink:causes"),
]
INCREASED = [
    ("biolink:object_direction_qualifier", "increased"),
    ("biolink:qualified_predicate", "biolink:causes"),
]
INHIBITION = [("biolink:causal_mechanism_qualifier", "inhibition")]

EDGES = [
    # disease entry hops
    (DISEASE, "biolink:associated_with", PROTEIN_A, []),
    (DISEASE, "biolink:associated_with", PROTEIN_B, []),
    (DISEASE, "biolink:associated_with", DRUG, []),
    (DISEASE, "biolink:has_phenotype", PHENOTYPE, []),
    (OTHER_DISEASE, "biolink:has_phenotype", PHENOTYPE, []),
    (CAUSAL, "biolink:contributes_to", DISEASE, []),
    (CAUSAL, "biolink:causes", DISEASE, []),
    # drug-side hops
    (SM, "biolink:affects", PROTEIN_A, DECREASED),
    (SM, "biolink:affects", PROTEIN_B, DECREASED),
    (SM, "biolink:affects", CAUSAL, DECREASED),
    (SM, "biolink:affects", PARTNER, DECREASED),
    (SM, "biolink:affects", PROTEIN_A, INCREASED),
    (SM, "biolink:affects", PROTEIN_A, INHIBITION),
    (DRUG, "biolink:affects", PROTEIN_A, DECREASED),
    (PROTEIN_A, "biolink:physically_interacts_with", SM, []),
    (PROTEIN_A, "biolink:physically_interacts_with", PARTNER, []),
    (PROTEIN_A, "biolink:participates_in", PATHWAY, []),
    (PATHWAY, "biolink:has_participant", SM, []),
    (PHENOTYPE, "biolink:associated_with", DRUG, []),
    (DRUG, "biolink:treats_or_applied_or_studied_to_treat", OTHER_DISEASE, []),
]


def write_fixtures(directory):
    nodes_path = os.path.join(directory, "nodes.jsonl")
    edges_path = os.path.join(directory, "edges.jsonl")
    with open(nodes_path, "w") as handle:
        for node_id, name, categories in NODES:
            handle.write(
                json.dumps(
                    {
                        "id": node_id,
                        "name": name,
                        "category": categories,
                        "equivalent_identifiers": [node_id],
                        "information_content": 90.0,
                    }
                )
                + "\n"
            )
    with open(edges_path, "w") as handle:
        for index, (subject, predicate, obj, qualifiers) in enumerate(EDGES):
            edge = {
                "id": f"e{index}",
                "subject": subject,
                "predicate": predicate,
                "object": obj,
                "sources": [
                    {
                        "resource_role": "primary_knowledge_source",
                        "resource_id": "infores:test",
                    }
                ],
                "knowledge_level": "knowledge_assertion",
                "agent_type": "manual_agent",
            }
            for type_id, value in qualifiers:
                edge[type_id.replace("biolink:", "")] = value
            handle.write(json.dumps(edge) + "\n")
    return edges_path, nodes_path


@pytest.fixture(scope="module")
def graph():
    with tempfile.TemporaryDirectory() as directory:
        edges_path, nodes_path = write_fixtures(directory)
        yield build_graph_from_jsonl(edges_path, nodes_path)


# Exactly what the worker renders: the caller's qnode keys, and the pinned node
# keeping the caller's broader category rather than the template's Disease.
PINNED_NODE = {
    "categories": ["biolink:DiseaseOrPhenotypicFeature"],
    "set_interpretation": "BATCH",
}
ANSWER_NODE = {"set_interpretation": "BATCH"}


@pytest.mark.parametrize("template", TEMPLATES, ids=lambda t: t.name)
def test_gandalf_accepts_the_rendered_template(template, graph):
    query_graph = template.render(
        DISEASE, "ON", "SN", pinned_node=PINNED_NODE, answer_node=ANSWER_NODE
    )
    response = lookup(
        graph,
        {"message": {"query_graph": query_graph}},
        filter_config=template.filter_config or None,
    )

    results = response["message"].get("results") or []
    assert results, f"{template.name} was accepted but returned nothing"
    # Every result must answer on the caller's answer qnode.
    for result in results:
        assert result["node_bindings"].get("SN")


def test_two_witness_returns_the_degenerate_pairs_it_documents(graph):
    """The defect in the template's notes, pinned to the behaviour that causes
    it: two associated proteins yield 2x2 ordered pairs, two of them degenerate.
    merge_message drops the degenerate ones and collapses the mirrored pair --
    see tests/unit/aragorn/test_aragorn_lookup_templates.py."""
    template = next(t for t in TEMPLATES if t.name == "two_witness_inhibition")
    query_graph = template.render(DISEASE, "ON", "SN", pinned_node=PINNED_NODE)
    response = lookup(graph, {"message": {"query_graph": query_graph}})

    results = response["message"]["results"]
    assert len(results) == 4
    degenerate = [
        result
        for result in results
        if result["node_bindings"]["n_protein_a"][0]["id"]
        == result["node_bindings"]["n_protein_b"][0]["id"]
    ]
    assert len(degenerate) == 2

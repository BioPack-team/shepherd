"""Write mock versions of the ARAX port's data files, for testing locally.

The real files (DEC-6 in docs/ARAX_PORT_BASELINE.md) come from the ARAX team.
Until they are available, this writes small files in exactly their shape --
the same filenames (from Shepherd's settings, so the workers' startup
downloads find them and skip), tables, columns and value formats -- that
every reader in the port accepts:

- ``curie_to_pmids``: NGD overlay, ``add_node_pmids``, xCRG
- ``ExplainableDTD``: Infer (xDTD) scores, paths, node and edge mappings
- ``COHDdatabase``: the clinical-info overlay
- ``fda_approved_drugs``: Expand's FDA-approval constraint
- ``autocomplete``: the UI's node-name autocomplete
- with ``--pathfinder``, also ``curie_ngd`` and the tier0 overlay file
  (Connect, xCRG, the Fisher exact test), which normally download from
  kg2webhost.rtx.ai

The content is built from a small seed of real, well-known curies, plus the
nodes and edges of any saved TRAPI responses given with ``--from-trapi`` (a
response from Retriever or from Shepherd's ``/arax/query``), so local queries
over those nodes get overlay values, xDTD predictions and autocomplete terms.
The values are made up (deterministically: the same inputs give the same
files) but consistent with each other: NGD values are computed from the PMID
sets, and every xDTD path step is an edge in its own edge mapping.

The clinical-info overlay maps curies to OMOP ids through the live cohd.io
API. So that its answers land in the mock COHD database, the generator asks
cohd.io for the same mapping (``--no-network`` skips it and makes up ids,
which then match nothing at query time).

Usage::

    python -m shepherd_utils.arax_mock_data [--pathfinder] [--from-trapi FILE ...]
        [--out DIR] [--pathfinder-out DIR] [--force] [--no-network]
"""

import argparse
import hashlib
import json
import logging
import math
import os
import pickle
import random
import sqlite3
import sys
from itertools import combinations
from typing import Dict, Iterable, List, Optional, Tuple

from shepherd_utils.config import settings
from shepherd_utils.data_download import (
    ARAX_AUTOCOMPLETE,
    ARAX_COHD,
    ARAX_CURIE_TO_PMIDS,
    ARAX_EXPLAINABLE_DTD,
    ARAX_FDA_APPROVED_DRUGS,
    arax_db_filename,
    arax_pathfinder_sqlite_paths,
)

LOGGER = logging.getLogger("shepherd.arax_mock_data")

# ---------------------------------------------------------------------------
# Seed graph: real curies, so queries against a real Retriever touch them.
# The relations are plausible, not curated facts -- this is mock data.
# ---------------------------------------------------------------------------

SM = "biolink:SmallMolecule"
DIS = "biolink:Disease"
PHE = "biolink:PhenotypicFeature"
GENE = "biolink:Gene"

SEED_NODES: Dict[str, Tuple[str, str]] = {
    "MONDO:0005148": ("type 2 diabetes mellitus", DIS),
    "MONDO:0005015": ("diabetes mellitus", DIS),
    "MONDO:0004979": ("asthma", DIS),
    "MONDO:0007254": ("breast cancer", DIS),
    "MONDO:0004975": ("Alzheimer disease", DIS),
    "MONDO:0005068": ("myocardial infarction", DIS),
    "MONDO:0005044": ("hypertensive disorder", DIS),
    "HP:0001945": ("Fever", PHE),
    "HP:0002094": ("Dyspnea", PHE),
    "HP:0000822": ("Hypertension", PHE),
    "CHEBI:6801": ("metformin", SM),
    "CHEBI:15365": ("acetylsalicylic acid", SM),
    "CHEBI:5855": ("ibuprofen", SM),
    "CHEBI:41879": ("dexamethasone", SM),
    "CHEBI:8746": ("salbutamol", SM),
    "CHEBI:45783": ("imatinib", SM),
    "CHEBI:39548": ("atorvastatin", SM),
    "CHEBI:43755": ("lisinopril", SM),
    "NCBIGene:5468": ("PPARG", GENE),
    "NCBIGene:3630": ("INS", GENE),
    "NCBIGene:7124": ("TNF", GENE),
    "NCBIGene:5743": ("PTGS2", GENE),
    "NCBIGene:25": ("ABL1", GENE),
    "NCBIGene:2099": ("ESR1", GENE),
    "NCBIGene:348": ("APOE", GENE),
    "NCBIGene:5465": ("PPARA", GENE),
    "NCBIGene:1636": ("ACE", GENE),
    "NCBIGene:154": ("ADRB2", GENE),
}

SEED_EDGES: List[Tuple[str, str, str, str]] = [
    # (subject, predicate, object, primary knowledge source)
    ("CHEBI:6801", "biolink:treats", "MONDO:0005148", "infores:drugcentral"),
    ("CHEBI:6801", "biolink:affects", "NCBIGene:5468", "infores:ctd"),
    ("CHEBI:15365", "biolink:affects", "NCBIGene:5743", "infores:ctd"),
    ("CHEBI:15365", "biolink:treats", "MONDO:0005068", "infores:drugcentral"),
    ("CHEBI:15365", "biolink:treats", "HP:0001945", "infores:semmeddb"),
    ("CHEBI:5855", "biolink:affects", "NCBIGene:5743", "infores:ctd"),
    ("CHEBI:5855", "biolink:treats", "HP:0001945", "infores:drugcentral"),
    ("CHEBI:41879", "biolink:treats", "MONDO:0004979", "infores:drugcentral"),
    ("CHEBI:41879", "biolink:affects", "NCBIGene:7124", "infores:ctd"),
    ("CHEBI:8746", "biolink:treats", "MONDO:0004979", "infores:drugcentral"),
    ("CHEBI:8746", "biolink:affects", "NCBIGene:154", "infores:ctd"),
    ("CHEBI:45783", "biolink:affects", "NCBIGene:25", "infores:ctd"),
    ("CHEBI:45783", "biolink:applied_to_treat", "MONDO:0007254", "infores:semmeddb"),
    ("CHEBI:39548", "biolink:treats", "MONDO:0005068", "infores:drugcentral"),
    ("CHEBI:39548", "biolink:affects", "NCBIGene:348", "infores:ctd"),
    ("CHEBI:43755", "biolink:treats", "MONDO:0005044", "infores:drugcentral"),
    ("CHEBI:43755", "biolink:affects", "NCBIGene:1636", "infores:ctd"),
    ("CHEBI:5855", "biolink:affects", "NCBIGene:5468", "infores:ctd"),
    ("CHEBI:39548", "biolink:affects", "NCBIGene:5465", "infores:ctd"),
    (
        "NCBIGene:3630",
        "biolink:gene_associated_with_condition",
        "MONDO:0005148",
        "infores:ctd",
    ),
    (
        "NCBIGene:5465",
        "biolink:gene_associated_with_condition",
        "MONDO:0005148",
        "infores:ctd",
    ),
    (
        "NCBIGene:5468",
        "biolink:gene_associated_with_condition",
        "MONDO:0005148",
        "infores:ctd",
    ),
    (
        "NCBIGene:5468",
        "biolink:gene_associated_with_condition",
        "MONDO:0005015",
        "infores:ctd",
    ),
    (
        "NCBIGene:7124",
        "biolink:gene_associated_with_condition",
        "MONDO:0004979",
        "infores:ctd",
    ),
    (
        "NCBIGene:154",
        "biolink:gene_associated_with_condition",
        "MONDO:0004979",
        "infores:ctd",
    ),
    (
        "NCBIGene:2099",
        "biolink:gene_associated_with_condition",
        "MONDO:0007254",
        "infores:ctd",
    ),
    (
        "NCBIGene:25",
        "biolink:gene_associated_with_condition",
        "MONDO:0007254",
        "infores:ctd",
    ),
    (
        "NCBIGene:348",
        "biolink:gene_associated_with_condition",
        "MONDO:0004975",
        "infores:ctd",
    ),
    (
        "NCBIGene:1636",
        "biolink:gene_associated_with_condition",
        "MONDO:0005044",
        "infores:ctd",
    ),
    (
        "NCBIGene:5743",
        "biolink:gene_associated_with_condition",
        "MONDO:0005068",
        "infores:ctd",
    ),
    ("MONDO:0004979", "biolink:has_phenotype", "HP:0002094", "infores:hpo-annotations"),
    ("MONDO:0005068", "biolink:has_phenotype", "HP:0002094", "infores:hpo-annotations"),
    ("MONDO:0005044", "biolink:has_phenotype", "HP:0000822", "infores:hpo-annotations"),
    ("NCBIGene:7124", "biolink:interacts_with", "NCBIGene:5743", "infores:string"),
    ("NCBIGene:5468", "biolink:interacts_with", "NCBIGene:5465", "infores:string"),
    ("NCBIGene:3630", "biolink:interacts_with", "NCBIGene:5468", "infores:string"),
]

# Ancestors of the categories the seed uses, for when BiolinkHelper can't load
# the model (offline). Retriever's nodes carry their ancestors already.
FALLBACK_ANCESTORS = {
    SM: [
        "biolink:SmallMolecule",
        "biolink:MolecularEntity",
        "biolink:ChemicalEntity",
        "biolink:PhysicalEssence",
        "biolink:ChemicalOrDrugOrTreatment",
        "biolink:ChemicalEntityOrGeneOrGeneProduct",
        "biolink:ChemicalEntityOrProteinOrPolypeptide",
        "biolink:NamedThing",
        "biolink:PhysicalEssenceOrOccurrent",
    ],
    DIS: [
        "biolink:Disease",
        "biolink:DiseaseOrPhenotypicFeature",
        "biolink:BiologicalEntity",
        "biolink:ThingWithTaxon",
        "biolink:NamedThing",
    ],
    PHE: [
        "biolink:PhenotypicFeature",
        "biolink:DiseaseOrPhenotypicFeature",
        "biolink:BiologicalEntity",
        "biolink:ThingWithTaxon",
        "biolink:NamedThing",
    ],
    GENE: [
        "biolink:Gene",
        "biolink:GeneOrGeneProduct",
        "biolink:MacromolecularMachineMixin",
        "biolink:GenomicEntity",
        "biolink:ChemicalEntityOrGeneOrGeneProduct",
        "biolink:PhysicalEssence",
        "biolink:OntologyClass",
        "biolink:BiologicalEntity",
        "biolink:ThingWithTaxon",
        "biolink:NamedThing",
        "biolink:PhysicalEssenceOrOccurrent",
    ],
}

DRUG_CATEGORIES = {
    "biolink:SmallMolecule",
    "biolink:Drug",
    "biolink:ChemicalEntity",
    "biolink:MolecularMixture",
    "biolink:ComplexMolecularMixture",
}
DISEASE_CATEGORIES = {
    "biolink:Disease",
    "biolink:PhenotypicFeature",
    "biolink:DiseaseOrPhenotypicFeature",
}
# The categories ARAX's clinical-info overlay decorates
COHD_CATEGORIES = {
    "biolink:SmallMolecule",
    "biolink:PhenotypicFeature",
    "biolink:Disease",
    "biolink:Drug",
}

# PubMed-sized background, so NGD values fall in the usual range
PMID_SPACE = 30_000_000
# How many nodes the mock tier0 "KG" pretends to have beyond the real ones, per
# category, so Fisher exact test tables look like a large KG's
BACKGROUND_NODES = 100_000
# Caps so a big --from-trapi input still gives small files
MAX_XDTD_PAIRS = 2000
MAX_PATHS_PER_PAIR = 25
MAX_COHD_PAIRS = 20000


def _rng(*parts) -> random.Random:
    """A generator seeded by its arguments (not the hash seed)."""
    digest = hashlib.sha256("\x1f".join(map(str, parts)).encode()).hexdigest()
    return random.Random(int(digest[:16], 16))


# ---------------------------------------------------------------------------
# The graph the files are built from
# ---------------------------------------------------------------------------


class MockGraph:
    def __init__(self):
        self.nodes: Dict[str, dict] = {}  # id -> {"name", "categories"}
        self.edges: Dict[str, dict] = (
            {}
        )  # key -> {"subject", "predicate", "object", "sources", "publications"}

    def add_node(self, curie, name, categories):
        node = self.nodes.setdefault(curie, {"name": name or curie, "categories": []})
        if name and node["name"] == curie:
            node["name"] = name
        for category in categories or []:
            if category not in node["categories"]:
                node["categories"].append(category)

    def add_edge(self, key, subject, predicate, obj, sources, publications=()):
        if subject not in self.nodes or obj not in self.nodes or subject == obj:
            return
        self.edges.setdefault(
            key,
            {
                "subject": subject,
                "predicate": predicate or "biolink:related_to",
                "object": obj,
                "sources": sources
                or [
                    {
                        "resource_id": "infores:mock",
                        "resource_role": "primary_knowledge_source",
                    }
                ],
                "publications": list(publications),
            },
        )

    @classmethod
    def seed(cls) -> "MockGraph":
        graph = cls()
        for curie, (name, category) in SEED_NODES.items():
            graph.add_node(curie, name, [category])
        for i, (s, p, o, source) in enumerate(SEED_EDGES):
            graph.add_edge(
                f"mock_seed_edge_{i}",
                s,
                p,
                o,
                [
                    {
                        "resource_id": source,
                        "resource_role": "primary_knowledge_source",
                    },
                    {
                        "resource_id": "infores:retriever",
                        "resource_role": "aggregator_knowledge_source",
                        "upstream_resource_ids": [source],
                    },
                ],
            )
        return graph

    def add_trapi(self, response: dict) -> Tuple[int, int]:
        """Add the knowledge graph of a TRAPI response (or message)."""
        message = (
            response.get("message", response) if isinstance(response, dict) else {}
        )
        kg = (message or {}).get("knowledge_graph") or {}
        n_nodes = n_edges = 0
        for curie, node in (kg.get("nodes") or {}).items():
            self.add_node(
                curie, (node or {}).get("name"), (node or {}).get("categories")
            )
            n_nodes += 1
        for key, edge in (kg.get("edges") or {}).items():
            edge = edge or {}
            publications = []
            for attribute in edge.get("attributes") or []:
                if attribute.get(
                    "attribute_type_id"
                ) == "biolink:publications" and isinstance(
                    attribute.get("value"), list
                ):
                    publications = [p for p in attribute["value"] if isinstance(p, str)]
            before = len(self.edges)
            self.add_edge(
                key,
                edge.get("subject"),
                edge.get("predicate"),
                edge.get("object"),
                [
                    s
                    for s in edge.get("sources") or []
                    if isinstance(s, dict) and s.get("resource_id")
                ],
                publications,
            )
            n_edges += len(self.edges) - before
        return n_nodes, n_edges

    def neighbors(self) -> Dict[str, set]:
        out: Dict[str, set] = {curie: set() for curie in self.nodes}
        for edge in self.edges.values():
            out[edge["subject"]].add(edge["object"])
            out[edge["object"]].add(edge["subject"])
        return out

    def is_drug(self, curie):
        return bool(DRUG_CATEGORIES.intersection(self.nodes[curie]["categories"]))

    def is_disease(self, curie):
        return bool(DISEASE_CATEGORIES.intersection(self.nodes[curie]["categories"]))


def _category_ancestors() -> Tuple[Dict[str, List[str]], List[str]]:
    """(ancestors of a category, every Biolink category) from the Biolink
    model the port uses, else the fallback table."""
    try:
        from shepherd_utils.arax.BiolinkHelper.biolink_helper import get_biolink_helper

        bh = get_biolink_helper()
        all_categories = sorted(
            bh.get_descendants("biolink:NamedThing", include_mixins=True)
        )

        def ancestors(category):
            return bh.get_ancestors(category, include_mixins=True)

        return {c: ancestors(c) for c in all_categories}, all_categories
    except Exception as e:  # offline, or the model can't be fetched
        LOGGER.warning(
            f"Biolink model unavailable ({type(e).__name__}: {e}); using built-in category ancestors"
        )
        all_categories = sorted(
            {a for ancestors in FALLBACK_ANCESTORS.values() for a in ancestors}
        )
        return dict(FALLBACK_ANCESTORS), all_categories


# ---------------------------------------------------------------------------
# PMIDs, shared along edges so NGD is finite for connected nodes
# ---------------------------------------------------------------------------


def build_pmids(graph: MockGraph) -> Dict[str, List[int]]:
    pmids: Dict[str, set] = {}
    for curie in sorted(graph.nodes):
        r = _rng("pmids", curie)
        pmids[curie] = {r.randrange(1, PMID_SPACE) for _ in range(r.randint(20, 400))}
    for key in sorted(graph.edges):
        edge = graph.edges[key]
        r = _rng("shared", edge["subject"], edge["object"])
        shared = {r.randrange(1, PMID_SPACE) for _ in range(r.randint(3, 60))}
        for publication in edge["publications"]:
            if publication.upper().startswith("PMID:") and publication[5:].isdigit():
                shared.add(int(publication[5:]))
        pmids[edge["subject"]] |= shared
        pmids[edge["object"]] |= shared
    return {curie: sorted(ids) for curie, ids in pmids.items()}


def ngd(a: set, b: set, n: float = 3.5e7 * 20) -> float:
    """ARAX's normalized Google distance formula (compute_ngd.py)."""
    both = len(a & b)
    if not a or not b or both == 0:
        return float("inf")
    fa, fb = math.log(len(a)), math.log(len(b))
    return (max(fa, fb) - math.log(both)) / (math.log(n) - min(fa, fb))


# ---------------------------------------------------------------------------
# The files
# ---------------------------------------------------------------------------


def _fresh_sqlite(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    if os.path.exists(path):
        os.remove(path)
    return sqlite3.connect(path)


def write_curie_to_pmids(path: str, pmids: Dict[str, List[int]]) -> None:
    """curie_to_pmids(curie, pmids): pmids is a JSON list of integers."""
    con = _fresh_sqlite(path)
    con.execute("CREATE TABLE curie_to_pmids (curie TEXT PRIMARY KEY, pmids TEXT)")
    con.executemany(
        "INSERT INTO curie_to_pmids VALUES (?, ?)",
        [(curie, json.dumps(ids)) for curie, ids in sorted(pmids.items())],
    )
    con.commit()
    con.close()


def write_curie_ngd(path: str, graph: MockGraph, pmids: Dict[str, List[int]]) -> None:
    """curie_ngd(curie, ngd, pmid_length): ngd is a list of [neighbor, ngd]
    pairs, readable both as JSON (xCRG) and by ast.literal_eval (pathfinder)."""
    sets = {curie: set(ids) for curie, ids in pmids.items()}
    con = _fresh_sqlite(path)
    con.execute(
        "CREATE TABLE curie_ngd (curie TEXT PRIMARY KEY, ngd TEXT, pmid_length INTEGER)"
    )
    rows = []
    for curie, neighbors in sorted(graph.neighbors().items()):
        pairs = []
        for neighbor in sorted(neighbors):
            value = ngd(sets[curie], sets[neighbor])
            if math.isfinite(value):
                pairs.append([neighbor, round(value, 6)])
        rows.append((curie, json.dumps(pairs), len(sets[curie])))
    con.executemany("INSERT INTO curie_ngd VALUES (?, ?, ?)", rows)
    con.commit()
    con.close()


def write_tier0(path: str, graph: MockGraph) -> None:
    """tier0-info-for-overlay: neighbors(id, neighbor_counts JSON by category)
    and category_counts(category, count) for every Biolink category."""
    ancestors, all_categories = _category_ancestors()

    def expanded(curie):
        out = set()
        for category in graph.nodes[curie]["categories"]:
            out.update(ancestors.get(category, [category]))
        out.add("biolink:NamedThing")
        return out

    categories_of = {curie: expanded(curie) for curie in graph.nodes}
    con = _fresh_sqlite(path)
    con.execute("CREATE TABLE neighbors (id TEXT PRIMARY KEY, neighbor_counts TEXT)")
    con.execute(
        "CREATE TABLE category_counts (category TEXT PRIMARY KEY, count INTEGER)"
    )
    rows = []
    for curie, neighbors in sorted(graph.neighbors().items()):
        counts: Dict[str, int] = {}
        for neighbor in neighbors:
            for category in categories_of[neighbor]:
                counts[category] = counts.get(category, 0) + 1
        # a KG-sized degree on top of the real neighbors
        r = _rng("degree", curie)
        for category in list(counts):
            counts[category] += r.randint(0, 200)
        rows.append((curie, json.dumps(dict(sorted(counts.items())))))
    con.executemany("INSERT INTO neighbors VALUES (?, ?)", rows)
    totals = {category: BACKGROUND_NODES for category in all_categories}
    for cats in categories_of.values():
        for category in cats:
            totals[category] = totals.get(category, BACKGROUND_NODES) + 1
    totals["biolink:NamedThing"] = max(totals.values()) * 10
    con.executemany("INSERT INTO category_counts VALUES (?, ?)", sorted(totals.items()))
    con.commit()
    con.close()


def cohd_omop_ids(curies: List[str], network: bool) -> Dict[str, int]:
    """curie -> OMOP concept id: cohd.io's own mapping when reachable (it is
    what the overlay asks at query time), else made-up ids."""
    mapping: Dict[str, int] = {}
    if network and curies:
        try:
            import requests

            for i in range(0, len(curies), 200):
                chunk = curies[i : i + 200]
                response = requests.post(
                    "https://cohd.io/api/translator/biolink_to_omop",
                    data=json.dumps({"curies": chunk}),
                    headers={
                        "accept": "application/json",
                        "Content-Type": "application/json",
                    },
                    timeout=30,
                )
                response.raise_for_status()
                for curie, value in response.json().items():
                    if value and value.get("omop_concept_id") is not None:
                        mapping[curie] = int(value["omop_concept_id"])
            LOGGER.info(
                f"cohd.io mapped {len(mapping)} of {len(curies)} curies to OMOP ids"
            )
        except Exception as e:
            LOGGER.warning(
                f"cohd.io lookup failed ({type(e).__name__}: {e}); using made-up OMOP ids"
            )
    for curie in curies:
        if curie not in mapping:
            # made up, outside the range of real OMOP concept ids
            mapping[curie] = 2_000_000_000 + _rng("omop", curie).randrange(
                1, 100_000_000
            )
    return mapping


def write_cohd(path: str, graph: MockGraph, network: bool) -> Dict[str, int]:
    """COHD's tables (COHDIndex.py), for the three datasets."""
    curies = sorted(
        c
        for c in graph.nodes
        if COHD_CATEGORIES.intersection(graph.nodes[c]["categories"])
    )
    omop = cohd_omop_ids(curies, network)
    ids = sorted(set(omop.values()))
    con = _fresh_sqlite(path)
    con.execute(
        "CREATE TABLE SINGLE_CONCEPT_COUNTS( dataset_id TINYINT, concept_id INT, concept_count INT, concept_prevalence FLOAT )"
    )
    con.execute(
        "CREATE TABLE CONCEPTS( concept_id INT PRIMARY KEY, concept_name VARCHAR(255), domain_id VARCHAR(255), vocabulary_id VARCHAR(255), concept_class_id VARCHAR(255), concept_code VARCHAR(255) )"
    )
    con.execute(
        "CREATE TABLE PATIENT_COUNT( dataset_id TINYINT PRIMARY KEY, count INT)"
    )
    con.execute(
        "CREATE TABLE DATASET( dataset_id TINYINT PRIMARY KEY, dataset_name VARCHAR(255), dataset_description VARCHAR(255))"
    )
    con.execute(
        "CREATE TABLE DOMAIN_CONCEPT_COUNTS( dataset_id TINYINT, domain_id VARCHAR(255), count INT)"
    )
    con.execute(
        "CREATE TABLE DOMAIN_PAIR_CONCEPT_COUNTS( dataset_id TINYINT, domain_id_1 VARCHAR(255), domain_id_2 VARCHAR(255), count INT)"
    )
    con.execute(
        "CREATE TABLE PAIRED_CONCEPT_COUNTS_ASSOCIATIONS( concept_pair_id VARCHAR(255), dataset_id TINYINT, concept_id_1 INT, concept_id_2 INT, concept_count INT, concept_prevalence FLOAT, chi_square_t FLOAT, chi_square_p FLOAT, expected_count FLOAT, ln_ratio FLOAT, rel_freq_1 FLOAT, rel_freq_2 FLOAT)"
    )
    names = {omop[c]: graph.nodes[c]["name"] for c in curies}
    domains = {omop[c]: ("Drug" if graph.is_drug(c) else "Condition") for c in curies}
    patients = 1_700_000
    for dataset_id, name in (
        (1, "5-year non-hierarchical"),
        (2, "Lifetime non-hierarchical"),
        (3, "5-year hierarchical"),
    ):
        con.execute(
            "INSERT INTO DATASET VALUES (?,?,?)",
            (dataset_id, f"Mock COHD {name}", "Mock data for local testing"),
        )
        con.execute("INSERT INTO PATIENT_COUNT VALUES (?,?)", (dataset_id, patients))
        singles = {}
        for cid in ids:
            n = _rng("single", dataset_id, cid).randint(100, 50_000)
            singles[cid] = n
            con.execute(
                "INSERT INTO SINGLE_CONCEPT_COUNTS VALUES (?,?,?,?)",
                (dataset_id, cid, n, n / patients),
            )
        for domain in sorted(set(domains.values())):
            con.execute(
                "INSERT INTO DOMAIN_CONCEPT_COUNTS VALUES (?,?,?)",
                (dataset_id, domain, sum(1 for d in domains.values() if d == domain)),
            )
        for n_pair, (a, b) in enumerate(combinations(ids, 2)):
            if n_pair >= MAX_COHD_PAIRS:
                break
            r = _rng("pair", dataset_id, a, b)
            observed = r.randint(1, max(2, min(singles[a], singles[b]) // 5))
            expected = singles[a] * singles[b] / patients
            ln_ratio = math.log(observed / expected) if expected > 0 else 0.0
            chi_t = (observed - expected) ** 2 / expected if expected > 0 else 0.0
            con.execute(
                "INSERT INTO PAIRED_CONCEPT_COUNTS_ASSOCIATIONS VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    f"{a}_{b}",
                    dataset_id,
                    a,
                    b,
                    observed,
                    observed / patients,
                    round(chi_t, 4),
                    round(math.exp(-min(chi_t, 700) / 2), 12),
                    round(expected, 4),
                    round(ln_ratio, 4),
                    round(observed / singles[a], 6),
                    round(observed / singles[b], 6),
                ),
            )
    for cid in ids:
        con.execute(
            "INSERT INTO CONCEPTS VALUES (?,?,?,?,?,?)",
            (
                cid,
                names[cid],
                domains[cid],
                "RxNorm" if domains[cid] == "Drug" else "SNOMED",
                "Ingredient" if domains[cid] == "Drug" else "Clinical Finding",
                str(cid),
            ),
        )
    for table, column in (
        ("SINGLE_CONCEPT_COUNTS", "concept_id"),
        ("PAIRED_CONCEPT_COUNTS_ASSOCIATIONS", "concept_id_1"),
        ("PAIRED_CONCEPT_COUNTS_ASSOCIATIONS", "concept_id_2"),
        ("PAIRED_CONCEPT_COUNTS_ASSOCIATIONS", "concept_pair_id"),
    ):
        con.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_{column} ON {table}({column})"
        )
    con.commit()
    con.close()
    return omop


def _paths(graph: MockGraph, drug: str, disease: str) -> List[List[str]]:
    """Directed 1- to 3-hop paths drug -> ... -> disease, as upstream's
    'A->pred->B->pred->C' elements."""
    out_edges: Dict[str, List[Tuple[str, str]]] = {}
    for key in sorted(graph.edges):
        edge = graph.edges[key]
        out_edges.setdefault(edge["subject"], []).append(
            (edge["predicate"], edge["object"])
        )
    found = []

    def walk(node, path, depth):
        if len(found) >= MAX_PATHS_PER_PAIR:
            return
        for predicate, target in out_edges.get(node, []):
            if target in path[::2]:
                continue
            step = path + [predicate, target]
            if target == disease:
                found.append(step)
            elif depth < 3:
                walk(target, step, depth + 1)

    walk(drug, [drug], 1)
    return found


def write_explainable_dtd(path: str, graph: MockGraph) -> None:
    """ExplainableDTD: prediction scores, explanation paths, and the node and
    edge mapping tables (ExplianableDTD_db.py, build_mapping_db.py)."""
    con = _fresh_sqlite(path)
    con.execute(
        "CREATE TABLE PREDICTION_SCORE_TABLE (drug_id VARCHAR(255), drug_name VARCHAR(255), disease_id VARCHAR(255), disease_name VARCHAR(255), tn_score FLOAT, tp_score FLOAT, unknown_score FLOAT)"
    )
    con.execute(
        "CREATE TABLE PATH_RESULT_TABLE (drug_id VARCHAR(255), drug_name VARCHAR(255), disease_id VARCHAR(255), disease_name VARCHAR(255), path VARCHAR(255), path_score FLOAT)"
    )
    con.execute(
        "CREATE TABLE NODE_MAPPING_TABLE (id TEXT NOT NULL, name TEXT, category TEXT, equivalent_identifiers TEXT, description TEXT, synonym TEXT, xref TEXT, chembl_natural_product TEXT, chembl_availability_type TEXT, chembl_black_box_warning TEXT)"
    )
    con.execute(
        "CREATE TABLE EDGE_MAPPING_TABLE (subject TEXT NOT NULL, predicate TEXT NOT NULL, object TEXT NOT NULL, id TEXT, category TEXT, qualifier TEXT, publications TEXT, sources TEXT, resource_id TEXT, resource_role TEXT, knowledge_level TEXT, agent_type TEXT, stage_qualifier TEXT, original_subject TEXT, original_object TEXT, extra_attributes TEXT)"
    )
    for curie in sorted(graph.nodes):
        node = graph.nodes[curie]
        con.execute(
            "INSERT INTO NODE_MAPPING_TABLE VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                curie,
                node["name"],
                json.dumps(node["categories"][:1] or ["biolink:NamedThing"]),
                json.dumps([curie]),
                f"Mock description of {node['name']}",
                json.dumps([node["name"]]),
                None,
                None,
                None,
                None,
            ),
        )
    for key in sorted(graph.edges):
        edge = graph.edges[key]
        sources = edge["sources"]
        con.execute(
            "INSERT INTO EDGE_MAPPING_TABLE VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                edge["subject"],
                edge["predicate"],
                edge["object"],
                key,
                json.dumps(["biolink:Association"]),
                None,
                json.dumps(edge["publications"]) if edge["publications"] else None,
                json.dumps(sources),
                "|".join(s.get("resource_id", "") for s in sources),
                "|".join(s.get("resource_role", "") for s in sources),
                "knowledge_assertion",
                "manual_agent",
                None,
                None,
                None,
                None,
            ),
        )
    drugs = sorted(c for c in graph.nodes if graph.is_drug(c))
    diseases = sorted(c for c in graph.nodes if graph.is_disease(c))
    # only pairs with an explanation path: a prediction without one is dropped
    # by the creative-mode filter (ResultTransformer), so it would only be noise
    pairs = [
        (d, s, paths) for d in drugs for s in diseases if (paths := _paths(graph, d, s))
    ][:MAX_XDTD_PAIRS]
    for drug, disease, paths in pairs:
        r = _rng("xdtd", drug, disease)
        tp = round(r.uniform(0.3, 0.99), 4)
        tn = round(r.uniform(0, 1 - tp), 4)
        con.execute(
            "INSERT INTO PREDICTION_SCORE_TABLE VALUES (?,?,?,?,?,?,?)",
            (
                drug,
                graph.nodes[drug]["name"],
                disease,
                graph.nodes[disease]["name"],
                tn,
                tp,
                round(1 - tp - tn, 4),
            ),
        )
        for p in paths:
            con.execute(
                "INSERT INTO PATH_RESULT_TABLE VALUES (?,?,?,?,?,?)",
                (
                    drug,
                    graph.nodes[drug]["name"],
                    disease,
                    graph.nodes[disease]["name"],
                    "->".join(p),
                    round(_rng("path", *p).uniform(0, 1), 4),
                ),
            )
    for table in ("PREDICTION_SCORE_TABLE", "PATH_RESULT_TABLE"):
        for column in ("drug_id", "drug_name", "disease_id", "disease_name"):
            con.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_{column} ON {table}({column})"
            )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_NODE_MAPPING_TABLE_id ON NODE_MAPPING_TABLE(id)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_EDGE_MAPPING_TABLE_triple ON EDGE_MAPPING_TABLE(subject, predicate, object)"
    )
    con.commit()
    con.close()


def write_fda_approved(path: str, graph: MockGraph) -> None:
    """A pickled set of FDA-approved drug curies: the seed's drugs and about
    half of the other drug-like nodes."""
    approved = {
        c
        for c in graph.nodes
        if graph.is_drug(c) and (c in SEED_NODES or _rng("fda", c).random() < 0.5)
    }
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(approved, f)


def write_autocomplete(path: str, graph: MockGraph) -> None:
    """terms(term): the node names (autocomplete/rtxcomplete.py)."""
    terms = sorted({node["name"] for node in graph.nodes.values() if node["name"]})
    con = _fresh_sqlite(path)
    con.execute("CREATE TABLE terms(term VARCHAR(255))")
    con.executemany("INSERT INTO terms VALUES (?)", [(t,) for t in terms])
    con.execute("CREATE INDEX idx_terms_term ON terms(term)")
    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def load_trapi_files(paths: Iterable[str]) -> List[dict]:
    out = []
    for path in paths:
        with open(path) as f:
            data = json.load(f)
        out.extend(data if isinstance(data, list) else [data])
    return out


def write_all(
    out_dir: str,
    graph: MockGraph,
    pathfinder_dir: Optional[str] = None,
    force: bool = False,
    network: bool = True,
) -> Dict[str, str]:
    """Write every file; returns {name: path}. Existing files are kept unless force."""
    targets = {
        ARAX_CURIE_TO_PMIDS: os.path.join(
            out_dir, arax_db_filename(ARAX_CURIE_TO_PMIDS)
        ),
        ARAX_EXPLAINABLE_DTD: os.path.join(
            out_dir, arax_db_filename(ARAX_EXPLAINABLE_DTD)
        ),
        ARAX_COHD: os.path.join(out_dir, arax_db_filename(ARAX_COHD)),
        ARAX_FDA_APPROVED_DRUGS: os.path.join(
            out_dir, arax_db_filename(ARAX_FDA_APPROVED_DRUGS)
        ),
        ARAX_AUTOCOMPLETE: os.path.join(out_dir, arax_db_filename(ARAX_AUTOCOMPLETE)),
    }
    if pathfinder_dir is not None:
        ngd_path, tier0_path = arax_pathfinder_sqlite_paths()
        targets["curie_ngd"] = os.path.join(pathfinder_dir, os.path.basename(ngd_path))
        targets["tier0_overlay"] = os.path.join(
            pathfinder_dir, os.path.basename(tier0_path)
        )
    existing = [p for p in targets.values() if os.path.exists(p)]
    if existing and not force:
        raise FileExistsError(
            "These files already exist (pass --force to replace them): "
            + ", ".join(existing)
        )
    pmids = build_pmids(graph)
    write_curie_to_pmids(targets[ARAX_CURIE_TO_PMIDS], pmids)
    write_explainable_dtd(targets[ARAX_EXPLAINABLE_DTD], graph)
    write_cohd(targets[ARAX_COHD], graph, network)
    write_fda_approved(targets[ARAX_FDA_APPROVED_DRUGS], graph)
    write_autocomplete(targets[ARAX_AUTOCOMPLETE], graph)
    if pathfinder_dir is not None:
        write_curie_ngd(targets["curie_ngd"], graph, pmids)
        write_tier0(targets["tier0_overlay"], graph)
    return targets


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m shepherd_utils.arax_mock_data",
        description="Write mock ARAX data files, in the real files' shape, for local testing.",
    )
    parser.add_argument(
        "--out",
        default=settings.arax_dbs_dir,
        help="where the ARAX data files go (default: ARAX_DBS_DIR, %(default)s)",
    )
    parser.add_argument(
        "--pathfinder",
        action="store_true",
        help="also write mock curie_ngd and tier0 overlay files (normally downloaded)",
    )
    parser.add_argument(
        "--pathfinder-out",
        default=settings.arax_pathfinder_dbs_dir,
        help="where those go (default: ARAX_PATHFINDER_DBS_DIR, %(default)s)",
    )
    parser.add_argument(
        "--from-trapi",
        nargs="+",
        default=[],
        metavar="FILE",
        help="saved TRAPI responses whose knowledge-graph nodes and edges the mocks should cover",
    )
    parser.add_argument(
        "--force", action="store_true", help="replace files that already exist"
    )
    parser.add_argument(
        "--no-network",
        action="store_true",
        help="don't ask cohd.io for OMOP ids (the clinical-info overlay then finds nothing)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    graph = MockGraph.seed()
    for response in load_trapi_files(args.from_trapi):
        n_nodes, n_edges = graph.add_trapi(response)
        LOGGER.info(f"added {n_nodes} nodes and {n_edges} edges from a TRAPI response")
    try:
        targets = write_all(
            args.out,
            graph,
            args.pathfinder_out if args.pathfinder else None,
            force=args.force,
            network=not args.no_network,
        )
    except FileExistsError as e:
        LOGGER.error(str(e))
        return 1
    LOGGER.info(f"Mock data for {len(graph.nodes)} nodes and {len(graph.edges)} edges:")
    for name, path in targets.items():
        LOGGER.info(f"  {name}: {path} ({os.path.getsize(path):,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

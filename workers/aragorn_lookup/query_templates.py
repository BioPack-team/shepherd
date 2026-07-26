"""Census-derived query templates for "what drugs may treat this disease".

This is the Shepherd port of ``scripts/query_templates.py`` on
``ranking-agent/gandalf`` (branch ``claude/metagraph-census-script-grwf83``).
The portfolio and the cost model come over unchanged; what is new here is that a
template renders against the *caller's* qnode keys rather than fixed ones, so
the expansions bind the same answer node as the original inferred query and
``merge_message`` can group them.

Every template was chosen against census numbers from the 28.7M-edge graph, not
from biomedical intuition.  Three findings did most of the shaping:

**Gene biology lives on ``biolink:Protein``.**  ``Disease -associated_with->
Protein`` has 99,582 edges over 8,832 diseases; the same shape against
``biolink:Gene`` has 1,329 over 436.  Every mechanism template pins Protein.

**The disease side has no direction qualifiers.**  Only HPO
``frequency_qualifier`` appears with a Disease subject, so the textbook reversal
template -- drug *decreases* what disease *increases* -- is not expressible.
``causal_gene_inhibition`` substitutes predicate-level causality.

**Qualifiers make the drug side cheaper *and* sharper.**  Constraining
``object_direction_qualifier=decreased`` on ``affects`` cuts drug-side fan-out
from ~148 to ~21 chemicals per protein while strengthening the claim.

Pricing reads the census TSVs from ``settings.census_dir``.  When that directory
is absent the per-template ``baseline`` below is used instead: those are the
numbers the census produced when the portfolio was built, carried over so the
worker still selects sensibly before the census ships alongside the graph.
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

logger = logging.getLogger(__name__)

DISEASE = "biolink:Disease"
CHEMICAL = "biolink:ChemicalEntity"
SMALL_MOLECULE = "biolink:SmallMolecule"
DRUG = "biolink:Drug"
PROTEIN = "biolink:Protein"
PATHWAY = "biolink:Pathway"
PHENOTYPE = "biolink:PhenotypicFeature"

DIRECTION = "biolink:object_direction_qualifier"
ASPECT = "biolink:object_aspect_qualifier"
MECHANISM = "biolink:causal_mechanism_qualifier"

# Predicates that assert the gene drives the disease, used where the census has
# no directional qualifier to offer.
CAUSAL_GENE = ("biolink:causes", "biolink:contributes_to")

# Tier firing order.  Budget selection walks this list and takes the cheapest
# templates within each tier first, so a tight budget keeps mechanism templates
# and sheds the broad ones -- never the other way round.
TIER_ORDER = (
    "A-mechanism",
    "B-broad",
    "C-associative",
    "D-leaky",
    "D-branching",
)

# Enough of the Biolink chemical hierarchy to tell whether a template's answer
# category is compatible with the one the caller asked for.  A template may
# narrow the request (caller wants ChemicalEntity, template pins SmallMolecule)
# but must never widen it (caller wants Drug, template pins SmallMolecule) --
# that would answer with things the caller excluded.  Categories missing from
# this map are treated as compatible rather than filtered out.
_CHEMICAL_ANCESTORS: dict[str, tuple[str, ...]] = {
    SMALL_MOLECULE: (
        SMALL_MOLECULE,
        "biolink:MolecularEntity",
        CHEMICAL,
        "biolink:ChemicalOrDrugOrTreatment",
        "biolink:NamedThing",
    ),
    DRUG: (
        DRUG,
        "biolink:MolecularMixture",
        "biolink:ChemicalMixture",
        CHEMICAL,
        "biolink:ChemicalOrDrugOrTreatment",
        "biolink:NamedThing",
    ),
    CHEMICAL: (
        CHEMICAL,
        "biolink:ChemicalOrDrugOrTreatment",
        "biolink:NamedThing",
    ),
}


# ---------------------------------------------------------------------------
# Template definition
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Hop:
    """One qedge: a census triple plus optional qualifier constraints.

    ``subject``/``object`` are template-local qnode keys.  The direction must
    match the census row the estimate is read from, since fan-out is not
    symmetric.
    """

    subject: str
    object: str
    predicates: tuple[str, ...]
    qualifiers: tuple[tuple[str, str], ...] = ()

    def qualifier_constraints(self) -> list[dict]:
        """TRAPI qualifier_constraints for this hop (AND within the set)."""
        if not self.qualifiers:
            return []
        return [
            {
                "qualifier_set": [
                    {"qualifier_type_id": type_id, "qualifier_value": value}
                    for type_id, value in self.qualifiers
                ]
            }
        ]


@dataclass(frozen=True)
class ProbeSpec:
    """The one-hop neighbourhood measurement a template's entry hop needs.

    Templates that share an entry hop share a probe, so the portfolio's four
    distinct disease-side hops cost four probe queries no matter how many
    templates are selected.
    """

    predicates: tuple[str, ...]
    category: str
    disease_is_subject: bool

    def key(self) -> str:
        direction = "out" if self.disease_is_subject else "in"
        return f"{direction}|{','.join(self.predicates)}|{self.category}"


@dataclass(frozen=True)
class Baseline:
    """What the census said when the portfolio was built.

    Used for selection when no census directory is mounted.  ``entry_fanout`` is
    only carried where §4/§3 of the handoff pins it down exactly; where it is
    ``None`` a probe measurement cannot be substituted into the baseline and the
    template keeps its static estimate (logged, not silently ignored).
    """

    expected_paths: int
    coverage: int
    entry_fanout: Optional[float] = None


@dataclass(frozen=True)
class QueryTemplate:
    """A named query shape, its mechanism claim, and how to execute it."""

    name: str
    tier: str
    mechanism: str
    categories: dict[str, str]
    hops: tuple[Hop, ...]
    baseline: Baseline
    pinned: str = "n_disease"
    answer: str = "n_chem"
    leaky: bool = False
    filter_config: dict = field(default_factory=dict)
    notes: str = ""

    def probe_spec(self, hop: Hop) -> Optional[ProbeSpec]:
        """The probe that measures ``hop``, or None if it is not an entry hop."""
        if hop.subject == self.pinned:
            return ProbeSpec(hop.predicates, self.categories[hop.object], True)
        if hop.object == self.pinned:
            return ProbeSpec(hop.predicates, self.categories[hop.subject], False)
        return None

    def probe_specs(self) -> list[ProbeSpec]:
        """Every distinct probe this template's entry hops need."""
        specs: dict[str, ProbeSpec] = {}
        for hop in self.hops:
            spec = self.probe_spec(hop)
            if spec is not None:
                specs.setdefault(spec.key(), spec)
        return list(specs.values())

    def answer_compatible(self, requested_categories: Sequence[str]) -> bool:
        """Whether this template's answer category is within what was asked for.

        An unpinned answer node with no categories accepts anything.
        """
        if not requested_categories:
            return True
        ancestors = _CHEMICAL_ANCESTORS.get(self.categories[self.answer])
        if ancestors is None:
            return True
        return any(category in ancestors for category in requested_categories)

    def render(
        self,
        disease_curie: str,
        question_qnode: str,
        answer_qnode: str,
        pinned_node: Optional[dict] = None,
        answer_node: Optional[dict] = None,
    ) -> dict:
        """Build the TRAPI query graph, bound to the caller's qnode keys.

        The pinned and answer nodes take the caller's keys so downstream merging
        groups these results with every other expansion; intermediate nodes keep
        their template-local names, renamed only if they would collide.

        The pinned node keeps the *caller's* categories rather than the
        template's ``biolink:Disease``: it is pinned by CURIE anyway, and
        forcing Disease would drop a query whose CURIE is typed as a
        PhenotypicFeature.  The answer node does take the template's category --
        narrowing the chemical side is the whole point of the portfolio.
        """
        rename = {self.pinned: question_qnode, self.answer: answer_qnode}
        taken = {question_qnode, answer_qnode}
        for key in self.categories:
            if key in rename:
                continue
            name = key
            suffix = 0
            while name in taken:
                suffix += 1
                name = f"{key}_{suffix}"
            rename[key] = name
            taken.add(name)

        nodes: dict[str, dict] = {}
        for key, category in self.categories.items():
            nodes[rename[key]] = {"categories": [category]}

        pinned = nodes[question_qnode]
        pinned["ids"] = [disease_curie]
        if pinned_node:
            if pinned_node.get("categories"):
                pinned["categories"] = list(pinned_node["categories"])
            if pinned_node.get("set_interpretation") is not None:
                pinned["set_interpretation"] = pinned_node["set_interpretation"]
        if answer_node and answer_node.get("set_interpretation") is not None:
            nodes[answer_qnode]["set_interpretation"] = answer_node[
                "set_interpretation"
            ]

        edges: dict[str, dict] = {}
        for index, hop in enumerate(self.hops):
            edge: dict = {
                "subject": rename[hop.subject],
                "object": rename[hop.object],
                "predicates": list(hop.predicates),
            }
            constraints = hop.qualifier_constraints()
            if constraints:
                edge["qualifier_constraints"] = constraints
            edges[f"e{index}"] = edge

        return {"nodes": nodes, "edges": edges}


# ---------------------------------------------------------------------------
# The portfolio
# ---------------------------------------------------------------------------

TEMPLATES: tuple[QueryTemplate, ...] = (
    # -- Tier A: mechanism, qualified --------------------------------------
    QueryTemplate(
        name="target_inhibition_sm",
        tier="A-mechanism",
        mechanism="A small molecule decreases the activity or abundance of a "
        "protein associated with the disease.",
        categories={
            "n_disease": DISEASE,
            "n_protein": PROTEIN,
            "n_chem": SMALL_MOLECULE,
        },
        hops=(
            Hop("n_disease", "n_protein", ("biolink:associated_with",)),
            Hop(
                "n_chem",
                "n_protein",
                ("biolink:affects",),
                ((DIRECTION, "decreased"),),
            ),
        ),
        baseline=Baseline(238, 8832, 99582 / 8832),
        filter_config={"max_node_degree": 500},
        notes="The workhorse. 8,832 diseases have the entry hop; the qualifier "
        "cuts drug-side fan-out from ~148 to ~21 per protein.",
    ),
    QueryTemplate(
        name="target_inhibition_drug",
        tier="A-mechanism",
        mechanism="An approved drug decreases the activity or abundance of a "
        "protein associated with the disease.",
        categories={"n_disease": DISEASE, "n_protein": PROTEIN, "n_chem": DRUG},
        hops=(
            Hop("n_disease", "n_protein", ("biolink:associated_with",)),
            Hop(
                "n_chem",
                "n_protein",
                ("biolink:affects",),
                ((DIRECTION, "decreased"),),
            ),
        ),
        baseline=Baseline(160, 8832, 99582 / 8832),
        filter_config={"max_node_degree": 500},
        notes="Same shape restricted to Drug (5,326 nodes): far smaller "
        "candidate pool, ~14 drugs per protein, and every hit is a real drug.",
    ),
    QueryTemplate(
        name="target_activation_sm",
        tier="A-mechanism",
        mechanism="A small molecule increases the activity or abundance of a "
        "protein associated with the disease -- the loss-of-function case.",
        categories={
            "n_disease": DISEASE,
            "n_protein": PROTEIN,
            "n_chem": SMALL_MOLECULE,
        },
        hops=(
            Hop("n_disease", "n_protein", ("biolink:associated_with",)),
            Hop(
                "n_chem",
                "n_protein",
                ("biolink:affects",),
                ((DIRECTION, "increased"),),
            ),
        ),
        baseline=Baseline(201, 8832, 99582 / 8832),
        filter_config={"max_node_degree": 500},
        notes="Mirror of target_inhibition_sm. Fire both: without disease-side "
        "direction the graph cannot say which way the protein should move.",
    ),
    QueryTemplate(
        name="causal_gene_inhibition",
        tier="A-mechanism",
        mechanism="A small molecule decreases a protein that causes or "
        "contributes to the disease.",
        categories={
            "n_disease": DISEASE,
            "n_protein": PROTEIN,
            "n_chem": SMALL_MOLECULE,
        },
        hops=(
            Hop("n_protein", "n_disease", CAUSAL_GENE),
            Hop(
                "n_chem",
                "n_protein",
                ("biolink:affects",),
                ((DIRECTION, "decreased"),),
            ),
        ),
        baseline=Baseline(134, 2436, 15393 / 2436),
        filter_config={"max_node_degree": 500},
        notes="The nearest thing to a reversal template this graph supports: "
        "causal direction from the predicate, drug direction from the "
        "qualifier. Narrower coverage (2,436 diseases) but the strongest claim.",
    ),
    QueryTemplate(
        name="inhibition_mechanism_sm",
        tier="A-mechanism",
        mechanism="A small molecule inhibits (by declared mechanism) a protein "
        "associated with the disease.",
        categories={
            "n_disease": DISEASE,
            "n_protein": PROTEIN,
            "n_chem": SMALL_MOLECULE,
        },
        hops=(
            Hop("n_disease", "n_protein", ("biolink:associated_with",)),
            Hop(
                "n_chem",
                "n_protein",
                ("biolink:affects",),
                ((MECHANISM, "inhibition"),),
            ),
        ),
        baseline=Baseline(403, 8832, 99582 / 8832),
        filter_config={"max_node_degree": 500},
        notes="causal_mechanism_qualifier=inhibition covers 176,212 edges over "
        "95,309 chemicals -- pharmacology rather than perturbation readout.",
    ),
    # -- Tier B: broad, unqualified ----------------------------------------
    QueryTemplate(
        name="target_binding_sm",
        tier="B-broad",
        mechanism="A small molecule physically binds a protein associated with "
        "the disease.",
        categories={
            "n_disease": DISEASE,
            "n_protein": PROTEIN,
            "n_chem": SMALL_MOLECULE,
        },
        hops=(
            Hop("n_disease", "n_protein", ("biolink:associated_with",)),
            Hop("n_protein", "n_chem", ("biolink:physically_interacts_with",)),
        ),
        baseline=Baseline(2096, 8832, 99582 / 8832),
        filter_config={"max_node_degree": 300},
        notes="Recall play: 1.18M binding edges, ~148 chemicals per protein. "
        "No direction, so it cannot tell a helper from a harmer -- but it is "
        "the widest mechanistically defensible net.",
    ),
    QueryTemplate(
        name="pathway_participation",
        tier="B-broad",
        mechanism="A chemical participates in a pathway that a "
        "disease-associated protein participates in.",
        categories={
            "n_disease": DISEASE,
            "n_protein": PROTEIN,
            "n_pathway": PATHWAY,
            "n_chem": SMALL_MOLECULE,
        },
        hops=(
            Hop("n_disease", "n_protein", ("biolink:associated_with",)),
            Hop("n_protein", "n_pathway", ("biolink:participates_in",)),
            Hop("n_pathway", "n_chem", ("biolink:has_participant",)),
        ),
        baseline=Baseline(2192, 8832, 99582 / 8832),
        filter_config={"max_node_degree": 200},
        notes="Reaches drugs that miss the disease protein but hit its pathway. "
        "Pathways are hubs -- the degree cap is doing real work here.",
    ),
    QueryTemplate(
        name="ppi_neighborhood",
        tier="B-broad",
        mechanism="A small molecule decreases a protein that physically "
        "interacts with a disease-associated protein.",
        categories={
            "n_disease": DISEASE,
            "n_protein": PROTEIN,
            "n_partner": PROTEIN,
            "n_chem": SMALL_MOLECULE,
        },
        hops=(
            Hop("n_disease", "n_protein", ("biolink:associated_with",)),
            Hop("n_protein", "n_partner", ("biolink:physically_interacts_with",)),
            Hop(
                "n_chem",
                "n_partner",
                ("biolink:affects",),
                ((DIRECTION, "decreased"),),
            ),
        ),
        baseline=Baseline(10986, 8832, 99582 / 8832),
        filter_config={"max_node_degree": 100},
        notes="The most explosive template in the set (~46 PPI partners per "
        "protein). Keep the degree cap tight or drop it for dense diseases.",
    ),
    # -- Tier C: non-mechanistic signal ------------------------------------
    QueryTemplate(
        name="phenotype_drug_bridge",
        tier="C-associative",
        mechanism="A drug is associated with a phenotype the disease presents.",
        categories={
            "n_disease": DISEASE,
            "n_phenotype": PHENOTYPE,
            "n_chem": DRUG,
        },
        hops=(
            Hop("n_disease", "n_phenotype", ("biolink:has_phenotype",)),
            Hop("n_phenotype", "n_chem", ("biolink:associated_with",)),
        ),
        baseline=Baseline(477, 10070, 171469 / 10070),
        notes="Only 380 phenotypes carry drug associations, so this fires for "
        "few diseases -- cheap enough to keep in the portfolio anyway.",
    ),
    QueryTemplate(
        name="direct_association",
        tier="C-associative",
        mechanism="A drug is directly associated or correlated with the disease.",
        categories={"n_disease": DISEASE, "n_chem": DRUG},
        hops=(Hop("n_disease", "n_chem", ("biolink:associated_with",)),),
        baseline=Baseline(16, 1906, 30660 / 1906),
        notes="One hop, 1,906 diseases covered, ~16 drugs each. Not a mechanism "
        "-- a baseline every other template should beat.",
    ),
    # -- Tier D: routes through the treats family --------------------------
    QueryTemplate(
        name="indication_transfer",
        tier="D-leaky",
        mechanism="A drug treats another disease that shares a phenotype with "
        "this one.",
        categories={
            "n_disease": DISEASE,
            "n_phenotype": PHENOTYPE,
            "n_other": DISEASE,
            "n_chem": DRUG,
        },
        hops=(
            Hop("n_disease", "n_phenotype", ("biolink:has_phenotype",)),
            Hop("n_other", "n_phenotype", ("biolink:has_phenotype",)),
            Hop(
                "n_chem",
                "n_other",
                ("biolink:treats_or_applied_or_studied_to_treat",),
            ),
        ),
        baseline=Baseline(8221, 10070, 171469 / 10070),
        leaky=True,
        filter_config={"max_node_degree": 200},
        notes="Empirically strong, mechanism-free, and it reads treats edges -- "
        "so it will flatter itself against any ground truth drawn from the same "
        "indication data. Evaluate it in its own bucket.",
    ),
    QueryTemplate(
        name="two_witness_inhibition",
        tier="D-branching",
        mechanism="A small molecule decreases two different proteins, both "
        "associated with the disease.",
        categories={
            "n_disease": DISEASE,
            "n_protein_a": PROTEIN,
            "n_protein_b": PROTEIN,
            "n_chem": SMALL_MOLECULE,
        },
        hops=(
            Hop("n_disease", "n_protein_a", ("biolink:associated_with",)),
            Hop("n_disease", "n_protein_b", ("biolink:associated_with",)),
            Hop(
                "n_chem",
                "n_protein_a",
                ("biolink:affects",),
                ((DIRECTION, "decreased"),),
            ),
            Hop(
                "n_chem",
                "n_protein_b",
                ("biolink:affects",),
                ((DIRECTION, "decreased"),),
            ),
        ),
        baseline=Baseline(2689, 8832, 99582 / 8832),
        filter_config={"max_node_degree": 500},
        notes="Branching, not linear: two independent witnesses for the same "
        "chemical -- the precision lever. TRAPI cannot say n_protein_a != "
        "n_protein_b, so results include degenerate pairs and each genuine pair "
        "comes back twice. In Shepherd both are already handled downstream: "
        "merge_message.filter_repeated_nodes drops results whose qnodes bind "
        "the same knode, and grouping by the answer node collapses (a,b) with "
        "(b,a). The estimate does not model the self-join, so the real count is "
        "well under it.",
    ),
)

TEMPLATES_BY_NAME: dict[str, QueryTemplate] = {t.name: t for t in TEMPLATES}


# ---------------------------------------------------------------------------
# Costing against the census
# ---------------------------------------------------------------------------


@dataclass
class Census:
    """The census tables a template estimate needs."""

    rollup: dict[tuple[str, str, str], dict]
    qualifier_values: dict[tuple[str, str, str, str, str], dict]
    signatures: dict[tuple[str, str, str, str], dict]
    # (subject, predicate, object) -> [(qualifier pairs on the edge, stats)]
    signature_index: dict[tuple[str, str, str], list] = field(default_factory=dict)
    # qualifier value -> its reflexive ancestor values
    value_ancestors: dict[tuple[str, str], tuple[str, ...]] = field(
        default_factory=dict
    )
    manifest: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Derive the signature index when only the flat table was supplied."""
        if self.signature_index or not self.signatures:
            return
        for (subject, predicate, obj, rendered), stats in self.signatures.items():
            pairs = {
                tuple(part.split("=", 1)) for part in rendered.split("|") if "=" in part
            }
            self.signature_index.setdefault((subject, predicate, obj), []).append(
                (pairs, stats)
            )

    def satisfies(self, edge_pairs: set, required: Sequence[tuple[str, str]]) -> bool:
        """Whether an edge's qualifiers satisfy a qualifier_set.

        Subset semantics, with the value hierarchy: a qedge asking for
        ``activity_or_abundance`` is satisfied by an edge qualified
        ``expression``, and extra qualifiers on the edge are irrelevant.
        """
        for type_id, value in required:
            if not any(
                edge_type == type_id
                and value
                in self.value_ancestors.get((edge_type, edge_value), (edge_value,))
                for edge_type, edge_value in edge_pairs
            ):
                return False
        return True

    @classmethod
    def load(
        cls,
        directory: Path,
        triples: Optional[set[tuple[str, str, str]]] = None,
        need_signatures: bool = True,
    ) -> "Census":
        """Read a census directory into memory.

        The full census is large -- ``qualifier_signatures.tsv`` alone is ~104MB
        and loading everything costs ~870MB of resident memory and ~15s, which
        is not something a lookup worker can carry.  Two knobs cut that down:

        ``triples`` keeps only rows for the ``(subject, predicate, object)``
        triples the caller will actually price, which for a fixed portfolio is a
        few dozen out of 7,925 rollup rows.

        ``need_signatures`` skips the signatures table and the qualifier closure
        entirely.  Those are only consulted for *conjunctions* of qualifier
        constraints; a template constraining a single qualifier reads
        ``qualifier_values.tsv``, which already unions every signature carrying
        that value.  The shipped portfolio has no conjunctions, so the default
        caller skips both.  A template that adds one flips this back on.
        """
        wanted = triples

        def keep(row: dict) -> bool:
            if wanted is None:
                return True
            return (
                row["subject_category"],
                row["predicate"],
                row["object_category"],
            ) in wanted

        rollup: dict[tuple[str, str, str], dict] = {}
        with open(directory / "census_rollup.tsv", encoding="utf-8") as handle:
            for row in csv.DictReader(handle, delimiter="\t"):
                if not keep(row):
                    continue
                triple_key = (
                    row["subject_category"],
                    row["predicate"],
                    row["object_category"],
                )
                rollup[triple_key] = {
                    "edges": int(row["edge_count"]),
                    "subjects": int(row["distinct_subjects"]),
                    "objects": int(row["distinct_objects"]),
                }

        qualifier_values: dict[tuple[str, str, str, str, str], dict] = {}
        path = directory / "qualifier_values.tsv"
        if path.exists():
            with open(path, encoding="utf-8") as handle:
                for row in csv.DictReader(handle, delimiter="\t"):
                    if not keep(row):
                        continue
                    value_key = (
                        row["subject_category"],
                        row["predicate"],
                        row["object_category"],
                        row["qualifier_type_id"],
                        row["qualifier_value"],
                    )
                    qualifier_values[value_key] = {
                        "edges": int(row["edge_count"]),
                        "subjects": int(row["distinct_subjects"]),
                        "objects": int(row["distinct_objects"]),
                    }

        signatures: dict[tuple[str, str, str, str], dict] = {}
        signature_index: dict[tuple[str, str, str], list] = {}
        path = directory / "qualifier_signatures.tsv"
        if need_signatures and path.exists():
            with open(path, encoding="utf-8") as handle:
                for row in csv.DictReader(handle, delimiter="\t"):
                    if not keep(row):
                        continue
                    triple = (
                        row["subject_category"],
                        row["predicate"],
                        row["object_category"],
                    )
                    rendered = row["qualifier_signature"]
                    stats = {
                        "edges": int(row["edge_count"]),
                        "subjects": int(row["distinct_subjects"]),
                        "objects": int(row["distinct_objects"]),
                    }
                    signatures[(*triple, rendered)] = stats
                    pairs = {
                        tuple(part.split("=", 1))
                        for part in rendered.split("|")
                        if "=" in part
                    }
                    signature_index.setdefault(triple, []).append((pairs, stats))

        value_ancestors: dict[tuple[str, str], tuple[str, ...]] = {}
        closure_path = directory / "biolink_closure.json"
        if need_signatures and closure_path.exists():
            with open(closure_path, encoding="utf-8") as handle:
                closure = json.load(handle)
            for entry in (closure.get("qualifiers") or {}).values():
                value_ancestors[
                    (entry["qualifier_type_id"], entry["qualifier_value"])
                ] = tuple(entry["ancestor_values"])

        manifest: dict = {}
        manifest_path = directory / "manifest.json"
        if manifest_path.exists():
            with open(manifest_path, encoding="utf-8") as handle:
                raw_manifest = json.load(handle)
            # Keep provenance only: the full manifest carries every unmapped
            # qualifier value and runs to ~2MB, none of which pricing needs.
            manifest = {
                key: raw_manifest[key]
                for key in (
                    "generated_at",
                    "source",
                    "biolink_version",
                    "match_semantics",
                    "nodes",
                    "edges",
                )
                if key in raw_manifest
            }

        return cls(
            rollup,
            qualifier_values,
            signatures,
            signature_index,
            value_ancestors,
            manifest,
        )

    def stats(self, template: QueryTemplate, hop: Hop) -> Optional[dict]:
        """Counts for one hop, honouring its qualifier constraints.

        A single qualifier constraint is read from the value table, which already
        unions every signature containing that value and rolls the value
        hierarchy up, exactly as the query would.

        A conjunction has to be summed over signatures instead, because TRAPI
        matches a qualifier_set as a *subset*: the census shows aspect and
        direction almost never occur alone -- they arrive bundled with
        ``qualified_predicate`` and species context -- so an exact signature
        match would find nothing.  Edge counts sum exactly; distinct endpoints
        cannot (one chemical may appear under several signatures), so the
        largest contributing signature is used, which biases the fan-out
        estimate high.  Over-budgeting is the safe direction.

        Multiple predicates take the largest matching row, since Gandalf ORs
        them.
        """
        subject_category = template.categories[hop.subject]
        object_category = template.categories[hop.object]

        best = None
        for predicate in hop.predicates:
            key = (subject_category, predicate, object_category)
            if len(hop.qualifiers) == 1:
                type_id, value = hop.qualifiers[0]
                found = self.qualifier_values.get((*key, type_id, value))
            elif hop.qualifiers:
                found = None
                matching = [
                    stats
                    for pairs, stats in self.signature_index.get(key, [])
                    if self.satisfies(pairs, hop.qualifiers)
                ]
                if matching:
                    found = {
                        "edges": sum(stats["edges"] for stats in matching),
                        "subjects": max(stats["subjects"] for stats in matching),
                        "objects": max(stats["objects"] for stats in matching),
                    }
            else:
                found = self.rollup.get(key)
            if found and (best is None or found["edges"] > best["edges"]):
                best = found
        return best


def census_triples(
    templates: Sequence[QueryTemplate] = TEMPLATES,
) -> set[tuple[str, str, str]]:
    """Every census row a set of templates can ask for.

    One row per (hop, predicate), since ``Census.stats`` reads each predicate of
    a multi-predicate qedge separately and takes the largest.
    """
    triples: set[tuple[str, str, str]] = set()
    for template in templates:
        for hop in template.hops:
            subject = template.categories[hop.subject]
            obj = template.categories[hop.object]
            for predicate in hop.predicates:
                triples.add((subject, predicate, obj))
    return triples


def needs_signatures(templates: Sequence[QueryTemplate] = TEMPLATES) -> bool:
    """Whether any template constrains more than one qualifier on a hop."""
    return any(
        len(hop.qualifiers) > 1 for template in templates for hop in template.hops
    )


def load_census(
    directory: Optional[str],
    templates: Sequence[QueryTemplate] = TEMPLATES,
) -> Optional[Census]:
    """Load a census directory, or return None if it is absent or unreadable.

    Only the rows ``templates`` can ask for are kept, so the worker holds a few
    hundred rows rather than the whole census.

    A missing census is not fatal: selection falls back to the per-template
    baselines.  It is logged at warning level because the fallback cannot adapt
    to a probe as precisely, so running without one should be visible.
    """
    if not directory:
        return None
    path = Path(directory)
    if not (path / "census_rollup.tsv").exists():
        logger.warning(
            "No census at %s (census_rollup.tsv missing); "
            "pricing query templates from baked-in baselines instead.",
            path,
        )
        return None
    try:
        census = Census.load(
            path,
            triples=census_triples(templates),
            need_signatures=needs_signatures(templates),
        )
    except Exception as error:  # noqa: BLE001 - never block lookup on the census
        logger.warning("Could not load census at %s: %s", path, error)
        return None
    logger.info(
        "Loaded census from %s (%d rollup triples, graph %s, biolink %s)",
        path,
        len(census.rollup),
        census.manifest.get("graph", "unknown"),
        census.manifest.get("biolink_version", "unknown"),
    )
    return census


def estimate(
    template: QueryTemplate,
    census: Census,
    probe: Optional[dict[str, int]] = None,
) -> dict:
    """Walk the query graph from the pinned node, multiplying fan-outs.

    Returns expected path count, the per-hop breakdown, and how many diseases
    can match the entry hop at all -- coverage being the number that decides
    whether a template is worth firing for an arbitrary disease.

    When ``probe`` carries a measurement for an entry hop, the measured degree
    of *this* disease replaces the census mean for that hop.  That is the whole
    point of the probe: the census means are global, and disease degree varies
    by orders of magnitude.
    """
    known = {template.pinned: 1.0}
    per_hop = []
    expected = 1.0
    coverage: Optional[int] = None
    missing = []
    probed = False

    remaining = list(template.hops)
    while remaining:
        progressed = False
        for hop in list(remaining):
            forward = hop.subject in known
            backward = hop.object in known
            if not (forward or backward):
                continue
            remaining.remove(hop)
            progressed = True

            stats = census.stats(template, hop)
            spec = template.probe_spec(hop) if probe else None
            measured = probe.get(spec.key()) if (probe and spec) else None

            if stats is None and measured is None:
                missing.append(f"{hop.subject}->{hop.object} {hop.predicates}")
                continue

            # The census gives this hop's mean fan-out; a probe measurement, when
            # there is one, gives this disease's actual degree and wins.
            if stats is None:
                fanout, anchor_count, edges = 0.0, 0, 0
            elif forward:
                fanout = stats["edges"] / max(stats["subjects"], 1)
                anchor_count, edges = stats["subjects"], stats["edges"]
            else:
                fanout = stats["edges"] / max(stats["objects"], 1)
                anchor_count, edges = stats["objects"], stats["edges"]

            if measured is not None:
                fanout = float(measured)
                probed = True

            target = hop.object if forward else hop.subject

            if coverage is None:
                coverage = anchor_count

            role = "expands"
            if target in known:
                # A closing edge constrains rather than expands; the join is not
                # modelled, so leave the running product alone and say so.
                role = "closes a cycle (not multiplied)"
            else:
                known[target] = known.get(target, 1.0) * fanout
                expected *= fanout

            per_hop.append(
                {
                    "hop": f"{hop.subject} -> {hop.object}",
                    "predicates": list(hop.predicates),
                    "qualifiers": [f"{t}={v}" for t, v in hop.qualifiers],
                    "edges": edges,
                    "fanout": round(fanout, 1),
                    "measured": measured is not None,
                    "role": role,
                }
            )
        if not progressed:
            break

    return {
        "template": template.name,
        "tier": template.tier,
        "leaky": template.leaky,
        "expected_paths": round(expected),
        "disease_coverage": coverage or 0,
        "probed": probed,
        "hops": per_hop,
        "missing_triples": missing,
    }


def baseline_estimate(
    template: QueryTemplate,
    probe: Optional[dict[str, int]] = None,
) -> dict:
    """Price a template without a census, from the numbers it was built with.

    A probe measurement is folded in by rescaling: the baseline is a product
    over hops, so swapping the entry hop's global mean for this disease's
    measured degree is one multiply.  Templates whose baseline does not record
    an ``entry_fanout`` keep their static estimate -- said out loud rather than
    silently, since it means the budget is working from a global mean for them.
    """
    expected = float(template.baseline.expected_paths)
    probed = False
    if probe:
        specs = template.probe_specs()
        measured = next(
            (probe[spec.key()] for spec in specs if spec.key() in probe), None
        )
        if measured is not None:
            if template.baseline.entry_fanout:
                expected *= measured / template.baseline.entry_fanout
                probed = True
            else:
                logger.debug(
                    "Template %s has no baseline entry fan-out; probe measurement "
                    "of %d not applied (mount a census to price it exactly).",
                    template.name,
                    measured,
                )
    return {
        "template": template.name,
        "tier": template.tier,
        "leaky": template.leaky,
        "expected_paths": round(expected),
        "disease_coverage": template.baseline.coverage,
        "probed": probed,
        "hops": [],
        "missing_triples": [],
        "source": "baseline",
    }


def price(
    template: QueryTemplate,
    census: Optional[Census],
    probe: Optional[dict[str, int]] = None,
) -> dict:
    """Estimate a template's cost, from the census when there is one."""
    if census is not None:
        return estimate(template, census, probe)
    return baseline_estimate(template, probe)


# ---------------------------------------------------------------------------
# Portfolio selection
# ---------------------------------------------------------------------------


def _tier_rank(tier: str) -> int:
    try:
        return TIER_ORDER.index(tier)
    except ValueError:
        return len(TIER_ORDER)


def select_portfolio(
    templates: Sequence[QueryTemplate],
    census: Optional[Census],
    probe: Optional[dict[str, int]] = None,
    budget: int = 0,
    exclude_leaky: bool = False,
    only: Optional[Sequence[str]] = None,
    tiers: Optional[Sequence[str]] = None,
    answer_categories: Sequence[str] = (),
) -> list[tuple[QueryTemplate, dict]]:
    """Choose which templates to fire, and price each one.

    Selection order is tier first (mechanism templates are never the ones
    dropped), then cheapest first within a tier, so a tight budget buys as many
    distinct shapes as it can.  ``budget`` of 0 fires everything that survives
    the other filters.

    ``tiers`` restricts to whole tiers, which is the unit an ablation wants:
    tier is the portfolio's own statement about how much mechanism a shape
    claims, so "does Tier B earn its recall?" is one run rather than a
    hand-listed set of names.

    A template whose entry hop the probe measured at zero is dropped outright:
    the disease has no neighbours on that hop, so the query cannot return a
    path and firing it only spends a lookup slot.
    """
    candidates = [
        template
        for template in templates
        if not (exclude_leaky and template.leaky)
        and (only is None or template.name in only)
        and (not tiers or template.tier in tiers)
        and template.answer_compatible(answer_categories)
    ]

    priced = [(template, price(template, census, probe)) for template in candidates]

    if probe is not None:
        kept = []
        for template, summary in priced:
            specs = template.probe_specs()
            measurements = [probe[s.key()] for s in specs if s.key() in probe]
            if measurements and not any(measurements):
                logger.debug(
                    "Dropping %s: probe found no entry-hop neighbours.", template.name
                )
                continue
            kept.append((template, summary))
        priced = kept

    priced.sort(
        key=lambda item: (
            _tier_rank(item[0].tier),
            item[1]["expected_paths"],
            item[0].name,
        )
    )

    if not budget:
        return priced

    selected: list[tuple[QueryTemplate, dict]] = []
    spent = 0
    for template, summary in priced:
        cost = summary["expected_paths"]
        if selected and spent + cost > budget:
            logger.debug(
                "Skipping %s (%d paths): would exceed the %d-path budget at %d.",
                template.name,
                cost,
                budget,
                spent,
            )
            continue
        selected.append((template, summary))
        spent += cost
    return selected


def probe_specs_for(templates: Sequence[QueryTemplate]) -> list[ProbeSpec]:
    """Every distinct entry-hop probe a set of templates needs."""
    specs: dict[str, ProbeSpec] = {}
    for template in templates:
        for spec in template.probe_specs():
            specs.setdefault(spec.key(), spec)
    return list(specs.values())

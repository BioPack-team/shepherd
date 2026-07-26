"""Tests for the census-derived query template portfolio.

The costing tests are carried over from ``tests/test_query_templates.py`` on the
Gandalf side, since the cost model ported unchanged and its numbers are all
verifiable by eye against the hand-built census below. The rendering and
selection tests are new: in Shepherd a template must render against the
caller's qnode keys, and the portfolio has to be chosen rather than merely
priced.
"""

import pytest

from workers.aragorn_lookup.query_templates import (
    TEMPLATES,
    TIER_ORDER,
    Baseline,
    Census,
    Hop,
    ProbeSpec,
    QueryTemplate,
    baseline_estimate,
    census_triples,
    estimate,
    needs_signatures,
    select_portfolio,
)

DISEASE = "biolink:Disease"
PROTEIN = "biolink:Protein"
SMALL_MOLECULE = "biolink:SmallMolecule"
DRUG = "biolink:Drug"
CHEMICAL = "biolink:ChemicalEntity"
DIRECTION = "biolink:object_direction_qualifier"


@pytest.fixture
def census():
    """Two triples: 10 proteins per disease, 20 chemicals per protein."""
    return Census(
        rollup={
            (DISEASE, "biolink:associated_with", PROTEIN): {
                "edges": 1000,
                "subjects": 100,
                "objects": 500,
            },
            (SMALL_MOLECULE, "biolink:affects", PROTEIN): {
                "edges": 8000,
                "subjects": 400,
                "objects": 200,
            },
        },
        qualifier_values={
            (
                SMALL_MOLECULE,
                "biolink:affects",
                PROTEIN,
                DIRECTION,
                "decreased",
            ): {"edges": 2000, "subjects": 300, "objects": 100},
        },
        signatures={
            (
                SMALL_MOLECULE,
                "biolink:affects",
                PROTEIN,
                # Rendered sorted, exactly as metagraph_census writes it.
                f"biolink:object_aspect_qualifier=activity|{DIRECTION}=decreased",
            ): {"edges": 500, "subjects": 200, "objects": 50},
        },
    )


def make_template(**overrides) -> QueryTemplate:
    defaults = dict(
        name="test_two_hop",
        tier="A-mechanism",
        mechanism="test",
        categories={
            "n_disease": DISEASE,
            "n_protein": PROTEIN,
            "n_chem": SMALL_MOLECULE,
        },
        hops=(
            Hop("n_disease", "n_protein", ("biolink:associated_with",)),
            Hop(
                "n_chem", "n_protein", ("biolink:affects",), ((DIRECTION, "decreased"),)
            ),
        ),
        baseline=Baseline(200, 100, 10.0),
    )
    defaults.update(overrides)
    return QueryTemplate(**defaults)


@pytest.fixture
def two_hop():
    return make_template()


# ---------------------------------------------------------------------------
# Rendering against the caller's qnode keys
# ---------------------------------------------------------------------------


def test_render_binds_the_callers_qnode_keys(two_hop):
    """The pinned and answer nodes must take the original query's keys.

    merge_message groups creative results by the *original* answer qnode, so a
    template that kept its own key would produce results nothing can merge.
    """
    query_graph = two_hop.render("MONDO:0004979", "ON", "SN")

    assert query_graph["nodes"]["ON"]["ids"] == ["MONDO:0004979"]
    assert query_graph["nodes"]["SN"]["categories"] == [SMALL_MOLECULE]
    assert "ids" not in query_graph["nodes"]["SN"]
    assert "n_disease" not in query_graph["nodes"]
    assert "n_chem" not in query_graph["nodes"]
    # Intermediates keep their template-local names.
    assert query_graph["nodes"]["n_protein"]["categories"] == [PROTEIN]
    assert query_graph["edges"]["e0"]["subject"] == "ON"
    assert query_graph["edges"]["e1"]["subject"] == "SN"


def test_render_keeps_the_callers_pinned_categories(two_hop):
    """A CURIE typed DiseaseOrPhenotypicFeature must not be narrowed to Disease.

    The template's ``biolink:Disease`` is what the census row is keyed on, not
    a constraint worth imposing on a node that is already pinned by CURIE.
    """
    query_graph = two_hop.render(
        "MONDO:0004979",
        "ON",
        "SN",
        pinned_node={
            "categories": ["biolink:DiseaseOrPhenotypicFeature"],
            "set_interpretation": "BATCH",
        },
        answer_node={"set_interpretation": "BATCH"},
    )

    assert query_graph["nodes"]["ON"]["categories"] == [
        "biolink:DiseaseOrPhenotypicFeature"
    ]
    assert query_graph["nodes"]["ON"]["set_interpretation"] == "BATCH"
    assert query_graph["nodes"]["SN"]["set_interpretation"] == "BATCH"
    # The answer node still takes the template's category -- that is the point.
    assert query_graph["nodes"]["SN"]["categories"] == [SMALL_MOLECULE]


def test_render_renames_intermediates_that_collide_with_caller_keys(two_hop):
    """A caller whose qnode is named n_protein must not collapse the template."""
    query_graph = two_hop.render("MONDO:1", "n_protein", "SN")

    assert len(query_graph["nodes"]) == 3
    assert query_graph["nodes"]["n_protein"]["ids"] == ["MONDO:1"]
    assert "n_protein_1" in query_graph["nodes"]
    assert query_graph["edges"]["e0"]["object"] == "n_protein_1"


def test_render_emits_trapi_qualifier_constraints(two_hop):
    edges = two_hop.render("MONDO:1", "ON", "SN")["edges"]

    assert edges["e1"]["qualifier_constraints"] == [
        {
            "qualifier_set": [
                {
                    "qualifier_type_id": DIRECTION,
                    "qualifier_value": "decreased",
                }
            ]
        }
    ]
    assert "qualifier_constraints" not in edges["e0"]


@pytest.mark.parametrize("template", TEMPLATES, ids=lambda t: t.name)
def test_every_template_renders_a_connected_query_graph(template):
    query_graph = template.render("MONDO:0004979", "ON", "SN")

    assert query_graph["nodes"]["ON"]["ids"] == ["MONDO:0004979"]
    assert len(query_graph["nodes"]) == len(template.categories)

    # Every qnode must be reachable from the pinned node, or the query graph
    # has an orphan component Gandalf would reject.
    adjacency: dict[str, set[str]] = {key: set() for key in query_graph["nodes"]}
    for edge in query_graph["edges"].values():
        adjacency[edge["subject"]].add(edge["object"])
        adjacency[edge["object"]].add(edge["subject"])
    seen = {"ON"}
    stack = ["ON"]
    while stack:
        for neighbour in adjacency[stack.pop()]:
            if neighbour not in seen:
                seen.add(neighbour)
                stack.append(neighbour)
    assert seen == set(query_graph["nodes"])


# ---------------------------------------------------------------------------
# Costing
# ---------------------------------------------------------------------------


def test_estimate_multiplies_fanouts_along_the_path(two_hop, census):
    summary = estimate(two_hop, census)

    # 1000/100 = 10 proteins per disease, then the qualified row backwards:
    # 2000 edges / 100 distinct proteins = 20 chemicals per protein.
    assert summary["expected_paths"] == 200
    assert summary["disease_coverage"] == 100
    assert [hop["fanout"] for hop in summary["hops"]] == [10.0, 20.0]


def test_qualifier_constraint_changes_the_row_used(two_hop, census):
    """Dropping the qualifier reads the unqualified row, which is bigger."""
    unqualified = make_template(
        hops=(
            two_hop.hops[0],
            Hop("n_chem", "n_protein", ("biolink:affects",)),
        )
    )

    # 8000 edges / 200 distinct proteins = 40 chemicals per protein, vs 20.
    assert estimate(unqualified, census)["expected_paths"] == 400


def test_missing_census_row_is_reported_not_guessed(two_hop, census):
    absent = make_template(
        hops=(
            two_hop.hops[0],
            Hop("n_chem", "n_protein", ("biolink:nonexistent_predicate",)),
        )
    )
    summary = estimate(absent, census)

    assert summary["missing_triples"]
    # The missing hop contributes nothing rather than a made-up multiplier.
    assert summary["expected_paths"] == 10


def test_closing_edge_does_not_inflate_the_estimate(census):
    """A hop whose target is already known constrains; it must not multiply."""
    branching = make_template(
        hops=(
            Hop("n_disease", "n_protein", ("biolink:associated_with",)),
            Hop(
                "n_chem", "n_protein", ("biolink:affects",), ((DIRECTION, "decreased"),)
            ),
            # Same two endpoints again: closes a cycle.
            Hop(
                "n_chem", "n_protein", ("biolink:affects",), ((DIRECTION, "decreased"),)
            ),
        )
    )
    summary = estimate(branching, census)

    assert summary["expected_paths"] == 200
    assert summary["hops"][-1]["role"] == "closes a cycle (not multiplied)"


def test_probe_replaces_the_census_mean_on_the_entry_hop(two_hop, census):
    """A measured disease degree beats the global mean for that disease."""
    spec = two_hop.probe_spec(two_hop.hops[0])
    summary = estimate(two_hop, census, probe={spec.key(): 3})

    # 3 measured proteins (not the mean of 10) x 20 chemicals per protein.
    assert summary["expected_paths"] == 60
    assert summary["probed"] is True
    assert summary["hops"][0]["measured"] is True
    assert summary["hops"][1]["measured"] is False


def test_probe_does_not_touch_non_entry_hops(two_hop, census):
    """Only hops touching the pinned node are measurable."""
    assert two_hop.probe_spec(two_hop.hops[1]) is None


def test_baseline_estimate_rescales_on_a_probe():
    """Without a census the baseline is still adapted to the pinned disease."""
    template = make_template(baseline=Baseline(200, 100, 10.0))
    spec = template.probe_spec(template.hops[0])

    assert baseline_estimate(template)["expected_paths"] == 200
    # Measured 30 against a baseline entry fan-out of 10: three times the cost.
    assert baseline_estimate(template, {spec.key(): 30})["expected_paths"] == 600


def test_baseline_without_entry_fanout_keeps_its_static_estimate():
    template = make_template(baseline=Baseline(200, 100, None))
    spec = template.probe_spec(template.hops[0])
    summary = baseline_estimate(template, {spec.key(): 30})

    assert summary["expected_paths"] == 200
    assert summary["probed"] is False


# ---------------------------------------------------------------------------
# Portfolio selection
# ---------------------------------------------------------------------------


def test_selection_orders_by_tier_then_cost():
    selected = select_portfolio(TEMPLATES, census=None)
    ranks = [TIER_ORDER.index(template.tier) for template, _ in selected]

    assert ranks == sorted(ranks)
    assert selected[0][0].tier == "A-mechanism"
    # Within each tier, cheapest first.
    for tier in TIER_ORDER:
        costs = [s["expected_paths"] for t, s in selected if t.tier == tier]
        assert costs == sorted(costs)


def test_budget_sheds_broad_templates_before_mechanism_ones():
    selected = select_portfolio(TEMPLATES, census=None, budget=1200)
    names = [template.name for template, _ in selected]

    # Tier A prices at ~1.1k paths, so the whole of it fits and the 2k+ Tier B
    # templates do not.
    assert "target_inhibition_sm" in names
    assert "ppi_neighborhood" not in names
    assert sum(s["expected_paths"] for _, s in selected) <= 1200


def test_zero_budget_fires_the_whole_portfolio():
    assert len(select_portfolio(TEMPLATES, census=None, budget=0)) == len(TEMPLATES)


def test_budget_always_buys_at_least_one_template():
    """A budget below the cheapest template still fires something.

    Tier ordering dominates cost, so the one it buys is the cheapest
    *mechanism* template, not the globally cheapest one (direct_association,
    16 paths, is Tier C -- the baseline every other template should beat).
    """
    selected = select_portfolio(TEMPLATES, census=None, budget=1)

    assert len(selected) == 1
    assert selected[0][0].name == "causal_gene_inhibition"


def test_exclude_leaky_drops_the_treats_reading_template():
    names = [t.name for t, _ in select_portfolio(TEMPLATES, None, exclude_leaky=True)]

    assert "indication_transfer" not in names
    assert "two_witness_inhibition" in names


def test_probe_of_zero_drops_a_template_that_cannot_answer():
    """No neighbours on the entry hop means no possible path -- do not fire."""
    associated = ProbeSpec(("biolink:associated_with",), PROTEIN, True)
    selected = select_portfolio(TEMPLATES, census=None, probe={associated.key(): 0})
    names = [template.name for template, _ in selected]

    assert "target_inhibition_sm" not in names
    # A template entering on a different hop is unaffected.
    assert "phenotype_drug_bridge" in names


def test_answer_category_narrower_than_the_template_is_not_answered():
    """Asking for Drug must not be answered with arbitrary small molecules."""
    names = [
        template.name
        for template, _ in select_portfolio(
            TEMPLATES, census=None, answer_categories=[DRUG]
        )
    ]

    assert "target_inhibition_drug" in names
    assert "target_inhibition_sm" not in names


def test_answer_category_wider_than_the_template_is_answered():
    names = [
        template.name
        for template, _ in select_portfolio(
            TEMPLATES, census=None, answer_categories=[CHEMICAL]
        )
    ]

    assert "target_inhibition_sm" in names
    assert "target_inhibition_drug" in names


def test_only_restricts_to_named_templates():
    selected = select_portfolio(TEMPLATES, census=None, only=["direct_association"])

    assert [template.name for template, _ in selected] == ["direct_association"]


# ---------------------------------------------------------------------------
# Portfolio invariants (carried over from the Gandalf suite)
# ---------------------------------------------------------------------------


def test_portfolio_names_are_unique():
    names = [template.name for template in TEMPLATES]
    assert len(names) == len(set(names))


def test_leaky_templates_are_the_only_ones_touching_the_treats_family():
    for template in TEMPLATES:
        touches_treats = any(
            "treat" in predicate
            for hop in template.hops
            for predicate in hop.predicates
        )
        assert touches_treats == template.leaky, template.name


def test_mechanism_templates_pin_protein_not_gene():
    """Disease-Gene has 1,329 edges over 436 diseases; Protein has 99,582."""
    for template in TEMPLATES:
        if template.tier == "A-mechanism":
            assert PROTEIN in template.categories.values(), template.name
            assert "biolink:Gene" not in template.categories.values(), template.name


def test_every_template_declares_a_baseline():
    """Selection has to work before the census ships alongside the graph."""
    for template in TEMPLATES:
        assert template.baseline.expected_paths > 0, template.name
        assert template.baseline.coverage > 0, template.name


# ---------------------------------------------------------------------------
# Census loading is restricted to what the portfolio prices
# ---------------------------------------------------------------------------


def test_census_triples_cover_every_hop_predicate():
    triples = census_triples(TEMPLATES)

    for template in TEMPLATES:
        for hop in template.hops:
            for predicate in hop.predicates:
                key = (
                    template.categories[hop.subject],
                    predicate,
                    template.categories[hop.object],
                )
                assert key in triples


def test_shipped_portfolio_needs_no_signature_table():
    """Every template constrains at most one qualifier, so the 104MB
    signatures table can be skipped at load."""
    assert needs_signatures(TEMPLATES) is False


def test_a_conjunction_would_turn_the_signature_table_back_on():
    conjunction = make_template(
        hops=(
            Hop(
                "n_disease",
                "n_protein",
                ("biolink:associated_with",),
                ((DIRECTION, "decreased"), ("biolink:object_aspect_qualifier", "a")),
            ),
        )
    )
    assert needs_signatures([conjunction]) is True


def test_conjunction_matches_signatures_as_a_subset(census):
    """TRAPI matches a qualifier_set as a subset, so a constraint on direction
    alone is satisfied by an edge carrying direction *and* aspect."""
    census.value_ancestors = {}
    census.__post_init__()
    conjunction = make_template(
        hops=(
            Hop("n_disease", "n_protein", ("biolink:associated_with",)),
            Hop(
                "n_chem",
                "n_protein",
                ("biolink:affects",),
                (
                    (DIRECTION, "decreased"),
                    ("biolink:object_aspect_qualifier", "activity"),
                ),
            ),
        )
    )
    summary = estimate(conjunction, census)

    # The one matching signature: 500 edges / 50 distinct proteins = 10.
    assert summary["hops"][1]["fanout"] == 10.0

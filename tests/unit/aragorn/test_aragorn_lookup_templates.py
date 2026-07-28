"""Tests for census-template expansion in the aragorn lookup worker.

Covers which template set a creative query gets (the A/B switch), that the
direct lookup query is emitted exactly once however many sets fire, and that
each template's filter_config reaches Gandalf's parameters.
"""

import copy
import json
import logging

import pytest

from tests.helpers.generate_messages import creative_query
from workers.aragorn_lookup import worker as lookup_worker
from workers.aragorn_lookup.probe import (
    FAR_QNODE,
    build_probe_query,
    count_neighbours,
    probe_disease,
)
from workers.aragorn_lookup.query_templates import TIER_ORDER, ProbeSpec

logger = logging.getLogger(__name__)

PROTEIN = "biolink:Protein"


@pytest.fixture
def message():
    msg = copy.deepcopy(creative_query)
    msg["parameters"] = {"timeout": 60, "probe": False}
    msg["submitter"] = "test"
    return msg


@pytest.fixture(autouse=True)
def no_census(mocker):
    """Price from baselines; the census TSVs are a deploy artifact."""
    mocker.patch("workers.aragorn_lookup.worker.get_census", return_value=None)


# ---------------------------------------------------------------------------
# Applicability
# ---------------------------------------------------------------------------


def test_treats_with_the_disease_pinned_is_applicable():
    assert lookup_worker.census_templates_applicable("biolink:treats", {}, False)


def test_treats_with_the_chemical_pinned_is_not_applicable():
    """The portfolio pins a disease; "what does this drug treat" is a different
    question and has no templates."""
    assert not lookup_worker.census_templates_applicable("biolink:treats", {}, True)


def test_contraindication_is_applicable():
    """contraindicated_for is the AMIE key; this graph records
    contraindicated_in. Both route to the same portfolio."""
    assert lookup_worker.census_templates_applicable(
        "biolink:contraindicated_for", {}, False
    )
    assert lookup_worker.census_templates_applicable(
        "biolink:contraindicated_in", {}, False
    )


def test_a_predicate_with_no_portfolio_is_not_applicable():
    assert not lookup_worker.census_templates_applicable(
        "biolink:genetic_association", {}, False
    )


def test_a_qualified_creative_edge_is_not_applicable():
    """The qualified affects rules stay with the AMIE expansions."""
    qualifiers = {"qualifier_constraints": [{"qualifier_set": [{"a": "b"}]}]}
    assert not lookup_worker.census_templates_applicable(
        "biolink:treats", qualifiers, False
    )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_census_set_fires_the_portfolio(message):
    message["parameters"]["template_set"] = "census"
    messages, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert labels[0] == "direct_lookup"
    assert "target_inhibition_sm" in labels
    assert len(messages) == len(labels)


@pytest.mark.asyncio
async def test_amie_set_fires_no_census_templates(message, mocker):
    mocker.patch(
        "workers.aragorn_lookup.worker.get_amie_expansions",
        return_value={},
    )
    message["parameters"]["template_set"] = "amie"
    messages, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert labels == ["direct_lookup"]
    assert len(messages) == 1


@pytest.mark.asyncio
async def test_both_emits_one_direct_query_not_two(message, mocker):
    """merge_message identifies the direct lookup by shape, so a duplicate
    would be counted as a creative result as well as a lookup one."""
    rule_template = {
        "query_graph": {
            "nodes": {
                "$source": {
                    "categories": ["biolink:ChemicalEntity"],
                    "ids": ["$source_id"],
                },
                "$target": {
                    "categories": ["biolink:DiseaseOrPhenotypicFeature"],
                    "ids": ["$target_id"],
                },
            },
            "edges": {
                "expanded": {
                    "subject": "$source",
                    "object": "$target",
                    "predicates": ["biolink:related_to"],
                }
            },
        }
    }
    mocker.patch(
        "workers.aragorn_lookup.worker.get_amie_expansions",
        return_value={
            json.dumps({"predicate": "biolink:treats"}): [{"template": rule_template}]
        },
    )
    message["parameters"]["template_set"] = "both"
    messages, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert labels.count("direct_lookup") == 1
    assert "target_inhibition_sm" in labels
    assert any(label.startswith("amie_") for label in labels)
    direct = messages[0]["message"]["query_graph"]
    assert sum(1 for m in messages if m["message"]["query_graph"] == direct) == 1


@pytest.mark.asyncio
async def test_non_treats_creative_edge_falls_back_to_amie(message, mocker):
    """A creative edge the portfolio cannot answer must not fire nothing."""
    mocker.patch("workers.aragorn_lookup.worker.get_amie_expansions", return_value={})
    edge = message["message"]["query_graph"]["edges"]["e0"]
    edge["predicates"] = ["biolink:genetic_association"]
    message["parameters"]["template_set"] = "census"

    messages, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert labels == ["direct_lookup"]


@pytest.mark.asyncio
async def test_unknown_template_set_falls_back_to_the_configured_default(
    message, mocker
):
    message["parameters"]["template_set"] = "nonsense"
    messages, labels = await lookup_worker.build_lookup_messages(message, logger)

    # settings.template_set defaults to census.
    assert "target_inhibition_sm" in labels


@pytest.mark.asyncio
async def test_templates_parameter_restricts_the_portfolio(message):
    message["parameters"]["template_tiers"] = list(TIER_ORDER)
    message["parameters"]["templates"] = ["direct_association"]
    messages, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert labels == ["direct_lookup", "direct_association"]


@pytest.mark.asyncio
async def test_leaky_templates_are_held_back_by_default(message):
    """indication_transfer reads treats-family edges, so it flatters itself on
    ground truth drawn from the same indication data. It stays in the registry
    but does not fire unless asked for."""
    _, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert "indication_transfer" not in labels
    assert "target_inhibition_sm" in labels


@pytest.mark.asyncio
async def test_exclude_leaky_false_fires_the_leaky_template(message):
    """Measuring what the leaky tier is worth has to remain one flag away."""
    message["parameters"]["template_tiers"] = list(TIER_ORDER)
    message["parameters"]["exclude_leaky"] = False
    _, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert "indication_transfer" in labels


@pytest.mark.asyncio
async def test_template_tiers_parameter_restricts_to_whole_tiers(message):
    message["parameters"]["template_tiers"] = ["A-mechanism"]
    _, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert "target_inhibition_sm" in labels
    assert "ppi_neighborhood" not in labels
    assert "direct_association" not in labels


@pytest.mark.asyncio
async def test_template_tiers_and_exclude_leaky_compose(message):
    """A tier ablation must not silently re-admit the leaky tier."""
    message["parameters"]["template_tiers"] = ["D-leaky", "D-branching"]
    _, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert "indication_transfer" not in labels
    assert "two_witness_inhibition" in labels


@pytest.mark.asyncio
async def test_budget_parameter_trims_the_portfolio(message):
    message["parameters"]["template_tiers"] = list(TIER_ORDER)
    message["parameters"]["template_path_budget"] = 1200
    _, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert "target_inhibition_sm" in labels
    assert "ppi_neighborhood" not in labels


# ---------------------------------------------------------------------------
# What each expansion carries
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_each_template_carries_its_own_filter_config(message):
    message["parameters"]["template_tiers"] = list(TIER_ORDER)
    messages, labels = await lookup_worker.build_lookup_messages(message, logger)
    by_label = dict(zip(labels, messages))

    assert by_label["ppi_neighborhood"]["parameters"]["filter_config"] == {
        "max_node_degree": 100
    }
    assert by_label["target_inhibition_sm"]["parameters"]["filter_config"] == {
        "max_node_degree": 500
    }
    # A template with no filter_config does not invent one.
    assert "filter_config" not in by_label["direct_association"]["parameters"]


@pytest.mark.asyncio
async def test_caller_filter_config_wins_over_the_template_default(message):
    message["parameters"]["template_tiers"] = list(TIER_ORDER)
    message["parameters"]["filter_config"] = {"max_node_degree": 42}
    messages, labels = await lookup_worker.build_lookup_messages(message, logger)
    by_label = dict(zip(labels, messages))

    assert by_label["ppi_neighborhood"]["parameters"]["filter_config"] == {
        "max_node_degree": 42
    }


@pytest.mark.asyncio
async def test_parameters_are_not_shared_between_expansions(message):
    """Per-template filter_config must not leak into the sibling queries."""
    messages, _ = await lookup_worker.build_lookup_messages(message, logger)
    parameter_ids = [id(m["parameters"]) for m in messages]

    assert len(parameter_ids) == len(set(parameter_ids))


@pytest.mark.asyncio
async def test_expansions_bind_the_original_answer_qnode(message):
    """Every expansion must answer on SN, or merge_message cannot group it."""
    messages, _ = await lookup_worker.build_lookup_messages(message, logger)

    for expansion in messages:
        nodes = expansion["message"]["query_graph"]["nodes"]
        assert "SN" in nodes
        assert nodes["ON"]["ids"] == ["MONDO:0001"]
        assert "ids" not in nodes["SN"]


# ---------------------------------------------------------------------------
# The probe
# ---------------------------------------------------------------------------


def test_probe_query_orients_the_edge_from_the_spec():
    outgoing = build_probe_query(
        ProbeSpec(("biolink:associated_with",), (PROTEIN,), True), "MONDO:1", 1000
    )
    edge = outgoing["message"]["query_graph"]["edges"]["e0"]
    assert edge["subject"] == "probe_disease"
    assert edge["object"] == FAR_QNODE

    incoming = build_probe_query(
        ProbeSpec(("biolink:causes",), (PROTEIN,), False), "MONDO:1", 1000
    )
    edge = incoming["message"]["query_graph"]["edges"]["e0"]
    assert edge["subject"] == FAR_QNODE
    assert edge["object"] == "probe_disease"


def test_probe_query_is_dehydrated_and_degree_capped():
    """A probe on a hub disease must not return a large payload."""
    query = build_probe_query(
        ProbeSpec(("biolink:associated_with",), (PROTEIN,), True), "MONDO:1", 250
    )

    assert query["parameters"]["dehydrated"] is True
    assert query["parameters"]["filter_config"]["max_node_degree"] == 250


def test_count_neighbours_counts_distinct_nodes():
    response = {
        "message": {
            "results": [
                {"node_bindings": {FAR_QNODE: [{"id": "UniProtKB:A"}]}},
                {"node_bindings": {FAR_QNODE: [{"id": "UniProtKB:A"}]}},
                {"node_bindings": {FAR_QNODE: [{"id": "UniProtKB:B"}]}},
            ]
        }
    }
    assert count_neighbours(response) == 2


def test_count_neighbours_handles_an_empty_response():
    assert count_neighbours({"message": {"results": []}}) == 0
    assert count_neighbours({}) == 0


@pytest.mark.asyncio
async def test_probe_measurements_reach_the_estimates(message, mocker):
    """A sparse disease should price below the census mean, so a budget that
    excludes a broad template on the mean can afford it for this disease.

    ppi_neighborhood prices at 10,986 paths against the global mean of 11.3
    associated proteins. With one measured protein it prices at ~975, which is
    the difference between not firing and firing under a 2,000-path budget.
    """
    message["parameters"]["template_tiers"] = list(TIER_ORDER)
    message["parameters"]["probe"] = True
    message["parameters"]["template_path_budget"] = 2000
    spec = ProbeSpec(("biolink:associated_with",), (PROTEIN,), True)

    probe = mocker.patch(
        "workers.aragorn_lookup.worker.probe_disease",
        new=mocker.AsyncMock(return_value={}),
    )
    _, unprobed = await lookup_worker.build_lookup_messages(message, logger)

    probe.return_value = {spec.key(): 1}
    _, probed = await lookup_worker.build_lookup_messages(message, logger)

    assert "ppi_neighborhood" not in unprobed
    assert "ppi_neighborhood" in probed


@pytest.mark.asyncio
async def test_a_failing_probe_does_not_fail_the_query(mocker):
    """Gandalf being slow or down must degrade to census means, not raise."""
    mocker.patch(
        "httpx.AsyncClient.post",
        new=mocker.AsyncMock(side_effect=RuntimeError("connection refused")),
    )
    spec = ProbeSpec(("biolink:associated_with",), (PROTEIN,), True)

    assert await probe_disease("MONDO:1", [spec], logger) == {}


@pytest.mark.asyncio
async def test_probe_of_no_specs_makes_no_requests(mocker):
    post = mocker.patch("httpx.AsyncClient.post", new=mocker.AsyncMock())

    assert await probe_disease("MONDO:1", [], logger) == {}
    assert not post.called


@pytest.mark.asyncio
async def test_probe_ignores_a_non_200(mocker):
    response = mocker.Mock()
    response.status_code = 500
    mocker.patch("httpx.AsyncClient.post", new=mocker.AsyncMock(return_value=response))
    spec = ProbeSpec(("biolink:associated_with",), (PROTEIN,), True)

    assert await probe_disease("MONDO:1", [spec], logger) == {}


# ---------------------------------------------------------------------------
# The two_witness_inhibition defect is handled downstream
# ---------------------------------------------------------------------------


def _two_witness_result(protein_a, protein_b):
    return {
        "node_bindings": {
            "ON": [{"id": "MONDO:0004979"}],
            "n_protein_a": [{"id": protein_a}],
            "n_protein_b": [{"id": protein_b}],
            "SN": [{"id": "CHEBI:100001"}],
        },
        "analyses": [],
    }


def test_two_witness_degenerate_pairs_are_dropped_by_merge_message():
    """TRAPI cannot say n_protein_a != n_protein_b, so the template returns
    pairs where both witnesses are the same protein. merge_message already
    drops results whose qnodes bind the same knode, so the template needs no
    special-casing in the lookup worker -- this test pins that down.

    Running the rendered template against a two-protein fixture graph returns
    exactly these four results.
    """
    from workers.merge_message.worker import filter_repeated_nodes

    response = {
        "message": {
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "auxiliary_graphs": {},
            "results": [
                _two_witness_result("UniProtKB:P00001", "UniProtKB:P00001"),
                _two_witness_result("UniProtKB:P00001", "UniProtKB:P00002"),
                _two_witness_result("UniProtKB:P00002", "UniProtKB:P00001"),
                _two_witness_result("UniProtKB:P00002", "UniProtKB:P00002"),
            ],
        }
    }
    filter_repeated_nodes(response, logger)

    kept = response["message"]["results"]
    assert len(kept) == 2
    for result in kept:
        assert (
            result["node_bindings"]["n_protein_a"][0]["id"]
            != result["node_bindings"]["n_protein_b"][0]["id"]
        )


def test_two_witness_mirrored_pairs_collapse_on_the_answer_node():
    """(a,b) and (b,a) are the same finding. Grouping by the answer qnode --
    which is what merge_results_by_node does -- collapses them into one."""
    from workers.merge_message.worker import group_results_by_qnode

    result_message = {
        "message": {
            "results": [
                _two_witness_result("UniProtKB:P00001", "UniProtKB:P00002"),
                _two_witness_result("UniProtKB:P00002", "UniProtKB:P00001"),
            ]
        }
    }
    grouped = group_results_by_qnode("SN", result_message, [])

    assert len(grouped) == 1


# ---------------------------------------------------------------------------
# Observability: is the census actually loaded?
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_portfolio_log_says_when_it_priced_from_baselines(message, caplog):
    """A missing census mount has to be visible on every query, not only in the
    one startup line. The baselines agree with the census for an average
    disease, so the numbers alone will not give it away."""
    with caplog.at_level(logging.INFO):
        await lookup_worker.build_lookup_messages(message, logger)

    line = next(
        m for m in (r.getMessage() for r in caplog.records) if "Census portfolio" in m
    )
    assert "priced from baselines (no census mounted)" in line


@pytest.mark.asyncio
async def test_portfolio_log_says_when_it_priced_from_the_census(
    message, mocker, caplog
):
    from workers.aragorn_lookup.query_templates import Census

    mocker.patch(
        "workers.aragorn_lookup.worker.get_census",
        return_value=Census(rollup={}, qualifier_values={}, signatures={}),
    )
    with caplog.at_level(logging.INFO):
        await lookup_worker.build_lookup_messages(message, logger)

    line = next(
        m for m in (r.getMessage() for r in caplog.records) if "Census portfolio" in m
    )
    assert "priced from census" in line


@pytest.mark.asyncio
async def test_default_portfolio_is_mechanism_plus_two_witness(message):
    """The shipped default is Tier A plus the branching precision lever.

    Tier A alone was the first arm to beat the AMIE incumbent on TopAnswer
    (65 vs 61) while cutting badly-surfaced NeverShow results from 23 to 5.
    D-branching was added on top because two_witness_inhibition's answers are a
    strict subset of target_inhibition_sm's -- it cannot introduce a new
    candidate, only a denser path for one already present.

    Pinned rather than left to whatever the tier list happens to say, because
    this is what every deployment fires."""
    _, labels = await lookup_worker.build_lookup_messages(message, logger)

    assert set(labels) == {
        "direct_lookup",
        "target_inhibition_sm",
        "target_inhibition_drug",
        "target_activation_sm",
        "causal_gene_inhibition",
        "inhibition_mechanism_sm",
        "two_witness_inhibition",
    }


# ---------------------------------------------------------------------------
# Observability: what config is actually in effect?
# ---------------------------------------------------------------------------


def test_config_line_reports_the_effective_values():
    from workers.aragorn_lookup.worker import describe_expansion_config

    line = describe_expansion_config()

    assert "template_set=" in line
    assert "template_tiers=" in line
    assert "census_dir=" in line


def test_config_line_flags_a_value_overridden_by_the_environment(monkeypatch):
    """Settings resolves env and .env ahead of the defaults in config.py, and
    compose mounts the repo's .env into the worker. Editing a default and
    deploying it can therefore change nothing at all, so an override has to be
    stated rather than inferred from which templates show up in the logs."""
    from shepherd_utils.config import Settings
    from workers.aragorn_lookup import worker as w

    monkeypatch.setattr(w.settings, "template_tiers", "A-mechanism")
    monkeypatch.setenv("TEMPLATE_TIERS", "A-mechanism")
    line = w.describe_expansion_config()

    default = Settings.model_fields["template_tiers"].default
    assert "[env, overriding" in line
    assert repr(default) in line


def test_config_line_attributes_a_dotenv_override(monkeypatch):
    """Same override with no environment variable set came from .env."""
    from workers.aragorn_lookup import worker as w

    monkeypatch.setattr(w.settings, "template_tiers", "B-broad")
    monkeypatch.delenv("TEMPLATE_TIERS", raising=False)
    line = w.describe_expansion_config()

    assert "[.env, overriding" in line


# ---------------------------------------------------------------------------
# The other creative query types
# ---------------------------------------------------------------------------

AFFECTS_DECREASED = {
    "qualifier_constraints": [
        {
            "qualifier_set": [
                {
                    "qualifier_type_id": "biolink:object_aspect_qualifier",
                    "qualifier_value": "activity",
                },
                {
                    "qualifier_type_id": "biolink:object_direction_qualifier",
                    "qualifier_value": "decreased",
                },
            ]
        }
    ]
}


def test_affects_routes_by_which_end_is_pinned():
    """The same predicate is two different questions depending on the pin."""
    assert lookup_worker.creative_query_type(
        "biolink:affects", AFFECTS_DECREASED, False
    ) == ("affects_gene_pinned", "decreased")
    assert lookup_worker.creative_query_type(
        "biolink:affects", AFFECTS_DECREASED, True
    ) == ("affects_chemical_pinned", "decreased")


def test_affects_without_a_direction_has_no_sign_to_propagate():
    """Sign-carrying templates cannot be built without a requested direction."""
    assert lookup_worker.creative_query_type("biolink:affects", {}, False) == (
        None,
        None,
    )


def test_treats_and_contraindication_carry_no_direction():
    assert lookup_worker.creative_query_type("biolink:treats", {}, False) == (
        "treats",
        None,
    )
    assert lookup_worker.creative_query_type(
        "biolink:contraindicated_in", {}, False
    ) == ("contraindicated", None)


def affects_message(pinned_subject: bool, direction: str):
    """A creative affects query, pinned on one end or the other."""
    subject = {"categories": ["biolink:ChemicalEntity"]}
    obj = {"categories": ["biolink:Gene"]}
    if pinned_subject:
        subject["ids"] = ["CHEBI:6801"]
    else:
        obj["ids"] = ["NCBIGene:1017"]
    qualifiers = copy.deepcopy(AFFECTS_DECREASED)
    qualifiers["qualifier_constraints"][0]["qualifier_set"][1][
        "qualifier_value"
    ] = direction
    return {
        "message": {
            "query_graph": {
                "nodes": {"SN": subject, "ON": obj},
                "edges": {
                    "e0": {
                        "subject": "SN",
                        "object": "ON",
                        "predicates": ["biolink:affects"],
                        "knowledge_type": "inferred",
                        **qualifiers,
                    }
                },
            }
        },
        "parameters": {"timeout": 60, "probe": False},
        "submitter": "test",
    }


@pytest.mark.asyncio
async def test_gene_pinned_affects_fires_its_own_portfolio(mocker):
    mocker.patch("workers.aragorn_lookup.worker.get_census", return_value=None)
    msg = affects_message(pinned_subject=False, direction="decreased")
    msg["parameters"]["template_tiers"] = list(TIER_ORDER)
    _, labels = await lookup_worker.build_lookup_messages(msg, logger)

    assert "gene_family_analogue" in labels
    # Templates for other question types must not leak in.
    assert "target_inhibition_sm" not in labels
    assert "chem_binding_target" not in labels


@pytest.mark.asyncio
async def test_chemical_pinned_affects_fires_the_mirror_portfolio(mocker):
    mocker.patch("workers.aragorn_lookup.worker.get_census", return_value=None)
    msg = affects_message(pinned_subject=True, direction="increased")
    msg["parameters"]["template_tiers"] = list(TIER_ORDER)
    _, labels = await lookup_worker.build_lookup_messages(msg, logger)

    assert "chem_binding_target" in labels
    assert "gene_family_analogue" not in labels


@pytest.mark.asyncio
async def test_the_requested_direction_reaches_the_rendered_qedges(mocker):
    """The sign algebra has to survive into the actual TRAPI request."""
    mocker.patch("workers.aragorn_lookup.worker.get_census", return_value=None)
    msg = affects_message(pinned_subject=False, direction="decreased")
    msg["parameters"]["templates"] = ["gene_upstream_repressor"]
    msg["parameters"]["template_tiers"] = list(TIER_ORDER)
    messages, labels = await lookup_worker.build_lookup_messages(msg, logger)

    graph = dict(zip(labels, messages))["gene_upstream_repressor"]["message"][
        "query_graph"
    ]
    directions = {}
    for edge in graph["edges"].values():
        for constraint in edge.get("qualifier_constraints", []):
            for qualifier in constraint["qualifier_set"]:
                if qualifier["qualifier_type_id"].endswith("direction_qualifier"):
                    directions[edge["object"]] = qualifier["qualifier_value"]
    # Repressor decreases the gene; to DECREASE the gene the chemical must
    # INCREASE the repressor. No sentinel may survive into the request.
    assert directions["ON"] == "decreased"
    assert directions["n_reg"] == "increased"
    assert "@same" not in json.dumps(graph) and "@opposite" not in json.dumps(graph)

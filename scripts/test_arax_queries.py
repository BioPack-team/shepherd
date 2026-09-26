"""Validate every kind of query ARAX handles, against a running Shepherd /arax.

    python test_arax_queries.py                      # everything, against localhost
    python test_arax_queries.py --list               # the cases, by group
    python test_arax_queries.py --only mvp1 dsl_     # cases whose name contains any of these
    python test_arax_queries.py --group workflows    # one group
    python test_arax_queries.py --base http://localhost:5439/arax --workers 4

Groups: lookup (TRAPI one-hop to three-hop), creative (MVP1 xDTD, MVP2 xCRG,
pathfinder), flexible (not / any / all / optional groups / constraints),
araxi (the DSL: every command and the overlay, filter_kg and filter_results
actions), workflows (TRAPI workflow operations, as the CQS sends them),
validation (the errors ARAX answers), delivery (streaming, the KP cache,
message_uris), endpoints (/response, /status, /entity, /meta_knowledge_graph,
autocomplete).

Each case checks the HTTP status, ARAX's response status ("Success", or the
error code ARAX answers with, e.g. "QueryGraphNoIds"), a minimum number of
results, and evidence of the feature it exercises (an NGD attribute on the
edges, xDTD's inferred edges, path bindings, a cache hit, the stream's kill
token, ...). Cases that hit a recorded upstream ARAX defect (D-* in
docs/ARAX_PORT_BASELINE.md, reproduced by the port on purpose) are expected to
fail: XFAIL when they do, XPASS when they don't (D-23 only fires when the KP's
answer includes support-graph edges, so it can XPASS on a small answer).

The curies are real (type 2 diabetes, metformin, PPARG, ...) and are also in
the seed of shepherd_utils.arax_mock_data, so overlays, Infer and xCRG have data
with the mock files too. Result counts come from the live Retriever, so a
"0 results" failure on a lookup can be the KG's answer rather than the port's;
the saved response (--out) shows which.

Every response is saved under --out (default responses/arax-validation/), and
a summary is written to <out>/summary.json. The exit code is 1 if any case
FAILed.
"""

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import httpx

# ---------------------------------------------------------------------------
# Curies (real, and in the mock data's seed)
# ---------------------------------------------------------------------------

T2D = "MONDO:0005148"  # type 2 diabetes mellitus
ASTHMA = "MONDO:0004979"
METFORMIN = "CHEBI:6801"
IBUPROFEN = "CHEBI:5855"
PPARG = "NCBIGene:5468"
INS = "NCBIGene:3630"

C = "biolink:ChemicalEntity"
SM = "biolink:SmallMolecule"
D = "biolink:Disease"
G = "biolink:Gene"
P = "biolink:PhenotypicFeature"
TREATS = "biolink:treats"


def trapi(nodes, edges=None, paths=None, **kw):
    qg = {"nodes": nodes}
    if edges is not None:
        qg["edges"] = edges
    if paths is not None:
        qg["paths"] = paths
    body = {"message": {"query_graph": qg}, "submitter": "test_arax_queries"}
    body.update(kw)
    return body


def dsl(actions, **kw):
    body = {
        "message": {},
        "operations": {"actions": actions},
        "submitter": "test_arax_queries",
    }
    body.update(kw)
    return body


def workflow(ops, nodes, edges, **kw):
    return trapi(nodes, edges, workflow=ops, **kw)


# ---------------------------------------------------------------------------
# Checks on a response (each returns an error string, or None)
# ---------------------------------------------------------------------------


def _msg(r):
    return r.get("message") or {}


def _edges(r):
    return ((_msg(r).get("knowledge_graph") or {}).get("edges") or {}).values()


def _nodes(r):
    return ((_msg(r).get("knowledge_graph") or {}).get("nodes") or {}).values()


def _attr_names(items):
    for item in items:
        for a in item.get("attributes") or []:
            yield a.get("original_attribute_name") or a.get("attribute_type_id")


def edge_attribute(name):
    def check(r):
        if name not in set(_attr_names(_edges(r))):
            return f"no KG edge has a {name} attribute"

    check.__name__ = f"edge attribute {name}"
    return check


def node_attribute(name):
    def check(r):
        if name not in set(_attr_names(_nodes(r))):
            return f"no KG node has a {name} attribute"

    check.__name__ = f"node attribute {name}"
    return check


def edge_predicate(predicate):
    def check(r):
        if not any(e.get("predicate") == predicate for e in _edges(r)):
            return f"no KG edge with predicate {predicate}"

    check.__name__ = f"edge predicate {predicate}"
    return check


def log_contains(text):
    def check(r):
        if not any(
            text in (entry.get("message") or "") for entry in r.get("logs") or []
        ):
            return f"no log line containing {text!r}"

    check.__name__ = f"log contains {text!r}"
    return check


def xdtd_predictions(r):
    """MVP1: xDTD's inferred treats edges, with their support graphs."""
    inferred = [e for e in _edges(r) if "probability_treats" in set(_attr_names([e]))]
    if not inferred:
        return "no xDTD prediction edge (probability_treats attribute)"
    if not _msg(r).get("auxiliary_graphs"):
        return "xDTD predictions but no auxiliary (support) graphs"


def path_bindings(r):
    """Pathfinder: results bind the QG path to auxiliary graphs."""
    for result in _msg(r).get("results") or []:
        for analysis in result.get("analyses") or []:
            if analysis.get("path_bindings"):
                return None
    return "no result has path_bindings"


def query_plan_from_cache(r):
    plan = (r.get("query_options") or {}).get("query_plan") or {}
    text = json.dumps(plan)
    if "from cache" not in text:
        return "query plan does not say the KP answer came from the cache"


def results_at_most(n):
    def check(r):
        got = len(_msg(r).get("results") or [])
        if got > n:
            return f"{got} results, expected at most {n}"

    check.__name__ = f"at most {n} results"
    return check


def not_bound(qnode_key, curie):
    def check(r):
        for result in _msg(r).get("results") or []:
            for b in (result.get("node_bindings") or {}).get(qnode_key) or []:
                if b.get("id") == curie:
                    return f"{curie} is bound to {qnode_key} although excluded"

    check.__name__ = f"{curie} not bound to {qnode_key}"
    return check


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------


@dataclass
class Case:
    name: str
    group: str
    body: Optional[dict] = None  # a POST /query body
    http: int = 200
    status: Optional[str] = "Success"  # ARAX's response status; None: any
    min_results: int = 0
    checks: list = field(default_factory=list)
    known_defect: Optional[str] = None  # expected to fail because of this D-*
    timeout: float = 300
    # endpoints: (method, path) and a check on the httpx response
    method: Optional[str] = None
    path: Optional[str] = None
    json_body: Optional[object] = None
    endpoint_check: Optional[Callable] = None
    after: bool = False  # runs after the query cases (it uses their responses)
    stream: bool = False
    note: str = ""


CASES = []


def case(*a, **k):
    CASES.append(Case(*a, **k))


# --- lookup: TRAPI queries, answered through ARAX's query graph templates ---
case(
    "lookup_one_hop_treats",
    "lookup",
    trapi(
        {"n0": {"ids": [T2D]}, "n1": {"categories": [C]}},
        {"e0": {"subject": "n1", "object": "n0", "predicates": [TREATS]}},
    ),
    min_results=1,
)
case(
    "lookup_one_hop_gene",
    "lookup",
    trapi(
        {"n0": {"ids": [METFORMIN]}, "n1": {"categories": [G]}},
        {"e0": {"subject": "n0", "object": "n1"}},
    ),
    min_results=1,
)
case(
    "lookup_one_hop_both_pinned",
    "lookup",
    trapi(
        {"n0": {"ids": [METFORMIN]}, "n1": {"ids": [T2D]}},
        {"e0": {"subject": "n0", "object": "n1"}},
    ),
    min_results=1,
)
case(
    "lookup_one_hop_no_categories",
    "lookup",
    trapi(
        {"n0": {"ids": [PPARG]}, "n1": {}}, {"e0": {"subject": "n0", "object": "n1"}}
    ),
    min_results=1,
)
case(
    "lookup_one_hop_two_curies",
    "lookup",
    trapi(
        {"n0": {"ids": [METFORMIN, IBUPROFEN]}, "n1": {"categories": [G]}},
        {"e0": {"subject": "n0", "object": "n1"}},
    ),
    min_results=1,
)
case(
    "lookup_qualified_affects",
    "lookup",
    trapi(
        {"n0": {"ids": [METFORMIN]}, "n1": {"categories": [G]}},
        {
            "e0": {
                "subject": "n0",
                "object": "n1",
                "predicates": ["biolink:affects"],
                "qualifier_constraints": [
                    {
                        "qualifier_set": [
                            {
                                "qualifier_type_id": "biolink:object_aspect_qualifier",
                                "qualifier_value": "activity_or_abundance",
                            },
                            {
                                "qualifier_type_id": "biolink:object_direction_qualifier",
                                "qualifier_value": "decreased",
                            },
                        ]
                    }
                ],
            }
        },
    ),
)
case(
    "lookup_two_hop",
    "lookup",
    trapi(
        {
            "n0": {"ids": [METFORMIN]},
            "n1": {"categories": [G]},
            "n2": {"categories": [D]},
        },
        {
            "e0": {"subject": "n0", "object": "n1"},
            "e1": {"subject": "n1", "object": "n2"},
        },
    ),
    min_results=1,
    timeout=600,
)
case(
    "lookup_two_hop_curie_cat_curie",
    "lookup",
    trapi(
        {"n0": {"ids": [METFORMIN]}, "n1": {"categories": [G]}, "n2": {"ids": [T2D]}},
        {
            "e0": {"subject": "n0", "object": "n1"},
            "e1": {"subject": "n1", "object": "n2"},
        },
    ),
    min_results=1,
    timeout=600,
)
case(
    "lookup_three_hop",
    "lookup",
    trapi(
        {
            "n0": {"ids": [METFORMIN]},
            "n1": {"categories": [G]},
            "n2": {"categories": [G]},
            "n3": {"ids": [T2D]},
        },
        {
            "e0": {"subject": "n0", "object": "n1"},
            "e1": {"subject": "n1", "object": "n2"},
            "e2": {"subject": "n2", "object": "n3"},
        },
    ),
    timeout=900,
)
case(
    "lookup_single_node",
    "lookup",
    trapi({"n0": {"ids": [METFORMIN]}}, {}),
    min_results=1,
)
case(
    "lookup_kp_list_forwarded",
    "lookup",
    dsl(
        [
            f"add_qnode(key=n0, ids={T2D})",
            "add_qnode(key=n1, categories=biolink:ChemicalEntity)",
            "add_qedge(key=e0, subject=n1, object=n0)",
            "expand(kp=infores:retriever)",
            "resultify()",
        ]
    ),
    min_results=1,
)

# --- creative: MVP1 (xDTD), MVP2 (xCRG), pathfinder ---
case(
    "mvp1_inferred_treats",
    "creative",
    trapi(
        {"n0": {"ids": [T2D]}, "n1": {"categories": [C]}},
        {
            "e0": {
                "subject": "n1",
                "object": "n0",
                "predicates": [TREATS],
                "knowledge_type": "inferred",
            }
        },
    ),
    min_results=1,
    checks=[xdtd_predictions],
    timeout=600,
)
case(
    "mvp1_two_diseases",
    "creative",
    trapi(
        {"n0": {"ids": [T2D, ASTHMA]}, "n1": {"categories": [C]}},
        {
            "e0": {
                "subject": "n1",
                "object": "n0",
                "predicates": [TREATS],
                "knowledge_type": "inferred",
            }
        },
    ),
    min_results=1,
    timeout=600,
)
case(
    "mvp1_with_workflow",
    "creative",
    workflow(
        [{"id": "lookup"}, {"id": "score"}],
        {"n0": {"ids": [T2D]}, "n1": {"categories": [C]}},
        {
            "e0": {
                "subject": "n1",
                "object": "n0",
                "predicates": [TREATS],
                "knowledge_type": "inferred",
            }
        },
    ),
    min_results=1,
    timeout=600,
)
case(
    "mvp1_dsl_infer",
    "creative",
    dsl(
        [
            "add_qnode(key=drug, categories=biolink:Drug)",
            f"add_qnode(key=disease, ids={T2D})",
            "add_qedge(key=t, subject=drug, object=disease, predicates=biolink:treats)",
            f"infer(action=drug_treatment_graph_expansion, disease_curie={T2D}, qedge_id=t, n_drugs=5, n_paths=3)",
        ]
    ),
    min_results=1,
    checks=[xdtd_predictions],
)
case(
    "mvp1_dsl_infer_bad_param",
    "creative",
    dsl(
        [
            f"infer(action=drug_treatment_graph_expansion, disease_curie={T2D}, n_drugs=zero)"
        ]
    ),
    http=400,
    status="ValueError",
)
case(
    "mvp2_xcrg_decreases",
    "creative",
    trapi(
        {"chem": {"categories": [C]}, "gene": {"ids": [PPARG], "categories": [G]}},
        {
            "t": {
                "subject": "chem",
                "object": "gene",
                "predicates": ["biolink:affects"],
                "knowledge_type": "inferred",
                "qualifier_constraints": [
                    {
                        "qualifier_set": [
                            {
                                "qualifier_type_id": "biolink:object_aspect_qualifier",
                                "qualifier_value": "activity_or_abundance",
                            },
                            {
                                "qualifier_type_id": "biolink:object_direction_qualifier",
                                "qualifier_value": "decreased",
                            },
                        ]
                    }
                ],
            }
        },
    ),
    checks=[log_contains("xCRG")],
    timeout=900,
)
case(
    "mvp2_xcrg_increases",
    "creative",
    trapi(
        {"chem": {"categories": [C]}, "gene": {"ids": [INS], "categories": [G]}},
        {
            "t": {
                "subject": "chem",
                "object": "gene",
                "predicates": ["biolink:affects"],
                "knowledge_type": "inferred",
                "qualifier_constraints": [
                    {
                        "qualifier_set": [
                            {
                                "qualifier_type_id": "biolink:object_aspect_qualifier",
                                "qualifier_value": "activity_or_abundance",
                            },
                            {
                                "qualifier_type_id": "biolink:object_direction_qualifier",
                                "qualifier_value": "increased",
                            },
                        ]
                    }
                ],
            }
        },
    ),
    checks=[log_contains("xCRG")],
    timeout=900,
)
case(
    "pathfinder_trapi_paths",
    "creative",
    trapi(
        {"n0": {"ids": [METFORMIN]}, "n1": {"ids": [T2D]}},
        paths={"p0": {"subject": "n0", "object": "n1"}},
    ),
    status=None,
    min_results=1,
    checks=[path_bindings],
    timeout=900,
    note="routed to the arax.pathfinder worker",
)
case(
    "pathfinder_dsl_connect_nodes",
    "creative",
    dsl(
        [
            f"add_qnode(key=n0, ids={METFORMIN})",
            f"add_qnode(key=n1, ids={T2D})",
            "add_qpath(key=p0, subject=n0, object=n1)",
            "connect(action=connect_nodes, max_path_length=3)",
        ]
    ),
    min_results=1,
    checks=[path_bindings],
    timeout=900,
)

# --- flexible TRAPI: not / any / all / optional groups / constraints ---
case(
    "not_exclude_edge",
    "flexible",
    trapi(
        {"n0": {"ids": [METFORMIN]}, "n1": {"categories": [G]}, "n2": {"ids": [T2D]}},
        {
            "e0": {"subject": "n0", "object": "n1"},
            "e1": {"subject": "n1", "object": "n2", "exclude": True},
        },
    ),
    timeout=600,
)
case(
    "not_exclude_with_optional_group",
    "flexible",
    trapi(
        {
            "n0": {"ids": [METFORMIN]},
            "n1": {"categories": [G]},
            "n2": {"categories": [D], "option_group_id": "o1"},
            "n3": {"categories": [P]},
        },
        {
            "e0": {"subject": "n0", "object": "n1"},
            "e1": {"subject": "n1", "object": "n2", "option_group_id": "o1"},
            "e2": {"subject": "n0", "object": "n3", "exclude": True},
        },
    ),
    timeout=600,
)
case(
    "optional_group",
    "flexible",
    trapi(
        {
            "n0": {"ids": [METFORMIN]},
            "n1": {"categories": [G]},
            "n2": {"categories": [D], "option_group_id": "o1"},
        },
        {
            "e0": {"subject": "n0", "object": "n1"},
            "e1": {"subject": "n1", "object": "n2", "option_group_id": "o1"},
        },
    ),
    min_results=1,
    timeout=600,
)
case(
    "any_is_set",
    "flexible",
    trapi(
        {
            "n0": {"ids": [METFORMIN, IBUPROFEN], "is_set": True},
            "n1": {"categories": [G]},
        },
        {"e0": {"subject": "n0", "object": "n1"}},
    ),
    min_results=1,
)
case(
    "any_set_interpretation_many",
    "flexible",
    trapi(
        {
            "n0": {"ids": [METFORMIN, IBUPROFEN], "set_interpretation": "MANY"},
            "n1": {"categories": [G]},
        },
        {"e0": {"subject": "n0", "object": "n1"}},
    ),
    min_results=1,
)
case(
    "all_set_interpretation_all",
    "flexible",
    trapi(
        {
            "n0": {
                "ids": ["uuid:1"],
                "set_interpretation": "ALL",
                "member_ids": [METFORMIN, IBUPROFEN],
                "categories": [SM],
            },
            "n1": {"categories": [G]},
        },
        {"e0": {"subject": "n0", "object": "n1"}},
    ),
    note="member_ids are not forwarded to the KP, as in ARAX (D-25)",
)
case(
    "attribute_constraint_fda_approved",
    "flexible",
    trapi(
        {
            "n0": {"ids": [T2D]},
            "n1": {
                "categories": [C],
                "constraints": [
                    {
                        "id": "biolink:highest_FDA_approval_status",
                        "name": "FDA approval",
                        "operator": "==",
                        "value": "regular approval",
                    }
                ],
            },
        },
        {"e0": {"subject": "n1", "object": "n0", "predicates": [TREATS]}},
    ),
)
case(
    "query_by_name_trapi",
    "flexible",
    trapi(
        {
            "n0": {"name": "type 2 diabetes mellitus", "categories": [D]},
            "n1": {"categories": [C]},
        },
        {"e0": {"subject": "n1", "object": "n0"}},
    ),
    http=400,
    status="QueryGraphNoIds",
    note="ARAX does not resolve a TRAPI qnode name (D-25)",
)

# --- araxi: the DSL ---
BASE_ONE_HOP = [
    f"add_qnode(key=n0, ids={METFORMIN})",
    "add_qnode(key=n1, categories=biolink:Disease)",
    "add_qedge(key=e0, subject=n0, object=n1)",
    "expand()",
]
BASE_TWO_HOP = [
    f"add_qnode(key=n0, ids={METFORMIN})",
    "add_qnode(key=n1, categories=biolink:Gene)",
    "add_qnode(key=n2, categories=biolink:Disease)",
    "add_qedge(key=e0, subject=n0, object=n1)",
    "add_qedge(key=e1, subject=n1, object=n2)",
    "expand()",
]
case(
    "dsl_expand_resultify", "araxi", dsl(BASE_ONE_HOP + ["resultify()"]), min_results=1
)
case(
    "dsl_create_envelope_and_name",
    "araxi",
    dsl(
        [
            "create_envelope()",
            "add_qnode(key=n0, name=metformin)",
            "add_qnode(key=n1, categories=biolink:Disease)",
            "add_qedge(key=e0, subject=n0, object=n1)",
            "expand()",
            "resultify()",
        ]
    ),
    min_results=1,
)
case(
    "dsl_unknown_name",
    "araxi",
    dsl(["add_qnode(key=n0, name=no such thing at all)"]),
    http=400,
    status="UnresolvableNodeName",
)
case(
    "dsl_overlay_ngd",
    "araxi",
    dsl(
        BASE_ONE_HOP
        + [
            "overlay(action=compute_ngd, virtual_relation_label=N1, subject_qnode_key=n0, object_qnode_key=n1)",
            "resultify()",
        ]
    ),
    min_results=1,
    checks=[edge_attribute("normalized_google_distance")],
)
case(
    "dsl_overlay_ngd_all_edges",
    "araxi",
    dsl(BASE_ONE_HOP + ["overlay(action=compute_ngd)", "resultify()"]),
    min_results=1,
    checks=[edge_attribute("normalized_google_distance")],
)
case(
    "dsl_overlay_fisher",
    "araxi",
    dsl(
        BASE_TWO_HOP
        + [
            "overlay(action=fisher_exact_test, subject_qnode_key=n1, object_qnode_key=n2, virtual_relation_label=F1)",
            "resultify()",
        ]
    ),
    min_results=1,
    checks=[edge_attribute("fisher_exact_test_p-value")],
    timeout=600,
)
case(
    "dsl_overlay_jaccard",
    "araxi",
    dsl(
        BASE_TWO_HOP
        + [
            "overlay(action=compute_jaccard, start_node_key=n0, intermediate_node_key=n1, end_node_key=n2, virtual_relation_label=J1)",
            "resultify()",
        ]
    ),
    min_results=1,
    checks=[edge_attribute("jaccard_index")],
    timeout=600,
)
case(
    "dsl_overlay_clinical_info",
    "araxi",
    dsl(
        BASE_ONE_HOP
        + [
            "overlay(action=overlay_clinical_info, paired_concept_frequency=true, virtual_relation_label=C1, subject_qnode_key=n0, object_qnode_key=n1)",
            "overlay(action=overlay_clinical_info, observed_expected_ratio=true, virtual_relation_label=C2, subject_qnode_key=n0, object_qnode_key=n1)",
            "overlay(action=overlay_clinical_info, chi_square=true, virtual_relation_label=C3, subject_qnode_key=n0, object_qnode_key=n1)",
            "resultify()",
        ]
    ),
    min_results=1,
    note="COHD values need cohd.io's OMOP ids to be in the COHD file",
)
case(
    "dsl_overlay_add_node_pmids",
    "araxi",
    dsl(BASE_ONE_HOP + ["overlay(action=add_node_pmids, max_num=3)", "resultify()"]),
    min_results=1,
    checks=[node_attribute("pubmed_ids")],
)
case(
    "dsl_filter_kg_after_expand",
    "araxi",
    dsl(
        BASE_ONE_HOP
        + [
            "filter_kg(action=remove_edges_by_predicate, edge_predicate=biolink:related_to, remove_connected_nodes=f)",
            "filter_kg(action=remove_nodes_by_category, node_category=biolink:Gene)",
            "filter_kg(action=remove_general_concept_nodes, perform_action=true)",
            "filter_kg(action=remove_orphaned_nodes)",
            "resultify()",
        ]
    ),
    min_results=1,
)
case(
    "dsl_filter_kg_after_overlay",
    "araxi",
    dsl(
        BASE_ONE_HOP
        + [
            "overlay(action=compute_ngd, virtual_relation_label=N1, subject_qnode_key=n0, object_qnode_key=n1)",
            "filter_kg(action=remove_edges_by_top_n, edge_attribute=normalized_google_distance, n=5, direction=above, top=f, remove_connected_nodes=t, qnode_keys=[n1])",
            "resultify()",
        ]
    ),
    known_defect="D-23",
)
case(
    "dsl_filter_results",
    "araxi",
    dsl(
        BASE_ONE_HOP
        + [
            "overlay(action=compute_ngd, virtual_relation_label=N1, subject_qnode_key=n0, object_qnode_key=n1)",
            "resultify()",
            "filter_results(action=sort_by_edge_attribute, edge_attribute=normalized_google_distance, direction=ascending, max_results=5)",
            "filter_results(action=sort_by_edge_count, direction=descending)",
            "filter_results(action=limit_number_of_results, max_results=3)",
        ]
    ),
    min_results=1,
    checks=[results_at_most(3)],
)
case(
    "dsl_scoreless_resultify_and_rank",
    "araxi",
    dsl(
        BASE_ONE_HOP
        + ["scoreless_resultify(ignore_edge_direction=true)", "rank_results()"]
    ),
    min_results=1,
)
case(
    "dsl_return_store",
    "araxi",
    dsl(BASE_ONE_HOP + ["resultify()", "return(message=true, store=true)"]),
    min_results=1,
)
case(
    "dsl_unknown_command",
    "araxi",
    dsl([f"add_qnode(key=n0, ids={METFORMIN})", "frobnicate(x=1)"]),
    http=400,
    status="UnrecognizedCommand",
)
case(
    "dsl_parse_error",
    "araxi",
    dsl([f"add_qnode(key=n0 ids={METFORMIN}"]),
    http=400,
    status="ActionsListEmpty",
)
case(
    "dsl_unknown_action",
    "araxi",
    dsl(BASE_ONE_HOP + ["overlay(action=not_an_action)"]),
    http=400,
    status="UnknownAction",
)

# --- workflows: TRAPI workflow operations (what the CQS and the ARS send) ---
ONE_HOP_NODES = {"n0": {"ids": [T2D]}, "n1": {"categories": [C]}}
ONE_HOP_EDGES = {"e0": {"subject": "n1", "object": "n0"}}
TWO_HOP_NODES = {
    "n0": {"ids": [METFORMIN]},
    "n1": {"categories": [G]},
    "n2": {"categories": [D]},
}
TWO_HOP_EDGES = {
    "e0": {"subject": "n0", "object": "n1"},
    "e1": {"subject": "n1", "object": "n2"},
}
case(
    "wf_lookup",
    "workflows",
    workflow([{"id": "lookup"}], ONE_HOP_NODES, ONE_HOP_EDGES),
    min_results=1,
)
case(
    "wf_lookup_and_score",
    "workflows",
    workflow([{"id": "lookup_and_score"}], ONE_HOP_NODES, ONE_HOP_EDGES),
    min_results=1,
)
case(
    "wf_cqs_style",
    "workflows",
    workflow(
        [
            {"id": "lookup"},
            {"id": "score"},
            {
                "id": "sort_results_score",
                "parameters": {"ascending_or_descending": "descending"},
            },
            {"id": "filter_results_top_n", "parameters": {"max_results": 5}},
        ],
        ONE_HOP_NODES,
        ONE_HOP_EDGES,
    ),
    min_results=1,
    checks=[results_at_most(5)],
)
case(
    "wf_fill_bind_complete",
    "workflows",
    workflow(
        [{"id": "fill"}, {"id": "bind"}, {"id": "complete_results"}, {"id": "score"}],
        ONE_HOP_NODES,
        ONE_HOP_EDGES,
    ),
    min_results=1,
)
case(
    "wf_fill_allowlist",
    "workflows",
    workflow(
        [
            {"id": "fill", "parameters": {"allowlist": ["infores:retriever"]}},
            {"id": "bind"},
            {"id": "score"},
        ],
        ONE_HOP_NODES,
        ONE_HOP_EDGES,
    ),
    min_results=1,
)
case(
    "wf_fill_denylist",
    "workflows",
    workflow(
        [{"id": "fill", "parameters": {"denylist": ["infores:semmeddb"]}}],
        ONE_HOP_NODES,
        ONE_HOP_EDGES,
    ),
    http=400,
    status="NotImplementedError",
)
case(
    "wf_overlays",
    "workflows",
    workflow(
        [
            {"id": "lookup"},
            {
                "id": "overlay_compute_ngd",
                "parameters": {
                    "virtual_relation_label": "N1",
                    "qnode_keys": ["n0", "n2"],
                },
            },
            {
                "id": "overlay_compute_jaccard",
                "parameters": {
                    "virtual_relation_label": "J1",
                    "end_node_keys": ["n0", "n2"],
                    "intermediate_node_key": "n1",
                },
            },
            {
                "id": "overlay_fisher_exact_test",
                "parameters": {
                    "virtual_relation_label": "F1",
                    "subject_qnode_key": "n1",
                    "object_qnode_key": "n2",
                },
            },
            {"id": "score"},
        ],
        TWO_HOP_NODES,
        TWO_HOP_EDGES,
    ),
    min_results=1,
    checks=[edge_attribute("normalized_google_distance")],
    timeout=600,
)
case(
    "wf_annotate_nodes",
    "workflows",
    workflow(
        [
            {"id": "lookup"},
            {"id": "annotate_nodes", "parameters": {"attributes": ["pmids"]}},
            {"id": "score"},
        ],
        ONE_HOP_NODES,
        ONE_HOP_EDGES,
    ),
    min_results=1,
    checks=[node_attribute("pubmed_ids")],
)
case(
    "wf_filter_kgraph",
    "workflows",
    workflow(
        [
            {"id": "lookup"},
            {
                "id": "overlay_compute_ngd",
                "parameters": {
                    "virtual_relation_label": "N1",
                    "qnode_keys": ["n0", "n1"],
                },
            },
            {
                "id": "filter_kgraph_top_n",
                "parameters": {
                    "edge_attribute": "normalized_google_distance",
                    "max_edges": 5,
                },
            },
            {"id": "filter_kgraph_orphans"},
            {"id": "score"},
        ],
        ONE_HOP_NODES,
        ONE_HOP_EDGES,
    ),
    known_defect="D-23",
)
case(
    "wf_connect_knodes",
    "workflows",
    workflow(
        [{"id": "lookup"}, {"id": "overlay_connect_knodes"}, {"id": "score"}],
        TWO_HOP_NODES,
        TWO_HOP_EDGES,
    ),
    known_defect="D-26",
    timeout=600,
)
case(
    "wf_top_n_not_int",
    "workflows",
    workflow(
        [
            {"id": "lookup"},
            {"id": "filter_results_top_n", "parameters": {"max_results": "5"}},
        ],
        ONE_HOP_NODES,
        ONE_HOP_EDGES,
    ),
    http=400,
    status="UnhandledError",
    note="D-24: upstream's assert",
)
case(
    "wf_unknown_operation",
    "workflows",
    workflow([{"id": "teleport"}], ONE_HOP_NODES, ONE_HOP_EDGES),
    http=400,
    status=None,
    note="ARAX answers NotImplementedError / UnhandledError",
)

# --- validation: the errors ARAX answers ---
case(
    "val_no_message",
    "validation",
    {"submitter": "test_arax_queries"},
    http=400,
    status="NoQueryMessageOrOperations",
)
case(
    "val_unknown_qnode_property",
    "validation",
    trapi(
        {"n0": {"ids": [METFORMIN], "colour": "red"}, "n1": {}},
        {"e0": {"subject": "n0", "object": "n1"}},
    ),
    http=400,
    status="UnknownQNodeProperty",
)
case(
    "val_singular_predicate",
    "validation",
    trapi(
        {"n0": {"ids": [METFORMIN]}, "n1": {}},
        {"e0": {"subject": "n0", "object": "n1", "predicate": TREATS}},
    ),
    http=400,
    status="UnknownQEdgeProperty",
)
case(
    "val_no_edges_or_paths",
    "validation",
    trapi({"n0": {"ids": [METFORMIN]}}),
    http=400,
    status="MissingQEdgeAndQPath",
    note="edges: {} would be a single-node lookup",
)
case(
    "val_no_pinned_node",
    "validation",
    trapi(
        {"n0": {"categories": [C]}, "n1": {"categories": [D]}},
        {"e0": {"subject": "n0", "object": "n1"}},
    ),
    http=400,
    status="QueryGraphNoIds",
)
case(
    "val_bad_kp_timeout",
    "validation",
    trapi(ONE_HOP_NODES, ONE_HOP_EDGES, query_options={"kp_timeout": "soon"}),
    http=400,
    status="UserTimeoutNotInt",
)
case(
    "val_query_options",
    "validation",
    trapi(
        ONE_HOP_NODES,
        ONE_HOP_EDGES,
        query_options={"kp_timeout": 30, "prune_threshold": 50},
    ),
    min_results=1,
)

# --- delivery: streaming, the KP cache, message_uris ---
case(
    "stream_progress",
    "delivery",
    trapi(
        {"n0": {"ids": [T2D]}, "n1": {"categories": [C]}},
        {"e0": {"subject": "n1", "object": "n0", "predicates": [TREATS]}},
    ),
    stream=True,
    min_results=1,
)
case(
    "kp_cache_hit",
    "delivery",
    trapi(
        {"n0": {"ids": [ASTHMA]}, "n1": {"categories": [C]}},
        {"e0": {"subject": "n1", "object": "n0", "predicates": [TREATS]}},
    ),
    after=True,
    checks=[query_plan_from_cache],
    note="runs lookup_asthma_warm_cache's query again; needs ARAX_KP_CACHE_ENABLED",
)
case(
    "lookup_asthma_warm_cache",
    "delivery",
    trapi(
        {"n0": {"ids": [ASTHMA]}, "n1": {"categories": [C]}},
        {"e0": {"subject": "n1", "object": "n0", "predicates": [TREATS]}},
    ),
    min_results=0,
)
case(
    "bypass_cache",
    "delivery",
    trapi(
        {"n0": {"ids": [ASTHMA]}, "n1": {"categories": [C]}},
        {"e0": {"subject": "n1", "object": "n0", "predicates": [TREATS]}},
        query_options={"bypass_cache": True},
    ),
    after=True,
)
case(
    "message_uris_reuse_a_response",
    "delivery",
    None,
    after=True,
    min_results=1,
    note="loads dsl_return_store's response by its URL and filters it",
)

# --- endpoints the UI uses ---


def json_ok(pred=None, what=""):
    def check(resp):
        if resp.status_code != 200:
            return f"HTTP {resp.status_code}: {resp.text[:200]}"
        try:
            body = resp.json()
        except ValueError:
            return "response is not JSON"
        detail = what
        try:
            ok = pred is None or bool(pred(body))
        except Exception as e:  # an unexpected shape is a failure, not a crash
            ok = False
            detail = f"{what}; {type(e).__name__}: {e}".strip("; ")
        if not ok:
            return f"unexpected body ({detail}): {json.dumps(body)[:200]}"

    return check


case(
    "endpoint_response_by_id",
    "endpoints",
    method="GET",
    path="/response/{rid}",
    after=True,
    endpoint_check=json_ok(
        lambda b: (b.get("message") or {}).get("results"),
        "a stored response with results",
    ),
)
case(
    "endpoint_response_ui_x_prefix",
    "endpoints",
    method="GET",
    path="/response/X{rid}",
    after=True,
    endpoint_check=json_ok(
        lambda b: (b.get("message") or {}).get("results"), "the UI's X prefix (DEC-19)"
    ),
)
case(
    "endpoint_response_missing",
    "endpoints",
    method="GET",
    path="/response/nonexistent0",
    after=True,
    endpoint_check=lambda r: (
        None if r.status_code == 404 else f"HTTP {r.status_code}, expected 404"
    ),
)
case(
    "endpoint_status_recent",
    "endpoints",
    method="GET",
    path="/status?last_n_hours=1",
    after=True,
    endpoint_check=json_ok(lambda b: b.get("recent_queries"), "recent queries listed"),
)
case(
    "endpoint_status_active",
    "endpoints",
    method="GET",
    path="/status?mode=active",
    after=True,
    endpoint_check=json_ok(lambda b: "recent_queries" in b),
)
case(
    "endpoint_status_id",
    "endpoints",
    method="GET",
    path="/status?id={qid}",
    after=True,
    endpoint_check=json_ok(
        lambda b: b.get("message") is not None or b.get("operations") is not None,
        "the stored input query",
    ),
)
case(
    "endpoint_status_kp_cache",
    "endpoints",
    method="GET",
    path="/status?mode=kp_cache",
    after=True,
    endpoint_check=json_ok(lambda b: "cache_stats" in b and "cache_data" in b),
)
case(
    "endpoint_status_site_config",
    "endpoints",
    method="GET",
    path="/status?mode=site_config",
    endpoint_check=json_ok(lambda b: "config" in b),
)
case(
    "endpoint_status_smartapi",
    "endpoints",
    method="GET",
    path="/status?authorization=smartapi",
    endpoint_check=json_ok(),
    note="needs SmartAPI",
)
case(
    "endpoint_entity_get",
    "endpoints",
    method="GET",
    path=f"/entity?q={METFORMIN}",
    endpoint_check=json_ok(lambda b: METFORMIN in b),
)
case(
    "endpoint_entity_post",
    "endpoints",
    method="POST",
    path="/entity",
    json_body=[METFORMIN, "metformin"],
    endpoint_check=json_ok(lambda b: METFORMIN in b),
)
case(
    "endpoint_meta_kg",
    "endpoints",
    method="GET",
    path="/meta_knowledge_graph",
    endpoint_check=json_ok(lambda b: b.get("nodes") and b.get("edges")),
)
case(
    "endpoint_meta_kg_simple",
    "endpoints",
    method="GET",
    path="/meta_knowledge_graph?format=simple",
    endpoint_check=json_ok(lambda b: bool(b)),
)
case(
    "endpoint_autocomplete",
    "endpoints",
    method="GET",
    path="/rtxcomplete/nodeslike?word=metf&limit=5&callback=cb",
    endpoint_check=lambda r: (
        None
        if r.status_code == 200 and r.text.startswith("cb(")
        else f"HTTP {r.status_code}: {r.text[:120]}"
    ),
)
case(
    "endpoint_post_response",
    "endpoints",
    method="POST",
    path="/response",
    json_body={"message": {"results": []}, "description": "test_arax_queries callback"},
    endpoint_check=lambda r: (
        None if r.status_code < 300 else f"HTTP {r.status_code}: {r.text[:120]}"
    ),
)


# ---------------------------------------------------------------------------
# Running
# ---------------------------------------------------------------------------


@dataclass
class Outcome:
    case: Case
    verdict: str  # PASS FAIL XFAIL XPASS
    problems: list
    seconds: float
    n_results: Optional[int] = None
    status: Optional[str] = None
    http: Optional[int] = None


def check_query_response(c: Case, http_status: int, body: dict) -> list:
    problems = []
    if http_status != c.http:
        problems.append(f"HTTP {http_status}, expected {c.http}")
    status = body.get("status")
    if c.status is not None and status != c.status:
        problems.append(
            f"status {status!r}, expected {c.status!r} ({(body.get('description') or '')[:150]})"
        )
    n = len(_msg(body).get("results") or [])
    if n < c.min_results:
        problems.append(f"{n} results, expected at least {c.min_results}")
    for check in c.checks:
        error = check(body)
        if error:
            problems.append(error)
    return problems


def read_stream(resp) -> tuple[dict, list]:
    """The final envelope of ARAX's NDJSON stream, and the problems with the stream."""
    lines = [line for line in resp.iter_lines() if line.strip()]
    problems = []
    parsed = []
    for line in lines:
        try:
            parsed.append(json.loads(line))
        except ValueError:
            problems.append(f"stream line is not JSON: {line[:80]}")
    if not parsed:
        return {}, ["the stream was empty"]
    final = parsed[-1]
    progress = parsed[:-1]
    if not any("pid" in p and "authorization" in p for p in progress):
        problems.append("no kill token ({pid, authorization}) in the stream")
    if not any(p.get("level") and p.get("message") for p in progress):
        problems.append("no log lines in the stream")
    if not any("qedge_keys" in p for p in progress):
        problems.append("no query_plan update in the stream")
    if "logs" not in final:
        problems.append("the last line is not the response envelope")
    return final, problems


class Runner:
    def __init__(self, base, out_dir):
        self.base = base.rstrip("/")
        self.out_dir = out_dir
        self.bodies = {}  # case name -> response body

    def save(self, name, content):
        path = self.out_dir / f"{name}.json"
        path.write_text(
            json.dumps(content, indent=2) if not isinstance(content, str) else content
        )

    def run_query(self, c: Case) -> Outcome:
        body = c.body
        if c.name == "message_uris_reuse_a_response":
            stored = self.bodies.get("dsl_return_store") or {}
            if not stored.get("id"):
                return Outcome(
                    c, "FAIL", ["dsl_return_store has no response id to reuse"], 0
                )
            body = dsl(
                [
                    "filter_results(action=limit_number_of_results, max_results=2)",
                    "return(message=true, store=false)",
                ]
            )
            body["operations"]["message_uris"] = [stored["id"]]
            body.pop("message")
            c.checks = [results_at_most(2)]
        if c.stream:
            body = dict(body, stream_progress=True)
        start = time.perf_counter()
        problems = []
        try:
            with httpx.Client(timeout=httpx.Timeout(c.timeout + 60)) as client:
                if c.stream:
                    with client.stream("POST", f"{self.base}/query", json=body) as resp:
                        http_status = resp.status_code
                        result, stream_problems = read_stream(resp)
                        problems += stream_problems
                else:
                    resp = client.post(f"{self.base}/query", json=body)
                    http_status = resp.status_code
                    try:
                        result = resp.json()
                    except ValueError:
                        result = {"_not_json": resp.text[:2000]}
        except Exception as e:
            return self._outcome(
                c, [f"{type(e).__name__}: {e}"], time.perf_counter() - start
            )
        seconds = time.perf_counter() - start
        self.bodies[c.name] = result
        self.save(c.name, result)
        problems += check_query_response(c, http_status, result)
        out = self._outcome(c, problems, seconds)
        out.n_results = len(_msg(result).get("results") or [])
        out.status, out.http = result.get("status"), http_status
        return out

    def run_endpoint(self, c: Case) -> Outcome:
        path = c.path
        rid = self._some_response_id()
        qid = self._some_query_id()
        if "{rid}" in path or "{qid}" in path:
            if (rid is None and "{rid}" in path) or (qid is None and "{qid}" in path):
                return Outcome(c, "FAIL", ["no earlier response to look up"], 0)
            path = path.replace("{rid}", rid or "").replace("{qid}", qid or "")
        start = time.perf_counter()
        try:
            with httpx.Client(timeout=120) as client:
                resp = client.request(c.method, f"{self.base}{path}", json=c.json_body)
        except Exception as e:
            return self._outcome(
                c, [f"{type(e).__name__}: {e}"], time.perf_counter() - start
            )
        seconds = time.perf_counter() - start
        self.save(c.name, resp.text)
        error = c.endpoint_check(resp)
        out = self._outcome(c, [error] if error else [], seconds)
        out.http = resp.status_code
        return out

    def _some_response_id(self):
        for name in (
            "dsl_return_store",
            "lookup_one_hop_treats",
            "dsl_expand_resultify",
        ):
            env_id = (self.bodies.get(name) or {}).get("id") or ""
            if env_id and (self.bodies[name].get("message") or {}).get("results"):
                return env_id.rsplit("/", 1)[-1]
        return None

    def _some_query_id(self):
        try:
            with httpx.Client(timeout=60) as client:
                recent = client.get(f"{self.base}/status?last_n_hours=1").json()
            for q in recent.get("recent_queries") or []:
                if q.get("query_id"):
                    return q["query_id"]
        except Exception:
            pass
        return None

    @staticmethod
    def _outcome(c, problems, seconds):
        if c.known_defect:
            verdict = "XFAIL" if problems else "XPASS"
        else:
            verdict = "FAIL" if problems else "PASS"
        return Outcome(c, verdict, problems, seconds)

    def run(self, c: Case) -> Outcome:
        try:
            return self.run_endpoint(c) if c.method else self.run_query(c)
        except Exception as e:  # one broken case must not stop the run
            return self._outcome(
                c, [f"the check itself failed: {type(e).__name__}: {e}"], 0
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--base",
        default="http://localhost:5439/arax",
        help="Shepherd's ARAX API base URL",
    )
    parser.add_argument(
        "--only", nargs="+", help="run cases whose name contains any of these"
    )
    parser.add_argument("--group", nargs="+", help="run only these groups")
    parser.add_argument("--list", action="store_true", help="list the cases and exit")
    parser.add_argument("--workers", type=int, default=4, help="queries to run at once")
    parser.add_argument(
        "--out", default="responses/arax-validation", help="where responses are saved"
    )
    args = parser.parse_args()

    selected = [
        c
        for c in CASES
        if (not args.group or c.group in args.group)
        and (not args.only or any(s in c.name for s in args.only))
    ]
    if args.list:
        for group in dict.fromkeys(c.group for c in CASES):
            print(f"\n{group}")
            for c in CASES:
                if c.group == group:
                    extra = f"  [{c.known_defect}]" if c.known_defect else ""
                    print(f"  {c.name}{extra}" + (f"  -- {c.note}" if c.note else ""))
        return 0

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    runner = Runner(args.base, out_dir)
    first = [c for c in selected if not c.after]
    later = [c for c in selected if c.after]
    print(
        f"Running {len(selected)} cases against {args.base} ({args.workers} at a time)\n"
    )
    start = time.time()
    outcomes = []

    def report(o: Outcome):
        extra = []
        if o.http is not None:
            extra.append(f"HTTP {o.http}")
        if o.status:
            extra.append(o.status)
        if o.n_results is not None:
            extra.append(f"{o.n_results} results")
        defect = f" [{o.case.known_defect}]" if o.case.known_defect else ""
        print(
            f"{o.verdict:5s} {o.case.name:40s} {o.seconds:6.1f}s  {', '.join(extra)}{defect}",
            flush=True,
        )
        for p in o.problems:
            print(f"        - {p}")

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for o in pool.map(runner.run, first):
            report(o)
            outcomes.append(o)
    for c in later:  # after the others: they use their responses, or the warmed cache
        o = runner.run(c)
        report(o)
        outcomes.append(o)

    counts = {
        v: sum(o.verdict == v for o in outcomes)
        for v in ("PASS", "FAIL", "XFAIL", "XPASS")
    }
    print(
        f"\n{counts['PASS']} passed, {counts['FAIL']} failed, {counts['XFAIL']} known upstream defects "
        f"(xfail), {counts['XPASS']} xpass in {time.time() - start:.0f}s. Responses are in {out_dir}/"
    )
    summary = [
        {
            "name": o.case.name,
            "group": o.case.group,
            "verdict": o.verdict,
            "problems": o.problems,
            "seconds": round(o.seconds, 2),
            "http": o.http,
            "status": o.status,
            "n_results": o.n_results,
            "known_defect": o.case.known_defect,
        }
        for o in outcomes
    ]
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())

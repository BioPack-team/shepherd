"""More end-to-end ARAXQuery cases, for what the ARAX team asked to be preserved.

Appended to query_cases.CASES, so they run through the same harness: recorded
from upstream ARAX's own ARAXQuery, replayed through the port, compared field
by field.

- ARAXi: every command, and every action of filter_kg, filter_results and
  overlay that runs offline (add_node_pmids with NCBI eUtils stubbed on both
  sides);
- TRAPI workflows (what CQS and the ARS send): every operation
  operation_to_ARAXi implements, the parameter variants it translates, and
  its errors;
- "not" (exclude=true), "any"/"all" (is_set, set_interpretation, member_ids),
  optional groups, and query by name (DSL add_qnode(name=...) and a TRAPI
  qnode's name);
- xDTD / MVP1 variants, and the xCRG / MVP2 TRAPI route.
"""

C = "biolink:ChemicalEntity"
SM = "biolink:SmallMolecule"
D = "biolink:Disease"
G = "biolink:Gene"
P = "biolink:PhenotypicFeature"
T = "biolink:treats"


def qg(nodes, edges=None, **kw):
    d = {"nodes": nodes}
    if edges is not None:
        d["edges"] = edges
    d.update(kw)
    return d


def trapi(query_graph, **kw):
    q = {"message": {"query_graph": query_graph}}
    q.update(kw)
    return q


def ops(actions, message=None, **kw):
    q = {"operations": {"actions": actions}}
    if message is not None:
        q["message"] = message
    q.update(kw)
    return q


# A two-hop chemical -> gene -> disease graph with NGD and FET on it, the
# starting point for the filter cases
TWO_HOP = [
    "add_qnode(key=n0, ids=CHEBI:3)",
    "add_qnode(key=n1, categories=biolink:Gene)",
    "add_qnode(key=n2, categories=biolink:Disease)",
    "add_qedge(key=e0, subject=n0, object=n1)",
    "add_qedge(key=e1, subject=n1, object=n2)",
    "expand()",
    "overlay(action=compute_ngd, virtual_relation_label=N1, subject_qnode_key=n0, object_qnode_key=n2)",
    "overlay(action=fisher_exact_test, subject_qnode_key=n1, object_qnode_key=n2, virtual_relation_label=F1)",
]

# applied_to_treat edges carry a numeric biolink:number_of_cases attribute
APPLIED_TO_TREAT = [
    "add_qnode(key=n0, ids=[CHEBI:1,CHEBI:2,CHEBI:4,CHEBI:5,CHEBI:7])",
    "add_qnode(key=n1, categories=biolink:Disease)",
    "add_qedge(key=e0, subject=n0, object=n1, predicates=biolink:applied_to_treat)",
    "expand()",
]

ONE_HOP_QG = qg(
    {"n0": {"ids": ["MONDO:2"]}, "n1": {"categories": [C]}},
    {"e0": {"subject": "n1", "object": "n0"}},
)
TWO_HOP_QG = qg(
    {
        "n0": {"ids": ["CHEBI:3"]},
        "n1": {"categories": [G]},
        "n2": {"categories": [D]},
    },
    {"e0": {"subject": "n0", "object": "n1"}, "e1": {"subject": "n1", "object": "n2"}},
)


def wf(workflow, query_graph=TWO_HOP_QG, **kw):
    return trapi(query_graph, workflow=workflow, **kw)


LOOKUP_NGD = [
    {"id": "lookup"},
    {
        "id": "overlay_compute_ngd",
        "parameters": {"virtual_relation_label": "N1", "qnode_keys": ["n0", "n2"]},
    },
]

CASES = [
    # --- ARAXi commands ---
    (
        "araxi_create_envelope_and_name",
        ops(
            [
                "create_envelope()",
                "add_qnode(key=n0, name=MONDO thing 2)",
                "add_qnode(key=n1, categories=biolink:SmallMolecule)",
                "add_qedge(key=e0, subject=n1, object=n0)",
                "expand()",
                "resultify()",
            ]
        ),
    ),
    (
        "araxi_unknown_name",
        ops(["add_qnode(key=n0, name=no such thing)"]),
    ),
    (
        "araxi_add_qpath",
        ops(
            [
                "add_qnode(key=n0, ids=CHEBI:1)",
                "add_qnode(key=n1, ids=MONDO:1)",
                "add_qpath(key=p0, subject=n0, object=n1)",
                "return(message=true, store=false)",
            ]
        ),
    ),
    (
        "araxi_is_set_and_option_group",
        ops(
            [
                "add_qnode(key=n0, ids=[CHEBI:3,CHEBI:6], is_set=true)",
                "add_qnode(key=n1, categories=biolink:Gene)",
                "add_qnode(key=n2, categories=biolink:Disease, option_group_id=g1)",
                "add_qedge(key=e0, subject=n0, object=n1)",
                "add_qedge(key=e1, subject=n1, object=n2, option_group_id=g1)",
                "expand()",
                "resultify()",
            ]
        ),
    ),
    (
        "araxi_exclude",
        ops(
            [
                "add_qnode(key=n0, ids=CHEBI:3)",
                "add_qnode(key=n1, categories=biolink:Gene)",
                "add_qnode(key=n2, categories=biolink:Disease)",
                "add_qedge(key=e0, subject=n0, object=n1)",
                "add_qedge(key=e1, subject=n1, object=n2, exclude=true)",
                "expand()",
                "resultify()",
            ]
        ),
    ),
    # --- overlay ---
    (
        "overlay_add_node_pmids",
        ops(
            [
                "add_qnode(key=n0, ids=CHEBI:3)",
                "add_qnode(key=n1, categories=biolink:Gene)",
                "add_qedge(key=e0, subject=n0, object=n1)",
                "expand()",
                "overlay(action=add_node_pmids, max_num=3)",
                "resultify()",
            ]
        ),
    ),
    (
        "overlay_jaccard_and_fet_rel_edge",
        ops(
            TWO_HOP
            + [
                "overlay(action=compute_jaccard, start_node_key=n0, intermediate_node_key=n1, end_node_key=n2, virtual_relation_label=J1)",
                "overlay(action=fisher_exact_test, subject_qnode_key=n0, object_qnode_key=n1, rel_edge_key=e0, virtual_relation_label=F2)",
                "resultify()",
            ]
        ),
    ),
    # --- filter_kg: every action ---
    (
        "filter_kg_predicate_and_orphans",
        ops(
            TWO_HOP
            + [
                "filter_kg(action=remove_edges_by_predicate, edge_predicate=biolink:interacts_with, remove_connected_nodes=f)",
                "filter_kg(action=remove_orphaned_nodes)",
                "resultify()",
            ]
        ),
    ),
    (
        "filter_kg_top_n_and_percentile",
        ops(
            TWO_HOP
            + [
                "filter_kg(action=remove_edges_by_top_n, edge_attribute=normalized_google_distance, n=3, direction=above, top=f, remove_connected_nodes=t, qnode_keys=[n2])",
                "filter_kg(action=remove_edges_by_percentile, edge_attribute=fisher_exact_test_p-value, threshold=60, direction=above, remove_connected_nodes=f)",
                "resultify()",
            ]
        ),
    ),
    (
        "filter_kg_stats",
        ops(
            TWO_HOP
            + [
                "filter_kg(action=remove_edges_by_stats, edge_attribute=normalized_google_distance, type=n, value=2, direction=below, top=f, remove_connected_nodes=t, qnode_keys=[n2])",
                "resultify()",
            ]
        ),
    ),
    (
        "filter_kg_discrete_attribute_and_property",
        ops(
            TWO_HOP
            + [
                "filter_kg(action=remove_edges_by_discrete_attribute, edge_attribute=biolink:primary_knowledge_source, value=infores:semmeddb, remove_connected_nodes=f)",
                "filter_kg(action=remove_nodes_by_property, node_property=name, property_value=NCBIGene thing 1)",
                "resultify()",
            ]
        ),
    ),
    # The same actions straight after expand() (no overlay or resultify first,
    # which leave edges without qedge_keys and make upstream crash above), on
    # the number_of_cases attribute the Retriever returns
    (
        "filter_kg_after_expand_numeric",
        ops(
            APPLIED_TO_TREAT
            + [
                "filter_kg(action=remove_edges_by_top_n, edge_attribute=biolink:number_of_cases, n=4, direction=below, top=t, remove_connected_nodes=f)",
                "filter_kg(action=remove_edges_by_percentile, edge_attribute=biolink:number_of_cases, threshold=20, direction=below, remove_connected_nodes=t, qnode_keys=[n1])",
                "filter_kg(action=remove_edges_by_std_dev, edge_attribute=biolink:number_of_cases, threshold=1, direction=above, top=t, remove_connected_nodes=f)",
                "filter_kg(action=remove_edges_by_continuous_attribute, edge_attribute=biolink:number_of_cases, threshold=5, direction=below, remove_connected_nodes=f)",
                "filter_kg(action=remove_orphaned_nodes)",
                "resultify()",
            ]
        ),
    ),
    (
        "filter_kg_after_expand_discrete_and_property",
        ops(
            APPLIED_TO_TREAT
            + [
                "filter_kg(action=remove_edges_by_discrete_attribute, edge_attribute=biolink:number_of_cases, value=7, remove_connected_nodes=f)",
                "filter_kg(action=remove_nodes_by_property, node_property=name, property_value=MONDO thing 4)",
                "filter_kg(action=remove_nodes_by_category, node_category=biolink:Gene)",
                "resultify()",
            ]
        ),
    ),
    (
        "filter_results_counts",
        ops(
            TWO_HOP
            + [
                "resultify()",
                "filter_results(action=sort_by_edge_count, direction=descending)",
                "filter_results(action=sort_by_node_count, direction=ascending, max_results=4)",
                "filter_results(action=sort_by_edge_attribute, edge_attribute=normalized_google_distance, direction=ascending, max_results=3, prune_kg=true)",
            ]
        ),
    ),
    (
        "wf_filter_kgraph_after_fill",
        wf(
            [
                {"id": "fill"},
                {
                    "id": "filter_kgraph_top_n",
                    "parameters": {
                        "edge_attribute": "biolink:number_of_cases",
                        "max_edges": 3,
                    },
                },
                {
                    "id": "filter_kgraph_discrete_kedge_attribute",
                    "parameters": {
                        "edge_attribute": "biolink:number_of_cases",
                        "remove_value": 7,
                    },
                },
                {"id": "filter_kgraph_orphans"},
                {"id": "bind"},
                {"id": "score"},
            ],
            qg(
                {
                    "n0": {
                        "ids": ["CHEBI:1", "CHEBI:2", "CHEBI:4", "CHEBI:5", "CHEBI:7"]
                    },
                    "n1": {"categories": [D]},
                },
                {
                    "e0": {
                        "subject": "n0",
                        "object": "n1",
                        "predicates": ["biolink:applied_to_treat"],
                    }
                },
            ),
        ),
    ),
    # --- filter_results: every sort ---
    (
        "filter_results_sorts",
        ops(
            TWO_HOP
            + [
                "resultify()",
                "filter_results(action=sort_by_node_attribute, node_attribute=biolink:description, direction=descending, max_results=6)",
                "filter_results(action=sort_by_edge_count, direction=descending)",
                "filter_results(action=sort_by_node_count, direction=ascending, max_results=4)",
            ]
        ),
    ),
    # --- TRAPI workflows: every operation operation_to_ARAXi implements ---
    (
        "wf_lookup_and_score",
        wf([{"id": "lookup_and_score"}], ONE_HOP_QG),
    ),
    (
        "wf_cqs_style",
        wf(
            [
                {"id": "lookup"},
                {"id": "score"},
                {
                    "id": "sort_results_score",
                    "parameters": {"ascending_or_descending": "descending"},
                },
                {"id": "filter_results_top_n", "parameters": {"max_results": 5}},
            ],
            ONE_HOP_QG,
        ),
    ),
    (
        "wf_overlays",
        wf(
            LOOKUP_NGD
            + [
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
                {
                    "id": "overlay_fisher_exact_test",
                    "parameters": {
                        "virtual_relation_label": "F2",
                        "subject_qnode_key": "n0",
                        "object_qnode_key": "n1",
                        "rel_edge_key": "e0",
                    },
                },
                {"id": "score"},
            ]
        ),
    ),
    (
        "wf_connect_knodes",
        wf([{"id": "lookup"}, {"id": "overlay_connect_knodes"}, {"id": "score"}]),
    ),
    (
        "wf_filter_kgraph_numeric",
        wf(
            LOOKUP_NGD
            + [
                {
                    "id": "filter_kgraph_top_n",
                    "parameters": {
                        "edge_attribute": "normalized_google_distance",
                        "max_edges": 4,
                        "keep_top_or_bottom": "bottom",
                        "qnode_keys": ["n2"],
                    },
                },
                {
                    "id": "filter_kgraph_std_dev",
                    "parameters": {
                        "edge_attribute": "normalized_google_distance",
                        "threshold": 0.5,
                        "remove_above_or_below": "above",
                        "qedge_keys": ["N1"],
                    },
                },
                {
                    "id": "filter_kgraph_percentile",
                    "parameters": {
                        "edge_attribute": "normalized_google_distance",
                        "threshold": 90,
                        "remove_above_or_below": "above",
                    },
                },
                {
                    "id": "filter_kgraph_continuous_kedge_attribute",
                    "parameters": {
                        "edge_attribute": "normalized_google_distance",
                        "threshold": 0.8,
                        "remove_above_or_below": "above",
                        "qnode_keys": ["n2"],
                    },
                },
                {"id": "filter_kgraph_orphans"},
                {"id": "score"},
            ]
        ),
    ),
    (
        "wf_filter_kgraph_discrete",
        wf(
            [
                {"id": "lookup"},
                {
                    "id": "filter_kgraph_discrete_kedge_attribute",
                    "parameters": {
                        "edge_attribute": "biolink:primary_knowledge_source",
                        "remove_value": "infores:ctd",
                    },
                },
                {"id": "score"},
            ]
        ),
    ),
    (
        "wf_fill_qedge_keys_and_allowlist",
        wf(
            [
                {"id": "fill", "parameters": {"qedge_keys": ["e0"]}},
                {
                    "id": "fill",
                    "parameters": {
                        "allowlist": ["infores:retriever"],
                        "qedge_keys": ["e1"],
                    },
                },
                {"id": "bind"},
                {"id": "complete_results"},
                {"id": "score"},
            ]
        ),
    ),
    (
        "wf_fill_denylist_error",
        wf([{"id": "fill", "parameters": {"denylist": ["infores:semmeddb"]}}]),
    ),
    (
        "wf_annotate_nodes",
        wf(
            [
                {"id": "lookup"},
                {"id": "annotate_nodes", "parameters": {"attributes": ["pmids"]}},
                {"id": "annotate_nodes", "parameters": {"attributes": ["other"]}},
                {"id": "score"},
            ],
            ONE_HOP_QG,
        ),
    ),
    (
        "wf_filter_results_top_n_not_int",
        wf(
            [
                {"id": "lookup"},
                {"id": "filter_results_top_n", "parameters": {"max_results": "5"}},
            ],
            ONE_HOP_QG,
        ),
    ),
    (
        "wf_with_runner_parameters",
        wf(
            [
                {
                    "id": "lookup",
                    "runner_parameters": {"allowlist": ["infores:retriever"]},
                },
                {"id": "score"},
            ],
            ONE_HOP_QG,
        ),
    ),
    # --- TRAPI "not", "any"/"all", names ---
    (
        "trapi_exclude",
        trapi(
            qg(
                {
                    "n0": {"ids": ["CHEBI:3"]},
                    "n1": {"categories": [G]},
                    "n2": {"categories": [D]},
                },
                {
                    "e0": {"subject": "n0", "object": "n1"},
                    "e1": {"subject": "n1", "object": "n2", "exclude": True},
                },
            )
        ),
    ),
    (
        "trapi_set_interpretation_all",
        trapi(
            qg(
                {
                    "n0": {
                        "ids": ["uuid:set1"],
                        "set_interpretation": "ALL",
                        "member_ids": ["CHEBI:3", "CHEBI:6"],
                        "categories": [SM],
                    },
                    "n1": {"categories": [G]},
                },
                {"e0": {"subject": "n0", "object": "n1"}},
            )
        ),
    ),
    (
        "trapi_set_interpretation_many_and_is_set",
        trapi(
            qg(
                {
                    "n0": {"ids": ["CHEBI:3", "CHEBI:6"], "set_interpretation": "MANY"},
                    "n1": {"categories": [G], "is_set": True},
                    "n2": {"categories": [D]},
                },
                {
                    "e0": {"subject": "n0", "object": "n1"},
                    "e1": {"subject": "n1", "object": "n2"},
                },
            )
        ),
    ),
    (
        "trapi_qnode_name",
        trapi(
            qg(
                {
                    "n0": {"name": "MONDO thing 2", "categories": [D]},
                    "n1": {"categories": [C]},
                },
                {"e0": {"subject": "n1", "object": "n0"}},
            )
        ),
    ),
    (
        "trapi_optional_and_exclude_mixed",
        trapi(
            qg(
                {
                    "n0": {"ids": ["CHEBI:3"]},
                    "n1": {"categories": [G]},
                    "n2": {"categories": [D], "option_group_id": "o1"},
                    "n3": {"categories": [P]},
                },
                {
                    "e0": {"subject": "n0", "object": "n1"},
                    "e1": {"subject": "n1", "object": "n2", "option_group_id": "o1"},
                    "e2": {"subject": "n0", "object": "n3", "exclude": True},
                },
            )
        ),
    ),
    # --- xDTD / MVP1 ---
    (
        "xdtd_infer_params",
        ops(
            [
                "add_qnode(key=drug, categories=biolink:Drug)",
                "add_qnode(key=disease, ids=MONDO:2)",
                "add_qedge(key=t, subject=drug, object=disease, predicates=biolink:treats)",
                "infer(action=drug_treatment_graph_expansion, disease_curie=MONDO:2, qedge_id=t, n_drugs=2, n_paths=1)",
                "filter_results(action=limit_number_of_results, max_results=10)",
            ]
        ),
    ),
    (
        "xdtd_infer_drug_curie",
        ops(
            [
                "add_qnode(key=drug, ids=CHEBI:1)",
                "add_qnode(key=disease, categories=biolink:Disease)",
                "add_qedge(key=t, subject=drug, object=disease, predicates=biolink:treats)",
                "infer(action=drug_treatment_graph_expansion, drug_curie=CHEBI:1, qedge_id=t)",
            ]
        ),
    ),
    (
        "xdtd_infer_bad_param",
        ops(
            [
                "infer(action=drug_treatment_graph_expansion, disease_curie=MONDO:2, n_drugs=zero)"
            ]
        ),
    ),
    (
        "mvp1_two_diseases",
        trapi(
            qg(
                {"n0": {"ids": ["MONDO:2", "MONDO:3"]}, "n1": {"categories": [C]}},
                {
                    "e0": {
                        "subject": "n1",
                        "object": "n0",
                        "predicates": [T],
                        "knowledge_type": "inferred",
                    }
                },
            )
        ),
    ),
    (
        "mvp1_with_workflow",
        trapi(
            qg(
                {"n0": {"ids": ["MONDO:3"]}, "n1": {"categories": [C]}},
                {
                    "e0": {
                        "subject": "n1",
                        "object": "n0",
                        "predicates": [T],
                        "knowledge_type": "inferred",
                    }
                },
            ),
            workflow=[{"id": "lookup"}, {"id": "score"}],
        ),
    ),
    # --- xCRG / MVP2 ---
    (
        "mvp2_xcrg_route",
        trapi(
            qg(
                {
                    "chem": {"categories": [C]},
                    "gene": {"ids": ["NCBIGene:3"], "categories": [G]},
                },
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
            )
        ),
    ),
]

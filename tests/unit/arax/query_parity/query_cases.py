"""End-to-end ARAXQuery cases: (name, query dict), run through ARAXQuery.query().

They cover the dispatch paths (QG interpreter templates, ARAXi operations, TRAPI
workflows), the actions each path runs (expand, overlays, filter_kg,
resultify + automatic ranking, filter_results, infer, the ResultTransformer)
and ARAX's input validation and error handling.
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


CASES = [
    # --- QG interpreter templates (QGI-*) ---
    (
        "tpl_one_hop_classic",
        trapi(
            qg(
                {"n0": {"ids": ["MONDO:1"]}, "n1": {"categories": [C]}},
                {"e0": {"subject": "n1", "object": "n0", "predicates": [T]}},
            )
        ),
    ),
    (
        "tpl_one_hop_gene",
        trapi(
            qg(
                {"n0": {"ids": ["CHEBI:3"]}, "n1": {"categories": [G]}},
                {"e0": {"subject": "n0", "object": "n1"}},
            )
        ),
    ),
    (
        "tpl_one_hop_two_curie",
        trapi(
            qg(
                {"n0": {"ids": ["CHEBI:2"]}, "n1": {"ids": ["MONDO:2", "MONDO:3"]}},
                {"e0": {"subject": "n0", "object": "n1"}},
            )
        ),
    ),
    (
        "tpl_one_hop_all_connections",
        trapi(
            qg(
                {"n0": {"ids": ["NCBIGene:4"]}, "n1": {}},
                {"e0": {"subject": "n0", "object": "n1"}},
            )
        ),
    ),
    (
        "tpl_two_hop_classic",
        trapi(
            qg(
                {
                    "n0": {"ids": ["CHEBI:1"]},
                    "n1": {"categories": [G]},
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
        "tpl_two_hop_curie_cat_curie",
        trapi(
            qg(
                {
                    "n0": {"ids": ["CHEBI:4"]},
                    "n1": {"categories": [G]},
                    "n2": {"ids": ["MONDO:5"]},
                },
                {
                    "e0": {"subject": "n0", "object": "n1"},
                    "e1": {"subject": "n1", "object": "n2"},
                },
            )
        ),
    ),
    (
        "tpl_three_hop_fet",
        trapi(
            qg(
                {
                    "n00": {"ids": ["CHEBI:6"]},
                    "n01": {"categories": [G]},
                    "n02": {"categories": [D]},
                    "n03": {"categories": [P]},
                },
                {
                    "e00": {"subject": "n00", "object": "n01"},
                    "e01": {"subject": "n01", "object": "n02"},
                    "e02": {"subject": "n02", "object": "n03"},
                },
            )
        ),
    ),
    (
        "tpl_one_node",
        trapi(qg({"n0": {"ids": ["CHEBI:2", "CHEBI:9"]}}, {})),
    ),
    (
        "tpl_no_match_three_pinned",
        trapi(
            qg(
                {
                    "n0": {"ids": ["CHEBI:1"]},
                    "n1": {"ids": ["NCBIGene:2"]},
                    "n2": {"ids": ["MONDO:2"]},
                },
                {
                    "e0": {"subject": "n0", "object": "n1"},
                    "e1": {"subject": "n1", "object": "n2"},
                },
            )
        ),
    ),
    (
        "tpl_empty_answer",
        trapi(
            qg(
                {"n0": {"ids": ["HP:13"]}, "n1": {"categories": [G]}},
                {"e0": {"subject": "n0", "object": "n1", "predicates": [T]}},
            )
        ),
    ),
    # --- creative treats through Expand + Infer (CRT-*, INF-*) ---
    (
        "creative_treats",
        trapi(
            qg(
                {"drug": {"categories": [C]}, "disease": {"ids": ["MONDO:1"]}},
                {
                    "t": {
                        "subject": "drug",
                        "object": "disease",
                        "predicates": [T],
                        "knowledge_type": "inferred",
                    }
                },
            )
        ),
    ),
    (
        "creative_treats_no_prediction",
        trapi(
            qg(
                {"drug": {"categories": [C]}, "disease": {"ids": ["MONDO:9"]}},
                {
                    "t": {
                        "subject": "drug",
                        "object": "disease",
                        "predicates": [T],
                        "knowledge_type": "inferred",
                    }
                },
            )
        ),
    ),
    # --- ARAXi operations ---
    (
        "dsl_expand_overlays_filters",
        ops(
            [
                "add_qnode(key=n0, ids=CHEBI:3)",
                "add_qnode(key=n1, categories=biolink:Gene)",
                "add_qnode(key=n2, categories=biolink:Disease)",
                "add_qedge(key=e0, subject=n0, object=n1)",
                "add_qedge(key=e1, subject=n1, object=n2)",
                "expand()",
                "overlay(action=compute_ngd, virtual_relation_label=N1, subject_qnode_key=n0, object_qnode_key=n2)",
                "overlay(action=compute_jaccard, start_node_key=n0, intermediate_node_key=n1, end_node_key=n2, virtual_relation_label=J1)",
                "overlay(action=fisher_exact_test, subject_qnode_key=n1, object_qnode_key=n2, virtual_relation_label=F1)",
                "filter_kg(action=remove_edges_by_continuous_attribute, edge_attribute=jaccard_index, direction=below, threshold=0.2, remove_connected_nodes=t, qnode_keys=[n2])",
                "resultify()",
                "filter_results(action=sort_by_edge_attribute, edge_attribute=jaccard_index, direction=descending, max_results=10)",
                "return(message=true, store=false)",
            ]
        ),
    ),
    (
        "dsl_clinical_info",
        ops(
            [
                "add_qnode(key=n0, ids=[MONDO:2,MONDO:3])",
                "add_qnode(key=n1, categories=biolink:PhenotypicFeature)",
                "add_qedge(key=e0, subject=n0, object=n1)",
                "expand()",
                "overlay(action=overlay_clinical_info, paired_concept_frequency=true)",
                "overlay(action=overlay_clinical_info, observed_expected_ratio=true, virtual_relation_label=C2, subject_qnode_key=n0, object_qnode_key=n1)",
                "overlay(action=overlay_clinical_info, chi_square=true, virtual_relation_label=C3, subject_qnode_key=n0, object_qnode_key=n1)",
                "resultify()",
                "filter_results(action=limit_number_of_results, max_results=5)",
            ]
        ),
    ),
    (
        "dsl_ngd_default_all_edges",
        ops(
            [
                "add_qnode(key=n0, ids=MONDO:4)",
                "add_qnode(key=n1, categories=biolink:ChemicalEntity)",
                "add_qedge(key=e0, subject=n1, object=n0)",
                "expand()",
                "overlay(action=compute_ngd)",
                "filter_kg(action=remove_edges_by_std_dev, edge_attribute=normalized_google_distance, direction=above, threshold=0.5)",
                "resultify()",
            ]
        ),
    ),
    (
        "dsl_scoreless_resultify",
        ops(
            [
                "add_qnode(key=n0, ids=NCBIGene:3)",
                "add_qnode(key=n1, categories=biolink:Gene)",
                "add_qedge(key=e0, subject=n0, object=n1)",
                "expand()",
                "scoreless_resultify()",
                "rank_results()",
            ]
        ),
    ),
    (
        "dsl_infer_no_qg",
        ops(
            [
                "infer(action=drug_treatment_graph_expansion, disease_curie=MONDO:2, n_drugs=4, n_paths=3)",
            ]
        ),
    ),
    (
        "dsl_infer_with_qg",
        ops(
            [
                "add_qnode(key=drug, categories=biolink:Drug)",
                "add_qnode(key=disease, ids=MONDO:3)",
                "add_qedge(key=t, subject=drug, object=disease, predicates=biolink:treats)",
                "infer(action=drug_treatment_graph_expansion, disease_curie=MONDO:3, qedge_id=t, n_drugs=99)",
            ]
        ),
    ),
    (
        "dsl_filter_kg_remove_nodes",
        ops(
            [
                "add_qnode(key=n0, ids=CHEBI:8)",
                "add_qnode(key=n1)",
                "add_qedge(key=e0, subject=n0, object=n1)",
                "expand()",
                "filter_kg(action=remove_nodes_by_category, node_category=biolink:Gene)",
                "filter_kg(action=remove_general_concept_nodes, perform_action=true)",
                "resultify(ignore_edge_direction=true)",
                "filter_results(action=sort_by_score, direction=ascending)",
            ]
        ),
    ),
    (
        "dsl_ops_with_message",
        ops(
            ["expand(edge_key=e0)", "resultify()", "return(message=true, store=true)"],
            message={
                "query_graph": qg(
                    {"n0": {"ids": ["MONDO:1"]}, "n1": {"categories": [SM]}},
                    {"e0": {"subject": "n1", "object": "n0", "predicates": [T]}},
                )
            },
        ),
    ),
    (
        "dsl_unknown_command",
        ops(["add_qnode(key=n0, ids=CHEBI:1)", "frobnicate(x=1)"]),
    ),
    (
        "dsl_parse_error",
        ops(["add_qnode(key=n0 ids=CHEBI:1"]),
    ),
    (
        "dsl_removed_filter_command",
        ops(["add_qnode(key=n0, ids=CHEBI:1)", "filter(maximum_results=5)"]),
    ),
    (
        "dsl_action_error_stops_plan",
        ops(
            [
                "add_qnode(key=n0, ids=CHEBI:1)",
                "add_qnode(key=n1, categories=biolink:Gene)",
                "add_qedge(key=e0, subject=n0, object=n1)",
                "expand()",
                "overlay(action=not_an_action)",
                "resultify()",
            ]
        ),
    ),
    # --- TRAPI workflows (WF-*) ---
    (
        "workflow_lookup_score",
        trapi(
            qg(
                {"n0": {"ids": ["MONDO:3"]}, "n1": {"categories": [C]}},
                {"e0": {"subject": "n1", "object": "n0"}},
            ),
            workflow=[
                {"id": "lookup"},
                {
                    "id": "overlay_compute_ngd",
                    "parameters": {
                        "virtual_relation_label": "N1",
                        "qnode_keys": ["n0", "n1"],
                    },
                },
                {"id": "score"},
                {"id": "filter_results_top_n", "parameters": {"max_results": 3}},
            ],
        ),
    ),
    (
        "workflow_fill_bind",
        trapi(
            qg(
                {"n0": {"ids": ["CHEBI:5"]}, "n1": {"categories": [D]}},
                {"e0": {"subject": "n0", "object": "n1"}},
            ),
            workflow=[
                {"id": "fill"},
                {"id": "bind"},
                {"id": "complete_results"},
                {
                    "id": "sort_results_score",
                    "parameters": {"ascending_or_descending": "ascending"},
                },
            ],
        ),
    ),
    (
        "workflow_unknown_op",
        trapi(
            qg(
                {"n0": {"ids": ["CHEBI:5"]}, "n1": {"categories": [D]}},
                {"e0": {"subject": "n0", "object": "n1"}},
            ),
            workflow=[{"id": "teleport"}],
        ),
    ),
    # --- validation, options and error handling (ORC-*) ---
    (
        "val_unknown_qnode_property",
        trapi(
            qg(
                {"n0": {"ids": ["CHEBI:1"], "colour": "red"}, "n1": {}},
                {"e0": {"subject": "n0", "object": "n1"}},
            )
        ),
    ),
    (
        "val_singular_predicate",
        trapi(
            qg(
                {"n0": {"ids": ["CHEBI:1"]}, "n1": {}},
                {"e0": {"subject": "n0", "object": "n1", "predicate": T}},
            )
        ),
    ),
    (
        "val_no_edges_or_paths",
        trapi({"nodes": {"n0": {"ids": ["CHEBI:1"]}}}),
    ),
    (
        "val_no_message",
        {"submitter": "tester"},
    ),
    (
        "val_no_pinned_node",
        trapi(
            qg(
                {"n0": {"categories": [C]}, "n1": {"categories": [D]}},
                {"e0": {"subject": "n0", "object": "n1"}},
            )
        ),
    ),
    (
        "opt_query_options_and_submitter",
        trapi(
            qg(
                {"n0": {"ids": ["MONDO:2"]}, "n1": {"categories": [C]}},
                {"e0": {"subject": "n1", "object": "n0", "predicates": [T]}},
            ),
            query_options={
                "kp_timeout": "30",
                "prune_threshold": 5,
                "bypass_cache": True,
            },
            return_minimal_metadata=True,
            submitter="infores:tester",
        ),
    ),
    (
        "opt_callback_submitter",
        trapi(
            qg(
                {"n0": {"ids": ["MONDO:2"]}, "n1": {"categories": [C]}},
                {"e0": {"subject": "n1", "object": "n0", "predicates": [T]}},
            ),
            callback="https://ars.example.org/ars/api/messages/abc",
        ),
    ),
    (
        "opt_bad_kp_timeout",
        trapi(
            qg(
                {"n0": {"ids": ["MONDO:2"]}, "n1": {"categories": [C]}},
                {"e0": {"subject": "n1", "object": "n0"}},
            ),
            query_options={"kp_timeout": "soon"},
        ),
    ),
    (
        "kp_error",
        trapi(
            qg(
                {"n0": {"ids": ["MONDO:500"]}, "n1": {"categories": [C]}},
                {"e0": {"subject": "n1", "object": "n0"}},
            )
        ),
    ),
    (
        "optional_group_query",
        trapi(
            qg(
                {
                    "n0": {"ids": ["CHEBI:3"]},
                    "n1": {"categories": [D]},
                    "n2": {"categories": [P], "option_group_id": "o1"},
                },
                {
                    "e0": {"subject": "n0", "object": "n1"},
                    "e1": {"subject": "n1", "object": "n2", "option_group_id": "o1"},
                },
            )
        ),
    ),
]

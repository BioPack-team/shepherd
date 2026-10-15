"""Expand parity cases: (name, query_graph, expand parameters, query_options)."""


def qn(ids=None, categories=None, **kw):
    d = {}
    if ids is not None:
        d["ids"] = ids
    if categories is not None:
        d["categories"] = categories
    d.update(kw)
    return d


def qe(s, o, predicates=None, **kw):
    d = {"subject": s, "object": o}
    if predicates is not None:
        d["predicates"] = predicates
    d.update(kw)
    return d


C = "biolink:ChemicalEntity"
D = "biolink:Disease"
G = "biolink:Gene"
P = "biolink:PhenotypicFeature"
T = "biolink:treats"

CASES = [
    (
        "one_hop_treats",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(ids=["MONDO:1"])},
            "edges": {"e0": qe("n0", "n1", [T])},
        },
        {},
        {},
    ),
    (
        "one_hop_no_pred",
        {
            "nodes": {"n0": qn(ids=["CHEBI:2"]), "n1": qn(categories=[D])},
            "edges": {"e0": qe("n0", "n1")},
        },
        {},
        {},
    ),
    (
        "one_hop_no_cats",
        {
            "nodes": {"n0": qn(ids=["CHEBI:4"]), "n1": qn()},
            "edges": {"e0": qe("n0", "n1")},
        },
        {},
        {},
    ),
    (
        "one_hop_gene_protein_conflation",
        {
            "nodes": {
                "n0": qn(ids=["CHEBI:3", "CHEBI:6"]),
                "n1": qn(categories=["biolink:Protein"]),
            },
            "edges": {"e0": qe("n0", "n1", ["biolink:interacts_with"])},
        },
        {},
        {},
    ),
    (
        "flip_noncanonical_pred",
        {
            "nodes": {"n0": qn(ids=["MONDO:2"]), "n1": qn(categories=[C])},
            "edges": {"e0": qe("n0", "n1", ["biolink:treated_by"])},
        },
        {},
        {},
    ),
    (
        "flip_treats_disease_subject",
        {
            "nodes": {"n0": qn(categories=[D]), "n1": qn(ids=["CHEBI:1"])},
            "edges": {"e0": qe("n0", "n1", [T])},
        },
        {},
        {},
    ),
    (
        "mixed_canonical_error",
        {
            "nodes": {"n0": qn(ids=["MONDO:2"]), "n1": qn(categories=[C])},
            "edges": {"e0": qe("n0", "n1", ["biolink:treated_by", T])},
        },
        {},
        {},
    ),
    (
        "subclass_query_ids",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(ids=["MONDO:1"])},
            "edges": {"e0": qe("n0", "n1", [T])},
        },
        {},
        {},
    ),
    (
        "subclass_chem_parent",
        {
            "nodes": {"n0": qn(ids=["CHEBI:2"]), "n1": qn(categories=[D])},
            "edges": {"e0": qe("n0", "n1", [T])},
        },
        {},
        {},
    ),
    (
        "two_hop",
        {
            "nodes": {
                "n0": qn(ids=["CHEBI:1"]),
                "n1": qn(categories=[G]),
                "n2": qn(categories=[D]),
            },
            "edges": {"e0": qe("n0", "n1"), "e1": qe("n1", "n2")},
        },
        {},
        {},
    ),
    (
        "two_hop_pinned_both",
        {
            "nodes": {
                "n0": qn(ids=["CHEBI:2"]),
                "n1": qn(categories=[G]),
                "n2": qn(ids=["MONDO:3", "MONDO:5"]),
            },
            "edges": {"e0": qe("n0", "n1"), "e1": qe("n1", "n2")},
        },
        {},
        {},
    ),
    (
        "three_hop",
        {
            "nodes": {
                "n0": qn(ids=["CHEBI:7"]),
                "n1": qn(categories=[G]),
                "n2": qn(categories=[D]),
                "n3": qn(categories=[P]),
            },
            "edges": {"e0": qe("n0", "n1"), "e1": qe("n1", "n2"), "e2": qe("n2", "n3")},
        },
        {},
        {},
    ),
    (
        "reverse_order_edges",
        {
            "nodes": {
                "n0": qn(categories=[P]),
                "n1": qn(categories=[D]),
                "n2": qn(ids=["NCBIGene:4"]),
            },
            "edges": {"e0": qe("n1", "n0"), "e1": qe("n2", "n1")},
        },
        {},
        {},
    ),
    (
        "kryptonite",
        {
            "nodes": {
                "n0": qn(ids=["CHEBI:1"]),
                "n1": qn(categories=[D]),
                "n2": qn(categories=[P]),
            },
            "edges": {"e0": qe("n0", "n1"), "e1": qe("n1", "n2", exclude=True)},
        },
        {},
        {},
    ),
    (
        "optional_group",
        {
            "nodes": {
                "n0": qn(ids=["CHEBI:3"]),
                "n1": qn(categories=[D]),
                "n2": qn(categories=[P], option_group_id="g1"),
            },
            "edges": {"e0": qe("n0", "n1"), "e1": qe("n1", "n2", option_group_id="g1")},
        },
        {},
        {},
    ),
    (
        "fda_constraint",
        {
            "nodes": {
                "n0": qn(
                    categories=[C],
                    constraints=[
                        {
                            "id": "biolink:highest_FDA_approval_status",
                            "name": "FDA approval",
                            "operator": "==",
                            "value": "regular approval",
                        }
                    ],
                ),
                "n1": qn(ids=["MONDO:1", "MONDO:2", "MONDO:3"]),
            },
            "edges": {"e0": qe("n0", "n1")},
        },
        {},
        {},
    ),
    (
        "fda_constraint_not",
        {
            "nodes": {
                "n0": qn(
                    categories=[C],
                    constraints=[
                        {
                            "id": "biolink:highest_FDA_approval_status",
                            "name": "FDA approval",
                            "operator": "==",
                            "value": "regular approval",
                            "not": True,
                        }
                    ],
                ),
                "n1": qn(ids=["MONDO:1", "MONDO:2", "MONDO:3"]),
            },
            "edges": {"e0": qe("n0", "n1")},
        },
        {},
        {},
    ),
    (
        "unsupported_constraint",
        {
            "nodes": {
                "n0": qn(
                    categories=[C],
                    constraints=[
                        {
                            "id": "biolink:foo",
                            "name": "foo",
                            "operator": "==",
                            "value": "x",
                        }
                    ],
                ),
                "n1": qn(ids=["MONDO:1"]),
            },
            "edges": {"e0": qe("n0", "n1")},
        },
        {},
        {},
    ),
    (
        "ks_denylist",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(ids=["MONDO:1", "MONDO:4"])},
            "edges": {
                "e0": qe(
                    "n0",
                    "n1",
                    attribute_constraints=[
                        {
                            "id": "knowledge_source",
                            "name": "knowledge source",
                            "operator": "==",
                            "value": ["infores:ctd"],
                            "not": True,
                        }
                    ],
                )
            },
        },
        {},
        {},
    ),
    (
        "ks_allowlist_retriever",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(ids=["MONDO:1", "MONDO:4"])},
            "edges": {
                "e0": qe(
                    "n0",
                    "n1",
                    attribute_constraints=[
                        {
                            "id": "knowledge_source",
                            "name": "knowledge source",
                            "operator": "==",
                            "value": ["infores:retriever"],
                        }
                    ],
                )
            },
        },
        {},
        {},
    ),
    (
        "self_qedge_error",
        {
            "nodes": {"n0": qn(ids=["CHEBI:1"])},
            "edges": {"e0": qe("n0", "n0", ["biolink:related_to"])},
        },
        {},
        {},
    ),
    (
        "disconnected_error",
        {
            "nodes": {
                "n0": qn(ids=["CHEBI:1"]),
                "n1": qn(categories=[D]),
                "n2": qn(ids=["HP:1"]),
                "n3": qn(categories=[G]),
            },
            "edges": {"e0": qe("n0", "n1"), "e1": qe("n2", "n3")},
        },
        {},
        {},
    ),
    (
        "no_ids_error",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(categories=[D])},
            "edges": {"e0": qe("n0", "n1")},
        },
        {},
        {},
    ),
    (
        "http_500",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(ids=["MONDO:500"])},
            "edges": {"e0": qe("n0", "n1")},
        },
        {},
        {},
    ),
    (
        "timeout",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(ids=["MONDO:504"])},
            "edges": {"e0": qe("n0", "n1")},
        },
        {"kp_timeout": 1},
        {},
    ),
    (
        "kp_timeout_option",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(ids=["MONDO:504"])},
            "edges": {"e0": qe("n0", "n1")},
        },
        {},
        {"kp_timeout": 1},
    ),
    (
        "edge_key_subset",
        {
            "nodes": {
                "n0": qn(ids=["CHEBI:1"]),
                "n1": qn(categories=[G]),
                "n2": qn(categories=[D]),
            },
            "edges": {"e0": qe("n0", "n1"), "e1": qe("n1", "n2")},
        },
        {"edge_key": "e0"},
        {},
    ),
    (
        "invalid_param",
        {
            "nodes": {"n0": qn(ids=["CHEBI:1"]), "n1": qn(categories=[D])},
            "edges": {"e0": qe("n0", "n1")},
        },
        {"bogus": "1"},
        {},
    ),
    (
        "prune_threshold",
        {
            "nodes": {
                "n0": qn(ids=["MONDO:1", "MONDO:2", "MONDO:3", "MONDO:4", "MONDO:5"]),
                "n1": qn(categories=[C]),
                "n2": qn(categories=[G]),
            },
            "edges": {"e0": qe("n1", "n0"), "e1": qe("n1", "n2")},
        },
        {"prune_threshold": 2},
        {},
    ),
    (
        "prune_threshold_option",
        {
            "nodes": {
                "n0": qn(ids=["NCBIGene:1", "NCBIGene:2", "NCBIGene:3"]),
                "n1": qn(categories=[D]),
                "n2": qn(categories=[P]),
            },
            "edges": {"e0": qe("n0", "n1"), "e1": qe("n1", "n2")},
        },
        {},
        {"prune_threshold": 3},
    ),
    (
        "single_node",
        {"nodes": {"n0": qn(ids=["CHEBI:1", "CHEBI:2"])}, "edges": {}},
        {},
        {},
    ),
    (
        "symmetric_gene_gene",
        {
            "nodes": {"n0": qn(ids=["NCBIGene:5"]), "n1": qn(categories=[G])},
            "edges": {"e0": qe("n0", "n1", ["biolink:interacts_with"])},
        },
        {},
        {},
    ),
    (
        "affects_qualified",
        {
            "nodes": {
                "n0": qn(ids=["CHEBI:1", "CHEBI:2", "CHEBI:3"]),
                "n1": qn(categories=[G]),
            },
            "edges": {"e0": qe("n0", "n1", ["biolink:affects"])},
        },
        {},
        {},
    ),
    (
        "treats_or_applied",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(ids=["MONDO:3", "MONDO:6"])},
            "edges": {
                "e0": qe("n0", "n1", ["biolink:treats_or_applied_or_studied_to_treat"])
            },
        },
        {},
        {},
    ),
]


# Port-only cases: behavior that intentionally differs from upstream ARAX, so
# there is no upstream golden; test_expand_parity.py asserts on them directly.
PORT_ONLY_CASES = [
    # DEC-10: a kp list is forwarded to Retriever as parameters.kp, exactly as given
    (
        "kp_forwarded",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(ids=["MONDO:1"])},
            "edges": {"e0": qe("n0", "n1", [T])},
        },
        {"kp": ["infores:ctd", "infores:drugbank", "infores:ctd"]},
        {},
    ),
    (
        "kp_forwarded_single",
        {
            "nodes": {"n0": qn(categories=[C]), "n1": qn(ids=["MONDO:1"])},
            "edges": {"e0": qe("n0", "n1", [T])},
        },
        {"kp": "infores:drugbank"},
        {},
    ),
    (
        "kp_forwarded_single_node",
        {"nodes": {"n0": qn(ids=["CHEBI:1"])}, "edges": {}},
        {"kp": ["infores:rtx-kg2"]},
        {},
    ),
]

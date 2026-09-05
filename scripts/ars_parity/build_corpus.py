"""Build the ARS parity corpus fixtures.

Deterministically writes the TRAPI-shaped inputs that both the upstream ARS
functions (via generate_goldens.py, run in the pinned Relay checkout's venv)
and the Shepherd ports (via tests/unit/ars/test_golden_parity.py) are run
against. Edit here, re-run, then regenerate goldens.

Usage: python scripts/ars_parity/build_corpus.py
"""

import json
import pathlib

OUT = pathlib.Path(__file__).resolve().parents[2] / "tests/fixtures/ars_corpus"


def edge(subject, obj, predicate="biolink:affects", sources=None, attributes=None, **extra):
    """A TRAPI-1.5-valid KG edge: reasoner-pydantic 5.1.1 requires both
    ``attributes`` and ``sources`` on every knowledge-graph edge, so the
    defaults keep corpus responses upstream-valid."""
    e = {"subject": subject, "object": obj, "predicate": predicate}
    e["sources"] = sources if sources is not None else [primary("infores:test-kp")]
    e["attributes"] = attributes if attributes is not None else []
    e.update(extra)
    return e


def primary(resource_id):
    return {
        "resource_id": resource_id,
        "resource_role": "primary_knowledge_source",
        "upstream_resource_ids": [],
    }


def aggregator(resource_id, upstream=None):
    return {
        "resource_id": resource_id,
        "resource_role": "aggregator_knowledge_source",
        "upstream_resource_ids": upstream or [],
    }


QUERY_GRAPH = {
    "nodes": {
        "sn": {"ids": ["MONDO:0005148"], "categories": ["biolink:Disease"]},
        "on": {"categories": ["biolink:ChemicalEntity"]},
    },
    "edges": {
        "t_edge": {
            "subject": "on",
            "object": "sn",
            "predicates": ["biolink:treats"],
            "knowledge_type": "inferred",
        }
    },
}


def response_aragorn():
    """A well-formed creative-ish ARA response with aux graphs and scores."""
    return {
        "message": {
            "query_graph": QUERY_GRAPH,
            "knowledge_graph": {
                "nodes": {
                    "MONDO:0005148": {
                        "name": "type 2 diabetes",
                        "categories": ["biolink:Disease"],
                        "attributes": [],
                    },
                    "CHEBI:6801": {
                        "name": "metformin",
                        "categories": ["biolink:ChemicalEntity"],
                        "attributes": [
                            {
                                "attribute_type_id": "biolink:xref",
                                "value": ["DRUGBANK:DB00331"],
                            }
                        ],
                    },
                    "NCBIGene:5468": {
                        "name": "PPARG",
                        "categories": ["biolink:Gene"],
                        "attributes": [],
                    },
                },
                "edges": {
                    "e1": edge(
                        "CHEBI:6801",
                        "MONDO:0005148",
                        "biolink:treats",
                        sources=[primary("infores:ctd"), aggregator("infores:aragorn", ["infores:ctd"])],
                        attributes=[
                            {
                                "attribute_type_id": "biolink:publications",
                                "value": ["PMID:1", "PMID:2"],
                            }
                        ],
                    ),
                    "e2": edge(
                        "CHEBI:6801",
                        "NCBIGene:5468",
                        "biolink:affects",
                        sources=[primary("infores:aragorn")],
                        attributes=[
                            {
                                "attribute_type_id": "biolink:support_graphs",
                                "value": ["aux1"],
                            }
                        ],
                    ),
                    "e3": edge(
                        "NCBIGene:5468",
                        "MONDO:0005148",
                        "biolink:gene_associated_with_condition",
                        sources=[primary("infores:ctd")],
                    ),
                },
            },
            "auxiliary_graphs": {
                "aux1": {"edges": ["e1", "e3"], "attributes": []},
            },
            "results": [
                {
                    "node_bindings": {
                        "sn": [{"id": "MONDO:0005148", "attributes": []}],
                        "on": [{"id": "CHEBI:6801", "attributes": []}],
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:aragorn",
                            "edge_bindings": {
                                "t_edge": [{"id": "e1", "attributes": []}]
                            },
                            "score": 0.83,
                        }
                    ],
                },
                {
                    "node_bindings": {
                        "sn": [{"id": "MONDO:0005148", "attributes": []}],
                        "on": [{"id": "NCBIGene:5468", "attributes": []}],
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:aragorn",
                            "edge_bindings": {
                                "t_edge": [{"id": "e3", "attributes": []}]
                            },
                            "score": 0.41,
                        }
                    ],
                },
            ],
        },
        "logs": [{"message": "aragorn did a lookup", "level": "INFO", "timestamp": "2026-09-01T00:00:00Z"}],
    }


def response_arax():
    """Overlaps aragorn: same first result bindings (merge collision), the
    shared e1 edge with a unionable publications attribute and a qualifier
    list, one new node/edge/result, an analysis with a null score."""
    return {
        "message": {
            "query_graph": QUERY_GRAPH,
            "knowledge_graph": {
                "nodes": {
                    "MONDO:0005148": {
                        "name": "type 2 diabetes mellitus",
                        "categories": ["biolink:Disease"],
                        "attributes": [],
                    },
                    "CHEBI:6801": {
                        "name": "metformin",
                        "categories": ["biolink:ChemicalEntity", "biolink:Drug"],
                        "attributes": [
                            {
                                "attribute_type_id": "biolink:xref",
                                "value": ["MESH:D008687"],
                            }
                        ],
                    },
                    "CHEBI:17234": {
                        "name": "glucose",
                        "categories": ["biolink:ChemicalEntity"],
                        "attributes": [],
                    },
                },
                "edges": {
                    "e1": edge(
                        "CHEBI:6801",
                        "MONDO:0005148",
                        "biolink:treats",
                        sources=[primary("infores:ctd"), aggregator("infores:arax", ["infores:ctd"])],
                        attributes=[
                            {
                                "attribute_type_id": "biolink:publications",
                                "value": ["PMID:2", "PMID:3"],
                            }
                        ],
                        qualifiers=[
                            {
                                "qualifier_type_id": "biolink:object_direction_qualifier",
                                "qualifier_value": "decreased",
                            }
                        ],
                    ),
                    "e9": edge(
                        "CHEBI:6801",
                        "CHEBI:17234",
                        "biolink:affects",
                        sources=[primary("infores:arax")],
                    ),
                },
            },
            "auxiliary_graphs": {
                "aux9": {"edges": ["e9"], "attributes": []},
            },
            "results": [
                {
                    "node_bindings": {
                        "sn": [{"id": "MONDO:0005148", "attributes": []}],
                        "on": [{"id": "CHEBI:6801", "attributes": []}],
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:arax",
                            "edge_bindings": {
                                "t_edge": [{"id": "e1", "attributes": []}]
                            },
                            "score": 0.91,
                        }
                    ],
                },
                {
                    "node_bindings": {
                        "sn": [{"id": "MONDO:0005148", "attributes": []}],
                        "on": [{"id": "CHEBI:17234", "attributes": []}],
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:arax",
                            "edge_bindings": {
                                "t_edge": [{"id": "e9", "attributes": []}]
                            },
                            "score": None,
                        },
                        {
                            "resource_id": "infores:arax-second",
                            "edge_bindings": {
                                "t_edge": [{"id": "e9", "attributes": []}]
                            },
                            "score": 0.2,
                        },
                    ],
                },
            ],
        },
        "logs": [],
    }


def response_blocklist():
    """Exercises every removal path in remove_blocked. Blocked CURIEs are
    real keys from config/blocklist.json (LOINC:LP231631-5, PSY:25450)."""
    return {
        "message": {
            "query_graph": QUERY_GRAPH,
            "knowledge_graph": {
                "nodes": {
                    "LOINC:LP231631-5": {"name": "blocked one", "categories": ["biolink:NamedThing"], "attributes": []},
                    "PSY:25450": {"name": "blocked two", "categories": ["biolink:NamedThing"], "attributes": []},
                    "CHEBI:6801": {"name": "metformin", "categories": ["biolink:ChemicalEntity"], "attributes": []},
                    "MONDO:0005148": {"name": "t2d", "categories": ["biolink:Disease"], "attributes": []},
                    "NCBIGene:5468": {"name": "PPARG", "categories": ["biolink:Gene"], "attributes": []},
                },
                "edges": {
                    "bad_subj": edge("LOINC:LP231631-5", "MONDO:0005148"),
                    "bad_obj": edge("CHEBI:6801", "PSY:25450"),
                    "clean1": edge("CHEBI:6801", "MONDO:0005148", "biolink:treats"),
                    "clean2": edge("NCBIGene:5468", "MONDO:0005148"),
                    # only support graph (aux_all_bad) removed -> edge removed too
                    "support_only": edge(
                        "CHEBI:6801",
                        "MONDO:0005148",
                        attributes=[
                            {"attribute_type_id": "biolink:support_graphs", "value": ["aux_all_bad"]}
                        ],
                    ),
                    # one of two support graphs removed -> edge survives
                    "support_partial": edge(
                        "CHEBI:6801",
                        "NCBIGene:5468",
                        attributes=[
                            {"attribute_type_id": "biolink:support_graphs", "value": ["aux_all_bad", "aux_partial"]}
                        ],
                    ),
                },
            },
            "auxiliary_graphs": {
                "aux_all_bad": {"edges": ["bad_subj", "bad_obj"], "attributes": []},
                "aux_partial": {"edges": ["bad_obj", "clean1"], "attributes": []},
                "aux_clean": {"edges": ["clean2"], "attributes": []},
            },
            "results": [
                {
                    # bound to a blocked node -> result removed
                    "node_bindings": {
                        "sn": [{"id": "MONDO:0005148", "attributes": []}],
                        "on": [{"id": "LOINC:LP231631-5", "attributes": []}],
                    },
                    "analyses": [
                        {"resource_id": "infores:x", "edge_bindings": {"t_edge": [{"id": "bad_subj", "attributes": []}]}, "score": 0.5}
                    ],
                },
                {
                    # analysis with two bindings, one to a removed edge -> binding pruned
                    "node_bindings": {
                        "sn": [{"id": "MONDO:0005148", "attributes": []}],
                        "on": [{"id": "CHEBI:6801", "attributes": []}],
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:x",
                            "edge_bindings": {
                                "t_edge": [
                                    {"id": "bad_obj", "attributes": []},
                                    {"id": "clean1", "attributes": []},
                                ]
                            },
                            "score": 0.7,
                        },
                        {
                            # single binding to a removed edge -> analysis removed
                            "resource_id": "infores:y",
                            "edge_bindings": {"t_edge": [{"id": "bad_obj", "attributes": []}]},
                            # support_graphs on the analysis referencing a removed EDGE id
                            # (upstream checks membership against edges_to_remove)
                            "support_graphs": ["bad_subj", "aux_clean"],
                            "score": 0.6,
                        },
                    ],
                },
                {
                    # every analysis removed -> result removed
                    "node_bindings": {
                        "sn": [{"id": "MONDO:0005148", "attributes": []}],
                        "on": [{"id": "NCBIGene:5468", "attributes": []}],
                    },
                    "analyses": [
                        {"resource_id": "infores:x", "edge_bindings": {"t_edge": [{"id": "bad_subj", "attributes": []}]}, "score": 0.3}
                    ],
                },
                {
                    # pathfinder-style: path binding referencing removed aux graph
                    "node_bindings": {
                        "sn": [{"id": "MONDO:0005148", "attributes": []}],
                        "on": [{"id": "CHEBI:6801", "attributes": []}],
                    },
                    "analyses": [
                        {
                            "resource_id": "infores:x",
                            "path_bindings": {
                                "p0": [
                                    {"id": "aux_all_bad"},
                                    {"id": "aux_clean"},
                                ]
                            },
                            "score": 0.9,
                        },
                        {
                            "resource_id": "infores:z",
                            "path_bindings": {"p0": [{"id": "aux_all_bad"}]},
                            "score": 0.1,
                        },
                    ],
                },
            ],
        }
    }


def scrub_input():
    return {
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "n1": {"attributes": [None, {"attribute_type_id": "x", "value": 1}, None]},
                    "n2": {"attributes": None},
                    "n3": {},
                },
                "edges": {
                    "e1": {
                        "subject": "n1",
                        "object": "n2",
                        "attributes": [
                            None,
                            {"attribute_type_id": "y", "value": 2, "attributes": None},
                        ],
                        "sources": [
                            {"resource_id": "infores:a", "resource_role": "primary_knowledge_source"},
                            {"resource_role": "aggregator_knowledge_source"},
                            {"resource_id": None, "resource_role": "aggregator_knowledge_source"},
                            {"resource_id": "infores:b", "resource_role": "aggregator_knowledge_source", "upstream_resource_ids": None},
                            {"resource_id": "infores:c", "resource_role": "aggregator_knowledge_source", "upstream_resource_ids": ["infores:a", None, "infores:b"]},
                        ],
                    },
                },
            },
            "auxiliary_graphs": {
                "a1": {"edges": ["e1"], "attributes": None},
                "a2": {"edges": ["e1"], "attributes": [{"attribute_type_id": "k"}]},
                "a3": {"edges": ["e1"]},
            },
        }
    }


def decorate_cases():
    return [
        {
            "name": "no_sources_key",
            "inforesid": "infores:aragorn",
            "data": {"message": {"knowledge_graph": {"edges": {"e1": {"subject": "a", "object": "b"}}}}},
        },
        {
            "name": "empty_sources",
            "inforesid": "infores:aragorn",
            "data": {"message": {"knowledge_graph": {"edges": {"e1": {"subject": "a", "object": "b", "sources": []}}}}},
        },
        {
            "name": "has_primary_not_self",
            "inforesid": "infores:aragorn",
            "data": {
                "message": {
                    "knowledge_graph": {
                        "edges": {
                            "e1": {
                                "subject": "a",
                                "object": "b",
                                "sources": [
                                    {"resource_id": "infores:ctd", "resource_role": "primary_knowledge_source"}
                                ],
                            }
                        }
                    }
                }
            },
        },
        {
            "name": "already_self",
            "inforesid": "infores:aragorn",
            "data": {
                "message": {
                    "knowledge_graph": {
                        "edges": {
                            "e1": {
                                "subject": "a",
                                "object": "b",
                                "sources": [
                                    {"resource_id": "infores:aragorn", "resource_role": "primary_knowledge_source"}
                                ],
                            }
                        }
                    }
                }
            },
        },
        {
            "name": "none_inforesid",
            "inforesid": None,
            "data": {"message": {"knowledge_graph": {"edges": {"e1": {"subject": "a", "object": "b", "sources": []}}}}},
        },
        {
            # sources present, none primary, not self: upstream hits an
            # uninitialized has_primary -> raises. Recorded as an error case.
            "name": "no_primary_raises",
            "inforesid": "infores:aragorn",
            "data": {
                "message": {
                    "knowledge_graph": {
                        "edges": {
                            "e1": {
                                "subject": "a",
                                "object": "b",
                                "sources": [
                                    {"resource_id": "infores:x", "resource_role": "aggregator_knowledge_source"}
                                ],
                            }
                        }
                    }
                }
            },
        },
    ]


def scores_cases():
    return [
        {
            "name": "mixed",
            "results": [
                {"analyses": [{"score": 0.9}]},
                {"analyses": [{"score": 0.1}, {"score": 0.3}]},
                {"analyses": [{"score": None}, {"score": 0.5}]},
                {"analyses": [{"score": 0.9}]},  # tie with first
                {"analyses": [{}]},  # single analysis without score
            ],
        },
        {
            "name": "missing_analyses_key",
            "results": [
                {"analyses": [{"score": 0.9}]},
                {"node_bindings": {}},
            ],
        },
        {"name": "empty", "results": []},
        {
            "name": "single",
            "results": [{"analyses": [{"score": 0.42}]}],
        },
        {
            "name": "empty_analyses_list",
            "results": [
                {"analyses": [{"score": 0.9}]},
                {"analyses": []},
            ],
        },
    ]


def ordering_cases():
    return [
        {
            "name": "standard",
            "results": [
                {"ordering_components": {"novelty": 0.2, "confidence": 0.9, "clinical_evidence": 0.3}},
                {"ordering_components": {"novelty": 0.8, "confidence": 0.1, "clinical_evidence": 0.0}},
                {"ordering_components": {"confidence": 0.5}},
                {},
                {"ordering_components": {"novelty": 0.2, "confidence": 0.9, "clinical_evidence": 0.3}},
            ],
        },
        {
            "name": "all_zero",
            "results": [{"ordering_components": {"novelty": 0, "confidence": 0, "clinical_evidence": 0}}, {}],
        },
        {
            "name": "extremes",
            "results": [
                {"ordering_components": {"novelty": 1.0, "confidence": 1.0, "clinical_evidence": 1.0}},
                {"ordering_components": {"novelty": 0.0, "confidence": 1.0, "clinical_evidence": 0.0}},
                {"ordering_components": {"novelty": 1.0, "confidence": 0.0, "clinical_evidence": 1.0}},
            ],
        },
    ]


def filters_input():
    results = [
        {
            "node_bindings": {
                "n0": [{"id": "CHEBI:6801"}],
                "n1": [{"id": "MONDO:0005148"}],
            },
            "normalized_score": 90.0,
        },
        {
            "node_bindings": {
                "n0": [{"id": "NCBIGene:5468"}],
                "n1": [{"id": "MONDO:0005148"}],
                "n2": [{"id": "CHEBI:17234"}],
            },
            "normalized_score": 50.0,
        },
        {
            "node_bindings": {
                "n0": [{"id": "CHEBI:17234"}],
                "n1": [{"id": "MONDO:0005148"}],
                "n2": [{"id": "NCBIGene:5468"}],
                "n3": [{"id": "CHEBI:6801"}],
            },
            "normalized_score": 25.0,
        },
        {
            "node_bindings": {
                "n0": [{"id": "UMLS:C0004096"}],
                "n1": [{"id": "MONDO:0005148"}],
            }
            # no normalized_score
        },
    ]
    kg_nodes = {
        "CHEBI:6801": {"categories": ["biolink:ChemicalEntity", "biolink:Drug"]},
        "MONDO:0005148": {"categories": ["biolink:Disease"]},
        "NCBIGene:5468": {"categories": ["biolink:Gene"]},
        "CHEBI:17234": {"categories": ["ChemicalEntity"]},  # no prefix
        "UMLS:C0004096": {"categories": ["biolink:DiseaseOrPhenotypicFeature"]},
    }
    return {"results": results, "kg_nodes": kg_nodes}


def mergedicts_cases():
    return [
        {"name": "disjoint", "dcurrent": {"a": 1}, "dmerged": {"b": 2}},
        {"name": "equal_scalars", "dcurrent": {"a": 1}, "dmerged": {"a": 1}},
        {"name": "scalar_conflict", "dcurrent": {"a": 2}, "dmerged": {"a": 1}},
        {"name": "none_current", "dcurrent": {"a": None}, "dmerged": {"a": 1}},
        {"name": "none_merged", "dcurrent": {"a": 5}, "dmerged": {"a": None}},
        {"name": "score_conflict", "dcurrent": {"score": 0.5}, "dmerged": {"score": 0.7}},
        {"name": "query_ids", "dcurrent": {"query_ids": "q2"}, "dmerged": {"query_ids": "q1"}},
        {"name": "name_skipped", "dcurrent": {"name": "x"}, "dmerged": {"name": "y"}},
        {"name": "list_plus_scalar", "dcurrent": {"a": 3}, "dmerged": {"a": [1, 2]}},
        {"name": "hashable_lists", "dcurrent": {"a": ["x", "y"]}, "dmerged": {"a": ["y", "z"]}},
        {"name": "nested_dicts", "dcurrent": {"a": {"x": 1, "y": 2}}, "dmerged": {"a": {"y": 2, "z": 3}}},
        {
            "name": "resource_id_lists",
            "dcurrent": {"sources": [{"resource_id": "infores:a", "n": 1}, {"resource_id": "infores:b", "n": 2}]},
            "dmerged": {"sources": [{"resource_id": "infores:a", "n": 1}, {"resource_id": "infores:c", "n": 3}]},
        },
        {
            "name": "qualifier_lists",
            "dcurrent": {"qualifiers": [{"qualifier_type_id": "biolink:object_direction_qualifier", "qualifier_value": "up"}]},
            "dmerged": {"qualifiers": [{"qualifier_type_id": "biolink:object_direction_qualifier", "qualifier_value": "up"}]},
        },
        {
            # distinguishes the Relay #883 fix (qualifiers keyed by
            # qualifier_type_id) from the old KeyError-and-bail behavior
            "name": "qualifier_lists_disjoint_types",
            "dcurrent": {
                "qualifiers": [
                    {"qualifier_type_id": "biolink:object_direction_qualifier", "qualifier_value": "up"},
                    {"qualifier_type_id": "biolink:object_aspect_qualifier", "qualifier_value": "activity"},
                ]
            },
            "dmerged": {
                "qualifiers": [
                    {"qualifier_type_id": "biolink:object_direction_qualifier", "qualifier_value": "down"}
                ]
            },
        },
        {
            "name": "qualifier_lists_conflicting_values",
            "dcurrent": {"qualifiers": [{"qualifier_type_id": "biolink:object_direction_qualifier", "qualifier_value": "up"}]},
            "dmerged": {"qualifiers": [{"qualifier_type_id": "biolink:object_direction_qualifier", "qualifier_value": "down"}]},
        },
        {
            "name": "attributes_union_lists",
            "dcurrent": {"attributes": [{"attribute_type_id": "biolink:publications", "value": ["PMID:2", "PMID:3"]}]},
            "dmerged": {"attributes": [{"attribute_type_id": "biolink:publications", "value": ["PMID:1", "PMID:2"]}]},
        },
        {
            "name": "attributes_scalar_value_appends",
            "dcurrent": {"attributes": [{"attribute_type_id": "biolink:knowledge_level", "value": "assertion"}]},
            "dmerged": {"attributes": [{"attribute_type_id": "biolink:knowledge_level", "value": "assertion"}]},
        },
        {
            "name": "attributes_new_type_appends",
            "dcurrent": {"attributes": [{"attribute_type_id": "biolink:agent_type", "value": ["a"]}]},
            "dmerged": {"attributes": [{"attribute_type_id": "biolink:publications", "value": ["PMID:1"]}]},
        },
        {
            "name": "attributes_multiple_existing_appends",
            "dcurrent": {"attributes": [{"attribute_type_id": "t", "value": ["v3"]}]},
            "dmerged": {"attributes": [{"attribute_type_id": "t", "value": ["v1"]}, {"attribute_type_id": "t", "value": ["v2"]}]},
        },
        {
            "name": "analyses_append",
            "dcurrent": {"analyses": [{"score": 1}]},
            "dmerged": {"analyses": [{"score": 2}]},
        },
        {
            "name": "node_bindings",
            "dcurrent": {"node_bindings": {"n0": [{"id": "A", "attributes": []}], "n1": [{"id": "B"}]}},
            "dmerged": {"node_bindings": {"n0": [{"id": "A", "attributes": []}], "n1": [{"id": "C"}]}},
        },
        {
            "name": "unhashable_lists_concat",
            "dcurrent": {"a": [{"x": 1}]},
            "dmerged": {"a": [{"y": 2}]},
        },
    ]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    fixtures = {
        "response_aragorn.json": response_aragorn(),
        "response_arax.json": response_arax(),
        "response_empty.json": {
            "message": {
                "query_graph": QUERY_GRAPH,
                "knowledge_graph": {"nodes": {}, "edges": {}},
                "results": [],
                "auxiliary_graphs": {},
            }
        },
        "response_blocklist.json": response_blocklist(),
        "scrub_input.json": scrub_input(),
        "decorate_cases.json": decorate_cases(),
        "scores_cases.json": scores_cases(),
        "ordering_cases.json": ordering_cases(),
        "filters_input.json": filters_input(),
        "mergedicts_cases.json": mergedicts_cases(),
    }
    for name, data in fixtures.items():
        path = OUT / name
        path.write_text(json.dumps(data, indent=2) + "\n")
        print(f"wrote {path}")


if __name__ == "__main__":
    main()

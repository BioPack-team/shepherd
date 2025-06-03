"""Generate an assortment of TRAPI messages."""

import copy


def generate_query():
    """Give a clean copy of a TRAPI query graph."""
    return copy.deepcopy(creative_query)


def generate_response():
    """Give a clean copy of a TRAPI response."""
    return copy.deepcopy(response_1)


creative_query = {
    "message": {
        "query_graph": {
            "nodes": {
                "SN": {
                    "categories": ["biolink:ChemicalEntity"],
                    "set_interpretation": "BATCH",
                },
                "ON": {
                    "ids": ["MONDO:0001"],
                    "categories": ["biolink:DiseaseOrPhenotypicFeature"],
                    "set_interpretation": "BATCH",
                },
            },
            "edges": {
                "e0": {
                    "subject": "SN",
                    "object": "ON",
                    "predicates": ["biolink:treats"],
                    "knowledge_type": "inferred",
                },
            },
        },
    },
}


response_1 = {
    "message": {
        "query_graph": {
            "nodes": {
                "SN": {
                    "categories": ["biolink:ChemicalEntity"],
                    "set_interpretation": "BATCH",
                },
                "ON": {
                    "ids": ["MONDO:0001"],
                    "categories": ["biolink:DiseaseOrPhenotypicFeature"],
                    "set_interpretation": "BATCH",
                },
            },
            "edges": {
                "e0": {
                    "subject": "SN",
                    "object": "ON",
                    "predicates": ["biolink:treats_or_applied_or_studied_to_treat"],
                }
            },
        },
        "knowledge_graph": {
            "nodes": {
                "MONDO:0001": {
                    "categories": ["biolink:Disease"],
                    "name": "type 2 diabetes mellitus",
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:source",
                            "value": "Disease Ontology",
                            "original_attribute_name": "source",
                        },
                        {
                            "attribute_type_id": "biolink:full_name",
                            "value": "type 2 diabetes mellitus",
                            "original_attribute_name": "name",
                        },
                        {
                            "attribute_type_id": "biolink:synonym",
                            "value": [
                                "type 2 diabetes mellitus",
                                "Diabetes Mellitus, Non-Insulin-Dependent",
                                "CORONARY ARTERY DISEASE, SUSCEPTIBILITY TO",
                                "INSULIN RESISTANCE, SUSCEPTIBILITY TO",
                                "PON1 ENZYME ACTIVITY, VARIATION IN",
                                "ORGANOPHOSPHATE POISONING, SUSCEPTIBILITY TO",
                                "CORONARY ARTERY SPASM 2, SUSCEPTIBILITY TO",
                                "TYPE 2 DIABETES MELLITUS, PROTECTION AGAINST",
                                "Diabetes Mellitus, Type 2",
                                "Type 2 Diabetes Mellitus",
                                "Type II diabetes mellitus",
                            ],
                        },
                    ],
                },
                "CHEBI:0001": {
                    "categories": ["biolink:ChemicalEntity"],
                    "name": "Sirolimus",
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:full_name",
                            "value": "Sirolimus",
                            "original_attribute_name": "name",
                        },
                    ],
                },
                "CHEBI:0002": {
                    "categories": ["biolink:ChemicalEntity"],
                    "name": "Obesity, Metabolically Benign",
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:full_name",
                            "value": "Obesity, Metabolically Benign",
                            "original_attribute_name": "name",
                        },
                    ],
                },
            },
            "edges": {
                "123": {
                    "subject": "CHEBI:0001",
                    "object": "MONDO:0001",
                    "predicate": "biolink:treats_or_applied_or_studied_to_treat",
                    "sources": [
                        {
                            "resource_id": "infores:aragorn",
                            "resource_role": "aggregator_knowledge_source",
                            "upstream_resource_ids": ["infores:spoke"],
                        },
                        {
                            "resource_id": "infores:spoke",
                            "resource_role": "aggregator_knowledge_source",
                            "upstream_resource_ids": ["infores:mesh"],
                        },
                        {
                            "resource_id": "infores:mesh",
                            "resource_role": "primary_knowledge_source",
                        },
                    ],
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:agent_type",
                            "value": "computational_model",
                            "attribute_source": "infores:spoke",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 585,
                            "original_attribute_name": "cooccur",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 4.007046832523005,
                            "original_attribute_name": "odds",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 3.677070454054205,
                            "original_attribute_name": "enrichment",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 4.2455109347801505e-156,
                            "original_attribute_name": "fisher",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:knowledge_level",
                            "value": "statistical_association",
                            "attribute_source": "infores:spoke",
                        },
                    ],
                },
                "234": {
                    "subject": "CHEBI:0002",
                    "object": "MONDO:0001",
                    "predicate": "biolink:treats_or_applied_or_studied_to_treat",
                    "sources": [
                        {
                            "resource_id": "infores:aragorn",
                            "resource_role": "aggregator_knowledge_source",
                            "upstream_resource_ids": ["infores:spoke"],
                        },
                        {
                            "resource_id": "infores:spoke",
                            "resource_role": "aggregator_knowledge_source",
                            "upstream_resource_ids": ["infores:mesh"],
                        },
                        {
                            "resource_id": "infores:mesh",
                            "resource_role": "primary_knowledge_source",
                        },
                    ],
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:agent_type",
                            "value": "computational_model",
                            "attribute_source": "infores:spoke",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 11,
                            "original_attribute_name": "cooccur",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 7.10693419457017e-5,
                            "original_attribute_name": "fisher",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 4.210169926265298,
                            "original_attribute_name": "enrichment",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:knowledge_level",
                            "value": "statistical_association",
                            "attribute_source": "infores:spoke",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 4.488906436622872,
                            "original_attribute_name": "odds",
                            "attribute_source": "infores:mesh",
                        },
                    ],
                },
            },
        },
        "results": [
            {
                "node_bindings": {
                    "SN": [{"id": "CHEBI:0001", "attributes": []}],
                    "ON": [{"id": "MONDO:0001", "attributes": []}],
                },
                "analyses": [
                    {
                        "resource_id": "infores:aragorn",
                        "edge_bindings": {"e0": [{"id": "123", "attributes": []}]},
                    }
                ],
            },
            {
                "node_bindings": {
                    "SN": [{"id": "CHEBI:0002", "attributes": []}],
                    "ON": [{"id": "MONDO:0001", "attributes": []}],
                },
                "analyses": [
                    {
                        "resource_id": "infores:aragorn",
                        "edge_bindings": {"e0": [{"id": "234", "attributes": []}]},
                    }
                ],
            },
        ],
        "auxiliary_graphs": {},
    },
    "log_level": "INFO",
    "bypass_cache": True,
    "logs": [
        {
            "message": "Doing lookup",
            "timestamp": "2024-10-31T14:40:50.537320",
            "level": "INFO",
        },
    ],
}

response_2 = {
    "message": {
        "query_graph": {
            "nodes": {
                "SN": {
                    "categories": ["biolink:ChemicalEntity"],
                    "set_interpretation": "BATCH",
                },
                "ON": {
                    "ids": ["MONDO:0001"],
                    "categories": ["biolink:DiseaseOrPhenotypicFeature"],
                    "set_interpretation": "BATCH",
                },
                "g": {"categories": ["biolink:Gene"]},
            },
            "edges": {
                "e0": {
                    "subject": "SN",
                    "object": "ON",
                    "predicates": ["biolink:treats_or_applied_or_studied_to_treat"],
                },
                "e1": {
                    "subject": "g",
                    "object": "SN",
                    "predicates": ["biolink:affects_response_to"],
                },
            },
        },
        "knowledge_graph": {
            "nodes": {
                "MONDO:0001": {
                    "categories": ["biolink:Disease"],
                    "name": "type 2 diabetes mellitus",
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:source",
                            "value": "Disease Ontology",
                            "original_attribute_name": "source",
                        },
                        {
                            "attribute_type_id": "biolink:full_name",
                            "value": "type 2 diabetes mellitus",
                            "original_attribute_name": "name",
                        },
                        {
                            "attribute_type_id": "biolink:synonym",
                            "value": [
                                "type 2 diabetes mellitus",
                                "Diabetes Mellitus, Non-Insulin-Dependent",
                                "CORONARY ARTERY DISEASE, SUSCEPTIBILITY TO",
                                "INSULIN RESISTANCE, SUSCEPTIBILITY TO",
                                "PON1 ENZYME ACTIVITY, VARIATION IN",
                                "ORGANOPHOSPHATE POISONING, SUSCEPTIBILITY TO",
                                "CORONARY ARTERY SPASM 2, SUSCEPTIBILITY TO",
                                "TYPE 2 DIABETES MELLITUS, PROTECTION AGAINST",
                                "Diabetes Mellitus, Type 2",
                                "Type 2 Diabetes Mellitus",
                                "Type II diabetes mellitus",
                            ],
                        },
                    ],
                },
                "NCBIGene:0001": {
                    "categories": ["biolink:Gene"],
                    "name": "BRCA1",
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:full_name",
                            "value": "BRCA1",
                            "original_attribute_name": "name",
                        },
                    ],
                },
                "CHEBI:0003": {
                    "categories": ["biolink:ChemicalEntity"],
                    "name": "Obesity, Metabolically Benign",
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:full_name",
                            "value": "Obesity, Metabolically Benign",
                            "original_attribute_name": "name",
                        },
                    ],
                },
            },
            "edges": {
                "345": {
                    "subject": "CHEBI:0003",
                    "object": "MONDO:0001",
                    "predicate": "biolink:treats_or_applied_or_studied_to_treat",
                    "sources": [
                        {
                            "resource_id": "infores:aragorn",
                            "resource_role": "aggregator_knowledge_source",
                            "upstream_resource_ids": ["infores:spoke"],
                        },
                        {
                            "resource_id": "infores:spoke",
                            "resource_role": "aggregator_knowledge_source",
                            "upstream_resource_ids": ["infores:mesh"],
                        },
                        {
                            "resource_id": "infores:mesh",
                            "resource_role": "primary_knowledge_source",
                        },
                    ],
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:agent_type",
                            "value": "computational_model",
                            "attribute_source": "infores:spoke",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 585,
                            "original_attribute_name": "cooccur",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 4.007046832523005,
                            "original_attribute_name": "odds",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 3.677070454054205,
                            "original_attribute_name": "enrichment",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 4.2455109347801505e-156,
                            "original_attribute_name": "fisher",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:knowledge_level",
                            "value": "statistical_association",
                            "attribute_source": "infores:spoke",
                        },
                    ],
                },
                "456": {
                    "subject": "NCBIGene:0001",
                    "object": "CHEBI:0003",
                    "predicate": "biolink:treats_or_applied_or_studied_to_treat",
                    "sources": [
                        {
                            "resource_id": "infores:aragorn",
                            "resource_role": "aggregator_knowledge_source",
                            "upstream_resource_ids": ["infores:spoke"],
                        },
                        {
                            "resource_id": "infores:spoke",
                            "resource_role": "aggregator_knowledge_source",
                            "upstream_resource_ids": ["infores:mesh"],
                        },
                        {
                            "resource_id": "infores:mesh",
                            "resource_role": "primary_knowledge_source",
                        },
                    ],
                    "attributes": [
                        {
                            "attribute_type_id": "biolink:agent_type",
                            "value": "computational_model",
                            "attribute_source": "infores:spoke",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 11,
                            "original_attribute_name": "cooccur",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 7.10693419457017e-5,
                            "original_attribute_name": "fisher",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 4.210169926265298,
                            "original_attribute_name": "enrichment",
                            "attribute_source": "infores:mesh",
                        },
                        {
                            "attribute_type_id": "biolink:knowledge_level",
                            "value": "statistical_association",
                            "attribute_source": "infores:spoke",
                        },
                        {
                            "attribute_type_id": "biolink:has_evidence",
                            "value": 4.488906436622872,
                            "original_attribute_name": "odds",
                            "attribute_source": "infores:mesh",
                        },
                    ],
                },
            },
        },
        "results": [
            {
                "node_bindings": {
                    "SN": [{"id": "CHEBI:0003", "attributes": []}],
                    "ON": [{"id": "MONDO:0001", "attributes": []}],
                    "g": [{"id": "NCBIGene:0001", "attributes": []}],
                },
                "analyses": [
                    {
                        "resource_id": "infores:aragorn",
                        "edge_bindings": {"e0": [{"id": "123", "attributes": []}]},
                    }
                ],
            },
        ],
        "auxiliary_graphs": {},
    },
    "log_level": "INFO",
    "bypass_cache": True,
    "logs": [
        {
            "message": "Doing lookup",
            "timestamp": "2024-10-31T14:40:50.537320",
            "level": "INFO",
        },
    ],
}

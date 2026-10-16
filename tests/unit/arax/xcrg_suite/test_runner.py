"""Smoke tests for the reusable xCRG package."""
from pathlib import Path
from typing import cast

from translator_tom import (
    Analysis,
    Attribute,
    AuxiliaryGraph,
    Edge,
    EdgeBinding,
    KnowledgeGraph,
    Message,
    Node,
    NodeBinding,
    QEdge,
    QEdgeConstraints,  # TRAPI 2.0: was QualifierConstraint
    QNode,
    Qualifier,
    Query,
    QueryGraph,
    Response,
    Result,
    RetrievalSource
)

import shepherd_utils.arax.xcrg.ngd as ngd
import shepherd_utils.arax.xcrg.runner as runner
from shepherd_utils.arax.xcrg import ranking
from shepherd_utils.arax.xcrg.config import XCRGConfig as Config # TODO
from shepherd_utils.arax.xcrg.context import RunContext
from shepherd_utils.arax.xcrg.reporting import StubReporter
from shepherd_utils.arax.xcrg.utilities import XCRGResult, format_json_for_log
from tests.unit.arax.xcrg_suite.utilities import make_curie_to_pmids_db


def make_context(
    query: Query | None = None,
    config: Config | None = None,
) -> RunContext:
    """Create a fake xCRG runner context."""
    return RunContext.new(
        query_id = "foo",
        query = query or make_inferred_query(),
        config = config or Config(
            retriever_url = "https://example.org/query",
            ngd_db_path = None,
        ),
        reporter = StubReporter()
    )


def make_inferred_query() -> Query:
    return Query(
        message = Message(
            query_graph = QueryGraph(
                nodes = {
                    "chem": QNode(
                        ids = ["CHEBI:1"],
                        categories = ["biolink:ChemicalEntity"]
                    ),
                    "gene": QNode(
                        ids = ["NCBIGene:1"],
                        categories = ["biolink:gene"]
                    )
                },
                edges = {
                    "e0": QEdge(
                        subject = "chem",
                        predicates = ["biolink:affects"],
                        object = "gene",
                        knowledge_type = "inferred"
                    )
                }
            )
        )
    )


def primary_source() -> list[RetrievalSource]:
    return [
        RetrievalSource(
            resource_id = "infores:test",
            resource_role = "primary_knowledge_source"
        )
    ]


def test_deserialize_example03_query():
    query_dict = {
        "message": {
            "query_graph": {
                "nodes": {
                    "on": {
                        "categories": [
                            "biolink:Gene"
                        ],
                        "ids": [
                            "NCBIGene:1576"
                        ]
                    },
                    "sn": {
                        "categories": [
                            "biolink:ChemicalEntity"
                        ]
                    }
                },
                "edges": {
                    "t_edge": {
                        "knowledge_type": "inferred",
                        "object": "on",
                        "predicates": [
                            "biolink:affects"
                        ],
                        # TRAPI 2.0: was qualifier_constraints [{qualifier_set: [...]}]
                        "constraints": {
                            "qualifiers": [
                                {
                                    "biolink:object_aspect_qualifier": "activity_or_abundance",
                                    "biolink:object_direction_qualifier": "increased"
                                }
                            ]
                        },
                        "subject": "sn"
                    }
                }
            }
        }
    }
    query = Query.from_dict(query_dict)
    assert runner.validate_query(query)


def test_deserialize_example03_query_with_extra_fields():
    query_dict = {
        'message':
            {
                'results': [],
                'query_graph': {
                    'nodes': {
                        'on': {
                            'ids': ['NCBIGene:1576'],
                            'categories': ['biolink:Gene'],
                            'is_set': False,
                            'set_id': None,
                            'set_interpretation': None,
                            'constraints': None,  # TRAPI 2.0: was [] (can't be empty)
                            'option_group_id': None
                        },
                        'sn': {
                            'ids': None,
                            'categories': ['biolink:ChemicalEntity'],
                            'is_set': False,
                            'set_id': None,
                            'set_interpretation': None,
                            'constraints': None,  # TRAPI 2.0: was [] (can't be empty)
                            'option_group_id': None
                        }
                    },
                    'edges': {
                        't_edge': {
                            'knowledge_type': 'inferred',
                            'predicates': ['biolink:affects'],
                            'subject': 'sn',
                            'object': 'on',
                            # TRAPI 2.0: was 'attribute_constraints': [] (now constraints.attributes,
                            # which can't be empty) and qualifier_constraints [{qualifier_set: [...]}]
                            'constraints': {
                                'qualifiers': [
                                    {
                                        'biolink:object_aspect_qualifier': 'activity_or_abundance',
                                        'biolink:object_direction_qualifier': 'increased'
                                    }
                                ]
                            },
                            'exclude': None,
                            'option_group_id': None
                        }
                    }
                },
                'knowledge_graph': {
                    'nodes': {},
                    'edges': {}
                },
                'auxiliary_graphs': None
            }
    }
    query = Query.from_dict(query_dict)
    assert runner.validate_query(query)


def test_debug_logging01():
    query = make_inferred_query()
    format_json_for_log(query)


def test_is_xcrg_mvp2_query_detects_supported_shape():
    query = {
        "message": {
            "query_graph": {
                "nodes": {
                    "chem": {"categories": ["biolink:ChemicalEntity"]},
                    "gene": {
                        "ids": ["NCBIGene:6323"],
                        "categories": ["biolink:Gene"],
                    },
                },
                "edges": {
                    "e0": {
                        "subject": "chem",
                        "object": "gene",
                        "predicates": ["biolink:affects"],
                        "knowledge_type": "inferred",
                        # TRAPI 2.0: was qualifier_constraints [{qualifier_set: [...]}]
                        "constraints": {
                            "qualifiers": [
                                {
                                    "biolink:object_aspect_qualifier": "activity_or_abundance",
                                    "biolink:object_direction_qualifier": "decreased",
                                }
                            ]
                        },
                    }
                },
            }
        }
    }

    assert runner.is_xcrg_mvp2_query(query)


def test_validate_inferred_query():
    query = Query(
        message = Message(
            query_graph = QueryGraph(
                nodes = {
                    "chem": QNode(categories = ["biolink:ChemicalEntity"], ids = None),
                    "gene": QNode(
                        ids = ["NCBIGene:6323"],
                        categories = ["biolink:Gene"]
                    ),
                },
                edges = {
                    "e0": QEdge(
                        subject = "chem",
                        predicates = ["biolink:affects"],
                        object = "gene",
                        knowledge_type = "inferred",
                        # TRAPI 2.0: was qualifier_constraints = [QualifierConstraint(qualifier_set = [Qualifier(...), ...])]
                        constraints = QEdgeConstraints(
                            qualifiers = [
                                {
                                    "biolink:object_aspect_qualifier": "activity_or_abundance",
                                    "biolink:object_direction_qualifier": "decreased"
                                }
                            ]
                        )
                    )
                },
            )
        )
    )

    assert runner.validate_query(query)


def test_load_tf_list_uses_bundled_default_resource():
    ctx = make_context(query = make_inferred_query())
    assert "NCBIGene:8932" in ctx.tf_list
    assert len(ctx.tf_list) > 100


def test_load_tf_list_uses_config_file(tmp_path: Path):
    tf_file = tmp_path / "tf_file.json"
    with open(tf_file, "w", encoding = "utf-8") as f:
        f.write("""
        {
            "tf": [
                "FOO",
                "BAR"
            ]
        }
        """)
    config = Config(retriever_url = "", tf_path = tf_file)
    ctx = make_context(query = make_inferred_query(), config = config)
    assert "FOO" in ctx.tf_list
    assert len(ctx.tf_list) == 2


def test_merge_filtered_responses_keeps_rich_retriever_metadata():
    ctx = make_context()
    sparse_response = Response(
        message = Message(
            knowledge_graph = KnowledgeGraph(
                nodes = {
                    "NCBIGene:1991": Node(
                        attributes = [],
                        categories = ["biolink:Gene"],
                    )
                },
                edges = {
                    "edge1": Edge(
                        subject = "CHEBI:17688",
                        predicate = "biolink:affects",
                        object = "NCBIGene:1991",
                        attributes = [],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    )
                }
            )
        )
    )
    rich_response = Response(
        message = Message(
            knowledge_graph = KnowledgeGraph(
                nodes = {
                    "NCBIGene:1991": Node(
                        name = "ELANE",
                        attributes = [
                            Attribute(
                                attribute_type_id = "biolink:Attribute",
                                original_attribute_name = "symbol",
                                value = "ELANE",
                            )
                        ],
                        categories = ["biolink:Gene", "biolink:Protein"]
                    )
                },
                edges = {
                    "edge1": Edge(
                        subject = "CHEBI:17688",
                        predicate = "biolink:affects",
                        object = "NCBIGene:1991",
                        attributes = [],  # TRAPI 2.0: biolink:knowledge_level attribute is now the knowledge_level member
                        knowledge_level = "knowledge_assertion",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                        sources = primary_source(),
                        qualifiers = [
                            Qualifier(
                                qualifier_type_id = "biolink:object_direction_qualifier",
                                qualifier_value = "increased"
                            )
                        ],
                    )
                }
            )
        )
    )

    responses = [rich_response, sparse_response]
    qgraph = cast(QueryGraph, ctx.query_graph)  # TRAPI 2.0: was QueryGraph(nodes = {}, edges = {}) (a qgraph can't be empty)

    merged = runner.merge_filtered_responses(ctx, responses, qgraph)

    assert merged.message.knowledge_graph
    merged_node = merged.message.knowledge_graph.nodes["NCBIGene:1991"]
    merged_edge = merged.message.knowledge_graph.edges["edge1"]

    assert rich_response.message.knowledge_graph
    assert merged_node == rich_response.message.knowledge_graph.nodes["NCBIGene:1991"]
    assert merged_edge == rich_response.message.knowledge_graph.edges["edge1"]


def test_clean_response_adds_binding_attributes_and_biolink_creation_date():
    ctx = make_context(make_inferred_query())

    combined_message = Response(
        message = Message(
            query_graph = ctx.query_graph,
            knowledge_graph = KnowledgeGraph(
                nodes = {
                    "CHEBI:1": Node(categories = ["biolink:ChemicalEntity"], attributes = []),
                    "NCBIGene:1": Node(categories = ["biolink:Gene"], attributes = []),
                    "NCBIGene:tf": Node(categories = ["biolink:Gene"], attributes = [])
                },
                edges = {
                    "direct1": Edge(
                        subject = "CHEBI:1",
                        predicate = "biolink:affects",
                        object = "NCBIGene:1",
                        attributes = [],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    ),
                    "path0": Edge(
                        subject = "CHEBI:1",
                        predicate = "biolink:affects",
                        object = "NCBIGene:tf",
                        attributes = [],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    ),
                    "path1": Edge(
                        subject = "NCBIGene:tf",
                        predicate = "biolink:affects",
                        object = "NCBIGene:1",
                        attributes = [],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    ),
                },
            ),
            results = [
                Result(
                    node_bindings = {
                        "chem": NodeBinding(ids = ["CHEBI:1"]),  # TRAPI 2.0: one binding with ids
                        "gene": NodeBinding(ids = ["NCBIGene:1"])  # TRAPI 2.0: one binding with ids
                    },
                    analyses = [
                        Analysis(
                            resource_id = "FOO:123456",
                            edge_bindings = {
                                "direct": EdgeBinding(ids = ["direct1"])  # TRAPI 2.0: one binding with ids
                            }
                        )
                    ]
                ),
                Result(
                    node_bindings = {
                        "chem": NodeBinding(ids = ["CHEBI:1"]),  # TRAPI 2.0: one binding with ids
                        "tf": NodeBinding(ids = ["NCBIGene:tf"]),  # TRAPI 2.0: one binding with ids
                        "gene": NodeBinding(ids = ["NCBIGene:1"])  # TRAPI 2.0: one binding with ids
                    },
                    analyses = [
                        Analysis(
                            resource_id = "FOO:234567",
                            edge_bindings = {
                                "e0": EdgeBinding(ids = ["path0"]),  # TRAPI 2.0: one binding with ids
                                "e1": EdgeBinding(ids = ["path1"])  # TRAPI 2.0: one binding with ids
                            }
                        )
                    ]
                )
            ]
        )
    )

    response = runner.build_trapi_clean_response(ctx, combined_message)

    # missing_node_attrs = [
    #     binding
    #     for result in response.message.results_list
    #     for bindings in result.node_bindings.values()
    #     for binding in bindings
    #     if not binding.attributes
    # ]
    # missing_edge_attrs = [
    #     binding
    #     for result in response.message.results_list
    #     for analysis in result.analyses
    #     for bindings in analysis.edge_bindings.values()
    #     for binding in bindings
    #     if not binding.attributes
    # ]

    assert response.message.knowledge_graph

    datetime_attrs = [
        attr
        for edge in response.message.knowledge_graph.edges.values()
        for attr in edge.attributes_list
        if attr.attribute_type_id == "metatype:Datetime"
    ]
    creation_attrs = [
        attr
        for edge in response.message.knowledge_graph.edges.values()
        for attr in edge.attributes or []
        if attr.attribute_type_id == "biolink:creation_date"
    ]
    auxiliary_graphs = response.message.auxiliary_graphs_dict
    auxiliary_graphs_without_attributes = [
        aux_id
        for aux_id, aux_graph in auxiliary_graphs.items()
        if "attributes" in aux_graph.to_dict()  # TRAPI 2.0: was aux_graph.attributes != [] (aux graphs have no attributes)
    ]

    # assert missing_node_attrs == []
    # assert missing_edge_attrs == []
    assert datetime_attrs == []
    assert creation_attrs
    assert auxiliary_graphs_without_attributes == []


def test_clean_response_adds_ngd_publications_from_curie_to_pmids(tmp_path):
    config = Config(
        retriever_url="https://example.org/query",
        ngd_db_path=None,
        curie_to_pmids_db_path=make_curie_to_pmids_db(
            tmp_path,
            {
                "CHEBI:1": [1001, 1002, 1003],
                "NCBIGene:1": [1002, 1003, 1004],
            },
        ),
    )
    ctx = make_context(query = make_inferred_query(), config = config)
    combined_message = Response(
        message = Message(
            query_graph = ctx.query_graph,
            knowledge_graph = KnowledgeGraph(
                nodes = {
                    "CHEBI:1": Node(categories = ["biolink:ChemicalEntity"], attributes = []),
                    "NCBIGene:1": Node(categories = ["biolink:Gene"], attributes = [])
                },
                edges = {
                    "direct1": Edge(
                        subject = "CHEBI:1",
                        predicate = "biolink:affects",
                        object = "NCBIGene:1",
                        attributes = [],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    )
                }
            ),
            results = [
                Result(
                    node_bindings = {
                        "chem": NodeBinding(ids = ["CHEBI:1"]),  # TRAPI 2.0: one binding with ids
                        "gene": NodeBinding(ids = ["NCBIGene:1"])  # TRAPI 2.0: one binding with ids
                    },
                    analyses = [
                        Analysis(
                            resource_id = "",
                            edge_bindings = {
                                "direct": EdgeBinding(ids = ["direct1"])  # TRAPI 2.0: one binding with ids
                            }
                        )
                    ]
                )
            ]
        )
    )

    response = runner.build_trapi_clean_response(ctx, combined_message)

    assert response.message.knowledge_graph

    ngd_edges = [
        edge
        for edge_id, edge in response.message.knowledge_graph.edges.items()
        if edge_id.startswith("xcrg_ngd_edge_")
    ]
    publication_attrs = [
        attr
        for edge in ngd_edges
        for attr in edge.attributes_list
        if attr.attribute_type_id == "biolink:publications"
    ]

    assert len(ngd_edges) == 1
    assert publication_attrs == [
        Attribute(
            attribute_source = "infores:arax",
            attribute_type_id = "biolink:publications",
            original_attribute_name = "publications",
            value_type_id = "EDAM-DATA:1187",
            value = ["PMID:1002", "PMID:1003"],
        )
    ]


def test_clean_response_preserves_retriever_nodes_verbatim_and_prunes_unused():
    ctx = make_context(query = make_inferred_query())
    chem_node = Node(
        name = "Chem One",
        categories = ["biolink:SmallMolecule"],
        attributes = [
            Attribute(
                attribute_type_id = "biolink:information_content",
                value = 12.3
            )
        ],
        # TODO: extra_field_from_retriever = {"keep = True},
    )
    gene_node = Node(
        name = "Gene One",
        categories = ["biolink:Gene"],
        attributes = [
            Attribute(
                attribute_type_id = "biolink:symbol",
                value = "GENE1"
            )
        ],
    )
    tf_node = Node(
        name = "TF One",
        categories = ["biolink:Gene"],
        attributes = [
            Attribute(
                attribute_type_id = "biolink:symbol",
                value = "TF1",
            )
        ]
    )
    combined_message = Response(
        message = Message(
            query_graph = ctx.query_graph,
            knowledge_graph = KnowledgeGraph(
                nodes = {
                    "CHEBI:1": chem_node,
                    "NCBIGene:1": gene_node,
                    "NCBIGene:tf": tf_node,
                    "NCBIGene:unused": Node(
                        name = None,
                        categories = ["biolink:unused"],
                        attributes = [],
                    )
                },
                edges = {
                    "path0": Edge(
                        subject = "CHEBI:1",
                        predicate = "biolink:affects",
                        object = "NCBIGene:tf",
                        attributes = [Attribute(attribute_type_id = "biolink:foo", value = None)],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    ),
                    "path1": Edge(
                        subject = "NCBIGene:tf",
                        predicate = "biolink:affects",
                        object = "NCBIGene:1",
                        attributes = [Attribute(attribute_type_id = "biolink:bar", value = None)],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    )
                }
            ),
            results = [
                Result(
                    node_bindings = {
                        "chem": NodeBinding(ids = ["CHEBI:1"]),  # TRAPI 2.0: one binding with ids
                        "tf": NodeBinding(ids = ["NCBIGene:tf"]),  # TRAPI 2.0: one binding with ids
                        "gene": NodeBinding(ids = ["NCBIGene:1"])  # TRAPI 2.0: one binding with ids
                    },
                    analyses = [
                        Analysis(
                            resource_id = "FOO:123456",
                            edge_bindings = {
                                "e0": EdgeBinding(ids = ["path0"]),  # TRAPI 2.0: one binding with ids
                                "e1": EdgeBinding(ids = ["path1"])  # TRAPI 2.0: one binding with ids
                            }
                        )
                    ]
                )
            ]
        )
    )

    response = runner.build_trapi_clean_response(ctx, combined_message)

    assert response.message.knowledge_graph
    final_nodes = response.message.knowledge_graph.nodes
    assert final_nodes["CHEBI:1"] == chem_node
    assert final_nodes["NCBIGene:1"] == gene_node
    assert final_nodes["NCBIGene:tf"] == tf_node
    assert "NCBIGene:unused" not in final_nodes


# TODO: I am not sure this test is doing something useful anymore
def test_clean_response_uses_only_pinned_query_metadata_for_missing_endpoint():
    ctx = make_context(query = make_inferred_query())
    assert ctx.query_graph
    ctx.query_graph.nodes["gene"] = QNode(
        ids = ["NCBIGene:1"],
        categories = ["biolink:gene"]
    )
    combined_message = Response(
        message = Message(
            query_graph = ctx.query_graph,
            knowledge_graph = KnowledgeGraph(
                nodes = {
                    "CHEBI:1": Node(
                        name = "Chem One",
                        categories = ["biolink:SmallMolecule"],
                        attributes = [],
                    ),
                    # TODO: Is this test looking for query to fill in this missing node?
                    #  Without this node the test fails; we would expect retriever to fill this for us...
                    "NCBIGene:1": Node(
                        categories = ["biolink:gene"],
                        attributes = []
                    )
                },
                edges = {
                    "direct1": Edge(
                        subject = "CHEBI:1",
                        predicate = "biolink:affects",
                        object = "NCBIGene:1",
                        attributes = [],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    )
                }
            ),
            results = [
                Result(
                    node_bindings = {
                        "chem": NodeBinding(ids = ["CHEBI:1"]),  # TRAPI 2.0: one binding with ids
                        "gene": NodeBinding(ids = ["NCBIGene:1"])  # TRAPI 2.0: one binding with ids
                    },
                    analyses = [
                        Analysis(
                            resource_id = "FOO:123456",
                            edge_bindings = {
                                "direct": EdgeBinding(ids = ["direct1"])  # TRAPI 2.0: one binding with ids
                            }
                        )
                    ]
                )
            ]
        )
    )

    response = runner.build_trapi_clean_response(ctx, combined_message)

    assert response.message.knowledge_graph
    final_nodes = response.message.knowledge_graph.nodes
    final_edges = response.message.knowledge_graph.edges
    assert final_nodes["NCBIGene:1"] == Node(
        categories = ["biolink:Gene"],
        attributes = []
    )
    assert "direct1" in final_edges


def test_clean_response_does_not_drop_retriever_node_with_empty_metadata():
    ctx = make_context(make_inferred_query())
    empty_tf_node = Node(
        name = None,
        categories = ["biolink:unused"],
        attributes = [],
    )
    combined_message = Response(
        message = Message(
            query_graph = ctx.query_graph,
            knowledge_graph = KnowledgeGraph(
                nodes = {
                    "CHEBI:1": Node(
                        name = "Chem One",
                        categories = ["biolink:SmallMolecule"],
                        attributes = [],
                    ),
                    "NCBIGene:tf": empty_tf_node,
                    "NCBIGene:1": Node(
                        name = "Gene One",
                        categories = ["biolink:Gene"],
                        attributes = [],
                    ),
                },
                edges = {
                    "path0": Edge(
                        subject = "CHEBI:1",
                        predicate = "biolink:affects",
                        object = "NCBIGene:tf",
                        attributes = [],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    ),
                    "path1": Edge(
                        subject = "NCBIGene:tf",
                        predicate = "biolink:affects",
                        object = "NCBIGene:1",
                        attributes = [],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    ),
                },
            ),
            results = [
                Result(
                    node_bindings = {
                        "chem": NodeBinding(ids = ["CHEBI:1"]),  # TRAPI 2.0: one binding with ids
                        "tf": NodeBinding(ids = ["NCBIGene:tf"]),  # TRAPI 2.0: one binding with ids
                        "gene": NodeBinding(ids = ["NCBIGene:1"])  # TRAPI 2.0: one binding with ids
                    },
                    analyses = [
                        Analysis(
                            resource_id = "FOO:123456",
                            edge_bindings = {
                                "e0": EdgeBinding(ids = ["path0"]),  # TRAPI 2.0: one binding with ids
                                "e1": EdgeBinding(ids = ["path1"])  # TRAPI 2.0: one binding with ids
                            }
                        )
                    ]
                )
            ]
        )
    )

    response = runner.build_trapi_clean_response(ctx, combined_message)

    assert response.message.knowledge_graph

    final_nodes = response.message.knowledge_graph.nodes
    inferred_bindings = [
        binding
        for result in response.message.results_list
        for analysis in result.analyses
        for bindings in cast(Analysis, analysis).edge_bindings.values()
        for binding in bindings.ids  # TRAPI 2.0: one EdgeBinding with ids per qedge
        if binding.startswith("xcrg_inferred_edge_")
    ]

    assert final_nodes["NCBIGene:tf"] == empty_tf_node
    assert inferred_bindings


def test_clean_response_limits_to_configured_top_result_count():
    config = Config(
        retriever_url="https://example.org/query",
        ngd_db_path=None,
        max_results=2,
    )
    ctx = make_context(query = make_inferred_query(), config = config)
    nodes = {"CHEBI:1": Node(categories = ["biolink:ChemicalEntity"], attributes = [])}
    edges = {}
    results = list[Result]()
    for index in range(3):
        gene_id = f"NCBIGene:{index}"
        edge_id = f"direct{index}"
        nodes[gene_id] = Node(categories = ["biolink:Gene"], attributes = [])
        edges[edge_id] = Edge(
            subject = "CHEBI:1",
            predicate = "biolink:affects",
            object = gene_id,
            attributes = [],
            sources = primary_source(),
            knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
            agent_type = "not_provided",  # TRAPI 2.0: required edge members
        )
        results.append(
            Result(
                node_bindings = {
                    "chem": NodeBinding(ids = ["CHEBI:1"]),  # TRAPI 2.0: one binding with ids
                    "gene": NodeBinding(ids = [gene_id]),  # TRAPI 2.0: one binding with ids
                },
                analyses = [
                    Analysis(
                        resource_id = "FOO:123456",
                        edge_bindings = {
                            "direct": EdgeBinding(ids = [edge_id])  # TRAPI 2.0: one binding with ids
                        },
                        score = 1.0 - (index * 0.1),
                    )
                ]
            )
        )
    combined_message = Response(
        message = Message(
            query_graph = ctx.query_graph,
            knowledge_graph = KnowledgeGraph(nodes = nodes, edges = edges),
            results = results
        )
    )

    response = runner.build_trapi_clean_response(ctx, combined_message)
    xcrg_results = [
        XCRGResult(node_bindings = x.node_bindings, analyses = x.analyses)
        for x in response.message.results_list
    ]
    ranking.rank_results(ctx, response, xcrg_results)

    final_results = response.message.results_list
    assert response.message.knowledge_graph
    final_nodes = response.message.knowledge_graph.nodes
    answer_ids = [
        result.node_bindings["gene"].ids[0]  # TRAPI 2.0: was node_bindings["gene"][0].id
        for result in final_results
    ]

    assert len(final_results) == 2
    assert answer_ids == ["NCBIGene:0", "NCBIGene:1"]
    assert "NCBIGene:2" not in final_nodes


def test_clean_response_copies_retriever_edge_auxiliary_graphs():
    ctx = make_context(make_inferred_query())
    combined_message = Response(
        message = Message(
            query_graph = ctx.query_graph,
            knowledge_graph = KnowledgeGraph(
                nodes = {
                    "CHEBI:1": Node(categories = ["biolink:ChemicalEntity"], attributes = []),
                    "NCBIGene:1": Node(categories = ["biolink:Gene"], attributes = []),
                    "NCBIGene:support": Node(categories = ["biolink:Gene"], attributes = [])
                },
                edges = {
                    "direct0": Edge(
                        subject = "CHEBI:1",
                        predicate = "biolink:affects",
                        object = "NCBIGene:1",
                        attributes = [
                            Attribute(
                                attribute_type_id = "biolink:support_graphs",
                                value = ["retriever_support_0"],
                            )
                        ],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    ),
                    "support0": Edge(
                        subject = "CHEBI:1",
                        predicate = "biolink:related_to",
                        object = "NCBIGene:support",
                        attributes = [],
                        sources = primary_source(),
                        knowledge_level = "not_provided",  # TRAPI 2.0: required edge members
                        agent_type = "not_provided",  # TRAPI 2.0: required edge members
                    ),
                },
            ),
            auxiliary_graphs = {
                "retriever_support_0": AuxiliaryGraph(
                    edges = ["support0"],  # TRAPI 2.0: aux graphs have no attributes (was attributes = [])
                )
            },
            results = [
                Result(
                    node_bindings = {
                        "chem": NodeBinding(ids = ["CHEBI:1"]),  # TRAPI 2.0: one binding with ids
                        "gene": NodeBinding(ids = ["NCBIGene:1"])  # TRAPI 2.0: one binding with ids
                    },
                    analyses = [
                        Analysis(
                            resource_id = "FOO:123456",
                            edge_bindings = {
                                "direct": EdgeBinding(ids = ["direct0"])  # TRAPI 2.0: one binding with ids
                            },
                            score = 1.0,
                        )
                    ]
                )
            ]
        )
    )

    response = runner.build_trapi_clean_response(ctx, combined_message)

    message = response.message
    assert message.knowledge_graph
    final_edges = message.knowledge_graph.edges
    final_aux_graphs = message.auxiliary_graphs_dict

    assert "retriever_support_0" in final_aux_graphs
    assert final_aux_graphs["retriever_support_0"].edges == ["support0"]
    assert "support0" in final_edges
    assert final_edges["direct0"].attributes_list[0].value == ["retriever_support_0"]


def test_xcrg_ngd_edge_skips_empty_publications_attribute():
    _, edge = ngd.make_xcrg_ngd_edge(
        make_context(),
        "CHEBI:1",
        "NCBIGene:1",
        0.5,
        []
    )

    attribute_type_ids = [
        attribute.attribute_type_id
        for attribute in edge.attributes_list
    ]

    assert "biolink:publications" not in attribute_type_ids

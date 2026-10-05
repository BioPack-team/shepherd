"""A small deterministic 'Retriever' universe shared by the mock server and runners."""

import random

CATS = {
    "CHEBI": "biolink:SmallMolecule",
    "MONDO": "biolink:Disease",
    "NCBIGene": "biolink:Gene",
    "HP": "biolink:PhenotypicFeature",
}
PREFIXES = sorted(CATS)
SOURCES = [
    "infores:semmeddb",
    "infores:drugbank",
    "infores:ctd",
    "infores:chembl",
    "infores:text-mining-provider-targeted",
    "infores:hpo-annotations",
]
# (subject prefix, predicate, object prefix)
TRIPLES = [
    ("CHEBI", "biolink:treats", "MONDO"),
    ("CHEBI", "biolink:applied_to_treat", "MONDO"),
    ("CHEBI", "biolink:in_clinical_trials_for", "MONDO"),
    ("CHEBI", "biolink:interacts_with", "NCBIGene"),
    ("CHEBI", "biolink:affects", "NCBIGene"),
    ("NCBIGene", "biolink:gene_associated_with_condition", "MONDO"),
    ("MONDO", "biolink:has_phenotype", "HP"),
    ("NCBIGene", "biolink:interacts_with", "NCBIGene"),
    ("CHEBI", "biolink:has_adverse_event", "HP"),
]
SYMMETRIC = {"biolink:interacts_with"}
# child -> parent (subclass_of), used for query_id bindings
SUBCLASS = {"MONDO:10": "MONDO:1", "MONDO:11": "MONDO:1", "CHEBI:20": "CHEBI:2"}
PRED_PARENTS = {
    "biolink:treats": {"biolink:treats_or_applied_or_studied_to_treat"},
    "biolink:applied_to_treat": {"biolink:treats_or_applied_or_studied_to_treat"},
    "biolink:in_clinical_trials_for": {"biolink:treats_or_applied_or_studied_to_treat"},
}
ERROR_CURIE = "MONDO:500"
SLOW_CURIE = "MONDO:504"


def build(seed=7, n_per_prefix=14, n_edges=160):
    r = random.Random(seed)
    nodes = {}
    for p in PREFIXES:
        for i in range(1, n_per_prefix + 1):
            nodes[f"{p}:{i}"] = {
                "name": f"{p} thing {i}",
                "categories": [CATS[p]],
                "attributes": (
                    [
                        {
                            "attribute_type_id": "biolink:description",
                            "value": f"desc {p}:{i}",
                        }
                    ]
                    if i % 3
                    else []
                ),
            }
    for c in list(SUBCLASS) + [ERROR_CURIE, SLOW_CURIE]:
        p = c.split(":")[0]
        nodes[c] = {
            "name": f"{p} special {c}",
            "categories": [CATS[p]],
            "attributes": [],
        }
    edges = {}
    ids = {p: [k for k in nodes if k.startswith(p + ":")] for p in PREFIXES}
    for i in range(n_edges):
        sp, pred, op = r.choice(TRIPLES)
        s, o = r.choice(ids[sp]), r.choice(ids[op])
        if s == o:
            continue
        src = r.choice(SOURCES)
        attrs = []
        if r.random() < 0.5:
            attrs.append(
                {
                    "attribute_type_id": "biolink:publications",
                    "value": [
                        f"PMID:{r.randint(1, 90)}" for _ in range(r.randint(1, 14))
                    ],
                }
            )
        if r.random() < 0.08:
            attrs.append(
                {"attribute_type_id": "biolink:p_value", "value": float("nan")}
            )
        if pred == "biolink:applied_to_treat" and r.random() < 0.6:
            attrs.append(
                {
                    "attribute_type_id": "biolink:number_of_cases",
                    "value": r.randint(1, 60),
                }
            )
        edge = {
            "subject": s,
            "object": o,
            "predicate": pred,
            "sources": [
                {"resource_id": src, "resource_role": "primary_knowledge_source"},
                {
                    "resource_id": "infores:retriever",
                    "resource_role": "aggregator_knowledge_source",
                    "upstream_resource_ids": [src],
                },
            ],
            "attributes": attrs,
        }
        if pred == "biolink:affects" and r.random() < 0.7:
            edge["qualifiers"] = [
                {
                    "qualifier_type_id": "biolink:object_direction_qualifier",
                    "qualifier_value": r.choice(["increased", "decreased"]),
                },
                {
                    "qualifier_type_id": "biolink:object_aspect_qualifier",
                    "qualifier_value": "activity",
                },
            ]
        edges[f"re{i}"] = edge
    # edges for the subclass children and the special curies
    for j, (child, parent) in enumerate(sorted(SUBCLASS.items())):
        p = child.split(":")[0]
        if p == "MONDO":
            edges[f"sc{j}"] = {
                "subject": "CHEBI:3",
                "object": child,
                "predicate": "biolink:treats",
                "sources": [
                    {
                        "resource_id": "infores:ctd",
                        "resource_role": "primary_knowledge_source",
                    }
                ],
                "attributes": [],
            }
        else:
            edges[f"sc{j}"] = {
                "subject": child,
                "object": "MONDO:4",
                "predicate": "biolink:treats",
                "sources": [
                    {
                        "resource_id": "infores:drugbank",
                        "resource_role": "primary_knowledge_source",
                    }
                ],
                "attributes": [],
            }
    for k, c in enumerate([ERROR_CURIE, SLOW_CURIE]):
        edges[f"sp{k}"] = {
            "subject": "CHEBI:5",
            "object": c,
            "predicate": "biolink:treats",
            "sources": [
                {
                    "resource_id": "infores:ctd",
                    "resource_role": "primary_knowledge_source",
                }
            ],
            "attributes": [],
        }
    # a few support-graph edges: unbound edges referenced by aux graphs
    aux = {}
    bound = sorted(edges)[::11]
    for n, ek in enumerate(bound):
        e = edges[ek]
        support_key = f"support{n}"
        edges[support_key] = {
            "subject": e["subject"],
            "object": e["object"],
            "predicate": "biolink:related_to",
            "sources": [
                {
                    "resource_id": "infores:semmeddb",
                    "resource_role": "primary_knowledge_source",
                }
            ],
            "attributes": [],
        }
        aux[f"aux{n}"] = {
            "edges": [support_key] + (["missing_edge"] if n % 4 == 0 else []),
            "attributes": [],
        }
        e["attributes"] = e["attributes"] + [
            {"attribute_type_id": "biolink:support_graphs", "value": [f"aux{n}"]}
        ]
    return nodes, edges, aux

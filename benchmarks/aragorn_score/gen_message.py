"""Generate a synthetic, creative-mode shaped TRAPI 2.0 response for the
aragorn.score benchmark.

Shape: one pinned disease, N chemicals each "treats" it through an inferred
edge whose support graph holds 1..max_paths two-hop chemical -> gene -> disease
paths, plus (like the omnicorp worker adds) literature co-occurrence edges
between pairs of nodes in each answer, attached as an analysis support graph.

    python gen_message.py --results 2000 --out out/synthetic.json.zst

``--nested`` makes every chemical -> gene edge itself supported by a further
support graph; ``--quirks`` adds answers that exercise the ranker's odd inputs
and failure paths (string publication lists, p-values as strings, a support
cycle, an omnicorp edge whose node is missing from the KG, a result missing a
probe binding), which is what you want when checking parity, not speed.

A ``.zst`` output is written the way Shepherd stores messages in Redis
(zstd-compressed orjson); anything else is plain JSON.
"""

import argparse
import random
import sys
from itertools import combinations
from pathlib import Path

import orjson
import zstandard

SOURCES_PATH = [
    "infores:drugcentral",
    "infores:chembl",
    "infores:text-mining-provider-targeted",
]
SOURCES_GENE = ["infores:ctd", "infores:hetionet", "infores:gwas-catalog"]


def make_message(
    n_results=2000, n_genes=400, max_paths=12, seed=0, omnicorp=True, nested=False
):
    rnd = random.Random(seed)
    nodes, edges, aux = {}, {}, {}
    disease = "MONDO:0000001"

    def node(nid):
        nodes.setdefault(
            nid,
            {
                "categories": ["biolink:NamedThing"],
                "attributes": [
                    {
                        "original_attribute_name": "omnicorp_article_count",
                        "attribute_type_id": "biolink:has_count",
                        "value": rnd.randint(10, 100000),
                    }
                ],
            },
        )

    def edge(eid, s, o, src, pred="biolink:related_to", extra=()):
        n_pubs = rnd.randint(0, 6)
        edges[eid] = {
            "subject": s,
            "object": o,
            "predicate": pred,
            "knowledge_level": "knowledge_assertion",
            "agent_type": "manual_agent",
            "sources": [
                {"resource_id": src, "resource_role": "primary_knowledge_source"}
            ],
            "attributes": [
                {
                    "attribute_type_id": "biolink:publications",
                    "value": [f"PMID:{rnd.randint(1, 10**7)}" for _ in range(n_pubs)],
                },
                *extra,
            ],
        }

    node(disease)
    genes = [f"NCBIGene:{i}" for i in range(n_genes)]
    for g in genes:
        node(g)
        edge(
            f"gd_{g}",
            g,
            disease,
            rnd.choice(SOURCES_GENE),
            extra=[
                {
                    "original_attribute_name": "p_value",
                    "attribute_type_id": "biolink:p_value",
                    "value": rnd.random() * 0.1,
                }
            ],
        )

    results = []
    for i in range(n_results):
        chem = f"CHEBI:{i}"
        node(chem)
        path_genes = rnd.sample(genes, rnd.randint(1, max_paths))
        sg_edges = []
        for g in path_genes:
            eid = f"cg_{chem}_{g}"
            edge(eid, chem, g, rnd.choice(SOURCES_PATH))
            if nested:
                # supported in turn by three gene -> disease edges
                sub_id = f"nested_{eid}"
                aux[sub_id] = {"edges": [f"gd_{x}" for x in rnd.sample(genes, 3)]}
                edges[eid]["attributes"].append(
                    {"attribute_type_id": "biolink:support_graphs", "value": [sub_id]}
                )
            sg_edges += [eid, f"gd_{g}"]
        aux[f"aux_{i}"] = {"edges": sg_edges}
        inferred = f"inferred_{i}"
        edges[inferred] = {
            "subject": chem,
            "object": disease,
            "predicate": "biolink:treats",
            "knowledge_level": "prediction",
            "agent_type": "computational_model",
            "sources": [
                {
                    "resource_id": "infores:aragorn",
                    "resource_role": "primary_knowledge_source",
                }
            ],
            "attributes": [
                {"attribute_type_id": "biolink:support_graphs", "value": [f"aux_{i}"]}
            ],
        }
        analysis = {
            "resource_id": "infores:aragorn",
            "edge_bindings": {"t_edge": {"ids": [inferred]}},
        }
        if omnicorp:
            literature = []
            for a, b in combinations([chem, disease, *path_genes], 2):
                if rnd.random() < 0.5:
                    oid = f"omni_{len(edges)}"
                    edges[oid] = {
                        "subject": a,
                        "object": b,
                        "predicate": "biolink:occurs_together_in_literature_with",
                        "knowledge_level": "statistical_association",
                        "agent_type": "statistical_association_pipeline",
                        "sources": [
                            {
                                "resource_id": "infores:omnicorp",
                                "resource_role": "primary_knowledge_source",
                            }
                        ],
                        "attributes": [
                            {
                                "original_attribute_name": "num_publications",
                                "attribute_type_id": "biolink:has_count",
                                "value": rnd.randint(1, 500),
                            }
                        ],
                    }
                    literature.append(oid)
            if literature:
                aux[f"OMNICORP_support_graph_{i}"] = {"edges": literature}
                analysis["support_graphs"] = [f"OMNICORP_support_graph_{i}"]
        results.append(
            {
                "node_bindings": {"on": {"ids": [disease]}, "sn": {"ids": [chem]}},
                "analyses": [analysis],
            }
        )

    message = {
        "query_graph": {
            "nodes": {
                "on": {"ids": [disease]},
                "sn": {"categories": ["biolink:ChemicalEntity"]},
            },
            "edges": {
                "t_edge": {
                    "subject": "sn",
                    "object": "on",
                    "predicates": ["biolink:treats"],
                    "knowledge_type": "inferred",
                }
            },
        },
        "knowledge_graph": {"nodes": nodes, "edges": edges},
        "auxiliary_graphs": aux,
        "results": results,
    }
    return message


def add_quirks(message):
    """Answers that take the ranker's unusual paths. Appended at the end."""
    kg = message["knowledge_graph"]
    aux = message["auxiliary_graphs"]
    disease = "MONDO:0000001"
    src = lambda r: [{"resource_id": r, "resource_role": "primary_knowledge_source"}]

    def answer(chem, edge_ids, support_graphs=None, bindings=None):
        kg["nodes"].setdefault(chem, {"categories": ["biolink:ChemicalEntity"]})
        analysis = {
            "resource_id": "infores:aragorn",
            "edge_bindings": {"t_edge": {"ids": edge_ids}},
        }
        if support_graphs:
            analysis["support_graphs"] = support_graphs
        message["results"].append(
            {
                "node_bindings": bindings
                or {"on": {"ids": [disease]}, "sn": {"ids": [chem]}},
                "analyses": [analysis],
            }
        )

    # pipe- and comma-joined publication strings, evidence_count, string and
    # list p-values, affinity, tmkp confidence score, unknown source
    for i, attrs in enumerate(
        [
            [
                {
                    "attribute_type_id": "biolink:publications",
                    "value": "PMID:1|PMID:2|PMID:3",
                }
            ],
            [{"original_attribute_name": "publications", "value": ["PMID:1,PMID:2"]}],
            [{"attribute_type_id": "biolink:evidence_count", "value": 12}],
            [{"original_attribute_name": "pValue", "value": "0.0004"}],
            [{"original_attribute_name": "gwas_pvalue", "value": [0.02]}],
            [{"original_attribute_name": "affinity", "value": 7.5}],
            [
                {
                    "original_attribute_name": "biolink:tmkp_confidence_score",
                    "value": 0.8,
                }
            ],
            [{"attribute_type_id": "biolink:p_value", "value": "not a number"}],
        ]
    ):
        eid = f"quirk_edge_{i}"
        kg["edges"][eid] = {
            "subject": f"QUIRK:{i}",
            "object": disease,
            "predicate": "biolink:treats",
            "sources": src(
                [
                    "infores:x",
                    "infores:icees-kg",
                    "infores:text-mining-provider-targeted",
                ][i % 3]
            ),
            "attributes": attrs,
        }
        answer(f"QUIRK:{i}", [eid])

    # a support-graph cycle: a -> g_b -> b -> g_a -> a
    for name, other in (("a", "b"), ("b", "a")):
        kg["edges"][f"cyc_{name}"] = {
            "subject": "QUIRK:cycle",
            "object": f"GENE:cyc_{name}",
            "predicate": "biolink:related_to",
            "sources": src("infores:x"),
            "attributes": [
                {
                    "attribute_type_id": "biolink:support_graphs",
                    "value": [f"g_cyc_{other}"],
                }
            ],
        }
        aux[f"g_cyc_{other}"] = {"edges": [f"cyc_{other}"]}
    kg["edges"]["cyc_top"] = {
        "subject": "QUIRK:cycle",
        "object": disease,
        "predicate": "biolink:treats",
        "sources": src("infores:aragorn"),
        "attributes": [
            {"attribute_type_id": "biolink:support_graphs", "value": ["g_cyc_a"]}
        ],
    }
    answer("QUIRK:cycle", ["cyc_top"])

    # an omnicorp edge to a node missing from the KG: KeyError -> scored 0
    kg["edges"]["omni_missing"] = {
        "subject": "QUIRK:missing_node",
        "object": disease,
        "predicate": "biolink:occurs_together_in_literature_with",
        "sources": src("infores:omnicorp"),
        "attributes": [{"attribute_type_id": "biolink:has_count", "value": 3}],
    }
    kg["edges"]["missing_top"] = {
        "subject": "QUIRK:missing_node",
        "object": disease,
        "predicate": "biolink:treats",
        "sources": src("infores:x"),
    }
    answer("QUIRK:missing_node", ["missing_top", "omni_missing"])
    kg["nodes"].pop("QUIRK:missing_node", None)

    # a result with no binding for a probe q_node: ValueError -> scored 0
    kg["edges"]["no_probe"] = {
        "subject": "QUIRK:no_probe",
        "object": disease,
        "predicate": "biolink:treats",
        "sources": src("infores:x"),
    }
    answer("QUIRK:no_probe", ["no_probe"], bindings={"on": {"ids": [disease]}})

    # an edge binding to an edge that isn't in the KG
    answer("QUIRK:dangling", ["not_in_kg"])

    # self-loops (an r_node wired to itself), which make the Laplacian's row
    # sums non-zero: an edge from a node to itself in a support graph, and a
    # k_node bound to both q_nodes
    kg["edges"]["self_loop"] = {
        "subject": "QUIRK:self",
        "object": "QUIRK:self",
        "predicate": "biolink:related_to",
        "sources": src("infores:x"),
    }
    kg["edges"]["self_top"] = {
        "subject": "QUIRK:self",
        "object": disease,
        "predicate": "biolink:treats",
        "sources": src("infores:x"),
        "attributes": [
            {"attribute_type_id": "biolink:support_graphs", "value": ["g_self"]}
        ],
    }
    aux["g_self"] = {"edges": ["self_loop"]}
    answer("QUIRK:self", ["self_top"])
    kg["edges"]["both_bound"] = {
        "subject": disease,
        "object": disease,
        "predicate": "biolink:treats",
        "sources": src("infores:x"),
    }
    answer(
        disease,
        ["both_bound"],
        bindings={"on": {"ids": [disease]}, "sn": {"ids": [disease]}},
    )
    return message


def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--results", type=int, default=2000)
    p.add_argument("--genes", type=int, default=400)
    p.add_argument("--max-paths", type=int, default=12)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--no-omnicorp", action="store_true")
    p.add_argument("--nested", action="store_true")
    p.add_argument("--quirks", action="store_true")
    p.add_argument("--out", required=True, type=Path)
    a = p.parse_args(argv)

    message = make_message(
        a.results, a.genes, a.max_paths, a.seed, not a.no_omnicorp, a.nested
    )
    if a.quirks:
        add_quirks(message)
    data = orjson.dumps({"message": message})
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_bytes(zstandard.compress(data) if a.out.suffix == ".zst" else data)
    print(
        f"wrote {a.out}: {len(message['results'])} results, "
        f"{len(message['knowledge_graph']['edges'])} KG edges, {len(data) / 1e6:.1f} MB JSON",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()

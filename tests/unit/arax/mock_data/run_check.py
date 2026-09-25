"""Generate the mock ARAX data files and run ARAX over them, in-process.

Usage: PYTHONHASHSEED=0 python run_check.py OUT.json

Runs in its own process so the data directories are set (ARAX_DBS_DIR,
ARAX_PATHFINDER_DBS_DIR) before shepherd_utils.config is imported: some ARAX
modules read their paths at import. NodeNorm and COHD's biolink_to_omop call
are stubbed from the mock graph, so nothing leaves the machine.
"""

import copy
import json
import os
import shutil
import sys
import tempfile
import warnings

warnings.simplefilter("ignore")
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(HERE, "..", "..", "..", "..")))

DATA = tempfile.mkdtemp(prefix="arax_mock_data_")
os.environ["ARAX_DBS_DIR"] = os.path.join(DATA, "arax")
# as upstream's goldens were recorded: the KP cache always misses, stores nothing
os.environ["ARAX_KP_CACHE_ENABLED"] = "false"
os.environ["ARAX_PATHFINDER_DBS_DIR"] = os.path.join(DATA, "pathfinder")
os.environ["ARAX_BIOLINK_CACHE_DIR"] = os.path.join(DATA, "biolink")
os.environ["SERVER_URL"] = "http://shepherd.test"
os.makedirs(os.environ["ARAX_BIOLINK_CACHE_DIR"])
shutil.copy(
    os.path.join(HERE, "..", "expand_parity", "biolink_lookup_map_4.2.5_v5.pickle"),
    os.environ["ARAX_BIOLINK_CACHE_DIR"],
)

from shepherd_utils import arax_mock_data as M  # noqa: E402

assert M.main(["--pathfinder", "--no-network"]) == 0
GRAPH = M.MockGraph.seed()
BY_NAME = {node["name"]: curie for curie, node in GRAPH.nodes.items()}


def _as_list(x):
    return x if isinstance(x, list) else [x]


def fake_canonical(self, curies=None, names=None, return_all_categories=False, **kw):
    out = {}
    for c in _as_list(curies):
        curie = c if c in GRAPH.nodes else BY_NAME.get(c)
        if curie is None:
            out[c] = None
            continue
        category = GRAPH.nodes[curie]["categories"][0]
        info = {
            "preferred_curie": curie,
            "preferred_name": GRAPH.nodes[curie]["name"],
            "preferred_category": category,
        }
        if return_all_categories:
            info["all_categories"] = {category: 1}
        out[c] = info
    return out


from shepherd_utils.arax.NodeSynonymizer import node_synonymizer as ns  # noqa: E402
from shepherd_utils.arax.KnowledgeSources.COHD_local.scripts import (  # noqa: E402
    COHDIndex as cohd,
)

ns.NodeSynonymizer.__init__ = lambda self, *a, **k: None
ns.NodeSynonymizer.get_canonical_curies = fake_canonical
ns.NodeSynonymizer.get_equivalent_nodes = lambda self, curies=None, **k: {
    c: [c] for c in _as_list(curies)
}
ns.NodeSynonymizer.get_curie_names = lambda self, curies, **k: {
    c: GRAPH.nodes.get(c, {}).get("name", c) for c in _as_list(curies)
}
# the made-up OMOP ids the generator wrote (--no-network)
OMOP = M.cohd_omop_ids(sorted(GRAPH.nodes), network=False)
cohd.COHDIndex._call_cohd_biolink_to_omop_api = staticmethod(
    lambda query: {c: [OMOP[c]] if c in OMOP else [] for c in query["curies"]}
)

from shepherd_utils.arax.ARAX_query import ARAXQuery  # noqa: E402


def run(actions, message=None):
    body = {"operations": {"actions": actions + ["return(message=true, store=false)"]}}
    if message is not None:
        body["message"] = copy.deepcopy(message)
    araxq = ARAXQuery(response_id="mock")
    araxq.query(body)
    response = araxq.response
    return response, response.envelope.message.to_dict()


def problems(response):
    return [
        m["message"] for m in response.messages if m["level"] in ("ERROR", "WARNING")
    ]


def edge_values(message, attribute_name):
    """{(subject, object): value} of an attribute on the KG edges."""
    out = {}
    for edge in message["knowledge_graph"]["edges"].values():
        for attribute in edge.get("attributes") or []:
            if attribute.get("original_attribute_name") == attribute_name:
                out[f"{edge['subject']}|{edge['object']}"] = attribute["value"]
    return out


# 1. Overlays over an uploaded message: every seed drug -> disease/phenotype edge
kg_nodes, kg_edges, results = {}, {}, []
for key, edge in sorted(GRAPH.edges.items()):
    if not GRAPH.is_drug(edge["subject"]) or not GRAPH.is_disease(edge["object"]):
        continue
    kg_edges[key] = {
        "subject": edge["subject"],
        "object": edge["object"],
        "predicate": edge["predicate"],
        "sources": edge["sources"],
        "attributes": [],
    }
    for curie in (edge["subject"], edge["object"]):
        node = GRAPH.nodes[curie]
        kg_nodes[curie] = {
            "name": node["name"],
            "categories": node["categories"],
            "attributes": [],
        }
    results.append(
        {
            "node_bindings": {
                "n0": [{"id": edge["subject"], "attributes": []}],
                "n1": [{"id": edge["object"], "attributes": []}],
            },
            "analyses": [
                {
                    "resource_id": "infores:mock",
                    "edge_bindings": {"e0": [{"id": key, "attributes": []}]},
                }
            ],
        }
    )
drugs = sorted({e["subject"] for e in kg_edges.values()})
message = {
    "query_graph": {
        "nodes": {
            "n0": {"ids": drugs},
            "n1": {"categories": ["biolink:DiseaseOrPhenotypicFeature"]},
        },
        "edges": {"e0": {"subject": "n0", "object": "n1"}},
    },
    "knowledge_graph": {"nodes": kg_nodes, "edges": kg_edges},
    "results": results,
}
response, out_message = run(
    [
        "overlay(action=compute_ngd, virtual_relation_label=N1, subject_qnode_key=n0, object_qnode_key=n1)",
        "overlay(action=fisher_exact_test, subject_qnode_key=n0, object_qnode_key=n1, virtual_relation_label=F1)",
        "overlay(action=overlay_clinical_info, paired_concept_frequency=true, virtual_relation_label=C1, subject_qnode_key=n0, object_qnode_key=n1)",
    ],
    message,
)
overlays = {
    "status": response.status,
    "problems": problems(response),
    "n_input_edges": len(kg_edges),
    "ngd": edge_values(out_message, "normalized_google_distance"),
    "fisher": edge_values(out_message, "fisher_exact_test_p-value"),
    "cohd": edge_values(out_message, "paired_concept_frequency"),
}

# 2. Infer (xDTD) for a seed disease
response, out_message = run(
    [
        "add_qnode(key=drug, categories=biolink:Drug)",
        "add_qnode(key=disease, ids=MONDO:0005148)",
        "add_qedge(key=t, subject=drug, object=disease, predicates=biolink:treats)",
        "infer(action=drug_treatment_graph_expansion, disease_curie=MONDO:0005148, qedge_id=t, n_drugs=10, n_paths=3)",
    ]
)
infer = {
    "status": response.status,
    "problems": problems(response),
    "n_results": len(out_message.get("results") or []),
    "n_aux_graphs": len(out_message.get("auxiliary_graphs") or {}),
    "predicted_drugs": sorted(
        e["subject"]
        for e in out_message["knowledge_graph"]["edges"].values()
        if e["predicate"] == "biolink:treats"
        and e["object"] == "MONDO:0005148"
        and any(
            a.get("original_attribute_name") == "probability_treats"
            for a in e.get("attributes") or []
        )
    ),
}

with open(sys.argv[1], "w") as f:
    json.dump({"overlays": overlays, "infer": infer}, f, sort_keys=True, default=str)
shutil.rmtree(DATA, ignore_errors=True)

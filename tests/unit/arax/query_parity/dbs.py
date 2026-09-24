"""Small synthetic ARAX data files built from the Expand parity universe.

The same files are written for upstream ARAX (under its KnowledgeSources/ dirs)
and for the port (under Shepherd's data dirs), so the overlays and Infer read
identical data: curie_to_pmids (NGD), the COHD database (overlay_clinical_info),
and the ExplainableDTD database (Infer / creative treats).
"""

import json
import os
import random
import sqlite3

import fixtures as F
import universe as U

NODES, EDGES = F.NODES, F.EDGES


def _fresh(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        os.remove(path)
    return sqlite3.connect(path)


def write_curie_to_pmids(path, seed=11):
    r = random.Random(seed)
    con = _fresh(path)
    con.execute("CREATE TABLE curie_to_pmids (curie TEXT, pmids TEXT)")
    rows = []
    for i, curie in enumerate(sorted(NODES)):
        if i % 5 == 4:
            continue  # some curies have no PMIDs
        pmids = sorted(r.sample(range(1, 400), r.randint(1, 60)))
        rows.append((curie, json.dumps(pmids)))
    con.executemany("INSERT INTO curie_to_pmids VALUES (?,?)", rows)
    con.commit()
    con.close()


# COHD: curie -> OMOP concept id, answered by a stub of COHD's biolink_to_omop API
OMOP = {c: 1000 + i for i, c in enumerate(sorted(NODES)) if i % 4 != 3}


def fake_biolink_to_omop(query):
    return {c: ([OMOP[c]] if c in OMOP else []) for c in query["curies"]}


def write_cohd(path, seed=13):
    r = random.Random(seed)
    con = _fresh(path)
    con.execute(
        "CREATE TABLE SINGLE_CONCEPT_COUNTS( dataset_id TINYINT, concept_id INT, concept_count INT, concept_prevalence FLOAT )"
    )
    con.execute(
        "CREATE TABLE CONCEPTS( concept_id INT PRIMARY KEY, concept_name VARCHAR(255), domain_id VARCHAR(255), vocabulary_id VARCHAR(255), concept_class_id VARCHAR(255), concept_code VARCHAR(255) )"
    )
    con.execute(
        "CREATE TABLE PATIENT_COUNT( dataset_id TINYINT PRIMARY KEY, count INT)"
    )
    con.execute(
        "CREATE TABLE DATASET( dataset_id TINYINT PRIMARY KEY, dataset_name VARCHAR(255), dataset_description VARCHAR(255))"
    )
    con.execute(
        "CREATE TABLE DOMAIN_CONCEPT_COUNTS( dataset_id TINYINT, domain_id VARCHAR(255), count INT)"
    )
    con.execute(
        "CREATE TABLE DOMAIN_PAIR_CONCEPT_COUNTS( dataset_id TINYINT, domain_id_1 VARCHAR(255), domain_id_2 VARCHAR(255), count INT)"
    )
    con.execute(
        "CREATE TABLE PAIRED_CONCEPT_COUNTS_ASSOCIATIONS( concept_pair_id VARCHAR(255), dataset_id TINYINT, concept_id_1 INT, concept_id_2 INT, concept_count INT, concept_prevalence FLOAT, chi_square_t FLOAT, chi_square_p FLOAT, expected_count FLOAT, ln_ratio FLOAT, rel_freq_1 FLOAT, rel_freq_2 FLOAT)"
    )
    ids = sorted(OMOP.values())
    for dataset_id in (1, 2, 3):
        con.execute("INSERT INTO PATIENT_COUNT VALUES (?,?)", (dataset_id, 1000000))
        for cid in ids:
            n = r.randint(10, 5000)
            con.execute(
                "INSERT INTO SINGLE_CONCEPT_COUNTS VALUES (?,?,?,?)",
                (dataset_id, cid, n, n / 1e6),
            )
        for i, a in enumerate(ids):
            for b in ids[i + 1 :]:
                if r.random() < 0.35:
                    continue
                n = r.randint(1, 300)
                con.execute(
                    "INSERT INTO PAIRED_CONCEPT_COUNTS_ASSOCIATIONS VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        f"{a}_{b}",
                        dataset_id,
                        a,
                        b,
                        n,
                        n / 1e6,
                        round(r.uniform(0, 50), 4),
                        round(r.uniform(0, 1e-3), 8),
                        round(r.uniform(1, 200), 4),
                        round(r.uniform(-2, 4), 4),
                        round(r.uniform(0, 1), 4),
                        round(r.uniform(0, 1), 4),
                    ),
                )
    for cid in ids:
        con.execute(
            "INSERT INTO CONCEPTS VALUES (?,?,?,?,?,?)",
            (
                cid,
                f"concept {cid}",
                "Condition",
                "SNOMED",
                "Clinical Finding",
                str(cid),
            ),
        )
    con.commit()
    con.close()


# xDTD: drugs predicted to treat diseases, with explanation paths over real
# universe edges (so the mapping table has their provenance)
XDTD_DISEASES = ["MONDO:1", "MONDO:2", "MONDO:3"]


def _paths_to(disease, r):
    """Up to 6 explanation paths ending at the disease: 1-hop and 2-hop, from universe edges."""
    into = [
        e
        for e in EDGES.values()
        if e["object"] == disease and e["subject"].startswith("CHEBI:")
    ]
    two = []
    for e2 in EDGES.values():
        if e2["object"] != disease or not e2["subject"].startswith("NCBIGene:"):
            continue
        for e1 in EDGES.values():
            if e1["object"] == e2["subject"] and e1["subject"].startswith("CHEBI:"):
                two.append((e1, e2))
    paths = []
    for e in into[:3]:
        paths.append((e["subject"], [e]))
    for e1, e2 in two[:5]:
        paths.append((e1["subject"], [e1, e2]))
    return paths


def write_xdtd(path, seed=17):
    r = random.Random(seed)
    con = _fresh(path)
    con.execute(
        "CREATE TABLE PREDICTION_SCORE_TABLE (drug_id VARCHAR(255), drug_name VARCHAR(255), disease_id VARCHAR(255), disease_name VARCHAR(255), tn_score FLOAT, tp_score FLOAT, unknown_score FLOAT)"
    )
    con.execute(
        "CREATE TABLE PATH_RESULT_TABLE (drug_id VARCHAR(255), drug_name VARCHAR(255), disease_id VARCHAR(255), disease_name VARCHAR(255), path VARCHAR(255), path_score FLOAT)"
    )
    con.execute(
        "CREATE TABLE NODE_MAPPING_TABLE (id TEXT NOT NULL, name TEXT, category TEXT, equivalent_identifiers TEXT, description TEXT, synonym TEXT, xref TEXT, chembl_natural_product TEXT, chembl_availability_type TEXT, chembl_black_box_warning TEXT)"
    )
    con.execute(
        "CREATE TABLE EDGE_MAPPING_TABLE (subject TEXT NOT NULL, predicate TEXT NOT NULL, object TEXT NOT NULL, id TEXT, category TEXT, qualifier TEXT, publications TEXT, sources TEXT, resource_id TEXT, resource_role TEXT, knowledge_level TEXT, agent_type TEXT, stage_qualifier TEXT, original_subject TEXT, original_object TEXT, extra_attributes TEXT)"
    )
    for nid in sorted(NODES):
        n = NODES[nid]
        con.execute(
            "INSERT INTO NODE_MAPPING_TABLE VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                nid,
                n["name"],
                json.dumps(n["categories"]),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        )
    seen_edges = set()
    for disease in XDTD_DISEASES:
        paths = _paths_to(disease, r)
        drugs = sorted({d for d, _ in paths} | {f"CHEBI:{i}" for i in (1, 4, 7)})
        for drug in drugs:
            tp = round(r.uniform(0.3, 0.99), 4)
            con.execute(
                "INSERT INTO PREDICTION_SCORE_TABLE VALUES (?,?,?,?,?,?,?)",
                (
                    drug,
                    NODES[drug]["name"],
                    disease,
                    NODES[disease]["name"],
                    round(1 - tp, 4),
                    tp,
                    0.0,
                ),
            )
        for drug, edges in paths:
            parts = [edges[0]["subject"]]
            for e in edges:
                parts += [e["predicate"], e["object"]]
            con.execute(
                "INSERT INTO PATH_RESULT_TABLE VALUES (?,?,?,?,?,?)",
                (
                    drug,
                    NODES[drug]["name"],
                    disease,
                    NODES[disease]["name"],
                    "->".join(parts),
                    round(r.uniform(0, 1), 4),
                ),
            )
            for e in edges:
                key = (e["subject"], e["predicate"], e["object"])
                if key in seen_edges:
                    continue
                seen_edges.add(key)
                sources = [
                    {
                        "resource_id": s["resource_id"],
                        "resource_role": s["resource_role"],
                        **(
                            {"upstream_resource_ids": s["upstream_resource_ids"]}
                            if s.get("upstream_resource_ids")
                            else {}
                        ),
                    }
                    for s in e["sources"]
                ]
                pubs = [f"PMID:{r.randint(1, 999)}" for _ in range(r.randint(0, 3))]
                con.execute(
                    "INSERT INTO EDGE_MAPPING_TABLE VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        e["subject"],
                        e["predicate"],
                        e["object"],
                        f"xdtd_edge_{len(seen_edges)}",
                        json.dumps(["biolink:Association"]),
                        None,
                        json.dumps(pubs) if pubs else None,
                        json.dumps(sources),
                        "|".join(s["resource_id"] for s in sources),
                        "|".join(s["resource_role"] for s in sources),
                        "knowledge_assertion",
                        "manual_agent",
                        None,
                        None,
                        None,
                        (
                            json.dumps({"object_aspect_qualifier": "activity"})
                            if len(seen_edges) % 3 == 0
                            else None
                        ),
                    ),
                )
    con.commit()
    con.close()


def write_all(curie_to_pmids, cohd, xdtd):
    write_curie_to_pmids(curie_to_pmids)
    write_cohd(cohd)
    write_xdtd(xdtd)

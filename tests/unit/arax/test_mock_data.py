"""The mock ARAX data files (shepherd_utils/arax_mock_data.py) are in the shape
ARAX's readers expect.

Each file is opened by the code that reads the real one: the port's overlays and
Infer (through ARAXQuery, in a subprocess, see mock_data/run_check.py), the
autocomplete, the arax_pathfinder repositories and xCRG's lookups.
"""

import json
import logging
import os
import pickle
import sqlite3
import subprocess
import sys

import pytest

from shepherd_utils import arax_mock_data as M
from shepherd_utils.config import settings
from shepherd_utils.data_download import (
    ARAX_AUTOCOMPLETE,
    ARAX_COHD,
    ARAX_CURIE_TO_PMIDS,
    ARAX_DB_NAMES,
    ARAX_EXPLAINABLE_DTD,
    ARAX_FDA_APPROVED_DRUGS,
    arax_db_filename,
    arax_db_path,
    arax_pathfinder_sqlite_paths,
)

HERE = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="module")
def dirs(tmp_path_factory):
    root = tmp_path_factory.mktemp("arax_mock_data")
    arax, pathfinder = str(root / "arax"), str(root / "pathfinder")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(settings, "arax_dbs_dir", arax)
        mp.setattr(settings, "arax_pathfinder_dbs_dir", pathfinder)
        assert M.main(["--pathfinder", "--no-network"]) == 0
        yield arax, pathfinder


@pytest.fixture
def paths(dirs, monkeypatch):
    arax, pathfinder = dirs
    monkeypatch.setattr(settings, "arax_dbs_dir", arax)
    monkeypatch.setattr(settings, "arax_pathfinder_dbs_dir", pathfinder)
    return dirs


def _rows(path, sql, *args):
    con = sqlite3.connect(path)
    try:
        return con.execute(sql, args).fetchall()
    finally:
        con.close()


def test_every_file_is_written_where_shepherd_looks(paths):
    for name in ARAX_DB_NAMES:
        assert os.path.getsize(arax_db_path(name)) > 0, name
    for path in arax_pathfinder_sqlite_paths():
        assert os.path.getsize(path) > 0, path


def test_existing_files_are_kept_unless_forced(paths, caplog):
    path = arax_db_path(ARAX_FDA_APPROVED_DRUGS)
    before = os.path.getmtime(path)
    assert M.main(["--no-network"]) == 1
    assert os.path.getmtime(path) == before
    assert "--force" in caplog.text


def test_fda_approved_drugs_is_a_set_of_drug_curies(paths):
    with open(arax_db_path(ARAX_FDA_APPROVED_DRUGS), "rb") as f:
        approved = pickle.load(f)
    assert isinstance(approved, set)
    assert "CHEBI:6801" in approved
    assert all(c.startswith("CHEBI:") for c in approved)


def test_curie_to_pmids_gives_finite_ngd_for_seed_edges(paths):
    rows = dict(
        _rows(
            arax_db_path(ARAX_CURIE_TO_PMIDS), "SELECT curie, pmids FROM curie_to_pmids"
        )
    )
    pmids = {curie: set(json.loads(v)) for curie, v in rows.items()}
    assert set(M.SEED_NODES) <= set(pmids)
    for subject, _, obj, _ in M.SEED_EDGES:
        assert pmids[subject] & pmids[obj], (subject, obj)
        assert M.ngd(pmids[subject], pmids[obj]) != float("inf")


def test_cohd_has_every_dataset(paths):
    path = arax_db_path(ARAX_COHD)
    datasets = {r[0] for r in _rows(path, "SELECT dataset_id FROM PATIENT_COUNT")}
    assert datasets == {1, 2, 3}
    for table in (
        "SINGLE_CONCEPT_COUNTS",
        "PAIRED_CONCEPT_COUNTS_ASSOCIATIONS",
        "CONCEPTS",
    ):
        assert _rows(path, f"SELECT COUNT(*) FROM {table}")[0][0] > 0, table


def test_xdtd_predictions_all_have_explanation_paths(paths):
    path = arax_db_path(ARAX_EXPLAINABLE_DTD)
    scored = set(_rows(path, "SELECT drug_id, disease_id FROM PREDICTION_SCORE_TABLE"))
    with_paths = set(_rows(path, "SELECT drug_id, disease_id FROM PATH_RESULT_TABLE"))
    assert ("CHEBI:6801", "MONDO:0005148") in scored
    assert scored == with_paths


def test_xdtd_readers(paths):
    from shepherd_utils.arax.Infer.scripts.ExplianableDTD_db import ExplainableDTD
    from shepherd_utils.arax.Infer.scripts.build_mapping_db import xDTDMappingDB

    path = arax_db_path(ARAX_EXPLAINABLE_DTD)
    xdtd = ExplainableDTD(
        database_name=os.path.basename(path), outdir=os.path.dirname(path)
    )
    scores = xdtd.get_score_table(disease_curie_ids="MONDO:0005148")
    assert "CHEBI:6801" in set(scores["drug_id"])
    top_paths = xdtd.get_top_path(disease_curie_ids="MONDO:0005148")
    assert ("CHEBI:6801", "MONDO:0005148") in top_paths
    xdtd.disconnect()

    mapping = xDTDMappingDB(
        database_name=os.path.basename(path), mode="run", db_loc=os.path.dirname(path)
    )
    assert mapping.get_node_info(node_id="CHEBI:6801").name == "metformin"
    edge = mapping.get_edge_info(
        subject="CHEBI:6801", predicate="biolink:treats", object_id="MONDO:0005148"
    )
    assert edge


def test_autocomplete_finds_seed_names(paths):
    from shepherd_utils.arax.autocomplete import rtxcomplete

    assert rtxcomplete.load()
    names = [match["name"] for match in rtxcomplete.get_nodes_like("metf", 10)]
    assert "metformin" in names


def test_pathfinder_repositories(paths):
    from pathfinder.core.repo.NGDRepository import NGDRepository
    from pathfinder.core.repo.NodeDegreeRepo import NodeDegreeRepo

    curie_ngd, tier0 = arax_pathfinder_sqlite_paths()
    ngd = NGDRepository(curie_ngd)
    neighbors = dict(ngd.get_curie_ngd("CHEBI:6801"))
    assert "MONDO:0005148" in neighbors
    assert 0 <= neighbors["MONDO:0005148"] < float("inf")
    lengths = dict(ngd.get_curies_pmid_length(["CHEBI:6801", "MONDO:0005148"]))
    assert lengths["CHEBI:6801"] > 0

    degrees = NodeDegreeRepo(tier0)
    assert degrees.get_node_degree("CHEBI:6801") > 0
    counts = degrees.get_degrees_by_node(["CHEBI:6801"])["CHEBI:6801"]
    assert counts["biolink:Disease"] >= 1


def test_fisher_exact_test_has_every_category(paths):
    """ComputeFTEST looks up the size of each neighbor's category and crashes
    when one is missing."""
    _, tier0 = arax_pathfinder_sqlite_paths()
    categories = {r[0] for r in _rows(tier0, "SELECT category FROM category_counts")}
    for (neighbor_counts,) in _rows(tier0, "SELECT neighbor_counts FROM neighbors"):
        assert set(json.loads(neighbor_counts)) <= categories


def test_xcrg_lookups(paths):
    import xcrg.runner as runner
    from xcrg import XCRGConfig

    curie_ngd, _ = arax_pathfinder_sqlite_paths()
    config = XCRGConfig(
        retriever_url="http://retriever.test",
        ngd_db_path=curie_ngd,
        curie_to_pmids_db_path=arax_db_path(ARAX_CURIE_TO_PMIDS),
    )
    logger = logging.getLogger("test")
    try:
        assert "NCBIGene:5468" in runner.get_ngd_neighbors("CHEBI:6801", config, logger)
        assert runner.get_curie_pmids("CHEBI:6801", config, logger)
    finally:
        for cache in ("_NGD_CONNECTIONS", "_NGD_NEIGHBOR_CACHE", "_PMID_CACHE"):
            getattr(runner, cache, {}).clear()


def test_from_trapi_adds_the_response_graph(tmp_path, monkeypatch):
    response = {
        "message": {
            "knowledge_graph": {
                "nodes": {
                    "CHEBI:6801": {
                        "name": "metformin",
                        "categories": ["biolink:SmallMolecule"],
                    },
                    "NCBIGene:9999": {
                        "name": "MOCKGENE",
                        "categories": ["biolink:Gene"],
                    },
                },
                "edges": {
                    "x": {
                        "subject": "CHEBI:6801",
                        "predicate": "biolink:affects",
                        "object": "NCBIGene:9999",
                        "sources": [
                            {
                                "resource_id": "infores:x",
                                "resource_role": "primary_knowledge_source",
                            }
                        ],
                    }
                },
            }
        }
    }
    trapi = tmp_path / "response.json"
    trapi.write_text(json.dumps(response))
    monkeypatch.setattr(settings, "arax_dbs_dir", str(tmp_path / "arax"))
    assert M.main(["--no-network", "--from-trapi", str(trapi)]) == 0
    names = {
        r[0] for r in _rows(arax_db_path(ARAX_AUTOCOMPLETE), "SELECT term FROM terms")
    }
    assert "MOCKGENE" in names
    assert "NCBIGene:9999" in dict(
        _rows(
            arax_db_path(ARAX_CURIE_TO_PMIDS), "SELECT curie, pmids FROM curie_to_pmids"
        )
    )


def test_filenames_match_the_download_settings():
    assert arax_db_filename(ARAX_COHD) == "COHDdatabase_v1.0_KG2.8.0.db"


@pytest.fixture(scope="module")
def araxquery_run(tmp_path_factory):
    out = tmp_path_factory.mktemp("mock_check") / "out.json"
    env = dict(os.environ, PYTHONHASHSEED="0")
    proc = subprocess.run(
        [sys.executable, os.path.join(HERE, "mock_data", "run_check.py"), str(out)],
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    return json.loads(out.read_text())


def test_overlays_read_the_mock_files(araxquery_run):
    overlays = araxquery_run["overlays"]
    assert overlays["status"] == "OK", overlays["problems"]
    assert overlays["problems"] == []
    n = overlays["n_input_edges"]
    # one virtual edge per drug-disease edge, each with a finite value
    assert len(overlays["ngd"]) == n
    assert all(
        v != "inf" and 0 <= float(v) < float("inf") for v in overlays["ngd"].values()
    )
    assert len(overlays["fisher"]) == n
    assert all(0 <= float(v) <= 1 for v in overlays["fisher"].values())
    # COHD decorates every drug x disease/phenotype pair it has counts for
    assert len(overlays["cohd"]) >= n


def test_infer_reads_the_mock_xdtd(araxquery_run):
    infer = araxquery_run["infer"]
    assert infer["status"] == "OK", infer["problems"]
    assert infer["n_results"] >= 2
    assert infer["n_aux_graphs"] >= 1
    assert "CHEBI:6801" in infer["predicted_drugs"]

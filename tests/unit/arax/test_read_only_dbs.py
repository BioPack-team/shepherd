"""Every reader of the ARAX databases opens them read-only.

In production the databases sit on one read-only volume shared by every arax
pod, and they are in WAL mode. A plain sqlite3.connect() -- or even mode=ro --
on such a file has to create a ``-shm`` file beside it before it can read, so
it fails with "unable to open database file". These tests turn each mock
database into a WAL file and check that its reader works without creating
anything next to it (that holds even when the tests run as root, where a
read-only directory would not stop the writes); when not root, the directory
is made read-only as well.
"""

import os
import sqlite3
import stat

import pytest

from shepherd_utils import arax_mock_data as M
from shepherd_utils.config import settings
from shepherd_utils.data_download import (
    ARAX_AUTOCOMPLETE,
    ARAX_COHD,
    ARAX_EXPLAINABLE_DTD,
    arax_biolink_cache_path,
    arax_cache_path,
    arax_db_path,
    arax_pathfinder_sqlite_paths,
)


def _to_wal(path):
    con = sqlite3.connect(path)
    try:
        assert con.execute("PRAGMA journal_mode = wal").fetchone() == ("wal",)
    finally:
        con.close()
    # closing the last connection checkpoints and removes the -wal/-shm files
    assert not os.path.exists(path + "-wal")
    assert not os.path.exists(path + "-shm")


@pytest.fixture(scope="module")
def read_only_dirs(tmp_path_factory):
    root = tmp_path_factory.mktemp("arax_read_only")
    arax, pathfinder = str(root / "arax"), str(root / "pathfinder")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(settings, "arax_dbs_dir", arax)
        mp.setattr(settings, "arax_pathfinder_dbs_dir", pathfinder)
        assert M.main(["--pathfinder", "--no-network"]) == 0
        for directory in (arax, pathfinder):
            for name in os.listdir(directory):
                path = os.path.join(directory, name)
                if name.endswith((".db", ".sqlite")):
                    _to_wal(path)
                os.chmod(path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
            os.chmod(directory, stat.S_IRUSR | stat.S_IXUSR)
    try:
        yield arax, pathfinder
    finally:
        for directory in (arax, pathfinder):
            os.chmod(directory, stat.S_IRWXU)


@pytest.fixture
def dirs(read_only_dirs, monkeypatch, tmp_path):
    arax, pathfinder = read_only_dirs
    monkeypatch.setattr(settings, "arax_dbs_dir", arax)
    monkeypatch.setattr(settings, "arax_pathfinder_dbs_dir", pathfinder)
    monkeypatch.setattr(settings, "arax_cache_dir", str(tmp_path / "cache"))
    monkeypatch.setattr(settings, "arax_biolink_cache_dir", "")
    yield arax, pathfinder
    for directory in (arax, pathfinder):
        assert not [
            name
            for name in os.listdir(directory)
            if name.endswith(("-shm", "-wal", "-journal"))
        ], f"a reader wrote beside the databases in {directory}"


def test_wal_database_in_a_read_only_directory_needs_the_read_only_open(dirs):
    """The failure from production: plain connect can't read it, the helper can."""
    from shepherd_utils.arax.util import connect_to_sqlite_read_only

    path = arax_db_path(ARAX_EXPLAINABLE_DTD)
    if os.geteuid() != 0:
        with pytest.raises(
            sqlite3.OperationalError,
            match="unable to open database file|attempt to write a readonly database",
        ):
            sqlite3.connect(path).execute(
                "SELECT 1 FROM PREDICTION_SCORE_TABLE"
            ).fetchone()
    con = connect_to_sqlite_read_only(path)
    try:
        assert (
            con.execute("SELECT count(*) FROM PREDICTION_SCORE_TABLE").fetchone()[0] > 0
        )
    finally:
        con.close()


def test_xdtd_readers(dirs):
    from shepherd_utils.arax.Infer.scripts.build_mapping_db import xDTDMappingDB
    from shepherd_utils.arax.Infer.scripts.ExplianableDTD_db import ExplainableDTD

    path = arax_db_path(ARAX_EXPLAINABLE_DTD)
    xdtd = ExplainableDTD(
        database_name=os.path.basename(path), outdir=os.path.dirname(path)
    )
    assert "CHEBI:6801" in set(
        xdtd.get_score_table(disease_curie_ids="MONDO:0005148")["drug_id"]
    )
    assert ("CHEBI:6801", "MONDO:0005148") in xdtd.get_top_path(
        disease_curie_ids="MONDO:0005148"
    )
    xdtd.disconnect()

    mapping = xDTDMappingDB(
        database_name=os.path.basename(path), mode="run", db_loc=os.path.dirname(path)
    )
    assert mapping.get_node_info(node_id="CHEBI:6801").name == "metformin"
    mapping.conn.close()


def test_cohd_index(dirs):
    from shepherd_utils.arax.KnowledgeSources.COHD_local.scripts.COHDIndex import (
        COHDIndex,
    )

    index = COHDIndex.__new__(
        COHDIndex
    )  # skip the NodeSynonymizer the constructor builds
    path = arax_db_path(ARAX_COHD)
    index.databaseLocation, index.databaseName = os.path.dirname(
        path
    ), os.path.basename(path)
    index.success_con = index.connect()
    assert (
        index.connection.execute("SELECT count(*) FROM sqlite_master").fetchone()[0] > 0
    )
    index.disconnect()


def test_fisher_exact_test_reads_the_tier0_database(dirs):
    from shepherd_utils.arax.util import connect_to_sqlite_read_only

    # ComputeFTEST opens self.sqlite_file_path through the helper; check the same file
    _, tier0 = arax_pathfinder_sqlite_paths()
    con = connect_to_sqlite_read_only(tier0)
    try:
        assert con.execute("SELECT count(*) FROM neighbors").fetchone()[0] > 0
    finally:
        con.close()


def test_autocomplete_keeps_its_cache_out_of_the_database_directory(dirs, tmp_path):
    from shepherd_utils.arax.autocomplete import rtxcomplete

    assert os.path.exists(arax_db_path(ARAX_AUTOCOMPLETE))
    os.makedirs(arax_cache_path())
    assert rtxcomplete.load()
    names = [match["name"] for match in rtxcomplete.get_nodes_like("metf", 10)]
    assert "metformin" in names
    assert os.path.exists(os.path.join(arax_cache_path(), "rtxcomplete_cache.sqlite"))


def test_pathfinder_repositories(dirs):
    from pathfinder.core.repo.NGDRepository import NGDRepository
    from pathfinder.core.repo.NodeDegreeRepo import NodeDegreeRepo

    from shepherd_utils.arax.util import (
        THIRD_PARTY_SQLITE_READERS,
        use_read_only_sqlite,
    )

    use_read_only_sqlite(*THIRD_PARTY_SQLITE_READERS)
    curie_ngd, tier0 = arax_pathfinder_sqlite_paths()
    assert "MONDO:0005148" in dict(NGDRepository(curie_ngd).get_curie_ngd("CHEBI:6801"))
    assert NodeDegreeRepo(tier0).get_node_degree("CHEBI:6801") > 0


def test_read_only_stand_in_accepts_uri_connects(dirs):
    """xcrg opens its databases as file:...?mode=ro URIs."""
    from shepherd_utils.arax.util import _READ_ONLY_SQLITE3

    curie_ngd, _ = arax_pathfinder_sqlite_paths()
    con = _READ_ONLY_SQLITE3.connect(
        f"file:{curie_ngd}?mode=ro", uri=True, check_same_thread=False
    )
    try:
        assert con.execute("SELECT count(*) FROM curie_ngd").fetchone()[0] > 0
    finally:
        con.close()
    assert _READ_ONLY_SQLITE3.Error is sqlite3.Error


def test_cache_dir_defaults_to_the_data_dir(monkeypatch):
    monkeypatch.setattr(settings, "arax_dbs_dir", "/data/arax_dbs")
    monkeypatch.setattr(settings, "arax_cache_dir", "")
    monkeypatch.setattr(settings, "arax_biolink_cache_dir", "")
    assert arax_cache_path() == "/data/arax_dbs"
    assert arax_biolink_cache_path() == "/data/arax_dbs/biolink"
    monkeypatch.setattr(settings, "arax_cache_dir", "/tmp/arax_cache")
    assert arax_cache_path() == "/tmp/arax_cache"
    assert arax_biolink_cache_path() == "/tmp/arax_cache/biolink"

"""Tests for ``shepherd_utils.data_download``.

Exercises the first-run LMDB download/extract helper without touching the
network by serving a locally-built ``.tar.gz`` over a ``file://`` URL.
"""

import logging
import tarfile

import pytest

from shepherd_utils import data_download
from shepherd_utils.data_download import (
    ensure_lmdb_dataset,
    ensure_omnicorp_lmdb,
    ensure_pathfinder_embeddings,
)

logger = logging.getLogger(__name__)


def _make_archive(tmp_path, names):
    """Build a .tar.gz containing ``names`` (each a small file) and return its
    ``file://`` URL."""
    payload_dir = tmp_path / "payload"
    payload_dir.mkdir()
    for name in names:
        (payload_dir / name).write_bytes(b"lmdb-bytes-for-" + name.encode())
    archive = tmp_path / "dataset.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for name in names:
            tar.add(payload_dir / name, arcname=name)
    return archive.as_uri()


def test_downloads_and_extracts_when_missing(tmp_path):
    url = _make_archive(tmp_path, ["curies.lmdb", "shared_counts.lmdb"])
    target = tmp_path / "omnicorp_lmdb"

    ensure_lmdb_dataset(
        name="test",
        target_dir=str(target),
        required_files=["curies.lmdb", "shared_counts.lmdb"],
        url=url,
        logger=logger,
    )

    assert (target / "curies.lmdb").read_bytes() == b"lmdb-bytes-for-curies.lmdb"
    assert (target / "shared_counts.lmdb").exists()
    # The temp archive is cleaned up -- only the extracted files remain.
    assert not any(p.name.endswith(".tar.gz") for p in target.iterdir())


def test_noop_when_already_present(tmp_path, mocker):
    target = tmp_path / "data"
    target.mkdir()
    (target / "data.mdb").write_bytes(b"already here")
    spy = mocker.patch.object(data_download, "_download")

    ensure_lmdb_dataset(
        name="test",
        target_dir=str(target),
        required_files=["data.mdb"],
        url="file:///should/not/be/used",
        logger=logger,
    )

    spy.assert_not_called()
    assert (target / "data.mdb").read_bytes() == b"already here"


def test_noop_and_warns_when_missing_without_url(tmp_path, mocker, caplog):
    target = tmp_path / "data"
    spy = mocker.patch.object(data_download, "_download")

    with caplog.at_level(logging.WARNING):
        ensure_lmdb_dataset(
            name="score_paths",
            target_dir=str(target),
            required_files=["data.mdb"],
            url="",
            logger=logger,
        )

    spy.assert_not_called()
    assert not target.exists()
    assert any("no download URL configured" in r.message for r in caplog.records)


def test_raises_when_archive_missing_expected_files(tmp_path):
    # Archive only ships one of the two required files.
    url = _make_archive(tmp_path, ["curies.lmdb"])
    target = tmp_path / "omnicorp_lmdb"

    with pytest.raises(RuntimeError, match="still missing expected files"):
        ensure_lmdb_dataset(
            name="test",
            target_dir=str(target),
            required_files=["curies.lmdb", "shared_counts.lmdb"],
            url=url,
            logger=logger,
        )


def test_rejects_path_traversal_member(tmp_path):
    # Craft an archive whose member escapes the target directory.
    archive = tmp_path / "evil.tar.gz"
    payload = tmp_path / "evil.txt"
    payload.write_bytes(b"pwned")
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(payload, arcname="../escaped.txt")
    target = tmp_path / "dest"

    with pytest.raises(RuntimeError, match="unsafe path"):
        ensure_lmdb_dataset(
            name="test",
            target_dir=str(target),
            required_files=["escaped.txt"],
            url=archive.as_uri(),
            logger=logger,
        )
    assert not (tmp_path / "escaped.txt").exists()


def test_ensure_omnicorp_lmdb_wires_settings(tmp_path, mocker):
    url = _make_archive(tmp_path, ["curies.lmdb", "shared_counts.lmdb"])
    target = tmp_path / "omnicorp_lmdb"
    mocker.patch.object(
        data_download.settings,
        "omnicorp_curies_lmdb_path",
        str(target / "curies.lmdb"),
    )
    mocker.patch.object(
        data_download.settings,
        "omnicorp_shared_counts_lmdb_path",
        str(target / "shared_counts.lmdb"),
    )
    mocker.patch.object(data_download.settings, "omnicorp_lmdb_url", url)

    ensure_omnicorp_lmdb(logger)

    assert (target / "curies.lmdb").exists()
    assert (target / "shared_counts.lmdb").exists()


def test_ensure_pathfinder_embeddings_wires_settings(tmp_path, mocker):
    url = _make_archive(tmp_path, ["data.mdb", "lock.mdb"])
    target = tmp_path / "pathfinder_embeddings"
    mocker.patch.object(
        data_download.settings, "pathfinder_embeddings_dir", str(target)
    )
    mocker.patch.object(data_download.settings, "pathfinder_embeddings_url", url)

    ensure_pathfinder_embeddings(logger)

    assert (target / "data.mdb").exists()
    assert (target / "lock.mdb").exists()

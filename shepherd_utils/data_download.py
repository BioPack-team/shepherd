"""Ensure large read-only LMDB datasets are present, downloading them on first
run so new developers can spin the stack up locally.

The ``aragorn_omnicorp`` and ``score_paths`` workers read from LMDB datasets
that are far too large to commit to git -- they're gitignored and volume-mounted
from the host (``./omnicorp_lmdb`` and ``./pathfinder_embeddings``). In
production these volumes are provisioned out of band, but a developer running
``docker compose up`` for the first time has empty directories, and the workers
crash on startup trying to open a missing LMDB.

When a download URL is configured (``OMNICORP_LMDB_URL`` /
``PATHFINDER_EMBEDDINGS_URL``, read via :mod:`shepherd_utils.config`), each of
those workers calls the matching ``ensure_*`` helper at startup:

* if the expected files are already present it's a no-op;
* otherwise the dataset is fetched as a ``.tar.gz`` from the external server and
  extracted into the (volume-mounted) target directory, so it persists on the
  host and is only downloaded once.

With no URL configured the call is a no-op that logs how to enable the download,
so production -- where the data is already mounted -- is unaffected.
"""

import logging
import os
import tarfile
import tempfile
import urllib.request
from typing import List, Optional

from shepherd_utils.config import settings


def _missing_files(target_dir: str, required_files: List[str]) -> List[str]:
    """Return the required files that are not present under ``target_dir``."""
    return [
        name
        for name in required_files
        if not os.path.exists(os.path.join(target_dir, name))
    ]


def _download(url: str, dest_path: str, logger: logging.Logger) -> None:
    """Stream ``url`` to ``dest_path``, logging progress periodically."""
    logger.info(f"Downloading dataset from {url} ...")
    # nosec B310: the URL is operator-configured (an env var), not user input.
    with urllib.request.urlopen(url) as resp:  # noqa: S310
        header = resp.headers.get("Content-Length")
        total = int(header) if header and header.isdigit() else None
        read = 0
        step = 50 * 1024 * 1024  # log roughly every 50 MB
        next_log = step
        with open(dest_path, "wb") as out:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                read += len(chunk)
                if read >= next_log:
                    if total:
                        logger.info(
                            f"  ... {read / 1e6:.0f}/{total / 1e6:.0f} MB "
                            f"({100 * read / total:.0f}%)"
                        )
                    else:
                        logger.info(f"  ... {read / 1e6:.0f} MB")
                    next_log += step
    logger.info(f"Download complete: {read / 1e6:.0f} MB")


def _safe_extract(tar: tarfile.TarFile, dest_dir: str) -> None:
    """Extract ``tar`` into ``dest_dir``, refusing any member that would land
    outside it (path traversal via absolute paths or ``..``)."""
    dest_root = os.path.realpath(dest_dir)
    for member in tar.getmembers():
        member_path = os.path.realpath(os.path.join(dest_dir, member.name))
        if member_path != dest_root and not member_path.startswith(dest_root + os.sep):
            raise RuntimeError(
                f"Refusing to extract unsafe path {member.name!r} from archive"
            )
    # ``filter="data"`` is the safe extraction policy (rejects absolute paths,
    # ``..``, and links escaping the tree) that becomes the default in Python
    # 3.14; setting it explicitly keeps behavior stable and silences the
    # transitional deprecation warning.
    tar.extractall(dest_dir, filter="data")


def ensure_lmdb_dataset(
    name: str,
    target_dir: str,
    required_files: List[str],
    url: str,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Ensure ``required_files`` exist under ``target_dir``.

    If any are missing and ``url`` is set, download the ``.tar.gz`` at ``url``
    and extract it into ``target_dir``. If ``url`` is empty the call only warns
    (the worker will fail to open the LMDB, exactly as it did before) so
    production deployments, where the data is mounted out of band, are a no-op.

    Idempotent: once the files are present this returns immediately, so it's safe
    to call unconditionally on every worker startup.
    """
    logger = logger or logging.getLogger(__name__)

    missing = _missing_files(target_dir, required_files)
    if not missing:
        logger.info(
            f"{name}: dataset already present in {target_dir}; skipping download."
        )
        return

    if not url:
        logger.warning(
            f"{name}: dataset missing from {target_dir} (missing: {missing}) and no "
            f"download URL configured. Set the corresponding *_URL env var (see the "
            f"README) to download it automatically, or provide the files manually. "
            f"The worker will fail to start without them."
        )
        return

    os.makedirs(target_dir, exist_ok=True)
    logger.info(
        f"{name}: dataset missing from {target_dir} (missing: {missing}); "
        f"downloading from external server so the worker can start."
    )

    # Download to a temp file inside target_dir so a partial/interrupted download
    # is never mistaken for a complete dataset and the extract lands on the same
    # (volume-mounted) filesystem.
    tmp_fd, tmp_archive = tempfile.mkstemp(suffix=".tar.gz", dir=target_dir)
    os.close(tmp_fd)
    try:
        _download(url, tmp_archive, logger)
        logger.info(f"{name}: extracting archive into {target_dir} ...")
        with tarfile.open(tmp_archive, "r:*") as tar:
            _safe_extract(tar, target_dir)
    finally:
        try:
            os.remove(tmp_archive)
        except OSError:
            pass

    still_missing = _missing_files(target_dir, required_files)
    if still_missing:
        raise RuntimeError(
            f"{name}: downloaded and extracted {url} but still missing expected "
            f"files: {still_missing}. Check the archive's contents / layout -- it "
            f"should contain {required_files} at its top level."
        )
    logger.info(f"{name}: dataset ready in {target_dir}.")


def ensure_omnicorp_lmdb(logger: Optional[logging.Logger] = None) -> None:
    """Ensure the omnicorp curies + shared-counts LMDBs are present.

    Both are single-file LMDBs (``subdir=False``) living side by side in the
    directory holding ``omnicorp_curies_lmdb_path``.
    """
    curies = settings.omnicorp_curies_lmdb_path
    shared_counts = settings.omnicorp_shared_counts_lmdb_path
    target_dir = os.path.dirname(curies)
    ensure_lmdb_dataset(
        name="aragorn_omnicorp",
        target_dir=target_dir,
        required_files=[
            os.path.basename(curies),
            os.path.basename(shared_counts),
        ],
        url=settings.omnicorp_lmdb_url,
        logger=logger,
    )


def ensure_pathfinder_embeddings(logger: Optional[logging.Logger] = None) -> None:
    """Ensure the score_paths embeddings LMDB is present.

    This is a directory-style LMDB (``subdir=True``); ``data.mdb`` is the file
    that must exist for the environment to open.
    """
    ensure_lmdb_dataset(
        name="score_paths",
        target_dir=settings.pathfinder_embeddings_dir,
        required_files=["data.mdb"],
        url=settings.pathfinder_embeddings_url,
        logger=logger,
    )

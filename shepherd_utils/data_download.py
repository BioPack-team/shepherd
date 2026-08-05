"""Ensure large read-only datasets are present, downloading them on first
run so new developers can spin the stack up locally.

Several workers read from datasets that are far too large to commit to git --
they're gitignored and volume-mounted from the host (``./omnicorp_lmdb``,
``./pathfinder_embeddings``, ``./arax_pathfinder_dbs``). In production these
volumes are provisioned out of band, but a developer running
``docker compose up`` for the first time has empty directories, and the
workers crash on startup trying to open missing files.

Two flavors of HTTP source are supported:

* **Archive** -- a single ``.tar.gz`` fetched via ``urllib`` and extracted in
  place (``OMNICORP_LMDB_URL`` / ``PATHFINDER_EMBEDDINGS_URL``, used by
  ``aragorn_omnicorp`` and ``score_paths`` below).
* **Per-file** -- individual files fetched directly, no archive/extract step
  (``arax_pathfinder`` below, whose two sqlite databases are served as plain
  files rather than bundled into one archive).

When a download source is configured (read via :mod:`shepherd_utils.config`),
each worker calls its matching ``ensure_*`` helper at startup:

* if the expected files are already present it's a no-op;
* otherwise the dataset is fetched and written into the (volume-mounted)
  target directory, so it persists on the host and is only downloaded once.

With no source configured the call is a no-op that logs how to enable the
download, so production -- where the data is already mounted -- is
unaffected.
"""

import logging
import os
import tarfile
import tempfile
import urllib.error
import urllib.request
from typing import Dict, List, Optional, Tuple

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
    try:
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
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"download failed for {url}: HTTP {e.code} {e.reason}. Confirm the "
            f"URL is correct and reachable."
        ) from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"download failed for {url}: {e.reason}. Confirm the URL is correct "
            f"and reachable from inside the container."
        ) from e
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


def ensure_http_files_dataset(
    name: str,
    target_dir: str,
    file_sources: Dict[str, str],
    logger: Optional[logging.Logger] = None,
) -> None:
    """Ensure each file in ``file_sources`` exists under ``target_dir``,
    fetching any missing ones directly over HTTP(S) -- one URL per file, no
    archive/extract step (contrast with ``ensure_lmdb_dataset``'s single
    ``.tar.gz``).

    ``file_sources`` maps the expected local filename to its URL; a filename
    whose URL is empty is skipped (warned about) rather than downloaded, same
    as an unset ``url`` in ``ensure_lmdb_dataset``.

    Idempotent: once a file is present it's left alone, so it's safe to call
    unconditionally on every worker startup.
    """
    logger = logger or logging.getLogger(__name__)
    required_files = list(file_sources.keys())

    missing = _missing_files(target_dir, required_files)
    if not missing:
        logger.info(
            f"{name}: dataset already present in {target_dir}; skipping download."
        )
        return

    os.makedirs(target_dir, exist_ok=True)
    logger.info(f"{name}: dataset missing from {target_dir} (missing: {missing}).")

    attempted = []
    for filename in missing:
        url = file_sources.get(filename)
        if not url:
            logger.warning(
                f"{name}: {filename} missing from {target_dir} and no URL "
                f"configured for it. Set the corresponding *_URL env var (see "
                f"the README) to download it automatically, or provide the file "
                f"manually. The worker will fail to start without it."
            )
            continue
        attempted.append(filename)

        dest_path = os.path.join(target_dir, filename)
        # Download to a temp file in the same dir first, then atomically rename,
        # so a partial/interrupted transfer is never mistaken for a complete
        # file (same reasoning as the tar.gz download above).
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".part", dir=target_dir)
        os.close(tmp_fd)
        try:
            _download(url, tmp_path, logger)
            os.replace(tmp_path, dest_path)
        except Exception:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise

    # Only files we actually attempted (had a URL) count toward failure -- a
    # file with no URL configured was already warned about above and is
    # expected to still be missing, same as an unset url in
    # ensure_lmdb_dataset. Checking against `required_files` here would raise
    # even when nothing went wrong.
    still_missing = _missing_files(target_dir, attempted)
    if still_missing:
        raise RuntimeError(
            f"{name}: still missing expected files after download attempt: "
            f"{still_missing}."
        )
    if _missing_files(target_dir, required_files):
        logger.warning(
            f"{name}: dataset partially ready in {target_dir} -- some files have "
            f"no URL configured (see warnings above). The worker will fail "
            f"when it tries to open them."
        )
    else:
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


def arax_pathfinder_sqlite_paths() -> Tuple[str, str]:
    """Return ``(curie_ngd_path, node_degree_path)`` for the arax_pathfinder
    sqlite databases, built from ``arax_pathfinder_dbs_dir`` + the filename
    templates + the current ``arax_pathfinder_tier_version``.

    Single source of truth for these two paths: ``ensure_arax_pathfinder_dbs``
    (below) uses it to know what to download and where, and worker.py's
    ``execute_pathfinding_sync`` uses it to know what to open, so the two can
    never disagree about a file's location the way two independently-defined
    settings could.
    """
    version = settings.arax_pathfinder_tier_version
    curie_ngd_path = os.path.join(
        settings.arax_pathfinder_dbs_dir,
        settings.arax_pathfinder_curie_ngd_sqlite_filename.format(version=version),
    )
    node_degree_path = os.path.join(
        settings.arax_pathfinder_dbs_dir,
        settings.arax_pathfinder_tier0_overlay_sqlite_filename.format(version=version),
    )
    return curie_ngd_path, node_degree_path


def ensure_arax_pathfinder_dbs(logger: Optional[logging.Logger] = None) -> None:
    """Ensure the arax_pathfinder worker's two sqlite databases are present.

    Both are served as plain files over HTTPS, so each is fetched individually
    with a normal GET -- no archive/extract step. Both are expected in the
    same directory (see the ``arax_pathfinder`` volume mount in
    docker-compose.yml).

    The version tag shows up in the filename (e.g.
    ``curie_ngd_v1.0_tier0-20260621.sqlite``) but not in the URL path -- the
    ``tier0`` segment in ``arax_pathfinder_sqlite_base_url`` is fixed, not the
    tier version. Only the filename templates are filled in from
    ``arax_pathfinder_tier_version``. Bumping to a new tier is one env var
    change (``ARAX_PATHFINDER_TIER_VERSION``).
    """
    curie_ngd_path, node_degree_path = arax_pathfinder_sqlite_paths()
    target_dir = settings.arax_pathfinder_dbs_dir
    base_url = settings.arax_pathfinder_sqlite_base_url

    curie_ngd_filename = os.path.basename(curie_ngd_path)
    node_degree_filename = os.path.basename(node_degree_path)

    ensure_http_files_dataset(
        name="arax_pathfinder",
        target_dir=target_dir,
        file_sources={
            curie_ngd_filename: f"{base_url}/{curie_ngd_filename}",
            node_degree_filename: f"{base_url}/{node_degree_filename}",
        },
        logger=logger,
    )

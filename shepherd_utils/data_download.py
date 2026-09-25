"""Ensure large read-only datasets are present, downloading them on first
run so new developers can spin the stack up locally.

Several workers read from datasets that are far too large to commit to git --
they're gitignored and volume-mounted from the host (``./omnicorp_lmdb``,
``./pathfinder_embeddings``, ``./arax_pathfinder_dbs``, ``./arax_dbs``). In
production these volumes are provisioned out of band, but a developer running
``docker compose up`` for the first time has empty directories, and the
workers crash on startup trying to open missing files.

Two flavors of HTTP source are supported:

* **Archive** -- a single ``.tar.gz`` fetched via ``urllib`` and extracted in
  place (``OMNICORP_LMDB_URL`` / ``PATHFINDER_EMBEDDINGS_URL``, used by
  ``aragorn_omnicorp`` and ``score_paths`` below).
* **Per-file** -- individual files fetched directly, no archive/extract step
  (``arax_pathfinder`` below, whose two sqlite databases are served as plain
  files rather than bundled into one archive, plus the ARAX blocked-concept
  list that shares their directory; and the ARAX port's other data files via
  ``ensure_arax_dbs``).

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
from typing import Dict, Iterable, List, Optional, Tuple

from shepherd_utils.config import settings


def _missing_files(target_dir: str, required_files: List[str]) -> List[str]:
    """Return the required files that are not present under ``target_dir``."""
    return [
        name
        for name in required_files
        if not os.path.exists(os.path.join(target_dir, name))
    ]


def _download(url: str, dest_path: str, logger: logging.Logger) -> None:
    """Stream ``url`` to ``dest_path``, logging progress periodically.

    Bounded by ``dataset_download_timeout_sec`` (see config). The timeout is
    per socket operation rather than for the whole transfer, so a multi-GB file
    downloads for as long as it needs while a stalled connection raises instead
    of hanging worker startup indefinitely.
    """
    logger.info(f"Downloading dataset from {url} ...")
    timeout = float(settings.dataset_download_timeout_sec)
    # urlopen treats timeout=None as "no timeout"; 0 is the documented opt-out.
    kwargs = {"timeout": timeout} if timeout > 0 else {}
    # Hoisted so the timeout handler below can report how far the transfer got
    # even when it never made it past opening the connection.
    read = 0
    try:
        # nosec B310: the URL is operator-configured (an env var), not user input.
        with urllib.request.urlopen(url, **kwargs) as resp:  # noqa: S310
            header = resp.headers.get("Content-Length")
            total = int(header) if header and header.isdigit() else None
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
        # A connect timeout arrives here, wrapped, with e.reason set to the
        # underlying socket.timeout.
        raise RuntimeError(
            f"download failed for {url}: {e.reason}. Confirm the URL is correct "
            f"and reachable from inside the container."
        ) from e
    except TimeoutError as e:
        # A stall part-way through the body raises straight out of resp.read()
        # rather than being wrapped in URLError, so it needs its own handler --
        # otherwise this surfaces as a bare TimeoutError with no context.
        raise RuntimeError(
            f"download failed for {url}: no data for {timeout:.0f}s "
            f"({read / 1e6:.0f} MB transferred). The host is reachable but the "
            f"transfer stalled -- check for an egress proxy or a rate limit, or "
            f"raise DATASET_DOWNLOAD_TIMEOUT_SEC."
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
    logger = logger or logging.getLogger("shepherd.data_download")

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
    logger = logger or logging.getLogger("shepherd.data_download")
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


# The ARAX blocked-concept list lives alongside the pathfinder sqlite databases
# so it lands on the same mounted volume. It previously went to the worker's
# working directory (``/app``), which is the container's writable layer and
# therefore thrown away on every restart -- meaning each new pod re-fetched it
# from GitHub during startup, before the poll loop, on a code path whose logs
# were being discarded. On the volume it is fetched once and persists.
ARAX_BLOCKED_LIST_FILENAME = "general_concepts.json"


def arax_blocked_list_path() -> str:
    """Return the on-disk path of the ARAX blocked-concept list.

    Single source of truth, in the same spirit as
    ``arax_pathfinder_sqlite_paths``: ``ensure_arax_blocked_list`` uses it to
    know where to download, and worker.py uses it to know what to open.
    """
    return os.path.join(settings.arax_pathfinder_dbs_dir, ARAX_BLOCKED_LIST_FILENAME)


def ensure_arax_blocked_list(logger: Optional[logging.Logger] = None) -> None:
    """Ensure the ARAX blocked-concept list is present next to the sqlite dbs.

    Fetched via the shared downloader so it lands through a temp file + atomic
    rename (a direct write let concurrent tasks race on a half-written file)
    and inherits the download timeout. Idempotent, so it is safe to call at
    startup and again lazily from a pool child.

    Note the flip side of persisting this on the volume: it is now only fetched
    when absent, so a refreshed upstream list is not picked up until the file is
    deleted. Delete it from the volume to force a re-fetch on the next restart.
    """
    ensure_http_files_dataset(
        name="arax_blocked_list",
        # ``or "."`` so an unset/blank dbs dir degrades to the working directory
        # rather than handing makedirs an empty path.
        target_dir=os.path.dirname(arax_blocked_list_path()) or ".",
        file_sources={ARAX_BLOCKED_LIST_FILENAME: settings.arax_blocked_list_url},
        logger=logger,
    )


# --- ARAX port data files ----------------------------------------------------
#
# The data files the ARAX port's workers read (Overlay, Infer, Expand, and the
# UI-facing API), set up exactly like the pathfinder DBs: plain files over
# HTTPS, one volume-mounted directory, fetched on first startup. Each worker
# asks only for the files it opens, via the names below.

ARAX_CURIE_TO_PMIDS = "curie_to_pmids"  # NGD overlay, add_node_pmids, xCRG
ARAX_EXPLAINABLE_DTD = "explainable_dtd"  # Infer (xDTD) scores, paths, mappings
ARAX_AUTOCOMPLETE = "autocomplete"  # UI node-name autocomplete
ARAX_FDA_APPROVED_DRUGS = "fda_approved_drugs"  # Expand's FDA-approval constraint
ARAX_COHD = "cohd"  # overlay_clinical_info

# name -> (filename-template setting, per-file URL override setting)
_ARAX_DB_SETTINGS = {
    ARAX_CURIE_TO_PMIDS: (
        "arax_curie_to_pmids_sqlite_filename",
        "arax_curie_to_pmids_url",
    ),
    ARAX_EXPLAINABLE_DTD: (
        "arax_explainable_dtd_db_filename",
        "arax_explainable_dtd_url",
    ),
    ARAX_AUTOCOMPLETE: ("arax_autocomplete_sqlite_filename", "arax_autocomplete_url"),
    ARAX_FDA_APPROVED_DRUGS: (
        "arax_fda_approved_drugs_filename",
        "arax_fda_approved_drugs_url",
    ),
    ARAX_COHD: ("arax_cohd_db_filename", "arax_cohd_url"),
}
ARAX_DB_NAMES = tuple(_ARAX_DB_SETTINGS)


def _arax_db_setting_names(name: str) -> Tuple[str, str]:
    try:
        return _ARAX_DB_SETTINGS[name]
    except KeyError:
        raise ValueError(
            f"Unknown ARAX data file {name!r}; expected one of {list(ARAX_DB_NAMES)}"
        ) from None


def arax_db_filename(name: str) -> str:
    """The on-disk filename for ARAX data file ``name``, with ``{version}``
    filled from ``arax_tier_version`` (templates without it are fixed names,
    e.g. the KG2.8.0 COHD build)."""
    filename_setting, _ = _arax_db_setting_names(name)
    return getattr(settings, filename_setting).format(
        version=settings.arax_tier_version
    )


def arax_db_path(name: str) -> str:
    """Return the on-disk path of ARAX data file ``name``.

    Single source of truth, as ``arax_pathfinder_sqlite_paths`` is for the
    pathfinder DBs: ``ensure_arax_dbs`` uses it to know where to download, and
    the workers use it to know what to open, so the two can never disagree.
    """
    return os.path.join(settings.arax_dbs_dir, arax_db_filename(name))


def arax_biolink_cache_path() -> str:
    """Where ARAX's BiolinkHelper caches the Biolink model and lookup map:
    ``arax_biolink_cache_dir`` when set, else ``{arax_dbs_dir}/biolink``."""
    return settings.arax_biolink_cache_dir or os.path.join(
        settings.arax_dbs_dir, "biolink"
    )


def arax_db_url(name: str) -> str:
    """Where ARAX data file ``name`` is downloaded from: its ``*_url`` setting
    when set, otherwise ``{arax_dbs_base_url}/{filename}``."""
    _, url_setting = _arax_db_setting_names(name)
    override = getattr(settings, url_setting)
    if override:
        return override
    return f"{settings.arax_dbs_base_url.rstrip('/')}/{arax_db_filename(name)}"


def ensure_arax_dbs(
    names: Iterable[str], logger: Optional[logging.Logger] = None
) -> None:
    """Ensure the named ARAX data files are present in ``arax_dbs_dir``.

    Same mechanism and caveats as ``ensure_arax_pathfinder_dbs``: files that
    are already present are left alone, missing ones are fetched one by one
    (temp file + atomic rename), and the presence check is an exact match on
    the directory and the (tier-versioned) filenames. Pass only the files the
    calling worker opens, e.g. ``ensure_arax_dbs([ARAX_CURIE_TO_PMIDS,
    ARAX_COHD])`` for the overlay worker.
    """
    names = list(dict.fromkeys(names))  # de-duplicate, keep order
    for name in names:
        _arax_db_setting_names(name)  # fail fast on an unknown name
    if not names:
        return
    ensure_http_files_dataset(
        name="arax_dbs",
        target_dir=settings.arax_dbs_dir or ".",
        file_sources={arax_db_filename(n): arax_db_url(n) for n in names},
        logger=logger,
    )

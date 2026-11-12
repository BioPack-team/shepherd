"""The cold store: durable payloads outside Redis (and outside Postgres).

Redis holds a payload while it is hot. What has to outlive that is kept
here, as one object per message holding the same zstd frame Redis holds:

* ``ARS`` -- an ARS message's durable copy, written when the message goes
  terminal. Upstream keeps that copy in MySQL; Shepherd used to keep it in
  ``ars_message.data``. A single stress run of large merged results filled
  a 5GB Postgres volume, and freeing space inside Postgres doesn't return
  it to the disk.
* ``ARA`` -- the archive of a finished ARA query: its response exactly as
  the pipeline finished it, with its query and logs, written by
  ``finish_query`` for every query, whether it came in directly or from the
  ARS (shepherd_utils/response_archive.py).

Two backends, chosen by configuration:

* a directory (``ars_payload_dir``): one file per object. It must be shared
  by every process that reads or writes payloads (the server, finish_query
  and the ars_fanout / ars_premerge / ars_merge / ars_watchdog workers), so
  in Kubernetes it is a ReadWriteMany volume. Files are written to a
  temporary name in the same directory and renamed into place, so a reader
  never sees a half-written payload. Deleting a file frees its space at
  once, which is what lets the watchdog evict by disk usage
  (``shepherd_utils.ars.retention``).
* an S3 bucket (``payload_s3_bucket``): one object per payload, under the
  same relative key the directory backend uses as a path -- so a directory
  can be copied into a bucket as it is. A bucket has no capacity, so the
  disk-pressure eviction never runs against it.

With both set, the bucket is used. With neither, nothing here is used: ARS
payloads stay in Postgres as before and ARA responses are not archived.

Everything here blocks (disk or network I/O): call it through
``asyncio.to_thread``.
"""

import os
import threading
import uuid
from typing import Iterator, NamedTuple, Optional, Tuple, Union

from shepherd_utils.config import settings

MessageId = Union[str, uuid.UUID]

# Payload classes. A class's value is the directory (key prefix) its objects
# live under; ARS messages keep the root, where they have always been.
ARS = ""
ARA = "ara"

FILE = "file"
S3 = "s3"

SUFFIX = ".zst"


class Entry(NamedTuple):
    """One stored object, as ``entries`` lists it."""

    message_id: str
    size: int
    modified: float  # seconds since the epoch


def backend() -> Optional[str]:
    """Which backend is configured: ``S3``, ``FILE`` or None."""
    if settings.payload_s3_bucket:
        return S3
    if settings.ars_payload_dir:
        return FILE
    return None


def enabled() -> bool:
    return backend() is not None


def bounded() -> bool:
    """True when the store sits on a volume that can fill (``usage`` means
    something and eviction under disk pressure applies)."""
    return backend() == FILE


def relative_key(message_id: MessageId, kind: str = ARS) -> str:
    """``[<kind>/]<first two hex digits>/<message id>.zst``, the object's
    place in either backend. The fan-out keeps any one directory small;
    message ids are UUIDs, so this is even.

    Raises ValueError for an id that is not a UUID. Parsing it is also what
    keeps a caller-supplied id from naming anything outside the store."""
    name = str(uuid.UUID(str(message_id)))
    parts = [kind] if kind else []
    return "/".join(parts + [name[:2], f"{name}{SUFFIX}"])


def path_for(message_id: MessageId, kind: str = ARS) -> str:
    """The object's file under ``ars_payload_dir`` (directory backend)."""
    return os.path.join(
        settings.ars_payload_dir, *relative_key(message_id, kind).split("/")
    )


# --- S3 ---------------------------------------------------------------------

_s3_client = None
_s3_client_lock = threading.Lock()


def _s3():
    """This process's S3 client (boto3 clients are thread-safe). Credentials
    and region come from the standard AWS chain: the pod's IAM role in
    production, environment variables against MinIO."""
    global _s3_client
    if _s3_client is None:
        with _s3_client_lock:
            if _s3_client is None:
                import boto3
                from botocore.config import Config

                _s3_client = boto3.client(
                    "s3",
                    endpoint_url=settings.payload_s3_endpoint_url or None,
                    # Bounded: callers run this in a thread they cannot
                    # cancel, so a hung request has to give up on its own.
                    config=Config(
                        retries={"max_attempts": 3, "mode": "standard"},
                        connect_timeout=5,
                        read_timeout=30,
                    ),
                )
    return _s3_client


def _s3_key(message_id: MessageId, kind: str) -> str:
    prefix = settings.payload_s3_prefix.strip("/")
    key = relative_key(message_id, kind)
    return f"{prefix}/{key}" if prefix else key


def _s3_missing(error: Exception) -> bool:
    """True for the errors S3 answers a missing object with."""
    response = getattr(error, "response", None) or {}
    code = str((response.get("Error") or {}).get("Code", ""))
    return code in ("NoSuchKey", "404", "NotFound")


# --- Operations -------------------------------------------------------------


def write(message_id: MessageId, blob: bytes, kind: str = ARS) -> int:
    """Store ``blob`` durably, replacing any previous copy. Returns its size."""
    if backend() == S3:
        _s3().put_object(
            Bucket=settings.payload_s3_bucket,
            Key=_s3_key(message_id, kind),
            Body=blob,
        )
        return len(blob)
    path = path_for(message_id, kind)
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    tmp = os.path.join(directory, f".{os.path.basename(path)}.{uuid.uuid4().hex}.tmp")
    try:
        with open(tmp, "wb") as f:
            f.write(blob)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return len(blob)


def read(message_id: MessageId, kind: str = ARS) -> Optional[bytes]:
    """The stored blob, or None when this message has no object."""
    if backend() == S3:
        try:
            response = _s3().get_object(
                Bucket=settings.payload_s3_bucket, Key=_s3_key(message_id, kind)
            )
        except Exception as e:
            if _s3_missing(e):
                return None
            raise
        return response["Body"].read()
    try:
        with open(path_for(message_id, kind), "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None


def exists(message_id: MessageId, kind: str = ARS) -> bool:
    if backend() == S3:
        try:
            _s3().head_object(
                Bucket=settings.payload_s3_bucket, Key=_s3_key(message_id, kind)
            )
        except Exception as e:
            if _s3_missing(e):
                return False
            raise
        return True
    return os.path.exists(path_for(message_id, kind))


def delete(message_id: MessageId, kind: str = ARS) -> None:
    """Remove a message's object; a missing one is fine."""
    if backend() == S3:
        _s3().delete_object(
            Bucket=settings.payload_s3_bucket, Key=_s3_key(message_id, kind)
        )
        return
    try:
        os.unlink(path_for(message_id, kind))
    except FileNotFoundError:
        pass


def entries(kind: str) -> Iterator[Entry]:
    """Every object of one class, in no particular order, for the retention
    sweeps. Only for the classes with a directory of their own: ARS messages
    are tracked by their rows, not by listing the store."""
    if not kind:
        raise ValueError("entries() needs a payload class with its own prefix")
    if backend() == S3:
        prefix = settings.payload_s3_prefix.strip("/")
        prefix = f"{prefix}/{kind}/" if prefix else f"{kind}/"
        paginator = _s3().get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=settings.payload_s3_bucket, Prefix=prefix
        ):
            for obj in page.get("Contents") or []:
                name = obj["Key"].rsplit("/", 1)[-1]
                if name.endswith(SUFFIX):
                    yield Entry(
                        name[: -len(SUFFIX)],
                        int(obj["Size"]),
                        obj["LastModified"].timestamp(),
                    )
        return
    root = os.path.join(settings.ars_payload_dir, kind)
    try:
        fanout = list(os.scandir(root))
    except FileNotFoundError:
        return
    for directory in fanout:
        if not directory.is_dir(follow_symlinks=False):
            continue
        with os.scandir(directory.path) as files:
            for item in files:
                # temporary files of a write in progress start with a dot
                if item.name.startswith(".") or not item.name.endswith(SUFFIX):
                    continue
                try:
                    st = item.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue  # deleted while we were listing
                yield Entry(item.name[: -len(SUFFIX)], st.st_size, st.st_mtime)


def usage() -> Tuple[int, int]:
    """(used bytes, total bytes) of the filesystem holding the payloads.
    Only meaningful for a ``bounded`` store."""
    st = os.statvfs(settings.ars_payload_dir)
    total = st.f_blocks * st.f_frsize
    free = st.f_bavail * st.f_frsize
    return total - free, total

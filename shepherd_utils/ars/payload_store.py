"""Durable ARS payloads on a filesystem volume instead of in Postgres.

Every ARS message payload lives in Redis while it is hot. Once a message goes
terminal, a durable copy is kept so clients can still read it after the Redis
TTL. Upstream keeps that copy in MySQL; Shepherd used to keep it in
``ars_message.data``. A single stress run of large merged results filled a
5GB Postgres volume, and freeing space inside Postgres doesn't return it to
the disk, so when ``ars_payload_dir`` is set the copies go here instead: one
zstd file per message, the same frame Redis holds. Deleting a file frees its
space at once, which is what lets the watchdog evict by disk usage
(``shepherd_utils.ars.retention``).

The directory must be shared by every process that reads or writes payloads
(the server and the ars_fanout / ars_premerge / ars_merge / ars_watchdog
workers), so in Kubernetes it is a ReadWriteMany volume.

Files are written to a temporary name in the same directory and renamed into
place, so a reader never sees a half-written payload. Unset, nothing here is
used and payloads stay in Postgres as before.
"""

import os
import uuid
from typing import Optional, Tuple, Union

from shepherd_utils.config import settings

MessageId = Union[str, uuid.UUID]


def enabled() -> bool:
    return bool(settings.ars_payload_dir)


def path_for(message_id: MessageId) -> str:
    """``<dir>/<first two hex digits>/<message id>.zst``. The fan-out keeps
    any one directory small; message ids are UUIDs, so this is even."""
    name = str(uuid.UUID(str(message_id)))
    return os.path.join(settings.ars_payload_dir, name[:2], f"{name}.zst")


def write(message_id: MessageId, blob: bytes) -> int:
    """Store ``blob`` durably, replacing any previous copy. Returns its size.

    Blocking (disk I/O and fsync): call it through ``asyncio.to_thread``.
    """
    path = path_for(message_id)
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


def read(message_id: MessageId) -> Optional[bytes]:
    """The stored blob, or None when this message has no file. Blocking."""
    try:
        with open(path_for(message_id), "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None


def exists(message_id: MessageId) -> bool:
    return os.path.exists(path_for(message_id))


def delete(message_id: MessageId) -> None:
    """Remove a message's file; a missing file is fine. Blocking."""
    try:
        os.unlink(path_for(message_id))
    except FileNotFoundError:
        pass


def usage() -> Tuple[int, int]:
    """(used bytes, total bytes) of the filesystem holding the payloads."""
    st = os.statvfs(settings.ars_payload_dir)
    total = st.f_blocks * st.f_frsize
    free = st.f_bavail * st.f_frsize
    return total - free, total

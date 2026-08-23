"""Tests for the batched/coalesced merge_message path.

Covers:
- ``merge_messages_by_ids`` folds multiple callbacks with a single load/save and
  is equivalent to merging them one at a time (the pre-existing behavior).
- The KG retrieval logs a callback carries back are handed to the parent
  instead of being dropped with the rest of the callback message.
- ``merge_messages_by_id`` still works as a one-callback delegate.
- The per-query "ready callback" index helpers in ``shepherd_utils.db``.
"""

import copy
import logging

import pytest

from shepherd_utils.db import (
    add_ready_callback,
    clear_ready_callback,
    get_message_sync,
    get_ready_callbacks,
    is_ready_callback,
)
from tests.helpers.generate_messages import (
    generate_response,
    response_1,
    response_2,
)
from shepherd_utils.config import settings
from workers.merge_message.worker import (
    merge_messages,
    merge_messages_by_id,
    merge_messages_by_ids,
    take_callback_logs,
)

logger = logging.getLogger(__name__)


def _patch_sync_store(mocker):
    """Route the sync data-db client through an in-memory dict (as the real
    ProcessPoolExecutor worker would hit Redis)."""
    storage = {}
    sync_client = mocker.Mock()
    sync_client.set.side_effect = lambda key, blob, ex=None: storage.__setitem__(
        key, blob
    )
    sync_client.get.side_effect = lambda key: storage.get(key)
    mocker.patch("shepherd_utils.db._get_sync_data_db", return_value=sync_client)
    return storage


def test_merge_messages_by_ids_equivalent_to_sequential(mocker):
    """Folding [c1, c2] in one batched call yields the same knowledge graph and
    result count as merging c1 then c2 one at a time."""
    query_graph = response_1["message"]["query_graph"]

    # Sequential reference: base ⊕ c1 ⊕ c2.
    seq = generate_response()
    for cb in (copy.deepcopy(response_2), copy.deepcopy(response_2)):
        seq = merge_messages("test_ara", query_graph, seq, cb, logger)

    # Batched: seed the sync store and fold both callbacks in one call.
    _patch_sync_store(mocker)
    from shepherd_utils.db import save_message_sync

    save_message_sync("qid", {"message": {"query_graph": copy.deepcopy(query_graph)}})
    save_message_sync("rid", generate_response())
    save_message_sync("c1", copy.deepcopy(response_2))
    save_message_sync("c2", copy.deepcopy(response_2))

    merged, _ = merge_messages_by_ids("test_ara", "qid", "rid", ["c1", "c2"])
    saved = get_message_sync("rid")

    # The merge synthesizes creative-mode knowledge edges with random UUIDs, so
    # compare counts (stable) rather than the generated keys.
    assert merged == ["c1", "c2"]
    assert len(saved["message"]["results"]) == len(seq["message"]["results"])
    assert len(saved["message"]["knowledge_graph"]["nodes"]) == len(
        seq["message"]["knowledge_graph"]["nodes"]
    )
    assert len(saved["message"]["knowledge_graph"]["edges"]) == len(
        seq["message"]["knowledge_graph"]["edges"]
    )
    assert len(saved["message"]["auxiliary_graphs"]) == len(
        seq["message"]["auxiliary_graphs"]
    )


def test_merge_messages_by_ids_skips_missing_callback(mocker):
    """A callback whose payload has vanished is skipped, not fatal, and is not
    reported as merged."""
    query_graph = response_1["message"]["query_graph"]
    _patch_sync_store(mocker)
    from shepherd_utils.db import save_message_sync

    save_message_sync("qid", {"message": {"query_graph": copy.deepcopy(query_graph)}})
    save_message_sync("rid", generate_response())
    save_message_sync("c1", copy.deepcopy(response_2))
    # "c2" intentionally not stored.

    merged, _ = merge_messages_by_ids("test_ara", "qid", "rid", ["c1", "c2"])
    assert merged == ["c1"]


def test_merge_messages_by_ids_returns_child_logs(mocker):
    """The merge child runs in a subprocess with no access to the parent's
    query logger, so it hands its own formatted log records back across the
    process boundary for the parent to fold into the query's log list. A missing
    callback logs an error there, so it must surface in the returned entries."""
    query_graph = response_1["message"]["query_graph"]
    _patch_sync_store(mocker)
    from shepherd_utils.db import save_message_sync

    save_message_sync("qid", {"message": {"query_graph": copy.deepcopy(query_graph)}})
    save_message_sync("rid", generate_response())
    save_message_sync("c1", copy.deepcopy(response_2))
    # "c2" intentionally not stored so the child logs a "Missing callback" error.

    _, log_entries = merge_messages_by_ids(
        "test_ara", "qid", "rid", ["c1", "c2"], logging.INFO
    )
    # Entries are ReasonerLogEntryFormatter dicts, oldest-first.
    assert any(
        "Missing callback c2" in entry.get("message", "") for entry in log_entries
    )
    assert all("timestamp" in entry and "level" in entry for entry in log_entries)


def test_merge_messages_by_ids_child_handler_not_leaked(mocker):
    """The child logger is a per-process singleton, so the call-scoped handler
    must be removed afterward -- otherwise handlers accumulate across the
    child's successive tasks and bleed one query's logs into the next."""
    import os

    query_graph = response_1["message"]["query_graph"]
    _patch_sync_store(mocker)
    from shepherd_utils.db import save_message_sync

    save_message_sync("qid", {"message": {"query_graph": copy.deepcopy(query_graph)}})
    save_message_sync("rid", generate_response())
    save_message_sync("c1", copy.deepcopy(response_2))

    merge_messages_by_ids("test_ara", "qid", "rid", ["c1"], logging.INFO)
    child_logger = logging.getLogger(f"merge_message.worker.{os.getpid()}")
    assert not any(
        getattr(h, "name", None) == "query_log_handler" for h in child_logger.handlers
    )


def _callback_with_logs(logs):
    """A callback message carrying the log entries a subservice returned."""
    callback = copy.deepcopy(response_2)
    callback["logs"] = logs
    return callback


def test_take_callback_logs_returns_and_clears_entries():
    """The subservice's entries come back out, and the field is blanked on the
    message so the log store stays the single source of the final logs."""
    entries = [
        {
            "timestamp": "2024-01-01T00:00:00+00:00",
            "level": "INFO",
            "message": "Calling KP infores:example",
        },
        {
            "timestamp": "2024-01-01T00:00:01+00:00",
            "level": "WARNING",
            "message": "KP infores:example timed out",
        },
    ]
    callback = _callback_with_logs(copy.deepcopy(entries))

    taken = take_callback_logs(callback, "c1", logging.INFO, logger)

    assert taken == entries
    assert callback["logs"] == []


def test_take_callback_logs_filters_below_requested_level():
    """A subservice that reports at DEBUG shouldn't flood an INFO query."""
    callback = _callback_with_logs(
        [
            {"level": "DEBUG", "message": "chatty"},
            {"level": "INFO", "message": "useful"},
            {"message": "no level at all"},
        ]
    )

    taken = take_callback_logs(callback, "c1", logging.INFO, logger)

    assert [entry["message"] for entry in taken] == ["useful", "no level at all"]


def test_take_callback_logs_handles_malformed_logs():
    """Missing, null, non-list, and non-dict logs are all survivable."""
    assert take_callback_logs({}, "c1", logging.INFO, logger) == []
    assert take_callback_logs({"logs": None}, "c1", logging.INFO, logger) == []
    assert take_callback_logs({"logs": "nope"}, "c1", logging.INFO, logger) == []
    assert take_callback_logs(
        {"logs": ["bare string"]}, "c1", logging.INFO, logger
    ) == [{"message": "bare string", "level": "INFO"}]


def test_take_callback_logs_caps_entry_count(mocker):
    """A subservice dumping tens of thousands of entries is truncated rather
    than stored (and echoed back) in full."""
    mocker.patch.object(settings, "merge_max_callback_logs", 2)
    callback = _callback_with_logs(
        [{"level": "INFO", "message": f"log {i}"} for i in range(5)]
    )

    taken = take_callback_logs(callback, "c1", logging.INFO, logger)

    assert [entry["message"] for entry in taken] == ["log 0", "log 1"]


def test_merge_messages_by_ids_returns_callback_logs(mocker):
    """The logs the KG retrieval sent back with each callback are returned to
    the parent (which folds them into the query's log list) rather than being
    dropped when the callback is merged into the response."""
    query_graph = response_1["message"]["query_graph"]
    _patch_sync_store(mocker)
    from shepherd_utils.db import save_message_sync

    save_message_sync("qid", {"message": {"query_graph": copy.deepcopy(query_graph)}})
    save_message_sync("rid", generate_response())
    save_message_sync(
        "c1", _callback_with_logs([{"level": "INFO", "message": "c1 retrieval log"}])
    )
    save_message_sync(
        "c2", _callback_with_logs([{"level": "ERROR", "message": "c2 retrieval log"}])
    )

    merged, log_entries = merge_messages_by_ids(
        "test_ara", "qid", "rid", ["c1", "c2"], logging.INFO
    )

    assert merged == ["c1", "c2"]
    messages = [entry.get("message") for entry in log_entries]
    assert "c1 retrieval log" in messages
    assert "c2 retrieval log" in messages
    # Retrieval logs are oldest-first and lead the merge's own records.
    assert messages.index("c1 retrieval log") < messages.index("c2 retrieval log")
    # ...and they aren't left on the merged response, which would duplicate
    # them once finish_query splices the log store in.
    assert get_message_sync("rid").get("logs") == []


def test_merge_messages_by_ids_direct_lookup_logs_not_left_on_message(mocker):
    """The direct-lookup path returns the callback message as the accumulator
    verbatim, so its logs have to be taken off it too."""
    query_graph = response_2["message"]["query_graph"]
    _patch_sync_store(mocker)
    from shepherd_utils.db import save_message_sync

    save_message_sync("qid", {"message": {"query_graph": copy.deepcopy(query_graph)}})
    save_message_sync("rid", copy.deepcopy(response_2))
    save_message_sync(
        "c1", _callback_with_logs([{"level": "INFO", "message": "lookup log"}])
    )

    _, log_entries = merge_messages_by_ids(
        "test_ara", "qid", "rid", ["c1"], logging.INFO
    )

    assert "lookup log" in [entry.get("message") for entry in log_entries]
    assert get_message_sync("rid").get("logs") == []


def test_merge_messages_by_id_delegates(mocker):
    """The single-callback entry point still works via the batched path."""
    query_graph = response_1["message"]["query_graph"]
    _patch_sync_store(mocker)
    from shepherd_utils.db import save_message_sync

    save_message_sync("qid", {"message": {"query_graph": copy.deepcopy(query_graph)}})
    save_message_sync("rid", generate_response())
    save_message_sync("c1", copy.deepcopy(response_2))

    assert merge_messages_by_id("test_ara", "qid", "rid", "c1") is True


def test_merge_messages_by_ids_missing_response_raises(mocker):
    _patch_sync_store(mocker)
    from shepherd_utils.db import save_message_sync

    save_message_sync("qid", {"message": {"query_graph": {}}})
    # No "rid" stored.
    with pytest.raises(KeyError):
        merge_messages_by_ids("test_ara", "qid", "rid", ["c1"])


@pytest.mark.asyncio
async def test_ready_callback_index_roundtrip(redis_mock):
    """add -> get -> is_member -> clear behaves as a per-query set."""
    assert await get_ready_callbacks("rid", logger) == []

    await add_ready_callback("rid", "cb1", logger)
    await add_ready_callback("rid", "cb2", logger)
    assert set(await get_ready_callbacks("rid", logger)) == {"cb1", "cb2"}
    assert await is_ready_callback("rid", "cb1", logger) is True
    assert await is_ready_callback("rid", "missing", logger) is False

    await clear_ready_callback("rid", "cb1", logger)
    assert await is_ready_callback("rid", "cb1", logger) is False
    assert set(await get_ready_callbacks("rid", logger)) == {"cb2"}

    await clear_ready_callback("rid", "cb2", logger)
    assert await get_ready_callbacks("rid", logger) == []


@pytest.mark.asyncio
async def test_ready_callback_index_is_per_query(redis_mock):
    """Callbacks for different queries live in different sets."""
    await add_ready_callback("rid-a", "cb1", logger)
    await add_ready_callback("rid-b", "cb2", logger)
    assert await get_ready_callbacks("rid-a", logger) == ["cb1"]
    assert await get_ready_callbacks("rid-b", logger) == ["cb2"]

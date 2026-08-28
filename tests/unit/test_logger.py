"""Tests for ``shepherd_utils.logger``: per-query log handler + formatter."""

import logging
import os

from shepherd_utils.logger import (
    QueryLogger,
    ReasonerLogEntryFormatter,
    attach_query_handler,
    get_logging_config,
    get_query_handler,
    get_worker_logger,
    resolve_log_level,
)


def _make_record(msg, level=logging.INFO, name="testlogger"):
    return logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=10,
        msg=msg,
        args=None,
        exc_info=None,
    )


def test_reasoner_formatter_string_message():
    formatter = ReasonerLogEntryFormatter()
    out = formatter.format(_make_record("hello"))
    assert out["message"] == "hello"
    assert out["level"] == "INFO"
    # Timestamp is iso8601-ish (T separator).
    assert "T" in out["timestamp"]


def test_reasoner_formatter_dict_message_merges_extra_keys():
    formatter = ReasonerLogEntryFormatter()
    record = _make_record(
        {"message": "embedded", "extra": "data"}, level=logging.WARNING
    )
    out = formatter.format(record)
    assert out["message"] == "embedded"
    assert out["extra"] == "data"
    assert out["level"] == "WARNING"


def test_query_logger_handler_collects_records_newest_first():
    """Records should be appended to the front of the deque (appendleft)."""
    ql = QueryLogger()
    handler = ql.log_handler
    sub_logger = logging.getLogger("test_query_logger.collects")
    sub_logger.handlers.clear()
    sub_logger.addHandler(handler)
    sub_logger.setLevel(logging.DEBUG)
    try:
        sub_logger.info("first")
        sub_logger.info("second")
        contents = list(handler.contents())
        # newest first
        assert [c["message"] for c in contents] == ["second", "first"]
    finally:
        sub_logger.removeHandler(handler)


def test_query_logger_handler_named_query_log_handler():
    """save_logs in db.py looks up the handler by name; verify the contract."""
    handler = QueryLogger().log_handler
    assert handler.name == "query_log_handler"


def test_resolve_log_level_maps_trapi_level_names():
    """The one place level names are understood -- the requested level on a
    query, and the level on a log entry a subservice sends back."""
    assert resolve_log_level("DEBUG") == logging.DEBUG
    assert resolve_log_level("debug") == logging.DEBUG
    assert resolve_log_level("WARNING") == logging.WARNING
    assert resolve_log_level("ERROR") == logging.ERROR


def test_resolve_log_level_falls_back_for_anything_unusable():
    """A query naming a level we can't parse should still run."""
    assert resolve_log_level(None) == logging.INFO
    assert resolve_log_level("") == logging.INFO
    assert resolve_log_level("LOUD") == logging.INFO
    assert resolve_log_level("LOUD", logging.WARNING) == logging.WARNING
    assert resolve_log_level(None, logging.DEBUG) == logging.DEBUG


def test_drain_empties_the_queue_and_returns_oldest_first():
    """Reading is destructive: the same records must not be handed out twice."""
    handler = QueryLogger().log_handler
    sub_logger = logging.getLogger("test_query_logger.drain")
    sub_logger.handlers.clear()
    sub_logger.addHandler(handler)
    sub_logger.setLevel(logging.DEBUG)
    try:
        sub_logger.info("first")
        sub_logger.info("second")
        assert [entry["message"] for entry in handler.drain()] == ["first", "second"]
        assert handler.drain() == []
        sub_logger.info("third")
        assert [entry["message"] for entry in handler.drain()] == ["third"]
    finally:
        sub_logger.removeHandler(handler)


def test_drained_records_can_be_put_back():
    """A failed flush returns its records to the queue for the next attempt."""
    handler = QueryLogger().log_handler
    sub_logger = logging.getLogger("test_query_logger.putback")
    sub_logger.handlers.clear()
    sub_logger.addHandler(handler)
    sub_logger.setLevel(logging.DEBUG)
    try:
        sub_logger.info("first")
        sub_logger.info("second")
        entries = handler.drain()
        handler.ingest(entries)
        assert [entry["message"] for entry in handler.drain()] == ["first", "second"]
    finally:
        sub_logger.removeHandler(handler)


def test_attach_query_handler_does_not_stack_handlers():
    """``logging.getLogger`` hands back one object per name, so a caller that
    "makes a logger per task" keeps getting the same one. Attaching must be
    idempotent -- stacked handlers queued every record once per handler and made
    each flush re-persist the earlier tasks' records."""
    sub_logger = logging.getLogger("test_query_logger.attach")
    sub_logger.handlers.clear()
    sub_logger.setLevel(logging.DEBUG)
    try:
        first = attach_query_handler(sub_logger)
        second = attach_query_handler(sub_logger)
        assert first is second
        assert sub_logger.handlers == [first]
        assert get_query_handler(sub_logger) is first

        sub_logger.info("once")
        assert [entry["message"] for entry in first.drain()] == ["once"]
    finally:
        sub_logger.handlers.clear()


def test_get_query_handler_returns_none_without_one():
    sub_logger = logging.getLogger("test_query_logger.none")
    sub_logger.handlers.clear()
    assert get_query_handler(sub_logger) is None


def test_query_logger_respects_maxlen():
    """When a maxlen is set, oldest records get dropped."""
    ql = QueryLogger(maxlen=2)
    handler = ql.log_handler
    sub_logger = logging.getLogger("test_query_logger.maxlen")
    sub_logger.handlers.clear()
    sub_logger.addHandler(handler)
    sub_logger.setLevel(logging.DEBUG)
    try:
        for i in range(5):
            sub_logger.info(f"msg-{i}")
        contents = list(handler.contents())
        # Newest two
        assert [c["message"] for c in contents] == ["msg-4", "msg-3"]
    finally:
        sub_logger.removeHandler(handler)


def test_get_logging_config_local_includes_file_handler(monkeypatch, tmp_path):
    """Outside Kubernetes, the config should set up a rotating file handler."""
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.chdir(tmp_path)
    config = get_logging_config()
    assert "file" in config["handlers"]
    assert "console" in config["handlers"]
    assert set(config["root"]["handlers"]) == {"console", "file"}
    # The function eagerly creates the logs/ dir for file output.
    assert os.path.isdir(tmp_path / "logs")


def test_get_logging_config_kubernetes_skips_file_handler(monkeypatch):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    config = get_logging_config()
    assert "file" not in config["handlers"]
    assert config["root"]["handlers"] == ["console"]


def test_get_logging_config_pool_child_skips_file_handler(monkeypatch, tmp_path):
    """A spawned pool child (name != MainProcess) logs to console only, so many
    children don't drive one non-multiprocess-safe RotatingFileHandler."""
    import types

    from shepherd_utils import logger as logger_module

    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        logger_module.multiprocessing,
        "current_process",
        lambda: types.SimpleNamespace(name="SpawnProcess-1"),
    )
    config = get_logging_config()
    assert "file" not in config["handlers"]
    assert config["root"]["handlers"] == ["console"]


# --- worker logger namespacing ----------------------------------------------
#
# Regression cover for a silent-startup bug: workers logged through
# ``logging.getLogger(STREAM)`` (e.g. "arax.pathfinder"), which sits outside the
# only namespace ``setup_logging`` attaches handlers to. Those records reached a
# handler-less root and were dropped by logging's lastResort fallback, so a
# worker that hung during startup produced no output whatsoever.


def test_get_worker_logger_namespaces_stream_names():
    assert get_worker_logger("arax.pathfinder").name == "shepherd.arax.pathfinder"


def test_get_worker_logger_does_not_double_prefix():
    """Safe to apply to names that are already namespaced."""
    assert get_worker_logger("shepherd.monitor").name == "shepherd.monitor"
    assert get_worker_logger("shepherd").name == "shepherd"


def test_worker_logger_inherits_configured_handlers(monkeypatch):
    """The whole point: a worker logger must resolve to a real handler at INFO."""
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    logging.config.dictConfig(get_logging_config())

    worker_logger = get_worker_logger("arax.pathfinder")
    # Walk the ancestry the way logging does when emitting a record.
    effective = []
    node = worker_logger
    while node:
        effective.extend(node.handlers)
        node = node.parent if node.propagate else None

    assert effective, "worker logger resolved to no handler"
    assert worker_logger.getEffectiveLevel() <= logging.INFO


def test_root_logger_configured_as_warning_backstop(monkeypatch):
    """Stray records outside ``shepherd.*`` still land somewhere, formatted.

    WARNING rather than INFO on purpose -- at INFO, libraries such as httpx emit
    a line per request and drown out our own logs.
    """
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    config = get_logging_config()
    assert config["root"]["level"] == "WARNING"
    assert config["root"]["handlers"] == ["console"]


def test_handlers_are_attached_only_once(monkeypatch):
    """Handlers live on root only; shepherd sets a level and propagates to them.

    Attaching them in both places would emit every shepherd record twice.
    """
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    config = get_logging_config()
    assert config["root"]["handlers"] == ["console"]
    assert "handlers" not in config["loggers"]["shepherd"]
    assert config["loggers"]["shepherd"]["level"] == "DEBUG"

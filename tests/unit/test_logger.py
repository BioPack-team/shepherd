"""Tests for ``shepherd_utils.logger``: per-query log handler + formatter."""

import logging
import os

from shepherd_utils.logger import (
    QueryLogger,
    ReasonerLogEntryFormatter,
    get_logging_config,
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
    assert set(config["loggers"]["shepherd"]["handlers"]) == {"console", "file"}
    # The function eagerly creates the logs/ dir for file output.
    assert os.path.isdir(tmp_path / "logs")


def test_get_logging_config_kubernetes_skips_file_handler(monkeypatch):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
    config = get_logging_config()
    assert "file" not in config["handlers"]
    assert config["loggers"]["shepherd"]["handlers"] == ["console"]

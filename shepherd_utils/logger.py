"""Set up logging."""

import logging
import logging.config
import os
from collections import deque
from datetime import datetime


class ReasonerLogEntryFormatter(logging.Formatter):
    """Format to match Reasoner API LogEntry"""

    def format(self, record):
        log_entry = {}

        # If given a string use that as the message
        if isinstance(record.msg, str):
            log_entry["message"] = record.msg

        # If given a dict, just use that as the log entry
        # Make sure everything is serializeable
        if isinstance(record.msg, dict):
            log_entry |= record.msg

        # Add timestamp
        iso_timestamp = datetime.utcfromtimestamp(record.created).isoformat()
        log_entry["timestamp"] = iso_timestamp

        # Add level
        log_entry["level"] = record.levelname

        return log_entry


class QueryLogHandler(logging.Handler):
    """Log Handler."""

    def __init__(self, log_queue):
        logging.Handler.__init__(self)
        self.log_queue = log_queue
        self.name = "query_log_handler"

    def emit(self, record):
        # put newer messages in front
        self.log_queue.appendleft(self.format(record))

    def contents(self):
        """Get stored logs from handler."""
        return self.log_queue


# Create unique logger for each query
# https://stackoverflow.com/a/37967421
class QueryLogger(object):
    """Query-specific logger."""

    def __init__(self, maxlen=None):
        self._log_queue = deque(maxlen=maxlen)
        self._log_handler = QueryLogHandler(self._log_queue)
        self._log_handler.setFormatter(ReasonerLogEntryFormatter())

    @property
    def log_handler(self):
        """Return the internal log handler."""
        return self._log_handler


def get_logging_config():
    """
    Returns logging configuration.
    File handler is only included when running locally (not in Kubernetes).
    """
    # Check if running in Kubernetes
    is_kubernetes = bool(os.getenv("KUBERNETES_SERVICE_HOST"))

    # Base handlers that are always included
    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "default",
        }
    }

    # Add file handler only for local development
    if not is_kubernetes:
        # create the logs folder
        os.makedirs("logs", exist_ok=True)
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "DEBUG",
            "formatter": "default",
            "filename": "./logs/shepherd.log",
            "mode": "a",
            "encoding": "utf-8",
            "maxBytes": 100000000,
            "backupCount": 9,
        }

    # Determine which handlers to use for the logger
    logger_handlers = ["console", "file"] if not is_kubernetes else ["console"]

    logging_config = {
        "version": 1,
        "formatters": {
            "default": {"format": "[%(asctime)s: %(levelname)s/%(name)s]: %(message)s"}
        },
        "handlers": handlers,
        "loggers": {
            "shepherd": {
                "level": "DEBUG",
                "handlers": logger_handlers,
            }
        },
        "incremental": False,
        "disable_existing_loggers": False,
    }

    return logging_config


def setup_logging():
    """Set up logging."""
    config = get_logging_config()
    logging.config.dictConfig(config)

"""Set up logging."""

import logging
import logging.config
import multiprocessing
import os
from collections import deque
from datetime import datetime, timezone


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
        iso_timestamp = datetime.fromtimestamp(
            record.created, tz=timezone.utc
        ).isoformat()
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

    def ingest(self, entries):
        """Merge already-formatted log entries into the queue.

        Used to fold in log records produced somewhere the handler couldn't be
        attached directly -- e.g. a ProcessPoolExecutor child that formats its
        own records and hands them back across the process boundary. ``entries``
        must be oldest-first; each is placed at the front so the queue stays
        newest-first, matching ``emit``.
        """
        for entry in entries:
            self.log_queue.appendleft(entry)


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
    # Only the main process writes the rotating log file. A spawned process-pool
    # child re-runs this on import; if every child also attached the file
    # handler, many processes would drive one RotatingFileHandler on the same
    # (locally bind-mounted) file -- and that handler isn't multiprocess-safe.
    # Children log to console, which the container/collector already captures.
    is_main = multiprocessing.current_process().name == "MainProcess"
    use_file = (not is_kubernetes) and is_main

    # Base handlers that are always included
    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "default",
        }
    }

    # Add file handler only for local development in the main process
    if use_file:
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
    logger_handlers = ["console", "file"] if use_file else ["console"]

    logging_config = {
        "version": 1,
        "formatters": {
            "default": {"format": "[%(asctime)s: %(levelname)s/%(name)s]: %(message)s"}
        },
        "handlers": handlers,
        # The output handlers live on root, and root alone. Previously they were
        # attached to ``shepherd`` and root was left unconfigured, so any record
        # logged outside that namespace -- a stray ``logging.info``, a
        # third-party library, a logger named for its module or its Redis stream
        # -- reached a handler-less root and was dropped by logging's
        # ``lastResort`` fallback, which only emits WARNING+ and ignores our
        # formatter. That silently swallowed every worker's entire startup
        # phase; see ``get_worker_logger``.
        #
        # Root's level applies only to records logged directly on root, not to
        # ones propagated up from a child (those are level-checked at the
        # originating logger). So WARNING here means third-party libraries are
        # quiet below WARNING -- httpx logs a line per request at INFO, which
        # would bury our own output -- while ``shepherd.*`` still emits at DEBUG
        # via the entry below. Everything then reaches these handlers by
        # propagation, which also keeps pytest's ``caplog`` working.
        "root": {
            "level": "WARNING",
            "handlers": logger_handlers,
        },
        "loggers": {
            # No handlers: records propagate to root's. Only the level is set
            # here, which is what lets our own logging through at DEBUG while
            # leaving third-party loggers at root's WARNING.
            "shepherd": {
                "level": "DEBUG",
            },
            # psycopg's pool retries to keep min_size connections warm and logs
            # a WARNING on every failed attempt. When the DB is down that floods
            # the logs from every service; raise the threshold so only genuine
            # pool errors surface (our own code already logs the outage once).
            "psycopg.pool": {
                "level": "ERROR",
                "handlers": logger_handlers,
                "propagate": False,
            },
        },
        "incremental": False,
        "disable_existing_loggers": False,
    }

    return logging_config


def get_worker_logger(name: str) -> logging.Logger:
    """Return a logger under the configured ``shepherd`` namespace.

    ``setup_logging`` only attaches handlers to ``shepherd`` (and, as a
    WARNING-level backstop, root), so a logger named for the stream alone
    (``logging.getLogger("arax.pathfinder")``) inherits no handler at INFO and
    its records vanish. Every worker's startup phase -- dataset downloads, pool
    sizing, poll-loop errors -- logged through exactly such a logger, so a
    worker that hung before reaching ``get_tasks`` produced no output at all
    and looked identical to a healthy idle one.

    Passing a name that is already namespaced is a no-op, so this is safe to
    apply to existing ``shepherd.``-prefixed names.
    """
    if name == "shepherd" or name.startswith("shepherd."):
        return logging.getLogger(name)
    return logging.getLogger(f"shepherd.{name}")


def setup_logging():
    """Set up logging."""
    config = get_logging_config()
    logging.config.dictConfig(config)

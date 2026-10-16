# Vendored from Translator-CATRAX/xCRG @ e67f2c0, src/xcrg/reporting.py (DEC-21).
# Changes from upstream:
#   - unchanged
# See README.md.
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum


# The default logger for the LogReporter
_LOGGER = logging.getLogger(__name__)


class Message:
    """Marker class for messages reported by the xCRG module."""
    pass


class LogLevel(IntEnum):
    DEBUG    = logging.DEBUG
    INFO     = logging.INFO
    WARNING  = logging.WARNING
    ERROR    = logging.ERROR
    CRITICAL = logging.CRITICAL
    FATAL    = logging.FATAL


@dataclass
class LogMessage(Message):
    level : LogLevel
    msg   : str
    args  : tuple[object, ...] = field(default_factory = tuple)
    time  : datetime           = field(default = datetime.now(timezone.utc))


# @dataclass
# class ProgressMessage(Message):
#     pct_done: float


class Reporter(ABC):
    """Abstract class for xCRG reporter that informs caller about events and progress."""
    @abstractmethod
    def handle_message(self, message: Message) -> None:
        """Handle a message from the xCRG module."""
        ...

    def debug(self, msg: str, *args: object):
        self.handle_message(LogMessage(LogLevel.DEBUG, msg, args))

    def info(self, msg: str, *args: object):
        self.handle_message(LogMessage(LogLevel.INFO, msg, args))

    def warning(self, msg: str, *args: object):
        self.handle_message(LogMessage(LogLevel.WARNING, msg, args))

    def error(self, msg: str, *args: object):
        self.handle_message(LogMessage(LogLevel.ERROR, msg, args))

    def critical(self, msg: str, *args: object):
        self.handle_message(LogMessage(LogLevel.CRITICAL, msg, args))

    def fatal(self, msg: str, *args: object):
        self.handle_message(LogMessage(LogLevel.FATAL, msg, args))


class StubReporter(Reporter):
    """A reporter that does nothing with messages."""
    def handle_message(self, message: Message) -> None:
        pass


@dataclass
class LogReporter(Reporter):
    """A reporter that wraps the standard logging.Logger class."""
    logger: logging.Logger = field(default = _LOGGER)

    def handle_message(self, message: Message) -> None:
        match message:
            case LogMessage() as log:
                match log.level:
                    case LogLevel.DEBUG:    self.logger.debug(log.msg, *log.args)
                    case LogLevel.INFO:     self.logger.info(log.msg, *log.args)
                    case LogLevel.WARNING:  self.logger.warning(log.msg, *log.args)
                    case LogLevel.ERROR:    self.logger.error(log.msg, *log.args)
                    case LogLevel.CRITICAL: self.logger.critical(log.msg, *log.args)
                    case LogLevel.FATAL:    self.logger.fatal(log.msg, *log.args)

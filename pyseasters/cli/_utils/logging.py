import logging
import logging.config
import sys
from dataclasses import dataclass, field
from typing import Callable, List, Tuple

__all__ = ["setup_cli_logging", "LoggingStack"]

log = logging.getLogger(__name__)


class LevelFilter(logging.Filter):
    """Class to filter out records with level greater of equal to ``threshold``."""

    def __init__(self, threshold):
        super().__init__()
        self.threshold = threshold

    def filter(self, record):
        return record.levelno < self.threshold


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(levelname)s: %(message)s",
        },
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "stream": sys.stdout,
            "formatter": "default",
            "filters": ["stdout_filter"],
        },
        "stderr": {
            "class": "logging.StreamHandler",
            "level": "WARNING",
            "stream": sys.stderr,
            "formatter": "default",
            "filters": [],
        },
    },
    "filters": {
        "stdout_filter": {
            "()": LevelFilter,
            "threshold": logging.WARNING,
        },
    },
    "loggers": {
        "": {
            "handlers": ["stdout", "stderr"],
            "level": "DEBUG",  # Can be modified later
            "propagate": True,
        },
    },
}


def setup_cli_logging(level: int = logging.INFO) -> None:
    """Configure logging to output to stdout for CLI usage.

    This sets up the logging system to display messages to standard output,
    using a simple message-only format. Intended to be called at the start of
    CLI entry points to ensure log messages are visible to the user.

    Args:
        level: Logging level to use (e.g., ``logging.INFO`` or ``logging.DEBUG``).
    """
    root_log = logging.getLogger()
    logging.config.dictConfig(LOGGING_CONFIG)
    accepted_level = level not in [logging.ERROR, logging.CRITICAL]
    if not accepted_level:
        log.warning("WARNING is the least accepted logging level for the CLI.")
        log.warning(
            "Cannot set %s level. Reset to WARNING.", logging.getLevelName(level)
        )
    root_log.setLevel(level if accepted_level else logging.WARNING)


@dataclass
class LoggingStack:
    name: str
    messages: List[Tuple[str, ...]] = field(default_factory=list)

    def append(self, level: str, message: str, *args: str) -> None:
        self.messages.append(tuple([level, message, *args]))

    def __getattr__(self, name: str) -> Callable[..., None]:
        if name.lower() not in ["debug", "info", "warning", "error", "critical"]:
            raise AttributeError(
                f"Attribute {name} invalid for class {self.__class__.__name__}."
            )

        def method(*args, **kwargs) -> None:
            self.append(name, *args, **kwargs)

        return method

    def flush(self, logger: logging.Logger):
        while self.messages:
            params = self.messages.pop(0)
            getattr(logger, params[0].lower())(
                f"[{self.name}] {params[1]}", *params[2:]
            )

    def picklable(self) -> Tuple[str, List[Tuple[str, ...]]]:
        return self.name, self.messages

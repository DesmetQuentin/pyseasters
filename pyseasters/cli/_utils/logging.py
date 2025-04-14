import functools
import logging
import logging.config
import sys
from io import StringIO
from typing import Callable

__all__ = ["setup_cli_logging", "capture_logging"]

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
    logging.config.dictConfig(LOGGING_CONFIG)
    accepted_level = level not in [logging.ERROR, logging.CRITICAL]
    if not accepted_level:
        log.warning("WARNING is the least accepted logging level for the CLI.")
        log.warning(
            "Cannot set %s level. Reset to WARNING.", logging.getLevelName(level)
        )
    logging.getLogger().setLevel(level if accepted_level else logging.WARNING)


def capture_logging(func: Callable) -> Callable:
    """Decorator aiming to capture and return in-function logging output."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        log = func.__module__
        log_stream = StringIO()
        temp_handler = logging.StreamHandler(log_stream)
        temp_handler.setLevel(logging.DEBUG)
        log.addHandler(temp_handler)
        try:
            func(*args, **kwargs)
        finally:
            log.removeHandler(temp_handler)
        return log_stream.getvalue()

    return wrapper

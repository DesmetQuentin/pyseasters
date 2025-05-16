import logging
import logging.config
import sys
from dataclasses import dataclass, field
from typing import Callable, List, Tuple

from pyseasters.utils._typing import LoggingStackPickle

__all__ = ["setup_cli_logging", "LoggingStack"]

log = logging.getLogger(__name__)


class _LevelFilter(logging.Filter):
    """Class to filter out records with level greater of equal to ``threshold``."""

    def __init__(self, threshold):
        super().__init__()
        self.threshold = threshold

    def filter(self, record):
        return record.levelno < self.threshold


_LOGGING_CONFIG = {
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
            "()": _LevelFilter,
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

    Parameters
    ----------
    level
        Logging level to use: ``logging.INFO``, ``logging.DEBUG`` or
        ``logging.WARNING``.
    """
    root_log = logging.getLogger()
    logging.config.dictConfig(_LOGGING_CONFIG)
    accepted_level = level not in [logging.ERROR, logging.CRITICAL]
    if not accepted_level:
        log.warning("WARNING is the least accepted logging level for the CLI.")
        log.warning(
            "Cannot set %s level. Reset to WARNING.", logging.getLevelName(level)
        )
    root_log.setLevel(level if accepted_level else logging.WARNING)


@dataclass
class LoggingStack:
    """
    A lightweight, picklable logging buffer designed for use in delayed or parallel
    execution contexts (e.g., Dask tasks), where logging should be deferred until
    after task execution.

    This class mimics the ``logging.Logger`` interface and stacks logging statements
    for later flushing into a real logger. Particularly useful for capturing logs
    during lazy computations.

    Attributes
    ----------
    name: str
        Identifier prepended to all log messages when flushed.
    messages: List[Tuple[str, ...]], default []
        Internal list of ``(level, message, *args)`` tuples.

    Examples
    --------
    >>> import logging
    >>>
    >>> # In a parallel task with an ID 'task_id'
    >>> logstack = LoggingStack("task_id")
    >>> logstack.info("Starting heavy computation on %s", "input.nc")
    >>> logstack.warning("Took longer than expected.")
    >>> pickled_stack = logstack.picklable()  # retrieve only (name, messages) for serialization
    >>>
    >>> # In a post-computation context:
    >>> logstack = LoggingStack(*pickled_stack)  # reconstruct the stack
    >>> logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)  # some config
    >>> logger = logging.getLogger(__name__)  # create a logging.Logger
    >>> logstack.flush(logger)  # flush the stack into the logger
    INFO: [task_id] Starting heavy computation on input.nc
    WARNING: [task_id] Took longer than expected.
    """

    name: str
    messages: List[Tuple[str, ...]] = field(default_factory=list)

    def _append(self, level: str, message: str, *args: str) -> None:
        """Append ``(level, message, *args)`` to ``self.messages``."""
        self.messages.append(tuple([level, message, *args]))

    def __getattr__(self, name: str) -> Callable[..., None]:
        """Dynamically intercept standard log levels."""
        if name.lower() not in ["debug", "info", "warning", "error", "critical"]:
            raise AttributeError(
                f"Attribute {name} invalid for class {self.__class__.__name__}."
            )

        def method(*args, **kwargs) -> None:
            self._append(name, *args, **kwargs)

        return method

    def flush(self, logger: logging.Logger):
        """Send all buffered logs to a real logger."""
        while self.messages:
            params = self.messages.pop(0)
            getattr(logger, params[0].lower())(
                f"[{self.name}] {params[1]}", *params[2:]
            )

    def picklable(self) -> LoggingStackPickle:
        """Return a tuple representation for pickling or serialization."""
        return self.name, self.messages

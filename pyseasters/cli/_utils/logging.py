import logging
import sys

__all__ = ["setup_cli_logging"]


def setup_cli_logging(level: int = logging.INFO) -> None:
    """Configure logging to output to stdout for CLI usage.

    This sets up the logging system to display messages to standard output,
    using a simple message-only format. Intended to be called at the start of
    CLI entry points to ensure log messages are visible to the user.

    Args:
        level: Logging level to use (e.g., ``logging.INFO`` or ``logging.DEBUG``).
    """
    logging.basicConfig(level=level, format="%(message)s", stream=sys.stdout)

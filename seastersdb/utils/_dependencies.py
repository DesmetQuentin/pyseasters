import functools
import shutil
from typing import Callable

__all__ = ["require_tools"]

_checked_tools = set()


def require_tools(*tools: str) -> Callable:
    """
    Decorator to ensure required system tools are available in ``PATH``
    (only once per tool).

    Parameters
    ----------
    *tools
        One or more tool names (e.g., 'csvsql', 'ffmpeg') to check.

    Returns
    -------
    decorator : Callable
        A wrapped function that raises a RuntimeError if any tool is not found.

    Raises
    ------
    RuntimeError
        If a required tool is not available in the system ``PATH``.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            missing = []
            for tool in tools:
                if tool not in _checked_tools:
                    if shutil.which(tool) is None:
                        missing.append(tool)
                    else:
                        _checked_tools.add(tool)
            if missing:
                raise RuntimeError(
                    f"Required tools not found in `PATH`: {', '.join(missing)}"  # noqa: E713
                )
            return func(*args, **kwargs)

        return wrapper

    return decorator

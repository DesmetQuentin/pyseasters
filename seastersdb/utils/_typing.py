import logging
from pathlib import Path
from typing import TypeAlias, Union

from ._logging import LoggingStack

__all__ = ["PathLike", "LoggerLike"]

PathLike: TypeAlias = Union[str, Path]
LoggerLike: TypeAlias = Union[logging.Logger, LoggingStack]

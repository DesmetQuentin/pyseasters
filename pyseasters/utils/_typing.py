import logging
from pathlib import Path
from typing import List, Tuple, TypeAlias, Union

from ._logging import LoggingStack

__all__ = ["PathLike", "LoggerLike", "LoggingStackPickle"]

PathLike: TypeAlias = Union[str, Path]
LoggerLike: TypeAlias = Union[logging.Logger, LoggingStack]
LoggingStackPickle: TypeAlias = Tuple[str, List[Tuple[str, ...]]]

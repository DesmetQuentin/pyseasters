import os
from typing import TypeAlias, Union

__all__ = ["PathType"]

PathType: TypeAlias = Union[str, os.PathLike[str]]
"""Type alias for path-like objects."""

from logging import Logger
from typing import Any, Tuple

import pandas as pd

__all__ = ["search_fallback_single", "search_fallback_double"]


def _search_fallback_log(
    log: Logger, context: str, cause: str, cause_value: Any
) -> None:
    """Log warnings giving context to the search failing."""
    log.warning(f"No {context} data meets the search criteria.")
    log.warning(f"Limiting factor: ``{cause}``. Provided: {cause_value}.")


def search_fallback_single(
    log: Logger, context: str, cause: str, cause_value: Any, df: pd.DataFrame
) -> pd.DataFrame:
    """
    Fallback for search function failing to meet the search criteria
    (one return DataFrame version).
    """
    _search_fallback_log(log, context, cause, cause_value)
    res = df.iloc[0:0]
    return res


def search_fallback_double(
    log: Logger,
    context: str,
    cause: str,
    cause_value: Any,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fallback for search function failing to meet the search criteria
    (two return DataFrame version).
    """
    _search_fallback_log(log, context, cause, cause_value)
    res = (df1.iloc[0:0], df2.iloc[0:0])
    return res

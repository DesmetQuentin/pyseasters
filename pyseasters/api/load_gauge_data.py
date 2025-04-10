from datetime import datetime
from typing import Callable, List, Optional, Tuple

import pandas as pd

from .ghcnd import load_ghcnd_data
from .utils import check_dataframe_unit

__all__ = ["load_gauge_data"]

_gauge_data_sources = [
    "GHCNd",
]


def _renamer(source: str) -> Callable[[str], str]:
    """
    Return a callable that aims at adding the ``'source:'`` prefix to the argument.
    """

    def mapper(station_id):
        return f"{source}:{station_id}"

    return mapper


def _dispatcher(source: str, **kwargs) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return the (data, metadata) pandas DataFrame tuple returned by the right loader.

    Based on ``source``, this functions calls the right loader, passing the adequate
    kwargs in ``kwargs``. Station IDs in the returned DataFrames are prefixed by the
    the ``source`` keyword, with an colon separator.

    Args:
        source: The keyword associated with the desired source. Available sources are
            'GHCNd'.

        **kwargs: kwargs to pass to the loader.

    Returns:
        A tuple:
            - A DataFrame with station data as columns and dates as index, and with
            the 'units' attribute indicating its units.
            - A DataFrame of metadata for the selected stations.

    Raises:
        ValueError: If the ``source`` is not valid.
    """
    if source == "GHCNd":
        data, metadata = load_ghcnd_data(
            var=kwargs.get("var", "PRCP"),
            filter_condition=kwargs.get("filter_condition"),
            time_range=kwargs.get("time_range"),
            from_parquet=kwargs.get("from_parquet", True),
        )
    else:
        raise ValueError(
            f"'{source}' is not a valid source. Please provide one of "
            + f"""{",".join(["'%s'" %(key) for key in _gauge_data_sources])}."""
        )

    # Add the source as a prefix to the station ID
    data = data.rename(_renamer(source), axis=1)
    metadata.index = metadata.index.map(_renamer(source))

    return data, metadata


def load_gauge_data(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = _gauge_data_sources,
    unit: str = "mm/day",
    from_parquet: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load rain gauge data and associated station metadata from multiple sources.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes. Sources can be
    selected using ``usesources``, the default corresponding to looking into every
    matching station across all available sources. ``unit`` allows to choose the output
    unit for rain gauge data.

    Args:
        filter_condition: An optional query string to filter the station metadata.
            Available attributes are 'station_id', 'lon', 'lat', 'elevation'
            and 'station_name'.

        time_range: A tuple of (start_datetime, end_datetime) for selecting time
            coverage.

        usesources: A list of the sources to include in the search
            (default: all available sources).
            Available sources are 'GHCNd'.

        unit: The output unit for the rain gauge data DataFrame (default: 'mm/day').

        from_parquet: Whether the data to load is stored in the parquet format.

    Returns:
        A tuple:
            - A DataFrame with station data as columns and dates as index, and with
            the 'units' attribute indicating its units.
            - A DataFrame of metadata for the selected stations.

    Raises:
        RuntimeError: If the filter condition is invalid or raises an exception.
    """

    all_data, all_metadata = [], []
    for source in usesources:
        data, metadata = _dispatcher(
            source=source,
            var="PRCP",
            filter_condition=filter_condition,
            time_range=time_range,
            from_parquet=from_parquet,
        )
        data = check_dataframe_unit(data, target_unit=unit)
        all_data.append(data)
        all_metadata.append(metadata)

    combined_data = pd.concat(all_data, axis=1)
    combined_metadata = pd.concat(all_metadata, axis=0)

    return combined_data, combined_metadata

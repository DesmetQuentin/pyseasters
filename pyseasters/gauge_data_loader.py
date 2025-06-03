from datetime import datetime
from typing import Callable, List, Optional, Tuple

import pandas as pd

from .ghcnd import load_ghcnd_data
from .utils import check_dataframe_unit

__all__ = ["load_gauge_data"]

_GAUGE_DATA_SOURCES = [
    "GHCNd",
]


def _renamer(source: str) -> Callable[[str], str]:
    """
    Return a callable that aims at adding the ``'source:'`` prefix to the argument.
    """

    def mapper(station_id):
        return f"{source}:{station_id}"  # noqa: E231

    return mapper


def _dispatcher(source: str, **kwargs) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return the (data, metadata) pandas DataFrame tuple returned by the right loader.

    Based on ``source``, this functions calls the right loader, passing the adequate
    kwargs in ``kwargs``. Station IDs in the returned DataFrames are prefixed by the
    the ``source`` keyword, with an colon separator.

    Parameters
    ----------
    source
        The keyword associated with the desired source. Available sources are 'GHCNd'.
    **kwargs
        Kwargs to pass to the loader.

    Returns
    -------
    (data, metadata) : Tuple[DataFrame, DataFrame]
        A tuple of DataFrames, with (1) Gauge data, with station IDs as columns, dates
        as index and a 'units' attribute, and (2) Metadata for the associated stations.

    Raises
    ------
    ValueError
        If the ``source`` is not valid.
    """
    if source == "GHCNd":
        data, metadata = load_ghcnd_data(
            var=kwargs.get("var", "PRCP"),
            filter_condition=kwargs.get("filter_condition"),
            time_range=kwargs.get("time_range"),
        )
    else:
        raise ValueError(
            f"'{source}' is not a valid source. Please provide one of "
            + f"""{",".join(["'%s'" % (key) for key in _GAUGE_DATA_SOURCES])}."""
        )

    # Add the source as a prefix to the station ID
    data = data.rename(_renamer(source), axis=1)
    metadata = metadata.rename(_renamer(source))

    return data, metadata


def load_gauge_data(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = _GAUGE_DATA_SOURCES,
    units: str = "mm",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load rain gauge data and associated station metadata from multiple sources.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes. Sources can be
    selected using ``usesources``, the default corresponding to looking into every
    matching station across all available sources. ``units`` allows to choose the output
    unit for rain gauge data.

    Parameters
    ----------
    filter_condition
        An optional query string to filter the station metadata.
        Available attributes are 'station_id', 'lon', 'lat', 'elevation'
        and 'station_name'.
    time_range
        An optional time range for selecting time coverage.
    usesources
        A list of the sources to include in the search (default is all available
        sources).
        Available sources are 'GHCNd'.
    units
        The output unit for the rain gauge data.

    Returns
    -------
    (data, metadata) : Tuple[DataFrame, DataFrame]
        A tuple of DataFrames, with (1) Gauge data, with station IDs as columns, dates
        as index and a 'units' attribute, and (2) Metadata for the associated stations.

    Raises
    ------
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """

    all_data, all_metadata = [], []
    for source in usesources:
        data, metadata = _dispatcher(
            source=source,
            var="PRCP",
            filter_condition=filter_condition,
            time_range=time_range,
        )
        data = check_dataframe_unit(data, target_unit=units)
        all_data.append(data)
        all_metadata.append(metadata)

    combined_data = pd.concat(all_data, axis=1)
    combined_metadata = pd.concat(all_metadata, axis=0)
    combined_data = combined_data.loc[:, combined_metadata.index.to_list()]

    return combined_data, combined_metadata

from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from .ghcnd import load_ghcnd
from .ghcnh import load_ghcnh
from .gsdr import load_gsdr
from .utils import check_dataframe_unit

__all__ = ["load_1h_gauge_data", "load_all_gauge_data"]

_GAUGE_DATA_SOURCES = [
    "GHCNd",
    "GHCNh",
    "GSDR",
]

_1H_GAUGE_DATA_SOURCES = ["GHCNh", "GSDR"]

_PERIOD_SOURCE_VAR = {
    "precipitation_1d": {
        "GHCNd": "PRCP",
        "GHCNh": "precipitation_24h",
    },
    "precipitation_21h": {
        "GHCNh": "precipitation_21h",
    },
    "precipitation_18h": {
        "GHCNh": "precipitation_18h",
    },
    "precipitation_15h": {
        "GHCNh": "precipitation_15h",
    },
    "precipitation_12h": {
        "GHCNh": "precipitation_12h",
    },
    "precipitation_9h": {
        "GHCNh": "precipitation_9h",
    },
    "precipitation_6h": {
        "GHCNh": "precipitation_6h",
    },
    "precipitation_3h": {
        "GHCNh": "precipitation_3h",
    },
    "precipitation_1h": {
        "GHCNh": "precipitation",
        "GSDR": "Precipitation",
    },
}


def _renamer(source: str) -> Callable[[str], str]:
    """
    Return a callable that aims at adding the ``'source:'`` prefix to the argument.
    """

    def mapper(station_id):
        return f"{source}:{station_id}"  # noqa: E231

    return mapper


def _dispatcher(
    source: str, var: Optional[str] = None, **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return the (data, metadata) pandas DataFrame tuple returned by the right loader.

    Based on ``source``, this functions calls the right loader, passing the adequate
    kwargs in ``kwargs``. Station IDs in the returned DataFrames are prefixed by the
    the ``source`` keyword, with an colon separator.

    Parameters
    ----------
    source
        The keyword associated with the desired source.
        Available sources are 'GHCNd', 'GHCNh', 'GSDR'.
    var
        An optional var name to load from source. Default is None, leading to loading
        the legacy precipitation variable from ``source``.
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
        data, metadata = load_ghcnd(
            var="PRCP" if not var else var,
            filter_condition=kwargs.get("filter_condition"),
            time_range=kwargs.get("time_range"),
        )
    elif source == "GHCNh":
        data, metadata = load_ghcnh(
            var="precipitation" if not var else var,
            filter_condition=kwargs.get("filter_condition"),
            time_range=kwargs.get("time_range"),
        )
    elif source == "GSDR":
        data, metadata = load_gsdr(
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


def load_1h_gauge_data(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = _1H_GAUGE_DATA_SOURCES,
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
        Available sources are 'GHCNh', 'GSDR'.
    units
        The output unit for the rain gauge data.

    Returns
    -------
    (data, metadata) : Tuple[DataFrame, DataFrame]
        A tuple of DataFrames, with (1) Gauge data, with station IDs as columns, dates
        as index and a 'units' attribute, and (2) Metadata for the associated stations.

    Raises
    ------
    ValueError
        If the sources are not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    if any([source in _1H_GAUGE_DATA_SOURCES for source in usesources]):
        raise ValueError(
            f"{usesources!r} is not valid."
            + f" Please provide sources of {_1H_GAUGE_DATA_SOURCES!r}."
        )

    all_data, all_metadata = [], []
    for source in usesources:
        data, metadata = _dispatcher(
            source=source,
            filter_condition=filter_condition,
            time_range=time_range,
        )
        data = check_dataframe_unit(data, target_unit=units)

        all_data.append(data)
        all_metadata.append(metadata)

    combined_metadata = pd.concat(all_metadata, axis=0).sort_index()
    combined_data = pd.concat(all_data, axis=1)
    combined_data = combined_data.loc[:, combined_metadata.index.to_list()]

    return combined_data, combined_metadata


def load_all_gauge_data(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = _GAUGE_DATA_SOURCES,
    units: str = "mm",
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
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
        Available sources are 'GHCNd', 'GHCNh', 'GSDR'.
    units
        The output unit for the rain gauge data.

    Returns
    -------
    (data, metadata) : Tuple[Dict[str, DataFrame], DataFrame]
        A tuple, with (1) Gauge data Dataframes stored by accumulation period in a
        dictionary, with station IDs as columns, dates as index and several attribute,
        and (2) One single metadata Dataframe for the associated stations.

    Raises
    ------
    ValueError
        If the sources are not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    if any([source in _GAUGE_DATA_SOURCES for source in usesources]):
        raise ValueError(
            f"{usesources!r} is not valid."
            + f" Please provide sources of {_GAUGE_DATA_SOURCES!r}."
        )

    combined_data, all_metadata = {}, {}
    for period, source_var in _PERIOD_SOURCE_VAR.items():
        period_data, period_metadata = [], []
        for source, var in source_var.items():
            if source not in usesources:
                continue

            data, metadata = _dispatcher(
                source=source,
                var=var,
                filter_condition=filter_condition,
                time_range=time_range,
            )
            data = check_dataframe_unit(data, target_unit=units)

            period_data.append(data)
            period_metadata.append(metadata)

        all_metadata[period] = pd.concat(period_metadata, axis=0).sort_index()
        combined_data[period] = pd.concat(period_data, axis=1)
        combined_data[period] = combined_data[period].loc[
            :, all_metadata[period].index.to_list()
        ]
        combined_data[period].attrs = dict(
            name=f"{period.split('_')[-1]} total liquid precipitation",
            long_name=f"{period.split('_')[-1]} total liquid precipitation accumulation",
            units=units,
        )

    combined_metadata = (
        pd.concat(list(all_metadata.values()), axis=0).drop_duplicates().sort_index()
    )

    return combined_data, combined_metadata

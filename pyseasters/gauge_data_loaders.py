"""Provide searchers and loaders of rain gauge data, accross multiple data sources.

Functions of this modules can be separated into ``search*()`` and ``load*()``
categories. The former apply the search criteria to the metadata without loading the
actual records, optionally estimating the memory usage once the corresponding data
records are loader. The latter does the search but also load and return the data.

Another separation can be made between ``*_1h_*()`` and ``*_all_*()`` functions. The
former focuses on hourly records, throughout the GHCNh and GSDR datasets. The latter
extends the scope to all available rain gauge data, currently from daily to hourly. The
returned values are formatted in the form of dictionaries where keys refer to the
accumulation period.
"""

import logging
from datetime import datetime
from typing import Any, Callable, Dict, Hashable, List, Optional, Tuple

import pandas as pd

from .ghcnd.data_loaders import (
    _load_ghcnd_attrs_pr,
    _load_ghcnd_concat_pr,
    _load_ghcnd_noattrs_pr,
)
from .ghcnd.metadata_loaders import _search_ghcnd_pr
from .ghcnh.data_loaders import (
    _load_ghcnh_attrs_pr,
    _load_ghcnh_concat_pr,
    _load_ghcnh_noattrs_pr,
)
from .ghcnh.metadata_loaders import _search_ghcnh_pr
from .gsdr import load_gsdr_concat, search_gsdr
from .gsdr.data_loaders import _load_gsdr_df, _load_gsdr_series
from .utils import check_dataframe_unit
from .utils._args import localize_time_range
from .utils._memory import format_memory

__all__ = [
    "load_1h_gauge_data",
    "load_1h_gauge_data_attrs",
    "load_1h_gauge_data_concat",
    "load_1h_gauge_data_noattrs",
    "load_all_gauge_data",
    "load_all_gauge_data_attrs",
    "load_all_gauge_data_concat",
    "load_all_gauge_data_noattrs",
    "search_1h_gauge_data",
    "search_all_gauge_data",
]

log = logging.getLogger(__name__)

_ALL_GAUGE_DATA_SOURCES = [
    "GHCNd",
    "GHCNh",
    "GSDR",
]

_1H_GAUGE_DATA_SOURCES = ["GHCNh", "GSDR"]

_load_1h_concat: Dict[
    str,
    Callable[
        ...,
        Tuple[pd.DataFrame, pd.DataFrame],
    ],
] = {
    "GHCNh": _load_ghcnh_concat_pr(),
    "GSDR": load_gsdr_concat,
}

_load_1h_attrs: Dict[
    str,
    Callable[
        ...,
        Tuple[Dict[str, pd.DataFrame], pd.DataFrame],
    ],
] = {
    "GHCNh": _load_ghcnh_attrs_pr(),
    "GSDR": _load_gsdr_df,
}

_load_1h_noattrs: Dict[
    str,
    Callable[
        ...,
        Tuple[Dict[str, pd.Series], pd.DataFrame],
    ],
] = {
    "GHCNh": _load_ghcnh_noattrs_pr(),
    "GSDR": _load_gsdr_series,
}

_GAUGE_DATA_SOURCES_PER_PERIOD: Dict[
    str,
    List[str],
] = {
    "precipitation_1_day": ["GHCNd", "GHCNh"],
    "precipitation_21_hour": ["GHCNh"],
    "precipitation_18_hour": ["GHCNh"],
    "precipitation_15_hour": ["GHCNh"],
    "precipitation_12_hour": ["GHCNh"],
    "precipitation_9_hour": ["GHCNh"],
    "precipitation_6_hour": ["GHCNh"],
    "precipitation_3_hour": ["GHCNh"],
    "precipitation_1_hour": _1H_GAUGE_DATA_SOURCES,
}

_load_all_concat: Dict[
    str,
    Dict[
        str,
        Callable[
            ...,
            Tuple[pd.DataFrame, pd.DataFrame],
        ],
    ],
] = {
    "precipitation_1_day": {
        "GHCNd": _load_ghcnd_concat_pr,
        "GHCNh": _load_ghcnh_concat_pr("24_hour"),
    },
    "precipitation_21_hour": {
        "GHCNh": _load_ghcnh_concat_pr("21_hour"),
    },
    "precipitation_18_hour": {
        "GHCNh": _load_ghcnh_concat_pr("18_hour"),
    },
    "precipitation_15_hour": {
        "GHCNh": _load_ghcnh_concat_pr("15_hour"),
    },
    "precipitation_12_hour": {
        "GHCNh": _load_ghcnh_concat_pr("12_hour"),
    },
    "precipitation_9_hour": {
        "GHCNh": _load_ghcnh_concat_pr("9_hour"),
    },
    "precipitation_6_hour": {
        "GHCNh": _load_ghcnh_concat_pr("6_hour"),
    },
    "precipitation_3_hour": {
        "GHCNh": _load_ghcnh_concat_pr("3_hour"),
    },
    "precipitation_1_hour": _load_1h_concat,
}

_load_all_attrs: Dict[
    str,
    Dict[
        str,
        Callable[
            ...,
            Tuple[Dict[str, pd.DataFrame], pd.DataFrame],
        ],
    ],
] = {
    "precipitation_1_day": {
        "GHCNd": _load_ghcnd_attrs_pr,
        "GHCNh": _load_ghcnh_attrs_pr("24_hour"),
    },
    "precipitation_21_hour": {
        "GHCNh": _load_ghcnh_attrs_pr("21_hour"),
    },
    "precipitation_18_hour": {
        "GHCNh": _load_ghcnh_attrs_pr("18_hour"),
    },
    "precipitation_15_hour": {
        "GHCNh": _load_ghcnh_attrs_pr("15_hour"),
    },
    "precipitation_12_hour": {
        "GHCNh": _load_ghcnh_attrs_pr("12_hour"),
    },
    "precipitation_9_hour": {
        "GHCNh": _load_ghcnh_attrs_pr("9_hour"),
    },
    "precipitation_6_hour": {
        "GHCNh": _load_ghcnh_attrs_pr("6_hour"),
    },
    "precipitation_3_hour": {
        "GHCNh": _load_ghcnh_attrs_pr("3_hour"),
    },
    "precipitation_1_hour": _load_1h_attrs,
}

_load_all_noattrs: Dict[
    str,
    Dict[
        str,
        Callable[
            ...,
            Tuple[Dict[str, pd.Series], pd.DataFrame],
        ],
    ],
] = {
    "precipitation_1_day": {
        "GHCNd": _load_ghcnd_noattrs_pr,
        "GHCNh": _load_ghcnh_noattrs_pr("24_hour"),
    },
    "precipitation_21_hour": {
        "GHCNh": _load_ghcnh_noattrs_pr("21_hour"),
    },
    "precipitation_18_hour": {
        "GHCNh": _load_ghcnh_noattrs_pr("18_hour"),
    },
    "precipitation_15_hour": {
        "GHCNh": _load_ghcnh_noattrs_pr("15_hour"),
    },
    "precipitation_12_hour": {
        "GHCNh": _load_ghcnh_noattrs_pr("12_hour"),
    },
    "precipitation_9_hour": {
        "GHCNh": _load_ghcnh_noattrs_pr("9_hour"),
    },
    "precipitation_6_hour": {
        "GHCNh": _load_ghcnh_noattrs_pr("6_hour"),
    },
    "precipitation_3_hour": {
        "GHCNh": _load_ghcnh_noattrs_pr("3_hour"),
    },
    "precipitation_1_hour": _load_1h_noattrs,
}


def _renamer(source: str) -> Callable[[str], str]:
    """
    Return a callable that aims at adding the ``'source:'`` prefix to the argument.
    """

    def mapper(station_id):
        return f"{source}:{station_id}"  # noqa: E231

    return mapper


def search_1h_gauge_data(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = _1H_GAUGE_DATA_SOURCES,
    memory_est: str = "none",
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """
    Search hourly rain gauge station matching the search criteria from multiple sources.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes. Sources can be
    selected using ``usesources``, the default corresponding to looking into every
    matching station across all available sources.


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
    memory_est
        A string indicating how to estimate memory usage.
        Available strings are 'none' (no memory estimate), 'attrs' and 'noattrs', which
        yield identical results since GSDR data does not have attributes.

    Returns
    -------
    (metadata, ghcnh_inventory) : Tuple[DataFrame, DataFrame | None]
        ``metadata`` contains the metadata for all station matching the search, with
        (if requested with the ``memory_est`` argument)
        a 'memory_est' attribute providing an estimation of the memory usage, once the
        associated data is loaded with ``pandas``. ``ghcnh_inventory`` contains the
        inventory associated with GHCNh stations (if any), i.e., the number of records
        per year and per station.

    Raises
    ------
    ValueError
        If the sources or ``memory_est`` are not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if any([source not in _1H_GAUGE_DATA_SOURCES for source in usesources]):
        raise ValueError(
            f"{usesources!r} is not valid."
            + f" Please provide sources of {_1H_GAUGE_DATA_SOURCES!r}."
        )
    if time_range:
        time_range = localize_time_range(time_range)
    if memory_est not in {"none", "attrs", "noattrs"}:
        raise ValueError(
            f"Provided `memory_est` ({memory_est!r}) is not valid."
            + f" Accepted values are in {['none', 'attrs', 'noattrs']!r}."
        )

    # Core
    all_metadata: List[pd.DataFrame] = []
    ghcnh_inventory: Optional[pd.DataFrame] = None
    if memory_est != "none":
        est_bytes = 0
    for source in usesources:
        # Search
        if source == "GHCNh":
            metadata, ghcnh_inventory = _search_ghcnh_pr()(
                filter_condition=filter_condition,
                time_range=time_range,
                memory_est=memory_est,
            )

        elif source == "GSDR":
            metadata = search_gsdr(
                filter_condition=filter_condition,
                time_range=time_range,
                memory_est=memory_est,
            )

        # Add the source as a prefix to the station ID
        metadata = metadata.rename(_renamer(source))
        if source == "GHCNh":
            assert isinstance(ghcnh_inventory, pd.DataFrame)
            ghcnh_inventory = ghcnh_inventory.rename(_renamer(source))
            if "memory_est" in ghcnh_inventory.attrs:
                del ghcnh_inventory.attrs["memory_est"]

        # Memory
        if memory_est != "none" and "memory_est" in metadata.attrs:
            est_bytes += metadata.attrs["memory_est"]

        # Append
        all_metadata.append(metadata)

    # Concat
    combined_metadata = pd.concat(all_metadata, axis=0, copy=False).sort_index()

    # Memory
    if memory_est != "none":
        log.info(
            f"Memory estimate when loading with `pandas`: {format_memory(est_bytes)}."
        )
        combined_metadata.attrs.update(dict(memory_est=est_bytes))

    return combined_metadata, ghcnh_inventory


def load_1h_gauge_data_concat(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = list(_load_1h_concat.keys()),
    units: str = "mm",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load hourly rain gauge data as a concatenated DataFrame, with associated station
    metadata, from multiple sources.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes. Sources can be
    selected using ``usesources``, the default corresponding to looking into every
    matching station across all available sources. ``units`` allows to choose the output
    unit for rain gauge data.

    .. attention::

       Do not use this function (with the ``_concat`` suffix) unless you are sure to
       have enough memory available for the data you are searching. See
       :func:`search_1h_gauge_data` to get a memory estimate of the data matching your
       search criteria.


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
    (data, metadata) : Tuple[pd.DataFrame, pd.DataFrame]
        ``data`` is a single concatenated DataFrame, with station IDs as columns and
        dates as index. ``metadata`` contains the metadata for all associated stations.

    Raises
    ------
    ValueError
        If the sources are not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if any([source not in _load_1h_concat.keys() for source in usesources]):
        raise ValueError(
            f"{usesources!r} is not valid."
            + f" Please provide sources of {list(_load_1h_concat.keys())!r}."
        )
    if time_range:
        time_range = localize_time_range(time_range)

    # Core
    all_data: List[pd.DataFrame] = []
    all_metadata: List[pd.DataFrame] = []
    for source in usesources:
        # Load
        data, metadata = _load_1h_concat[source](
            filter_condition=filter_condition,
            time_range=time_range,
        )

        # Add the source as a prefix to the station ID
        data = data.rename(_renamer(source), axis=1)
        metadata = metadata.rename(_renamer(source))

        # Units
        data = check_dataframe_unit(data, target_unit=units)

        # Append
        all_data.append(data)
        all_metadata.append(metadata)

    # Concat
    combined_metadata = pd.concat(all_metadata, axis=0, copy=False).sort_index()
    combined_data = pd.concat(all_data, axis=1, copy=False)
    combined_data = combined_data.loc[:, combined_metadata.index.to_list()]

    return combined_data, combined_metadata


def load_1h_gauge_data_attrs(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = list(_load_1h_attrs.keys()),
    units: str = "mm",
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Load hourly rain gauge data as a dictionary of DataFrames containing precipitation
    data as well as attributes (if relevant), with associated station metadata, from
    multiple sources.

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
    (data, metadata) : Tuple[Dict[str, pd.DataFrame], pd.DataFrame]
        ``data`` is a dictionary of DataFrame, with station IDs as keys and dates as
        DataFrame index. ``metadata`` contains the metadata for all associated stations.

    Raises
    ------
    ValueError
        If the sources are not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if any([source not in _load_1h_attrs.keys() for source in usesources]):
        raise ValueError(
            f"{usesources!r} is not valid."
            + f" Please provide sources of {list(_load_1h_attrs.keys())!r}."
        )
    if time_range:
        time_range = localize_time_range(time_range)

    # Core
    data_dict: Dict[str, pd.DataFrame] = {}
    all_metadata: List[pd.DataFrame] = []
    for source in usesources:
        # Load
        data, metadata = _load_1h_attrs[source](
            filter_condition=filter_condition,
            time_range=time_range,
        )

        # Update/Append & Add the source as a prefix to the station ID
        data_dict.update(
            {
                _renamer(source)(k): check_dataframe_unit(v, target_unit=units)
                for k, v in data.items()
            }
        )
        all_metadata.append(metadata.rename(_renamer(source)))

    # Concat metadata
    combined_metadata = pd.concat(all_metadata, axis=0, copy=False).sort_index()

    return data_dict, combined_metadata


def load_1h_gauge_data_noattrs(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = list(_load_1h_noattrs.keys()),
    units: str = "mm",
) -> Tuple[Dict[str, pd.Series], pd.DataFrame]:
    """
    Load hourly rain gauge data as a dictionary of Series, with associated station
    metadata, from multiple sources.

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
    (data, metadata) : Tuple[Dict[str, pd.Series], pd.DataFrame]
        ``data`` is a dictionary of Series, with station IDs as keys and dates as Series
        index. ``metadata`` contains the metadata for all associated stations.

    Raises
    ------
    ValueError
        If the sources are not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if any([source not in _load_1h_noattrs.keys() for source in usesources]):
        raise ValueError(
            f"{usesources!r} is not valid."
            + f" Please provide sources of {list(_load_1h_noattrs.keys())!r}."
        )
    if time_range:
        time_range = localize_time_range(time_range)

    # Core
    data_dict: Dict[str, pd.Series] = {}
    all_metadata: List[pd.DataFrame] = []
    for source in usesources:
        # Load
        data, metadata = _load_1h_noattrs[source](
            filter_condition=filter_condition,
            time_range=time_range,
        )

        # Update/Append & Add the source as a prefix to the station ID
        data_dict.update(
            {
                _renamer(source)(k): check_dataframe_unit(v, target_unit=units)
                for k, v in data.items()
            }
        )
        all_metadata.append(metadata.rename(_renamer(source)))

    # Concat metadata
    combined_metadata = pd.concat(all_metadata, axis=0, copy=False).sort_index()

    return data_dict, combined_metadata


def _attrs(period: str, units: str) -> Dict[Hashable | None, Any]:
    time = " ".join(period.split("_")[1:])
    return dict(
        name=f"{time} total liquid precipitation",
        long_name=f"{time} total liquid precipitation accumulation",
        units=units,
    )


def search_all_gauge_data(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = _ALL_GAUGE_DATA_SOURCES,
    memory_est: str = "none",
) -> Tuple[pd.DataFrame, Dict[str, Optional[pd.DataFrame]]]:
    """
    Search rain gauge stations matching the search criteria, from multiple sources and
    all accumulation periods.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes. Sources can be
    selected using ``usesources``, the default corresponding to looking into every
    matching station across all available sources.

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
    memory_est
        A string indicating how to estimate memory usage.
        Available strings are 'none' (no memory estimate), 'attrs' and 'noattrs', which
        yield identical results since GSDR data does not have attributes.

    Returns
    -------
    (metadata, ghcnh_inventory) : Tuple[DataFrame, Dict[str, DataFrame | None]]
        ``metadata`` contains the metadata for all station matching the search, with
        (if requested with the ``memory_est`` argument)
        a 'memory_est' attribute providing an estimation of the memory usage, once the
        associated data is loaded with ``pandas``. ``ghcnh_inventory`` contains the
        inventory associated with GHCNh stations (if any) for all accumulation periods,
        i.e., the number of records per year and per station.

    Raises
    ------
    ValueError
        If the sources or ``memory_est`` are not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if any([source not in _ALL_GAUGE_DATA_SOURCES for source in usesources]):
        raise ValueError(
            f"{usesources!r} is not valid."
            + f" Please provide sources of {_ALL_GAUGE_DATA_SOURCES!r}."
        )
    if time_range:
        time_range = localize_time_range(time_range)
    if memory_est not in {"none", "attrs", "noattrs"}:
        raise ValueError(
            f"Provided `memory_est` ({memory_est!r}) is not valid."
            + f" Accepted values are in {['none', 'attrs', 'noattrs']!r}."
        )

    # Core
    all_metadata: Dict[str, pd.DataFrame] = {}
    ghcnh_inventory: Dict[str, Optional[pd.DataFrame]] = {}
    if memory_est != "none":
        est_bytes = 0
    for period, sources in _GAUGE_DATA_SOURCES_PER_PERIOD.items():

        period_metadata: List[pd.DataFrame] = []
        period_ghcnh_inventory: Optional[pd.DataFrame] = None
        for source in sources:
            if source not in usesources:
                continue

            # Search
            if source == "GHCNh":
                metadata, period_ghcnh_inventory = _search_ghcnh_pr(
                    ""
                    if period == "precipitation_1_hour"
                    else (
                        "24_hour"
                        if period == "precipitation_1_day"
                        else period.split("_", 1)[-1]
                    )
                )(
                    filter_condition=filter_condition,
                    time_range=time_range,
                    memory_est=memory_est,
                )

            elif source == "GHCNd":
                metadata = _search_ghcnd_pr(
                    filter_condition=filter_condition,
                    time_range=time_range,
                    memory_est=memory_est,
                )

            elif source == "GSDR":
                metadata = search_gsdr(
                    filter_condition=filter_condition,
                    time_range=time_range,
                    memory_est=memory_est,
                )

            # Add the source as a prefix to the station ID
            metadata = metadata.rename(_renamer(source))
            if source == "GHCNh":
                assert isinstance(period_ghcnh_inventory, pd.DataFrame)
                period_ghcnh_inventory = period_ghcnh_inventory.rename(_renamer(source))
                if "memory_est" in period_ghcnh_inventory.attrs:
                    del period_ghcnh_inventory.attrs["memory_est"]

            # Memory
            if memory_est != "none" and "memory_est" in metadata.attrs:
                est_bytes += metadata.attrs["memory_est"]

            # Append
            period_metadata.append(metadata)

        # Concat
        all_metadata[period] = pd.concat(
            period_metadata, axis=0, copy=False
        ).sort_index()
        ghcnh_inventory[period] = period_ghcnh_inventory

    # Concat all metadata
    combined_metadata = (
        pd.concat(list(all_metadata.values()), axis=0).drop_duplicates().sort_index()
    )

    # Memory
    if memory_est != "none":
        log.info(
            f"Memory estimate when loading with `pandas`: {format_memory(est_bytes)}."
        )
        combined_metadata.attrs.update(dict(memory_est=est_bytes))

    return combined_metadata, ghcnh_inventory


def load_all_gauge_data_concat(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = _ALL_GAUGE_DATA_SOURCES,
    units: str = "mm",
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Load rain gauge data per accumulation period as concatenated DataFrames, with
    associated station metadata, from multiple sources.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes. Sources can be
    selected using ``usesources``, the default corresponding to looking into every
    matching station across all available sources. ``units`` allows to choose the output
    unit for rain gauge data.

    .. attention::

       Do not use this function (with the ``_concat`` suffix) unless you are sure to
       have enough memory available for the data you are searching. See
       :func:`search_all_gauge_data` to get a memory estimate of the data matching your
       search criteria.


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
    (data, metadata) : Tuple[Dict[str, pd.DataFrame], pd.DataFrame]
        ``data`` contains gauge data stored by accumulation period in a dictionary.
        Dictionary values are Dataframes, with station IDs as columns and dates as
        index. ``metadata`` contains the metadata for all associated stations.

    Raises
    ------
    ValueError
        If the sources are not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if any([source not in _ALL_GAUGE_DATA_SOURCES for source in usesources]):
        raise ValueError(
            f"{usesources!r} is not valid."
            + f" Please provide sources of {_ALL_GAUGE_DATA_SOURCES!r}."
        )
    if time_range:
        time_range = localize_time_range(time_range)

    # Core
    combined_data: Dict[str, pd.DataFrame] = {}
    all_metadata: Dict[str, pd.DataFrame] = {}
    for period, dispatcher in _load_all_concat.items():

        period_data: List[pd.DataFrame] = []
        period_metadata: List[pd.DataFrame] = []
        for source, load in dispatcher.items():
            if source not in usesources:
                continue

            # Load
            data, metadata = load(
                filter_condition=filter_condition,
                time_range=time_range,
            )

            # Add the source as a prefix to the station ID
            data = data.rename(_renamer(source), axis=1)
            metadata = metadata.rename(_renamer(source))

            # Units
            data = check_dataframe_unit(data, target_unit=units)

            # Append
            period_data.append(data)
            period_metadata.append(metadata)

        # Concat
        all_metadata[period] = pd.concat(period_metadata, axis=0).sort_index()
        combined_data[period] = pd.concat(period_data, axis=1)
        combined_data[period] = combined_data[period].loc[
            :, all_metadata[period].index.to_list()
        ]

        # Attrs
        combined_data[period].attrs.update(_attrs(period, units))

    # Concat all metadata
    combined_metadata = (
        pd.concat(list(all_metadata.values()), axis=0).drop_duplicates().sort_index()
    )

    return combined_data, combined_metadata


def load_all_gauge_data_attrs(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = _ALL_GAUGE_DATA_SOURCES,
    units: str = "mm",
) -> Tuple[Dict[str, Dict[str, pd.DataFrame]], pd.DataFrame]:
    """
    Load rain gauge data per accumulation period as dictionaries of DataFrames
    containing precipitation data as well as attributes, with associated station
    metadata, from multiple sources.

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
    (data, metadata) : Tuple[Dict[str, Dict[str, pd.DataFrame]], pd.DataFrame]
        ``data`` contains gauge data stored by accumulation period in a dictionary.
        Dictionary values are themselves dictionaries of Dataframes, with station IDs as
        keys and dates as DataFrame index. ``metadata`` contains the metadata for all
        associated stations.

    Raises
    ------
    ValueError
        If the sources are not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if any([source not in _ALL_GAUGE_DATA_SOURCES for source in usesources]):
        raise ValueError(
            f"{usesources!r} is not valid."
            + f" Please provide sources of {_ALL_GAUGE_DATA_SOURCES!r}."
        )
    if time_range:
        time_range = localize_time_range(time_range)

    # Core
    combined_data: Dict[str, Dict[str, pd.DataFrame]] = {}
    all_metadata: Dict[str, pd.DataFrame] = {}
    for period, dispatcher in _load_all_attrs.items():

        period_data_dict: Dict[str, pd.DataFrame] = {}
        period_metadata: List[pd.DataFrame] = []
        for source, load in dispatcher.items():
            if source not in usesources:
                continue

            # Load
            data, metadata = load(
                filter_condition=filter_condition,
                time_range=time_range,
            )

            for k, v in data.items():
                # Units
                _v = check_dataframe_unit(v, target_unit=units)

                # Attrs
                _v.attrs.update(_attrs(period, units))

                # Update data & Add the source as a prefix to the station ID
                period_data_dict[_renamer(source)(k)] = _v

            # Append metadata
            period_metadata.append(metadata.rename(_renamer(source)))

        # Concat/Update
        all_metadata[period] = pd.concat(period_metadata, axis=0).sort_index()
        combined_data[period] = period_data_dict

    # Concat all metadata
    combined_metadata = (
        pd.concat(list(all_metadata.values()), axis=0).drop_duplicates().sort_index()
    )

    return combined_data, combined_metadata


def load_all_gauge_data_noattrs(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = _ALL_GAUGE_DATA_SOURCES,
    units: str = "mm",
) -> Tuple[Dict[str, Dict[str, pd.Series]], pd.DataFrame]:
    """
    Load rain gauge data per accumulation period as dictionaries of Series, with
    associated station metadata, from multiple sources.

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
    (data, metadata) : Tuple[Dict[str, Dict[str, pd.Series]], pd.DataFrame]
        ``data`` contains gauge data stored by accumulation period in a dictionary.
        Dictionary values are themselves dictionaries of Series, with station IDs as
        keys and dates as Series index. ``metadata`` contains the metadata for all
        associated stations.

    Raises
    ------
    ValueError
        If the sources are not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if any([source not in _ALL_GAUGE_DATA_SOURCES for source in usesources]):
        raise ValueError(
            f"{usesources!r} is not valid."
            + f" Please provide sources of {_ALL_GAUGE_DATA_SOURCES!r}."
        )
    if time_range:
        time_range = localize_time_range(time_range)

    # Core
    combined_data: Dict[str, Dict[str, pd.Series]] = {}
    all_metadata: Dict[str, pd.DataFrame] = {}
    for period, dispatcher in _load_all_noattrs.items():

        period_data_dict: Dict[str, pd.Series] = {}
        period_metadata: List[pd.DataFrame] = []
        for source, load in dispatcher.items():
            if source not in usesources:
                continue

            # Load
            data, metadata = load(
                filter_condition=filter_condition,
                time_range=time_range,
            )

            for k, v in data.items():
                # Units
                _v = check_dataframe_unit(v, target_unit=units)

                # Attrs
                _v.attrs.update(_attrs(period, units))

                # Update data & Add the source as a prefix to the station ID
                period_data_dict[_renamer(source)(k)] = _v

            # Append metadata
            period_metadata.append(metadata.rename(_renamer(source)))

        # Concat/Update
        all_metadata[period] = pd.concat(period_metadata, axis=0).sort_index()
        combined_data[period] = period_data_dict

    # Concat all metadata
    combined_metadata = (
        pd.concat(list(all_metadata.values()), axis=0).drop_duplicates().sort_index()
    )

    return combined_data, combined_metadata


def load_1h_gauge_data(*args, **kwargs):
    """Alias for :func:`load_1h_gauge_data_noattrs`."""
    return load_1h_gauge_data_noattrs(*args, **kwargs)


def load_all_gauge_data(*args, **kwargs):
    """Alias for :func:`load_all_gauge_data_noattrs`."""
    return load_all_gauge_data_noattrs(*args, **kwargs)

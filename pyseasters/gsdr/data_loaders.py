import logging
from datetime import datetime
from typing import Dict, Optional, Tuple

import pandas as pd

from pyseasters.constants import paths
from pyseasters.utils._args import localize_time_range

from .metadata_loaders import search_gsdr

__all__ = [
    "load_gsdr",
    "load_gsdr_concat",
    "load_gsdr_single_station",
]

log = logging.getLogger(__name__)


def _load_gsdr_single_station_df(
    station_id: str,
) -> pd.DataFrame:
    """
    Load data from the single GSDR file associated with ``station_id`` as a DataFrame.
    """
    df = pd.read_parquet(paths.gsdr_file(station_id)).dropna()
    df.attrs.update(dict(name="Precipitation", long_name="Precipitation", units="mm"))
    return df


def _load_gsdr_single_station_series(
    station_id: str,
) -> pd.Series:
    """
    Load data from the single GSDR file associated with ``station_id`` as a Series.
    """
    df = _load_gsdr_single_station_df(station_id)
    return df["Precipitation"]


load_gsdr_single_station = _load_gsdr_single_station_series


def _load_gsdr_df(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """Load GSDR data as a dictionary of DataFrames, with associated station metadata.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    Parameters
    ----------
    filter_condition
        An optional query string to filter the station metadata.
        Available core attributes are 'station_id', 'lon', 'lat', 'elevation'
        and 'station_name'. Additional attributes are 'original_timestep',
        'original_units', 'daylight_saving_info', 'resolution' and
        'percent_missing_data'.
    time_range
        An optional time range for selecting time coverage.

    Returns
    -------
    (data, metadata) : Tuple[Dict[str, DataFrame], DataFrame]
        ``data`` is a dictionary of DataFrames, with station IDs as keys and dates as
        DataFrame index. ``metadata`` contains the metadata for all associated stations.

    Raises
    ------
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if time_range:
        time_range = localize_time_range(time_range)

    # Search
    metadata = search_gsdr(filter_condition=filter_condition, time_range=time_range)

    # Load data for the selected stations and refine the time range filtering
    data_dict: Dict[str, pd.DataFrame] = {}
    for station in metadata.index:
        if time_range:
            data_dict[station] = _load_gsdr_single_station_df(station).loc[
                pd.Timestamp(time_range[0]) : pd.Timestamp(time_range[1])  # noqa: E203
            ]
        else:
            data_dict[station] = _load_gsdr_single_station_df(
                station,
            )

    return data_dict, metadata


def _load_gsdr_series(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
) -> Tuple[Dict[str, pd.Series], pd.DataFrame]:
    """Load GSDR data as a dictionary of Series, with associated station metadata.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    Parameters
    ----------
    filter_condition
        An optional query string to filter the station metadata.
        Available core attributes are 'station_id', 'lon', 'lat', 'elevation'
        and 'station_name'. Additional attributes are 'original_timestep',
        'original_units', 'daylight_saving_info', 'resolution' and
        'percent_missing_data'.
    time_range
        An optional time range for selecting time coverage.

    Returns
    -------
    (data, metadata) : Tuple[Dict[str, Series], DataFrame]
        ``data`` is a dictionary of Series, with station IDs as keys and dates as Series
        index. ``metadata`` contains the metadata for all associated stations.

    Raises
    ------
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if time_range:
        time_range = localize_time_range(time_range)

    # Search
    metadata = search_gsdr(filter_condition=filter_condition, time_range=time_range)

    # Load data for the selected stations and refine the time range filtering
    data_dict: Dict[str, pd.Series] = {}
    for station in metadata.index:
        if time_range:
            data_dict[station] = _load_gsdr_single_station_series(station).loc[
                pd.Timestamp(time_range[0]) : pd.Timestamp(time_range[1])  # noqa: E203
            ]
        else:
            data_dict[station] = _load_gsdr_single_station_series(
                station,
            )

    return data_dict, metadata


load_gsdr = _load_gsdr_series


def load_gsdr_concat(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load GSDR data as a concatenated DataFrame, with associated station metadata.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    .. attention::

       Do not use this function (with the ``_concat`` suffix) unless you are sure to
       have enough memory available for the data you are searching. See
       :func:`~pyseasters.gsdr.metadata_loaders.search_gsdr` to get a
       memory estimate of the data matching your search criteria.


    Parameters
    ----------
    filter_condition
        An optional query string to filter the station metadata.
        Available core attributes are 'station_id', 'lon', 'lat', 'elevation'
        and 'station_name'. Additional attributes are 'original_timestep',
        'original_units', 'daylight_saving_info', 'resolution' and
        'percent_missing_data'.
    time_range
        An optional time range for selecting time coverage.

    Returns
    -------
    (data, metadata) : Tuple[DataFrame, DataFrame]
        ``data`` is a single concatenated DataFrame, with station IDs as columns and
        dates as index. ``metadata`` contains the metadata for all associated stations.

    Raises
    ------
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    data_dict, metadata = _load_gsdr_series(
        filter_condition=filter_condition, time_range=time_range
    )
    data_df = pd.concat(data_dict, axis=1, copy=False)
    return data_df, metadata

import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from pyseasters.constants import paths
from pyseasters.utils._args import localize_time_range

from ._constants import VAR_TO_META
from .metadata_loaders import search_ghcnh

__all__ = [
    "load_ghcnh",
    "load_ghcnh_attrs",
    "load_ghcnh_concat",
    "load_ghcnh_noattrs",
    "load_ghcnh_single_var_station",
    "load_ghcnh_single_var_station_attrs",
    "load_ghcnh_single_var_station_noattrs",
]

log = logging.getLogger(__name__)


def load_ghcnh_single_var_station_attrs(
    station_id: str,
    var: str,
    year_list: List[int],
) -> pd.DataFrame:
    """
    Load ``var`` data and attributes from the GHCNh files associated with ``station_id``
    and ``year_list``.
    """
    df = pd.concat(
        [
            pd.read_parquet(paths.ghcnh_file(station_id, year, var)).dropna(how="all")
            for year in year_list
        ],
        axis=0,
        copy=False,
    )
    df.attrs.update(VAR_TO_META[var])
    return df


def load_ghcnh_single_var_station_noattrs(
    station_id: str,
    var: str,
    year_list: List[int],
) -> pd.Series:
    """
    Load ``var`` data from the GHCNh files associated with ``station_id`` and
    ``year_list``.
    """
    df = pd.concat(
        [
            pd.read_parquet(
                paths.ghcnh_file(station_id, year, var), columns=[var]
            ).dropna(how="all")
            for year in year_list
        ],
        axis=0,
        copy=False,
    )
    df.attrs.update(VAR_TO_META[var])
    return df[var]


def load_ghcnh_attrs(
    var: str,
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Load GHCNh data for a given variable as a dictionary of DataFrames containing the
    variable as well as attributes, with associated station metadata.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    Parameters
    ----------
    var
        The variable to load.
    filter_condition
        An optional query string to filter the station metadata.
        Available attributes are 'station_id', 'lon', 'lat', 'elevation'
        and 'station_name'.
    time_range
        An optional time range for selecting time coverage.

    Returns
    -------
    (data, metadata) : Tuple[Dict[str, DataFrame], DataFrame]
        ``data`` is a dictionary of DataFrame, with station IDs as keys and dates as
        DataFrame index. ``metadata`` contains the metadata for all associated stations.

    Raises
    ------
    ValueError
        If ``var`` is not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if var not in VAR_TO_META.keys():
        raise ValueError(
            f"Provided `var` ({var!r}) is not valid."
            + f" Accepted values are in {list(VAR_TO_META.keys())!r}."
        )
    if time_range:
        time_range = localize_time_range(time_range)

    # Search
    station_list, inventory = search_ghcnh(
        var, filter_condition=filter_condition, time_range=time_range
    )

    # Load data for the selected stations and refine the time range filtering
    data_dict: Dict[str, pd.DataFrame] = {}
    for station in station_list.index:
        if time_range:
            data_dict[station] = load_ghcnh_single_var_station_attrs(
                station,
                var,
                year_list=inventory.loc[station].dropna().index.to_list(),
            ).loc[
                pd.Timestamp(time_range[0]) : pd.Timestamp(time_range[1])  # noqa: E203
            ]
        else:
            data_dict[station] = load_ghcnh_single_var_station_attrs(
                station,
                var,
                year_list=inventory.loc[station].dropna().index.to_list(),
            )

    return data_dict, station_list


def load_ghcnh_noattrs(
    var: str,
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
) -> Tuple[Dict[str, pd.Series], pd.DataFrame]:
    """
    Load GHCNh data for a given variable as a dictionary of Series, with associated
    station metadata.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    Parameters
    ----------
    var
        The variable to load.
    filter_condition
        An optional query string to filter the station metadata.
        Available attributes are 'station_id', 'lon', 'lat', 'elevation'
        and 'station_name'.
    time_range
        An optional time range for selecting time coverage.

    Returns
    -------
    (data, metadata) : Tuple[Dict[str, Series], DataFrame]
        ``data`` is a dictionary of Series, with station IDs as keys and dates as Series
        index. ``metadata`` contains the metadata for all associated stations.

    Raises
    ------
    ValueError
        If ``var`` is not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if var not in VAR_TO_META.keys():
        raise ValueError(
            f"Provided `var` ({var!r}) is not valid."
            + f" Accepted values are in {list(VAR_TO_META.keys())!r}."
        )
    if time_range:
        time_range = localize_time_range(time_range)

    # Search
    station_list, inventory = search_ghcnh(
        var, filter_condition=filter_condition, time_range=time_range
    )

    # Load data for the selected stations and refine the time range filtering
    data_dict: Dict[str, pd.Series] = {}
    for station in station_list.index:
        if time_range:
            data_dict[station] = load_ghcnh_single_var_station_noattrs(
                station,
                var,
                year_list=inventory.loc[station].dropna().index.to_list(),
            ).loc[
                pd.Timestamp(time_range[0]) : pd.Timestamp(time_range[1])  # noqa: E203
            ]
        else:
            data_dict[station] = load_ghcnh_single_var_station_noattrs(
                station,
                var,
                year_list=inventory.loc[station].dropna().index.to_list(),
            )

    return data_dict, station_list


def load_ghcnh_concat(
    var: str,
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load GHCNh data for a given variable as a concatenated DataFrame, with associated
    station metadata.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    .. attention::

       Do not use this function (with the ``_concat`` suffix) unless you are sure to
       have enough memory available for the data you are searching. See
       :func:`~pyseasters.ghcnh.metadata_loaders.search_ghcnh` to get a
       memory estimate of the data matching your search criteria.


    Parameters
    ----------
    var
        The variable to load.
    filter_condition
        An optional query string to filter the station metadata.
        Available attributes are 'station_id', 'lon', 'lat', 'elevation'
        and 'station_name'.
    time_range
        An optional time range for selecting time coverage.

    Returns
    -------
    (data, metadata) : Tuple[DataFrame, DataFrame]
        ``data`` is a single concatenated DataFrame, with station IDs as columns and
        dates as index. ``metadata`` contains the metadata for all associated stations.

    Raises
    ------
    ValueError
        If ``var`` is not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    data_dict, station_list = load_ghcnh_noattrs(
        var, filter_condition=filter_condition, time_range=time_range
    )
    data_df = pd.concat(data_dict, axis=1, copy=False)
    return data_df, station_list


def _load_ghcnh_concat_pr(
    period: str = "",
) -> Callable[
    ...,
    Tuple[pd.DataFrame, pd.DataFrame],
]:

    def func(
        filter_condition: Optional[str] = None,
        time_range: Optional[Tuple[datetime, datetime]] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return load_ghcnh_concat(
            "precipitation" + (f"_{period}" if period else ""),
            filter_condition=filter_condition,
            time_range=time_range,
        )

    return func


def _load_ghcnh_attrs_pr(
    period: str = "",
) -> Callable[
    ...,
    Tuple[Dict[str, pd.DataFrame], pd.DataFrame],
]:

    def func(
        filter_condition: Optional[str] = None,
        time_range: Optional[Tuple[datetime, datetime]] = None,
    ) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame]:
        return load_ghcnh_attrs(
            "precipitation" + (f"_{period}" if period else ""),
            filter_condition=filter_condition,
            time_range=time_range,
        )

    return func


def _load_ghcnh_noattrs_pr(
    period: str = "",
) -> Callable[
    ...,
    Tuple[Dict[str, pd.Series], pd.DataFrame],
]:

    def func(
        filter_condition: Optional[str] = None,
        time_range: Optional[Tuple[datetime, datetime]] = None,
    ) -> Tuple[Dict[str, pd.Series], pd.DataFrame]:
        return load_ghcnh_noattrs(
            "precipitation" + (f"_{period}" if period else ""),
            filter_condition=filter_condition,
            time_range=time_range,
        )

    return func


def load_ghcnh_single_var_station(*args, **kwargs):
    """Alias for :func:`load_ghcnh_single_var_station_noattrs`."""
    return load_ghcnh_single_var_station_noattrs(*args, **kwargs)


def load_ghcnh(*args, **kwargs):
    """Alias for :func:`load_ghcnh_noattrs`."""
    return load_ghcnh_noattrs(*args, **kwargs)

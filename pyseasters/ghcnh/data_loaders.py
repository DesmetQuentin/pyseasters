import importlib.resources
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml

from pyseasters.constants import paths

from .metadata_loaders import load_ghcnh_inventory, load_ghcnh_station_list

__all__ = ["load_ghcnh", "load_ghcnh_single_var_station"]

log = logging.getLogger(__name__)

with importlib.resources.files("pyseasters.ghcnh.data").joinpath(
    "var_metadata.yaml"
).open("r") as file:
    _VAR_TO_META = yaml.safe_load(file)

_ATTRIBUTES = [
    "_Measurement_Code",
    "_Quality_Code",
    "_Report_Type",
    "_Source_Code",
    "_Source_Station_ID",
]


def load_ghcnh_single_var_station(
    station_id: str,
    var: str,
    year_list: List[int],
    load_attributes: bool = True,
) -> pd.DataFrame | pd.Series:
    """Load ``var`` data GHCNh file associated with ``station_id`` and ``year_list``."""
    kws: Dict[str, Any] = {} if load_attributes else dict(columns=[var])
    df = pd.concat(
        [
            pd.read_parquet(paths.ghcnh_file(station_id, year, var), **kws).dropna(
                how="all"
            )
            for year in year_list
        ],
        axis=0,
        copy=False,
    )
    df.attrs.update(_VAR_TO_META[var])
    if not load_attributes:
        series = df[var]
        return series
    else:
        return df


def load_ghcnh(
    var: str = "precipitation",
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    concat: bool = False,
    load_attributes: bool = False,
) -> Tuple[pd.DataFrame | Dict[str, pd.Series | pd.DataFrame], pd.DataFrame]:
    """Load hourly GHCNh data and associated station metadata for a given variable.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    .. attention::

       Do not use ``concat=True`` unless you are sure to have enough memory available
       for the data you are searching.


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
    concat
        A boolean enabling concatenation of the data into one single DataFrame. Default
        is False. Note that ``load_attributes`` is incompatible with ``concat``. If both
        are enabled, ``concat`` will reset to False.
    load_attributes
        A boolean enabling loading the variable's attributes. Default is False. Note
        that ``load_attributes`` is incompatible with ``concat``. If both are enabled,
        ``concat`` will reset to False.


    Returns
    -------
    (data, metadata) : Tuple[DataFrame | Dict[str, Series | DataFrame], DataFrame]
        If ``concat`` is True: ``data`` is a single concatenated DataFrame, with station
        IDs as columns and dates as index; otherwise: ``data`` is a dictionary of
        ``pandas`` objects, the keys being the station IDs. Objects are DataFrames if
        ``load_attributes`` is True (with one column for the variables, then one per
        attribute); they are Series otherwise. In any case, ``metadata`` contains the
        metadata for all associated stations.

    Raises
    ------
    ValueError
        If ``var`` is not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """

    # Check arguments
    if var not in _VAR_TO_META.keys():
        raise ValueError(
            f"Provided `var` ({var!r}) is not valid."
            + f" Accepted values are in {list(_VAR_TO_META.keys())!r}."
        )
    if time_range:
        time_localized = [dt.tzinfo for dt in time_range]
        if not all(time_localized):
            if not any(time_localized):
                log.info("Time range datetimes assumed assumed to be in UTC...")
                time_range[0] = time_range[0].replace(tzinfo=timezone.utc)
                time_range[1] = time_range[1].replace(tzinfo=timezone.utc)
            elif not time_localized[0]:
                log.info(
                    "Time range start datetime is assumed to be in the same time "
                    + "zone as end datetime."
                )
                time_range[0] = time_range[0].replace(tzinfo=time_localized[1])
            else:
                log.info(
                    "Time range start datetime is assumed to be in the same time "
                    + "zone as start datetime."
                )
                time_range[1] = time_range[1].replace(tzinfo=time_localized[0])
    if concat and load_attributes:
        log.warning("Boolean arguments incompatible: `concat` and `load_attributes`.")
        log.warning("Now ignoring `concat`.")
        concat = False

    # Load the station list
    station_list = load_ghcnh_station_list()
    inventory = load_ghcnh_inventory(var=var).unstack()["count"]

    # Select the list of stations matching ``time_range`` and ``filter_condition``
    if time_range:
        year_list = list(np.arange(time_range[0].year, time_range[1].year + 1))
        inventory = inventory[list(map(str, year_list))].dropna()
        station_list = station_list.loc[inventory.index]

    if filter_condition:
        try:
            station_list = station_list.query(filter_condition)
        except Exception as e:
            raise RuntimeError(f"Error applying filter `{filter_condition}`: {e}")

    # Load data for the selected stations and refine the time range filtering
    if time_range:
        data_dict = {
            station: load_ghcnh_single_var_station(
                station,
                var,
                year_list=inventory.loc[station].dropna().index.to_list(),
                load_attributes=load_attributes,
            ).loc[
                pd.Timestamp(time_range[0]) : pd.Timestamp(time_range[1])  # noqa: E203
            ]
            for station in station_list.index
        }
    else:
        data_dict = {
            station: load_ghcnh_single_var_station(
                station,
                var,
                year_list=inventory.loc[station].dropna().index.to_list(),
                load_attributes=load_attributes,
            )
            for station in station_list.index
        }

    # Concat
    if concat:
        data_df = pd.concat(data_dict, axis=1, copy=False)
        return data_df, station_list
    else:
        return data_dict, station_list

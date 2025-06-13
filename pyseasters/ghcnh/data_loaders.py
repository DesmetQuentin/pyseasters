import importlib.resources
import logging
from datetime import datetime
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
) -> pd.DataFrame:
    """Load ``var`` data GHCNh file associated with ``station_id`` and ``year_list``."""
    kws: Dict[str, Any] = {} if load_attributes else dict(columns=[var])
    data = pd.concat(
        [
            pd.read_parquet(paths.ghcnh_file(station_id, year, var), **kws)
            for year in year_list
        ],
        axis=0,
    )
    return data


def load_ghcnh(
    var: str = "precipitation",
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load hourly GHCNh data and associated station metadata for a given variable.

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
    (data, metadata) : Tuple[DataFrame, DataFrame]
        A tuple of DataFrames, with (1) Gauge data, with station IDs as columns, dates
        as index and a 'units' attribute, and (2) Metadata for the associated stations.

    Raises
    ------
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """

    # Check arguments
    if var not in _VAR_TO_META.keys():
        raise ValueError(
            f"Provided `var` ({var!r}) is not valid."
            + f" Accepted values are in {list(_VAR_TO_META.keys())!r}."
        )

    # Load the station list
    station_list = load_ghcnh_station_list()
    inventory = load_ghcnh_inventory(var=var).unstack()["count"]

    # Select the list of stations matching ``time_range`` and ``filter_condition``
    if time_range is not None:
        year_list = list(np.arange(time_range[0].year, time_range[1].year + 1))
        inventory = inventory[list(map(str, year_list))].dropna()
        station_list = station_list.loc[inventory.index]

    if filter_condition is not None:
        try:
            station_list = station_list.query(filter_condition)
        except Exception as e:
            raise RuntimeError(f"Error applying filter `{filter_condition}`: {e}")

    # Load data for the selected stations and refine the time range filtering
    data = pd.concat(
        [
            load_ghcnh_single_var_station(
                station,
                var,
                year_list=inventory.loc[station].dropna().index.to_list(),
                load_attributes=False,
            ).rename(columns={var: station})
            for station in station_list.index
        ],
        axis=1,
    )
    if time_range is not None:
        data = data.loc[
            pd.Timestamp(time_range[0]) : pd.Timestamp(time_range[1])  # noqa: E203
        ]

    # Add attributes
    data.attrs.update(_VAR_TO_META[var])

    # Match station order
    data = data.loc[:, station_list.index.to_list()]

    return data, station_list

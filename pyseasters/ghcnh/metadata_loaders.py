import logging
from datetime import datetime
from typing import Callable, List, Optional, Tuple, cast

import numpy as np
import pandas as pd

from pyseasters.constants import paths
from pyseasters.utils._args import localize_time_range
from pyseasters.utils._memory import format_memory, memory_estimate
from pyseasters.utils._search_fallback import search_fallback_double

from ._constants import VAR_TO_META

__all__ = [
    "get_ghcnh_station_list",
    "load_ghcnh_inventory",
    "load_ghcnh_station_list",
    "search_ghcnh",
]

log = logging.getLogger(__name__)


def load_ghcnh_station_list() -> pd.DataFrame:
    """Load the 'ghcnh-station-list' parquet file into a pandas DataFrame."""
    return pd.read_parquet(paths.ghcnh_station_list())


def get_ghcnh_station_list() -> List[str]:
    """Return GHCNh station IDs as a list."""
    return load_ghcnh_station_list().index.to_list()


def load_ghcnh_inventory(var: str = "") -> pd.DataFrame:
    """Load the 'ghcnh-inventory' parquet file into a pandas DataFrame."""
    return pd.read_parquet(paths.ghcnh_inventory(var=var))


def search_ghcnh(
    var: str,
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    memory_est: str = "none",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Search GHCNh stations and years matching the search criteria.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    Parameters
    ----------
    var
        The variable to search.
    filter_condition
        An optional query string to filter the station metadata.
        Available attributes are 'station_id', 'lon', 'lat', 'elevation'
        and 'station_name'.
    time_range
        An optional time range for selecting time coverage.
    memory_est
        A string indicating how to estimate memory usage.
        Available strings are 'none' (no memory estimate), 'attrs' (memory estimate
        if loading all attributes), 'noattrs' (memory estimate if loading only ``var``).

    Returns
    -------
    (station_list, inventory) : Tuple[DataFrame, DataFrame]
        ``station_list`` contains the metadata for all GHCNh stations matching the
        search. ``inventory`` contains the number of records per year for these same
        stations.
        Memory estimate
        (if requested with the ``memory_est`` argument)
        is provided in both DataFrames' 'memory_est' attribute.

    Raises
    ------
    ValueError
        If ``var`` or ``memory_est`` are not valid.
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
    if memory_est not in {"none", "attrs", "noattrs"}:
        raise ValueError(
            f"Provided `memory_est` ({memory_est!r}) is not valid."
            + f" Accepted values are in {['none', 'attrs', 'noattrs']!r}."
        )

    # Load the station list
    station_list = load_ghcnh_station_list()
    unstacked = load_ghcnh_inventory(var=var).unstack()
    inventory = cast(pd.DataFrame, unstacked["count"])

    # Select the list of stations matching ``time_range`` and ``filter_condition``
    if time_range:
        year_list = list(np.arange(time_range[0].year, time_range[1].year + 1))
        assert len(year_list) != 0
        if not inventory.columns.intersection(list(map(str, year_list))):
            return search_fallback_double(
                log, "GHCNh", "time_range", time_range, station_list, inventory
            )
        else:
            cols = inventory.columns.intersection(list(map(str, year_list)))
            assert len(cols) > 0
            inventory = inventory[cols].dropna()
        if inventory.empty:
            return search_fallback_double(
                log, "GHCNh", "time_range", time_range, station_list, inventory
            )
        station_list = station_list.loc[inventory.index]

    if filter_condition:
        try:
            station_list = station_list.query(filter_condition)
        except Exception as e:
            raise RuntimeError(f"Error applying filter `{filter_condition}`: {e}")
    if station_list.empty:
        return search_fallback_double(
            log, "GHCNh", "filter_condition", filter_condition, station_list, inventory
        )
    inventory = inventory.loc[station_list.index]

    log.info("Search completed.")

    if memory_est != "none":
        files = [
            paths.ghcnh_file(station_id, year, var)
            for station_id, year in inventory.index
        ]
        usecols: Optional[List[str]] = [var] if memory_est == "noattrs" else None
        est = memory_estimate(files, usecols=usecols)
        log.info(f"Memory estimate when loading with `pandas`: {format_memory(est)}.")
        station_list.attrs.update(dict(memory_est=est))
        inventory.attrs.update(dict(memory_est=est))

    return station_list, inventory


def _search_ghcnh_pr(
    period: str = "",
) -> Callable[..., Tuple[pd.DataFrame, pd.DataFrame]]:

    def func(
        filter_condition: Optional[str] = None,
        time_range: Optional[Tuple[datetime, datetime]] = None,
        memory_est: str = "none",
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        return search_ghcnh(
            "precipitation" + (f"_{period}" if period else ""),
            filter_condition=filter_condition,
            time_range=time_range,
            memory_est=memory_est,
        )

    return func

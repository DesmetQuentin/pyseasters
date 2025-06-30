"""Loaders for GHCNd metadata.

In the GHCNd framework, ``stations`` refer to the stations' static metadata, and the
``inventory`` tells the years of available records per station and variable.
"""

import logging
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd

from pyseasters.constants import paths
from pyseasters.utils._args import localize_time_range
from pyseasters.utils._memory import format_memory, memory_estimate
from pyseasters.utils._search_fallback import search_fallback_single

from ._constants import VAR_TO_META

__all__ = [
    "get_ghcnd_metadata",
    "get_ghcnd_station_list",
    "load_ghcnd_inventory",
    "load_ghcnd_stations",
    "search_ghcnd",
]

log = logging.getLogger(__name__)


def load_ghcnd_stations() -> pd.DataFrame:
    """Load the 'ghcnd-stations' parquet file into a pandas DataFrame."""
    return pd.read_parquet(paths.ghcnd_stations())


def get_ghcnd_station_list() -> List[str]:
    """Return GHCNd station IDs as a list."""
    return load_ghcnd_stations().index.to_list()


def load_ghcnd_inventory(
    usevars: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Load the 'ghcnd-inventory' parquet file into a pandas DataFrame,
    with optional variable filtering through ``usevars``.
    """

    # Load
    inventory = pd.read_parquet(paths.ghcnd_inventory())

    # Select variables in ``usevars``
    if usevars:
        if len(usevars) == 0:
            raise ValueError("`usevars` cannot have zero length.")
        elif len(usevars) == 1:
            selection = inventory.xs(key=usevars[0], level="var", drop_level=True)
            assert isinstance(selection, pd.DataFrame)
            inventory = selection
        else:
            inventory = inventory.loc[pd.IndexSlice[:, usevars], :]

    return inventory


def get_ghcnd_metadata(var: str) -> pd.DataFrame:
    """
    Concatenate station and inventory GHCNd files for the variable ``var`` into a pandas
    DataFrame.
    """
    stations = load_ghcnd_stations()
    inventory = load_ghcnd_inventory(usevars=[var])
    metadata = pd.concat([stations.loc[inventory.index], inventory], axis=1)
    return metadata


def search_ghcnd(
    var: str,
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    memory_est: str = "none",
) -> pd.DataFrame:
    """Search GHCNd stations matching the search criteria.

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
    metadata : DataFrame
        Contains the metadata for all GHCNd stations matching the search.
        Memory estimate
        (if requested with the ``memory_est`` argument)
        is provided in the DataFrame's 'memory_est' attribute.

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

    # Load metadata
    metadata = get_ghcnd_metadata(var=var)

    # Select the list of stations matching ``time_range`` and ``filter_condition``
    if time_range:
        start, end = time_range[0].year, time_range[1].year

        metadata = metadata[
            ((start >= metadata["start"]) & (start <= metadata["end"]))
            | ((end >= metadata["start"]) & (end <= metadata["end"]))
        ]
        if metadata.empty:
            return search_fallback_single(
                log, "GHCNd", "time_range", time_range, metadata
            )
    metadata.drop(["start", "end"], axis=1, inplace=True)

    if filter_condition:
        try:
            metadata = metadata.query(filter_condition)
        except Exception as e:
            raise RuntimeError(f"Error applying filter `{filter_condition}`: {e}")
    if metadata.empty:
        return search_fallback_single(
            log, "GHCNd", "filter_condition", filter_condition, metadata
        )

    log.info("Search completed.")

    if memory_est != "none":
        files = [paths.ghcnd_file(station_id) for station_id in metadata.index]
        usecols: Optional[List[str]] = [var] if memory_est == "noattrs" else None
        est = memory_estimate(files, usecols=usecols)
        log.info(f"Memory estimate when loading with `pandas`: {format_memory(est)}.")
        metadata.attrs.update(dict(memory_est=est))

    return metadata


def _search_ghcnd_pr(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    memory_est: str = "none",
) -> pd.DataFrame:
    return search_ghcnd(
        "PRCP",
        filter_condition=filter_condition,
        time_range=time_range,
        memory_est=memory_est,
    )

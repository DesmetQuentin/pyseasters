import logging
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd

from pyseasters.constants import paths
from pyseasters.utils._args import localize_time_range
from pyseasters.utils._memory import format_memory, memory_estimate
from pyseasters.utils._search_fallback import search_fallback_single

__all__ = [
    "get_gsdr_metadata",
    "get_gsdr_station_list",
    "load_gsdr_inventory",
    "load_gsdr_stations",
    "search_gsdr",
]
log = logging.getLogger(__name__)


def load_gsdr_stations() -> pd.DataFrame:
    """Load the 'gsdr-station-list' parquet file into a pandas DataFrame."""
    return pd.read_parquet(paths.gsdr_stations())


def get_gsdr_station_list() -> List[str]:
    """Return GHCNh station IDs as a list."""
    return load_gsdr_stations().index.to_list()


def load_gsdr_inventory() -> pd.DataFrame:
    """Load the 'gsdr-inventory' parquet file into a pandas DataFrame."""
    return pd.read_parquet(paths.gsdr_inventory())


def get_gsdr_metadata() -> pd.DataFrame:
    """Concatenate station and inventory GSDR files into a pandas DataFrame."""
    stations = load_gsdr_stations()
    inventory = load_gsdr_inventory()
    metadata = pd.concat([stations, inventory], axis=1)
    return metadata


def search_gsdr(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    memory_est: str = "none",
) -> pd.DataFrame:
    """Search GSDR stations matching the search criteria.

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
    memory_est
        A string indicating how to estimate memory usage.
        Available strings are 'none' (no memory estimate), and 'attrs' and 'noattrs'
        (which yield identical results since GSDR data does not have attributes).

    Returns
    -------
    metadata : DataFrame
        Contains the metadata for all GSDR station matching the search, with
        (if requested with the ``memory_est`` argument)
        a 'memory_est' attribute providing an estimation of the memory usage, once the
        associated data is loaded with ``pandas``.

    Raises
    ------
    ValueError
        If ``memory_est`` is not valid.
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """
    # Check arguments
    if time_range:
        time_range = localize_time_range(time_range)
    if memory_est not in {"none", "attrs", "noattrs"}:
        raise ValueError(
            f"Provided `memory_est` ({memory_est!r}) is not valid."
            + f" Accepted values are in {['none', 'attrs', 'noattrs']!r}."
        )

    # Load metadata
    metadata = get_gsdr_metadata()

    # Select the list of stations matching ``time_range`` and ``filter_condition``
    if time_range:
        start, end = time_range[0], time_range[1]

        metadata = metadata[
            ((start >= metadata["start"]) & (start <= metadata["end"]))
            | ((end >= metadata["start"]) & (end <= metadata["end"]))
        ]
        if metadata.empty:
            return search_fallback_single(
                log, "GSDR", "time_range", time_range, metadata
            )
    metadata.drop(["start", "end"], axis=1, inplace=True)

    if filter_condition:
        try:
            metadata = metadata.query(filter_condition)
        except Exception as e:
            raise RuntimeError(f"Error applying filter `{filter_condition}`: {e}")
    if metadata.empty:
        return search_fallback_single(
            log, "GSDR", "filter_condition", filter_condition, metadata
        )

    log.info("Search completed.")

    if memory_est != "none":
        files = [paths.gsdr_file(station_id) for station_id in metadata.index]
        est = memory_estimate(files)
        log.info(f"Memory estimate when loading with `pandas`: {format_memory(est)}.")
        metadata.attrs.update(dict(memory_est=est))

    return metadata

from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

from pyseasters.api.constants import paths

from .load_ghcnd_metadata import get_ghcnd_metadata, load_ghcnd_stations

__all__ = ["load_ghcnd_data"]


def _load_ghcnd_single_data(
    station_id: str,
    var: str = "PRCP",
) -> pd.DataFrame:
    """Load ``var`` data from the single GHCNd file associated with ``station_id``."""
    data = (
        pd.read_csv(
            paths.ghcnd_file(station_id),
            usecols=["DATE", var],
            index_col="DATE",
            parse_dates=["DATE"],
        )
        .dropna()
        .rename_axis("time")
    )
    return data


def load_ghcnd_data(
    var: str = "PRCP",
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load daily GHCNd data and associated station metadata for a given variable.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    Args:
        var: The variable code to load (one of 'PRCP', 'TMAX', 'TMIN', 'TAVG').

        filter_condition: An optional query string to filter the station metadata.
            Available attributes are 'station_id', 'lon', 'lat', 'elevation'
            and 'station_name'.

        time_range: A tuple of (start_datetime, end_datetime) for selecting time
            coverage.

    Returns:
        A tuple:
            - A DataFrame with station data as columns and dates as index, and with
            the 'units' attribute indicating its units, i.e., "mm/day".
            - A DataFrame of metadata for the selected stations.

    Raises:
        RuntimeError: If the filter condition is invalid or raises an exception.
    """

    # Load metadata
    if time_range is None:  # no need to load inventory
        metadata = load_ghcnd_stations()
    else:
        metadata = get_ghcnd_metadata(var=var)

    # Select the list of stations matching ``time_range`` and ``filter_condition``
    if time_range is not None:
        start, end = time_range[0].year, time_range[1].year + 1

        metadata = metadata[
            ((start >= metadata["start"]) & (start <= metadata["end"]))
            | ((end >= metadata["start"]) & (end <= metadata["end"]))
        ].drop(["start", "end"], axis=1)

    if filter_condition is not None:
        try:
            metadata = metadata.query(filter_condition)
        except Exception as e:
            raise RuntimeError(f"Error applying filter `{filter_condition}`: {e}")

    # Load data for the selected stations and refine the time range filtering
    data = pd.concat(
        [_load_ghcnd_single_data(station, var=var) for station in metadata.index],
        axis=1,
    )
    if time_range is not None:
        data = data.loc[
            pd.Timestamp(time_range[0]) : pd.Timestamp(time_range[1])  # noqa: E203
        ]

    # Add attributes
    data.attrs["units"] = "mm/day"

    return data, metadata

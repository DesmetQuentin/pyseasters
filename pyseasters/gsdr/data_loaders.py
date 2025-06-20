from datetime import datetime
from typing import Optional, Tuple

import pandas as pd

from pyseasters.constants import paths

from .metadata_loaders import get_gsdr_metadata

__all__ = ["load_gsdr_single_station", "load_gsdr"]


def load_gsdr_single_station(
    station_id: str,
) -> pd.DataFrame:
    """Load data from the single GSDR file associated with ``station_id``."""
    data = pd.read_parquet(paths.gsdr_file(station_id)).dropna()
    return data


def load_gsdr(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load GSDR data and associated station metadata.

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
    (data, metadata) : Tuple[DataFrame, DataFrame]
        A tuple of DataFrames, with (1) Gauge data, with station IDs as columns, dates
        as index and a 'units' attribute, and (2) Metadata for the associated stations.

    Raises
    ------
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """

    # Load metadata
    metadata = get_gsdr_metadata()

    # Select the list of stations matching ``time_range`` and ``filter_condition``
    if time_range is not None:
        start, end = time_range[0], time_range[1]

        metadata = metadata[
            ((start >= metadata["start"]) & (start <= metadata["end"]))
            | ((end >= metadata["start"]) & (end <= metadata["end"]))
        ]
    metadata.drop(["start", "end"], axis=1, inplace=True)

    if filter_condition is not None:
        try:
            metadata = metadata.query(filter_condition)
        except Exception as e:
            raise RuntimeError(f"Error applying filter `{filter_condition}`: {e}")

    # Load data for the selected stations and refine the time range filtering
    data = pd.concat(
        [
            load_gsdr_single_station(station).rename(columns={"Precipitation": station})
            for station in metadata.index
        ],
        axis=1,
    )
    if time_range is not None:
        data = data.loc[
            pd.Timestamp(time_range[0]) : pd.Timestamp(time_range[1])  # noqa: E203
        ]

    # Add attributes
    data.attrs.update(dict(name="Precipitation", long_name="Precipitation", units="mm"))

    # Match station order
    data = data.loc[:, metadata.index.to_list()]

    return data, metadata

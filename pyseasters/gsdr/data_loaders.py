import logging
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import pandas as pd

from pyseasters.constants import paths

from .metadata_loaders import get_gsdr_metadata

__all__ = ["load_gsdr_single_station", "load_gsdr"]

log = logging.getLogger(__name__)


def load_gsdr_single_station(
    station_id: str,
    dataframe: bool = False,
) -> pd.DataFrame | pd.Series:
    """Load data from the single GSDR file associated with ``station_id``."""
    df = pd.read_parquet(paths.gsdr_file(station_id)).dropna()
    df.attrs.update(dict(name="Precipitation", long_name="Precipitation", units="mm"))
    if dataframe:
        return df
    else:
        return df["Precipitation"]


def load_gsdr(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    concat: bool = False,
    dataframe: bool = False,
) -> Tuple[pd.DataFrame | Dict[str, pd.Series | pd.DataFrame], pd.DataFrame]:
    """Load GSDR data and associated station metadata.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    .. attention::

       Do not use ``concat=True`` unless you are sure to have enough memory available
       for the data you are searching.


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
    concat
        A boolean enabling concatenation of the data into one single DataFrame. Default
        is False.
    dataframe
        A boolean to force keeping the DataFrame type (Series otherwise) in the case
        when ``concat`` is False.

    Returns
    -------
    (data, metadata) : Tuple[DataFrame | Dict[str, Series], DataFrame]
        If ``concat`` is True: ``data`` is a single concatenated DataFrame, with station
        IDs as columns and dates as index; otherwise: ``data`` is a dictionary of
        Series (or single-column DataFrames if ``dataframe`` is True), the keys being
        the station IDs. In any case, ``metadata`` contains the metadata for all
        associated stations.

    Raises
    ------
    RuntimeError
        If the filter condition is invalid or raises an exception.
    """

    # Check arguments
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

    if filter_condition:
        try:
            metadata = metadata.query(filter_condition)
        except Exception as e:
            raise RuntimeError(f"Error applying filter `{filter_condition}`: {e}")

    # Load data for the selected stations and refine the time range filtering
    if time_range:
        data_dict = {
            station: load_gsdr_single_station(station, dataframe=dataframe).loc[
                pd.Timestamp(time_range[0]) : pd.Timestamp(time_range[1])  # noqa: E203
            ]
            for station in metadata.index
        }
    else:
        data_dict = {
            station: load_gsdr_single_station(station, dataframe=dataframe)
            for station in metadata.index
        }

    # Concat
    if concat:
        data_df = pd.concat(data_dict, axis=1, copy=False)
        return data_df, metadata
    else:
        return data_dict, metadata

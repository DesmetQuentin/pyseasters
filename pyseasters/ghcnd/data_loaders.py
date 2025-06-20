import importlib.resources
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import yaml

from pyseasters.constants import paths

from .metadata_loaders import get_ghcnd_metadata

__all__ = ["load_ghcnd", "load_ghcnd_single_var_station"]

log = logging.getLogger(__name__)

with importlib.resources.files("pyseasters.ghcnd.data").joinpath(
    "var_metadata.yaml"
).open("r") as file:
    _VAR_TO_META = yaml.safe_load(file)


def load_ghcnd_single_var_station(
    station_id: str,
    var: str,
    load_attributes: bool = True,
) -> pd.DataFrame:
    """Load ``var`` data from the single GHCNd file associated with ``station_id``."""
    kws: Dict[str, Any] = {} if load_attributes else dict(columns=[var])
    data = pd.read_parquet(paths.ghcnd_file(station_id), **kws).dropna()
    return data


def load_ghcnd(
    var: str = "PRCP",
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load daily GHCNd data and associated station metadata for a given variable.

    If a time range is specified, only stations with coverage overlapping the period
    are retained, and the data are time-sliced accordingly. A filter condition can
    also be used to subset stations based on metadata attributes.

    Parameters
    ----------
    var
        The variable code to load.
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

    # Load metadata
    metadata = get_ghcnd_metadata(var=var)

    # Select the list of stations matching ``time_range`` and ``filter_condition``
    if time_range is not None:
        start, end = time_range[0].year, time_range[1].year

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
            load_ghcnd_single_var_station(
                station, var=var, load_attributes=False
            ).rename(columns={var: station})
            for station in metadata.index
        ],
        axis=1,
    )
    if time_range:
        data = data.loc[
            pd.Timestamp(time_range[0]) : pd.Timestamp(time_range[1])  # noqa: E203
        ]

    # Add attributes
    data.attrs.update(_VAR_TO_META[var])

    # Match station order
    data = data.loc[:, metadata.index.to_list()]

    return data, metadata

"""Provide the single :func:`preprocess_gsdr` function."""

import logging
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yaml
from dask import compute, delayed
from dask.distributed import Client, LocalCluster
from pint import UnitRegistry

from pyseasters.constants import paths
from pyseasters.utils._logging import LoggingStack, LoggingStackPickle

__all__ = ["preprocess_gsdr"]

log = logging.getLogger(__name__)
ureg = UnitRegistry()

# Country-to-timezone mapping (standard time, no DST applied)
_COUNTRY_TO_TZ = {
    "Japan": "Asia/Tokyo",  # UTC+9, no DST
    "Malaysia": "Asia/Kuala_Lumpur",  # UTC+8, no DST
    "India": "Asia/Kolkata",  # UTC+5:30, no DST
    "Australia": "Australia/Sydney",  # UTC+10, DST in summer (but we want standard time)
}


def _parse_timestep(timestep_str: str) -> timedelta:
    """Convert string like '1hr' to timedelta."""
    try:
        q = ureg.Quantity(timestep_str)
        return timedelta(seconds=q.to(ureg.second).magnitude)
    except Exception as e:
        raise ValueError(f"Unsupported timestep format: {timestep_str}") from e


def _parse_elevation(elevation_str: str) -> float:
    """Convert elevation like '22m' to float in meters, or np.nan."""
    if elevation_str.strip().lower() in {"na", "nan"}:
        return np.nan
    try:
        q = ureg.Quantity(elevation_str)
        return q.to(ureg.meter).magnitude
    except Exception as e:
        raise ValueError(f"Invalid elevation format: {elevation_str}") from e


def _parse_resolution(resolution_str: Optional[str | float]) -> float:
    """Parse resolution into float, or return np.nan if missing."""
    if resolution_str is None or str(resolution_str).strip().lower() in {"na", "nan"}:
        return np.nan
    try:
        return float(resolution_str)
    except Exception as e:
        raise ValueError(f"Invalid resolution format: {resolution_str}") from e


@delayed
def _preprocess_single_station(
    file: Path,
) -> Tuple[LoggingStackPickle, Optional[pd.Series], Optional[pd.Series]]:
    """Dask task preprocessing one single station ASCII file."""

    logger = LoggingStack(
        name=file.stem.replace(" ", "_")
    )  # Initiate logging stack for deferred logging

    # File existence check
    if not file.exists():
        logger.error("File %s not found.", file)
        logger.error(
            "Abort preprocessing for station %s.",
            file.stem.replace(" ", "_"),
        )
        return logger.picklable(), None, None
    with open(file, "r") as f:
        lines = f.readlines()

    # Split YAML metadata (first 20 lines) and data
    yaml_lines = lines[:20]
    data_lines = lines[21:]  # skip 'Other:\n' at line 21 (starting from 1)

    # Parse metadata using YAML
    metadata = yaml.safe_load("\n".join(yaml_lines))

    # Data existence check
    try:
        assert metadata["Number of hours"] != 0
    except AssertionError:
        logger.error("No data. Remove.")
        file.unlink()
        return logger.picklable(), None, None
    except Exception as e:
        logger.error("Error while checking data existence: %s", e)
        return logger.picklable(), None, None

    # Units check
    try:
        assert metadata["New Units"] == "mm"
    except AssertionError:
        logger.error("Data is not in millimeters (got %s).", metadata["Units"])
        return logger.picklable(), None, None
    except Exception as e:
        logger.error("Error while asserting units: %s", e)
        return logger.picklable(), None, None

    # Time zone check
    try:
        assert metadata["Time Zone"] in {
            "Local Standard Time",
            "Indian Standard Time (IST)",
            "Local Standard",
        }
        tz = ZoneInfo(_COUNTRY_TO_TZ[metadata["Country"]])
    except AssertionError:
        logger.error("Data time zone is not accepted (got %s).", metadata["Time Zone"])
        return logger.picklable(), None, None
    except Exception as e:
        logger.error("Error while asserting time zone: %s", e)
        return logger.picklable(), None, None

    # Fix and parse resolution
    try:
        metadata["Resolution"] = _parse_resolution(metadata["Resolution"])
    except Exception as e:
        logger.error("Error while parsing resolution: %s", e)
        return logger.picklable(), None, None

    # Fix and parse elevation
    try:
        metadata["Elevation"] = _parse_elevation(metadata["Elevation"])
    except Exception as e:
        logger.error("Error while parsing elevation: %s", e)
        return logger.picklable(), None, None

    # Fix and parse timestep
    try:
        delta = _parse_timestep(metadata["New timestep"])
    except Exception as e:
        logger.error("Error while parsing timestep: %s", e)
        return logger.picklable(), None, None

    # Timestep check
    try:
        assert delta == timedelta(hours=1)
    except AssertionError:
        logger.error("Data is not hourly (got %s).", delta)
        return logger.picklable(), None, None
    except Exception as e:
        logger.error("Error while asserting timestep: %s", e)
        return logger.picklable(), None, None

    # Build datetime index
    try:
        start = (
            datetime.strptime(str(metadata["Start datetime"]), "%Y%m%d%H")
            .replace(tzinfo=tz)
            .astimezone(timezone.utc)
        )
        end = (
            datetime.strptime(str(metadata["End datetime"]), "%Y%m%d%H")
            .replace(tzinfo=tz)
            .astimezone(timezone.utc)
        )
        index = [start + i * delta for i in range(len(data_lines))]
    except Exception as e:
        logger.error("Error while building datetime index: %s", e)
        return logger.picklable(), None, None

    # Read data into DataFrame
    df = pd.read_csv(
        StringIO("".join(data_lines)), header=None, names=["Precipitation"]
    )

    # Handle no-data value
    try:
        no_data_value = float(metadata["No data value"])
        df["Precipitation"] = df["Precipitation"].replace(no_data_value, pd.NA)
    except Exception as e:
        logger.error("Error while parsing no-data value: %s", e)
        return logger.picklable(), None, None

    # Row count check
    try:
        assert len(df) == metadata["Number of hours"]
    except AssertionError:
        logger.error(
            "Row count mismatch: expected %i, got %i.",
            metadata["Number of hours"],
            len(df),
        )
        return logger.picklable(), None, None
    except Exception as e:
        logger.error("Error while asserting row count: %s", e)
        return logger.picklable(), None, None

    # Fill the metadata series
    try:
        metadata_dict: Dict[str, Any] = {
            "station_id": str(metadata["station ID"]).replace(" ", "_"),
            "lat": float(metadata["Latitude"]),
            "lon": float(metadata["Longitude"]),
            "elevation": metadata["Elevation"],
            "station_name": f"{metadata['Original Station Name']} ({metadata['Country']})",
            "original_timestep": metadata["Original timestep"],
            "original_units": metadata["Original Units"],
            "daylight_saving_info": metadata["Daylight Saving info"],
            "resolution": metadata["Resolution"],
        }
        metadata_series = pd.Series(metadata_dict)
    except Exception as e:
        logger.error("Error while building the metadata series: %s", e)
        return logger.picklable(), None, None

    # Fill the inventory series
    try:

        inventory_dict: Dict[str, Any] = {
            "station_id": metadata_series["station_id"],
            "start": start,
            "end": end,
            "percent_missing_data": metadata["Percent missing data"],
        }
        inventory_series = pd.Series(inventory_dict)
    except Exception as e:
        logger.error("Error while building the inventory: %s", e)
        return logger.picklable(), metadata_series, None

    # Assign datetime index
    try:
        df.index = pd.DatetimeIndex(index, name="time")
    except Exception as e:
        logger.error("Error while assigning datetime index: %s", e)
        return logger.picklable(), metadata_series, inventory_series

    # Write
    try:
        df.dropna().to_parquet(paths.gsdr_file(metadata_series["station_id"]))
    except Exception as e:
        logger.error("Error while exporting data to parquet file: %s", e)
        return logger.picklable(), metadata_series, inventory_series

    logger.info("Task completed for station %s.", metadata_series["station_id"])

    return logger.picklable(), metadata_series, inventory_series


def preprocess_gsdr(
    ntasks: Optional[int] = None, memory_limit: Optional[str] = None
) -> None:
    """Preprocess GSDR files."""

    # Get file list
    file_list = [
        f
        for country in _COUNTRY_TO_TZ.keys()
        for f in (paths.gsdr() / "data" / country).glob("*.txt")
    ]

    # Set up Dask
    cluster = (
        LocalCluster()
        if ntasks is None and memory_limit is None
        else (
            LocalCluster(n_workers=ntasks, threads_per_worker=1)
            if memory_limit is None
            else (
                LocalCluster(memory_limit=memory_limit)
                if ntasks is None
                else LocalCluster(
                    n_workers=ntasks, threads_per_worker=1, memory_limit=memory_limit
                )
            )
        )
    )
    client = Client(cluster)
    log.info("Dask cluster is running.")
    tasks = [_preprocess_single_station(file) for file in file_list]

    # Run parallel Dask tasks
    try:
        results = compute(*tasks)
        [stacked_messages, metadata_series, inventory_series] = list(zip(*results))
        del results
    finally:
        client.shutdown()
        client.close()
        cluster.close()
        log.info("Dask cluster has been properly shut down.")

    # Flush logging statements per station
    log.info("Logging statements (if any) are printed below.")
    for args in stacked_messages:
        LoggingStack(*args).flush(logger=log)

    # Process and write station metadata
    metadata = pd.concat(
        [s.to_frame().T for s in metadata_series if s is not None]
    ).set_index("station_id")
    if paths.gsdr_stations().exists():
        log.info("Update existing station metadata file.")
        metadata = metadata.combine_first(pd.read_parquet(paths.gsdr_stations()))
    metadata.sort_index().to_parquet(paths.gsdr_stations())

    # Process and write station inventory
    inventory = pd.concat(
        [s.to_frame().T for s in inventory_series if s is not None]
    ).set_index("station_id")
    if paths.gsdr_inventory().exists():
        log.info("Update existing inventory file.")
        inventory = inventory.combine_first(pd.read_parquet(paths.gsdr_inventory()))
    inventory.sort_index().to_parquet(paths.gsdr_inventory())

    # Delete original files
    completed_stations = metadata.index.intersection(inventory.index).to_list()
    for file in file_list:
        if file.stem.replace(" ", "_") in completed_stations:
            file.unlink()
    for folder in _COUNTRY_TO_TZ.keys():
        if not any((paths.gsdr() / "data" / folder).iterdir()):
            (paths.gsdr() / "data" / folder).rmdir()

    log.info("GSDR data preprocessing completed.")

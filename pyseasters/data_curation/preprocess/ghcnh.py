import importlib.resources
import itertools
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from dask import compute, delayed
from dask.distributed import Client, LocalCluster

from pyseasters.constants import paths
from pyseasters.ghcnh import load_ghcnh_inventory, load_ghcnh_station_list
from pyseasters.ghcnh.data_loaders import _ATTRIBUTES, _VAR_TO_META
from pyseasters.utils._logging import LoggingStack, LoggingStackPickle
from pyseasters.utils._typing import LoggerLike

__all__ = ["preprocess_ghcnh"]

log = logging.getLogger(__name__)


def _ghcnh_file_by_station(station_id: str) -> Path:
    """Return path to ``psv`` file in GHCNh ``by-station`` data directory."""
    return paths.ghcnh() / "data" / "by-station" / f"GHCNh_{station_id}_por.psv"


def _ghcnh_file_by_year(station_id: str, year: int) -> Path:
    """Return path to ``parquet`` file in GHCNh ``by-year`` data directory."""
    return (
        paths.ghcnh()
        / "data"
        / "by-year"
        / str(year)
        / f"GHCNh_{station_id}_{year}.parquet"
    )


def _export_variables(
    df: pd.DataFrame, station_id: str, year: int, logger: LoggerLike
) -> Dict[str, int]:
    """Export DataFrame into 38 files (one per variable)."""
    var_to_count = {}
    for var in _VAR_TO_META.keys():
        cols = [var] + [f"{var}{attr}" for attr in _ATTRIBUTES]
        var_df = df[cols].dropna(how="all")
        var_to_count[var] = len(var_df.index)
        if not var_df.empty:
            var_df.to_parquet(paths.ghcnh_file(station_id, year, var))
    logger.debug(
        "Variable export completed for (station_id, year) = (%s, %i).", station_id, year
    )
    return var_to_count


def _preprocess_single_parquet_station_year(
    station_id: str, year: int, logger: LoggerLike
) -> Dict[str, int]:
    """Preprocess a single ``parquet`` station-year file."""
    # File existence check
    file = _ghcnh_file_by_year(station_id, year)
    if not file.exists():
        logger.error("File %s not found.", file)
        logger.error(
            "Abort `parquet` preprocessing for (station_id, year) = (%s, %i).",
            station_id,
            year,
        )
        return {}
    df = pd.read_parquet(file)

    # Parse datetime
    try:
        df["time"] = pd.to_datetime(df["DATE"])
    except ValueError:
        df["time"] = pd.to_datetime(df["DATE"], format="mixed")
    except Exception as e:
        logger.error("Problem while parsing datetime: %s", e)
        logger.error(
            "Abort `parquet` preprocessing for (station_id, year) = (%s, %i).",
            station_id,
            year,
        )
        return {}
    df.set_index("time", inplace=True)

    # Clean columns
    try:
        df.drop(
            columns=[
                "Station_ID",
                "Station_name",
                "DATE",
                "Latitude",
                "Longitude",
                "Elevation",
            ],
            inplace=True,
        )
    except Exception as e:
        logger.error("Problem while cleaning columns: %s", e)
        logger.error(
            "Abort `parquet` preprocessing for (station_id, year) = (%s, %i).",
            station_id,
            year,
        )
        return {}

    # Compute inventory
    try:
        var_to_count = _export_variables(df, station_id, year, logger=logger)
    except Exception as e:
        logger.error("Problem while exporting variables: %s", e)
        logger.error(
            "Abort `parquet` preprocessing for (station_id, year) = (%s, %i).",
            station_id,
            year,
        )
        return {}

    return var_to_count


@delayed
def _preprocess_single_parquet_station(
    station_id: str, years: List[int]
) -> Tuple[LoggingStackPickle, Tuple[str, Dict[str, Dict[str, int]]]]:
    """Dask task preprocessing all years ``parquet`` files for one station."""

    logger = LoggingStack(
        name=station_id
    )  # Initiate logging stack for deferred logging

    inv_by_var: Dict[str, Dict[str, int]] = defaultdict(dict)
    for year in years:
        var_to_count = _preprocess_single_parquet_station_year(
            station_id, year, logger=logger
        )
        for var, count in var_to_count.items():
            if count != 0:
                inv_by_var[var][str(year)] = count

    logger.info("Task completed for station %s (`parquet` version).", station_id)

    return logger.picklable(), (station_id, dict(inv_by_var))


@delayed
def _preprocess_single_psv_station(
    station_id: str, years: list, return_metadata: bool
) -> Tuple[
    LoggingStackPickle,
    Optional[Tuple[str, Dict[str, Dict[str, int]]]],
    Optional[Tuple[str, float, float, float, str]],
]:
    """Dask task preprocessing a single ``psv`` station file."""

    logger = LoggingStack(
        name=station_id
    )  # Initiate logging stack for deferred logging

    # File existence check
    file = _ghcnh_file_by_station(station_id)
    if not file.exists():
        logger.error("File %s not found.", file)
        logger.error("Abort task for station %s (`psv` version).", station_id)
        return logger.picklable(), None, None

    # Parse dtypes
    try:
        col_dtype = {
            var: (
                str
                if (
                    (var[: len("pres_wx")] == "pres_wx")
                    or (
                        len(var) == len("sky_cover_1")
                        and var[: len("sky_cover")] == "sky_cover"
                    )
                    or (var in ["remarks"])
                )
                else float
            )
            for var in _VAR_TO_META.keys()
        }
        col_dtype.update(
            {
                f"{var}{attr}": str
                for var, attr in itertools.product(_VAR_TO_META.keys(), _ATTRIBUTES)
            }
        )
        df_my = pd.read_csv(file, sep="|", dtype=col_dtype)
        del col_dtype
    except Exception as e:
        logger.error("Problem while parsing dtypes: %s", e)
        logger.error("Abort task for station %s (`psv` version).", station_id)
        return logger.picklable(), None, None

    # Parse datetime
    try:
        df_my = df_my[df_my["Year"].isin(years)]
        df_my["time"] = (
            pd.to_datetime(df_my[["Year", "Month", "Day"]])
            + pd.to_timedelta(df_my["Hour"], unit="h")
            + pd.to_timedelta(df_my["Minute"], unit="m")
        )
        df_my.set_index("time", inplace=True)
        assert isinstance(df_my.index, pd.DatetimeIndex)
    except Exception as e:
        logger.error("Problem while parsing datetime: %s", e)
        logger.error("Abort task for station %s (`psv` version).", station_id)
        return logger.picklable(), None, None

    # Record metadata (if needed)
    if return_metadata:
        try:
            metadata_series = df_my.iloc[0][
                ["Station_ID", "Latitude", "Longitude", "Elevation", "Station_name"]
            ]
            if metadata_series["Elevation"] == -999.9:
                metadata_series["Elevation"] = np.nan
            metadata = tuple(metadata_series.to_list())
            logger.debug("Recorded metadata.")
            del metadata_series
        except Exception as e:
            logger.error("Problem while recording metadata: %s", e)
            logger.error("Abort task for station %s (`psv` version).", station_id)
            return logger.picklable(), None, None
    else:
        metadata = None

    # Clean columns
    try:
        df_my.drop(
            columns=[
                "Station_ID",
                "Latitude",
                "Longitude",
                "Elevation",
                "Station_name",
                "Year",
                "Month",
                "Day",
                "Hour",
                "Minute",
            ],
            inplace=True,
        )
    except Exception as e:
        logger.error("Problem while cleaning columns: %s", e)
        logger.error("Abort task for station %s (`psv` version).", station_id)
        return logger.picklable(), None, metadata

    # Compute inventory
    inv_by_var: Dict[str, Dict[str, int]] = defaultdict(dict)
    for year in years:
        try:
            df = df_my[df_my.index.year == int(year)]
            var_to_count = _export_variables(df, station_id, year, logger=logger)
        except Exception as e:
            logger.error("Problem while exporting variables for year %i: %s", year, e)
            logger.error(
                "Abort `psv` preprocessing for (station_id, year) = (%s, %i).",
                station_id,
                year,
            )
            continue
        for var, count in var_to_count.items():
            if count != 0:
                inv_by_var[var][str(year)] = count

    logger.info("Task completed for station %s (`psv` version).", station_id)

    return logger.picklable(), (station_id, dict(inv_by_var)), metadata


def preprocess_ghcnh(
    step: int, ntasks: Optional[int] = None, memory_limit: Optional[str] = None
) -> None:
    """Preprocess GHCNh data and metadata files."""

    # Load metadata
    station_year = load_ghcnh_inventory().index.to_frame(index=False)
    station_list = load_ghcnh_station_list().index.to_frame(index=False)
    with importlib.resources.files("pyseasters.data_curation.data").joinpath(
        "ghcnh_station-year_missing_by-year.parquet"
    ).open("rb") as file:
        station_year_missing_by_year = pd.read_parquet(file)

    # Map station IDs to year lists for parquet files
    if step == 1:
        merged = station_year.merge(
            station_year_missing_by_year,
            on=["station_id", "year"],
            how="left",
            indicator=True,
        )
        station_year_parquet_df = merged[merged["_merge"] == "left_only"].drop(
            columns="_merge"
        )
        del merged
        station_year_parquet = (
            station_year_parquet_df.groupby("station_id")["year"].agg(list).to_dict()
        )
        del station_year_parquet_df

    # Map station IDs to year lists and booleans (need for metadata) for psv files
    elif step == 2:
        merged = (
            station_year[["station_id"]]
            .drop_duplicates()
            .merge(
                station_list,
                on=["station_id"],
                how="left",
                indicator=True,
            )
        )
        station_need_metadata_df = (
            merged[merged["_merge"] == "left_only"]
            .drop(columns="_merge")
            .reset_index(drop=True)
        )
        del merged
        station_psv = (
            pd.concat(
                [
                    station_need_metadata_df,
                    station_year_missing_by_year[["station_id"]].drop_duplicates(),
                ]
            )
            .drop_duplicates()
            .reset_index(drop=True)
        )["station_id"].to_list()
        station_need_metadata = station_need_metadata_df["station_id"].to_list()
        del station_need_metadata_df
        station_year_psv = (
            station_year_missing_by_year.groupby("station_id")["year"]
            .agg(list)
            .to_dict()
        )
        station_year_metadata_psv = {
            sid: (station_year_psv.get(sid, []), sid in station_need_metadata)
            for sid in station_psv
        }
        del station_year_psv, station_psv
    del station_list, station_year_missing_by_year

    # Prepare directories
    for var, year in itertools.product(
        _VAR_TO_META.keys(), station_year["year"].drop_duplicates().to_list()
    ):
        folder = paths.ghcnh() / "data" / var / str(year)
        if not folder.exists():
            folder.mkdir(parents=True)
    del folder
    log.info("Prepared directories.")

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
    if step == 1:
        tasks_parquet = [
            _preprocess_single_parquet_station(station_id, years)
            for station_id, years in station_year_parquet.items()
        ]
    elif step == 2:
        tasks_psv = [
            _preprocess_single_psv_station(station_id, years, return_metadata)
            for station_id, (
                years,
                return_metadata,
            ) in station_year_metadata_psv.items()
        ]

    # Run parallel Dask tasks
    try:
        if step == 1:
            results_parquet = compute(*tasks_parquet)
            [stacked_messages_parquet, inv_by_var_parquet] = list(zip(*results_parquet))
            del results_parquet
        elif step == 2:
            results_psv = compute(*tasks_psv)
            [stacked_messages_psv, inv_by_var_psv, metadata] = list(zip(*results_psv))
            del results_psv
    finally:
        client.shutdown()
        client.close()
        cluster.close()
        log.info("Dask cluster has been properly shut down.")

    # Flush logging statements per station
    log.info("Logging statements (if any) are printed below.")
    if step == 1:
        for args in stacked_messages_parquet:
            LoggingStack(*args).flush(logger=log)
    elif step == 2:
        for args in stacked_messages_psv:
            LoggingStack(*args).flush(logger=log)

    # Complete/correct station list
    if step == 2:
        stations = load_ghcnh_station_list()
        stations = stations[
            stations.index.isin(station_year["station_id"].drop_duplicates().to_list())
        ]
        stations = pd.concat(
            [stations]
            + [
                pd.DataFrame(
                    [list(md)],
                    columns=[
                        "station_id",
                        "lat",
                        "lon",
                        "elevation",
                        "station_name",
                    ],
                ).set_index("station_id")
                for md in metadata
                if md
            ]
        )
        stations.to_parquet(paths.ghcnh_station_list())
        log.info("Updated station metadata in %s.", paths.ghcnh_station_list())
        del stations

    # Export/Update per-variable inventories
    inv_by_var = defaultdict(list)
    if step == 1:
        for station_id, var_data in inv_by_var_parquet:
            for var, year_count in var_data.items():
                for year, count in year_count.items():
                    inv_by_var[var].append((station_id, year, count))
        del inv_by_var_parquet
        for var, inv in inv_by_var.items():
            pd.DataFrame(inv, columns=["station_id", "year", "count"]).set_index(
                ["station_id", "year"]
            ).sort_index().to_parquet(paths.ghcnh_inventory(var=var))
        log.info(
            "Exported per-variable inventories in %s.",
            paths.ghcnh_inventory(var="<var>"),
        )
    elif step == 2:
        for station_id, var_data in [inv for inv in inv_by_var_psv if inv]:
            for var, year_count in var_data.items():
                for year, count in year_count.items():
                    inv_by_var[var].append((station_id, year, count))
        del inv_by_var_psv
        for var, inv in inv_by_var.items():
            existing = pd.read_parquet(paths.ghcnh_inventory(var=var))
            update = (
                pd.DataFrame(inv, columns=["station_id", "year", "count"])
                .set_index(["station_id", "year"])
                .sort_index()
            )
            existing.combine_first(update).sort_index().to_parquet(
                paths.ghcnh_inventory(var=var)
            )
        log.info(
            "Updated per-variable inventories in %s.",
            paths.ghcnh_inventory(var="<var>"),
        )
    del inv_by_var

    log.info("GHCNh data preprocessing completed.")

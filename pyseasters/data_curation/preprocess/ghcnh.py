import importlib.resources
import itertools
import logging

# import subprocess
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from dask import compute, delayed
from dask.distributed import Client, LocalCluster

from pyseasters.constants import paths
from pyseasters.ghcnh import load_ghcnh_inventory, load_ghcnh_station_list
from pyseasters.ghcnh.data_loader import _ATTRIBUTES, _VARIABLES

# from pyseasters.utils._dependencies import require_tools
from pyseasters.utils._logging import LoggingStack
from pyseasters.utils._typing import LoggingStackPickle

# from pyseasters.utils._typing import LoggerLike

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


def _export_variables(df: pd.DataFrame, station_id: str, year: int) -> Dict[str, int]:
    """Export DataFrame into 38 files (one per variable)."""
    var_to_count = {}
    for var in _VARIABLES:
        cols = [var] + [f"{var}{attr}" for attr in _ATTRIBUTES]
        var_df = df[cols].dropna(how="all")
        var_to_count[var] = len(var_df.index)
        if not var_df.empty:
            var_df.to_parquet(paths.ghcnh_file(station_id, year, var))
    return var_to_count


@delayed
def _preprocess_single_parquet_station(
    station_id: str, years: list
) -> Tuple[LoggingStackPickle, Tuple[str, Dict[str, Dict[str, int]]]]:
    """Dask task preprocessing a single ``parquet`` station file."""

    logger = LoggingStack(
        name=station_id
    )  # Initiate logging stack for deferred logging

    inv_by_var: Dict[str, Dict[str, int]] = defaultdict(dict)
    for year in years:
        # Preprocess the single-year file
        df = pd.read_parquet(_ghcnh_file_by_year(station_id, year))
        df["time"] = pd.to_datetime(df["DATE"])
        df.set_index("time", inplace=True)

        # Clean columns
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

        # Compute inventory
        var_to_count = _export_variables(df, station_id, year)
        for var, count in var_to_count.items():
            if count != 0:
                inv_by_var[var][str(year)] = count

    return logger.picklable(), (station_id, inv_by_var)


@delayed
def _preprocess_single_psv_station(
    station_id: str, years: list, return_metadata: bool
) -> Tuple[
    LoggingStackPickle,
    Tuple[str, Dict[str, Dict[str, int]]],
    Tuple[str, float, float, float, str],
]:
    """Dask task preprocessing a single ``psv`` station file."""

    logger = LoggingStack(
        name=station_id
    )  # Initiate logging stack for deferred logging

    # Preprocess the multi-year file
    df_my = pd.read_parquet(_ghcnh_file_by_station(station_id))
    df_my = df_my[df_my["Year"].isin(years)]
    df_my["time"] = (
        pd.to_datetime(df_my[["Year", "Month", "Day"]])
        + pd.to_timedelta(df_my["Hour"], unit="h")
        + pd.to_timedelta(df_my["Minute"], unit="m")
    )
    df_my.set_index("time", inplace=True)
    assert isinstance(df_my.index, pd.DatetimeIndex)

    # Record metadata (if needed)
    if return_metadata:
        metadata_series = df_my.iloc[0][
            ["Station_ID", "Latitude", "Longitude", "Elevation", "Station_name"]
        ]
        if metadata_series["Elevation"] == -999.9:
            metadata_series["Elevation"] = np.nan
        metadata = tuple(metadata_series.to_list())

    # Clean columns
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

    # Compute inventory
    inv_by_var: Dict[str, Dict[str, int]] = defaultdict(dict)
    for year in years:
        df = df_my[df_my.index.year == int(year)]
        var_to_count = _export_variables(df, station_id, year)
        for var, count in var_to_count.items():
            if count != 0:
                inv_by_var[var][str(year)] = count

    return logger.picklable(), (station_id, inv_by_var), metadata


def preprocess_ghcnh(ntasks: Optional[int] = None) -> None:
    """Preprocess GHCNh data and metadata files."""

    # Load metadata
    station_year = load_ghcnh_inventory().index.to_frame(index=False)
    station_list = load_ghcnh_station_list().index.to_frame(index=False)
    with importlib.resources.files("pyseasters.data_curation.data").joinpath(
        "ghcnh_station-year_missing_by-year.parquet"
    ).open("rb") as file:
        station_year_missing_by_year = pd.read_parquet(file)

    # Map station IDs to year lists for parquet files
    merged = station_year.merge(
        station_year_missing_by_year,
        on=["station_id", "year"],
        how="left",
        indicator=True,
    )
    station_year_parquet_df = merged[merged["_merge"] == "left_only"].drop(
        columns="_merge"
    )
    station_year_parquet = (
        station_year_parquet_df.groupby("station_id")["year"].agg(list).to_dict()
    )

    # Map station IDs to year lists and booleans (need for metadata) for psv files
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
    station_year_psv = (
        station_year_missing_by_year.groupby("station_id")["year"].agg(list).to_dict()
    )
    station_year_metadata_psv = {
        sid: (station_year_psv.get(sid, []), sid in station_need_metadata)
        for sid in station_psv
    }

    # Prepare directories
    for var, year in itertools.product(
        _VARIABLES, station_year["year"].drop_duplicates().to_list()
    ):
        (paths.ghcnh() / "data" / var / str(year)).mkdir(parents=True, exist_ok=True)

    # Set up Dask
    cluster = (
        LocalCluster()
        if ntasks is None
        else LocalCluster(n_workers=ntasks, threads_per_worker=1)
    )
    client = Client(cluster)
    log.info("Dask cluster is running.")
    tasks_parquet = [
        _preprocess_single_parquet_station(station_id, years)
        for station_id, years in station_year_parquet.items()
    ]
    tasks_psv = [
        _preprocess_single_psv_station(station_id, years, return_metadata)
        for station_id, (years, return_metadata) in station_year_metadata_psv.items()
    ]

    # Run parallel Dask tasks
    try:
        results_parquet = compute(*tasks_parquet)
        results_psv = compute(*tasks_psv)
        [stacked_messages_parquet, inv_by_var_parquet] = np.transpose(
            np.array(results_parquet)
        )
        [stacked_messages_psv, inv_by_var_psv, metadata] = np.transpose(
            np.array(results_psv)
        )
    finally:
        client.close()
        cluster.close()
        log.info("Dask cluster has been properly shut down.")

    # Flush logging statements per station
    log.info("Logging statements (if any) are printed below.")
    for args in stacked_messages_parquet:
        LoggingStack(*args).flush(logger=log)
    for args in stacked_messages_psv:
        LoggingStack(*args).flush(logger=log)

    # Complete/correct station list
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
        ]
    )
    stations.to_parquet(paths.ghcnh_station_list())
    log.info("Updated station metadata in %s.", paths.ghcnh_station_list())

    # Merge per-variable inventories
    inv_by_var_psv_t = np.transpose(np.array(inv_by_var_psv))
    inv_by_var_draft = []
    for station_id, var_data_parquet in inv_by_var_parquet:
        if station_id in inv_by_var_psv_t[0]:
            var_data = {}
            var_data_psv = inv_by_var_psv_t[1][
                inv_by_var_psv_t[0].tolist().index(station_id)
            ].copy()
            for var in var_data_parquet.keys():
                for year in var_data_parquet[var].keys():
                    try:
                        assert (year not in list(var_data_psv[var].keys())) or (
                            var_data_parquet[var][year] == var_data_psv[var][year]
                        )
                    except AssertionError:
                        log.warning(
                            "Conflicting inventories found for (station_id, var, year) = (%s, %s, %s).",
                            station_id,
                            var,
                            year,
                        )
                        log.warning(
                            "Inventories are (`parquet`, `psv`) = (%i, %i)",
                            var_data_parquet[var][year],
                            var_data_psv[var][year],
                        )
                        log.warning(
                            "Prioritize inventory issued from the `parquet` file."
                        )
                        var_data_psv[var].pop(year)
                year_count = var_data_parquet[var]
                year_count.update(var_data_psv[var])
                var_data[var] = year_count
            inv_by_var_draft.append(tuple([station_id, var_data]))
        else:
            inv_by_var_draft.append(tuple([station_id, var_data_parquet]))

    # Export per-variable inventories
    inv_by_var = defaultdict(list)
    for station_id, var_data in inv_by_var_draft:
        for var, year_count in var_data.items():
            for year, count in year_count.items():
                inv_by_var[var].append((station_id, year, count))
    for var, inv in inv_by_var.items():
        pd.DataFrame(inv, columns=["station_id", "year", "count"]).set_index(
            ["station_id", "year"]
        ).sort_index().to_parquet(paths.ghcnh_inventory(var=var))
    log.info(
        "Exported per-variable inventories in %s.", paths.ghcnh_inventory(var="<var>")
    )

    log.info("GHCNh data preprocessing completed.")

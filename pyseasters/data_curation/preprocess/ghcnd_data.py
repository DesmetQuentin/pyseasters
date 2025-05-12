import logging
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from dask import compute, delayed
from dask.distributed import Client, LocalCluster

from pyseasters.constants import paths
from pyseasters.ghcnd import load_ghcnd_inventory
from pyseasters.utils._dependencies import require_tools
from pyseasters.utils._logging import LoggingStack
from pyseasters.utils._typing import LoggerLike

__all__ = ["preprocess_ghcnd_data"]

log = logging.getLogger(__name__)


@require_tools("csvcut", "wc")
def _clean_columns(
    input: Path,
    output: Path,
    indices: List[int],
    names: List[str],  # used in case manual cleaning is needed
    logger: LoggerLike,
    expected_ncol: Optional[int] = None,
) -> None:
    """
    Remove from input data the columns corresponding to indices (starts at 1),
    with an optional prior check about the expected number of columns.
    """

    # Check the number of columns (prevent ruining the files in case already ran)
    if expected_ncol is not None:
        try:
            ncol = int(
                subprocess.run(
                    f"csvcut -n {input} | wc -l",
                    shell=True,
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
            )
            if ncol == expected_ncol:
                need_manual_cleaning = False
            else:
                logger.warning("Number of columns different from expected.")
                logger.debug(
                    "Number of columns vs. expected: %i vs. %i",
                    ncol,
                    expected_ncol,
                )
                need_manual_cleaning = True
        except subprocess.CalledProcessError as e:
            logger.error("csvcut/wc failed: %s", e)
            raise RuntimeError("csvcut/wc failed.") from e
    else:
        need_manual_cleaning = False

    # Actually clean columns
    if need_manual_cleaning:

        # Manually: read, drop, write with pandas
        logger.info("Attempt cleaning columns manually.")
        df = pd.read_csv(input)
        df.drop(columns=names, inplace=True)
        df.to_csv(output, index=False)
        logger.info("Success.")

    else:

        # Otherwise: use csvcut
        try:
            command = f"csvcut -C {','.join(map(str, indices))} {input} > {output}"
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            logger.error("csvcut failed: %s", e)
            raise RuntimeError("csvcut failed.") from e

    logger.info("Column cleaning completed.")
    logger.debug("Input -> output: %s -> %s", input, output)


def _single_station_to_parquet(station_id: str, logger: LoggerLike) -> None:
    """Convert a single station csv file for ``station_id`` into parquet."""
    data = (
        pd.read_csv(
            paths.ghcnd_file(station_id, ext="csv"),
            index_col="DATE",
            parse_dates=["DATE"],
        )
        .dropna()
        .rename_axis("time")
    )
    data.to_parquet(paths.ghcnd_file(station_id))
    logger.info("Conversion to parquet completed.")
    logger.debug(
        "Input -> output: %s -> %s",
        paths.ghcnd_file(station_id, ext="csv"),
        paths.ghcnd_file(station_id),
    )


@delayed
def _preprocess_single_station(
    station_id: str, expected_ncol: int
) -> Tuple[str, List[Tuple[str, ...]]]:
    """Dask task preprocessing a single station file."""

    logger = LoggingStack(
        name=station_id
    )  # Initiate logging stack for deferred logging

    # File existence check
    file = paths.ghcnd_file(station_id, ext="csv")
    if not file.exists():
        logger.error("File %s not found", file)
        logger.error("Abort task for station %s.", station_id)
        return logger.picklable()

    # Clean columns
    try:
        _clean_columns(
            file,
            paths.ghcnd() / "data" / f"tmp-{station_id}.csv",
            [1, 3, 4, 5, 6],
            ["STATION", "LONGITUDE", "LATITUDE", "ELEVATION", "NAME"],
            logger=logger,
            expected_ncol=expected_ncol,
        )
        subprocess.run(
            f"mv {paths.ghcnd() / 'data' / ('tmp-%s.csv' % (station_id))} {file}",
            shell=True,
            check=True,
        )
    except Exception as e:
        logger.error("Problem while cleaning columns: %s", e)
        logger.error("Abort task for station %s.", station_id)
        return logger.picklable()

    # Convert to parquet
    try:
        _single_station_to_parquet(station_id, logger=logger)
        subprocess.run(f"rm {file}", shell=True, check=True)
    except Exception as e:
        logger.error("Problem while converting to parquet: %s", e)
        logger.error("Abort task for station %s.", station_id)
        return logger.picklable()

    logger.info("Task completed for station %s", station_id)

    return logger.picklable()


def preprocess_ghcnd_data(ntasks: Optional[int] = None) -> None:
    """Remove duplicate columns and compress GHCNd data files."""

    # Calculate expected number of columns per station based on the inventory
    inventory = load_ghcnd_inventory()
    station_to_ncol = {
        k: len(v) * 2 + 6 for k, v in inventory.groupby(level=0).groups.items()
    }

    # Set up Dask
    cluster = (
        LocalCluster()
        if ntasks is None
        else LocalCluster(n_workers=ntasks, threads_per_worker=1)
    )
    client = Client(cluster)
    log.info("Dask cluster is running.")
    tasks = [
        _preprocess_single_station(station_id, expected_ncol)
        for station_id, expected_ncol in station_to_ncol.items()
    ]

    # Run parallel Dask tasks
    try:
        stacked_messages = compute(*tasks)
    finally:
        client.close()
        cluster.close()
        log.info("Dask cluster has been properly shut down.")

    # Flush logging statements per station
    log.info("Logging statements (if any) are printed below.")
    for args in stacked_messages:
        LoggingStack(*args).flush(logger=log)

    log.info("GHCNd data preprocessing completed.")

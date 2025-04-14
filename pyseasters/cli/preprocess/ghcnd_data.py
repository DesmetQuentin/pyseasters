import logging
import subprocess
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from dask import compute, delayed
from dask.distributed import Client, LocalCluster

from pyseasters.api import load_ghcnd_inventory, paths
from pyseasters.api.ghcnd.load_ghcnd_data import _load_ghcnd_single_station
from pyseasters.cli._utils import LoggingStack, require_tools

__all__ = ["preprocess_ghcnd_data"]

log = logging.getLogger(__name__)


@require_tools("csvcut", "wc")
def _clean_columns(
    input: Path,
    output: Path,
    indices: List[int],
    names: List[str],  # used in case manual cleaning is needed
    logger: LoggingStack,
    expected_ncol: Optional[int] = None,
) -> bool:
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
                if ncol < expected_ncol:
                    logger.warning("Abort cleaning columns.")
                    return False
                elif ncol > expected_ncol:
                    logger.debug("Attempt cleaning columns manually.")
                    need_manual_cleaning = True
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"csvcut error: {e}")

    # Actually clean columns
    if need_manual_cleaning:
        df = pd.read_csv(input)
        df.drop(columns=names, inplace=True)
        df.to_csv(output, index=False)

    else:
        command = f"csvcut -C {','.join([str(i) for i in indices])} {input} > {output}"

        try:
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"csvcut error: {e}")

    logger.debug(f"Cleaning completed on {input}. Output saved to {output}.")
    return True


def _single_station_to_parquet(station_id: str, logger: LoggingStack) -> None:
    """Convert a single station csv file for ``station_id`` into parquet."""
    data = _load_ghcnd_single_station(station_id, from_parquet=False)
    data.to_parquet(paths.ghcnd_file(station_id))
    logger.debug(
        "Converted %s into parquet.", str(paths.ghcnd_file(station_id, ext="csv"))
    )


@delayed
def _preprocess_single_station(
    station_id: str, expected_ncol: int, to_parquet: bool = True
) -> Tuple[str, List[Tuple[str, ...]]]:
    """Dask task preprocessing a single station file."""

    logger = LoggingStack(name=station_id)

    file = paths.ghcnd_file(station_id, ext="csv")
    if not file.exists():
        logger.warning(
            "File %s not found. Abort preprocessing for this station.", str(file)
        )
        return logger.picklable()

    done = _clean_columns(
        file,
        paths.ghcnd() / "data" / f"tmp-{station_id}.csv",
        [1, 3, 4, 5, 6],
        ["STATION", "LONGITUDE", "LATITUDE", "ELEVATION", "NAME"],
        logger=logger,
        expected_ncol=expected_ncol,
    )
    # If the file has actually changed
    if done:
        subprocess.run(
            f"mv {paths.ghcnd() / 'data' / ('tmp-%s.csv' % (station_id))} {file}",
            shell=True,
            check=True,
        )
    if to_parquet:
        _single_station_to_parquet(station_id, logger=logger)
        subprocess.run(f"rm {file}", shell=True, check=True)

    return logger.picklable()


def preprocess_ghcnd_data(
    ntasks: Optional[int] = None, to_parquet: bool = True
) -> None:
    """Remove duplicate columns and compress GHCNd data files."""

    inventory = load_ghcnd_inventory()
    station_to_ncol = {
        k: len(v) * 2 + 6 for k, v in inventory.groupby(level=0).groups.items()
    }

    cluster = (
        LocalCluster()
        if ntasks is None
        else LocalCluster(n_workers=ntasks, threads_per_worker=1)
    )
    client = Client(cluster)
    tasks = [
        _preprocess_single_station(station_id, expected_ncol, to_parquet=to_parquet)
        for station_id, expected_ncol in station_to_ncol.items()
    ]

    try:
        log.info("Dask cluster is running.")
        stacked_messages = compute(*tasks)
    finally:
        client.close()
        cluster.close()
        log.info("Dask cluster has been properly shut down.")

    log.info("Logging statements (if any) are printed below.")
    for args in stacked_messages:
        LoggingStack(*args).flush(logger=log)

    log.info("GHCNd data preprocessing completed.")

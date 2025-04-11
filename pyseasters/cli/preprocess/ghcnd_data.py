import logging
import subprocess
from pathlib import Path
from typing import List, Optional, Union

from dask import compute, delayed

from pyseasters.api import load_ghcnd_inventory, paths
from pyseasters.api.ghcnd.load_ghcnd_data import _load_ghcnd_single_station
from pyseasters.cli._utils import require_tools

__all__ = ["preprocess_ghcnd_data"]

log = logging.getLogger(__name__)


@require_tools("csvcut", "wc")
def _clean_columns(
    input: Union[str, Path],
    output: Union[str, Path],
    indices: List[int],
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
            if ncol != expected_ncol:
                log.warning(
                    "Number of columns in %s different from expected. "
                    + "Abort cleaning columns.",
                    str(input),
                )
                log.debug(
                    "(Number of columns vs. expected: %i vs. %i)",
                    ncol,
                    expected_ncol,
                )
                return False
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"csvcut error: {e}")

    # Actually clean columns
    command = f"csvcut -C {','.join([str(i) for i in indices])} {input} > {output}"

    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"csvcut error: {e}")

    log.debug(f"Cleaning completed on {input}. Output saved to {output}.")
    return True


def _single_station_to_parquet(station_id: str) -> None:
    """Convert a single station csv file for ``station_id`` into parquet."""
    data = _load_ghcnd_single_station(station_id, from_parquet=False)
    data.to_parquet(paths.ghcnd_file(station_id))
    log.debug(
        "Converted %s into parquet.", str(paths.ghcnd_file(station_id, ext="csv"))
    )


@delayed
def _preprocess_single_station(
    station_id: str, expected_ncol: int, to_parquet: bool = True
) -> None:
    """Dask task preprocessing a single station file."""
    file = paths.ghcnd_file(station_id, ext="csv")
    if not file.exists():
        log.warning(
            "File %s not found. Abort preprocessing for this station", str(file)
        )
        return

    done = _clean_columns(
        file,
        paths.ghcnd() / "data" / "tmp.csv",
        [1, 3, 4, 5, 6],
        expected_ncol=expected_ncol,
    )
    # If the file has actually changed
    if done:
        subprocess.run(
            f"mv {paths.ghcnd() / "data" / "tmp.csv"} {file}", shell=True, check=True
        )
    if to_parquet:
        _single_station_to_parquet(station_id)
        subprocess.run(f"rm {file}", shell=True, check=True)


def preprocess_ghcnd_data(to_parquet: bool = True) -> None:
    """Remove duplicate columns and compress GHCNd data files."""

    inventory = load_ghcnd_inventory()
    station_to_ncol = {
        k: len(v) * 2 + 6 for k, v in inventory.groupby(level=0).groups.items()
    }

    tasks = [
        _preprocess_single_station(station_id, expected_ncol, to_parquet=to_parquet)
        for station_id, expected_ncol in station_to_ncol.items()
    ]

    compute(*tasks)
    log.info("GHCNd data preprocessing completed.")

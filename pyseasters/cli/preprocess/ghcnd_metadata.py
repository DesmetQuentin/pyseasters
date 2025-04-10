import logging
import subprocess
from pathlib import Path
from typing import List, Optional, Union

from pyseasters.api import COUNTRIES, load_ghcnd_inventory, load_ghcnd_stations, paths
from pyseasters.cli._utils import require_tools

__all__ = ["preprocess_ghcnd_metadata"]

log = logging.getLogger(__name__)


@require_tools("awk")
def _filter_countries(input: Union[str, Path], output: Union[str, Path]) -> None:
    """
    Filter input data by removing lines where the first column's first two characters
    do not match any FIPS code of the global ``COUNTRIES`` constant.
    """

    regex_pattern = f"^({'|'.join(COUNTRIES['FIPS'].to_list())})"
    command = f"awk '$1 ~ /{regex_pattern}/' {input} > {output}"

    try:
        subprocess.run(command, shell=True, check=True)
        log.info(f"Filtering completed on {input}. Output saved to {output}.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"awk error: {e}")


@require_tools("cat", "tr", "cut", "awk")
def _clean_columns(
    input: Union[str, Path],
    output: Union[str, Path],
    indices: List[int],
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
                    'awk -F" " "{print NF; exit}"' + f" {input}",
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
                    "Number of columns vs. expected: %i vs. %i",
                    ncol,
                    expected_ncol,
                )
                return
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"csvcut error: {e}")

    # Actually clean columns
    command = (
        f"cat {input} | tr -s ' ' | cut -d' ' --complement "
        + f"-f{','.join([str(i) for i in indices])} > {output}"
    )

    try:
        subprocess.run(command, shell=True, check=True)
        log.info(f"Cleaning completed on {input}. Output saved to {output}.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"cat/tr/cut error: {e}")


def _stations_to_parquet() -> None:
    """Convert the 'ghcnd-stations' ASCII file into parquet."""
    data = load_ghcnd_stations(from_parquet=False)
    data.to_parquet(paths.ghcnd_stations())
    log.info("Converted %s into parquet.", str(paths.ghcnd_stations(ext="txt")))


def _inventory_to_parquet() -> None:
    """Convert the 'ghcnd-inventory' ASCII file into parquet."""
    data = load_ghcnd_inventory(from_parquet=False)
    data.to_parquet(paths.ghcnd_inventory())
    log.info("Converted %s into parquet.", str(paths.ghcnd_inventory(ext="txt")))


def preprocess_ghcnd_metadata(to_parquet: bool = True) -> None:
    """Filter countries, remove duplicate columns and compress GHCNd metadata files."""

    buffer = "tmp.txt"

    _filter_countries(paths.ghcnd_stations(ext="txt"), paths.ghcnd() / buffer)
    subprocess.run(
        f"mv {paths.ghcnd() / buffer} {paths.ghcnd_stations(ext='txt')}",
        shell=True,
        check=True,
    )

    _filter_countries(paths.ghcnd_inventory(ext="txt"), paths.ghcnd() / buffer)
    subprocess.run(
        f"mv {paths.ghcnd() / buffer} {paths.ghcnd_inventory(ext='txt')}",
        shell=True,
        check=True,
    )

    _clean_columns(
        paths.ghcnd_inventory(ext="txt"),
        paths.ghcnd() / buffer,
        [2, 3],
        expected_ncol=6,
    )
    subprocess.run(
        f"mv {paths.ghcnd() / buffer} {paths.ghcnd_inventory(ext='txt')}",
        shell=True,
        check=True,
    )

    if to_parquet:
        _stations_to_parquet()
        subprocess.run(f"rm {paths.ghcnd_stations(ext='txt')}", shell=True, check=True)
        _inventory_to_parquet()
        subprocess.run(f"rm {paths.ghcnd_inventory(ext='txt')}", shell=True, check=True)

    log.info("GHCNd metadata preprocessing completed.")

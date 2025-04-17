import logging
import subprocess
from typing import List, Optional

from pyseasters.constants import COUNTRIES, paths
from pyseasters.ghcnd import load_ghcnd_inventory, load_ghcnd_stations
from pyseasters.utils._dependencies import require_tools
from pyseasters.utils._typing import PathLike

__all__ = ["preprocess_ghcnd_metadata"]

log = logging.getLogger(__name__)


@require_tools("awk")
def _filter_countries(input: PathLike, output: PathLike) -> None:
    """
    Filter input data by removing lines where the first column's first two characters
    do not match any FIPS code of the global ``COUNTRIES`` constant.
    """
    try:
        regex_pattern = f"^({'|'.join(COUNTRIES['FIPS'].to_list())})"
        command = f"awk '$1 ~ /{regex_pattern}/' {input} > {output}"
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        log.error("awk failed: %s", e)
        raise RuntimeError("awk failed.") from e

    log.info("Country filtering completed.")
    log.debug("Input -> output: %s -> %s", input, output)


@require_tools("cat", "tr", "cut", "awk")
def _clean_columns(
    input: PathLike,
    output: PathLike,
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
                    'awk -F" " "{print NF; exit}"' + f" {input}",
                    shell=True,
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
            )
            if ncol != expected_ncol:
                log.warning("Number of columns different from expected.")
                log.debug(
                    "Number of columns vs. expected: %i vs. %i",
                    ncol,
                    expected_ncol,
                )
                log.warning("Abort cleaning columns for file %s.", input)
                return False
        except subprocess.CalledProcessError as e:
            log.error("awk failed: %s", e)
            raise RuntimeError("awk failed.") from e

    # Actually clean columns
    try:
        command = (
            f"cat {input} | tr -s ' ' | cut -d' ' --complement "
            + f"-f{','.join(map(str, indices))} > {output}"
        )
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        log.error("cat/tr/cut failed: %s", e)
        raise RuntimeError("cat/tr/cut failed.") from e

    log.info("Column cleaning completed.")
    log.debug("Input -> output: %s -> %s", input, output)
    return True


def _stations_to_parquet() -> None:
    """Convert the 'ghcnd-stations' ASCII file into parquet."""
    data = load_ghcnd_stations(from_parquet=False)
    data.to_parquet(paths.ghcnd_stations())
    log.info("Conversion to parquet completed.")
    log.debug(
        "Input -> output: %s -> %s",
        paths.ghcnd_stations(ext="txt"),
        paths.ghcnd_stations(),
    )


def _inventory_to_parquet() -> None:
    """Convert the 'ghcnd-inventory' ASCII file into parquet."""
    data = load_ghcnd_inventory(from_parquet=False, multiindex=False)
    data.to_parquet(paths.ghcnd_inventory())
    log.info("Conversion to parquet completed.")
    log.debug(
        "Input -> output: %s -> %s",
        paths.ghcnd_inventory(ext="txt"),
        paths.ghcnd_inventory(),
    )


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

    done = _clean_columns(
        paths.ghcnd_inventory(ext="txt"),
        paths.ghcnd() / buffer,
        [2, 3],
        expected_ncol=6,
    )
    if done:
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

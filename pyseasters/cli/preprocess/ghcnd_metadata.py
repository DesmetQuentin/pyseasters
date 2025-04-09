import logging
import subprocess
from pathlib import Path
from typing import List, Union

from pyseasters.api import COUNTRIES, paths
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


@require_tools("cat", "tr", "cut")
def _clean_columns(
    input: Union[str, Path], output: Union[str, Path], indices: List[int]
) -> None:
    """Remove from input data the columns corresponding to indices (starts at 1)."""

    command = (
        f"cat {input} | tr -s ' ' | cut -d' ' --complement "
        + f"-f{','.join([str(i) for i in indices])} > {output}"
    )

    try:
        subprocess.run(command, shell=True, check=True)
        log.info(f"Cleaning completed on {input}. Output saved to {output}.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"cat/tr/cut error: {e}")


def preprocess_ghcnd_metadata() -> None:
    """Filter countries and remove duplicate columns in GHCNd metadata files."""

    buffer = "tmp.txt"

    _filter_countries(paths.ghcnd_stations(), paths.ghcnd() / buffer)
    subprocess.run(
        f"mv {paths.ghcnd() / buffer} {paths.ghcnd_stations()}", shell=True, check=True
    )

    _filter_countries(paths.ghcnd_inventory(), paths.ghcnd() / buffer)
    subprocess.run(
        f"mv {paths.ghcnd() / buffer} {paths.ghcnd_inventory()}", shell=True, check=True
    )

    _clean_columns(paths.ghcnd_inventory(), paths.ghcnd() / buffer, [2, 3])
    subprocess.run(
        f"mv {paths.ghcnd() / buffer} {paths.ghcnd_inventory()}", shell=True, check=True
    )

    log.info("GHCNd metadata preprocessing completed.")

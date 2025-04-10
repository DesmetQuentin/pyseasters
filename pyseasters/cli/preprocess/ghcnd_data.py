import logging
import subprocess
from pathlib import Path
from typing import List, Optional, Union

from pyseasters.api import get_ghcnd_station_list, paths
from pyseasters.cli._utils import require_tools

__all__ = ["preprocess_ghcnd_data"]

log = logging.getLogger(__name__)


@require_tools("csvcut", "wc")
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
                    "Number of columns vs. expected: %i vs. %i",
                    ncol,
                    expected_ncol,
                )
                return
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"csvcut error: {e}")

    # Actually clean columns
    command = f"csvcut -C {','.join([str(i) for i in indices])} {input} > {output}"

    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"csvcut error: {e}")

    log.debug(f"Cleaning completed on {input}. Output saved to {output}.")


def preprocess_ghcnd_data() -> None:
    """Remove duplicate columns in GHCNd data files."""

    buffer = "tmp.txt"

    for station_id in get_ghcnd_station_list():
        file = paths.ghcnd_file(station_id)
        if file.exists():
            _clean_columns(
                file,
                paths.ghcnd() / buffer,
                [1, 3, 4, 5, 6],
                expected_ncol=14,
            )
            subprocess.run(
                f"mv {paths.ghcnd() / buffer} {file}", shell=True, check=True
            )

    log.info("GHCNd data preprocessing completed.")

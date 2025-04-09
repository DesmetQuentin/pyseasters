import logging
import subprocess
from pathlib import Path
from typing import List, Union

from pyseasters.api import get_ghcnd_station_list, paths
from pyseasters.cli._utils import require_tools

__all__ = ["preprocess_ghcnd_data"]

log = logging.getLogger(__name__)


@require_tools("csvcut")
def _clean_columns(
    input: Union[str, Path], output: Union[str, Path], indices: List[int]
) -> None:
    """Remove from input data the columns corresponding to indices (starts at 1)."""

    command = f"csvcut -C {','.join([str(i) for i in indices])} {input} > {output}"

    try:
        subprocess.run(command, shell=True, check=True)
        log.debug(f"Cleaning completed on {input}. Output saved to {output}.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"csvcut error: {e}")


def preprocess_ghcnd_data() -> None:
    """Remove duplicate columns in GHCNd data files."""

    buffer = "tmp.txt"

    for station_id in get_ghcnd_station_list():
        file = paths.ghcnd_file(station_id)
        if file.exists():
            _clean_columns(file, paths.ghcnd() / buffer, [1, 3, 4, 5, 6])
            subprocess.run(
                f"mv {paths.ghcnd() / buffer} {file}", shell=True, check=True
            )

    log.info("GHCNd data preprocessing completed.")

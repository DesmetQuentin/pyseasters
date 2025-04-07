#!/bin/usr python3

"""Running this module as a script calls the `preprocess_data` function."""

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from typing import List, Union

from pyseasters.constants.pathconfig import paths
from pyseasters.parsers.ghcnd import _parse_ghcnd_stations

__all__ = ["preprocess_data"]

log = logging.getLogger(__name__)


def _clean_columns(
    input: Union[str, Path], output: Union[str, Path], indices: List[int]
) -> None:
    """Remove from input data the columns corresponding to indices (starts at 1)."""

    command = f"csvcut -C {','.join([str(i) for i in indices])} {input} > {output}"

    try:
        subprocess.run(command, shell=True, check=True)
        log.info(f"Cleaning completed on {input}. Output saved to {output}.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"cut error: {e}")


def preprocess_data() -> None:
    """Remove duplicate columns in GHCNd data files."""

    buffer = "tmp.txt"

    for station_id in (
        _parse_ghcnd_stations(paths.ghcnd_stations()).iloc[:, 0].to_list()
    ):
        file = paths.ghcnd_file(station_id)
        if file.exists():
            _clean_columns(file, paths.ghcnd() / buffer, [1, 3, 4, 5, 6])
            subprocess.run(
                f"mv {paths.ghcnd() / buffer} {file}", shell=True, check=True
            )

    log.info("Preprocessing completed for GHCNd data.")


def main():
    parser = argparse.ArgumentParser(
        description="Preprocess GHCNd data files (remove duplicate columns)."
    )
    parser.parse_args()

    response = (
        input(
            "This program modifies files in place. Are you sure you want to continue? "
            + "(y/[n]): "
        )
        .strip()
        .lower()
    )
    if response not in ("y", "yes"):
        sys.stderr.write("Aborted by user.\n")
        sys.exit(0)

    preprocess_data()
    sys.stdout.write("Done.\n")


if __name__ == "__main__":
    main()

#!/bin/usr python3

"""Running this module as a script calls the `preprocess_metadata` function."""

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from typing import List, Union

from pyseasters.constants.countries import COUNTRIES
from pyseasters.constants.pathconfig import paths

__all__ = ["preprocess_metadata"]

log = logging.getLogger(__name__)


def _filter_countries(input: Union[str, Path], output: Union[str, Path]) -> None:
    regex_pattern = f"^({'|'.join(COUNTRIES['FIPS'].to_list())})"
    command = f"awk '$1 ~ /{regex_pattern}/' {input} > {output}"

    try:
        subprocess.run(command, shell=True, check=True)
        log.info(f"Filtering completed on {input}. Output saved to {output}.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"awk error: {e}")


def _clean_columns(
    input: Union[str, Path], output: Union[str, Path], indices: List[int]
) -> None:
    command = (
        f"cat {input} | tr -s ' ' | cut -d' ' --complement "
        + f"-f{','.join([str(i) for i in indices])} > {output}"
    )

    try:
        subprocess.run(command, shell=True, check=True)
        log.info(f"Cleaning completed on {input}. Output saved to {output}.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"cut error: {e}")


def preprocess_metadata() -> None:
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

    log.info("Preprocessing completed for GHCNd data.")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Preprocess GHCNd metadata files "
            + "(filter countries and remove duplicate columns)."
        )
    )
    parser.parse_args()

    response = input(
        "This program modifies files in place. Are you sure you want to continue? "
        + "(y/[n]): "
    ).strip().lower()
    if response not in ("y", "yes"):
        sys.stderr.write("Aborted by user.\n")
        sys.exit(0)

    preprocess_metadata()
    sys.stdout.write("Done.\n")


if __name__ == "__main__":
    main()

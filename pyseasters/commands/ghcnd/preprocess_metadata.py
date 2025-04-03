import logging
import subprocess
from typing import List

from pyseasters.constants.countries import COUNTRIES
from pyseasters.constants.pathconfig import paths

__all__ = ["preprocess_metadata"]

log = logging.getLogger(__name__)


def _filter_countries(input: str, output: str) -> None:
    regex_pattern = f"^({'|'.join(COUNTRIES['FIPS'].to_list())})"
    command = f"awk '$1 ~ /{regex_pattern}/' {input} > {output}"

    try:
        subprocess.run(command, shell=True, check=True)
        log.info(f"Filtering completed on {input}. Output saved to {output}.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"awk error: {e}")


def _clean_columns(input: str, output: str, indices: List[str]) -> None:
    command = f"cat {input} | tr -s ' ' | cut -d' ' --complement -f{','.join([str(i) for i in indices])} > {output}"

    try:
        subprocess.run(command, shell=True, check=True)
        log.info(f"Cleaning completed on {input}. Output saved to {output}.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"cut error: {e}")


def preprocess_metadata() -> None:
    if not paths.is_initialized():
        paths.initialize()

    buffer = "tmp.txt"

    _filter_countries(paths.ghcnd_stations, paths.ghcnd / buffer)
    subprocess.run(
        f"mv {paths.ghcnd / buffer} {paths.ghcnd_stations}", shell=True, check=True
    )

    _filter_countries(paths.ghcnd_inventory, paths.ghcnd / buffer)
    subprocess.run(
        f"mv {paths.ghcnd / buffer} {paths.ghcnd_inventory}", shell=True, check=True
    )

    _clean_columns(paths.ghcnd_inventory, paths.ghcnd / buffer, [2, 3])
    subprocess.run(
        f"mv {paths.ghcnd / buffer} {paths.ghcnd_inventory}", shell=True, check=True
    )

    log.info("Preprocessing completed for GHCNd data.")


if __name__ == "__main__":
    preprocess_metadata()

import logging
import subprocess

import pandas as pd

from pyseasters.constants import COUNTRIES, paths
from pyseasters.utils._dependencies import require_tools
from pyseasters.utils._typing import PathLike

__all__ = ["preprocess_ghcnh_metadata"]

log = logging.getLogger(__name__)


@require_tools("awk")
def _filter_countries(input: PathLike, output: PathLike, header: bool = False) -> None:
    """
    Filter input data by removing lines where the first column's first two characters
    do not match any FIPS code of the global ``COUNTRIES`` constant.
    """
    try:
        regex_pattern = f"^({'|'.join(COUNTRIES['FIPS'].to_list())})"
        if header:
            first_line = "{ print $0; next }"
            next_lines = "$1 ~ /" + regex_pattern + "/ {print}"
            command = f"awk 'NR == 1 {first_line} {next_lines}' {input} > {output}"
        else:
            command = f"awk '$1 ~ /{regex_pattern}/' {input} > {output}"
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        log.error("awk failed: %s", e)
        raise RuntimeError("awk failed.") from e

    log.info("Country filtering completed.")
    log.debug("Input -> output: %s -> %s", input, output)


@require_tools("awk", "column")
def _simplify_inventory(output: PathLike) -> None:
    """Turn monthly inventory into yearly to match data file partitioning."""
    input = paths.ghcnh_inventory(ext="txt")
    try:
        first_line = '{ print $1, $2, "count"; next }'
        next_lines = "{ s = 0; for (i = 3; i <= 14; i++) s += $i; print $1, $2, s }"
        command = (
            f"awk 'NR == 1 {first_line} {next_lines}' {input}"
            + f" | column -t > {output}"
        )
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        log.error("awk failed: %s", e)
        raise RuntimeError("awk failed.") from e

    log.info("Inventory simplification completed.")
    log.debug("Input -> output: %s -> %s", input, output)


def _station_list_to_parquet() -> None:
    """Convert the 'ghcnh-station_list' csv file into parquet."""
    col_names = [
        "station_id",
        "lat",
        "lon",
        "elevation",
        "state",
        "name",
        "GSN flag",
        "HCN/CRN flag",
        "WMO ID",
    ]
    station_list = pd.read_csv(
        paths.ghcnh_station_list(ext="csv"),
        header=None,
        names=col_names,
        na_values={"elevation": ["-999.9"]},
    ).set_index(col_names[0])

    def parse_station_name(row):
        field = {
            "4": "US",
            "6": "GSN",
            "7": "HCN",
            "8": "WMO",
        }
        name = row[col_names[5]].strip()
        details = [
            f"{field[str(i)]}={row[col_names[i]].strip()}"
            for i in [4, 6, 7, 8]
            if row[col_names[i]].strip()
        ]
        return f"{name} [{', '.join(details)}]" if details else name

    station_list["station_name"] = station_list.apply(parse_station_name, axis=1)
    station_list.drop(columns=col_names[4:], inplace=True)
    station_list.to_parquet(paths.ghcnh_station_list())
    log.info("Conversion to parquet completed.")
    log.debug(
        "Input -> output: %s -> %s",
        paths.ghcnh_station_list(ext="csv"),
        paths.ghcnh_station_list(),
    )


def _inventory_to_parquet() -> None:
    """Convert the 'ghcnh-inventory' ASCII file into parquet."""
    inventory = (
        pd.read_fwf(
            paths.ghcnh_inventory(ext="txt"),
            colspecs="infer",
            header=0,
        )
        .rename(
            columns={
                "GHCNh_ID": "station_id",
                "YEAR": "year",
            },
            errors="ignore",
        )
        .set_index(["station_id", "year"])
    )
    inventory.to_parquet(paths.ghcnh_inventory())
    log.info("Conversion to parquet completed.")
    log.debug(
        "Input -> output: %s -> %s",
        paths.ghcnh_inventory(ext="txt"),
        paths.ghcnh_inventory(),
    )


def preprocess_ghcnh_metadata() -> None:
    """Filter countries, simplify inventory and compress GHCNh metadata files."""

    buffer = "tmp.txt"

    _filter_countries(paths.ghcnh_station_list(ext="csv"), paths.ghcnh() / buffer)
    subprocess.run(
        f"mv {paths.ghcnh() / buffer} {paths.ghcnh_station_list(ext='csv')}",
        shell=True,
        check=True,
    )

    _filter_countries(
        paths.ghcnh_inventory(ext="txt"), paths.ghcnh() / buffer, header=True
    )
    subprocess.run(
        f"mv {paths.ghcnh() / buffer} {paths.ghcnh_inventory(ext='txt')}",
        shell=True,
        check=True,
    )

    _simplify_inventory(paths.ghcnh() / buffer)
    subprocess.run(
        f"mv {paths.ghcnh() / buffer} {paths.ghcnh_inventory(ext='txt')}",
        shell=True,
        check=True,
    )

    _station_list_to_parquet()
    subprocess.run(f"rm {paths.ghcnh_station_list(ext='csv')}", shell=True, check=True)
    _inventory_to_parquet()
    subprocess.run(f"rm {paths.ghcnh_inventory(ext='txt')}", shell=True, check=True)

    log.info("GHCNh metadata preprocessing completed.")

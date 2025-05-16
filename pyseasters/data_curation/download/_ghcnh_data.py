import importlib.resources
import logging

import pandas as pd

from pyseasters.constants import paths
from pyseasters.ghcnh import load_ghcnh_inventory, load_ghcnh_station_list

__all__ = ["generate_ghcnh_data_download_script"]

log = logging.getLogger(__name__)

_DATA = (
    "https://www.ncei.noaa.gov/oa/global-historical-climatology-network"
    + "/hourly/access/by-year/%s/parquet/GHCNh_%s_%s.parquet"
)
_DATA_PSV = (
    "https://www.ncei.noaa.gov/oa/global-historical-climatology-network"
    + "/hourly/access/by-station/GHCNh_%s_por.psv"
)


def generate_ghcnh_data_download_script() -> None:
    """Generate a download script in bash for ghcnh data files."""

    folder = paths.ghcnh() / "data"
    folder.mkdir(parents=True, exist_ok=True)

    if not paths.ghcnh_inventory(ext="parquet").exists():
        raise RuntimeError(
            "File 'ghcnh-inventory.parquet' not found."
            + " Please download and preprocess GHCNh metadata before running this script."
        )
    inv = load_ghcnh_inventory()
    station_list = load_ghcnh_station_list().index.to_frame(index=False)

    years = inv.index.get_level_values("year").unique().to_list()
    fn_years = folder / "years.txt"
    with open(fn_years, "w") as file:
        file.write("\n".join(list(map(str, years))) + "\n")

    # Get station_id-year tuples from the inventory
    station_year = inv.index.to_frame(index=False)
    # Get station_id-year tuples we know are not available by year
    with importlib.resources.files("pyseasters.data_curation.data").joinpath(
        "ghcnh_station-year_missing_by-year.parquet"
    ).open("rb") as file:
        station_year_missing_by_year = pd.read_parquet(file)
    # Determine station_id-year tuples to download by year (in parquet)
    merged = station_year.merge(
        station_year_missing_by_year,
        on=["station_id", "year"],
        how="left",
        indicator=True,
    )
    station_year_parquet = merged[merged["_merge"] == "left_only"].drop(
        columns="_merge"
    )
    # Determine stations to download by station (in psv)
    merged = (
        station_year[["station_id"]]
        .drop_duplicates()
        .merge(
            station_list,
            on=["station_id"],
            how="left",
            indicator=True,
        )
    )
    station_psv = (
        pd.concat(
            [
                merged[merged["_merge"] == "left_only"].drop(
                    columns="_merge"
                ),  # stations in inventory but not in station-list
                station_year_missing_by_year[["station_id"]].drop_duplicates(),
            ]
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )
    # Save
    fn_station_year_parquet = folder / "station-year_parquet.txt"
    station_year_parquet.to_csv(
        fn_station_year_parquet, sep=" ", index=False, header=False
    )
    fn_station_psv = folder / "station_psv.txt"
    station_psv.to_csv(fn_station_psv, index=False, header=False)

    prefix = "by-year/${year}/"
    script = f"""#!/bin/bash

mkdir -p by-year
cd by-year
while IFS= read -r year; do
    mkdir -p year
done < years.txt
cd ..

while read station year; do
    wget -P {prefix} {_DATA % ('${year}', '${station}', '${year}')}
done < station-year_parquet.txt

mkdir -p by-station

while read station; do
    wget -P by-station/ {_DATA_PSV % ('${station}')}
done < station_psv.txt

"""

    fn_script = folder / "download.sh"
    with open(fn_script, "w") as file:
        file.write(script)
    log.info(
        "Script written in %s (with years in %s, station-year tuples in %s, "
        + "and station for psv file download in %s)",
        fn_script,
        fn_years,
        fn_station_year_parquet,
        fn_station_psv,
    )

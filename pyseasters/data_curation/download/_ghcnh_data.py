import logging

from pyseasters.constants import paths
from pyseasters.ghcnh import load_ghcnh_inventory

__all__ = ["generate_ghcnh_data_download_script"]

log = logging.getLogger(__name__)

_DATA = (
    "https://www.ncei.noaa.gov/oa/global-historical-climatology-network"
    + "/hourly/access/by-year/%s/parquet/GHCNh_%s_%s.parquet"
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

    years = inv.index.get_level_values("year").unique().to_list()
    fn_years = folder / "years.txt"
    with open(fn_years, "w") as file:
        file.write("\n".join(list(map(str, years))) + "\n")

    station_year = inv.index.to_frame(index=False)
    fn_station_year = folder / "station_year.txt"
    station_year.to_csv(fn_station_year, sep=" ", index=False, header=False)

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
done < station_year.txt

"""
    # noqa: E272, E702

    fn_script = folder / "download.sh"
    with open(fn_script, "w") as file:
        file.write(script)
    log.info(
        "Script written in %s (with years in %s and station-year tuples in %s)",
        fn_script,
        fn_years,
        fn_station_year,
    )

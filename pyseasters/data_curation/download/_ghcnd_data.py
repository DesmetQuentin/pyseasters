import logging

from pyseasters.constants import paths
from pyseasters.ghcnd import get_ghcnd_station_list

__all__ = ["generate_ghcnd_data_download_script"]

log = logging.getLogger(__name__)

_DATA = (
    "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily"
    + "/access/%s.csv"
)


def generate_ghcnd_data_download_script() -> None:
    """Generate a download script in bash for GHCNd data files."""

    folder = paths.ghcnd() / "data"
    folder.mkdir(parents=True, exist_ok=True)

    if not paths.ghcnd_stations(ext="parquet").exists():
        raise RuntimeError(
            "File 'ghcnd-stations.parquet' not found."
            + " Please download and preprocess GHCNd metadata before running this script."
        )
    stations = get_ghcnd_station_list()

    fn_stations = folder / "stations.txt"
    with open(fn_stations, "w") as file:
        file.write("\n".join(stations) + "\n")

    script = f"""#!/bin/bash

while IFS= read -r station; do
    wget {_DATA % ('${station}')}
done < stations.txt

"""  # noqa: E272, E702

    fn_script = folder / "download.sh"
    with open(fn_script, "w") as file:
        file.write(script)
    log.info("Script written in %s (with stations in %s)", fn_script, fn_stations)

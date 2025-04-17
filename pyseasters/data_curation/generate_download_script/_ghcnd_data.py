from pathlib import Path
from typing import Optional, Union

from pyseasters.ghcnd import get_ghcnd_station_list

DATA = (
    "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily"
    + "/access/%s.csv"
)


def generate_ghcnd_data_download_script(
    output: Optional[Union[str, Path]] = None,
) -> str:
    """Generate a download script in bash for GHCNd data files.

    Args:
        output: Optional path to an output file to write the script in.

    Returns:
        Returns the download script as an str.
    """

    stations = get_ghcnd_station_list()

    script = f"""#!/bin/bash

for station in {' '.join(stations)}; do
    wget {DATA % ('${station}')}
done
"""  # noqa: E272, E702

    if output is not None:
        with open(output, "w") as file:
            file.write(script)

    return script

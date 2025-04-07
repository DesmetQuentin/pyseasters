from pathlib import Path
from typing import Optional, Union

from pyseasters.commands.ghcnd.parse_station_metadata import _parse_station_metadata
from pyseasters.constants.pathconfig import paths

_STATIONS = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
_INVENTORY = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
_DATA = (
    "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily"
    + "/access/%s.csv"
)


def _generate_metadata_script(
    output: Optional[Union[str, Path]] = None,
) -> str:
    """Generate a download script in bash for GHCNd stations and inventory files.

    Args:
        output: Optional path to an output file to write the script in.

    Returns:
        Returns the download script as an str.

    """

    script = f"""#!/bin/bash

wget {_STATIONS}
wget {_INVENTORY}
"""

    if output is not None:
        with open(output, "w") as file:
            file.write(script)

    return script


def _generate_main_script(
    output: Optional[Union[str, Path]] = None,
) -> str:
    """Generate a download script in bash for GHCNd data files.

    Args:
        output: Optional path to an output file to write the script in.

    Returns:
        Returns the download script as an str.

    """

    try:
        stations = _parse_station_metadata(paths.ghcnd_stations()).iloc[:, 0].to_list()
    except FileNotFoundError:
        raise FileNotFoundError(
            "GHCNd metadata files not found. Please download and preprocess metadata "
            + "before running this function."
        )

    script = f"""#!/bin/bash

for station in {' '.join(stations)}; do
    wget {_DATA %('${station}')}
done
"""  # noqa: E272, E702

    if output is not None:
        with open(output, "w") as file:
            file.write(script)

    return script

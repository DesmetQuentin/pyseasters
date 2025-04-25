from typing import Optional

from pyseasters.ghcnd import get_ghcnd_station_list
from pyseasters.utils._typing import PathLike

__all__ = ["generate_ghcnd_data_download_script"]

_DATA = (
    "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily"
    + "/access/%s.csv"
)


def generate_ghcnd_data_download_script(
    output: Optional[PathLike] = None,
) -> str:
    """Generate a download script in bash for GHCNd data files.

    Parameters
    ----------
    output
        Optional path to an output file to write the script in.

    Returns
    -------
    script : str
        The download script.
    """

    stations = get_ghcnd_station_list()

    script = f"""#!/bin/bash

for station in {' '.join(stations)}; do
    wget {_DATA % ('${station}')}
done
"""  # noqa: E272, E702

    if output is not None:
        with open(output, "w") as file:
            file.write(script)

    return script

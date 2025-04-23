from typing import Optional

from pyseasters.utils._typing import PathLike

STATIONS = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
INVENTORY = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"


def generate_ghcnd_metadata_download_script(
    output: Optional[PathLike] = None,
) -> str:
    """Generate a download script in bash for GHCNd stations and inventory files.

    Parameters
    ----------
    output
        Optional path to an output file to write the script in.

    Returns
    -------
    script : str
        The download script.
    """

    script = f"""#!/bin/bash

wget {STATIONS}
wget {INVENTORY}
"""

    if output is not None:
        with open(output, "w") as file:
            file.write(script)

    return script

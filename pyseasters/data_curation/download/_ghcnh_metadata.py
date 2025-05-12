import logging

from pyseasters.constants import paths

__all__ = ["generate_ghcnh_metadata_download_script"]

log = logging.getLogger(__name__)

_STATION_LIST = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-station-list.csv"
_INVENTORY = "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/doc/ghcnh-inventory.txt"


def generate_ghcnh_metadata_download_script() -> None:
    """Generate a download script in bash for GHCNh station list and inventory files."""

    folder = paths.ghcnh() / "metadata"
    folder.mkdir(parents=True, exist_ok=True)

    script = f"""#!/bin/bash

wget {_STATION_LIST}
wget {_INVENTORY}
"""

    fn = folder / "download.sh"
    with open(fn, "w") as file:
        file.write(script)
    log.info("Script written in %s", fn)

import logging

from pyseasters.constants import paths

__all__ = ["generate_ghcnd_metadata_download_script"]

log = logging.getLogger(__name__)

_STATIONS = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
_INVENTORY = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"


def generate_ghcnd_metadata_download_script() -> None:
    """Generate a download script in bash for GHCNd stations and inventory files."""

    folder = paths.ghcnd() / "metadata"
    folder.mkdir(parents=True, exist_ok=True)

    script = f"""#!/bin/bash

wget {_STATIONS}
wget {_INVENTORY}
"""

    fn = folder / "download.sh"
    with open(fn, "w") as file:
        file.write(script)
    log.info("Script written in %s", fn)

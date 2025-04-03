from pathlib import Path
from typing import Optional

import pandas as pd

from pyseasters.config import paths

STATIONS = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
INVENTORY = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
DATA = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/%s.csv"


def _generate_metadata_script(
    output: Optional[str] = None,
) -> Optional[str]:

    script = f"""#!/bin/bash

wget {STATIONS}
wget {INVENTORY}
"""

    if output is not None:
        with open(output, "w") as file:
            file.write(script)
    else:
        return script


def _generate_main_script(
    output: Optional[str] = None,
) -> Optional[str]:

    print(pd.read_csv(paths.ghcnd_stations, delimiter=" "))
    try:
        stations = pd.read_csv(paths.ghcnd_stations, delimiter=" ").iloc[0].to_list()
    except FileNotFoundError:
        raise FileNotFoundError(
            "GHCNd metadata files not found. Please download and preprocess metadata before calling this function."
        )

    script = f"""#!/bin/bash

for link in {' '.join([DATA %(station) for station in stations])}; do
    wget $link
done
"""

    if output is not None:
        with open(output, "w") as file:
            file.write(script)
    else:
        return script

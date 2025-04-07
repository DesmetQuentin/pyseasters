from pathlib import Path
from typing import Union

import pandas as pd


def _parse_station_metadata(file: Union[str, Path]) -> pd.DataFrame:
    colspecs = [
        (0, 11),
        (12, 20),
        (21, 30),
        (31, 37),
        (38, 85),
    ]
    names = ["station id", "lat", "lon", "elevation", "long name"]
    stations = pd.read_fwf(file, colspecs=colspecs, names=names)
    return stations

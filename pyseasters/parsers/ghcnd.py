from datetime import datetime
from pathlib import Path
from typing import Union

import pandas as pd


def _parse_ghcnd_date(date: str) -> datetime:
    """Parse GHCNd date format into a `datetime.datetime`."""
    return datetime.strptime(date, "%Y-%m-%d")


def _parse_ghcnd_stations(file: Union[str, Path]) -> pd.DataFrame:
    """Parse the 'ghcnd-stations.txt' ASCII file into a `pd.DataFrame`."""
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

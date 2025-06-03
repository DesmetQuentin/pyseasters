from typing import List

import pandas as pd

from pyseasters.constants import paths

__all__ = [
    "load_ghcnh_station_list",
    "get_ghcnh_station_list",
    "load_ghcnh_inventory",
]


def load_ghcnh_station_list() -> pd.DataFrame:
    """Load the 'ghcnh-station-list' parquet file into a pandas DataFrame."""
    return pd.read_parquet(paths.ghcnh_station_list())


def get_ghcnh_station_list() -> List[str]:
    """Return GHCNh station IDs as a list."""
    return load_ghcnh_station_list().index.to_list()


def load_ghcnh_inventory(var: str = "") -> pd.DataFrame:
    """Load the 'ghcnh-inventory' parquet file into a pandas DataFrame."""
    return pd.read_parquet(paths.ghcnh_inventory(var=var))

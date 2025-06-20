from typing import List

import pandas as pd

from pyseasters.constants import paths

__all__ = [
    "load_gsdr_stations",
    "get_gsdr_station_list",
    "load_gsdr_inventory",
    "get_gsdr_metadata",
]


def load_gsdr_stations() -> pd.DataFrame:
    """Load the 'gsdr-station-list' parquet file into a pandas DataFrame."""
    return pd.read_parquet(paths.gsdr_stations())


def get_gsdr_station_list() -> List[str]:
    """Return GHCNh station IDs as a list."""
    return load_gsdr_stations().index.to_list()


def load_gsdr_inventory() -> pd.DataFrame:
    """Load the 'gsdr-inventory' parquet file into a pandas DataFrame."""
    return pd.read_parquet(paths.gsdr_inventory())


def get_gsdr_metadata() -> pd.DataFrame:
    """Concatenate station and inventory GSDR files into a pandas DataFrame."""
    stations = load_gsdr_stations()
    inventory = load_gsdr_inventory()
    metadata = pd.concat([stations, inventory], axis=1)
    return metadata

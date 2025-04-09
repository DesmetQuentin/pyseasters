from typing import List, Optional

import pandas as pd

from pyseasters.api.constants import paths

__all__ = [
    "load_ghcnd_stations",
    "get_ghcnd_station_list",
    "load_ghcnd_inventory",
    "get_ghcnd_metadata",
]

_col_names_stations = ["station_id", "lat", "lon", "elevation", "station_name"]
_col_names_inventory = ["station_id", "var", "start", "end"]


def load_ghcnd_stations() -> pd.DataFrame:
    """Load the 'ghcnd-stations.txt' ASCII file into a pandas DataFrame."""
    colspecs = [
        (0, 11),
        (12, 20),
        (21, 30),
        (31, 37),
        (38, 85),
    ]

    try:
        stations = pd.read_fwf(
            paths.ghcnd_stations(), colspecs=colspecs, names=_col_names_stations
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            "GHCNd station file not found. Please download and preprocess "
            + "'ghcnd-stations.txt' before running this function."
        )

    stations = stations.set_index(_col_names_stations[0])

    return stations


def get_ghcnd_station_list() -> list:
    """Return GHCNd station IDs as a list."""
    return load_ghcnd_stations().index.to_list()


def load_ghcnd_inventory(usevars: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Load the 'ghcnd-inventory.txt' ASCII file into a pandas DataFrame,
    with optional variable filtering through ``usevars``.
    """

    try:
        inventory = pd.read_csv(
            paths.ghcnd_inventory(),
            sep="\s+",  # noqa: W605
            header=None,
            names=_col_names_inventory,
            index_col=_col_names_inventory[0],
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            "GHCNd inventory file not found. Please download and preprocess "
            + "'ghcnd-inventory.txt' before running this function."
        )

    if usevars is not None:
        if len(usevars) == 0:
            raise ValueError("`usevars` cannot have zero length.")

        inventory = inventory[inventory["var"].isin(usevars)].drop("var", axis=1)

    return inventory


def get_ghcnd_metadata(var: str = "PRCP") -> pd.DataFrame:
    """
    Concatenate station and inventory GHCNd files for the variable ``var`` into a pandas
    DataFrame.
    """
    stations = load_ghcnd_stations()
    inventory = load_ghcnd_inventory(usevars=[var])
    metadata = pd.concat([stations, inventory], axis=1)
    return metadata

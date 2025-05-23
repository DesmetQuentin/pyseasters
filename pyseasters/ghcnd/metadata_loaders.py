from typing import List, Optional

import pandas as pd

from pyseasters.constants import paths

__all__ = [
    "load_ghcnd_stations",
    "get_ghcnd_station_list",
    "load_ghcnd_inventory",
    "get_ghcnd_metadata",
]


def load_ghcnd_stations(from_parquet: bool = True) -> pd.DataFrame:
    """Load the 'ghcnd-stations' ASCII/parquet file into a pandas DataFrame."""

    if from_parquet:
        stations = pd.read_parquet(paths.ghcnd_stations())

    else:
        col_names = ["station_id", "lat", "lon", "elevation", "station_name"]
        colspecs = [
            (0, 11),
            (12, 20),
            (21, 30),
            (31, 37),
            (38, 85),
        ]
        stations = pd.read_fwf(
            paths.ghcnd_stations(ext="txt"), colspecs=colspecs, names=col_names
        ).set_index(col_names[0])

    return stations


def get_ghcnd_station_list(from_parquet: bool = True) -> list:
    """Return GHCNd station IDs as a list."""
    return load_ghcnd_stations(from_parquet=from_parquet).index.to_list()


def load_ghcnd_inventory(
    usevars: Optional[List[str]] = None,
    from_parquet: bool = True,
    multiindex: bool = True,
) -> pd.DataFrame:
    """
    Load the 'ghcnd-inventory' ASCII/parquet file into a pandas DataFrame,
    with optional variable filtering through ``usevars``.
    """

    # Load
    if from_parquet:
        inventory = pd.read_parquet(paths.ghcnd_inventory())
    else:
        col_names = ["station_id", "var", "start", "end"]
        inventory = pd.read_csv(
            paths.ghcnd_inventory(ext="txt"),
            sep=r"\s+",
            header=None,
            names=col_names,
        )

    # Select variables in ``usevars``
    if usevars is not None:
        if len(usevars) == 0:
            raise ValueError("`usevars` cannot have zero length.")

        inventory = inventory[inventory["var"].isin(usevars)]

        if len(usevars) == 1:
            inventory = inventory.drop("var", axis=1)

    # Set index/multiindex
    if multiindex and ((usevars is None) or (len(usevars) > 1)):
        inventory.set_index(["station_id", "var"], inplace=True)
    elif (usevars is not None) and (len(usevars) == 1):
        inventory.set_index("station_id", inplace=True)

    return inventory


def get_ghcnd_metadata(var: str = "PRCP", from_parquet: bool = True) -> pd.DataFrame:
    """
    Concatenate station and inventory GHCNd files for the variable ``var`` into a pandas
    DataFrame.
    """
    stations = load_ghcnd_stations(from_parquet=from_parquet)
    inventory = load_ghcnd_inventory(usevars=[var], from_parquet=from_parquet)
    metadata = pd.concat([stations.loc[inventory.index], inventory], axis=1)
    return metadata

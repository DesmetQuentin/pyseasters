from typing import List, Optional

import pandas as pd

from pyseasters.constants import paths

__all__ = [
    "load_ghcnd_stations",
    "get_ghcnd_station_list",
    "load_ghcnd_inventory",
    "get_ghcnd_metadata",
]


def load_ghcnd_stations() -> pd.DataFrame:
    """Load the 'ghcnd-stations' parquet file into a pandas DataFrame."""
    return pd.read_parquet(paths.ghcnd_stations())


def get_ghcnd_station_list() -> List[str]:
    """Return GHCNd station IDs as a list."""
    return load_ghcnd_stations().index.to_list()


def load_ghcnd_inventory(
    usevars: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Load the 'ghcnd-inventory' parquet file into a pandas DataFrame,
    with optional variable filtering through ``usevars``.
    """

    # Load
    inventory = pd.read_parquet(paths.ghcnd_inventory())

    # Select variables in ``usevars``
    if usevars is not None:
        if len(usevars) == 0:
            raise ValueError("`usevars` cannot have zero length.")
        elif len(usevars) == 1:
            selection = inventory.xs(key=usevars[0], level="var", drop_level=True)
            assert isinstance(selection, pd.DataFrame)
            inventory = selection
        else:
            inventory = inventory.loc[pd.IndexSlice[:, usevars], :]

    return inventory


def get_ghcnd_metadata(var: str = "PRCP") -> pd.DataFrame:
    """
    Concatenate station and inventory GHCNd files for the variable ``var`` into a pandas
    DataFrame.
    """
    stations = load_ghcnd_stations()
    inventory = load_ghcnd_inventory(usevars=[var])
    metadata = pd.concat([stations.loc[inventory.index], inventory], axis=1)
    return metadata

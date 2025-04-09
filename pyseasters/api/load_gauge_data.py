from datetime import datetime
from typing import Callable, List, Optional, Tuple

import pandas as pd

from .ghcnd import load_ghcnd_data
from .utils import check_dataframe_unit

__all__ = ["load_gauge_data"]

_gauge_data_sources = [
    "GHCNd",
]


def _renamer(source: str) -> Callable[[str], str]:
    def mapper(station_id):
        return f"{source}:{station_id}"

    return mapper


def _dispatcher(source: str, **kwargs) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if source == "GHCNd":
        data, metadata = load_ghcnd_data(
            var=kwargs.get("var", "PRCP"),
            filter_condition=kwargs.get("filter_condition"),
            time_range=kwargs.get("time_range"),
        )
    else:
        raise ValueError(
            f"'{source}' is not a valid source. Please provide one in "
            + f"{_gauge_data_sources}."
        )

    # Add the source as a prefix to the station ID
    data = data.rename(_renamer(source), axis=1)
    metadata.index = metadata.index.map(_renamer(source))

    return data, metadata


def load_gauge_data(
    filter_condition: Optional[str] = None,
    time_range: Optional[Tuple[datetime, datetime]] = None,
    usesources: List[str] = _gauge_data_sources,
    unit: str = "mm day-1",
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    all_data, all_metadata = [], []
    for source in usesources:
        data, metadata = _dispatcher(
            source=source,
            var="PRCP",
            filter_condition=filter_condition,
            time_range=time_range,
        )
        data = check_dataframe_unit(data, target_unit="mm/day")
        all_data.append(data)
        all_metadata.append(metadata)

    combined_data = pd.concat(all_data, axis=1)
    combined_metadata = pd.concat(all_metadata, axis=0)

    return combined_data, combined_metadata

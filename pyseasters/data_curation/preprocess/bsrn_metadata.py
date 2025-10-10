"""Provide the single :func:`preprocess_bsrn_metadata` function."""

import logging

import pandas as pd

from pyseasters.constants import paths

__all__ = ["preprocess_bsrn_metadata"]

log = logging.getLogger(__name__)


def preprocess_bsrn_metadata() -> None:
    """Rename and reorder columns and compress BSRN stations and parameters files."""
    # Station metadata
    stations = (
        pd.read_csv(
            paths.bsrn_stations(ext="tab"),
            sep="\t",
            header=0,
        )
        .rename(
            columns={
                "Station long name": "name",
                "Abbreviation": "station_id",
                "Latitude": "lat",
                "Longitude": "lon",
                "Elevation": "elevation,",
            }
        )
        .set_index("station_id")
    )
    stations.to_parquet(paths.bsrn_stations())
    paths.bsrn_stations(ext="tab").unlink()

    log.info("Station metadata preprocessing completed.")

    # Parameter metadata
    parameters = pd.read_csv(
        paths.bsrn_parameters(ext="tab"), sep="\t", header=0, index_col="Abbreviation"
    )
    parameters.to_parquet(paths.bsrn_parameters())
    paths.bsrn_parameters(ext="tab").unlink()

    log.info("Parameter metadata preprocessing completed.")

    log.info("BSRN metadata preprocessing completed.")

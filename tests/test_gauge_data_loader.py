import pandas as pd
import pytest

from pyseasters.gauge_data_loader import (
    _GAUGE_DATA_SOURCES,
    _dispatcher,
    _renamer,
    load_1h_gauge_data,
)


@pytest.fixture
def tmp_data_df():
    """Fixture for a realistic data DataFrame."""
    df = pd.DataFrame(
        {
            "station1": [1.0, 2.0, 3.0],
            "station2": [20.0, 21.0, 22.0],
        },
        index=pd.to_datetime(
            [
                "2000-01-01",
                "2001-01-01",
                "2002-01-01",
            ]
        ),
    ).rename_axis("time")
    df.attrs["units"] = "mm/day"
    return df


@pytest.fixture
def tmp_metadata_df():
    """Fixture for a realistic metadata DataFrame."""
    df = pd.DataFrame(
        {
            "lat": [10.0, 20.0],
            "lon": [100.0, 110.0],
            "elevation": [50.0, 100.0],
            "station_name": ["Station A", "Station B"],
        },
        index=["station1", "station2"],
    ).rename_axis("station_id")
    return df


def patch_source_functions(data, metadata, patch):
    """Patch source functions to return predefined DataFrames."""
    for source in _GAUGE_DATA_SOURCES:
        if source == "GHCNd":
            patch.setattr(
                "pyseasters.gauge_data_loader.load_ghcnd",
                lambda *args, **kwargs: (data, metadata),
            )


class TestRenamer:
    def test(self):
        """Single test for this function."""
        mapper = _renamer("SOURCE")
        assert mapper("station_id") == "SOURCE:station_id"


class TestDispatcher:
    def test_known_sources(self, tmp_data_df, tmp_metadata_df, monkeypatch):
        """Test when called with known sources."""
        patch_source_functions(tmp_data_df, tmp_metadata_df, monkeypatch)
        for source in _GAUGE_DATA_SOURCES:
            data, metadata = _dispatcher(source)
            assert data.columns.to_list() == list(
                map(_renamer(source), tmp_data_df.columns)
            )
            assert metadata.index.to_list() == list(
                map(_renamer(source), tmp_metadata_df.index)
            )

    def test_error_invalid_source(self):
        """Test error raising when source is invalid."""
        with pytest.raises(ValueError):
            _dispatcher("UNKNOWN_SOURCE")


class TestLoadGaugeData:
    def test(self, tmp_data_df, tmp_metadata_df, monkeypatch):
        """Single test for this function."""
        patch_source_functions(tmp_data_df, tmp_metadata_df, monkeypatch)
        data, metadata = load_1h_gauge_data()
        assert data.shape[1] == tmp_data_df.shape[1] * len(_GAUGE_DATA_SOURCES)
        assert metadata.shape[0] == tmp_metadata_df.shape[0] * len(_GAUGE_DATA_SOURCES)

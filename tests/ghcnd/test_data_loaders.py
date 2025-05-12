from datetime import datetime

import pandas as pd
import pytest

from pyseasters.ghcnd.data_loaders import (
    _load_ghcnd_single_var_station,
    load_ghcnd_data,
)


@pytest.fixture
def tmp_data1_df():
    """Fixture for a realistic data DataFrame version 1."""
    return pd.DataFrame(
        {
            "PRCP": [1.0, 2.0, 3.0],
            "TAVG": [20.0, 21.0, 22.0],
        },
        index=pd.to_datetime(
            [
                "2000-01-01",
                "2001-01-01",
                "2002-01-01",
            ]
        ),
    ).rename_axis("time")


@pytest.fixture
def tmp_data2_df():
    """Fixture for a realistic data DataFrame version 2."""
    return pd.DataFrame(
        {
            "PRCP": [4.0, 5.0, 6.0],
        },
        index=pd.to_datetime(
            [
                "2002-01-01",
                "2003-01-01",
                "2004-01-01",
            ]
        ),
    ).rename_axis("time")


@pytest.fixture
def tmp_station_df():
    """Fixture for a realistic station metadata DataFrame."""
    df = pd.DataFrame(
        {
            "lat": [10.0, 20.0],
            "lon": [100.0, 110.0],
            "elevation": [50.0, 100.0],
            "station_name": ["Station A", "Station B"],
        },
        index=["ID000000001", "ID000000002"],
    ).rename_axis("station_id")
    return df


@pytest.fixture
def tmp_metadata_prcp_df():
    """Fixture for a realistic PRCP metadata DataFrame."""
    df = pd.DataFrame(
        {
            "lat": [10.0, 20.0],
            "lon": [100.0, 110.0],
            "elevation": [50.0, 100.0],
            "station_name": ["Station A", "Station B"],
            "start": [2000, 2002],
            "end": [2002, 2004],
        },
        index=["ID000000001", "ID000000002"],
    ).rename_axis("station_id")
    return df


class TestLoadGHCNdSingleVarStation:
    def test_from_parquet(self, tmp_path, tmp_data1_df, monkeypatch):
        """Test when loading from parquet."""
        # Save dummy DataFrame as parquet
        station_id = "ID000000001"
        fn = tmp_path / f"{station_id}.parquet"
        tmp_data1_df.to_parquet(fn)

        # Patch paths.ghcnd_stations() to return this path
        from pyseasters.constants import paths

        monkeypatch.setattr(
            paths, "ghcnd_file", lambda station_id=station_id, ext="parquet": fn
        )

        # Assert
        result = _load_ghcnd_single_var_station(station_id=station_id, var="PRCP")
        expected = (
            tmp_data1_df.loc[:, "PRCP"]
            .to_frame()
            .dropna()
            .rename(columns={"PRCP": station_id})
        )
        pd.testing.assert_frame_equal(result, expected)


class TestLoadGHCNdData:
    def setup_dummy_files(self, path, df1, df2, df_station, df_meta, patch):
        """Save inventory fixture to a temporary file and patch paths method to it."""
        # Save dummy DataFrames as parquet
        station_id1, station_id2 = "ID000000001", "ID000000002"
        fn1 = path / f"{station_id1}.parquet"
        fn2 = path / f"{station_id2}.parquet"
        df1.to_parquet(fn1)
        df2.to_parquet(fn2)

        # Patch paths.ghcnd_stations() to return those paths
        from pyseasters.constants import paths

        patch.setattr(
            paths,
            "ghcnd_file",
            lambda station_id="ID000000001", ext="parquet": {
                station_id1: fn1,
                station_id2: fn2,
            }[station_id],
        )
        patch.setattr(
            "pyseasters.ghcnd.data_loaders.get_ghcnd_metadata",
            lambda var="PRCP": df_meta,
        )

    def test(
        self,
        tmp_path,
        tmp_data1_df,
        tmp_data2_df,
        tmp_station_df,
        tmp_metadata_prcp_df,
        monkeypatch,
    ):
        """Test nominal behavior."""
        self.setup_dummy_files(
            tmp_path,
            tmp_data1_df,
            tmp_data2_df,
            tmp_station_df,
            tmp_metadata_prcp_df,
            monkeypatch,
        )
        data, metadata = load_ghcnd_data(var="PRCP")
        assert data.shape == (5, 2)
        assert "units" in data.attrs
        assert data.attrs["units"] == "mm/day"
        assert not data.empty
        assert not metadata.empty

    def test_with_filter_condition(
        self,
        tmp_path,
        tmp_data1_df,
        tmp_data2_df,
        tmp_station_df,
        tmp_metadata_prcp_df,
        monkeypatch,
    ):
        """Test when providing a filter condition."""
        self.setup_dummy_files(
            tmp_path,
            tmp_data1_df,
            tmp_data2_df,
            tmp_station_df,
            tmp_metadata_prcp_df,
            monkeypatch,
        )
        filter_condition = "lat > 15"
        data, metadata = load_ghcnd_data(var="PRCP", filter_condition=filter_condition)
        assert data.shape == (3, 1)
        assert not data.empty
        assert not metadata.empty

    def test_error_invalid_filter(
        self,
        tmp_path,
        tmp_data1_df,
        tmp_data2_df,
        tmp_station_df,
        tmp_metadata_prcp_df,
        monkeypatch,
    ):
        """Test error raising when providing an invalid filter condition."""
        self.setup_dummy_files(
            tmp_path,
            tmp_data1_df,
            tmp_data2_df,
            tmp_station_df,
            tmp_metadata_prcp_df,
            monkeypatch,
        )
        filter_condition = "lattitude > 15"
        with pytest.raises(RuntimeError):
            load_ghcnd_data(var="PRCP", filter_condition=filter_condition)

    def test_with_time_range(
        self,
        tmp_path,
        tmp_data1_df,
        tmp_data2_df,
        tmp_station_df,
        tmp_metadata_prcp_df,
        monkeypatch,
    ):
        """Test when providing a time range."""
        self.setup_dummy_files(
            tmp_path,
            tmp_data1_df,
            tmp_data2_df,
            tmp_station_df,
            tmp_metadata_prcp_df,
            monkeypatch,
        )
        time_range = (datetime(2000, 1, 1), datetime(2002, 1, 1))
        data, metadata = load_ghcnd_data(var="PRCP", time_range=time_range)
        assert data.shape == (3, 2)
        assert not data.empty
        assert not metadata.empty

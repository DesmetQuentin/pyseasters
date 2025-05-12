import pandas as pd
import pytest

from pyseasters.ghcnd.metadata_loaders import (
    get_ghcnd_metadata,
    get_ghcnd_station_list,
    load_ghcnd_inventory,
    load_ghcnd_stations,
)


@pytest.fixture
def tmp_station_df():
    """Fixture of a realistic station metadata DataFrame."""
    return pd.DataFrame(
        {
            "station_id": ["ID000000001", "ID000000002"],
            "lat": [10.0, 20.0],
            "lon": [100.0, 110.0],
            "elevation": [50.0, 100.0],
            "station_name": ["Station A", "Station B"],
        }
    ).set_index("station_id")


@pytest.fixture
def tmp_inventory_df():
    """Fixture of a realistic inventory DataFrame."""
    return pd.DataFrame(
        {
            "station_id": ["ID000000001", "ID000000001", "ID000000001", "ID000000002"],
            "var": ["TMAX", "TAVG", "PRCP", "PRCP"],
            "start": [1950, 1980, 2000, 2020],
            "end": [1960, 1990, 2010, 2021],
        }
    )


class TestLoadGHCNdStations:
    def test_from_parquet(self, tmp_path, tmp_station_df, monkeypatch):
        """Test when loading from parquet."""
        # Save dummy DataFrame as parquet
        fn = tmp_path / "ghcnd-stations.parquet"
        tmp_station_df.to_parquet(fn)

        # Patch paths.ghcnd_stations() to return this path
        from pyseasters.constants import paths

        monkeypatch.setattr(paths, "ghcnd_stations", lambda ext="parquet": fn)

        # Assert
        result = load_ghcnd_stations()
        pd.testing.assert_frame_equal(result, tmp_station_df)

    def test_from_txt(self, tmp_path, tmp_station_df, monkeypatch):
        """Test when loading from txt."""
        # Save dummy DataFrame as fixed-width text
        txt = (
            "ID000000001  10.0000  100.0000   50.0    Station A\n"
            "ID000000002  20.0000  110.0000  100.0    Station B\n"
        )
        fn = tmp_path / "ghcnd-stations.txt"
        fn.write_text(txt)

        # Patch paths.ghcnd_stations() to return this path
        from pyseasters.constants import paths

        monkeypatch.setattr(paths, "ghcnd_stations", lambda ext="txt": fn)

        # Assert
        result = load_ghcnd_stations()
        pd.testing.assert_frame_equal(result, tmp_station_df)


class TestGetGHCNdList:
    def test(self, tmp_path, tmp_station_df, monkeypatch):
        """Single test for this function."""
        # Save dummy DataFrame as parquet
        fn = tmp_path / "ghcnd-stations.parquet"
        tmp_station_df.to_parquet(fn)

        # Patch paths.ghcnd_stations() to return this path
        from pyseasters.constants import paths

        monkeypatch.setattr(paths, "ghcnd_stations", lambda ext="parquet": fn)

        # Assert
        result = get_ghcnd_station_list()
        assert result == tmp_station_df.index.to_list()


class TestLoadGHCNdInventory:
    def setup_dummy_file(self, path, df, patch):
        """Save inventory fixture to a temporary file and patch paths method to it."""
        # Save dummy DataFrame as parquet
        fn = path / "ghcnd-inventory.parquet"
        df.to_parquet(fn)

        # Patch paths.ghcnd_inventory() to return this path
        from pyseasters.constants import paths

        patch.setattr(paths, "ghcnd_inventory", lambda ext="parquet": fn)

    def test_from_parquet(self, tmp_path, tmp_inventory_df, monkeypatch):
        """Test when loading from parquet."""
        self.setup_dummy_file(tmp_path, tmp_inventory_df, monkeypatch)
        result = load_ghcnd_inventory()
        pd.testing.assert_frame_equal(result, tmp_inventory_df)

    def test_use_two_vars(self, tmp_path, tmp_inventory_df, monkeypatch):
        """Test when selecting two variables."""
        self.setup_dummy_file(tmp_path, tmp_inventory_df, monkeypatch)
        twovars = ["TMAX", "PRCP"]
        result = load_ghcnd_inventory(usevars=twovars)
        pd.testing.assert_frame_equal(
            result, tmp_inventory_df[tmp_inventory_df["var"].isin(twovars)]
        )

    def test_use_one_var(self, tmp_path, tmp_inventory_df, monkeypatch):
        """Test when selecting one single variable."""
        self.setup_dummy_file(tmp_path, tmp_inventory_df, monkeypatch)
        onevar = ["PRCP"]
        result = load_ghcnd_inventory(usevars=onevar)
        pd.testing.assert_frame_equal(
            result,
            tmp_inventory_df[tmp_inventory_df["var"].isin(onevar)]
            .drop("var", axis=1)
            .set_index("station_id"),
        )

    def test_error_usevars_empty(self, tmp_path, tmp_inventory_df, monkeypatch):
        """Test error raising when usevars is an empty list."""
        self.setup_dummy_file(tmp_path, tmp_inventory_df, monkeypatch)
        with pytest.raises(ValueError):
            load_ghcnd_inventory(usevars=[])

    def test_multiindex(self, tmp_path, tmp_inventory_df, monkeypatch):
        """Test when asking for multiindex formatting."""
        self.setup_dummy_file(tmp_path, tmp_inventory_df, monkeypatch)
        result = load_ghcnd_inventory()
        pd.testing.assert_frame_equal(
            result, tmp_inventory_df.set_index(["station_id", "var"])
        )


class TestGetGHCNdMetadata:
    def test(self, tmp_path, tmp_station_df, tmp_inventory_df, monkeypatch):
        """Single test for this function."""
        # Save dummy DataFrames as parquet
        fn_stations = tmp_path / "ghcnd-stations.parquet"
        tmp_station_df.to_parquet(fn_stations)
        fn_inventory = tmp_path / "ghcnd-inventory.parquet"
        tmp_inventory_df.to_parquet(fn_inventory)

        # Patch paths.ghcnd_stations() and paths.ghcnd_inventory() to return those paths
        from pyseasters.constants import paths

        monkeypatch.setattr(paths, "ghcnd_stations", lambda ext="parquet": fn_stations)
        monkeypatch.setattr(
            paths, "ghcnd_inventory", lambda ext="parquet": fn_inventory
        )

        # Assert
        result = get_ghcnd_metadata(var="PRCP")
        expected = pd.concat(
            [
                tmp_station_df,
                tmp_inventory_df[tmp_inventory_df["var"] == "PRCP"]
                .drop("var", axis=1)
                .set_index("station_id"),
            ],
            axis=1,
        )
        pd.testing.assert_frame_equal(result, expected)

import subprocess
from unittest.mock import patch

import pandas as pd
import pytest

from pyseasters.constants import paths
from pyseasters.data_curation.preprocess.ghcnd_metadata import (
    _clean_columns,
    _filter_countries,
    _inventory_to_parquet,
    _stations_to_parquet,
    preprocess_ghcnd_metadata,
)


@pytest.fixture(autouse=True)
def patch_require_tools():
    """Cancel the `require_tools` decorator."""
    with patch(
        "pyseasters.utils._dependencies.require_tools",
        lambda *args, **kwargs: (lambda f: f),
    ):
        yield


def patch_subprocess_run(key):
    """
    Patch `subprocess.run` depending on `key`
    in 'failure:command', 'count:i' or 'no_run'.
    """

    def failure(args, *a, **kw):
        for command in {"mv", "rm", "cat", "tr", "cut", "awk"}:
            if key.split(":")[1] == command and command in args:
                raise subprocess.CalledProcessError(1, args[0])
        return subprocess.CompletedProcess(args, 0)

    def count(args, *a, **kw):
        return subprocess.CompletedProcess(args, 0, stdout=key.split(":")[1] + "\n")

    def no_run(args, *a, **kw):
        return subprocess.CompletedProcess(args, 0)

    run_type = key.split(":")[0]
    if run_type == "failure":
        return failure
    elif run_type == "count":
        return count
    elif run_type == "no_run":
        return no_run


@pytest.fixture
def tmp_sid():
    """Fixture to return a station ID."""
    return "ID001"


@pytest.fixture
def tmp_indices():
    """Fixture to return a list of indices."""
    return [2, 3]


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


class TestFilterCountries:
    def test(self, tmp_path, monkeypatch):
        """Test nominal behavior."""
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("no_run"))
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        _filter_countries(
            input=file_in,
            output=file_out,
        )

    def test_error_awk(self, tmp_path, monkeypatch):
        """Test error raising when `awk` fails."""
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("failure:awk"))
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        with pytest.raises(RuntimeError):
            _filter_countries(
                input=file_in,
                output=file_out,
            )


class TestCleanColumns:
    def test(self, tmp_path, tmp_indices, monkeypatch):
        """Test nominal behavior."""
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("no_run"))
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        result = _clean_columns(
            input=file_in,
            output=file_out,
            indices=tmp_indices,
            expected_ncol=None,
        )
        assert result

    def test_error_cut(self, tmp_path, tmp_indices, monkeypatch):
        """Test error raising when the normal stream's `cut` call fails."""
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("failure:cut"))
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        with pytest.raises(RuntimeError):
            _clean_columns(
                input=file_in,
                output=file_out,
                indices=tmp_indices,
                expected_ncol=None,
            )

    def test_expected_ncol(self, tmp_path, tmp_indices, monkeypatch):
        """Test behavior when `expected_ncol` matches actual ncol."""
        monkeypatch.setattr(
            subprocess,
            "run",
            patch_subprocess_run(f"count:{len(tmp_indices)}"),  # noqa: E231
        )
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        result = _clean_columns(
            input=file_in,
            output=file_out,
            indices=tmp_indices,
            expected_ncol=len(tmp_indices),
        )
        assert result

    def test_error_awk_counting(self, tmp_path, tmp_indices, monkeypatch):
        """Test error raising when the counting command fails."""
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("failure:awk"))
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        with pytest.raises(RuntimeError):
            _clean_columns(
                input=file_in,
                output=file_out,
                indices=tmp_indices,
                expected_ncol=len(tmp_indices),
            )

    def test_wrong_expected_ncol(self, tmp_path, tmp_indices, monkeypatch):
        """Test when providing a wrong `expected_ncol`."""
        monkeypatch.setattr(
            subprocess,
            "run",
            patch_subprocess_run(f"count:{len(tmp_indices)}"),  # noqa: E231
        )
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        result = _clean_columns(
            input=file_in,
            output=file_out,
            indices=tmp_indices,
            expected_ncol=len(tmp_indices) + 1,  # wrong expected_ncol
        )
        assert not result


class TestStationsToParquet:
    def test(self, tmp_path, tmp_station_df, monkeypatch):
        """Single test for this function."""
        file_in = tmp_path / "input.txt"
        file_out = tmp_path / "output.parquet"
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_metadata.load_ghcnd_stations",
            lambda: tmp_station_df,
        )
        monkeypatch.setattr(
            paths,
            "ghcnd_stations",
            lambda ext="parquet": dict(txt=file_in, parquet=file_out)[ext],
        )
        _stations_to_parquet()


class TestInventoryToParquet:
    def test(self, tmp_path, tmp_inventory_df, monkeypatch):
        """Single test for this function."""
        file_in = tmp_path / "input.txt"
        file_out = tmp_path / "output.parquet"
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_metadata.load_ghcnd_inventory",
            lambda: tmp_inventory_df,
        )
        monkeypatch.setattr(
            paths,
            "ghcnd_inventory",
            lambda ext="parquet": dict(txt=file_in, parquet=file_out)[ext],
        )
        _inventory_to_parquet()


class TestPreprocessGHCNdMetadata:
    def patch_paths(self, path, patch):
        """Patch GHCNd paths."""
        patch.setattr(paths, "ghcnd", lambda: path)
        patch.setattr(
            paths,
            "ghcnd_stations",
            lambda ext="parquet": path / f"ghcnd-stations.{ext}",
        )
        patch.setattr(
            paths,
            "ghcnd_inventory",
            lambda ext="parquet": path / f"ghcnd-inventory.{ext}",
        )

    def patch_no_run(self):
        """Patch function doing nothing."""

        def patch(*args, **kwargs):
            pass

        return patch

    def patch_return(self, result):
        """Patch function returning `result`."""

        def patch(*args, **kwargs):
            return result

        return patch

    def test(self, tmp_path, monkeypatch):
        """Test nominal behavior."""
        self.patch_paths(tmp_path, monkeypatch)
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_metadata._filter_countries",
            self.patch_no_run(),
        )
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_metadata._clean_columns",
            self.patch_return(True),
        )
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_metadata._stations_to_parquet",
            self.patch_no_run(),
        )
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_metadata._inventory_to_parquet",
            self.patch_no_run(),
        )
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("no_run"))
        preprocess_ghcnd_metadata()

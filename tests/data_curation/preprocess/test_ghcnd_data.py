import logging
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from dask import delayed

from pyseasters.constants import paths
from pyseasters.data_curation.preprocess.ghcnd_data import (
    _clean_columns,
    _preprocess_single_station,
    _single_station_to_parquet,
    preprocess_ghcnd_data,
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
        for command in {"csvcut", "wc", "mv", "rm"}:
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
def tmp_names():
    """Fixture to return a list of names."""
    return ["LATITUDE", "LONGITUDE"]


@pytest.fixture
def tmp_log():
    """Fixture to return a Logger."""
    return logging.getLogger()


@pytest.fixture
def tmp_df():
    """Fixture to return a realistic DataFrame."""
    return pd.DataFrame(
        {
            "DATE": ["2025-01-01", "2025-01-02"],
            "LATITUDE": 2 * [10.0],
            "LONGITUDE": 2 * [100.0],
            "PRCP": [1.0, 2.0],
        }
    )


class TestCleanColumns:
    def test(self, tmp_path, tmp_indices, tmp_names, tmp_log, monkeypatch):
        """Test nominal behavior."""
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("no_run"))
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        _clean_columns(
            input=file_in,
            output=file_out,
            indices=tmp_indices,
            names=tmp_names,
            logger=tmp_log,
            expected_ncol=None,
        )

    def test_error_csvcut(self, tmp_path, tmp_indices, tmp_names, tmp_log, monkeypatch):
        """Test error raising when the normal stream's `csvcut` call fails."""
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("failure:csvcut"))
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        with pytest.raises(RuntimeError):
            _clean_columns(
                input=file_in,
                output=file_out,
                indices=tmp_indices,
                names=tmp_names,
                logger=tmp_log,
                expected_ncol=None,
            )

    def test_expected_ncol(
        self, tmp_path, tmp_indices, tmp_names, tmp_log, monkeypatch
    ):
        """Test behavior when `expected_ncol` matches actual ncol."""
        monkeypatch.setattr(
            subprocess,
            "run",
            patch_subprocess_run(f"count:{len(tmp_indices)}"),  # noqa: E231
        )
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        _clean_columns(
            input=file_in,
            output=file_out,
            indices=tmp_indices,
            names=tmp_names,
            logger=tmp_log,
            expected_ncol=len(tmp_indices),
        )

    def test_error_csvcut_wc_counting(
        self, tmp_path, tmp_indices, tmp_names, tmp_log, monkeypatch
    ):
        """Test error raising when the counting command fails."""
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("failure:csvcut"))
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.csv"
        with pytest.raises(RuntimeError):
            _clean_columns(
                input=file_in,
                output=file_out,
                indices=tmp_indices,
                names=tmp_names,
                logger=tmp_log,
                expected_ncol=len(tmp_indices),
            )

    def test_manual_cleaning(
        self, tmp_path, tmp_df, tmp_indices, tmp_names, tmp_log, monkeypatch
    ):
        """
        Test manual cleaning, i.e., when `expected_ncol` does not matches actual ncol.
        """
        monkeypatch.setattr(
            subprocess,
            "run",
            patch_subprocess_run(f"count:{len(tmp_indices)}"),  # noqa: E231
        )
        file_in = tmp_path / "input.csv"
        tmp_df.to_csv(file_in, index=False)
        file_out = tmp_path / "output.csv"
        _clean_columns(
            input=file_in,
            output=file_out,
            indices=tmp_indices,
            names=tmp_names,
            logger=tmp_log,
            expected_ncol=len(tmp_indices)
            + 1,  # wrong expected_ncol to trigger mannual cleaning
        )


class TestSingleStationToParquet:
    def test(self, tmp_path, tmp_sid, tmp_df, tmp_log, monkeypatch):
        """Single test for this function."""
        file_in = tmp_path / "input.csv"
        file_out = tmp_path / "output.parquet"
        monkeypatch.setattr(
            paths,
            "ghcnd_file",
            lambda sid=tmp_sid, ext="parquet": dict(csv=file_in, parquet=file_out)[ext],
        )
        _single_station_to_parquet(station_id=tmp_sid, logger=tmp_log)


class TestPreprocessSingleStation:
    def patch_paths(self, path, sid, patch):
        """Patch GHCNd paths."""
        patch.setattr(paths, "ghcnd", lambda: path)
        patch.setattr(paths, "ghcnd_file", lambda s, ext="csv": path / f"{s}.{ext}")
        (path / f"{sid}.csv").write_text("A,B,C\n1,2,3\n")

    def patch_failure(self, error):
        """Path function raising an error."""

        def patch(*args, **kwargs):
            raise error

        return patch

    def patch_no_run(self):
        """Patch function doing nothing."""

        def patch(*args, **kwargs):
            pass

        return patch

    def patch_subprocess_dual(self):
        """Patch `subprocess.run` to do nothing on `mv` but fail on `rm`."""

        def patch(*args, **kwargs):
            if "mv" == args[0][:2]:
                pass
            elif "rm" == args[0][:2]:
                raise RuntimeError

        return patch

    def test_error_file_not_found(self, tmp_sid, monkeypatch):
        """Test error logging when file is non existent."""
        monkeypatch.setattr(
            paths, "ghcnd_file", lambda *_args, **_kwargs: Path("/nonexistent/path.csv")
        )
        name, messages = _preprocess_single_station(
            station_id=tmp_sid, expected_ncol=10
        ).compute()
        assert (
            (len(messages[-2]) == 3)
            and ("error" == messages[-2][0])
            and ("File %s not found" in messages[-2][1])
        )
        assert (
            (len(messages[-1]) == 3)
            and ("error" == messages[-1][0])
            and ("Abort task for station %s." in messages[-1][1])
        )

    def test_error_clean_columns_fails(self, tmp_path, tmp_sid, monkeypatch):
        """Test error logging when `_clean_columns()` fails."""
        self.patch_paths(tmp_path, tmp_sid, monkeypatch)
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_data._clean_columns",
            self.patch_failure(RuntimeError),
        )
        name, messages = _preprocess_single_station(
            station_id=tmp_sid, expected_ncol=10
        ).compute()
        assert (
            (len(messages[-2]) == 3)
            and ("error" == messages[-2][0])
            and ("Problem while cleaning columns: %s" in messages[-2][1])
        )
        assert (
            (len(messages[-1]) == 3)
            and ("error" == messages[-1][0])
            and ("Abort task for station %s." in messages[-1][1])
        )

    def test_error_clean_columns_mv_fails(self, tmp_path, tmp_sid, monkeypatch):
        """Test error logging when the `mv` after `_clean_columns()` fails."""
        self.patch_paths(tmp_path, tmp_sid, monkeypatch)
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_data._clean_columns",
            self.patch_no_run(),
        )
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("failure:mv"))
        name, messages = _preprocess_single_station(
            station_id=tmp_sid, expected_ncol=10
        ).compute()
        assert (
            (len(messages[-2]) == 3)
            and ("error" == messages[-2][0])
            and ("Problem while cleaning columns: %s" in messages[-2][1])
        )
        assert (
            (len(messages[-1]) == 3)
            and ("error" == messages[-1][0])
            and ("Abort task for station %s." in messages[-1][1])
        )

    def test_error_to_parquet_fails(self, tmp_path, tmp_sid, monkeypatch):
        """Test error logging when `_single_station_to_parquet()` fails."""
        self.patch_paths(tmp_path, tmp_sid, monkeypatch)
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_data._clean_columns",
            self.patch_no_run(),
        )
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("no_run"))
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_data._single_station_to_parquet",
            self.patch_failure(RuntimeError),
        )
        name, messages = _preprocess_single_station(
            station_id=tmp_sid, expected_ncol=10
        ).compute()
        assert (
            (len(messages[-2]) == 3)
            and ("error" == messages[-2][0])
            and ("Problem while converting to parquet: %s" in messages[-2][1])
        )
        assert (
            (len(messages[-1]) == 3)
            and ("error" == messages[-1][0])
            and ("Abort task for station %s." in messages[-1][1])
        )

    def test_error_to_parquet_rm_fails(self, tmp_path, tmp_sid, monkeypatch):
        """Test error logging when the `rm` after `_single_station_to_parquet()` fails."""
        self.patch_paths(tmp_path, tmp_sid, monkeypatch)
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_data._clean_columns",
            self.patch_no_run(),
        )
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_data._single_station_to_parquet",
            self.patch_no_run(),
        )
        monkeypatch.setattr(subprocess, "run", self.patch_subprocess_dual())
        name, messages = _preprocess_single_station(
            station_id=tmp_sid, expected_ncol=10
        ).compute()
        assert (
            (len(messages[-2]) == 3)
            and ("error" == messages[-2][0])
            and ("Problem while converting to parquet: %s" in messages[-2][1])
        )
        assert (
            (len(messages[-1]) == 3)
            and ("error" == messages[-1][0])
            and ("Abort task for station %s." in messages[-1][1])
        )

    def test_no_problem(self, tmp_path, tmp_sid, monkeypatch):
        """Test nominal behavior."""
        self.patch_paths(tmp_path, tmp_sid, monkeypatch)
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_data._clean_columns",
            self.patch_no_run(),
        )
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_data._single_station_to_parquet",
            self.patch_no_run(),
        )
        monkeypatch.setattr(subprocess, "run", patch_subprocess_run("no_run"))
        name, messages = _preprocess_single_station(
            station_id=tmp_sid, expected_ncol=10
        ).compute()
        assert (
            (len(messages[-1]) == 3)
            and ("info" == messages[-1][0])
            and ("Task completed for station %s" in messages[-1][1])
        )


class TestPreprocessGHCNdData:
    def test(self, monkeypatch):
        """Single test for this function."""
        # Patch inventory
        inventory = pd.DataFrame(
            {
                "station_id": ["ID001", "ID001", "ID002"],
                "var": ["PRCP", "TAVG", "PRCP"],
                "start": 3 * [2000],
                "end": 3 * [2010],
            }
        ).set_index(["station_id", "var"])
        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_data.load_ghcnd_inventory",
            lambda *a, **kw: inventory,
        )

        # Patch Dask processes
        monkeypatch.setattr(
            "dask.distributed.LocalCluster", lambda *a, **kw: Mock(close=lambda: None)
        )
        monkeypatch.setattr(
            "dask.distributed.Client", lambda *a, **kw: Mock(close=lambda: None)
        )
        monkeypatch.setattr(
            "dask.compute", lambda *tasks, **kw: [task.compute() for task in tasks]
        )

        # Patch _preprocess_single_station()
        @delayed
        def patch_task(*args, **kwargs):
            sid = args[0]
            return sid, [("info", "Task completed for station %s", sid)]

        monkeypatch.setattr(
            "pyseasters.data_curation.preprocess.ghcnd_data._preprocess_single_station",
            patch_task,
        )
        preprocess_ghcnd_data(ntasks=2)

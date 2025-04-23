import importlib.resources
import logging
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

import pyseasters.constants.pathconfig


def td(i: int = 1):
    """Test directory."""
    return f"/test/directory{i}"


def tm(i: int = 1):
    """Test machine."""
    return f"machine{i}"


def tn(i: int = 1):
    """Test network."""
    return f"network{i}"


def mock_pathsyaml(
    machine: bool = False,
    network: bool = False,
    sequence: bool = False,
    duplicate: bool = False,
):
    """Mock content of paths.yaml."""
    content = ""
    i = 0
    while i <= (1 if duplicate else 0):
        if machine:
            if sequence:
                content += f"{td(i + 1)}:\n  - machine:{tm(1)}\n  - machine:{tm(2)}\n"
            else:
                content += f"{td(i + 1)}: machine:{tm()}\n"
        if network:
            if sequence:
                content += f"{td(i + 3)}:\n  - network:{tn(1)}\n  - network:{tn(2)}\n"
            else:
                content += f"{td(i + 3)}: network:{tn()}\n"
        i += 1
    return content


@contextmanager
def secure_constants():
    original_machine_to_root = pyseasters.constants.pathconfig._MACHINE_TO_ROOT.copy()
    original_network_to_root = pyseasters.constants.pathconfig._NETWORK_TO_ROOT.copy()
    try:
        yield
    finally:
        pyseasters.constants.pathconfig._MACHINE_TO_ROOT.clear()
        pyseasters.constants.pathconfig._MACHINE_TO_ROOT.update(
            original_machine_to_root
        )
        pyseasters.constants.pathconfig._NETWORK_TO_ROOT.clear()
        pyseasters.constants.pathconfig._NETWORK_TO_ROOT.update(
            original_network_to_root
        )


def test_pathsyaml_machine():
    """Test behavior when a machine is found alone in paths.yaml."""

    with secure_constants(), patch("importlib.resources.files") as mock_files:
        mock_file = mock_open(read_data=mock_pathsyaml(machine=True))
        mock_files.return_value.joinpath.return_value.open = mock_file

        importlib.reload(pyseasters.constants.pathconfig)

        assert len(pyseasters.constants.pathconfig._MACHINE_TO_ROOT) == 1
        assert tm() in pyseasters.constants.pathconfig._MACHINE_TO_ROOT.keys()
        assert pyseasters.constants.pathconfig._MACHINE_TO_ROOT[tm()] == Path(td())


def test_pathsyaml_network():
    """Test behavior when a network is found alone in paths.yaml."""

    with secure_constants(), patch("importlib.resources.files") as mock_files:
        mock_file = mock_open(read_data=mock_pathsyaml(network=True))
        mock_files.return_value.joinpath.return_value.open = mock_file

        importlib.reload(pyseasters.constants.pathconfig)

        assert len(pyseasters.constants.pathconfig._NETWORK_TO_ROOT) == 1
        assert tn() in pyseasters.constants.pathconfig._NETWORK_TO_ROOT.keys()
        assert pyseasters.constants.pathconfig._NETWORK_TO_ROOT[tn()] == Path(td(3))


def test_pathsyaml_machine_list():
    """Test behavior when a machine is found in a list in paths.yaml."""

    with secure_constants(), patch("importlib.resources.files") as mock_files:
        mock_file = mock_open(read_data=mock_pathsyaml(machine=True, sequence=True))
        mock_files.return_value.joinpath.return_value.open = mock_file

        importlib.reload(pyseasters.constants.pathconfig)

        assert len(pyseasters.constants.pathconfig._MACHINE_TO_ROOT) == 2
        assert tm(1) in pyseasters.constants.pathconfig._MACHINE_TO_ROOT.keys()
        assert tm(2) in pyseasters.constants.pathconfig._MACHINE_TO_ROOT.keys()
        assert pyseasters.constants.pathconfig._MACHINE_TO_ROOT[tm(1)] == Path(td())
        assert pyseasters.constants.pathconfig._MACHINE_TO_ROOT[tm(2)] == Path(td())


def test_pathsyaml_network_list():
    """Test behavior when a network is found in a list in paths.yaml."""

    with secure_constants(), patch("importlib.resources.files") as mock_files:
        mock_file = mock_open(read_data=mock_pathsyaml(network=True, sequence=True))
        mock_files.return_value.joinpath.return_value.open = mock_file

        importlib.reload(pyseasters.constants.pathconfig)

        assert len(pyseasters.constants.pathconfig._NETWORK_TO_ROOT) == 2
        assert tn(1) in pyseasters.constants.pathconfig._NETWORK_TO_ROOT.keys()
        assert tn(2) in pyseasters.constants.pathconfig._NETWORK_TO_ROOT.keys()
        assert pyseasters.constants.pathconfig._NETWORK_TO_ROOT[tn(1)] == Path(td(3))
        assert pyseasters.constants.pathconfig._NETWORK_TO_ROOT[tn(2)] == Path(td(3))


def test_error_pathsyaml_machine_duplicate():
    """Test error raising when duplicate machines are found alone in paths.yaml."""

    with secure_constants(), patch("importlib.resources.files") as mock_files:
        mock_file = mock_open(read_data=mock_pathsyaml(machine=True, duplicate=True))
        mock_files.return_value.joinpath.return_value.open = mock_file

        with pytest.raises(RuntimeError):
            importlib.reload(pyseasters.constants.pathconfig)


def test_error_pathsyaml_network_duplicate():
    """Test error raising when duplicate networks are found alone in paths.yaml."""

    with secure_constants(), patch("importlib.resources.files") as mock_files:
        mock_file = mock_open(read_data=mock_pathsyaml(network=True, duplicate=True))
        mock_files.return_value.joinpath.return_value.open = mock_file

        with pytest.raises(RuntimeError):
            importlib.reload(pyseasters.constants.pathconfig)


def test_error_pathsyaml_machine_list_duplicate():
    """Test error raising when duplicate machines are found in lists in paths.yaml."""

    with secure_constants(), patch("importlib.resources.files") as mock_files:
        mock_file = mock_open(
            read_data=mock_pathsyaml(machine=True, sequence=True, duplicate=True)
        )
        mock_files.return_value.joinpath.return_value.open = mock_file

        with pytest.raises(RuntimeError):
            importlib.reload(pyseasters.constants.pathconfig)


def test_error_pathsyaml_network_list_duplicate():
    """Test error raising when duplicate networks are found in lists in paths.yaml."""

    with secure_constants(), patch("importlib.resources.files") as mock_files:
        mock_file = mock_open(
            read_data=mock_pathsyaml(network=True, sequence=True, duplicate=True)
        )
        mock_files.return_value.joinpath.return_value.open = mock_file

        with pytest.raises(RuntimeError):
            importlib.reload(pyseasters.constants.pathconfig)


def test_error_missing_pathsyaml(caplog):
    """Test behavior when paths.yaml is missing."""

    with secure_constants(), patch(
        "importlib.resources.files", side_effect=FileNotFoundError
    ):
        # Reload the module to trigger the import
        with caplog.at_level(logging.WARNING):
            importlib.reload(pyseasters.constants.pathconfig)

        # Check if warning was logged
        assert (
            "No path data found. Have you forgotten to download paths.yaml?"
            in caplog.text
        )


class TestPathConfig:
    def init(self):
        return pyseasters.constants.pathconfig.PathConfig()

    def test_init_known_machine(self, monkeypatch):
        """Test initialization with a known machine."""
        with secure_constants():
            pyseasters.constants.pathconfig._MACHINE_TO_ROOT.update({tm(): td()})
            monkeypatch.setattr(
                "pyseasters.constants.pathconfig._CURRENT_MACHINE", tm()
            )
            paths = self.init()
            assert paths.root == pyseasters.constants.pathconfig._MACHINE_TO_ROOT[tm()]
            assert paths.is_operational()

    def test_init_known_network(self, monkeypatch):
        """Test initialization with known machine and network."""
        with secure_constants():
            pyseasters.constants.pathconfig._NETWORK_TO_ROOT.update({tn(): td()})
            monkeypatch.setattr(
                "pyseasters.constants.pathconfig._CURRENT_MACHINE", tm()
            )
            monkeypatch.setattr(
                "pyseasters.constants.pathconfig._CURRENT_NETWORK", tn()
            )
            paths = self.init()
            assert paths.root == pyseasters.constants.pathconfig._NETWORK_TO_ROOT[tn()]
            assert paths.is_operational()

    def test_init_known_machine_and_network(self, monkeypatch):
        """Test initialization with known machine and network."""
        with secure_constants():
            pyseasters.constants.pathconfig._MACHINE_TO_ROOT.update({tm(): td(1)})
            pyseasters.constants.pathconfig._NETWORK_TO_ROOT.update({tn(): td(2)})
            monkeypatch.setattr(
                "pyseasters.constants.pathconfig._CURRENT_MACHINE", tm()
            )
            monkeypatch.setattr(
                "pyseasters.constants.pathconfig._CURRENT_NETWORK", tn()
            )
            paths = self.init()
            assert paths.root == pyseasters.constants.pathconfig._MACHINE_TO_ROOT[tm()]
            assert paths.is_operational()

    def test_init_unknown(self, monkeypatch):
        """Test initializing on an unknown machine and network."""
        monkeypatch.setattr("pyseasters.constants.pathconfig._CURRENT_MACHINE", tm())
        monkeypatch.setattr("pyseasters.constants.pathconfig._CURRENT_NETWORK", tn())
        paths = self.init()
        assert paths.root == paths._dummy_root
        assert not paths.is_operational()

    def test_manual_config(self, tmp_path):
        """Test self.manual_config()."""
        paths = self.init()
        paths.manual_config(tmp_path)
        assert paths.root == Path(tmp_path).resolve()
        assert paths.is_operational()

    def test_error_manual_config_unknown(self):
        """Test error raising for non existing manual_config() root path."""
        paths = self.init()
        non_existent_path = Path(tempfile.gettempdir()) / "does_not_exist"
        with pytest.raises(FileNotFoundError):
            paths.manual_config(str(non_existent_path))

    def test_manual_config_known_machine(self, tmp_path, monkeypatch):
        """Test self.manual_config() when machine is known."""
        with secure_constants():
            pyseasters.constants.pathconfig._MACHINE_TO_ROOT.update({tm(): td()})
            monkeypatch.setattr(
                "pyseasters.constants.pathconfig._CURRENT_MACHINE", tm()
            )
            paths = self.init()
            paths.manual_config(tmp_path)
            assert paths.root == Path(tmp_path).resolve()
            assert paths.is_operational()

    def test_manual_config_known_network(self, tmp_path, monkeypatch):
        """Test self.manual_config() when network is known."""
        with secure_constants():
            pyseasters.constants.pathconfig._NETWORK_TO_ROOT.update({tn(): td()})
            monkeypatch.setattr(
                "pyseasters.constants.pathconfig._CURRENT_MACHINE", tm()
            )
            monkeypatch.setattr(
                "pyseasters.constants.pathconfig._CURRENT_NETWORK", tn()
            )
            paths = self.init()
            paths.manual_config(tmp_path)
            assert paths.root == Path(tmp_path).resolve()
            assert paths.is_operational()

    def test_various_paths(self, tmp_path):
        paths = self.init()
        paths.manual_config(tmp_path)
        assert paths.ghcnd() == tmp_path / "GHCNd"
        assert (
            paths.ghcnd_stations() == tmp_path / "GHCNd/metadata/ghcnd-stations.parquet"
        )
        assert (
            paths.ghcnd_inventory("csv")
            == tmp_path / "GHCNd/metadata/ghcnd-inventory.csv"
        )
        assert paths.ghcnd_file("ABC123", "csv") == tmp_path / "GHCNd/data/ABC123.csv"

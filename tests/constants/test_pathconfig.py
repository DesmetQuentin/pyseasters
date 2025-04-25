import logging
import socket
import subprocess
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

import pyseasters.constants.pathconfig as module


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
                content += f"{td(i + 1)}:\n  - machine:{tm(1)}\n  - machine:{tm(2)}\n"  # noqa: E221, E231
            else:
                content += f"{td(i + 1)}: machine:{tm()}\n"  # noqa: E231
        if network:
            if sequence:
                content += f"{td(i + 3)}:\n  - network:{tn(1)}\n  - network:{tn(2)}\n"  # noqa: E221, E231
            else:
                content += f"{td(i + 3)}: network:{tn()}\n"  # noqa: E231
        i += 1
    return content


class TestParsePathsyaml:
    def test_pathsyaml_machine(self):
        """Test when a machine is found alone in paths.yaml."""
        with patch("importlib.resources.files") as mock_files:
            mock_file = mock_open(read_data=mock_pathsyaml(machine=True))
            mock_files.return_value.joinpath.return_value.open = mock_file
            machine_to_root = module._parse_pathsyaml()[0]
            assert len(machine_to_root) == 1
            assert tm() in machine_to_root.keys()
            assert machine_to_root[tm()] == Path(td())

    def test_pathsyaml_network(self):
        """Test when a network is found alone in paths.yaml."""
        with patch("importlib.resources.files") as mock_files:
            mock_file = mock_open(read_data=mock_pathsyaml(network=True))
            mock_files.return_value.joinpath.return_value.open = mock_file
            network_to_root = module._parse_pathsyaml()[1]
            assert len(network_to_root) == 1
            assert tn() in network_to_root.keys()
            assert network_to_root[tn()] == Path(td(3))

    def test_pathsyaml_machine_list(self):
        """Test when a machine is found in a list in paths.yaml."""
        with patch("importlib.resources.files") as mock_files:
            mock_file = mock_open(read_data=mock_pathsyaml(machine=True, sequence=True))
            mock_files.return_value.joinpath.return_value.open = mock_file
            machine_to_root = module._parse_pathsyaml()[0]
            assert len(machine_to_root) == 2
            assert tm(1) in machine_to_root.keys()
            assert tm(2) in machine_to_root.keys()
            assert machine_to_root[tm(1)] == Path(td())
            assert machine_to_root[tm(2)] == Path(td())

    def test_pathsyaml_network_list(self):
        """Test when a network is found in a list in paths.yaml."""
        with patch("importlib.resources.files") as mock_files:
            mock_file = mock_open(read_data=mock_pathsyaml(network=True, sequence=True))
            mock_files.return_value.joinpath.return_value.open = mock_file
            network_to_root = module._parse_pathsyaml()[1]
            assert len(network_to_root) == 2
            assert tn(1) in network_to_root.keys()
            assert tn(2) in network_to_root.keys()
            assert network_to_root[tn(1)] == Path(td(3))
            assert network_to_root[tn(2)] == Path(td(3))

    def test_error_pathsyaml_machine_duplicate(self):
        """Test error raising when duplicate machines are found alone in paths.yaml."""
        with patch("importlib.resources.files") as mock_files:
            mock_file = mock_open(
                read_data=mock_pathsyaml(machine=True, duplicate=True)
            )
            mock_files.return_value.joinpath.return_value.open = mock_file
            with pytest.raises(RuntimeError):
                module._parse_pathsyaml()

    def test_error_pathsyaml_network_duplicate(self):
        """Test error raising when duplicate networks are found alone in paths.yaml."""
        with patch("importlib.resources.files") as mock_files:
            mock_file = mock_open(
                read_data=mock_pathsyaml(network=True, duplicate=True)
            )
            mock_files.return_value.joinpath.return_value.open = mock_file
            with pytest.raises(RuntimeError):
                module._parse_pathsyaml()

    def test_error_pathsyaml_machine_list_duplicate(self):
        """Test error raising when duplicate machines are found in lists in paths.yaml."""
        with patch("importlib.resources.files") as mock_files:
            mock_file = mock_open(
                read_data=mock_pathsyaml(machine=True, sequence=True, duplicate=True)
            )
            mock_files.return_value.joinpath.return_value.open = mock_file
            with pytest.raises(RuntimeError):
                module._parse_pathsyaml()

    def test_error_pathsyaml_network_list_duplicate(self):
        """Test error raising when duplicate networks are found in lists in paths.yaml."""
        with patch("importlib.resources.files") as mock_files:
            mock_file = mock_open(
                read_data=mock_pathsyaml(network=True, sequence=True, duplicate=True)
            )
            mock_files.return_value.joinpath.return_value.open = mock_file
            with pytest.raises(RuntimeError):
                module._parse_pathsyaml()

    def test_error_missing_pathsyaml(self, caplog):
        """Test when paths.yaml is missing."""
        with patch("importlib.resources.files", side_effect=FileNotFoundError):
            # Reload the module to trigger the import
            with caplog.at_level(logging.WARNING):
                module._parse_pathsyaml()

            # Check if warning was logged
            assert (
                "No path data found. Have you forgotten to download paths.yaml?"
                in caplog.text
            )


class TestPathConfig:
    def patch_machine_network(
        self, mp, machine="", network="", machine_to_root={}, network_to_root={}
    ):
        """Patch machine and network information sources for PathConfig objects."""
        mp.setattr(socket, "gethostname", lambda: machine)
        mp.setattr(
            subprocess,
            "run",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args, 0, stdout=f"{network}\n"
            ),
        )
        mp.setattr(
            "pyseasters.constants.pathconfig._parse_pathsyaml",
            lambda: (machine_to_root, network_to_root),
        )

    def test_auto_init_known_machine(self, tmp_path, monkeypatch):
        """Test auto initialization with a known machine."""
        self.patch_machine_network(
            monkeypatch, machine=tm(), machine_to_root={tm(): tmp_path}
        )
        paths = module.PathConfig(init_mode="auto")
        assert paths.root == paths._machine_to_root[paths._current_machine]
        assert paths.is_operational()

    def test_auto_init_known_machine_wrong_path(self, tmp_path, monkeypatch):
        """Test auto initialization with a known machine."""
        self.patch_machine_network(
            monkeypatch, machine=tm(), machine_to_root={tm(): tmp_path / "dir1"}
        )
        paths = module.PathConfig(init_mode="auto")
        assert not paths.is_operational()

    def test_auto_init_known_network(self, tmp_path, monkeypatch):
        """Test auto initialization with known machine and network."""
        self.patch_machine_network(
            monkeypatch, machine=tm(), network=tn(), network_to_root={tn(): tmp_path}
        )
        paths = module.PathConfig(init_mode="auto")
        assert paths.root == paths._network_to_root[paths._current_network]
        assert paths.is_operational()

    def test_auto_init_known_network_wrong_path(self, tmp_path, monkeypatch):
        """Test auto initialization with known machine and network."""
        self.patch_machine_network(
            monkeypatch,
            machine=tm(),
            network=tn(),
            network_to_root={tn(): tmp_path / "dir1"},
        )
        paths = module.PathConfig(init_mode="auto")
        assert not paths.is_operational()

    def test_auto_init_known_machine_and_network(self, tmp_path, monkeypatch):
        """Test auto initialization with known machine and network."""
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()
        self.patch_machine_network(
            monkeypatch,
            machine=tm(),
            network=tn(),
            machine_to_root={tm(): dir1},
            network_to_root={tn(): dir2},
        )
        paths = module.PathConfig(init_mode="auto")
        assert paths.root == paths._machine_to_root[paths._current_machine]
        assert paths.root != paths._network_to_root[paths._current_network]
        assert paths.is_operational()

    def test_auto_init_unknown(self, tmp_path, monkeypatch):
        """Test auto initializing on an unknown machine and network."""
        self.patch_machine_network(
            monkeypatch,
            machine=tm(),
            network=tn(),
            machine_to_root={tm(2): tmp_path},
            network_to_root={tn(2): tmp_path},
        )
        paths = module.PathConfig(init_mode="auto")
        assert paths.root == paths._dummy_root
        assert not paths.is_operational()

    def test_preconfig(self, tmp_path):
        """Test nominal initialization in preconfig mode."""
        with patch("importlib.resources.files") as mock_files:
            mock_file = mock_open(read_data=str(tmp_path))
            mock_files.return_value.joinpath.return_value.open = mock_file
            paths = module.PathConfig(init_mode="preconfig")
            assert paths.root == tmp_path
            assert paths.is_operational()

    def test_preconfig_non_existent_root_path(self, tmp_path):
        """Test initialization in preconfig mode when the root path does not exist."""
        with patch("importlib.resources.files") as mock_files:
            mock_file = mock_open(read_data=str(tmp_path / "dir1"))
            mock_files.return_value.joinpath.return_value.open = mock_file
            paths = module.PathConfig(init_mode="preconfig")
            assert not paths.is_operational()

    def test_preconfig_non_existent_pathtxt(self, tmp_path):
        """Test initialization in preconfig mode when the root path does not exist."""
        with patch("importlib.resources.files", side_effect=FileNotFoundError):
            paths = module.PathConfig(init_mode="preconfig")
            assert not paths.is_operational()

    def test_manual_config(self, tmp_path):
        """Test ``self.manual_config()``."""
        paths = module.PathConfig(init_mode=f"manual:{tmp_path}")  # noqa: E231
        assert paths.root == tmp_path
        assert paths.is_operational()

    def test_error_manual_config_unknown(self):
        """Test error raising for non existing ``manual_config()`` root path."""
        with pytest.raises(FileNotFoundError):
            module.PathConfig(init_mode="manual:/dummy/path")

    def test_manual_config_known_machine(self, tmp_path, monkeypatch):
        """Test PathConfig.manual_config() when machine is known."""
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()
        self.patch_machine_network(
            monkeypatch,
            machine=tm(),
            machine_to_root={tm(): dir1},
        )
        paths = module.PathConfig(init_mode="auto")
        assert paths.root == paths._machine_to_root[paths._current_machine]
        assert paths.is_operational()
        paths.manual_config(dir2)
        assert paths.root != paths._machine_to_root[paths._current_machine]
        assert paths.root == dir2
        assert paths.is_operational()

    def test_manual_config_known_network(self, tmp_path, monkeypatch):
        """Test PathConfig.manual_config() when network is known."""
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()
        self.patch_machine_network(
            monkeypatch,
            machine=tm(),
            network=tn(),
            network_to_root={tn(): dir1},
        )
        paths = module.PathConfig(init_mode="auto")
        assert paths.root == paths._network_to_root[paths._current_network]
        assert paths.is_operational()
        paths.manual_config(dir2)
        assert paths.root != paths._network_to_root[paths._current_network]
        assert paths.root == dir2
        assert paths.is_operational()

    def test_error_invalid_init_mode(self):
        """Test when providing an invalid ``init_mode``."""
        with pytest.raises(ValueError):
            module.PathConfig(init_mode="invalid mode")

    def test_various_paths(self, tmp_path):
        """Test simple path-returning single-line methods."""
        paths = module.PathConfig(init_mode="auto")
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

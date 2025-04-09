import importlib.resources
import logging
import shutil
import tempfile
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

import pyseasters.api.constants.paths


@pytest.fixture
def config():
    """Fixture to create a fresh instance of PathConfig."""
    return pyseasters.api.constants.paths.PathConfig()


@pytest.fixture
def temp_dir():
    """Fixture to create and clean up a temporary directory."""
    dir_path = tempfile.mkdtemp()
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


def mock_paths_yaml_list():
    """Mock content of paths.yaml."""
    content = """
    path to machine:
        /test/directory: machine1
        /test/directory/two:
        - machine1
        - machine2
    """
    return content


def mock_paths_yaml_standalone():
    """Mock content of paths.yaml."""
    content = """
    path to machine:
        /test/directory:
        - machine1
        - machine2
        /test/directory/two: machine2
    """
    return content


def test_missing_paths_yaml(caplog):
    """Test behavior when paths.yaml is missing."""

    original_machine_to_path = pyseasters.api.constants.paths.MACHINE_TO_PATH.copy()

    with patch("importlib.resources.files", side_effect=FileNotFoundError):
        # Reload the module to trigger the import
        with caplog.at_level(logging.WARNING):
            importlib.reload(pyseasters.api.constants.config)

        # Check if warning was logged
        assert (
            "No path data found. Have you forgotten to download paths.yaml?"
            in caplog.text
        )

    pyseasters.api.constants.paths.MACHINE_TO_PATH.clear()
    pyseasters.api.constants.paths.MACHINE_TO_PATH.update(original_machine_to_path)


def test_paths_yaml_inconsistent_list():
    """Test behavior when paths.yaml is inconsistent: one machine in a list has already been recorded."""

    original_machine_to_path = pyseasters.api.constants.paths.MACHINE_TO_PATH.copy()

    with patch("importlib.resources.files") as mock_files:
        mock_file = mock_open(read_data=mock_paths_yaml_list())
        mock_files.return_value.joinpath.return_value.open = mock_file

        with pytest.raises(RuntimeError):
            importlib.reload(pyseasters.api.constants.config)

    pyseasters.api.constants.paths.MACHINE_TO_PATH.clear()
    pyseasters.api.constants.paths.MACHINE_TO_PATH.update(original_machine_to_path)


def test_paths_yaml_inconsistent_standalone():
    """Test behavior when paths.yaml is inconsistent: one standalone machine has already been recorded."""

    original_machine_to_path = pyseasters.api.constants.paths.MACHINE_TO_PATH.copy()

    with patch("importlib.resources.files") as mock_files:
        mock_file = mock_open(read_data=mock_paths_yaml_standalone())
        mock_files.return_value.joinpath.return_value.open = mock_file

        with pytest.raises(RuntimeError):
            importlib.reload(pyseasters.api.constants.config)

    pyseasters.api.constants.paths.MACHINE_TO_PATH.clear()
    pyseasters.api.constants.paths.MACHINE_TO_PATH.update(original_machine_to_path)


def test_initialize_with_known_machine(config, monkeypatch):
    """Test initialization with a known machine from MACHINE_TO_PATH."""
    original_machine_to_path = pyseasters.api.constants.paths.MACHINE_TO_PATH.copy()
    pyseasters.api.constants.paths.MACHINE_TO_PATH.update(
        {"KNOWN MACHINE": "/known/directory"}
    )
    monkeypatch.setattr(
        "pyseasters.api.constants.paths.CURRENT_MACHINE", "KNOWN MACHINE"
    )
    config.initialize()
    print(pyseasters.api.constants.paths.MACHINE_TO_PATH)
    assert (
        config.root == pyseasters.api.constants.paths.MACHINE_TO_PATH["KNOWN MACHINE"]
    )
    pyseasters.api.constants.paths.MACHINE_TO_PATH.clear()
    pyseasters.api.constants.paths.MACHINE_TO_PATH.update(original_machine_to_path)


def test_initialize_with_custom_path(config, temp_dir):
    """Test initialization with a valid custom directory."""
    config.initialize(temp_dir)
    assert config.root == Path(temp_dir).resolve()


def test_initialize_with_unknown_machine(config, monkeypatch):
    """Test that initializing on an unknown machine raises an error."""
    monkeypatch.setattr(
        "pyseasters.api.constants.paths.CURRENT_MACHINE", "unknown_machine"
    )
    with pytest.raises(RuntimeError):
        config.initialize()


def test_initialize_with_non_existent_path(config):
    """Test that initializing with a non-existent path raises an error."""
    non_existent_path = Path(tempfile.gettempdir()) / "does_not_exist"
    with pytest.raises(FileNotFoundError):
        config.initialize(str(non_existent_path))


def test_is_initialized(config, temp_dir):
    """Test the is_initialized() method."""
    assert not config.is_initialized()
    config.initialize(temp_dir)
    assert config.is_initialized()

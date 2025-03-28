import importlib.resources
import logging
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

import pyseasters.config


@pytest.fixture
def config():
    """Fixture to create a fresh instance of PathConfig."""
    return pyseasters.config.PathConfig()


@pytest.fixture
def temp_dir():
    """Fixture to create and clean up a temporary directory."""
    dir_path = tempfile.mkdtemp()
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


def test_missing_paths_yaml(caplog):
    """Test behavior when paths.yaml is missing."""

    original_machine_config = pyseasters.config.MACHINE_CONFIG.copy()

    with patch("importlib.resources.files", side_effect=FileNotFoundError):
        # Reload the module to trigger the import
        with caplog.at_level(logging.WARNING):
            importlib.reload(pyseasters.config)

        # Check if warning was logged
        assert (
            "No path data found. Have you forgotten to download paths.yaml?"
            in caplog.text
        )

    pyseasters.config.MACHINE_CONFIG.clear()
    pyseasters.config.MACHINE_CONFIG.update(original_machine_config)


def test_initialize_with_known_machine(config, monkeypatch):
    """Test initialization with a known machine from MACHINE_CONFIG."""
    original_machine_config = pyseasters.config.MACHINE_CONFIG.copy()
    pyseasters.config.MACHINE_CONFIG.update({"KNOWN MACHINE": "/known/directory"})
    monkeypatch.setattr("pyseasters.config.CURRENT_MACHINE", "KNOWN MACHINE")
    config.initialize()
    print(pyseasters.config.MACHINE_CONFIG)
    assert config.root == pyseasters.config.MACHINE_CONFIG["KNOWN MACHINE"]
    pyseasters.config.MACHINE_CONFIG.clear()
    pyseasters.config.MACHINE_CONFIG.update(original_machine_config)


def test_initialize_with_custom_path(config, temp_dir):
    """Test initialization with a valid custom directory."""
    config.initialize(temp_dir)
    assert config.root == Path(temp_dir).resolve()


def test_initialize_with_unknown_machine(config, monkeypatch):
    """Test that initializing on an unknown machine raises an error."""
    monkeypatch.setattr("pyseasters.config.CURRENT_MACHINE", "unknown_machine")
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

import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from pyseasters.config import CURRENT_MACHINE, MACHINE_CONFIG, PathConfig


@pytest.fixture
def config():
    """Fixture to create a fresh instance of PathConfig."""
    return PathConfig()


@pytest.fixture
def temp_dir():
    """Fixture to create and clean up a temporary directory."""
    dir_path = tempfile.mkdtemp()
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


def test_initialize_with_known_machine(config, monkeypatch):
    """Test initialization with a known machine from MACHINE_CONFIG."""
    monkeypatch.setattr("pyseasters.config.CURRENT_MACHINE", "LALL2423D3")
    config.initialize()
    assert config.root == MACHINE_CONFIG["LALL2423D3"]


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

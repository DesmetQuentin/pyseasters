import pytest

from pyseasters.data_curation.download._ghcnd_data import (
    _DATA,
    generate_ghcnd_data_download_script,
)


@pytest.fixture
def tmp_station_list():
    """Fixture to return a realistic station list."""
    return ["ABC123", "XYZ789"]


class TestGenerateGHCNdDataDownloadScript:
    def patch_station_list(self, stations, patch):
        """Patch `get_ghcnd_station_list()` to return a given station list."""
        patch.setattr(
            "pyseasters.data_curation.download._ghcnd_data.get_ghcnd_station_list",
            lambda: stations,
        )

    def test_return(self, tmp_station_list, monkeypatch):
        """Test return value."""
        self.patch_station_list(tmp_station_list, monkeypatch)
        script = generate_ghcnd_data_download_script()
        assert "#!/bin/bash" in script
        assert (
            f"for station in {' '.join(tmp_station_list)}; do" in script  # noqa: E702
        )
        assert f"wget {_DATA % ('${station}')}" in script
        assert "done" in script

    def test_output(self, tmp_station_list, tmp_path, monkeypatch):
        """Test when requesting writing in output file."""
        self.patch_station_list(tmp_station_list, monkeypatch)
        output_file = tmp_path / "download.sh"
        script = generate_ghcnd_data_download_script(output=output_file)
        with open(output_file) as f:
            content = f.read()
            assert content == script

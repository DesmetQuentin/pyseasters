import pytest  # noqa: F401

from pyseasters.data_curation.download._ghcnd_metadata import (
    _INVENTORY,
    _STATIONS,
    generate_ghcnd_metadata_download_script,
)


class TestGenerateGHCNdMetadataDownloadScript:
    def test_return(self):
        """Test return value."""
        script = generate_ghcnd_metadata_download_script()
        assert "#!/bin/bash" in script
        assert f"wget {_STATIONS}" in script
        assert f"wget {_INVENTORY}" in script

    def test_output(self, tmp_path):
        """Test when requesting writing in output file."""
        output_file = tmp_path / "download.sh"
        script = generate_ghcnd_metadata_download_script(output=output_file)
        with open(output_file) as f:
            content = f.read()
            assert content == script

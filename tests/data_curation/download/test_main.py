import pytest

import pyseasters.data_curation.download.main as module


class TestGenerateDownloadScript:
    def test_known_key(self, monkeypatch):
        """Test when using a known key."""
        for key in module._dispatcher.keys():
            expected = f"{key} script"
            monkeypatch.setitem(module._dispatcher, key, lambda output=None: expected)
            script = module.generate_download_script(key)
            assert script == expected

    def test_error_invalid_key(self):
        """Test error raising when providing an invalid key."""
        with pytest.raises(ValueError):
            module.generate_download_script("unknown key")

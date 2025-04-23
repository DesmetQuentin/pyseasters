import pytest

import pyseasters.utils._dependencies as module


class TestRequireTools:
    def test_tool_exists(self, monkeypatch):
        """Test when tool exists."""
        monkeypatch.setattr(module.shutil, "which", lambda tool: "/usr/bin/" + tool)

        @module.require_tools("echo")
        def dummy():
            return "ok"

        assert dummy() == "ok"

    def test_error_tool_missing(self, monkeypatch):
        """Test error raising when tool is missing."""
        monkeypatch.setattr(module.shutil, "which", lambda tool: None)

        @module.require_tools("fake_tool_123")
        def dummy():
            return "fail"

        with pytest.raises(RuntimeError):
            dummy()

    def test_checked_tools_cache(self, monkeypatch):
        """Test _checked_tools cache actually capture checked tools."""
        module._checked_tools.clear()
        monkeypatch.setattr(module.shutil, "which", lambda tool: f"/usr/bin/{tool}")

        @module.require_tools("tool1")
        def dummy1():
            return "ok1"

        assert dummy1() == "ok1"
        assert "tool1" in module._checked_tools

        @module.require_tools("tool1")
        def dummy2():
            return "ok2"

        assert dummy2() == "ok2"
        assert "tool1" in module._checked_tools

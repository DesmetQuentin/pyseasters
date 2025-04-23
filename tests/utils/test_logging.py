import importlib.resources
import logging

import pytest

import pyseasters.utils._logging as module


class TestSetupCLILogging:
    def test_message_redirection(self, capsys):
        """Test that logging messages are redirected to the correct stream."""
        importlib.reload(module)
        module.setup_cli_logging(logging.DEBUG)
        logger = logging.getLogger()
        logger.debug("debug message")
        logger.info("info message")
        logger.warning("warning message")
        logger.error("error message")
        logger.critical("critical message")

        out, err = capsys.readouterr()
        assert "debug message" in out
        assert "debug message" not in err
        assert "info message" in out
        assert "info message" not in err
        assert "warning message" in err
        assert "warning message" not in out
        assert "error message" in err
        assert "error message" not in out
        assert "critical message" in err
        assert "critical message" not in out

    def test_maximum_level(self):
        """Test maximum level is warning."""
        module.setup_cli_logging(logging.ERROR)
        logger = logging.getLogger()
        assert logger.level == logging.WARNING


class TestLoggingStack:
    def setup_stack(self):
        """Return a LoggingStack with some messages."""
        stack = module.LoggingStack("testjob")
        stack.info("message %s", "A")
        stack.warning("message B")
        return stack

    def test_collect_and_flush(self, capsys):
        """Test collecting messages then flushing to a logging Logger."""
        stack = self.setup_stack()
        assert len(stack.messages) == 2
        importlib.reload(module)
        module.setup_cli_logging(logging.DEBUG)
        logger = logging.getLogger()
        stack.flush(logger)

        out, err = capsys.readouterr()
        assert "[testjob] message A" in out
        assert "[testjob] message B" in err
        assert len(stack.messages) == 0

    def test_picklable_and_reconstruction(self):
        """Test pickling then reconstructing."""
        stack = self.setup_stack()
        data = stack.picklable()
        restored = module.LoggingStack(*data)
        assert restored.messages == stack.messages
        assert restored.name == stack.name

    def test_error_wrong_level(self):
        stack = module.LoggingStack("testjob")
        with pytest.raises(AttributeError):
            stack.notalevel("message")

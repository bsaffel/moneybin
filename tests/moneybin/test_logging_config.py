"""Tests for centralized logging configuration."""

import logging
import sys
from collections.abc import Generator
from typing import Any, cast

import pytest

from moneybin.logging.config import LoggingConfig, setup_logging


def _force_config(log_to_file: bool = False) -> LoggingConfig:
    """Return a LoggingConfig that forces handler replacement."""
    return LoggingConfig(log_to_file=log_to_file, force_reconfigure=True)


class TestSetupLogging:
    """Tests for setup_logging console handler configuration."""

    @pytest.fixture(autouse=True)
    def _reset_root_logger(self) -> Generator[None, Any, None]:
        """Remove handlers added during each test to avoid leaking state."""
        root = logging.getLogger()
        original_handlers = list(root.handlers)
        yield
        root.handlers = original_handlers

    @pytest.mark.unit
    def test_console_handler_uses_stderr(self) -> None:
        """Console handler must write to stderr, not stdout.

        MCP stdio transport uses stdout for JSON-RPC; any log output on
        stdout corrupts the protocol.  This test guards against regression.
        """
        setup_logging(config=_force_config(), cli_mode=False)
        root = logging.getLogger()

        stream_handlers = [
            h
            for h in root.handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        assert stream_handlers, "Expected at least one StreamHandler"
        for h in stream_handlers:
            stream: object = getattr(cast(Any, h), "stream", None)
            assert stream is sys.stderr

    @pytest.mark.unit
    def test_console_handler_uses_stderr_in_cli_mode(self) -> None:
        """CLI mode should also log to stderr."""
        setup_logging(config=_force_config(), cli_mode=True)
        root = logging.getLogger()

        stream_handlers = [
            h
            for h in root.handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        assert stream_handlers, "Expected at least one StreamHandler"
        for h in stream_handlers:
            stream: object = getattr(cast(Any, h), "stream", None)
            assert stream is sys.stderr

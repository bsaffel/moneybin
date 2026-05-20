"""Tests for the observability facade."""

import logging
from collections.abc import Generator
from typing import Any
from unittest.mock import patch

import pytest


class TestSetupObservability:
    """Tests for setup_observability()."""

    @pytest.fixture(autouse=True)
    def _reset_root_logger(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> Generator[None, Any, None]:
        """Clean up handlers and reset _initialized after each test."""
        import moneybin.observability as obs_mod

        monkeypatch.setattr(obs_mod, "_initialized", False)
        root = logging.getLogger()
        original_handlers = list(root.handlers)
        original_level = root.level
        yield
        for h in root.handlers[:]:
            if h not in original_handlers:
                h.close()
        root.handlers = original_handlers
        root.level = original_level

    @pytest.mark.unit
    def test_setup_calls_setup_logging(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """setup_observability should delegate to setup_logging."""
        with patch("moneybin.observability.setup_logging") as mock_log:
            from moneybin.observability import setup_observability

            setup_observability(stream="cli", verbose=True)
            mock_log.assert_called_once_with(stream="cli", verbose=True)

    @pytest.mark.unit
    def test_public_api_exports(self) -> None:
        """The observability module should export tracked and track_duration."""
        from moneybin.observability import setup_observability, track_duration, tracked

        assert callable(setup_observability)
        assert callable(tracked)
        assert callable(track_duration)

    @pytest.mark.unit
    def test_cli_stream_registers_atexit_flush(self) -> None:
        """CLI sessions register flush_metrics as an atexit hook.

        Commands don't explicitly close_db() before the process ends, so
        atexit runs while the database singleton is still attached.
        """
        with (
            patch("moneybin.observability.setup_logging"),
            patch("atexit.register") as mock_register,
        ):
            from moneybin.observability import flush_metrics, setup_observability

            setup_observability(stream="cli")
            mock_register.assert_called_once_with(flush_metrics)

    @pytest.mark.unit
    def test_mcp_stream_does_not_register_atexit(self) -> None:
        """MCP sessions must NOT register atexit.

        close_db() runs before atexit and would clear the database singleton,
        leaving flush_metrics nothing to write to. MCP shutdown calls
        flush_metrics explicitly in the mcp/server.py finally-block instead.
        """
        with (
            patch("moneybin.observability.setup_logging"),
            patch("atexit.register") as mock_register,
        ):
            from moneybin.observability import setup_observability

            setup_observability(stream="mcp")
            mock_register.assert_not_called()

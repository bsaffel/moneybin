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
        """Clean up handlers and reset module state after each test."""
        import moneybin.observability as obs_mod

        monkeypatch.setattr(obs_mod, "_atexit_registered", False)
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
        """CLI sessions register flush_metrics as an atexit hook."""
        from moneybin.observability import flush_metrics, setup_observability

        with (
            patch("moneybin.observability.setup_logging"),
            patch("atexit.register") as mock_register,
        ):
            setup_observability(stream="cli")
            mock_register.assert_called_once_with(flush_metrics)

    @pytest.mark.unit
    def test_mcp_stream_does_not_register_atexit(self) -> None:
        """MCP sessions skip atexit — flush happens inside close_db()."""
        from moneybin.observability import setup_observability

        with (
            patch("moneybin.observability.setup_logging"),
            patch("atexit.register") as mock_register,
        ):
            setup_observability(stream="mcp")
            mock_register.assert_not_called()

    @pytest.mark.unit
    def test_cli_after_mcp_still_registers_atexit(self) -> None:
        """Atexit gating is per-stream, not first-call-wins.

        If a process boots with stream="mcp" or "sqlmesh" first and later
        runs CLI work, the CLI atexit hook must still register — otherwise
        write-path CLI counters never reach app.metrics.
        """
        from moneybin.observability import flush_metrics, setup_observability

        with (
            patch("moneybin.observability.setup_logging"),
            patch("atexit.register") as mock_register,
        ):
            setup_observability(stream="mcp")
            setup_observability(stream="sqlmesh")
            setup_observability(stream="cli")
            mock_register.assert_called_once_with(flush_metrics)

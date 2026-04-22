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
        monkeypatch.setenv("MONEYBIN_LOGGING__LOG_TO_FILE", "false")
        with patch("moneybin.observability.setup_logging") as mock_log:
            from moneybin.observability import setup_observability

            setup_observability(stream="cli", verbose=True)
            mock_log.assert_called_once_with(stream="cli", verbose=True, profile=None)

    @pytest.mark.unit
    def test_setup_registers_atexit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """setup_observability should register an atexit handler for metrics flush."""
        monkeypatch.setenv("MONEYBIN_LOGGING__LOG_TO_FILE", "false")
        import moneybin.observability as obs_mod

        monkeypatch.setattr(obs_mod, "_initialized", False)
        with patch("moneybin.observability.atexit") as mock_atexit:
            obs_mod.setup_observability(stream="cli")
            mock_atexit.register.assert_called_once()

    @pytest.mark.unit
    def test_public_api_exports(self) -> None:
        """The observability module should export tracked and track_duration."""
        from moneybin.observability import setup_observability, track_duration, tracked

        assert callable(setup_observability)
        assert callable(tracked)
        assert callable(track_duration)

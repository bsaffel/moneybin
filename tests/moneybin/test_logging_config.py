"""Tests for centralized logging configuration."""

import logging
import sys
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pytest

from moneybin.logging.config import session_log_path, setup_logging


class TestSessionLogPath:
    """Tests for session_log_path() path structure."""

    @pytest.mark.unit
    def test_path_structure(self) -> None:
        """Path follows logs/{profile}/stream_YYYY-MM-DD.log format."""
        now = datetime(2025, 4, 11, 13, 57, 18)
        result = session_log_path(Path("logs/test/moneybin.log"), prefix="cli", now=now)
        assert result == Path("logs/test/cli_2025-04-11.log")

    @pytest.mark.unit
    def test_prefix_is_applied(self) -> None:
        """Custom prefix appears in the filename."""
        now = datetime(2025, 4, 11, 13, 57, 18)
        result = session_log_path(
            Path("logs/prod/moneybin.log"), prefix="sqlmesh", now=now
        )
        assert result == Path("logs/prod/sqlmesh_2025-04-11.log")


class TestSetupLogging:
    """Tests for setup_logging handler configuration."""

    @pytest.fixture(autouse=True)
    def _reset_root_logger(self) -> Generator[None, Any, None]:
        """Remove handlers added during each test to avoid leaking state."""
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
    def test_console_handler_uses_stderr(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Console handler must write to stderr, not stdout."""
        monkeypatch.setenv("MONEYBIN_LOGGING__LOG_TO_FILE", "false")
        monkeypatch.setattr("moneybin.config._current_settings", None)
        setup_logging(stream="cli")
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
    def test_console_handler_uses_stderr_for_mcp(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """MCP stream should also log to stderr."""
        monkeypatch.setenv("MONEYBIN_LOGGING__LOG_TO_FILE", "false")
        monkeypatch.setattr("moneybin.config._current_settings", None)
        setup_logging(stream="mcp")
        root = logging.getLogger()

        stream_handlers = [
            h
            for h in root.handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        assert stream_handlers
        for h in stream_handlers:
            stream: object = getattr(cast(Any, h), "stream", None)
            assert stream is sys.stderr

    @pytest.mark.unit
    def test_verbose_sets_debug_level(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verbose flag should set root logger to DEBUG."""
        monkeypatch.setenv("MONEYBIN_LOGGING__LOG_TO_FILE", "false")
        monkeypatch.setattr("moneybin.config._current_settings", None)
        setup_logging(stream="cli", verbose=True)
        assert logging.getLogger().level == logging.DEBUG

    @pytest.mark.unit
    def test_file_handler_created_when_enabled(self, tmp_path: Path) -> None:
        """File handler should be created when log_to_file is enabled."""
        setup_logging(stream="cli", log_file_path=tmp_path / "moneybin.log")
        root = logging.getLogger()
        fhs = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
        assert fhs, "Expected at least one FileHandler"

    @pytest.mark.unit
    def test_file_handler_uses_sanitized_formatter(self, tmp_path: Path) -> None:
        """File handler must use SanitizedLogFormatter."""
        from moneybin.log_sanitizer import SanitizedLogFormatter

        setup_logging(stream="cli", log_file_path=tmp_path / "moneybin.log")
        root = logging.getLogger()
        fhs = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
        assert fhs
        assert isinstance(fhs[0].formatter, SanitizedLogFormatter)

    @pytest.mark.unit
    def test_file_handler_is_catch_all(self, tmp_path: Path) -> None:
        """File handler should accept records from any logger."""
        setup_logging(stream="cli", log_file_path=tmp_path / "moneybin.log")
        root = logging.getLogger()
        fhs = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
        assert fhs

        for name in (
            "moneybin.mcp.server",
            "sqlmesh.core.context",
            "urllib3",
            "root",
        ):
            record = logging.LogRecord(
                name=name,
                level=logging.INFO,
                pathname="",
                lineno=0,
                msg="test",
                args=(),
                exc_info=None,
            )
            for fh in fhs:
                assert fh.filter(record), (
                    f"FileHandler {fh} should accept records from '{name}'"
                )


class TestPydanticLoggingConfig:
    """Tests for the Pydantic LoggingConfig on MoneyBinSettings."""

    @pytest.mark.unit
    def test_default_format_is_human(self) -> None:
        """Default log format should be 'human'."""
        from moneybin.config import LoggingConfig as PydanticLoggingConfig

        config = PydanticLoggingConfig()
        assert config.format == "human"

    @pytest.mark.unit
    def test_json_format_accepted(self) -> None:
        """JSON format should be a valid option."""
        from moneybin.config import LoggingConfig as PydanticLoggingConfig

        config = PydanticLoggingConfig(format="json")
        assert config.format == "json"

    @pytest.mark.unit
    def test_invalid_format_rejected(self) -> None:
        """Invalid format values should raise ValidationError."""
        from pydantic import ValidationError

        from moneybin.config import LoggingConfig as PydanticLoggingConfig

        with pytest.raises(ValidationError):
            PydanticLoggingConfig(format="xml")  # type: ignore[arg-type]  # intentionally invalid for test

    @pytest.mark.unit
    def test_sanitization_always_on(self) -> None:
        """PII sanitization is always on — no config knob to disable it."""
        from moneybin.config import LoggingConfig as PydanticLoggingConfig

        config = PydanticLoggingConfig()
        assert not hasattr(config, "sanitize")

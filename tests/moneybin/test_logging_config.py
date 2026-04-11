"""Tests for centralized logging configuration."""

import logging
import sys
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pytest

from moneybin.logging.config import LoggingConfig, session_log_path, setup_logging


def _force_config(
    log_to_file: bool = False, tmp_path: Path | None = None
) -> LoggingConfig:
    """Return a LoggingConfig that forces handler replacement."""
    kwargs: dict[str, Any] = {"log_to_file": log_to_file, "force_reconfigure": True}
    if tmp_path is not None:
        kwargs["log_file_path"] = tmp_path / "moneybin.log"
    return LoggingConfig(**kwargs)


def _file_handlers(root: logging.Logger) -> list[logging.FileHandler]:
    return [h for h in root.handlers if isinstance(h, logging.FileHandler)]


class TestSessionLogPath:
    """Tests for session_log_path() path structure."""

    @pytest.mark.unit
    def test_path_structure(self) -> None:
        """Path follows logs/{profile}/YYYY-MM-DD/prefix_HH_MM_SS.log format."""
        now = datetime(2025, 4, 11, 13, 57, 18)
        result = session_log_path(
            Path("logs/test/moneybin.log"), prefix="moneybin", now=now
        )
        assert result == Path("logs/test/2025-04-11/moneybin_13_57_18.log")

    @pytest.mark.unit
    def test_prefix_is_applied(self) -> None:
        """Custom prefix appears in the filename."""
        now = datetime(2025, 4, 11, 13, 57, 18)
        result = session_log_path(
            Path("logs/prod/moneybin.log"), prefix="sqlmesh", now=now
        )
        assert result == Path("logs/prod/2025-04-11/sqlmesh_13_57_18.log")


class TestSetupLogging:
    """Tests for setup_logging console handler configuration."""

    @pytest.fixture(autouse=True)
    def _reset_root_logger(self) -> Generator[None, Any, None]:
        """Remove handlers added during each test to avoid leaking state."""
        root = logging.getLogger()
        original_handlers = list(root.handlers)
        yield
        for h in root.handlers[:]:
            if h not in original_handlers:
                h.close()
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

    @pytest.mark.unit
    def test_file_handler_is_catch_all(self, tmp_path: Path) -> None:
        """Moneybin file handler is a catch-all: no filters applied.

        It must accept records from moneybin.*, sqlmesh.*, third-party
        libraries, and the root logger so that a single file contains the
        complete session log.
        """
        setup_logging(config=_force_config(log_to_file=True, tmp_path=tmp_path))
        root = logging.getLogger()

        fhs = _file_handlers(root)
        assert fhs, "Expected at least one FileHandler"

        for name in ("moneybin.mcp.server", "sqlmesh.core.context", "urllib3", "root"):
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

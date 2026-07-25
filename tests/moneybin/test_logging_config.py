"""Tests for centralized logging configuration."""

import logging
import sys
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, cast

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


class _LoggingSetupTestBase:
    """Shared scaffolding for tests that call ``setup_logging``.

    ``setup_logging`` reconfigures the root logger process-wide, so every
    test that calls it has to put the root logger back.
    """

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

    @staticmethod
    def _console_handler(
        stream: Literal["cli", "mcp", "sqlmesh"] = "cli", **kwargs: Any
    ) -> logging.Handler:
        """Configure logging and return the console (non-file) handler."""
        setup_logging(stream=stream, **kwargs)
        console = [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        assert console, "Expected a console StreamHandler"
        return console[0]

    @staticmethod
    def _record(name: str, level: int = logging.INFO) -> logging.LogRecord:
        return logging.LogRecord(
            name=name,
            level=level,
            pathname="",
            lineno=0,
            msg="test",
            args=(),
            exc_info=None,
        )


class TestSetupLogging(_LoggingSetupTestBase):
    """Tests for setup_logging handler configuration."""

    @pytest.mark.unit
    def test_console_handler_uses_stderr(self) -> None:
        """Console handler must write to stderr, not stdout."""
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
    def test_console_handler_uses_stderr_for_mcp(self) -> None:
        """MCP stream should also log to stderr."""
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
    def test_verbose_sets_debug_level(self) -> None:
        """Verbose flag should set root logger to DEBUG."""
        setup_logging(stream="cli", verbose=True)
        assert logging.getLogger().level == logging.DEBUG

    @pytest.mark.unit
    def test_file_handler_created_when_enabled(self, tmp_path: Path) -> None:
        """File handler should be created when log_to_file is enabled."""
        setup_logging(
            stream="cli", log_to_file=True, log_file_path=tmp_path / "moneybin.log"
        )
        root = logging.getLogger()
        fhs = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
        assert fhs, "Expected at least one FileHandler"

    @pytest.mark.unit
    def test_file_handler_uses_sanitized_formatter(self, tmp_path: Path) -> None:
        """File handler must use SanitizedLogFormatter."""
        from moneybin.log_sanitizer import SanitizedLogFormatter

        setup_logging(
            stream="cli", log_to_file=True, log_file_path=tmp_path / "moneybin.log"
        )
        root = logging.getLogger()
        fhs = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
        assert fhs
        assert isinstance(fhs[0].formatter, SanitizedLogFormatter)

    @pytest.mark.unit
    def test_console_handler_uses_sanitized_formatter(self) -> None:
        """Console handler must use SanitizedLogFormatter."""
        from moneybin.log_sanitizer import SanitizedLogFormatter

        setup_logging(stream="cli")
        root = logging.getLogger()
        shs = [
            h
            for h in root.handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        assert shs, "Expected at least one StreamHandler (console)"
        assert isinstance(shs[0].formatter, SanitizedLogFormatter)

    @pytest.mark.unit
    def test_file_handler_is_catch_all(self, tmp_path: Path) -> None:
        """File handler should accept records from any logger."""
        setup_logging(
            stream="cli", log_to_file=True, log_file_path=tmp_path / "moneybin.log"
        )
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


class TestConsoleInfoAllowlist(_LoggingSetupTestBase):
    """The console shows INFO only from loggers declared user-facing.

    Diagnostics from every other logger reach the log file but not the
    user's terminal. The allowlist is the load-bearing part: a denylist
    would leak every dependency nobody has thought to suppress yet.
    """

    @pytest.mark.unit
    def test_info_from_undeclared_dependency_is_hidden(self) -> None:
        """An INFO logger nobody named must not reach the console.

        The fixture is a library that appears on no list. A denylist
        passes it (it isn't suppressed); only an allowlist rejects it.
        Named dependencies like httpx are the wrong fixture here — they
        cannot tell the two designs apart.
        """
        handler = self._console_handler()
        record = self._record("some_vendor_lib.client", logging.INFO)

        assert not handler.filter(record)

    @pytest.mark.unit
    def test_info_from_cli_reaches_console(self) -> None:
        """CLI progress is user-facing and must survive the allowlist."""
        handler = self._console_handler()
        record = self._record("moneybin.cli.commands.sync", logging.INFO)

        assert handler.filter(record)

    @pytest.mark.unit
    def test_warning_from_undeclared_dependency_reaches_console(self) -> None:
        """Quieting INFO must never quiet a problem."""
        handler = self._console_handler()
        record = self._record("some_vendor_lib.client", logging.WARNING)

        assert handler.filter(record)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "module_path",
        [
            "moneybin.services.refresh",
            "moneybin.services.transform_service",
            "moneybin.services.categorization.orchestrator",
            # Schema init and migrations run inline on the first command after
            # an upgrade and can take a while. Every command opens the DB, and
            # no CLI-layer line repeats this progress afterward.
            "moneybin.database",
            # Migration progress and the failure hint that pairs with it —
            # a sibling of moneybin.database, not a descendant, so the prefix
            # match does not cover it.
            "moneybin.migrations",
            # Tells the user their own --institution flag was overridden. A
            # silently-ignored argument is the one thing that must never be
            # quiet, and no CLI-layer line restates it.
            "moneybin.extractors.institution_resolution",
            # Both emit ⚙️/✅ progress, which `.claude/rules/cli.md` reserves
            # for user-facing output.
            "moneybin.services.demo_service",
            "moneybin.synthetic.merchant_seed",
        ],
    )
    def test_pipeline_progress_reaches_console(self, module_path: str) -> None:
        """Long-running stages report progress from below the CLI layer.

        The logger name is read off the real module rather than hardcoded,
        so moving or renaming one of these fails here instead of silently
        going quiet during a two-minute sync.
        """
        import importlib

        module = importlib.import_module(module_path)
        handler = self._console_handler()
        record = self._record(module.logger.name, logging.INFO)

        assert handler.filter(record)

    @pytest.mark.unit
    def test_debug_level_restores_undeclared_dependency_output(self) -> None:
        """Configuring DEBUG must show DEBUG, not file it away silently.

        `--verbose` is not the only way in: a profile can set
        `logging.level: DEBUG`. Asking for the most detailed level and
        getting a quieter console than INFO would be backwards.
        """
        handler = self._console_handler(level="DEBUG")
        record = self._record("some_vendor_lib.client", logging.INFO)

        assert handler.filter(record)

    @pytest.mark.unit
    def test_verbose_restores_undeclared_dependency_output(self) -> None:
        """`--verbose` is the escape hatch and must defeat the allowlist.

        Without this, the one flag a user reaches for when debugging is
        the flag that hides the evidence.
        """
        handler = self._console_handler(verbose=True)
        record = self._record("some_vendor_lib.client", logging.INFO)

        assert handler.filter(record)


class TestMcpStreamKeepsInfoOnStderr(_LoggingSetupTestBase):
    """The MCP stream's stderr is a host-visible channel, not a terminal.

    `docs/specs/observability.md` marks MCP stderr "Always" and shows
    `moneybin.mcp - INFO` lines as what the AI host sees. Under stdio
    transport the file handler is off by default, so filtering INFO here
    doesn't relocate those records — it destroys them.
    """

    @pytest.mark.unit
    def test_mcp_server_info_reaches_host_stderr(self) -> None:
        """Server startup, tool calls, and shutdown must stay visible."""
        handler = self._console_handler("mcp")

        assert handler.filter(self._record("moneybin.mcp.server"))

    @pytest.mark.unit
    def test_verbose_does_not_open_the_host_channel_to_sqlmesh(self) -> None:
        """`mcp serve --verbose` must not flood the host with SQLMesh chatter.

        The DEBUG escape hatch exists so a person can see more in their own
        terminal. On the MCP stream there is no person and no file behind
        stderr, so raising the level must not also lift the denylist — that
        protection was unconditional before the allowlist existed. SQLMesh
        detail is still reachable in `sqlmesh_YYYY-MM-DD.log`.
        """
        handler = self._console_handler("mcp", verbose=True)

        assert not handler.filter(self._record("sqlmesh.core.context"))

    @pytest.mark.unit
    def test_mcp_stream_still_suppresses_sqlmesh_noise(self) -> None:
        """Exempting MCP from the allowlist must not un-mute SQLMesh.

        SQLMesh INFO was suppressed on every stream before the allowlist
        existed. Its dedicated file handler — the other thing that keeps
        it off stderr — is only installed when file logging is on, which
        stdio transport turns off by default. So the filter is the only
        guard left on exactly the path the host reads.
        """
        handler = self._console_handler("mcp")

        assert not handler.filter(self._record("sqlmesh.core.context"))


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

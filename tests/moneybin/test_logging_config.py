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

    @pytest.fixture(autouse=True)
    def _log_file(self, tmp_path: Path) -> Path:
        """A writable log path so tests run the real, file-logging default.

        ``setup_logging``'s own signature defaults ``log_to_file`` to False,
        but ``MoneyBinSettings.logging.log_to_file`` defaults to True — so
        the no-file path is the exception, not the norm, and tests that
        forget the distinction silently assert against the wrong config.
        """
        self._log_path = tmp_path / "moneybin.log"
        return self._log_path

    def _console_handler(
        self, stream: Literal["cli", "mcp", "sqlmesh"] = "cli", **kwargs: Any
    ) -> logging.Handler:
        """Configure logging and return the console (non-file) handler."""
        kwargs.setdefault("log_to_file", True)
        kwargs.setdefault("log_file_path", self._log_path)
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


class TestConsoleNoiseFilter(_LoggingSetupTestBase):
    """CLI diagnostics are opt-in while warnings remain terminal-visible."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "logger_name",
        [
            "httpx",
            "httpcore.connection",
            "sqlmesh.core.context",
            "moneybin.matching.engine",
            "moneybin.extractors.plaid.extractor",
            "moneybin.cli.utils.profile_source",
            "moneybin.services.categorization.orchestrator.engine_counts",
        ],
    )
    def test_cli_default_hides_info_from_every_logger(self, logger_name: str) -> None:
        """Normal CLI output owns results through presenters, never INFO logs."""
        handler = self._console_handler()

        assert not handler.filter(self._record(logger_name, logging.INFO))

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "logger_name",
        [
            "moneybin.matching.engine",
            "moneybin.extractors.plaid.extractor",
            # `categorize_run(methods=["rules"])` calls the engines directly,
            # bypassing categorize_pending()'s summary — and neither the MCP
            # tool nor `--output json` logs the result. On that path this is
            # the only record the count leaves anywhere.
            "moneybin.services.categorization.orchestrator.engine_counts",
        ],
    )
    def test_suppressed_moneybin_records_still_reach_the_file(
        self, logger_name: str
    ) -> None:
        """Hidden from the console is not the same as gone.

        These two carry detail that exists nowhere else once written: the
        per-tier match split (MatchResult.summary() reports only run-wide
        totals) and the per-table row counts (the CLI's totals go out via
        typer.echo, which never reaches the file). Suppressing them from
        stderr is fine only because the file still takes them — which is
        also why the call sites stay at INFO rather than becoming debug,
        since the root logger never emits DEBUG at its default level.
        """
        self._console_handler()
        record = self._record(logger_name, logging.INFO)
        file_handlers = [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.FileHandler)
        ]

        assert file_handlers, "Expected a FileHandler"
        for fh in file_handlers:
            assert fh.filter(record)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "logger_name", ["moneybin.cli.commands.sync", "some_vendor_lib.client"]
    )
    def test_cli_verbose_restores_diagnostics(self, logger_name: str) -> None:
        """Operators can request diagnostic INFO regardless of logger name."""
        handler = self._console_handler(verbose=True)

        assert handler.filter(self._record(logger_name, logging.INFO))

    @pytest.mark.unit
    def test_warning_from_denylisted_dependency_reaches_console(self) -> None:
        """Quieting noise must never quiet a problem."""
        handler = self._console_handler()

        assert handler.filter(self._record("httpx", logging.WARNING))

    @pytest.mark.unit
    def test_no_file_logging_still_hides_cli_info_by_default(self) -> None:
        """No file sink does not recreate the default diagnostic wall."""
        handler = self._console_handler(log_to_file=False)

        assert not handler.filter(self._record("sqlmesh.core.context", logging.INFO))
        assert not handler.filter(self._record("moneybin.database", logging.INFO))

    @pytest.mark.unit
    def test_missing_log_directory_still_hides_cli_info_by_default(
        self, tmp_path: Path
    ) -> None:
        """A failed file setup keeps the same normal CLI console policy."""
        missing = tmp_path / "no-such-profile" / "logs" / "moneybin.log"
        handler = self._console_handler(log_to_file=True, log_file_path=missing)

        assert not [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.FileHandler)
        ], "Expected no FileHandler when the log directory is missing"
        assert not handler.filter(self._record("httpx", logging.INFO))

    @pytest.mark.unit
    def test_real_cli_handlers_keep_pipeline_receipt_and_warning_without_info_wall(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A synthetic pipeline exercises real handlers, not filter internals.

        Services may retain durable INFO diagnostics; normal CLI output instead
        presents its receipt on stdout and the essential warning on stderr.
        """
        setup_logging(stream="cli", log_to_file=False)
        service = logging.getLogger("moneybin.synthetic.pipeline")
        service.info("sync stage complete")
        service.info("refresh stage complete")
        service.warning("refresh needs attention")
        sys.stdout.write("Pipeline complete\n")

        captured = capsys.readouterr()
        assert captured.out == "Pipeline complete\n"
        assert "refresh needs attention" in captured.err
        assert "sync stage complete" not in captured.err
        assert "refresh stage complete" not in captured.err

    @pytest.mark.unit
    def test_real_cli_handlers_verbose_restores_pipeline_diagnostics(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Verbose diagnostics are additive and do not replace the receipt."""
        setup_logging(stream="cli", verbose=True, log_to_file=False)
        service = logging.getLogger("moneybin.synthetic.pipeline")
        service.info("sync stage complete")
        service.warning("refresh needs attention")
        sys.stdout.write("Pipeline complete\n")

        captured = capsys.readouterr()
        assert captured.out == "Pipeline complete\n"
        assert "sync stage complete" in captured.err
        assert "refresh needs attention" in captured.err


class TestMcpStreamKeepsInfoOnStderr(_LoggingSetupTestBase):
    """The MCP stream's stderr is a host-visible channel, not a terminal.

    `docs/specs/observability.md` marks MCP stderr "Always" and shows
    `moneybin.mcp - INFO` lines as what the AI host sees. Under stdio
    transport the file handler is off by default, so anything filtered
    here is destroyed rather than relocated.
    """

    @pytest.mark.unit
    def test_mcp_server_info_reaches_host_stderr(self) -> None:
        """Server startup, tool calls, and shutdown must stay visible."""
        handler = self._console_handler("mcp")

        assert handler.filter(self._record("moneybin.mcp.server", logging.INFO))

    @pytest.mark.parametrize(
        "logger_name",
        [
            "httpx",
            "httpcore.connection",
            "moneybin.matching.engine",
            "moneybin.extractors.plaid.extractor",
        ],
    )
    def test_mcp_stream_suppresses_targeted_dependency_noise(
        self, logger_name: str
    ) -> None:
        """MCP retains application INFO while hiding request and engine chatter."""
        handler = self._console_handler("mcp")

        assert not handler.filter(self._record(logger_name, logging.INFO))

    @pytest.mark.unit
    def test_mcp_stream_still_suppresses_sqlmesh_noise(self) -> None:
        """SQLMesh has its own log file and has never belonged on stderr."""
        handler = self._console_handler("mcp")

        assert not handler.filter(self._record("sqlmesh.core.context", logging.INFO))


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

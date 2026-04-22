# Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unified observability system with consolidated logging, prometheus-backed metrics with DuckDB persistence, and a decorator/context-manager instrumentation API.

**Architecture:** A single `observability.py` facade delegates to internal `logging/` and `metrics/` packages. Logging is consolidated from a dual-config (dataclass + Pydantic) setup into Pydantic-only, with stream-based routing (cli/mcp/sqlmesh) and new HumanFormatter/JSONFormatter. Metrics use `prometheus_client` in-process, flushed to a DuckDB `app.metrics` table on shutdown and periodically for long-running processes (MCP).

**Tech Stack:** Python stdlib logging, prometheus_client, python-json-logger, DuckDB, Typer CLI, Pydantic Settings

---

## File Structure

### New Files

| File | Responsibility |
|------|---------------|
| `src/moneybin/observability.py` | Public API: `setup_observability()`, `tracked`, `track_duration` |
| `src/moneybin/logging/formatters.py` | `HumanFormatter`, `JSONFormatter` |
| `src/moneybin/metrics/__init__.py` | Re-exports: `init_metrics`, metric constants |
| `src/moneybin/metrics/registry.py` | Metric definitions (Counter, Histogram, Gauge) |
| `src/moneybin/metrics/instruments.py` | `@tracked` decorator, `track_duration` context manager |
| `src/moneybin/metrics/persistence.py` | `flush_to_duckdb()`, `load_from_duckdb()` |
| `src/moneybin/sql/schema/app_metrics.sql` | DDL for `app.metrics` table |
| `src/moneybin/cli/commands/stats.py` | `moneybin stats` command implementation |
| `tests/moneybin/test_formatters.py` | Tests for HumanFormatter, JSONFormatter |
| `tests/moneybin/test_observability.py` | Tests for `setup_observability()` |
| `tests/moneybin/test_metrics/__init__.py` | Package marker |
| `tests/moneybin/test_metrics/test_registry.py` | Tests for metric definitions |
| `tests/moneybin/test_metrics/test_instruments.py` | Tests for `@tracked`, `track_duration` |
| `tests/moneybin/test_metrics/test_persistence.py` | Tests for flush/load |
| `tests/moneybin/test_stats_command.py` | Tests for `moneybin stats` CLI command |

### Modified Files

| File | Changes |
|------|---------|
| `src/moneybin/config.py` | Add `format` and `sanitize` fields to `LoggingConfig` |
| `src/moneybin/logging/config.py` | Delete `LoggingConfig` dataclass, `from_environment()`, `setup_dagster_logging()`, `get_log_config_summary()`. Rewrite `setup_logging()` to read from `get_settings().logging` |
| `src/moneybin/logging/__init__.py` | Remove `LoggingConfig` export, add `formatters` |
| `src/moneybin/cli/main.py` | Replace `setup_logging()` with `setup_observability()` |
| `src/moneybin/cli/commands/mcp.py` | Add `setup_observability(stream="mcp")` before server start |
| `src/moneybin/cli/commands/stubs.py` | Remove `stats_app` stub (replaced by real implementation) |
| `src/moneybin/schema.py` | Add `app_metrics.sql` to `_SCHEMA_FILES` |
| `pyproject.toml` | Add `prometheus_client`, `python-json-logger` |
| `tests/moneybin/test_logging_config.py` | Update for new `setup_logging()` signature (no `config` param) |

---

## Task 1: Add Dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add prometheus_client and python-json-logger to dependencies**

In `pyproject.toml`, add these two lines to the `dependencies` array, after the `"typer>=0.24.1"` line:

```python
# Observability: metrics registry and JSON log formatting
("prometheus_client>=0.22.0",)
("python-json-logger>=3.3.0",)
```

- [ ] **Step 2: Install and verify**

Run: `uv sync`
Expected: Both packages install without errors.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "feat(observability): add prometheus_client and python-json-logger deps"
```

---

## Task 2: Update LoggingConfig in config.py

**Files:**
- Modify: `src/moneybin/config.py:132-150`
- Test: `tests/moneybin/test_logging_config.py` (existing, verify still passes)

- [ ] **Step 1: Write the test for new config fields**

Create test in `tests/moneybin/test_logging_config.py`. Add a new test class at the end of the file:

```python
from moneybin.config import LoggingConfig as PydanticLoggingConfig


class TestPydanticLoggingConfig:
    """Tests for the Pydantic LoggingConfig on MoneyBinSettings."""

    @pytest.mark.unit
    def test_default_format_is_human(self) -> None:
        """Default log format should be 'human'."""
        config = PydanticLoggingConfig()
        assert config.format == "human"

    @pytest.mark.unit
    def test_json_format_accepted(self) -> None:
        """JSON format should be a valid option."""
        config = PydanticLoggingConfig(format="json")
        assert config.format == "json"

    @pytest.mark.unit
    def test_invalid_format_rejected(self) -> None:
        """Invalid format values should raise ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            PydanticLoggingConfig(format="xml")

    @pytest.mark.unit
    def test_sanitize_defaults_true(self) -> None:
        """Sanitize flag should default to True."""
        config = PydanticLoggingConfig()
        assert config.sanitize is True

    @pytest.mark.unit
    def test_config_is_frozen(self) -> None:
        """LoggingConfig should be immutable."""
        from pydantic import ValidationError

        config = PydanticLoggingConfig()
        with pytest.raises(ValidationError):
            config.format = "json"  # type: ignore[misc]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/moneybin/test_logging_config.py::TestPydanticLoggingConfig -v`
Expected: FAIL — `PydanticLoggingConfig` has no `format` field yet.

- [ ] **Step 3: Add format and sanitize fields to LoggingConfig**

In `src/moneybin/config.py`, replace the existing `LoggingConfig` class (lines 132-150):

```python
class LoggingConfig(BaseModel):
    """Logging configuration settings."""

    model_config = ConfigDict(frozen=True)

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", description="Logging level"
    )
    log_to_file: bool = Field(default=True, description="Enable file logging")
    log_file_path: Path = Field(
        default=Path("logs/default/moneybin.log"), description="Path to log file"
    )
    max_file_size_mb: int = Field(
        default=50, ge=1, le=1000, description="Maximum log file size in MB"
    )
    backup_count: int = Field(
        default=5, ge=1, le=50, description="Number of log file backups to keep"
    )
    format: Literal["human", "json"] = Field(
        default="human", description="Log output format: human-readable or JSON"
    )
    sanitize: bool = Field(
        default=True,
        description="PII sanitization on all log output. Always on — exists for visibility only.",
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/moneybin/test_logging_config.py::TestPydanticLoggingConfig -v`
Expected: PASS (all 5 tests)

- [ ] **Step 5: Run full test suite to confirm no regressions**

Run: `uv run pytest tests/moneybin/test_logging_config.py -v`
Expected: All existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/config.py tests/moneybin/test_logging_config.py
git commit -m "feat(observability): add format and sanitize fields to LoggingConfig"
```

---

## Task 3: Create HumanFormatter and JSONFormatter

**Files:**
- Create: `src/moneybin/logging/formatters.py`
- Create: `tests/moneybin/test_formatters.py`

- [ ] **Step 1: Write tests for HumanFormatter**

Create `tests/moneybin/test_formatters.py`:

```python
"""Tests for log formatters."""

import json
import logging

import pytest

from moneybin.logging.formatters import HumanFormatter, JSONFormatter


class TestHumanFormatter:
    """Tests for HumanFormatter variants."""

    @pytest.mark.unit
    def test_cli_variant_message_only(self) -> None:
        """CLI variant should output only the message, no timestamp."""
        formatter = HumanFormatter(variant="cli")
        record = logging.LogRecord(
            name="moneybin.cli",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Hello world",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        assert result == "Hello world"

    @pytest.mark.unit
    def test_full_variant_includes_timestamp_and_name(self) -> None:
        """Full variant should include timestamp, logger name, and level."""
        formatter = HumanFormatter(variant="full")
        record = logging.LogRecord(
            name="moneybin.mcp",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Server started",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        assert "moneybin.mcp" in result
        assert "INFO" in result
        assert "Server started" in result


class TestJSONFormatter:
    """Tests for JSONFormatter."""

    @pytest.mark.unit
    def test_output_is_valid_json(self) -> None:
        """Each formatted line should be valid JSON."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="moneybin.import",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="Duplicate found",
            args=(),
            exc_info=None,
        )
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed["message"] == "Duplicate found"
        assert parsed["level"] == "WARNING"
        assert parsed["logger"] == "moneybin.import"
        assert "timestamp" in parsed

    @pytest.mark.unit
    def test_extra_fields_included(self) -> None:
        """Extra dict fields on the record should appear in JSON output."""
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="moneybin.import",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="Loaded records",
            args=(),
            exc_info=None,
        )
        record.source_type = "csv"  # type: ignore[attr-defined]
        result = formatter.format(record)
        parsed = json.loads(result)
        assert parsed["source_type"] == "csv"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_formatters.py -v`
Expected: FAIL — `moneybin.logging.formatters` module does not exist.

- [ ] **Step 3: Implement formatters**

Create `src/moneybin/logging/formatters.py`:

```python
"""Log formatters for MoneyBin.

HumanFormatter provides human-readable output in two variants:
- "cli": message-only for CLI stderr (no timestamp clutter)
- "full": timestamp + logger + level + message for files and MCP stderr

JSONFormatter provides one JSON object per line for structured log analysis.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Literal


class HumanFormatter(logging.Formatter):
    """Human-readable log formatter with CLI and full variants.

    Args:
        variant: "cli" for message-only, "full" for timestamped output.
    """

    _CLI_FMT = "%(message)s"
    _FULL_FMT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    def __init__(self, variant: Literal["cli", "full"] = "full") -> None:
        fmt = self._CLI_FMT if variant == "cli" else self._FULL_FMT
        super().__init__(fmt)


# Standard fields that should NOT be copied into the JSON "extra" bucket.
_RESERVED_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)


class JSONFormatter(logging.Formatter):
    """One JSON object per line.

    Output includes ``timestamp``, ``logger``, ``level``, ``message``,
    plus any extra attributes set on the LogRecord.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a single-line JSON object.

        Args:
            record: The log record to format.

        Returns:
            JSON string with log data.
        """
        record.message = record.getMessage()

        obj: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "logger": record.name,
            "level": record.levelname,
            "message": record.message,
        }

        # Copy non-standard attributes as extra fields
        for key, value in record.__dict__.items():
            if key not in _RESERVED_ATTRS and key != "message":
                try:
                    json.dumps(value)  # Only include JSON-serializable values
                    obj[key] = value
                except (TypeError, ValueError):
                    pass

        if record.exc_info and record.exc_info[1] is not None:
            obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(obj, default=str)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_formatters.py -v`
Expected: PASS (all 4 tests)

- [ ] **Step 5: Update logging/__init__.py exports**

Replace the contents of `src/moneybin/logging/__init__.py`:

```python
"""Centralized logging configuration for MoneyBin application.

This package provides unified logging configuration across all MoneyBin components.

Standard usage:
    ```python
    import logging

    logger = logging.getLogger(__name__)
    ```

Internal setup is called through ``moneybin.observability.setup_observability()``.
Direct import of ``setup_logging`` is for internal use only.
"""

from .config import session_log_path, setup_logging
from .formatters import HumanFormatter, JSONFormatter

__all__ = ["HumanFormatter", "JSONFormatter", "session_log_path", "setup_logging"]
```

- [ ] **Step 6: Commit**

```bash
git add src/moneybin/logging/formatters.py src/moneybin/logging/__init__.py tests/moneybin/test_formatters.py
git commit -m "feat(observability): add HumanFormatter and JSONFormatter"
```

---

## Task 4: Rewrite setup_logging() — Consolidate to Pydantic Config

**Files:**
- Modify: `src/moneybin/logging/config.py`
- Modify: `tests/moneybin/test_logging_config.py`

This task deletes the `LoggingConfig` dataclass, `from_environment()`, `setup_dagster_logging()`, and `get_log_config_summary()`. It rewrites `setup_logging()` to read from `get_settings().logging` and use the new formatters with stream-based log file naming.

- [ ] **Step 1: Write tests for the new setup_logging signature**

Replace the contents of `tests/moneybin/test_logging_config.py` entirely:

```python
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
            PydanticLoggingConfig(format="xml")

    @pytest.mark.unit
    def test_sanitize_defaults_true(self) -> None:
        """Sanitize flag should default to True."""
        from moneybin.config import LoggingConfig as PydanticLoggingConfig

        config = PydanticLoggingConfig()
        assert config.sanitize is True
```

- [ ] **Step 2: Run tests to see current failures**

Run: `uv run pytest tests/moneybin/test_logging_config.py -v`
Expected: Some tests fail — new signature `stream=` not yet implemented.

- [ ] **Step 3: Rewrite setup_logging and session_log_path**

Replace the entire contents of `src/moneybin/logging/config.py`:

```python
"""Logging configuration for MoneyBin.

This module provides ``setup_logging()`` which configures Python's logging
system based on settings from ``get_settings().logging``. It is called
internally by ``setup_observability()`` — application code should not call
it directly.
"""

import logging
import stat as stat_mod
import sys
from datetime import datetime
from pathlib import Path

from moneybin.log_sanitizer import SanitizedLogFormatter
from moneybin.logging.formatters import HumanFormatter, JSONFormatter


def session_log_path(
    configured_path: Path,
    prefix: str = "cli",
    now: datetime | None = None,
) -> Path:
    """Derive a daily log path from the configured log file path.

    Transforms ``logs/{profile}/moneybin.log`` into
    ``logs/{profile}/{prefix}_YYYY-MM-DD.log`` so that logs are
    grouped by stream and day. Each stream appends to its daily file.

    Args:
        configured_path: The log_file_path from configuration (used to find
            the profile log directory).
        prefix: Stream name prefix (e.g. "cli", "mcp", "sqlmesh").
        now: Timestamp to use for the path; defaults to the current time.

    Returns:
        Path to the stream-specific daily log file.
    """
    if now is None:
        now = datetime.now()
    profile_log_dir = configured_path.parent
    return profile_log_dir / f"{prefix}_{now.strftime('%Y-%m-%d')}.log"


def setup_logging(
    stream: str = "cli",
    verbose: bool = False,
    profile: str | None = None,
    *,
    log_file_path: Path | None = None,
) -> None:
    """Set up centralized logging configuration.

    Reads from ``get_settings().logging`` for all configuration. The optional
    ``log_file_path`` parameter is for testing only — production callers
    should not pass it.

    Args:
        stream: Log stream name — determines file prefix and console format.
            "cli" uses message-only console format; "mcp" and "sqlmesh" use
            full format with timestamps.
        verbose: If True, enable DEBUG level logging (overrides config level).
        profile: Optional profile name (unused — profile is set via
            ``set_current_profile()`` before this runs).
        log_file_path: Override log file path (testing only).
    """
    from moneybin.config import get_settings

    settings = get_settings()
    log_config = settings.logging

    # Determine log level
    if verbose:
        level = logging.DEBUG
    else:
        level = getattr(logging, log_config.level)

    # Build inner formatter based on config
    if log_config.format == "json":
        inner_formatter: logging.Formatter = JSONFormatter()
    elif stream == "cli":
        inner_formatter = HumanFormatter(variant="cli")
    else:
        inner_formatter = HumanFormatter(variant="full")

    # Console formatter: CLI gets message-only, others get full
    if stream == "cli":
        console_formatter: logging.Formatter = HumanFormatter(variant="cli")
    else:
        console_formatter = HumanFormatter(variant="full")

    # Prepare handlers
    handlers: list[logging.Handler] = []

    # Console handler (always present, writes to stderr)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(console_formatter)
    handlers.append(console_handler)

    # File handler
    file_path = log_file_path or log_config.log_file_path
    if log_config.log_to_file or log_file_path is not None:
        log_file = session_log_path(file_path, prefix=stream)
        log_file.parent.mkdir(parents=True, exist_ok=True)

        # Set restrictive permissions on log file (macOS/Linux)
        if sys.platform != "win32" and log_file.exists():
            try:
                log_file.chmod(stat_mod.S_IRUSR | stat_mod.S_IWUSR)  # 0600
            except OSError:
                pass

        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(SanitizedLogFormatter(inner_formatter))
        handlers.append(file_handler)

    # Configure root logger
    logging.basicConfig(
        level=level,
        handlers=handlers,
        force=True,
    )

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("plaid").setLevel(logging.INFO)
    logging.getLogger("sqlmesh.core.analytics.dispatcher").setLevel(logging.WARNING)

    # Suppress SQLMesh analytics shutdown message
    class _SuppressFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return "Shutting down the event dispatcher" not in record.getMessage()

    logging.root.addFilter(_SuppressFilter())
```

Note: `SanitizedLogFormatter` now wraps an inner formatter. We need to update it to support this.

- [ ] **Step 4: Update SanitizedLogFormatter to wrap an inner formatter**

In `src/moneybin/log_sanitizer.py`, replace the class:

```python
class SanitizedLogFormatter(logging.Formatter):
    """Log formatter that detects and masks PII patterns.

    Wraps an inner formatter, applying PII masking to its output.
    Can also be used standalone with a format string.

    Patterns detected:
    - SSN: NNN-NN-NNNN → ***-**-****
    - Account numbers: 8+ digits → ****...NNNN (last 4)
    - Dollar amounts: $N,NNN.NN → $***

    When a pattern is masked, a separate WARNING is emitted identifying
    the leak source (module, line number).

    Args:
        inner: Either a format string (str) or a Formatter instance to wrap.
    """

    def __init__(self, inner: str | logging.Formatter = "") -> None:
        if isinstance(inner, logging.Formatter):
            super().__init__()
            self._inner = inner
        else:
            super().__init__(inner)
            self._inner = None

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record, masking any PII patterns found.

        Args:
            record: The log record to format.

        Returns:
            Formatted and sanitized log string.
        """
        if self._inner is not None:
            formatted = self._inner.format(record)
        else:
            formatted = super().format(record)
        masked = False

        # Mask SSNs
        def ssn_replacer(match: re.Match[str]) -> str:
            nonlocal masked
            masked = True
            return "***-**-****"

        result = _SSN_PATTERN.sub(ssn_replacer, formatted)

        # Mask dollar amounts
        new_result = _DOLLAR_PATTERN.sub("$***", result)
        if new_result != result:
            masked = True
            result = new_result

        # Mask account numbers (8+ digit sequences; regex guarantees len >= 8)
        def account_replacer(match: re.Match[str]) -> str:
            nonlocal masked
            masked = True
            return f"****...{match.group(1)[-4:]}"

        result = _ACCOUNT_PATTERN.sub(account_replacer, result)

        # Guard against re-entrant calls
        if masked and record.name != __name__:
            _sanitizer_logger.warning(
                "PII pattern detected and masked in log output (source: %s:%s)",
                record.pathname,
                record.lineno,
            )

        return result
```

- [ ] **Step 5: Update logging/__init__.py**

Replace `src/moneybin/logging/__init__.py`:

```python
"""Centralized logging configuration for MoneyBin application.

Internal setup is called through ``moneybin.observability.setup_observability()``.
Direct import of ``setup_logging`` is for internal use only.

Standard usage:
    ```python
    import logging

    logger = logging.getLogger(__name__)
    ```
"""

from .config import session_log_path, setup_logging
from .formatters import HumanFormatter, JSONFormatter

__all__ = ["HumanFormatter", "JSONFormatter", "session_log_path", "setup_logging"]
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_logging_config.py -v`
Expected: All tests pass.

- [ ] **Step 7: Run full test suite to check for regressions**

Run: `uv run pytest tests/ -v`
Expected: All 519+ tests pass (some test files may need adjustment if they import the deleted `LoggingConfig` dataclass).

- [ ] **Step 8: Fix any import errors from deleted LoggingConfig dataclass**

If any test or source file imports `LoggingConfig` from `moneybin.logging`, update the import to use `moneybin.config.LoggingConfig` instead, or remove the import if it was only used for calling `from_environment()`.

- [ ] **Step 9: Commit**

```bash
git add src/moneybin/logging/config.py src/moneybin/logging/__init__.py src/moneybin/log_sanitizer.py tests/moneybin/test_logging_config.py
git commit -m "refactor(observability): consolidate logging to Pydantic config, add stream routing"
```

---

## Task 5: Create Metrics Registry

**Files:**
- Create: `src/moneybin/metrics/__init__.py`
- Create: `src/moneybin/metrics/registry.py`
- Create: `tests/moneybin/test_metrics/__init__.py`
- Create: `tests/moneybin/test_metrics/test_registry.py`

- [ ] **Step 1: Write tests for metric definitions**

Create `tests/moneybin/test_metrics/__init__.py` (empty file).

Create `tests/moneybin/test_metrics/test_registry.py`:

```python
"""Tests for metrics registry definitions."""

import pytest
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class TestMetricDefinitions:
    """Tests that all expected metrics are defined with correct types and labels."""

    @pytest.fixture(autouse=True)
    def _fresh_registry(self) -> None:
        """Import registry with a fresh prometheus registry each test."""
        # prometheus_client uses a global registry; we test against our module's
        # exported constants which are bound to the default registry.
        pass

    @pytest.mark.unit
    def test_import_records_total_is_counter(self) -> None:
        from moneybin.metrics.registry import IMPORT_RECORDS_TOTAL

        assert isinstance(IMPORT_RECORDS_TOTAL, Counter)

    @pytest.mark.unit
    def test_import_records_total_has_source_type_label(self) -> None:
        from moneybin.metrics.registry import IMPORT_RECORDS_TOTAL

        assert "source_type" in IMPORT_RECORDS_TOTAL._labelnames

    @pytest.mark.unit
    def test_import_duration_is_histogram(self) -> None:
        from moneybin.metrics.registry import IMPORT_DURATION_SECONDS

        assert isinstance(IMPORT_DURATION_SECONDS, Histogram)

    @pytest.mark.unit
    def test_import_errors_total_is_counter(self) -> None:
        from moneybin.metrics.registry import IMPORT_ERRORS_TOTAL

        assert isinstance(IMPORT_ERRORS_TOTAL, Counter)

    @pytest.mark.unit
    def test_categorization_auto_rate_is_gauge(self) -> None:
        from moneybin.metrics.registry import CATEGORIZATION_AUTO_RATE

        assert isinstance(CATEGORIZATION_AUTO_RATE, Gauge)

    @pytest.mark.unit
    def test_mcp_tool_calls_total_is_counter(self) -> None:
        from moneybin.metrics.registry import MCP_TOOL_CALLS_TOTAL

        assert isinstance(MCP_TOOL_CALLS_TOTAL, Counter)

    @pytest.mark.unit
    def test_mcp_tool_duration_is_histogram(self) -> None:
        from moneybin.metrics.registry import MCP_TOOL_DURATION_SECONDS

        assert isinstance(MCP_TOOL_DURATION_SECONDS, Histogram)

    @pytest.mark.unit
    def test_db_query_duration_is_histogram(self) -> None:
        from moneybin.metrics.registry import DB_QUERY_DURATION_SECONDS

        assert isinstance(DB_QUERY_DURATION_SECONDS, Histogram)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_metrics/test_registry.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the metrics registry**

Create `src/moneybin/metrics/__init__.py`:

```python
"""Metrics collection and instrumentation for MoneyBin.

Public API:
    - ``init_metrics()`` — initialize the registry and load prior state
    - Metric constants (``IMPORT_RECORDS_TOTAL``, etc.) for manual recording

For instrumentation, use ``moneybin.observability.tracked`` and
``moneybin.observability.track_duration`` instead of importing from here.
"""

from .registry import (
    CATEGORIZATION_AUTO_RATE,
    CATEGORIZATION_RULES_FIRED_TOTAL,
    DB_QUERY_DURATION_SECONDS,
    DEDUP_MATCHES_TOTAL,
    IMPORT_DURATION_SECONDS,
    IMPORT_ERRORS_TOTAL,
    IMPORT_RECORDS_TOTAL,
    MCP_TOOL_CALLS_TOTAL,
    MCP_TOOL_DURATION_SECONDS,
    SQLMESH_RUN_DURATION_SECONDS,
)

__all__ = [
    "CATEGORIZATION_AUTO_RATE",
    "CATEGORIZATION_RULES_FIRED_TOTAL",
    "DB_QUERY_DURATION_SECONDS",
    "DEDUP_MATCHES_TOTAL",
    "IMPORT_DURATION_SECONDS",
    "IMPORT_ERRORS_TOTAL",
    "IMPORT_RECORDS_TOTAL",
    "MCP_TOOL_CALLS_TOTAL",
    "MCP_TOOL_DURATION_SECONDS",
    "SQLMESH_RUN_DURATION_SECONDS",
]
```

Create `src/moneybin/metrics/registry.py`:

```python
"""Metric definitions for MoneyBin.

All metrics use the ``moneybin_`` prefix. Each metric is a module-level
constant bound to the default prometheus_client registry.

Adding a new metric: define it here, then either use ``@tracked`` at the
call site or record manually (e.g. ``CATEGORIZATION_AUTO_RATE.set(0.78)``).
"""

from prometheus_client import Counter, Gauge, Histogram

# ── Import pipeline ──────────────────────────────────────────────────────────

IMPORT_RECORDS_TOTAL = Counter(
    "moneybin_import_records_total",
    "Total records imported across all sources",
    ["source_type"],
)

IMPORT_DURATION_SECONDS = Histogram(
    "moneybin_import_duration_seconds",
    "Duration of import operations in seconds",
    ["source_type"],
)

IMPORT_ERRORS_TOTAL = Counter(
    "moneybin_import_errors_total",
    "Total import errors by source and error type",
    ["source_type", "error_type"],
)

# ── SQLMesh transforms ───────────────────────────────────────────────────────

SQLMESH_RUN_DURATION_SECONDS = Histogram(
    "moneybin_sqlmesh_run_duration_seconds",
    "Duration of SQLMesh model runs in seconds",
    ["model"],
)

# ── Deduplication ─────────────────────────────────────────────────────────────

DEDUP_MATCHES_TOTAL = Counter(
    "moneybin_dedup_matches_total",
    "Total duplicate records matched and merged",
)

# ── Categorization ────────────────────────────────────────────────────────────

CATEGORIZATION_AUTO_RATE = Gauge(
    "moneybin_categorization_auto_rate",
    "Fraction of transactions auto-categorized (0.0–1.0)",
)

CATEGORIZATION_RULES_FIRED_TOTAL = Counter(
    "moneybin_categorization_rules_fired_total",
    "Total categorization rule firings by rule",
    ["rule_id"],
)

# ── MCP server ────────────────────────────────────────────────────────────────

MCP_TOOL_CALLS_TOTAL = Counter(
    "moneybin_mcp_tool_calls_total",
    "Total MCP tool invocations by tool name",
    ["tool_name"],
)

MCP_TOOL_DURATION_SECONDS = Histogram(
    "moneybin_mcp_tool_duration_seconds",
    "Duration of MCP tool calls in seconds",
    ["tool_name"],
)

# ── Database ──────────────────────────────────────────────────────────────────

DB_QUERY_DURATION_SECONDS = Histogram(
    "moneybin_db_query_duration_seconds",
    "Duration of database queries in seconds",
    ["operation"],
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_metrics/test_registry.py -v`
Expected: PASS (all 8 tests)

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/metrics/__init__.py src/moneybin/metrics/registry.py tests/moneybin/test_metrics/__init__.py tests/moneybin/test_metrics/test_registry.py
git commit -m "feat(observability): add prometheus_client metric registry"
```

---

## Task 6: Create Instrumentation API (@tracked, track_duration)

**Files:**
- Create: `src/moneybin/metrics/instruments.py`
- Create: `tests/moneybin/test_metrics/test_instruments.py`

- [ ] **Step 1: Write tests for @tracked and track_duration**

Create `tests/moneybin/test_metrics/test_instruments.py`:

```python
"""Tests for instrumentation decorators and context managers."""

import logging
import time

import pytest
from prometheus_client import REGISTRY


class TestTracked:
    """Tests for the @tracked decorator."""

    @pytest.mark.unit
    def test_tracked_function_returns_normally(self) -> None:
        """Decorated function should return its result unchanged."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_op")
        def add(a: int, b: int) -> int:
            return a + b

        assert add(2, 3) == 5

    @pytest.mark.unit
    def test_tracked_increments_call_counter(self) -> None:
        """@tracked should increment the operation's call counter."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_calls")
        def noop() -> None:
            pass

        # Get baseline
        before = REGISTRY.get_sample_value(
            "moneybin_tracked_calls_total", {"operation": "test_calls"}
        )
        noop()
        after = REGISTRY.get_sample_value(
            "moneybin_tracked_calls_total", {"operation": "test_calls"}
        )
        assert (after or 0) - (before or 0) == 1

    @pytest.mark.unit
    def test_tracked_observes_duration(self) -> None:
        """@tracked should record duration in histogram."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_duration")
        def slow() -> None:
            time.sleep(0.05)

        before = REGISTRY.get_sample_value(
            "moneybin_tracked_duration_seconds_count",
            {"operation": "test_duration"},
        )
        slow()
        after = REGISTRY.get_sample_value(
            "moneybin_tracked_duration_seconds_count",
            {"operation": "test_duration"},
        )
        assert (after or 0) - (before or 0) == 1

    @pytest.mark.unit
    def test_tracked_increments_error_counter_on_exception(self) -> None:
        """@tracked should increment error counter when function raises."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_errors")
        def fail() -> None:
            raise ValueError("boom")

        before = REGISTRY.get_sample_value(
            "moneybin_tracked_errors_total",
            {"operation": "test_errors", "error_type": "ValueError"},
        )
        with pytest.raises(ValueError, match="boom"):
            fail()
        after = REGISTRY.get_sample_value(
            "moneybin_tracked_errors_total",
            {"operation": "test_errors", "error_type": "ValueError"},
        )
        assert (after or 0) - (before or 0) == 1

    @pytest.mark.unit
    def test_tracked_emits_debug_log(self, caplog: pytest.LogCaptureFixture) -> None:
        """@tracked should emit a DEBUG log line on completion."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_log")
        def ping() -> str:
            return "pong"

        with caplog.at_level(logging.DEBUG):
            ping()

        assert any("test_log" in r.message for r in caplog.records)

    @pytest.mark.unit
    def test_tracked_with_labels(self) -> None:
        """@tracked should support static labels."""
        from moneybin.metrics.instruments import tracked

        @tracked("test_labels", labels={"source_type": "csv"})
        def import_csv() -> None:
            pass

        import_csv()
        value = REGISTRY.get_sample_value(
            "moneybin_tracked_calls_total",
            {"operation": "test_labels", "source_type": "csv"},
        )
        assert value is not None and value >= 1


class TestTrackDuration:
    """Tests for the track_duration context manager."""

    @pytest.mark.unit
    def test_track_duration_records_histogram(self) -> None:
        """track_duration should record duration in the histogram."""
        from moneybin.metrics.instruments import track_duration

        before = REGISTRY.get_sample_value(
            "moneybin_tracked_duration_seconds_count",
            {"operation": "test_ctx"},
        )
        with track_duration("test_ctx"):
            time.sleep(0.01)
        after = REGISTRY.get_sample_value(
            "moneybin_tracked_duration_seconds_count",
            {"operation": "test_ctx"},
        )
        assert (after or 0) - (before or 0) == 1

    @pytest.mark.unit
    def test_track_duration_emits_debug_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """track_duration should emit a DEBUG log on exit."""
        from moneybin.metrics.instruments import track_duration

        with caplog.at_level(logging.DEBUG):
            with track_duration("test_ctx_log"):
                pass

        assert any("test_ctx_log" in r.message for r in caplog.records)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_metrics/test_instruments.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement instruments**

Create `src/moneybin/metrics/instruments.py`:

```python
"""Instrumentation helpers for MoneyBin.

Provides ``@tracked`` (decorator) and ``track_duration`` (context manager)
for recording call counts, durations, and errors with minimal boilerplate.

Both emit a DEBUG-level log line on completion with operation name and duration.
"""

import functools
import logging
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any, ParamSpec, TypeVar

from prometheus_client import Counter, Histogram

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

# Generic tracked metrics — used by @tracked and track_duration.
# Domain-specific metrics (IMPORT_RECORDS_TOTAL, etc.) are in registry.py
# and recorded manually at domain-specific call sites.
_TRACKED_CALLS = Counter(
    "moneybin_tracked_calls_total",
    "Total tracked operation calls",
    ["operation", "source_type"],
)

_TRACKED_DURATION = Histogram(
    "moneybin_tracked_duration_seconds",
    "Duration of tracked operations in seconds",
    ["operation", "source_type"],
)

_TRACKED_ERRORS = Counter(
    "moneybin_tracked_errors_total",
    "Total tracked operation errors",
    ["operation", "error_type", "source_type"],
)


def tracked(
    operation: str,
    labels: dict[str, str] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator that records call count, duration, and errors for a function.

    Args:
        operation: Name of the operation (e.g. "import", "dedup").
        labels: Optional static labels to attach to all metrics.

    Returns:
        Decorator that wraps the function with instrumentation.
    """
    extra_labels = labels or {}

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            metric_labels = {"operation": operation, **extra_labels}
            # Fill missing label keys with empty string for prometheus
            call_labels = {
                k: metric_labels.get(k, "") for k in ("operation", "source_type")
            }
            _TRACKED_CALLS.labels(**call_labels).inc()
            start = time.monotonic()
            try:
                result = func(*args, **kwargs)
                duration = time.monotonic() - start
                _TRACKED_DURATION.labels(**call_labels).observe(duration)
                logger.debug(f"{operation} completed in {duration:.3f}s")
                return result
            except Exception as exc:
                duration = time.monotonic() - start
                _TRACKED_DURATION.labels(**call_labels).observe(duration)
                error_labels = {
                    "operation": operation,
                    "error_type": type(exc).__name__,
                    "source_type": extra_labels.get("source_type", ""),
                }
                _TRACKED_ERRORS.labels(**error_labels).inc()
                logger.debug(
                    f"{operation} failed after {duration:.3f}s: {type(exc).__name__}"
                )
                raise

        return wrapper

    return decorator


@contextmanager
def track_duration(operation: str, labels: dict[str, str] | None = None):
    """Context manager that records the duration of a block.

    Args:
        operation: Name of the operation.
        labels: Optional static labels.

    Yields:
        None — the block executes normally.
    """
    extra_labels = labels or {}
    call_labels = {
        "operation": operation,
        "source_type": extra_labels.get("source_type", ""),
    }
    start = time.monotonic()
    try:
        yield
    finally:
        duration = time.monotonic() - start
        _TRACKED_DURATION.labels(**call_labels).observe(duration)
        logger.debug(f"{operation} completed in {duration:.3f}s")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_metrics/test_instruments.py -v`
Expected: PASS (all 8 tests)

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/metrics/instruments.py tests/moneybin/test_metrics/test_instruments.py
git commit -m "feat(observability): add @tracked decorator and track_duration context manager"
```

---

## Task 7: Create Metrics Persistence (flush/load to DuckDB)

**Files:**
- Create: `src/moneybin/sql/schema/app_metrics.sql`
- Modify: `src/moneybin/schema.py` (add to `_SCHEMA_FILES`)
- Create: `src/moneybin/metrics/persistence.py`
- Create: `tests/moneybin/test_metrics/test_persistence.py`

- [ ] **Step 1: Create the app_metrics DDL**

Create `src/moneybin/sql/schema/app_metrics.sql`:

```sql
/* Prometheus metric snapshots flushed periodically and on shutdown; each row is a point-in-time snapshot of one metric */
CREATE TABLE IF NOT EXISTS app.metrics (
    metric_name VARCHAR NOT NULL, -- Prometheus metric name (e.g. 'moneybin_import_records_total')
    metric_type VARCHAR NOT NULL, -- One of: 'counter', 'histogram', 'gauge'
    labels JSON, -- Label key-value pairs as JSON object
    value DOUBLE NOT NULL, -- Counter/gauge current value, or histogram sum
    bucket_bounds DOUBLE[], -- Histogram upper bounds (NULL for counter/gauge)
    bucket_counts BIGINT[], -- Histogram cumulative bucket counts (NULL for counter/gauge)
    recorded_at TIMESTAMP NOT NULL -- When this snapshot was taken
);
```

- [ ] **Step 2: Register the schema file**

In `src/moneybin/schema.py`, add `"app_metrics.sql"` to the end of the `_SCHEMA_FILES` list:

```python
_SCHEMA_FILES: list[str] = [
    "raw_schema.sql",
    "core_schema.sql",
    "app_schema.sql",
    "raw_ofx_institutions.sql",
    "raw_ofx_accounts.sql",
    "raw_ofx_transactions.sql",
    "raw_ofx_balances.sql",
    "raw_w2_forms.sql",
    "raw_csv_accounts.sql",
    "raw_csv_transactions.sql",
    "app_categories.sql",
    "app_merchants.sql",
    "app_categorization_rules.sql",
    "app_transaction_categories.sql",
    "app_budgets.sql",
    "app_transaction_notes.sql",
    "app_metrics.sql",
]
```

- [ ] **Step 3: Write tests for flush and load**

Create `tests/moneybin/test_metrics/test_persistence.py`:

```python
"""Tests for metrics persistence (flush to / load from DuckDB)."""

import json
from unittest.mock import MagicMock

import pytest
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


@pytest.fixture()
def fresh_registry() -> CollectorRegistry:
    """Create a fresh prometheus registry for isolation."""
    return CollectorRegistry()


@pytest.fixture()
def mock_db() -> MagicMock:
    """Create a mock Database with an in-memory DuckDB for real SQL."""
    import duckdb

    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA IF NOT EXISTS app")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app.metrics (
            metric_name VARCHAR NOT NULL,
            metric_type VARCHAR NOT NULL,
            labels JSON,
            value DOUBLE NOT NULL,
            bucket_bounds DOUBLE[],
            bucket_counts BIGINT[],
            recorded_at TIMESTAMP NOT NULL
        )
    """)

    db = MagicMock()
    db.conn = conn
    db.execute = conn.execute
    return db


class TestFlushToDuckDB:
    """Tests for flush_to_duckdb."""

    @pytest.mark.unit
    def test_flush_counter(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Counter values should be flushed as metric rows."""
        from moneybin.metrics.persistence import flush_to_duckdb

        counter = Counter(
            "test_counter", "A test counter", ["op"], registry=fresh_registry
        )
        counter.labels(op="read").inc(5)

        flush_to_duckdb(mock_db, registry=fresh_registry)

        rows = mock_db.execute(
            "SELECT metric_name, metric_type, labels, value FROM app.metrics "
            "WHERE metric_name = ?",
            ["test_counter_total"],
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "test_counter_total"
        assert rows[0][1] == "counter"
        labels = json.loads(rows[0][2])
        assert labels["op"] == "read"
        assert rows[0][3] == 5.0

    @pytest.mark.unit
    def test_flush_gauge(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Gauge values should be flushed."""
        from moneybin.metrics.persistence import flush_to_duckdb

        gauge = Gauge("test_gauge", "A test gauge", registry=fresh_registry)
        gauge.set(0.78)

        flush_to_duckdb(mock_db, registry=fresh_registry)

        rows = mock_db.execute(
            "SELECT metric_type, value FROM app.metrics WHERE metric_name = ?",
            ["test_gauge"],
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "gauge"
        assert abs(rows[0][1] - 0.78) < 0.001

    @pytest.mark.unit
    def test_flush_histogram(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Histogram should flush with bucket bounds and counts."""
        from moneybin.metrics.persistence import flush_to_duckdb

        hist = Histogram(
            "test_hist",
            "A test histogram",
            ["op"],
            buckets=[0.1, 0.5, 1.0],
            registry=fresh_registry,
        )
        hist.labels(op="query").observe(0.3)
        hist.labels(op="query").observe(0.7)

        flush_to_duckdb(mock_db, registry=fresh_registry)

        rows = mock_db.execute(
            "SELECT metric_type, value, bucket_bounds, bucket_counts FROM app.metrics "
            "WHERE metric_name = ?",
            ["test_hist"],
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "histogram"
        assert rows[0][1] == pytest.approx(1.0, abs=0.01)  # sum of 0.3 + 0.7
        assert rows[0][2] is not None  # bucket_bounds
        assert rows[0][3] is not None  # bucket_counts


class TestLoadFromDuckDB:
    """Tests for load_from_duckdb."""

    @pytest.mark.unit
    def test_load_restores_counter(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Counter should be restored from last snapshot."""
        from moneybin.metrics.persistence import flush_to_duckdb, load_from_duckdb

        counter = Counter(
            "test_restore_counter",
            "Restorable counter",
            ["op"],
            registry=fresh_registry,
        )
        counter.labels(op="write").inc(10)

        flush_to_duckdb(mock_db, registry=fresh_registry)

        # Create a new registry to simulate restart
        new_registry = CollectorRegistry()
        new_counter = Counter(
            "test_restore_counter",
            "Restorable counter",
            ["op"],
            registry=new_registry,
        )

        load_from_duckdb(mock_db, registry=new_registry)

        # Counter should be restored
        value = new_registry.get_sample_value(
            "test_restore_counter_total", {"op": "write"}
        )
        assert value == 10.0

    @pytest.mark.unit
    def test_load_does_not_restore_gauge(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Gauges are point-in-time; should NOT be restored."""
        from moneybin.metrics.persistence import flush_to_duckdb, load_from_duckdb

        gauge = Gauge("test_restore_gauge", "Restorable gauge", registry=fresh_registry)
        gauge.set(42.0)

        flush_to_duckdb(mock_db, registry=fresh_registry)

        new_registry = CollectorRegistry()
        Gauge("test_restore_gauge", "Restorable gauge", registry=new_registry)

        load_from_duckdb(mock_db, registry=new_registry)

        value = new_registry.get_sample_value("test_restore_gauge", {})
        assert value == 0.0  # Not restored — gauge starts at 0

    @pytest.mark.unit
    def test_load_empty_table_is_noop(
        self, fresh_registry: CollectorRegistry, mock_db: MagicMock
    ) -> None:
        """Loading from empty metrics table should not raise."""
        from moneybin.metrics.persistence import load_from_duckdb

        load_from_duckdb(mock_db, registry=fresh_registry)
        # No error raised
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_metrics/test_persistence.py -v`
Expected: FAIL — module not found.

- [ ] **Step 5: Implement persistence**

Create `src/moneybin/metrics/persistence.py`:

```python
"""Metrics persistence: flush prometheus metrics to DuckDB, load on startup.

Flush strategy:
- On shutdown (atexit) — primary persistence path.
- Periodic (every 5 min) — for long-running processes (MCP server).
- Each flush appends a new snapshot row per metric.

Load strategy:
- Counters — cumulative, restored from last snapshot.
- Gauges — point-in-time, NOT restored.
- Histograms — bucket counts restored for cross-session percentiles.
"""

import json
import logging
from datetime import datetime, timezone

from prometheus_client import CollectorRegistry
from prometheus_client import Histogram as PromHistogram
from prometheus_client.metrics import MetricWrapperBase

logger = logging.getLogger(__name__)


def flush_to_duckdb(
    db: object,
    *,
    registry: CollectorRegistry | None = None,
) -> None:
    """Serialize all metrics from the prometheus registry to app.metrics.

    Each metric+label combination becomes one row with a snapshot timestamp.

    Args:
        db: Database instance (must have an ``execute()`` method).
        registry: Prometheus registry to read from. Defaults to the
            global REGISTRY.
    """
    from prometheus_client import REGISTRY

    reg = registry or REGISTRY
    now = datetime.now(tz=timezone.utc)

    rows_written = 0
    for metric in reg.collect():
        # Skip internal prometheus metrics
        if metric.name.startswith(("python_", "process_")):
            continue

        for sample in metric.samples:
            name = sample.name
            labels = sample.labels

            # Determine metric type from the sample name suffix
            if name.endswith("_total"):
                metric_type = "counter"
                # Use the base name without _total for consistency
                base_name = name[: -len("_total")]
            elif name.endswith("_bucket"):
                # Skip individual bucket samples — we handle histograms below
                continue
            elif name.endswith("_count"):
                # Skip _count samples (part of histogram/summary)
                continue
            elif name.endswith("_sum"):
                # This is the histogram sum — record the full histogram
                metric_type = "histogram"
                base_name = name[: -len("_sum")]
            elif name.endswith("_created"):
                continue
            else:
                metric_type = "gauge"
                base_name = name

            # For histograms, gather bucket data
            bucket_bounds = None
            bucket_counts = None

            if metric_type == "histogram":
                # Find matching bucket samples
                bounds = []
                counts = []
                # Filter labels: remove 'le' for matching
                base_labels = {k: v for k, v in labels.items() if k != "le"}
                for s in metric.samples:
                    if s.name == f"{base_name}_bucket":
                        s_labels = {k: v for k, v in s.labels.items() if k != "le"}
                        if s_labels == base_labels:
                            le = s.labels.get("le", "")
                            if le != "+Inf":
                                bounds.append(float(le))
                            counts.append(int(s.value))
                if bounds:
                    bucket_bounds = bounds
                    bucket_counts = counts

            labels_json = json.dumps(labels) if labels else "{}"

            try:
                # DuckDB needs array literals for DOUBLE[] and BIGINT[]
                db.execute(  # type: ignore[union-attr]
                    """
                    INSERT INTO app.metrics
                        (metric_name, metric_type, labels, value,
                         bucket_bounds, bucket_counts, recorded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        base_name,
                        metric_type,
                        labels_json,
                        sample.value,
                        bucket_bounds,
                        bucket_counts,
                        now,
                    ],
                )
                rows_written += 1
            except Exception:
                logger.debug(f"Failed to flush metric {base_name}", exc_info=True)

    logger.debug(f"Flushed {rows_written} metric rows to app.metrics")


def load_from_duckdb(
    db: object,
    *,
    registry: CollectorRegistry | None = None,
) -> None:
    """Restore counter values from the most recent snapshot in app.metrics.

    Gauges are NOT restored (they reflect current state).
    Histograms: bucket restoration is deferred to a future task.

    Args:
        db: Database instance (must have an ``execute()`` method).
        registry: Prometheus registry to restore into. Defaults to the
            global REGISTRY.
    """
    from prometheus_client import REGISTRY

    reg = registry or REGISTRY

    try:
        rows = db.execute(  # type: ignore[union-attr]
            """
            SELECT metric_name, metric_type, labels, value
            FROM app.metrics
            WHERE (metric_name, labels, recorded_at) IN (
                SELECT metric_name, labels, MAX(recorded_at)
                FROM app.metrics
                GROUP BY metric_name, labels
            )
            """
        ).fetchall()
    except Exception:
        logger.debug("No metrics table found or empty — skipping restore")
        return

    if not rows:
        return

    # Build a lookup of registered metrics by name
    metric_lookup: dict[str, MetricWrapperBase] = {}
    for collector in list(reg._names_to_collectors.values()):
        if isinstance(collector, MetricWrapperBase):
            # Counters register with _total suffix but describe with base name
            metric_lookup[collector._name] = collector

    restored = 0
    for metric_name, metric_type, labels_json, value in rows:
        if metric_type != "counter":
            continue  # Only restore counters

        # Counter names in registry don't have _total suffix
        collector = metric_lookup.get(metric_name)
        if collector is None:
            # Try with _total stripped (flush stores with _total)
            collector = metric_lookup.get(
                metric_name[: -len("_total")]
                if metric_name.endswith("_total")
                else metric_name
            )
        if collector is None:
            logger.debug(f"No registered metric for {metric_name}, skipping")
            continue

        labels = json.loads(labels_json) if labels_json else {}
        try:
            collector.labels(**labels).inc(value)
            restored += 1
        except Exception:
            logger.debug(f"Failed to restore {metric_name}", exc_info=True)

    logger.debug(f"Restored {restored} counter(s) from app.metrics")
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_metrics/test_persistence.py -v`
Expected: PASS (all 6 tests)

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/sql/schema/app_metrics.sql src/moneybin/schema.py src/moneybin/metrics/persistence.py tests/moneybin/test_metrics/test_persistence.py
git commit -m "feat(observability): add metrics persistence (flush/load to DuckDB)"
```

---

## Task 8: Create setup_observability() Facade

**Files:**
- Create: `src/moneybin/observability.py`
- Create: `tests/moneybin/test_observability.py`

- [ ] **Step 1: Write tests for setup_observability**

Create `tests/moneybin/test_observability.py`:

```python
"""Tests for the observability facade."""

import logging
from collections.abc import Generator
from typing import Any
from unittest.mock import patch

import pytest


class TestSetupObservability:
    """Tests for setup_observability()."""

    @pytest.fixture(autouse=True)
    def _reset_root_logger(self) -> Generator[None, Any, None]:
        """Clean up handlers after each test."""
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
        with patch("moneybin.observability.atexit") as mock_atexit:
            from moneybin.observability import setup_observability

            setup_observability(stream="cli")
            mock_atexit.register.assert_called_once()

    @pytest.mark.unit
    def test_public_api_exports(self) -> None:
        """The observability module should export tracked and track_duration."""
        from moneybin.observability import setup_observability, track_duration, tracked

        assert callable(setup_observability)
        assert callable(tracked)
        assert callable(track_duration)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_observability.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement setup_observability**

Create `src/moneybin/observability.py`:

```python
"""Observability facade for MoneyBin.

This module is the single public entry point for all observability setup.
Consumers use:

    from moneybin.observability import setup_observability, tracked, track_duration

Standard Python logging remains unchanged:

    import logging
    logger = logging.getLogger(__name__)

Internal modules (``moneybin.logging``, ``moneybin.metrics``) should not
be imported directly by application code except for manual gauge/counter
access.
"""

import atexit
import logging

from moneybin.logging.config import setup_logging
from moneybin.metrics.instruments import track_duration, tracked

logger = logging.getLogger(__name__)

__all__ = ["setup_observability", "tracked", "track_duration"]


def setup_observability(
    stream: str = "cli",
    verbose: bool = False,
    profile: str | None = None,
) -> None:
    """Initialize logging and metrics for the application.

    This should be called once at application startup:

        # CLI (main.py callback)
        setup_observability(stream="cli", verbose=verbose)

        # MCP server
        setup_observability(stream="mcp")

        # SQLMesh transforms
        setup_observability(stream="sqlmesh")

    What it does:
        1. Calls setup_logging() — handlers, formatters, sanitizer
        2. Registers atexit handler for metrics flush on shutdown
        3. For MCP stream: starts periodic flush timer (every 5 min)

    Args:
        stream: Log stream name ("cli", "mcp", "sqlmesh").
        verbose: Enable DEBUG level logging.
        profile: Profile name (unused — set via set_current_profile before).
    """
    # Step 1: Configure logging
    setup_logging(stream=stream, verbose=verbose, profile=profile)

    # Step 2: Register atexit handler for metrics flush
    atexit.register(_flush_metrics_on_exit)

    # Step 3: For MCP, start periodic flush
    if stream == "mcp":
        _start_periodic_flush()

    logger.debug(f"Observability initialized (stream={stream})")


def _flush_metrics_on_exit() -> None:
    """Flush all metrics to DuckDB on process exit.

    This is best-effort — if the database is unavailable, metrics are lost
    for this session (they'll be re-accumulated on next run).
    """
    try:
        from moneybin.database import get_database
        from moneybin.metrics.persistence import flush_to_duckdb

        db = get_database()
        flush_to_duckdb(db)
    except Exception:
        logger.debug("Metrics flush on exit failed", exc_info=True)


_periodic_timer = None


def _start_periodic_flush(interval_seconds: int = 300) -> None:
    """Start a background timer that flushes metrics every interval.

    Args:
        interval_seconds: Seconds between flushes (default: 300 = 5 min).
    """
    import threading

    global _periodic_timer

    def _flush_and_reschedule() -> None:
        global _periodic_timer
        _flush_metrics_on_exit()
        _periodic_timer = threading.Timer(interval_seconds, _flush_and_reschedule)
        _periodic_timer.daemon = True
        _periodic_timer.start()

    _periodic_timer = threading.Timer(interval_seconds, _flush_and_reschedule)
    _periodic_timer.daemon = True
    _periodic_timer.start()
    logger.debug(f"Periodic metrics flush started (every {interval_seconds}s)")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_observability.py -v`
Expected: PASS (all 3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/observability.py tests/moneybin/test_observability.py
git commit -m "feat(observability): add setup_observability() facade"
```

---

## Task 9: Wire Up CLI and MCP to setup_observability

**Files:**
- Modify: `src/moneybin/cli/main.py`
- Modify: `src/moneybin/cli/commands/mcp.py`

- [ ] **Step 1: Update CLI main.py**

In `src/moneybin/cli/main.py`, change the import and the call.

Replace import line:
```python
from ..logging import setup_logging
```
with:
```python
from ..observability import setup_observability
```

Replace the call on line 70:
```python
    setup_logging(cli_mode=True, verbose=verbose, profile=profile_name)
```
with:
```python
    setup_observability(stream="cli", verbose=verbose, profile=profile_name)
```

- [ ] **Step 2: Update MCP serve command**

In `src/moneybin/cli/commands/mcp.py`, add `setup_observability` call before `init_db()` in the `serve()` function. After the `db_path` assignment and before `try: init_db()`:

```python
    from moneybin.observability import setup_observability

    setup_observability(stream="mcp")
```

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/moneybin/cli/main.py src/moneybin/cli/commands/mcp.py
git commit -m "refactor(observability): wire CLI and MCP to setup_observability()"
```

---

## Task 10: Implement `moneybin stats` Command

**Files:**
- Create: `src/moneybin/cli/commands/stats.py`
- Modify: `src/moneybin/cli/commands/stubs.py` (remove stats_app)
- Modify: `src/moneybin/cli/main.py` (import from stats instead of stubs)
- Create: `tests/moneybin/test_stats_command.py`

- [ ] **Step 1: Write tests for the stats command**

Create `tests/moneybin/test_stats_command.py`:

```python
"""Tests for the moneybin stats CLI command."""

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.stats import app as stats_app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


class TestStatsShow:
    """Tests for the stats show command."""

    @pytest.mark.unit
    def test_show_with_empty_metrics(self, runner: CliRunner) -> None:
        """Should display zeros when no metrics exist."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        with patch("moneybin.cli.commands.stats.get_database", return_value=mock_db):
            result = runner.invoke(stats_app, ["show"])

        assert result.exit_code == 0
        assert "Import Records" in result.output or "No metrics" in result.output

    @pytest.mark.unit
    def test_show_json_output(self, runner: CliRunner) -> None:
        """--output json should return valid JSON."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        with patch("moneybin.cli.commands.stats.get_database", return_value=mock_db):
            result = runner.invoke(stats_app, ["show", "--output", "json"])

        assert result.exit_code == 0
        parsed = json.loads(result.output)
        assert isinstance(parsed, dict)

    @pytest.mark.unit
    def test_show_with_since_filter(self, runner: CliRunner) -> None:
        """--since should filter metrics by time window."""
        mock_db = MagicMock()
        mock_db.execute.return_value.fetchall.return_value = []

        with patch("moneybin.cli.commands.stats.get_database", return_value=mock_db):
            result = runner.invoke(stats_app, ["show", "--since", "7d"])

        assert result.exit_code == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/moneybin/test_stats_command.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement the stats command**

Create `src/moneybin/cli/commands/stats.py`:

```python
"""Stats command for MoneyBin CLI.

Displays lifetime metric aggregates from the app.metrics table.
"""

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Annotated

import typer

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Show lifetime metric aggregates",
    no_args_is_help=True,
)


def _parse_duration(duration: str) -> timedelta:
    """Parse a duration string like '30d', '7d', '24h' into a timedelta.

    Args:
        duration: Duration string (e.g., "30d", "7d", "24h", "60m").

    Returns:
        timedelta for the specified duration.

    Raises:
        ValueError: If format is invalid.
    """
    match = re.match(r"^(\d+)([dhm])$", duration.strip())
    if not match:
        raise ValueError(
            f"Invalid duration format: '{duration}'. Use <number><unit> "
            "where unit is d (days), h (hours), or m (minutes)."
        )
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "d":
        return timedelta(days=value)
    elif unit == "h":
        return timedelta(hours=value)
    else:
        return timedelta(minutes=value)


@app.command("show")
def stats_show(
    since: Annotated[
        str | None,
        typer.Option("--since", help="Time window (e.g., 7d, 24h)"),
    ] = None,
    metric: Annotated[
        str | None,
        typer.Option("--metric", help="Filter to a metric family (e.g., import)"),
    ] = None,
    output: Annotated[
        str,
        typer.Option("--output", help="Output format: text or json"),
    ] = "text",
) -> None:
    """Display lifetime metric aggregates."""
    from moneybin.database import DatabaseKeyError, get_database

    try:
        db = get_database()
    except DatabaseKeyError as e:
        logger.error(f"❌ Database is locked: {e}")
        typer.echo(
            "💡 Run 'moneybin db unlock' to unlock the database first.",
            err=True,
        )
        raise typer.Exit(1) from e

    # Build query with optional filters
    where_clauses = []
    params: list[object] = []

    if since:
        try:
            delta = _parse_duration(since)
        except ValueError as e:
            logger.error(f"❌ {e}")
            raise typer.Exit(1) from e
        cutoff = datetime.now() - delta
        where_clauses.append("recorded_at >= ?")
        params.append(cutoff)

    if metric:
        where_clauses.append("metric_name LIKE ?")
        params.append(f"%{metric}%")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    try:
        rows = db.execute(
            f"""
            SELECT metric_name, metric_type,
                   SUM(value) as total_value,
                   COUNT(*) as snapshot_count,
                   MAX(recorded_at) as last_recorded
            FROM app.metrics
            {where_sql}
            GROUP BY metric_name, metric_type
            ORDER BY metric_name
            """,  # noqa: S608 — where_sql is built from validated fragments, not user input
            params if params else None,
        ).fetchall()
    except Exception:
        rows = []

    if output == "json":
        result = {
            "metrics": [
                {
                    "name": row[0],
                    "type": row[1],
                    "value": row[2],
                    "snapshots": row[3],
                    "last_recorded": row[4].isoformat() if row[4] else None,
                }
                for row in rows
            ]
        }
        typer.echo(json.dumps(result, indent=2))
        return

    if not rows:
        logger.info("No metrics recorded yet. Run some operations first.")
        return

    # Human-readable output
    for row in rows:
        name, metric_type, value, count, last = row
        display_name = name.replace("moneybin_", "").replace("_", " ").title()
        if metric_type == "counter":
            logger.info(f"{display_name}: {value:,.0f} total")
        elif metric_type == "gauge":
            logger.info(f"{display_name}: {value:.2f}")
        elif metric_type == "histogram":
            logger.info(f"{display_name}: {count} observations (sum={value:.2f}s)")
```

- [ ] **Step 4: Remove stats stub and wire up real command**

In `src/moneybin/cli/commands/stubs.py`, delete the `stats_app` definition and the `stats_show` command (lines 133-141).

In `src/moneybin/cli/main.py`:

Change the import from stubs — remove `stats_app`:
```python
from .commands.stubs import (
    db_migrate_app,
    export_app,
    matches_app,
    track_app,
)
```

Add import for the real stats command:
```python
from .commands import (
    categorize,
    db,
    import_cmd,
    logs,
    mcp,
    profile,
    stats,
    sync,
    transform,
)
```

Change the `stats_app` registration:
```python
app.add_typer(stats.app, name="stats", help="Show lifetime metric aggregates")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/moneybin/test_stats_command.py -v`
Expected: PASS (all 3 tests)

- [ ] **Step 6: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/moneybin/cli/commands/stats.py src/moneybin/cli/commands/stubs.py src/moneybin/cli/main.py tests/moneybin/test_stats_command.py
git commit -m "feat(observability): implement moneybin stats command"
```

---

## Task 11: Update logs Command for Stream-Based Routing

**Files:**
- Modify: `src/moneybin/cli/commands/logs.py`

The `logs tail` command needs to find the stream-specific daily log files (e.g., `cli_2026-04-21.log`) instead of looking at a single `moneybin.log` file. The `logs clean` command should scan subdirectories too.

- [ ] **Step 1: Update logs_tail to find stream-specific files**

In `src/moneybin/cli/commands/logs.py`, replace the `logs_tail` function:

```python
@app.command("tail")
def logs_tail(
    stream: str | None = typer.Option(
        None, "--stream", help="Stream to tail: cli (default), mcp, sqlmesh"
    ),
    follow: bool = typer.Option(False, "-f", "--follow", help="Follow log output"),
    lines: int = typer.Option(20, "-n", "--lines", help="Number of lines to show"),
) -> None:
    """Show recent log entries, optionally following new output."""
    settings = get_settings()
    log_dir = settings.logging.log_file_path.parent

    if not log_dir.exists():
        logger.info(f"No log directory found: {log_dir}")
        return

    # Find the most recent log file for the requested stream
    stream_prefix = (stream or "cli").lower()
    log_files = sorted(
        log_dir.glob(f"{stream_prefix}_*.log"),
        key=lambda p: p.name,
        reverse=True,
    )

    if not log_files:
        logger.info(f"No log files found for stream '{stream_prefix}' in {log_dir}")
        return

    log_path = log_files[0]  # Most recent by name (date-sorted)

    tail_buf: deque[str] = deque(maxlen=lines)
    with open(log_path) as f:
        for line in f:
            tail_buf.append(line)

    for line in tail_buf:
        typer.echo(line.rstrip())

    if follow:
        typer.echo("--- Following (Ctrl+C to stop) ---")
        try:
            with open(log_path) as f:
                f.seek(0, 2)
                while True:
                    line = f.readline()
                    if line:
                        typer.echo(line.rstrip())
                    else:
                        time.sleep(0.5)
        except KeyboardInterrupt:
            pass
```

- [ ] **Step 2: Update logs_clean to scan all log files**

In `src/moneybin/cli/commands/logs.py`, update the `logs_clean` function to also scan for `*.log` files (since files are now flat in the log directory, not in date subdirectories):

The existing implementation already works — it iterates `log_dir.iterdir()` and checks file modification times. The only change needed is to make sure it handles the new flat file naming pattern correctly. Since the existing code already handles flat files in the directory, no changes are needed to `logs_clean`.

- [ ] **Step 3: Update logs_path to reflect new layout**

No change needed — `logs_path` already prints `settings.logging.log_file_path.parent`, which is the profile log directory.

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/moneybin/cli/commands/logs.py
git commit -m "refactor(observability): update logs tail for stream-based daily files"
```

---

## Task 12: Lint, Type Check, and Final Verification

**Files:**
- All modified files

- [ ] **Step 1: Run formatter**

Run: `uv run ruff format .`

- [ ] **Step 2: Run linter**

Run: `uv run ruff check .`
Fix any issues found.

- [ ] **Step 3: Run type checker on modified files**

Run: `uv run pyright src/moneybin/observability.py src/moneybin/logging/config.py src/moneybin/logging/formatters.py src/moneybin/metrics/registry.py src/moneybin/metrics/instruments.py src/moneybin/metrics/persistence.py src/moneybin/cli/commands/stats.py`
Fix any type errors.

- [ ] **Step 4: Run full test suite**

Run: `make check test`
Expected: All checks pass, all tests pass.

- [ ] **Step 5: Commit any fixes**

```bash
git add -u
git commit -m "chore(observability): fix lint and type errors"
```

---

## Task 13: Update Spec Status and Documentation

**Files:**
- Modify: `docs/specs/observability.md` (status → in-progress or implemented)
- Modify: `docs/specs/INDEX.md` (update status)

- [ ] **Step 1: Update spec status to implemented**

In `docs/specs/observability.md`, change:
```
> Status: ready
```
to:
```
> Status: implemented
```

- [ ] **Step 2: Update INDEX.md**

Update the observability entry in `docs/specs/INDEX.md` to show `implemented` status.

- [ ] **Step 3: Move spec to archived**

```bash
mv docs/specs/observability.md docs/specs/archived/observability.md
```

Update the link in `INDEX.md` to point to `archived/observability.md`.

- [ ] **Step 4: Update README.md roadmap**

In `README.md`, change the observability roadmap entry icon from 📐 to ✅.

- [ ] **Step 5: Commit**

```bash
git add docs/specs/ README.md
git commit -m "docs: mark observability spec as implemented"
```

---

## Execution Notes

**Total tasks:** 13
**Estimated commits:** ~13

**Task dependencies:**
- Task 1 (deps) must come first
- Task 2 (config) before Task 4 (setup_logging rewrite)
- Task 3 (formatters) before Task 4 (setup_logging uses them)
- Task 5 (registry) before Task 6 (instruments) before Task 7 (persistence)
- Tasks 5-7 before Task 8 (observability facade)
- Task 8 before Task 9 (wiring)
- Task 10 (stats CLI) depends on Task 7 (persistence) for querying
- Task 11 (logs update) depends on Task 4 (new file naming)
- Task 12 (lint/type) after all code tasks
- Task 13 (docs) last

**Parallelizable:** Tasks 3 and 5 can run in parallel (formatters and registry are independent).

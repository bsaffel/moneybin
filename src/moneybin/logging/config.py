"""Logging configuration for MoneyBin.

This module provides ``setup_logging()`` which configures Python's logging
system. It is called internally by ``setup_observability()`` — application
code should not call it directly.

``setup_logging()`` does not call ``get_settings()``. All configuration
values are passed explicitly by the caller, decoupling logging setup from
profile resolution and settings loading.
"""

import logging
import stat as stat_mod
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

from moneybin.log_sanitizer import SanitizedLogFormatter
from moneybin.logging.formatters import HumanFormatter, JSONFormatter


class _SuppressFilter(logging.Filter):
    """Filter out noisy SQLMesh analytics shutdown messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "Shutting down the event dispatcher" not in record.getMessage()


# SQLMesh loggers whose INFO output is too noisy for the console but
# should still reach log files for debugging.
_CONSOLE_SUPPRESSED_LOGGERS = frozenset({
    "sqlmesh.core.state_sync.db.migrator",
    "sqlmesh.core.config.connection",
})


class _ConsoleNoiseFilter(logging.Filter):
    """Suppress noisy third-party INFO messages from the console only.

    Attached to the console handler so file handlers still see everything.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if (
            record.name in _CONSOLE_SUPPRESSED_LOGGERS
            and record.levelno < logging.WARNING
        ):
            return False
        return True


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


def _make_file_handler(
    log_file: Path, formatter: logging.Formatter
) -> logging.FileHandler:
    """Create a file handler with restrictive permissions.

    Args:
        log_file: Path to the log file.
        formatter: Formatter to apply (will be wrapped in SanitizedLogFormatter).

    Returns:
        Configured FileHandler.
    """
    handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    if sys.platform != "win32":
        try:
            log_file.chmod(stat_mod.S_IRUSR | stat_mod.S_IWUSR)  # 0600
        except OSError:
            pass
    handler.setFormatter(SanitizedLogFormatter(formatter))
    return handler


def _setup_sqlmesh_file_handler(
    log_file_path: Path, formatter: logging.Formatter
) -> None:
    """Route SQLMesh logger output to a dedicated sqlmesh log file.

    Per the observability spec, SQLMesh output goes to
    ``sqlmesh_YYYY-MM-DD.log`` (file only — suppressed from console).
    This attaches a file handler directly to the SQLMesh loggers so their
    output reaches the sqlmesh stream log regardless of the active CLI stream.

    Args:
        log_file_path: Base log file path (used to derive the sqlmesh log path).
        formatter: Formatter to apply to the file handler.
    """
    sqlmesh_log = session_log_path(log_file_path, prefix="sqlmesh")
    handler = _make_file_handler(sqlmesh_log, formatter)

    for logger_name in _CONSOLE_SUPPRESSED_LOGGERS:
        sqlmesh_logger = logging.getLogger(logger_name)
        # Avoid duplicate handlers on reconfiguration
        if not any(
            isinstance(h, logging.FileHandler)
            and getattr(h, "baseFilename", None) == str(sqlmesh_log.resolve())
            for h in sqlmesh_logger.handlers
        ):
            sqlmesh_logger.addHandler(handler)


def setup_logging(
    stream: Literal["cli", "mcp", "sqlmesh"] = "cli",
    verbose: bool = False,
    *,
    level: str = "INFO",
    log_format: Literal["human", "json"] = "human",
    log_to_file: bool = False,
    log_file_path: Path | None = None,
) -> None:
    """Set up centralized logging configuration.

    All parameters are passed explicitly — this function does not call
    ``get_settings()``. The caller (``setup_observability()``) resolves
    log settings from the profile config and passes them here.

    Args:
        stream: Log stream name — determines file prefix and console format.
            "cli" uses message-only console format; "mcp" and "sqlmesh" use
            full format with timestamps.
        verbose: If True, enable DEBUG level logging (overrides ``level``).
        level: Log level name (e.g. "INFO", "DEBUG"). Ignored when
            ``verbose`` is True.
        log_format: Log output format: "human" or "json".
        log_to_file: Whether to write logs to a file.
        log_file_path: Path used to derive the daily log file location.
            Required when ``log_to_file`` is True.
    """
    # Determine log level
    if verbose:
        resolved_level = logging.DEBUG
    else:
        resolved_level = getattr(logging, level)

    # Build inner formatter based on config
    if log_format == "json":
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
    console_handler.setFormatter(SanitizedLogFormatter(console_formatter))
    console_handler.addFilter(_ConsoleNoiseFilter())
    handlers.append(console_handler)

    # File handler — only write to file if the log directory's parent exists.
    # Profile directories are created by ProfileService.create(), not here.
    # Using parents=True would silently recreate a deleted profile's tree.
    if log_to_file and log_file_path is not None:
        log_file: Path | None = session_log_path(log_file_path, prefix=stream)
        try:
            log_file.parent.mkdir(parents=False, exist_ok=True)
        except FileNotFoundError:
            log_file = None  # Profile dir doesn't exist; skip file logging

        if log_file is not None:
            file_handler = _make_file_handler(log_file, inner_formatter)
            handlers.append(file_handler)

            # Dedicated sqlmesh log file — SQLMesh output is noisy so it
            # gets its own stream file per the observability spec. The
            # console filter already suppresses these from stderr; this
            # ensures they still reach a log file for debugging.
            _setup_sqlmesh_file_handler(log_file_path, inner_formatter)

    # Configure root logger
    logging.basicConfig(
        level=resolved_level,
        handlers=handlers,
        force=True,
    )

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("plaid").setLevel(logging.INFO)
    logging.getLogger("sqlmesh.core.analytics.dispatcher").setLevel(logging.WARNING)

    # Suppress SQLMesh analytics shutdown message (guard against duplicates)
    if not any(isinstance(f, _SuppressFilter) for f in logging.root.filters):
        logging.root.addFilter(_SuppressFilter())

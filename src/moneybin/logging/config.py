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


class _SuppressFilter(logging.Filter):
    """Filter out noisy SQLMesh analytics shutdown messages."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "Shutting down the event dispatcher" not in record.getMessage()


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
    cli_mode: bool | None = None,
    config: object | None = None,
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
        cli_mode: Deprecated — use ``stream="cli"`` instead. Accepted for
            backward compatibility until callers are migrated.
        config: Deprecated — ignored. Configuration is read from
            ``get_settings().logging``.
    """
    # Backward compatibility: map cli_mode to stream
    if cli_mode is not None:
        stream = "cli" if cli_mode else "mcp"
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

    # Suppress SQLMesh analytics shutdown message (guard against duplicates)
    if not any(isinstance(f, _SuppressFilter) for f in logging.root.filters):
        logging.root.addFilter(_SuppressFilter())

"""Logging configuration management for MoneyBin application.

This module provides centralized logging configuration that can be used across
all MoneyBin components, with support for different environments and use cases.
"""

import logging
import os
import stat as stat_mod
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from moneybin.log_sanitizer import SanitizedLogFormatter


@dataclass
class LoggingConfig:
    """Configuration settings for application logging."""

    level: str = "INFO"
    format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    cli_format_string: str = "%(message)s"
    log_to_file: bool = True
    log_file_path: Path = Path("logs/test/moneybin.log")  # Default to test profile
    max_file_size_mb: int = 50
    backup_count: int = 5
    force_reconfigure: bool = False

    @classmethod
    def from_environment(cls, profile: str | None = None) -> "LoggingConfig":
        """Create logging configuration from environment variables.

        Args:
            profile: Optional profile name for default log path. If None, uses 'test'.

        Returns:
            LoggingConfig: Configuration loaded from environment
        """
        from moneybin.config import get_base_dir
        from moneybin.utils.user_config import normalize_profile_name

        # Get profile from parameter or environment or default to 'test'
        if profile is None:
            profile = os.getenv("MONEYBIN_PROFILE", "test")

        # Normalize profile name BEFORE using it for paths
        profile = normalize_profile_name(profile)

        # Resolve log path against the base directory
        base = get_base_dir()
        env_log_path = os.getenv("LOG_FILE_PATH")
        if env_log_path:
            p = Path(env_log_path)
            log_file_path = p if p.is_absolute() else base / p
        else:
            log_file_path = base / f"logs/{profile}/moneybin.log"

        return cls(
            level=os.getenv("LOG_LEVEL", "INFO").upper(),
            log_to_file=os.getenv("LOG_TO_FILE", "true").lower() == "true",
            log_file_path=log_file_path,
            max_file_size_mb=int(os.getenv("LOG_MAX_FILE_SIZE_MB", "50")),
            backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5")),
        )


def session_log_path(
    configured_path: Path,
    prefix: str = "moneybin",
    now: datetime | None = None,
) -> Path:
    """Derive a daily/session log path from the configured log file path.

    Transforms ``logs/{profile}/moneybin.log`` into
    ``logs/{profile}/YYYY-MM-DD/{prefix}_HH_MM_SS.log`` so that logs are
    grouped by day and each application session gets its own file.

    Args:
        configured_path: The log_file_path from configuration (used to find
            the profile log directory).
        prefix: Filename prefix (e.g. "moneybin", "sqlmesh").
        now: Timestamp to use for the path; defaults to the current time.
            Pass a fixed value in tests to avoid timing coupling.

    Returns:
        Path to the session-specific log file.
    """
    if now is None:
        now = datetime.now()
    profile_log_dir = configured_path.parent
    daily_dir = profile_log_dir / now.strftime("%Y-%m-%d")
    return daily_dir / f"{prefix}_{now.strftime('%H_%M_%S')}.log"


def setup_logging(
    config: LoggingConfig | None = None,
    cli_mode: bool = False,
    verbose: bool = False,
    profile: str | None = None,
) -> None:
    """Set up centralized logging configuration for the application.

    Args:
        config: Optional logging configuration. If None, loads from environment.
        cli_mode: If True, use simplified CLI-friendly formatting
        verbose: If True, enable DEBUG level logging (overrides config level)
        profile: Optional profile name for log file paths
    """
    if config is None:
        config = LoggingConfig.from_environment(profile=profile)

    # Override level if verbose is requested
    if verbose:
        level = logging.DEBUG
    else:
        level = getattr(logging, config.level)

    # Prepare handlers
    handlers: list[logging.Handler] = []

    # Console handler (always present)
    console_handler = logging.StreamHandler(sys.stderr)
    if cli_mode:
        console_handler.setFormatter(logging.Formatter(config.cli_format_string))
    else:
        console_handler.setFormatter(logging.Formatter(config.format_string))
    handlers.append(console_handler)

    # File handler (if enabled)
    if config.log_to_file:
        log_file = session_log_path(config.log_file_path, prefix="moneybin")
        log_file.parent.mkdir(parents=True, exist_ok=True)

        # Set restrictive permissions on log file (macOS/Linux)
        if sys.platform != "win32" and log_file.exists():
            try:
                log_file.chmod(stat_mod.S_IRUSR | stat_mod.S_IWUSR)  # 0600
            except OSError:
                pass

        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_handler.setFormatter(SanitizedLogFormatter(config.format_string))
        handlers.append(file_handler)

    # Configure root logger
    logging.basicConfig(
        level=level,
        handlers=handlers,
        force=config.force_reconfigure,
    )

    # Set specific logger levels for third-party libraries to reduce noise
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("plaid").setLevel(logging.INFO)
    logging.getLogger("sqlmesh.core.analytics.dispatcher").setLevel(logging.WARNING)

    # SQLMesh analytics shutdown message uses the root logger directly (not a named logger),
    # so suppress its shutdown message via a filter on all handlers.
    class _SuppressFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return "Shutting down the event dispatcher" not in record.getMessage()

    suppress = _SuppressFilter()
    logging.root.addFilter(suppress)


def setup_dagster_logging(profile: str | None = None) -> None:
    """Set up logging configuration optimized for Dagster pipelines.

    Args:
        profile: Optional profile name for log file paths

    This function configures logging specifically for Dagster asset execution,
    with appropriate formatting and file output.
    """
    config = LoggingConfig.from_environment(profile=profile)

    # Dagster handles its own logging, but we can configure our application loggers
    setup_logging(config, cli_mode=False, verbose=False, profile=profile)

    # Configure Dagster-specific loggers if needed
    dagster_logger = logging.getLogger("dagster")
    dagster_logger.setLevel(logging.INFO)


def get_log_config_summary() -> dict[str, Any]:
    """Get a summary of current logging configuration.

    Returns:
        dict: Summary of logging configuration settings
    """
    config = LoggingConfig.from_environment()
    root_logger = logging.getLogger()

    # Read the active log path from the live handler rather than generating a new
    # timestamp, which would produce a fictitious path that was never opened.
    active_log_file = next(
        (
            h.baseFilename
            for h in root_logger.handlers
            if isinstance(h, logging.FileHandler)
        ),
        None,
    )

    return {
        "level": logging.getLevelName(root_logger.level),
        "handlers": [type(h).__name__ for h in root_logger.handlers],
        "log_to_file": config.log_to_file,
        "log_file_path": str(active_log_file) if active_log_file else None,
        "log_dir": str(config.log_file_path.parent),
        "format_string": config.format_string,
    }

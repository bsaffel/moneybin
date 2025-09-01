"""Logging configuration management for MoneyBin application.

This module provides centralized logging configuration that can be used across
all MoneyBin components, with support for different environments and use cases.
"""

import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class LoggingConfig:
    """Configuration settings for application logging."""

    level: str = "INFO"
    format_string: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    cli_format_string: str = "%(message)s"
    log_to_file: bool = True
    log_file_path: Path = Path("logs/moneybin.log")
    max_file_size_mb: int = 50
    backup_count: int = 5
    force_reconfigure: bool = False

    @classmethod
    def from_environment(cls) -> "LoggingConfig":
        """Create logging configuration from environment variables.

        Returns:
            LoggingConfig: Configuration loaded from environment
        """
        return cls(
            level=os.getenv("LOG_LEVEL", "INFO").upper(),
            log_to_file=os.getenv("LOG_TO_FILE", "true").lower() == "true",
            log_file_path=Path(os.getenv("LOG_FILE_PATH", "logs/moneybin.log")),
            max_file_size_mb=int(os.getenv("LOG_MAX_FILE_SIZE_MB", "50")),
            backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5")),
        )


def setup_logging(
    config: LoggingConfig | None = None,
    cli_mode: bool = False,
    verbose: bool = False,
) -> None:
    """Set up centralized logging configuration for the application.

    Args:
        config: Optional logging configuration. If None, loads from environment.
        cli_mode: If True, use simplified CLI-friendly formatting
        verbose: If True, enable DEBUG level logging (overrides config level)
    """
    if config is None:
        config = LoggingConfig.from_environment()

    # Override level if verbose is requested
    if verbose:
        level = logging.DEBUG
    else:
        level = getattr(logging, config.level)

    # Prepare handlers
    handlers: list[logging.Handler] = []

    # Console handler (always present)
    console_handler = logging.StreamHandler(sys.stdout)
    if cli_mode:
        console_handler.setFormatter(logging.Formatter(config.cli_format_string))
    else:
        console_handler.setFormatter(logging.Formatter(config.format_string))
    handlers.append(console_handler)

    # File handler (if enabled)
    if config.log_to_file:
        # Ensure log directory exists
        config.log_file_path.parent.mkdir(parents=True, exist_ok=True)

        from logging.handlers import RotatingFileHandler

        file_handler = RotatingFileHandler(
            config.log_file_path,
            maxBytes=config.max_file_size_mb * 1024 * 1024,
            backupCount=config.backup_count,
        )
        file_handler.setFormatter(logging.Formatter(config.format_string))
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


def setup_dagster_logging() -> None:
    """Set up logging configuration optimized for Dagster pipelines.

    This function configures logging specifically for Dagster asset execution,
    with appropriate formatting and file output.
    """
    config = LoggingConfig.from_environment()

    # Dagster handles its own logging, but we can configure our application loggers
    setup_logging(config, cli_mode=False, verbose=False)

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

    return {
        "level": logging.getLevelName(root_logger.level),
        "handlers": [type(h).__name__ for h in root_logger.handlers],
        "log_to_file": config.log_to_file,
        "log_file_path": str(config.log_file_path),
        "format_string": config.format_string,
    }

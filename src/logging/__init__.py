"""Centralized logging configuration for MoneyBin application.

This package provides unified logging configuration across all MoneyBin components,
including CLI commands, data extractors, Dagster pipelines, and utility modules.

Standard usage:
    ```python
    import logging
    from src.logging import setup_logging

    # Configure once at application startup
    setup_logging()

    # Get loggers in each module
    logger = logging.getLogger(__name__)
    ```
"""

from .config import LoggingConfig, setup_logging

__all__ = ["LoggingConfig", "setup_logging"]

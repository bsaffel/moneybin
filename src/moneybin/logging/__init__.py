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

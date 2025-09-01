"""MoneyBin CLI package.

This package provides a unified command-line interface for all MoneyBin operations,
including data extraction, credential management, and system utilities.
"""

from .main import app, main

__all__ = ["app", "main"]

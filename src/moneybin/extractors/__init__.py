"""Data extraction modules for MoneyBin financial data aggregation.

This package provides secure extractors for various financial data sources including
Plaid API, CSV files, PDF documents, and other financial institutions.
"""

from .plaid_extractor import (
    PlaidConnectionManager,
    PlaidCredentials,
    PlaidExtractionConfig,
    PlaidExtractor,
)

__all__ = [
    "PlaidExtractor",
    "PlaidConnectionManager",
    "PlaidCredentials",
    "PlaidExtractionConfig",
]

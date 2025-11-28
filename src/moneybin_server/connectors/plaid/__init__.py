"""Plaid API connector for MoneyBin Server.

This module provides server-side integration with the Plaid API for
automated bank account and transaction data synchronization.
"""

from .extractor import PlaidConnectionManager, PlaidExtractor
from .schemas import PlaidCredentials

__all__ = ["PlaidExtractor", "PlaidConnectionManager", "PlaidCredentials"]

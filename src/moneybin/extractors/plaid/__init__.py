"""Plaid provider — ingests SyncDataResponse payloads into raw.plaid_* tables."""

from moneybin.extractors.plaid.config import PlaidProviderConfig
from moneybin.extractors.plaid.extractor import PlaidExtractor

__all__ = ["PlaidExtractor", "PlaidProviderConfig"]

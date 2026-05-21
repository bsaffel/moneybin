"""Plaid provider — ingests SyncDataResponse payloads into raw.plaid_* tables.

``PlaidExtractor`` is lazy-loaded so that ``moneybin.config`` can pull in
``PlaidProviderConfig`` without dragging the extractor's transitive
``Database`` import — which would create a circular import via
``database.py``'s top-level ``from moneybin.config import get_settings``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from moneybin.extractors.plaid.config import PlaidProviderConfig

if TYPE_CHECKING:
    from moneybin.extractors.plaid.extractor import PlaidExtractor

__all__ = ["PlaidExtractor", "PlaidProviderConfig"]


def __getattr__(name: str) -> Any:
    if name == "PlaidExtractor":
        from moneybin.extractors.plaid.extractor import PlaidExtractor

        return PlaidExtractor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

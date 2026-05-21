"""Registry of available gsheet adapters."""

from __future__ import annotations

from moneybin.connectors.gsheet.adapters.base import GSheetAdapter

ADAPTERS: dict[str, GSheetAdapter] = {}  # populated by transactions + raw_seed modules

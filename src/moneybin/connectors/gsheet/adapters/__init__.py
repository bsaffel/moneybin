"""Registry of available gsheet adapters."""

from __future__ import annotations

from moneybin.connectors.gsheet.adapters.base import GSheetAdapter

ADAPTERS: dict[str, GSheetAdapter] = {}
"""Populated at import time by adapter modules (see _register_adapters)."""


def _register_adapters() -> None:
    """Import adapter modules for their ADAPTERS-registration side effects.

    Wrapped in a function so the imports are not module-level (avoids
    isort/ruff ordering complaints) and clearly named — the side effect is
    the point, the binding is discarded.
    """
    from moneybin.connectors.gsheet.adapters import (
        transactions as _transactions,  # noqa: F401
    )

    _ = _transactions


_register_adapters()

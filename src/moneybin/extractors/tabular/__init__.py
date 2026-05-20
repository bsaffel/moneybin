"""Tabular provider — ingests CSV/TSV/Excel/Parquet/Feather into raw.tabular_*.

``TabularExtractor`` and ``TabularProviderConfig`` are lazy-loaded so that
sub-module imports (e.g. ``from moneybin.extractors.tabular.formats import X``
from CLI cold-start paths) do not pull polars in transitively. Eager
loading would regress ``tests/moneybin/test_cli/test_cold_start.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from moneybin.extractors.tabular.config import TabularProviderConfig
    from moneybin.extractors.tabular.extractor import TabularExtractor

__all__ = ["TabularExtractor", "TabularProviderConfig"]


def __getattr__(name: str) -> Any:
    if name == "TabularExtractor":
        from moneybin.extractors.tabular.extractor import TabularExtractor

        return TabularExtractor
    if name == "TabularProviderConfig":
        from moneybin.extractors.tabular.config import TabularProviderConfig

        return TabularProviderConfig
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

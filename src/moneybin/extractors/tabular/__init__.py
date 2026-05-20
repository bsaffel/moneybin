"""Tabular provider — ingests CSV/TSV/Excel/Parquet/Feather into raw.tabular_*.

``TabularProviderConfig`` is eagerly exported (it only pulls pydantic +
``_types``, no polars). ``TabularExtractor`` is lazy-loaded because
sub-module imports (e.g. ``from moneybin.extractors.tabular.formats import X``
from CLI cold-start paths) would otherwise pull polars in transitively
via the extractor — eager extractor loading regresses
``tests/moneybin/test_cli/test_cold_start.py``. OFX and Plaid follow the
same convention (eager config, lazy/eager extractor as cold-start permits).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from moneybin.extractors.tabular.config import TabularProviderConfig

if TYPE_CHECKING:
    from moneybin.extractors.tabular.extractor import TabularExtractor

__all__ = ["TabularExtractor", "TabularProviderConfig"]


def __getattr__(name: str) -> Any:
    if name == "TabularExtractor":
        from moneybin.extractors.tabular.extractor import TabularExtractor

        return TabularExtractor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

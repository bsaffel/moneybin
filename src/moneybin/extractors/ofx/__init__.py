"""OFX provider — ingests OFX/QFX/QBO files into raw.ofx_* tables.

``OFXExtractor`` is lazy-loaded so that ``moneybin.config`` can pull in
``OFXProviderConfig`` without dragging the extractor's transitive polars
and ofxparse imports — load-bearing for the CLI cold-start path
(``tests/moneybin/test_cli/test_cold_start.py``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from moneybin.extractors.ofx.config import OFXProviderConfig

if TYPE_CHECKING:
    from moneybin.extractors.ofx.extractor import OFXExtractor

__all__ = ["OFXExtractor", "OFXProviderConfig"]


def __getattr__(name: str) -> Any:
    if name == "OFXExtractor":
        from moneybin.extractors.ofx.extractor import OFXExtractor

        return OFXExtractor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

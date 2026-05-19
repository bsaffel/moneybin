"""Negative-import tests: verify that deleted modules are unreachable."""

from __future__ import annotations

import pytest


def test_w2_modules_removed() -> None:
    """Confirm that the W-2 extractor and loader are fully removed."""
    with pytest.raises(ModuleNotFoundError):
        import moneybin.extractors.w2_extractor  # type: ignore[import]  # noqa: F401
    with pytest.raises(ModuleNotFoundError):
        import moneybin.loaders.w2_loader  # type: ignore[import]  # noqa: F401

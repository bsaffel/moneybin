"""Negative-import tests: verify that deleted modules are unreachable."""

from __future__ import annotations

import pytest


@pytest.mark.unit
def test_w2_modules_removed() -> None:
    """Confirm that the W-2 extractor is unreachable.

    The companion ``moneybin.loaders.w2_loader`` assertion was dropped when
    the whole ``loaders`` package was deleted: from then on it passed because
    its *parent package* was gone, not because the W-2 loader was, so it would
    have stayed green if a W-2 loader reappeared under any other package.
    """
    with pytest.raises(ModuleNotFoundError):
        import moneybin.extractors.w2_extractor  # type: ignore[import]  # noqa: F401  # the import is asserted to raise, so the name is never bound

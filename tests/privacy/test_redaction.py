"""Regression guards for redact_typed container reconstruction."""

from dataclasses import dataclass
from typing import Annotated

from moneybin.privacy.redaction import redact_records, redact_typed
from moneybin.privacy.taxonomy import DataClass


def test_redact_frozenset_reconstructs_type() -> None:
    """A frozenset[Annotated[..., CRITICAL]] round-trips to a masked frozenset.

    Pins the container-reconstruction branch in ``_redact`` (list stays a list;
    set/frozenset/tuple are rebuilt via ``type(value)(redacted)``). Without the
    guard, a frozenset field could silently degrade to a list or leave its
    CRITICAL elements unmasked.
    """

    @dataclass(frozen=True)
    class P:
        accts: frozenset[Annotated[str, DataClass.ACCOUNT_IDENTIFIER]]

    out: P = redact_typed(P(accts=frozenset({"123456789"})), consent=None)
    assert isinstance(out.accts, frozenset)
    assert out.accts == frozenset({"****6789"})


def test_redact_records_masks_critical_passes_through_rest() -> None:
    """CRITICAL columns are masked; HIGH/MEDIUM/LOW pass through (parity)."""
    rows = [{"account_id": "1234567890", "amount": -42.5, "category": "Food"}]
    out = redact_records(
        rows,
        {
            "account_id": DataClass.ACCOUNT_IDENTIFIER,
            "amount": DataClass.TXN_AMOUNT,
            "category": DataClass.CATEGORY,
        },
    )
    assert out == [{"account_id": "****7890", "amount": -42.5, "category": "Food"}]


def test_redact_records_passes_unmapped_columns_through() -> None:
    """A column with no class entry is left untouched."""
    rows = [{"x": 1}]
    assert redact_records(rows, {}) == [{"x": 1}]


def test_redact_records_empty_is_noop() -> None:
    """An empty result set returns unchanged (no per-column work)."""
    assert redact_records([], {"a": DataClass.TXN_AMOUNT}) == []

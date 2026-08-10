"""Regression guards for redact_typed container reconstruction."""

from dataclasses import dataclass
from typing import Annotated

import pytest

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


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("groceries", "groceries"),  # ordinary text passes through
        ("123-45-6789", "***-**-****"),  # SSN shape masked
        ("acct 987654321", "acct ****...4321"),  # account shape masked
        (None, None),
        (42, 42),  # short int untouched
        (987654321, "****...4321"),  # 8+ digit int masked
        (True, True),  # bool is not a digit string
    ],
)
def test_floored_masks_only_pii_shapes(value: object, expected: object) -> None:
    assert redact_records([{"c": value}], {"c": DataClass.FLOORED}, consent=None) == [
        {"c": expected}
    ]


def test_floored_recurses_into_containers() -> None:
    records = [{"c": {"inner": "123-45-6789"}}]
    assert redact_records(records, {"c": DataClass.FLOORED}, consent=None) == [
        {"c": {"inner": "***-**-****"}}
    ]


def test_floored_masks_mapping_keys() -> None:
    """A DuckDB MAP keys itself from row data, so the floor has to reach keys.

    ``SELECT map([account_number], [1])`` puts an account number exactly where a
    STRUCT puts a query-authored field name. Flooring values alone published the
    one number the same scalar in a bare column would have been masked for.
    """
    records = [{"c": {"987654321": "x"}}]
    assert redact_records(records, {"c": DataClass.FLOORED}, consent=None) == [
        {"c": {"****...4321": "x"}}
    ]


def test_floored_collapses_keys_that_mask_alike() -> None:
    """Two keys masking to one string collapse to one entry — the accepted cost.

    Pinned so the dropped entry is a known cost rather than a surprise: the
    account mask keeps only the last four digits, so any two keys sharing them
    land on the same masked string and the later one wins.
    """
    records = [{"c": {"111114321": "first", "222224321": "second"}}]
    assert redact_records(records, {"c": DataClass.FLOORED}, consent=None) == [
        {"c": {"****...4321": "second"}}
    ]

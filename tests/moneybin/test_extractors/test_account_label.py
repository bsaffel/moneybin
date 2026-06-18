"""Unit tests for the Tier-B account-label parser (Decision 8)."""

from __future__ import annotations

import pytest

from moneybin.extractors.tabular.account_label import parse_account_label


@pytest.mark.parametrize(
    ("label", "expected_name", "expected_last4"),
    [
        ("Daily Expense (...1789)", "Daily Expense", "1789"),
        ("Checking ····1789", "Checking", "1789"),  # noqa: RUF001  # U+00B7 mask dots are the input under test
        ("Savings x1789", "Savings", "1789"),
        ("Card ending in 1789", "Card", "1789"),
        ("Card ending 1789", "Card", "1789"),
        ("Card ends in 1789", "Card", "1789"),  # "ends in" must strip like "ending in"
        ("Checking end in 4267", "Checking", "4267"),  # "end in" variant
        ("WF CHECKING 9940", "WF CHECKING", "9940"),  # bare trailing 4-digit group
        ("Joint (xxxx4267)", "Joint", "4267"),
        ("365 Savings", "365 Savings", None),  # 3 digits → no last4
        ("Brokerage", "Brokerage", None),
        ("", "", None),
    ],
)
def test_parse_account_label(
    label: str, expected_name: str, expected_last4: str | None
) -> None:
    name, last4 = parse_account_label(label)
    assert (name, last4) == (expected_name, expected_last4)


def test_parse_account_label_none() -> None:
    assert parse_account_label(None) == ("", None)

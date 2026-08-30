"""Unit tests for the Tier-B account-label parser (Decision 8)."""

from __future__ import annotations

import pytest

from moneybin.extractors.tabular.account_label import parse_account_label


@pytest.mark.parametrize(
    ("label", "expected_name", "expected_last4"),
    [
        ("Everyday Spending (...7777)", "Everyday Spending", "7777"),
        ("Checking ····7777", "Checking", "7777"),  # noqa: RUF001  # U+00B7 mask dots are the input under test
        ("Savings x7777", "Savings", "7777"),
        ("Card ending in 7777", "Card", "7777"),
        ("Card ending 7777", "Card", "7777"),
        ("Card ends in 7777", "Card", "7777"),  # "ends in" must strip like "ending in"
        ("Checking end in 1212", "Checking", "1212"),  # "end in" variant
        ("WF CHECKING 3030", "WF CHECKING", "3030"),  # bare trailing 4-digit group
        ("Joint (xxxx1212)", "Joint", "1212"),
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

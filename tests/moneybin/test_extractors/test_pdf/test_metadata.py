"""Tests for statement-metadata capture (Req 7a)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from moneybin.extractors.pdf.metadata import StatementMetadata, capture_metadata

# ---------------------------------------------------------------------------
# Fixture text blocks
# ---------------------------------------------------------------------------

# Chase-style: "Account Number: ****1234", labelled balances, date range split
_CHASE_TEXT = """\
Chase Bank Statement
Account Number: ****1234
Statement Period: 01/01/2024
through 01/31/2024

Beginning Balance:    $1,234.56
Ending Balance:       $2,345.67

TRANSACTIONS
01/05/2024  Coffee Shop    -4.50
01/20/2024  Paycheck    1,500.00
TOTAL
"""

# Amex-style: "Account ending in 5678", ISO-adjacent labelled balances
_AMEX_TEXT = """\
American Express Statement
Account ending in 5678
From: 02/01/2024
To: 02/29/2024

Beginning Balance: $500.00
Ending Balance: $300.00

TRANSACTIONS
02/10/2024  Restaurant    -50.00
TOTAL
"""

# Chase credit-card layout: the card number prints as space-separated groups.
# The masked form carries the only account-discriminating digits the statement
# has, in the LAST group.
_CHASE_GROUPED_MASKED_TEXT = """\
Chase Bank Statement
Account Number: XXXX XXXX XXXX 1234
Opening/Closing Date 01/01/24 - 01/31/24

Previous Balance: $100.00
New Balance: $200.00

TRANSACTIONS
01/05/2024  Coffee Shop    -4.50
TOTAL
"""

# Same layout, unmasked. The last four are 3456 — a capture that stops at the
# first group would report 1234, an authoritative-looking but wrong last4 that
# then feeds the institution+last4 merge signal.
_CHASE_GROUPED_PLAIN_TEXT = """\
Chase Bank Statement
Account Number: 1234 5678 9012 3456
Opening/Closing Date 01/01/24 - 01/31/24

Previous Balance: $100.00
New Balance: $200.00

TRANSACTIONS
01/05/2024  Coffee Shop    -4.50
TOTAL
"""

# ---------------------------------------------------------------------------
# account_id
# ---------------------------------------------------------------------------


def test_chase_account_id() -> None:
    meta = capture_metadata(_CHASE_TEXT)
    assert meta.account_id == "****1234"
    assert meta.account_id_complete is False


def test_amex_account_id() -> None:
    meta = capture_metadata(_AMEX_TEXT)
    assert meta.account_id == "5678"
    assert meta.account_id_complete is False


def test_ending_in_more_than_four_digits_is_still_partial() -> None:
    meta = capture_metadata("Account ending in 123456\n")

    assert meta.account_id == "123456"
    assert meta.account_id_complete is False


def test_account_id_preserves_mask() -> None:
    # Raw masked string kept as-is; not stripped or normalised.
    meta = capture_metadata(_CHASE_TEXT)
    assert meta.account_id is not None
    assert "****" in meta.account_id


def test_grouped_masked_account_number_keeps_the_last_four() -> None:
    """A space-grouped card number must not be truncated to its first group.

    Chase prints "XXXX XXXX XXXX 1234". Capturing one whitespace-delimited
    token yields the bare mask "XXXX" and throws away the only digits the
    statement discloses, which is what produced the digit-free `chase_xxxx`
    account key in the wild.
    """
    meta = capture_metadata(_CHASE_GROUPED_MASKED_TEXT)
    assert meta.account_id is not None
    assert meta.account_id.endswith("1234")


def test_grouped_plain_account_number_last_four_is_the_final_group() -> None:
    """The trailing group is the last4 — never the leading one.

    Stopping at the first group reports 1234 for an account ending 3456. That
    is worse than capturing nothing: it looks authoritative and feeds the
    institution+last4 merge signal with the wrong value.
    """
    meta = capture_metadata(_CHASE_GROUPED_PLAIN_TEXT)
    assert meta.account_id is not None
    digits = "".join(c for c in meta.account_id if c.isdigit())
    assert digits[-4:] == "3456"
    assert meta.account_id_complete is True


@pytest.mark.parametrize(
    ("account_line", "expected"),
    [
        # Masked grouped card: every group is captured, so the trailing one —
        # the only account-discriminating digits on the page — survives.
        ("Account Number: XXXX XXXX XXXX 1234", "XXXX XXXX XXXX 1234"),
        # Unmasked grouped card: same shape, so last4 is the FINAL group.
        ("Account Number: 1234 5678 9012 3456", "1234 5678 9012 3456"),
        # A trailing statement date must not extend the capture.
        ("Account Number: XXXX1234 01/31/24", "XXXX1234"),
        # Institution token that opens with letters: the digit/mask-run anchor
        # never matches, so the (\\S+) anchor captures it.
        ("Account Number: ACCT-9Z", "ACCT-9Z"),
        # Institution token that opens with 3+ digits. The digit/mask-run
        # anchor CAN match its leading "123" — it must not, or `_first_match`
        # stops there and the (\\S+) anchor becomes permanently unreachable
        # for every token of this shape.
        ("Account Number: 123-ABC-456", "123-ABC-456"),
    ],
)
def test_account_anchor_ordering_captures_the_whole_token(
    account_line: str, expected: str
) -> None:
    """A partial digit-run match must never claim the account field.

    `capture_metadata` is first-match-wins across the ordered anchor list, so
    an anchor that matches *part* of a token silently retires every anchor
    after it. The grouped-card anchor and the institution-token fallback have
    to stay reachable from the same document.
    """
    meta = capture_metadata(f"Bank Statement\n{account_line}\n")
    assert meta.account_id == expected


def test_capture_metadata_collects_account_identity_evidence() -> None:
    text = """\
Account Name: Household Checking
Account Type: Personal Checking
Product Name: Total Checking
Routing Number: 021000021
Currency: usd
"""

    meta = capture_metadata(text)

    assert meta.account_label == "Household Checking"
    assert meta.account_type == "Personal Checking"
    assert meta.product_name == "Total Checking"
    assert meta.routing_number == "021000021"
    assert meta.currency_code == "USD"


# ---------------------------------------------------------------------------
# period_start / period_end
# ---------------------------------------------------------------------------


def test_chase_period_start() -> None:
    meta = capture_metadata(_CHASE_TEXT)
    assert meta.period_start == date(2024, 1, 1)


def test_chase_period_end() -> None:
    meta = capture_metadata(_CHASE_TEXT)
    assert meta.period_end == date(2024, 1, 31)


def test_amex_period_start() -> None:
    meta = capture_metadata(_AMEX_TEXT)
    assert meta.period_start == date(2024, 2, 1)


def test_amex_period_end() -> None:
    meta = capture_metadata(_AMEX_TEXT)
    assert meta.period_end == date(2024, 2, 29)


# ---------------------------------------------------------------------------
# opening_balance / closing_balance
# ---------------------------------------------------------------------------


def test_chase_opening_balance() -> None:
    meta = capture_metadata(_CHASE_TEXT)
    assert meta.opening_balance == Decimal("1234.56")


def test_chase_closing_balance() -> None:
    meta = capture_metadata(_CHASE_TEXT)
    assert meta.closing_balance == Decimal("2345.67")


def test_amex_opening_balance() -> None:
    meta = capture_metadata(_AMEX_TEXT)
    assert meta.opening_balance == Decimal("500.00")


def test_amex_closing_balance() -> None:
    meta = capture_metadata(_AMEX_TEXT)
    assert meta.closing_balance == Decimal("300.00")


# ---------------------------------------------------------------------------
# is_complete_for_reconciliation
# ---------------------------------------------------------------------------


def test_complete_when_both_balances_present() -> None:
    meta = capture_metadata(_CHASE_TEXT)
    assert meta.is_complete_for_reconciliation() is True


def test_incomplete_when_closing_balance_missing() -> None:
    meta = StatementMetadata(
        account_id="1234",
        period_start=date(2024, 1, 1),
        period_end=date(2024, 1, 31),
        opening_balance=Decimal("100.00"),
        closing_balance=None,
    )
    assert meta.is_complete_for_reconciliation() is False


def test_incomplete_when_opening_balance_missing() -> None:
    meta = StatementMetadata(
        account_id="1234",
        period_start=date(2024, 1, 1),
        period_end=date(2024, 1, 31),
        opening_balance=None,
        closing_balance=Decimal("100.00"),
    )
    assert meta.is_complete_for_reconciliation() is False


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_text_returns_all_none() -> None:
    meta = capture_metadata("")
    assert meta.account_id is None
    assert meta.period_start is None
    assert meta.period_end is None
    assert meta.opening_balance is None
    assert meta.closing_balance is None


def test_empty_text_not_complete_for_reconciliation() -> None:
    meta = capture_metadata("")
    assert meta.is_complete_for_reconciliation() is False


def test_custom_anchors_override_defaults() -> None:
    text = "Acct: XYZ999\nOB: 50.00\nCB: 75.00"
    custom: dict[str, list[str]] = {
        "account_id": [r"Acct:\s+(\S+)"],
        "period_start": [],
        "period_end": [],
        "opening_balance": [r"OB:\s+([\d.]+)"],
        "closing_balance": [r"CB:\s+([\d.]+)"],
    }
    meta = capture_metadata(text, anchors=custom)
    assert meta.account_id == "XYZ999"
    assert meta.opening_balance == Decimal("50.00")
    assert meta.closing_balance == Decimal("75.00")
    assert meta.period_start is None
    assert meta.period_end is None


def test_bad_date_format_returns_none() -> None:
    # Anchor matches but value can't be parsed as any supported format
    # (MM/DD/YYYY, MM/DD/YY, or ISO YYYY-MM-DD) — a spelled-out month is unsupported.
    custom: dict[str, list[str]] = {
        "account_id": [],
        "period_start": [r"Start:\s+(\S+)"],
        "period_end": [],
        "opening_balance": [],
        "closing_balance": [],
    }
    meta = capture_metadata("Start: 15-Jan-2024", anchors=custom)
    assert meta.period_start is None


def test_bad_decimal_returns_none() -> None:
    custom: dict[str, list[str]] = {
        "account_id": [],
        "period_start": [],
        "period_end": [],
        "opening_balance": [r"OB:\s+(\S+)"],
        "closing_balance": [],
    }
    meta = capture_metadata("OB: not-a-number", anchors=custom)
    assert meta.opening_balance is None


def test_capture_metadata_parses_iso_period_date() -> None:
    """A period anchor capturing an ISO date resolves (bridge ISO-anchor support).

    A bridge-authored recipe may declare a period anchor for a non-default label
    whose date is ISO-shaped; _parse_date must accept it so the year-less executor
    can bracket each MM/DD row against the captured period.
    """
    custom = {
        "period_start": [r"Cycle\s+(\d{4}-\d{2}-\d{2})"],
        "period_end": [r"Cycle\s+\d{4}-\d{2}-\d{2}\s*-\s*(\d{4}-\d{2}-\d{2})"],
    }
    meta = capture_metadata("Cycle 2024-12-23 - 2025-01-22", anchors=custom)
    assert meta.period_start == date(2024, 12, 23)
    assert meta.period_end == date(2025, 1, 22)

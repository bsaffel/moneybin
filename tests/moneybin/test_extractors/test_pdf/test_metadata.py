"""Tests for statement-metadata capture (Req 7a)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

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

# ---------------------------------------------------------------------------
# account_id
# ---------------------------------------------------------------------------


def test_chase_account_id() -> None:
    meta = capture_metadata(_CHASE_TEXT)
    assert meta.account_id == "****1234"


def test_amex_account_id() -> None:
    meta = capture_metadata(_AMEX_TEXT)
    assert meta.account_id == "5678"


def test_account_id_preserves_mask() -> None:
    # Raw masked string kept as-is; not stripped or normalised.
    meta = capture_metadata(_CHASE_TEXT)
    assert meta.account_id is not None
    assert "****" in meta.account_id


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

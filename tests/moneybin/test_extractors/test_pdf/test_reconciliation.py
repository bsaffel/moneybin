"""Tests for the reconciliation gate (Req 9)."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from moneybin.extractors.pdf.metadata import StatementMetadata
from moneybin.extractors.pdf.reconciliation import ReconciliationResult, reconcile

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _meta(
    opening: str | None = "1000.00",
    closing: str | None = "1100.00",
) -> StatementMetadata:
    return StatementMetadata(
        account_id="1234",
        period_start=None,
        period_end=None,
        opening_balance=Decimal(opening) if opening is not None else None,
        closing_balance=Decimal(closing) if closing is not None else None,
    )


def _signed_rows(*amounts: str) -> list[dict[str, Any]]:
    return [{"amount": Decimal(a)} for a in amounts]


def _split_rows(*pairs: tuple[str, str]) -> list[dict[str, Any]]:
    """Each pair is (debit, credit)."""
    return [{"debit": Decimal(d), "credit": Decimal(c)} for d, c in pairs]


# ---------------------------------------------------------------------------
# Plan-mandated: signed-amount column — pass (opening=1000, closing=1100)
# ---------------------------------------------------------------------------


def test_signed_amount_pass() -> None:
    # opening=1000, closing=1100 → expected_delta=100
    # rows sum: -30 + 130 = 100 → matches
    rows = _signed_rows("-30.00", "130.00")
    result = reconcile(rows, _meta("1000.00", "1100.00"), "negative_is_expense")
    assert result.passed is True
    assert result.reason == "passed"
    assert result.expected_delta == Decimal("100.00")
    assert result.observed_delta == Decimal("100.00")


# ---------------------------------------------------------------------------
# Plan-mandated: signed-amount column off by exactly one row — fail
# ---------------------------------------------------------------------------


def test_signed_amount_off_by_one_row() -> None:
    # Subtotal row (-30 + 130 = 100) accidentally counted as a transaction.
    # Rows: -30, 130, 100 (the subtotal) → sum=200, expected=100 → mismatch
    rows = _signed_rows("-30.00", "130.00", "100.00")
    result = reconcile(rows, _meta("1000.00", "1100.00"), "negative_is_expense")
    assert result.passed is False
    assert result.reason == "delta_mismatch"
    assert result.expected_delta == Decimal("100.00")
    assert result.observed_delta == Decimal("200.00")


# ---------------------------------------------------------------------------
# Plan-mandated: split debit/credit columns — pass
# ---------------------------------------------------------------------------


def test_split_debit_credit_pass() -> None:
    # opening=500, closing=350 → expected_delta=-150
    # credits - debits: 0 + 0 - 100 - 50 = -150 → matches
    rows = _split_rows(("100.00", "0.00"), ("50.00", "0.00"))
    result = reconcile(rows, _meta("500.00", "350.00"), "split_debit_credit")
    assert result.passed is True
    assert result.reason == "passed"
    assert result.expected_delta == Decimal("-150.00")
    assert result.observed_delta == Decimal("-150.00")


def test_split_debit_credit_credit_card_convention_fails() -> None:
    """Credit-card debit/credit semantics are explicitly unsupported.

    Bank statements treat a debit as money leaving the account
    (credits − debits = delta). Credit-card statements use the opposite
    convention: a charge appears in the "Debits" column AND raises the
    statement balance. Without a per-recipe statement_type discriminator
    in Phase 2a, every split_debit_credit recipe is interpreted as the
    bank convention; reconciliation comes out the wrong sign for a card
    statement and the document routes to seed. This test pins that
    behaviour so a future change that "fixes" credit-card sign handling
    can't silently flip the bank-account pass case.
    """
    # Credit-card framing: opening=500 (prior balance), $100 of new charges,
    # closing=600 (balance rose). Under credit-card semantics the row
    # delta is +100; under the bank semantics the reconciler applies
    # (credits − debits) = -100, which doesn't match expected_delta=+100.
    rows = _split_rows(("100.00", "0.00"))
    result = reconcile(rows, _meta("500.00", "600.00"), "split_debit_credit")
    assert result.passed is False
    assert result.reason == "delta_mismatch"
    # Observed sign reflects bank semantics, NOT card semantics — the gap
    # between observed=-100 and expected=+100 is the precise signal that
    # routes this layout to seed.
    assert result.expected_delta == Decimal("100.00")
    assert result.observed_delta == Decimal("-100.00")


# ---------------------------------------------------------------------------
# metadata_incomplete → short-circuit
# ---------------------------------------------------------------------------


def test_metadata_incomplete_closing_none() -> None:
    rows = _signed_rows("50.00")
    result = reconcile(rows, _meta("1000.00", None), "negative_is_expense")
    assert result.passed is False
    assert result.reason == "metadata_incomplete"
    assert result.expected_delta is None
    assert result.observed_delta is None


def test_metadata_incomplete_opening_none() -> None:
    rows = _signed_rows("50.00")
    result = reconcile(rows, _meta(None, "1000.00"), "negative_is_expense")
    assert result.passed is False
    assert result.reason == "metadata_incomplete"


def test_metadata_both_none() -> None:
    rows = _signed_rows("50.00")
    result = reconcile(rows, _meta(None, None), "negative_is_expense")
    assert result.passed is False
    assert result.reason == "metadata_incomplete"


# ---------------------------------------------------------------------------
# no_rows — always fails even if math would be zero
# ---------------------------------------------------------------------------


def test_no_rows_nonzero_delta() -> None:
    # opening=1000, closing=1100 → expected_delta=100, but no rows
    result = reconcile([], _meta("1000.00", "1100.00"), "negative_is_expense")
    assert result.passed is False
    assert result.reason == "no_rows"
    assert result.expected_delta == Decimal("100.00")
    assert result.observed_delta == Decimal("0")


def test_no_rows_zero_delta_still_fails() -> None:
    # opening==closing: delta=0, observed=0 → math "matches" but no evidence extraction worked
    result = reconcile([], _meta("1000.00", "1000.00"), "negative_is_expense")
    assert result.passed is False
    assert result.reason == "no_rows"


# ---------------------------------------------------------------------------
# Tolerance edge cases
# ---------------------------------------------------------------------------


def test_off_by_one_cent_passes() -> None:
    # expected=100.00, observed=100.01 — within 1¢ tolerance
    rows = _signed_rows("100.01")
    result = reconcile(rows, _meta("1000.00", "1100.00"), "negative_is_expense")
    assert result.passed is True
    assert result.reason == "passed"


def test_off_by_two_cents_fails() -> None:
    # expected=100.00, observed=100.02 — exceeds 1¢ tolerance
    rows = _signed_rows("100.02")
    result = reconcile(rows, _meta("1000.00", "1100.00"), "negative_is_expense")
    assert result.passed is False
    assert result.reason == "delta_mismatch"


# ---------------------------------------------------------------------------
# negative_is_income convention (same summation path as negative_is_expense)
# ---------------------------------------------------------------------------


def test_negative_is_income_pass() -> None:
    # opening=1000, closing=800 → expected=-200; single income row of -200 → pass
    rows = _signed_rows("-200.00")
    result = reconcile(rows, _meta("1000.00", "800.00"), "negative_is_income")
    assert result.passed is True


def test_card_statement_reconciles_under_negative_is_income() -> None:
    """A card's balance GROWS with charges: sum(raw) == closing - opening, unflipped.

    Reconciliation sums raw PRE-normalization amounts (see
    ``_sum_pre_normalization``), so a card statement ties out identically under
    either single-amount convention — the same +150.00 charge, -50.00 payment
    rows that pass here also pass under ``negative_is_expense``. That parity is
    exactly why reconciliation cannot tell a card statement from a checking one;
    pin it so nobody "fixes" reconcile to special-case cards and breaks the
    bank-account case pinned by ``test_split_debit_credit_credit_card_convention_fails``
    below. Numbers match the real card fixture
    (``tests/moneybin/pdf_statement_fixtures.py::write_card_statement_pdf``).
    """
    rows = _signed_rows("150.00", "-50.00")
    result = reconcile(rows, _meta("0.00", "100.00"), "negative_is_income")
    assert result.passed is True
    assert result.reason == "passed"
    assert result.expected_delta == Decimal("100.00")
    assert result.observed_delta == Decimal("100.00")


# ---------------------------------------------------------------------------
# split_debit_credit missing keys treated as zero
# ---------------------------------------------------------------------------


def test_split_debit_credit_missing_credit_treated_as_zero() -> None:
    # Row has only debit=50, no credit key → credit defaults to 0
    # opening=500, closing=450 → expected=-50; observed=0-50=-50 → pass
    rows = [{"debit": Decimal("50.00")}]
    result = reconcile(rows, _meta("500.00", "450.00"), "split_debit_credit")
    assert result.passed is True


def test_split_debit_credit_missing_debit_treated_as_zero() -> None:
    # Row has only credit=50, no debit key → debit defaults to 0
    # opening=500, closing=550 → expected=50; observed=50-0=50 → pass
    rows = [{"credit": Decimal("50.00")}]
    result = reconcile(rows, _meta("500.00", "550.00"), "split_debit_credit")
    assert result.passed is True


# ---------------------------------------------------------------------------
# ReconciliationResult dataclass
# ---------------------------------------------------------------------------


def test_reconciliation_result_frozen() -> None:
    r = ReconciliationResult(
        passed=True,
        expected_delta=Decimal("100"),
        observed_delta=Decimal("100"),
        reason="passed",
    )
    with pytest.raises(Exception):  # noqa: B017  # frozen dataclass raises on setattr
        r.passed = False  # type: ignore[misc]

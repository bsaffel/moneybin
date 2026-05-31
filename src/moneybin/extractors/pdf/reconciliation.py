"""Reconciliation gate for deterministic PDF extraction (Req 9).

Checks whether the sum of extracted row amounts matches the expected balance
delta (closing - opening) from statement metadata.  Computed on pre-sign-
normalization values in the statement's own terms — sign conventions vary
across institutions (some show -100 for spending, some split into separate
Debit/Credit columns), and this is the only place where we compare extracted
values directly against statement-reported totals before any downstream
sign flip.

Consumed by Task 9's routing orchestration: pass + high confidence → route
to raw.tabular_transactions; otherwise → seed fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

from moneybin.extractors.pdf.metadata import StatementMetadata

# 1¢ tolerance: DECIMAL arithmetic is exact but statement totals sometimes
# round odd cents differently than the sum of individual rows.
_TOLERANCE = Decimal("0.01")

# Matches Recipe.sign_convention (re-declared here to avoid coupling Recipe to
# this module before a second consumer exists — see implementation notes).
_SignConvention = Literal[
    "negative_is_expense", "negative_is_income", "split_debit_credit"
]


@dataclass(frozen=True)
class ReconciliationResult:
    """Outcome of the reconciliation gate."""

    passed: bool
    expected_delta: Decimal | None  # closing - opening; None when metadata incomplete
    observed_delta: Decimal | None
    reason: Literal["passed", "metadata_incomplete", "delta_mismatch", "no_rows"]


def reconcile(
    rows: list[dict[str, Any]],
    metadata: StatementMetadata,
    sign_convention: _SignConvention,
) -> ReconciliationResult:
    """Return whether extracted rows reconcile against statement balances.

    Args:
        rows: Typed row dicts produced by ``execute_recipe``.
        metadata: Statement-level fields from ``capture_metadata``.
        sign_convention: How amounts are encoded in this statement's rows.
            ``negative_is_expense`` / ``negative_is_income`` — rows have an
            ``"amount"`` key.  ``split_debit_credit`` — rows have ``"debit"``
            and ``"credit"`` keys.

    Returns:
        ``ReconciliationResult`` with ``passed=True`` only when metadata is
        complete, rows are present, and the delta is within ``_TOLERANCE``.
    """
    if not metadata.is_complete_for_reconciliation():
        return ReconciliationResult(
            passed=False,
            expected_delta=None,
            observed_delta=None,
            reason="metadata_incomplete",
        )

    # Both balances guaranteed non-None by is_complete_for_reconciliation().
    # Narrow explicitly so pyright sees Decimal, not Decimal | None.
    opening = metadata.opening_balance
    closing = metadata.closing_balance
    if opening is None or closing is None:  # unreachable — narrowing guard
        raise RuntimeError("is_complete_for_reconciliation contract violated")
    expected = closing - opening

    if not rows:
        # No rows means no evidence the extraction worked.  Even when
        # opening == closing (delta = 0), we cannot confirm the extractor
        # found and parsed all rows — the zero could be coincidental.
        return ReconciliationResult(
            passed=False,
            expected_delta=expected,
            observed_delta=Decimal("0"),
            reason="no_rows",
        )

    observed = _sum_pre_normalization(rows, sign_convention)

    if abs(expected - observed) <= _TOLERANCE:
        return ReconciliationResult(
            passed=True,
            expected_delta=expected,
            observed_delta=observed,
            reason="passed",
        )

    return ReconciliationResult(
        passed=False,
        expected_delta=expected,
        observed_delta=observed,
        reason="delta_mismatch",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sum_pre_normalization(
    rows: list[dict[str, Any]],
    sign_convention: _SignConvention,
) -> Decimal:
    """Sum row amounts in the statement's own sign convention.

    For single-amount conventions (``negative_is_expense`` /
    ``negative_is_income``), rows have an ``"amount"`` key.  The two
    conventions differ only in the downstream sign flip applied when writing
    to ``core.fct_transactions`` — the summation is identical here.

    For ``split_debit_credit``, rows have ``"debit"`` and ``"credit"`` keys.
    Missing keys default to ``Decimal("0")`` so a partially-populated row
    moves the observed delta rather than crashing the gate.
    """
    if sign_convention in ("negative_is_expense", "negative_is_income"):
        return sum(
            (row.get("amount") or Decimal("0") for row in rows),
            Decimal("0"),
        )
    # split_debit_credit
    total_credits = sum(
        (row.get("credit") or Decimal("0") for row in rows), Decimal("0")
    )
    total_debits = sum((row.get("debit") or Decimal("0") for row in rows), Decimal("0"))
    return total_credits - total_debits

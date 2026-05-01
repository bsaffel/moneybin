"""Per-transaction expectations — categorization, provenance, collapse counts."""

from __future__ import annotations

from typing import Any

from moneybin.database import Database
from moneybin.tables import FCT_TRANSACTION_PROVENANCE, FCT_TRANSACTIONS
from moneybin.validation.expectations._types import SourceTransactionRef
from moneybin.validation.result import ExpectationResult


def verify_gold_record_count(
    db: Database,
    *,
    expected_collapsed_count: int,
    fixture_source_ids: list[str] | None = None,
    description: str = "",
) -> ExpectationResult:
    """Verify gold record count, optionally scoped to fixture source IDs."""
    fixture_ids: list[str] = list(fixture_source_ids or [])
    if fixture_ids:
        placeholders = ",".join(["?"] * len(fixture_ids))
        sql = f"""
            SELECT COUNT(DISTINCT transaction_id)
            FROM {FCT_TRANSACTION_PROVENANCE.full_name}
            WHERE source_transaction_id IN ({placeholders})
        """  # noqa: S608 — placeholders count derived from typed list; values bound
        row = db.execute(sql, fixture_ids).fetchone()
    else:
        row = db.execute(
            f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name}"  # noqa: S608 — TableRef constant
        ).fetchone()
    actual = int(row[0]) if row is not None else 0
    return ExpectationResult(
        name=description or "gold_record_count",
        kind="gold_record_count",
        passed=actual == expected_collapsed_count,
        details={"expected": expected_collapsed_count, "actual": actual},
    )


def verify_category_for_transaction(
    db: Database,
    *,
    transaction_id: str,
    expected_category: str,
    # ``expected_categorized_by`` is an open vocabulary (rule, auto_rule, ai, user, …),
    # so it stays ``str | None`` rather than a closed Literal.
    expected_categorized_by: str | None = None,
    description: str = "",
) -> ExpectationResult:
    """Verify a transaction's category (and optionally its categorizer source)."""
    row = db.execute(
        "SELECT category, categorized_by "  # noqa: S608 — TableRef constant
        f"FROM {FCT_TRANSACTIONS.full_name} "
        "WHERE transaction_id = ?",
        [transaction_id],
    ).fetchone()
    if not row:
        return ExpectationResult(
            name=description or "category_for_transaction",
            kind="category_for_transaction",
            passed=False,
            details={
                "reason": "transaction not found",
                "transaction_id": transaction_id,
            },
        )
    actual_cat, actual_src = row
    passed = actual_cat == expected_category and (
        expected_categorized_by is None or actual_src == expected_categorized_by
    )
    return ExpectationResult(
        name=description or "category_for_transaction",
        kind="category_for_transaction",
        passed=passed,
        details={
            "expected": expected_category,
            "actual": actual_cat,
            "expected_source": expected_categorized_by,
            "actual_source": actual_src,
        },
    )


def verify_provenance_for_transaction(
    db: Database,
    *,
    transaction_id: str,
    expected_sources: list[SourceTransactionRef],
    description: str = "",
) -> ExpectationResult:
    """Verify the provenance source rows for a gold transaction match expected."""
    expected: list[tuple[Any, Any]] = sorted(
        (s.source_transaction_id, s.source_type) for s in expected_sources
    )
    rows: list[tuple[Any, ...]] = sorted(
        db.execute(
            "SELECT source_transaction_id, source_type "  # noqa: S608 — TableRef constant
            f"FROM {FCT_TRANSACTION_PROVENANCE.full_name} "
            "WHERE transaction_id = ?",
            [transaction_id],
        ).fetchall()
    )
    return ExpectationResult(
        name=description or "provenance_for_transaction",
        kind="provenance_for_transaction",
        passed=rows == expected,
        details={"expected": expected, "actual": rows},
    )

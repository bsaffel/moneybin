"""Completeness assertions — required values must be populated."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.validation.assertions._helpers import quote_ident
from moneybin.validation.result import AssertionResult


def assert_no_nulls(db: Database, *, table: str, columns: list[str]) -> AssertionResult:
    """Assert no null values exist in the given columns."""
    if not columns:
        raise ValueError("columns must be non-empty")
    t = quote_ident(table)
    per_col_select = ", ".join(
        f"SUM(CASE WHEN {quote_ident(col)} IS NULL THEN 1 ELSE 0 END)"
        for col in columns
    )
    sql = f"SELECT {per_col_select} FROM {t}"  # noqa: S608  # identifiers validated by quote_ident
    row = db.execute(sql).fetchone()
    counts = (
        [int(v) if v is not None else 0 for v in row] if row else [0] * len(columns)
    )
    per_col = dict(zip(columns, counts, strict=True))
    total = sum(per_col.values())
    return AssertionResult(
        name="no_nulls",
        passed=total == 0,
        details={"null_counts": per_col, "total": total},
    )

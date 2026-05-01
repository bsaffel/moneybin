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
    per_col: dict[str, int] = {}
    for col in columns:
        cq = quote_ident(col)
        null_sql = f"SELECT COUNT(*) FROM {t} WHERE {cq} IS NULL"  # noqa: S608  # identifiers validated by quote_ident
        per_col[col] = int(db.execute(null_sql).fetchone()[0])  # type: ignore[index]
    total = sum(per_col.values())
    return AssertionResult(
        name="no_nulls",
        passed=total == 0,
        details={"null_counts": per_col, "total": total},
    )

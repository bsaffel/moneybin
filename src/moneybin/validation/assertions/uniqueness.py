"""Uniqueness assertions — natural keys must not repeat."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.assertions._helpers import quote_ident
from moneybin.validation.result import AssertionResult


def assert_no_duplicates(
    conn: DuckDBPyConnection, *, table: str, columns: list[str]
) -> AssertionResult:
    """Assert no duplicate rows exist across the given column set."""
    if not columns:
        raise ValueError("columns must be non-empty")
    t = quote_ident(table)
    cols = ", ".join(quote_ident(c) for c in columns)
    dup_sql = f"SELECT COUNT(*) FROM (SELECT {cols} FROM {t} GROUP BY {cols} HAVING COUNT(*) > 1)"  # noqa: S608  # identifiers validated by quote_ident
    dup_groups = int(conn.execute(dup_sql).fetchone()[0])  # type: ignore[index]
    return AssertionResult(
        name="no_duplicates",
        passed=dup_groups == 0,
        details={"duplicate_groups": dup_groups, "columns": columns},
    )

"""Schema and row-count assertions."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.assertions.relational import (
    _quote_ident,  # pyright: ignore[reportPrivateUsage]  # intentionally shared within the assertions package
)
from moneybin.validation.result import AssertionResult


def _split(table: str) -> tuple[str | None, str]:
    """Split an optional schema-qualified table name into (schema, table)."""
    if "." in table:
        s, t = table.split(".", 1)
        return s, t
    return None, table


def _columns_with_types(conn: DuckDBPyConnection, table: str) -> dict[str, str]:
    """Return a mapping of column_name -> data_type for the given table."""
    schema, name = _split(table)
    if schema is None:
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ?",
            [name],
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchall()
    return {str(r[0]): str(r[1]) for r in rows}


def assert_columns_exist(
    conn: DuckDBPyConnection, *, table: str, columns: list[str]
) -> AssertionResult:
    """Assert each listed column exists in the table."""
    actual = set(_columns_with_types(conn, table))
    missing = [c for c in columns if c not in actual]
    return AssertionResult(
        name="columns_exist",
        passed=not missing,
        details={"missing": missing, "actual": sorted(actual)},
    )


def assert_column_types(
    conn: DuckDBPyConnection, *, table: str, types: dict[str, str]
) -> AssertionResult:
    """Assert each column has the expected data type."""
    actual = _columns_with_types(conn, table)
    mismatched = {
        col: {"expected": expected, "actual": actual.get(col)}
        for col, expected in types.items()
        if actual.get(col) != expected
    }
    return AssertionResult(
        name="column_types",
        passed=not mismatched,
        details={"mismatched": mismatched},
    )


def _row_count(conn: DuckDBPyConnection, table: str) -> int:
    """Return the row count for the given table."""
    sql = f"SELECT COUNT(*) FROM {_quote_ident(table)}"  # noqa: S608  # identifier validated by _quote_ident
    return int(conn.execute(sql).fetchone()[0])  # type: ignore[index]


def assert_row_count_exact(
    conn: DuckDBPyConnection, *, table: str, expected: int
) -> AssertionResult:
    """Assert the table contains exactly the expected number of rows."""
    actual = _row_count(conn, table)
    return AssertionResult(
        name="row_count_exact",
        passed=actual == expected,
        details={"expected": expected, "actual": actual},
    )


def assert_row_count_delta(
    conn: DuckDBPyConnection, *, table: str, expected: int, tolerance_pct: float
) -> AssertionResult:
    """Assert the row count is within tolerance_pct percent of expected."""
    actual = _row_count(conn, table)
    delta_pct = ((actual - expected) / expected) * 100 if expected else 0.0
    passed = abs(delta_pct) <= tolerance_pct
    return AssertionResult(
        name="row_count_delta",
        passed=passed,
        details={
            "expected": expected,
            "actual": actual,
            "delta_pct": round(delta_pct, 2),
            "tolerance_pct": tolerance_pct,
        },
    )

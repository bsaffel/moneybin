"""Schema and row-count assertions."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.validation.assertions._helpers import quote_ident, split_table_ident
from moneybin.validation.result import AssertionResult


def _columns_with_types(db: Database, table: str) -> dict[str, str]:
    """Return a mapping of column_name -> data_type for the given table."""
    schema, name = split_table_ident(table)
    if schema is None:
        rows = db.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = ?",
            [name],
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchall()
    return {str(r[0]): str(r[1]) for r in rows}


def assert_columns_exist(
    db: Database, *, table: str, columns: list[str]
) -> AssertionResult:
    """Assert each listed column exists in the table."""
    actual = set(_columns_with_types(db, table))
    missing = [c for c in columns if c not in actual]
    return AssertionResult(
        name="columns_exist",
        passed=not missing,
        details={"missing": missing, "actual": sorted(actual)},
    )


def assert_column_types(
    db: Database, *, table: str, types: dict[str, str]
) -> AssertionResult:
    """Assert each column has the expected data type."""
    actual = _columns_with_types(db, table)
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


def assert_schema_snapshot(
    db: Database, *, table: str, expected: dict[str, str]
) -> AssertionResult:
    """Assert table's columns match ``expected`` exactly — no missing, no extra, types match."""
    actual = _columns_with_types(db, table)
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    mismatched = {
        col: {"expected": exp_type, "actual": actual[col]}
        for col, exp_type in expected.items()
        if col in actual and actual[col] != exp_type
    }
    return AssertionResult(
        name="schema_snapshot",
        passed=not missing and not extra and not mismatched,
        details={
            "missing": missing,
            "extra": extra,
            "mismatched": mismatched,
        },
    )


def _row_count(db: Database, table: str) -> int:
    """Return the row count for the given table."""
    sql = f"SELECT COUNT(*) FROM {quote_ident(table)}"  # noqa: S608  # identifier validated by quote_ident
    return int(db.execute(sql).fetchone()[0])  # type: ignore[index]


def assert_row_count_exact(
    db: Database, *, table: str, expected: int
) -> AssertionResult:
    """Assert the table contains exactly the expected number of rows."""
    actual = _row_count(db, table)
    return AssertionResult(
        name="row_count_exact",
        passed=actual == expected,
        details={"expected": expected, "actual": actual},
    )


def assert_row_count_delta(
    db: Database, *, table: str, expected: int, tolerance_pct: float
) -> AssertionResult:
    """Assert the row count is within tolerance_pct percent of expected."""
    actual = _row_count(db, table)
    if expected == 0:
        delta_pct = 0.0 if actual == 0 else float("inf")
    else:
        delta_pct = ((actual - expected) / expected) * 100
    passed = abs(delta_pct) <= tolerance_pct
    return AssertionResult(
        name="row_count_delta",
        passed=passed,
        details={
            "expected": expected,
            "actual": actual,
            # JSON cannot represent ``inf``; encode unbounded delta as None.
            "delta_pct": round(delta_pct, 2) if delta_pct != float("inf") else None,
            "tolerance_pct": tolerance_pct,
        },
    )

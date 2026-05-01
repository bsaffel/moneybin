"""Completeness assertions — required values must be populated."""

from __future__ import annotations

from collections.abc import Iterable

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


def assert_source_system_populated(
    db: Database,
    *,
    table: str,
    expected_sources: Iterable[str],
    column: str = "source_system",
) -> AssertionResult:
    """Assert ``column`` is non-null on every row and all values are in ``expected_sources``.

    ``expected_sources`` accepts any iterable of strings (set, frozenset, list,
    tuple) so YAML scenarios — which deserialize as lists — can call this
    primitive without manual coercion in the registry shim.
    """
    sources = set(expected_sources)
    if not sources:
        raise ValueError("expected_sources must be non-empty")
    t = quote_ident(table)
    c = quote_ident(column)
    null_row = db.execute(
        f"SELECT COUNT(*) FROM {t} WHERE {c} IS NULL"  # noqa: S608  # identifiers validated by quote_ident
    ).fetchone()
    null_count = int(null_row[0]) if null_row else 0
    value_rows = db.execute(
        f"SELECT DISTINCT {c} FROM {t} WHERE {c} IS NOT NULL"  # noqa: S608  # identifiers validated by quote_ident
    ).fetchall()
    observed = {str(r[0]) for r in value_rows}
    unexpected = sorted(observed - sources)
    return AssertionResult(
        name="source_system_populated",
        passed=null_count == 0 and not unexpected,
        details={
            "null_count": null_count,
            "expected_sources": sorted(sources),
            "observed_sources": sorted(observed),
            "unexpected_values": unexpected,
        },
    )

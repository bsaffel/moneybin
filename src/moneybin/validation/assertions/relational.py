"""Referential-integrity assertions usable on any DuckDB connection."""

from __future__ import annotations

from duckdb import DuckDBPyConnection
from sqlglot import exp

from moneybin.validation.result import AssertionResult


def quote_ident(ident: str) -> str:
    """Quote a dotted identifier via sqlglot, per .claude/rules/security.md."""
    return ".".join(
        exp.to_identifier(seg, quoted=True).sql("duckdb") for seg in ident.split(".")
    )


def assert_valid_foreign_keys(
    conn: DuckDBPyConnection,
    *,
    child: str,
    column: str,
    parent: str,
    parent_column: str,
) -> AssertionResult:
    """Assert every non-null child column value exists in the parent table."""
    c = quote_ident(child)
    col = quote_ident(column)
    p = quote_ident(parent)
    pc = quote_ident(parent_column)
    total_sql = f"SELECT COUNT(*) FROM {c} WHERE {col} IS NOT NULL"  # noqa: S608  # identifiers validated by quote_ident
    total = int(conn.execute(total_sql).fetchone()[0])  # type: ignore[index]
    violations_sql = f"SELECT COUNT(*) FROM {c} ch WHERE ch.{col} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {p} pa WHERE pa.{pc} = ch.{col})"  # noqa: S608  # identifiers validated by quote_ident
    violations = int(conn.execute(violations_sql).fetchone()[0])  # type: ignore[index]
    return AssertionResult(
        name="valid_foreign_keys",
        passed=violations == 0,
        details={"checked_rows": total, "violations": violations},
    )


def assert_no_orphans(
    conn: DuckDBPyConnection,
    *,
    parent: str,
    parent_column: str,
    child: str,
    child_column: str,
) -> AssertionResult:
    """Assert every parent row has at least one matching child row."""
    p = quote_ident(parent)
    pc = quote_ident(parent_column)
    c = quote_ident(child)
    cc = quote_ident(child_column)
    orphans_sql = f"SELECT COUNT(*) FROM {p} pa WHERE NOT EXISTS (SELECT 1 FROM {c} ch WHERE ch.{cc} = pa.{pc})"  # noqa: S608  # identifiers validated by quote_ident
    orphans = int(conn.execute(orphans_sql).fetchone()[0])  # type: ignore[index]
    return AssertionResult(
        name="no_orphans",
        passed=orphans == 0,
        details={"orphan_count": orphans},
    )


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


def assert_no_nulls(
    conn: DuckDBPyConnection, *, table: str, columns: list[str]
) -> AssertionResult:
    """Assert no null values exist in the given columns."""
    if not columns:
        raise ValueError("columns must be non-empty")
    t = quote_ident(table)
    per_col_select = ", ".join(
        f"SUM(CASE WHEN {quote_ident(col)} IS NULL THEN 1 ELSE 0 END)"
        for col in columns
    )
    sql = f"SELECT {per_col_select} FROM {t}"  # noqa: S608  # identifiers validated by quote_ident
    row = conn.execute(sql).fetchone()
    counts = [int(v or 0) for v in row] if row else [0] * len(columns)
    per_col = dict(zip(columns, counts, strict=True))
    total = sum(per_col.values())
    return AssertionResult(
        name="no_nulls",
        passed=total == 0,
        details={"null_counts": per_col, "total": total},
    )

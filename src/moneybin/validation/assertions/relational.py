"""Referential-integrity assertions usable on any DuckDB connection."""

from __future__ import annotations

from duckdb import DuckDBPyConnection

from moneybin.validation.result import AssertionResult


def _quote_ident(ident: str) -> str:
    """Quote a dotted identifier, validating each segment contains only safe characters."""
    if not all(ch.isalnum() or ch in "_." for ch in ident):
        raise ValueError(f"invalid identifier: {ident!r}")
    return ".".join(f'"{seg}"' for seg in ident.split("."))


def assert_valid_foreign_keys(
    conn: DuckDBPyConnection,
    *,
    child: str,
    column: str,
    parent: str,
    parent_column: str,
) -> AssertionResult:
    """Assert every non-null child column value exists in the parent table."""
    c = _quote_ident(child)
    col = _quote_ident(column)
    p = _quote_ident(parent)
    pc = _quote_ident(parent_column)
    total_sql = f"SELECT COUNT(*) FROM {c} WHERE {col} IS NOT NULL"  # noqa: S608  # identifiers validated by _quote_ident
    total = int(conn.execute(total_sql).fetchone()[0])  # type: ignore[index]
    violations_sql = f"SELECT COUNT(*) FROM {c} ch WHERE ch.{col} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {p} pa WHERE pa.{pc} = ch.{col})"  # noqa: S608  # identifiers validated by _quote_ident
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
    p = _quote_ident(parent)
    pc = _quote_ident(parent_column)
    c = _quote_ident(child)
    cc = _quote_ident(child_column)
    orphans_sql = f"SELECT COUNT(*) FROM {p} pa WHERE NOT EXISTS (SELECT 1 FROM {c} ch WHERE ch.{cc} = pa.{pc})"  # noqa: S608  # identifiers validated by _quote_ident
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
    t = _quote_ident(table)
    cols = ", ".join(_quote_ident(c) for c in columns)
    dup_sql = f"SELECT COUNT(*) FROM (SELECT {cols} FROM {t} GROUP BY {cols} HAVING COUNT(*) > 1)"  # noqa: S608  # identifiers validated by _quote_ident
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
    t = _quote_ident(table)
    per_col: dict[str, int] = {}
    for col in columns:
        cq = _quote_ident(col)
        null_sql = f"SELECT COUNT(*) FROM {t} WHERE {cq} IS NULL"  # noqa: S608  # identifiers validated by _quote_ident
        per_col[col] = int(conn.execute(null_sql).fetchone()[0])  # type: ignore[index]
    total = sum(per_col.values())
    return AssertionResult(
        name="no_nulls",
        passed=total == 0,
        details={"null_counts": per_col, "total": total},
    )

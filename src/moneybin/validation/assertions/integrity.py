"""Referential-integrity assertions — every child row references a valid parent."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.validation.assertions._helpers import quote_ident
from moneybin.validation.result import AssertionResult


def assert_valid_foreign_keys(
    db: Database,
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
    total = int(db.execute(total_sql).fetchone()[0])  # type: ignore[index]
    violations_sql = f"SELECT COUNT(*) FROM {c} ch WHERE ch.{col} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM {p} pa WHERE pa.{pc} = ch.{col})"  # noqa: S608  # identifiers validated by quote_ident
    violations = int(db.execute(violations_sql).fetchone()[0])  # type: ignore[index]
    return AssertionResult(
        name="valid_foreign_keys",
        passed=violations == 0,
        details={"checked_rows": total, "violations": violations},
    )


def assert_no_orphans(
    db: Database,
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
    orphans = int(db.execute(orphans_sql).fetchone()[0])  # type: ignore[index]
    return AssertionResult(
        name="no_orphans",
        passed=orphans == 0,
        details={"orphan_count": orphans},
    )

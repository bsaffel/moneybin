"""Shared helpers for V0NN migration tests — see .claude/rules/database.md."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from moneybin.database import Database

logger = logging.getLogger(__name__)


def column_info(db: Database, schema: str, table: str, column: str) -> tuple[str, bool]:
    """Return (data_type, is_nullable_bool) for a column from duckdb_columns()."""
    row = db.execute(
        """
        SELECT data_type, is_nullable
        FROM duckdb_columns()
        WHERE schema_name = ? AND table_name = ? AND column_name = ?
        """,
        [schema, table, column],
    ).fetchone()
    assert row is not None, f"{schema}.{table}.{column} not found"
    data_type, is_nullable = row
    return data_type, bool(is_nullable)


def column_exists(db: Database, schema: str, table: str, column: str) -> bool:
    """True if (schema, table, column) is present in duckdb_columns()."""
    row = db.execute(
        "SELECT 1 FROM duckdb_columns() "
        "WHERE schema_name = ? AND table_name = ? AND column_name = ?",
        [schema, table, column],
    ).fetchone()
    return row is not None


def insert_rows(
    db: Database,
    schema: str,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> None:
    """Insert tuple/list rows into `schema.table` under `columns` order."""
    placeholders = ", ".join("?" * len(columns))
    column_list = ", ".join(columns)
    sql = f"INSERT INTO {schema}.{table} ({column_list}) VALUES ({placeholders})"  # noqa: S608  # schema/table/columns are caller-controlled, not user input
    for row in rows:
        db.execute(sql, list(row))


def run_migration(db: Database, migrate_fn: Callable[[object], None]) -> None:
    """Invoke a migration's `migrate()` inside the runner's BEGIN/COMMIT wrap.

    Required to reproduce bug classes that depend on the enclosing
    transaction — bare `migrate(db._conn)` auto-commits each statement and
    masks regressions like V010/V011's "Cannot create index with outstanding
    updates."
    """
    db.execute("BEGIN TRANSACTION")
    try:
        migrate_fn(db._conn)  # pyright: ignore[reportPrivateUsage]
        db.execute("COMMIT")
    except Exception:
        # Rollback is best-effort: V010/V011-style migrations may have already
        # COMMITted and reopened a transaction, so we may rollback either an
        # empty tx or none at all. Don't let a rollback failure mask the
        # original migration error.
        try:
            db.execute("ROLLBACK")
        except Exception as rollback_exc:  # noqa: BLE001 — log and continue; original error re-raised below
            logger.debug(f"ROLLBACK after migration failure raised: {rollback_exc!r}")
        raise

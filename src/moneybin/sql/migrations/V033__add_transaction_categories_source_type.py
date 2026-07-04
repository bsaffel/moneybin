"""V033: add source_type to app.transaction_categories.

Splits the origin aggregator out of categorized_by (which becomes
method-only). All existing rows use internal methods, so they backfill to
'internal'.

The column carries ``DEFAULT 'internal'`` (mirroring the fresh-install DDL in
``app_transaction_categories.sql``) so writers that don't list ``source_type``
explicitly — e.g. ``TransactionCategoriesRepo.set()``, the user-manual-edit
path — still land 'internal' on migrated databases, not NULL.

DuckDB rejects ``ADD COLUMN ... NOT NULL DEFAULT`` together in one statement
on a populated table ("Adding columns with constraints not yet supported"),
so this migration follows V010/V024's proven two-step shape: ADD COLUMN with
DEFAULT (backfills existing rows) → interim COMMIT/BEGIN TRANSACTION (DuckDB
refuses SET NOT NULL while the backfill writes are outstanding in the same
transaction) → ALTER COLUMN SET NOT NULL. transaction_categories carries no
secondary indexes — PRIMARY KEY (transaction_id) only, which duckdb_indexes()
omits and which does not block SET NOT NULL per V024's own note — so, unlike
V024's audit_log, there is no index snapshot/drop/restore step needed.

Idempotent via duckdb_columns() introspection:
- source_type absent (existing DB pre-V033): add with DEFAULT, tighten.
- source_type present but nullable (partial prior run): backfill NULL
  stragglers, tighten.
- source_type present and already NOT NULL (fresh install, or an
  already-migrated database): no-op.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add app.transaction_categories.source_type, backfill, tighten to NOT NULL. Idempotent."""
    cols: list[tuple[str, bool]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name, is_nullable FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'transaction_categories'
        """
    ).fetchall()
    col_map: dict[str, bool] = {c[0]: c[1] for c in cols}

    if not col_map:
        # transaction_categories absent — init_schemas creates it before
        # migrations run, so this is defensive only.
        return

    needs_tighten = False
    if "source_type" not in col_map:
        logger.debug(
            "V033: ADD COLUMN app.transaction_categories.source_type "
            "(DEFAULT 'internal')"
        )
        # ADD COLUMN ... DEFAULT 'internal' is accepted (DEFAULT is not a
        # "constraint") and backfills every existing row to 'internal' in one
        # statement.
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.transaction_categories "
            "ADD COLUMN source_type VARCHAR DEFAULT 'internal'"
        )
        needs_tighten = True
    elif col_map.get("source_type"):
        # Column exists but nullable (truthy is_nullable): partial prior run.
        # Backfill any NULL stragglers, then tighten.
        logger.debug(
            "V033: backfilling nullable app.transaction_categories.source_type"
        )
        conn.execute(  # type: ignore[union-attr]
            "UPDATE app.transaction_categories SET source_type = 'internal' "
            "WHERE source_type IS NULL"
        )
        needs_tighten = True
    # else: source_type exists and is NOT NULL (fresh install, or an
    # already-migrated database) — nothing to do.

    if needs_tighten:
        # Commit the backfill before SET NOT NULL: DuckDB raises while the
        # column's writes are outstanding in the same transaction.
        conn.execute("COMMIT")  # type: ignore[union-attr]
        conn.execute("BEGIN TRANSACTION")  # type: ignore[union-attr]
        logger.debug("V033: ALTER COLUMN source_type SET NOT NULL")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.transaction_categories "
            "ALTER COLUMN source_type SET NOT NULL"
        )

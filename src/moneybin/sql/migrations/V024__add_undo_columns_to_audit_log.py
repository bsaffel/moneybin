"""V024: add is_undo + undoes_operation_id to app.audit_log.

Per docs/specs/data-recovery-contract.md §4.4 (REC-PR3). Rows produced by
``system_audit_undo`` carry ``is_undo=TRUE`` and ``undoes_operation_id`` set to
the operation they reverse — so the consumer can detect an already-undone
operation and make the undo itself undoable.

Two additive columns:
- ``is_undo BOOLEAN NOT NULL DEFAULT FALSE`` — matches the schema file's
  CREATE TABLE so migrated and fresh-install databases stay schema-identical.
- ``undoes_operation_id VARCHAR`` — nullable; NULL on every non-undo row.

DuckDB rejects ``ADD COLUMN ... NOT NULL`` outright ("Adding columns with
constraints not yet supported") and rejects ``ALTER COLUMN ... SET NOT NULL``
while any non-constraint index exists (DependencyException) — and audit_log
carries several. So is_undo follows V023's proven path: ADD nullable-with-DEFAULT (the
DEFAULT backfills existing rows to FALSE) → interim COMMIT → drop the explicit
indexes → SET NOT NULL → restore them from catalog DDL. The dance is duplicated
from V023 rather than shared: a migration is a frozen historical artifact, so a
later edit to a shared helper must never change what V023/V024 already did.

Idempotent via duckdb_columns() introspection:
- Existing DB (is_undo absent): add, backfill via DEFAULT, tighten.
- Partial prior run (is_undo nullable): backfill stragglers, tighten.
- Fresh install (is_undo already NOT NULL from app_audit_log.sql): no-op.

Both columns also ship in src/moneybin/sql/schema/app_audit_log.sql, which
init_schemas runs before migrations — so fresh installs get them at open time.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add app.audit_log.is_undo + undoes_operation_id. Idempotent."""
    cols: list[tuple[str, bool]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name, is_nullable FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'audit_log'
        """
    ).fetchall()
    col_map: dict[str, bool] = {c[0]: c[1] for c in cols}

    if not col_map:
        # audit_log absent — init_schemas creates it before migrations run, so
        # this is defensive only.
        return

    # undoes_operation_id: pure additive, nullable, no constraint dance.
    if "undoes_operation_id" not in col_map:
        logger.info("V024: ADD COLUMN app.audit_log.undoes_operation_id")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.audit_log ADD COLUMN undoes_operation_id VARCHAR"
        )

    needs_tighten = False
    if "is_undo" not in col_map:
        logger.info("V024: ADD COLUMN app.audit_log.is_undo (DEFAULT FALSE)")
        # ADD COLUMN ... DEFAULT FALSE is accepted (DEFAULT is not a "constraint")
        # and backfills every existing row to FALSE in one statement.
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.audit_log ADD COLUMN is_undo BOOLEAN DEFAULT FALSE"
        )
        needs_tighten = True
    elif col_map.get("is_undo"):
        # Column exists but nullable (truthy is_nullable): partial prior run.
        # Backfill any NULL stragglers, then tighten. Truthiness, not `is True`,
        # matches migration_helpers' defensive bool() wrap and V010/V011.
        logger.info("V024: backfilling nullable app.audit_log.is_undo")
        conn.execute(  # type: ignore[union-attr]
            "UPDATE app.audit_log SET is_undo = FALSE WHERE is_undo IS NULL"
        )
        needs_tighten = True
    # else: is_undo exists and is NOT NULL (fresh install) — nothing to do.

    if needs_tighten:
        # Commit the backfill before SET NOT NULL: DuckDB raises while the
        # column's writes are outstanding in the same transaction.
        conn.execute("COMMIT")  # type: ignore[union-attr]
        conn.execute("BEGIN TRANSACTION")  # type: ignore[union-attr]
        # SET NOT NULL is refused while explicit indexes exist (DependencyException).
        # Snapshot → drop → tighten → restore. duckdb_indexes() omits the PRIMARY
        # KEY index, which does not block the ALTER.
        saved_indexes: list[tuple[str, str]] = conn.execute(  # type: ignore[union-attr]
            "SELECT index_name, sql FROM duckdb_indexes() "
            "WHERE schema_name = 'app' AND table_name = 'audit_log'"
        ).fetchall()
        for index_name, _create_sql in saved_indexes:
            conn.execute(f'DROP INDEX IF EXISTS app."{index_name}"')  # type: ignore[union-attr]  # noqa: S608  # catalog-sourced identifier, quoted
        logger.info("V024: ALTER COLUMN is_undo SET NOT NULL")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.audit_log ALTER COLUMN is_undo SET NOT NULL"
        )
        # create_sql is never NULL: duckdb_indexes() lists only explicit
        # CREATE INDEX entries (constraint-backed indexes excluded), so the
        # snapshot can't carry a NULL-sql row.
        for _index_name, create_sql in saved_indexes:
            conn.execute(create_sql)  # type: ignore[union-attr]

    # COMMENT ON COLUMN is idempotent (replaces existing), safe on replay.
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN app.audit_log.is_undo IS "
        "'TRUE for rows produced by system_audit_undo; FALSE for original mutations.'"
    )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN app.audit_log.undoes_operation_id IS "
        "'When is_undo=TRUE, the operation_id this undo reverses; NULL otherwise.'"
    )

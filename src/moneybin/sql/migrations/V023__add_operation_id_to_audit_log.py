"""V023: add operation_id to app.audit_log with legacy backfill + indexes.

Per docs/specs/data-recovery-contract.md §4.4. operation_id groups every
audit row written during one MCP/CLI call so a later undo consumer can
reverse the call as a unit. Pre-spec rows can never be retro-grouped, so they
backfill to the synthetic form op_legacy_<audit_id> — independently queryable
but not grouped.

DuckDB rejects `ADD COLUMN ... NOT NULL` in one statement and rejects
SET NOT NULL / CREATE INDEX while the backfill UPDATE's writes are still
outstanding in the same transaction. So: ADD nullable → backfill → interim
COMMIT → SET NOT NULL → CREATE INDEX. Same pattern as V010 (NOT NULL) and
V016 (index after backfill); see those for the recovery-branch reasoning.

DuckDB (1.5) additionally refuses ALTER COLUMN SET NOT NULL while any
non-constraint index exists on the table (DependencyException) — and
audit_log ships five base indexes. The spec's literal 4-step SQL would fail
on every existing database. So the tighten step snapshots the table's
explicit indexes from the catalog (the PRIMARY KEY is excluded by
duckdb_indexes() and does not block the ALTER), drops them, tightens, then
restores them from their stored DDL before adding the two new indexes.

Idempotent via duckdb_columns() introspection + IF NOT EXISTS:
- Existing DB (column absent): add, backfill, tighten, index.
- Partial prior run (column nullable): backfill stragglers, tighten, index.
- Fresh install (column already NOT NULL from app_audit_log.sql): index only.

The column also ships in src/moneybin/sql/schema/app_audit_log.sql, which
init_schemas runs before migrations — so fresh installs get the column at
open time and this migration no-ops it. The two indexes live ONLY here (not
in the schema file): the schema file runs before migrations, so an index DDL
there would bind against the pre-V023 table shape on existing databases.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_BACKFILL_SQL = (
    "UPDATE app.audit_log SET operation_id = 'op_legacy_' || audit_id "
    "WHERE operation_id IS NULL"
)


def migrate(conn: object) -> None:
    """Add app.audit_log.operation_id, backfill legacy rows, build indexes."""
    cols: list[tuple[str, bool]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name, is_nullable FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'audit_log'
        """
    ).fetchall()
    col_map: dict[str, bool] = {c[0]: c[1] for c in cols}

    if not col_map:
        # audit_log absent — init_schemas creates it before migrations run, so
        # this is defensive only. Nothing to add; indexes below would fail.
        return

    needs_tighten = False
    if "operation_id" not in col_map:
        logger.info("V023: ADD COLUMN app.audit_log.operation_id")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.audit_log ADD COLUMN operation_id VARCHAR"
        )
        conn.execute(_BACKFILL_SQL)  # type: ignore[union-attr]
        needs_tighten = True
    elif col_map["operation_id"] is True:
        # Column exists but nullable: partial prior run (crash between the two
        # ALTERs) or a hand-altered DB. Backfill any stragglers, then tighten.
        logger.info("V023: backfilling nullable app.audit_log.operation_id")
        conn.execute(_BACKFILL_SQL)  # type: ignore[union-attr]
        needs_tighten = True
    # else: column exists and is NOT NULL (fresh install via schema file) —
    # no add/backfill/tighten, just ensure the indexes below.

    if needs_tighten:
        # Commit the backfill before SET NOT NULL / CREATE INDEX. DuckDB raises
        # on both while the UPDATE's writes are outstanding in the same
        # transaction. A crash between this COMMIT and SET NOT NULL leaves the
        # column added-but-nullable; the next run hits the nullable branch above
        # and finishes (the runner also records a success=false row an operator
        # must clear first; a hard crash leaves no such row).
        conn.execute("COMMIT")  # type: ignore[union-attr]
        conn.execute("BEGIN TRANSACTION")  # type: ignore[union-attr]
        # SET NOT NULL is refused while explicit indexes exist (see module
        # docstring). Snapshot → drop → tighten → restore. duckdb_indexes()
        # omits the PRIMARY KEY index, which does not block the ALTER.
        saved_indexes: list[tuple[str, str]] = conn.execute(  # type: ignore[union-attr]
            "SELECT index_name, sql FROM duckdb_indexes() "
            "WHERE schema_name = 'app' AND table_name = 'audit_log'"
        ).fetchall()
        for index_name, _create_sql in saved_indexes:
            conn.execute(f'DROP INDEX IF EXISTS app."{index_name}"')  # type: ignore[union-attr]  # noqa: S608  # catalog-sourced identifier, quoted
        logger.info("V023: ALTER COLUMN operation_id SET NOT NULL")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.audit_log ALTER COLUMN operation_id SET NOT NULL"
        )
        # create_sql is never NULL here: duckdb_indexes() lists only explicit
        # CREATE INDEX entries (which always carry their DDL) and excludes
        # constraint-backed indexes — PRIMARY KEY and UNIQUE both absent,
        # verified on DuckDB 1.5 — so the snapshot can't contain a NULL-sql row.
        for _index_name, create_sql in saved_indexes:
            conn.execute(create_sql)  # type: ignore[union-attr]

    logger.info("V023: CREATE INDEX (operation_id, occurred_at+operation_id)")
    conn.execute(  # type: ignore[union-attr]
        "CREATE INDEX IF NOT EXISTS idx_audit_log_operation_id "
        "ON app.audit_log (operation_id)"
    )
    conn.execute(  # type: ignore[union-attr]
        "CREATE INDEX IF NOT EXISTS idx_audit_log_occurred_at_op "
        "ON app.audit_log (occurred_at DESC, operation_id)"
    )

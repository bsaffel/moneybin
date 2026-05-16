"""Add updated_at to app.user_categories/user_merchants; tighten override columns to NOT NULL.

Per docs/specs/core-updated-at-convention.md. DuckDB rejects
`ADD COLUMN ... NOT NULL` in one statement, so we ADD with DEFAULT (backfills
every existing row) then SET NOT NULL. The interim COMMIT is required because
DuckDB refuses to create the SET NOT NULL index while backfill writes from
the same transaction are still outstanding. Recovery from a crash between
those two statements goes through the idempotent re-run branch
(`elif col_map["updated_at"] is True`).
"""

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Add/tighten updated_at on four app tables. Idempotent."""
    for table in ("user_categories", "user_merchants"):
        cols: list[tuple[str, bool]] = conn.execute(  # type: ignore[union-attr]
            """
            SELECT column_name, is_nullable FROM duckdb_columns()
            WHERE schema_name = 'app' AND table_name = ?
            """,
            [table],
        ).fetchall()
        col_map: dict[str, bool] = {c[0]: c[1] for c in cols}

        if not col_map:
            # Fresh install: app_user_*.sql will already have created the
            # end-state via init_schemas(). Nothing to do.
            continue

        if "updated_at" not in col_map:
            logger.info(f"Adding updated_at to app.{table}")
            conn.execute(  # type: ignore[union-attr]
                f"ALTER TABLE app.{table} "  # noqa: S608  # allowlisted table names, not user input
                "ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            )
            # Commit the backfill before SET NOT NULL. A crash in the window
            # between this COMMIT and the SET NOT NULL below leaves the column
            # added-but-nullable with a success=false schema_migrations row;
            # recovery is: delete that row, re-run, and the `elif` branch
            # below tightens the constraint.
            conn.execute("COMMIT")  # type: ignore[union-attr]
            conn.execute("BEGIN TRANSACTION")  # type: ignore[union-attr]
            conn.execute(  # type: ignore[union-attr]
                f"ALTER TABLE app.{table} "  # noqa: S608  # allowlisted table names
                "ALTER COLUMN updated_at SET NOT NULL"
            )
        elif col_map["updated_at"] is True:
            # Column exists but is nullable (partial prior run, or a crash
            # between the two ALTERs above). No outstanding writes, so
            # SET NOT NULL is safe inside the runner's enclosing transaction.
            logger.info(f"Tightening app.{table}.updated_at to NOT NULL")
            conn.execute(  # type: ignore[union-attr]
                f"ALTER TABLE app.{table} "  # noqa: S608  # allowlisted table names
                "ALTER COLUMN updated_at SET NOT NULL"
            )

    # category_overrides / merchant_overrides: SET NOT NULL only — no
    # ADD COLUMN, no backfill, no outstanding writes. Safe inside the
    # runner's transaction. (V012 drops merchant_overrides outright.)
    for table in ("category_overrides", "merchant_overrides"):
        cols = conn.execute(  # type: ignore[union-attr]
            """
            SELECT column_name, is_nullable FROM duckdb_columns()
            WHERE schema_name = 'app' AND table_name = ?
            """,
            [table],
        ).fetchall()
        col_map = {c[0]: c[1] for c in cols}

        if not col_map:
            continue

        if col_map.get("updated_at") is True:
            logger.info(f"Tightening app.{table}.updated_at to NOT NULL")
            conn.execute(  # type: ignore[union-attr]
                f"ALTER TABLE app.{table} "  # noqa: S608  # allowlisted table names
                "ALTER COLUMN updated_at SET NOT NULL"
            )

    logger.info("V010 migration complete")

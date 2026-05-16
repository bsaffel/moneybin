"""Add updated_at to app.user_categories/user_merchants; tighten override columns to NOT NULL.

Per docs/specs/core-updated-at-convention.md — every app/core table that
participates in `updated_at` provenance must carry a non-null TIMESTAMP. This
migration brings the four affected app tables in line with the convention.

DuckDB does not support `ADD COLUMN ... NOT NULL` in a single statement
(Parser Error: 'Adding columns with constraints not yet supported'). We add
the column with a DEFAULT — every existing row receives CURRENT_TIMESTAMP —
then SET NOT NULL. Idempotent: re-running detects the end-state and skips.
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
            conn.execute(  # type: ignore[union-attr]
                f"ALTER TABLE app.{table} "  # noqa: S608  # allowlisted table names
                "ALTER COLUMN updated_at SET NOT NULL"
            )
        elif col_map["updated_at"] is True:
            # Column exists but is nullable (partial prior run).
            logger.info(f"Tightening app.{table}.updated_at to NOT NULL")
            conn.execute(  # type: ignore[union-attr]
                f"ALTER TABLE app.{table} "  # noqa: S608  # allowlisted table names
                "ALTER COLUMN updated_at SET NOT NULL"
            )

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

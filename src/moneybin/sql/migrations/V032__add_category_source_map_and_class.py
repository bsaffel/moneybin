"""V032: add app.category_source_map, app.user_categories.class, and seeds.categories.class.

DuckDB rejects `ADD COLUMN ... NOT NULL` in one statement (`Adding columns
with constraints not yet supported`), so the `class` column is added with
just a DEFAULT (backfills every existing row), then tightened to NOT NULL in
a second statement. The interim COMMIT is required because DuckDB refuses to
build the SET NOT NULL constraint while the backfill's writes are still
outstanding in the same transaction — the same two-step dance V010 uses for
`updated_at`. Recovery from a crash between those two statements goes through
the idempotent `elif` branch below.

`seeds.categories` needs the same `class` column so `refresh_views()` (called
right after migrations, on every ``Database`` open) can build
``core.dim_categories`` without a BinderException. V014 unconditionally
`CREATE TABLE IF NOT EXISTS seeds.categories` with the pre-``class`` shape
earlier in the migration chain (every install runs it, fresh or upgrade), so
by the time V032 runs the table always exists and needs the column added.
Rows are backfilled by the same category_id-prefix rule the seed CSV uses
(``INC``/``TRN``/``LNP``/else) rather than a blanket default, since these are
reference rows a real user may already be relying on — not user data with no
inferable class. `seeds.categories` is SQLMesh-owned reference data with no
NOT NULL requirement in the model, so it stays nullable here.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Create app.category_source_map; add class to user_categories + seeds.categories. Idempotent."""
    logger.debug("V032: creating app.category_source_map")
    conn.execute(  # type: ignore[union-attr]
        """
        CREATE TABLE IF NOT EXISTS app.category_source_map (
            source_type VARCHAR NOT NULL,
            source_category_code VARCHAR NOT NULL,
            code_level VARCHAR NOT NULL DEFAULT 'detailed',
            category_id VARCHAR NOT NULL,
            source_taxonomy_version VARCHAR,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_type, source_category_code)
        )
        """
    )
    cols: list[tuple[str, bool]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name, is_nullable FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'user_categories'
        """
    ).fetchall()
    col_map: dict[str, bool] = {c[0]: c[1] for c in cols}

    if "class" not in col_map:
        logger.debug("V032: adding class column to app.user_categories")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.user_categories ADD COLUMN class VARCHAR DEFAULT 'expense'"
        )
        conn.execute("COMMIT")  # type: ignore[union-attr]
        conn.execute("BEGIN TRANSACTION")  # type: ignore[union-attr]
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.user_categories ALTER COLUMN class SET NOT NULL"
        )
    elif col_map["class"] is True:
        logger.debug("V032: tightening app.user_categories.class to NOT NULL")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.user_categories ALTER COLUMN class SET NOT NULL"
        )
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN app.user_categories.class IS 'Accounting class: income | expense | transfer | debt'"
    )

    seed_cols: list[str] = [
        r[0]
        for r in conn.execute(  # type: ignore[union-attr]
            "SELECT column_name FROM duckdb_columns() "
            "WHERE schema_name = 'seeds' AND table_name = 'categories'"
        ).fetchall()
    ]
    if seed_cols and "class" not in seed_cols:
        logger.debug("V032: adding class column to seeds.categories")
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE seeds.categories ADD COLUMN class VARCHAR"
        )
        conn.execute(  # type: ignore[union-attr]
            """
            UPDATE seeds.categories SET class = CASE
                WHEN category_id LIKE 'INC%' THEN 'income'
                WHEN category_id LIKE 'TRN%' THEN 'transfer'
                WHEN category_id LIKE 'LNP%' THEN 'debt'
                ELSE 'expense'
            END
            """
        )
        conn.execute(  # type: ignore[union-attr]
            "COMMENT ON COLUMN seeds.categories.class IS 'Accounting class: income | expense | transfer | debt'"
        )

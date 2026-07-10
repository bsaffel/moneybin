"""V032: add app.category_source_map and app.user_categories.class.

DuckDB rejects both `ADD COLUMN ... NOT NULL` (`Adding columns with
constraints not yet supported`) and `ADD CONSTRAINT CHECK` (`No support for
that ALTER TABLE option yet`) in a single ALTER, and `class` needs both a
NOT NULL and a CHECK — a plain two-step ALTER (add nullable, backfill,
tighten to NOT NULL) has no third step that could bolt on the CHECK. So
`app.user_categories` is rebuilt wholesale: snapshot to a tmp table, DROP,
CREATE with the full target shape (NOT NULL + CHECK baked in), re-INSERT
via an explicit column list (never `SELECT *`, so a future column addition
here can't silently reorder/miscount against the old shape). This mirrors
V015's DROP-CONSTRAINT-via-rebuild pattern. The whole rebuild runs inside
the migration runner's single transaction — no interim COMMIT is needed
because there's no longer a separate ALTER ... SET NOT NULL step that must
follow a durable backfill.

`seeds.categories` is intentionally NOT touched here. It is a SQLMesh SEED
model: on a fully-materialized database it is exposed as a *view* over the
physical snapshot, and `ALTER TABLE seeds.categories` raises `Can only modify
view with ALTER VIEW statement`. The `class` column on the seed data is owned
by SQLMesh (the model declares it) and by `refresh_views()`, which derives
`class` from the `category_id` prefix on the fly whenever the column is absent
(see `moneybin.seeds._has_column`). `core.dim_categories` resolves `class`
either way, so a migration mutating SQLMesh-owned reference data would only
duplicate that pattern — and break on the view.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Create app.category_source_map; add class to app.user_categories. Idempotent."""
    logger.debug("V032: creating app.category_source_map")
    conn.execute(  # type: ignore[union-attr]
        """
        CREATE TABLE IF NOT EXISTS app.category_source_map (
            source_type VARCHAR NOT NULL,
            source_category_code VARCHAR NOT NULL,
            code_level VARCHAR NOT NULL DEFAULT 'detailed'
                CHECK (code_level IN ('detailed', 'primary')),
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
        logger.debug("V032: rebuilding app.user_categories to add class + CHECK")
        conn.execute(  # type: ignore[union-attr]
            "CREATE TABLE app.user_categories__v032_tmp AS "
            "SELECT * FROM app.user_categories"
        )
        conn.execute("DROP TABLE app.user_categories")  # type: ignore[union-attr]
        conn.execute(  # type: ignore[union-attr]
            """
            CREATE TABLE app.user_categories (
                category_id VARCHAR PRIMARY KEY,
                category VARCHAR NOT NULL,
                subcategory VARCHAR,
                description VARCHAR,
                class VARCHAR NOT NULL DEFAULT 'expense'
                    CHECK (class IN ('income', 'expense', 'transfer', 'debt')),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(  # type: ignore[union-attr]
            """
            INSERT INTO app.user_categories
                (category_id, category, subcategory, description,
                 is_active, created_at, updated_at)
            SELECT category_id, category, subcategory, description,
                   is_active, created_at, updated_at
            FROM app.user_categories__v032_tmp
            """
        )
        conn.execute("DROP TABLE app.user_categories__v032_tmp")  # type: ignore[union-attr]
    # class already present -> no-op (idempotent). Column comments are not
    # preserved through the rebuild; the COMMENT ON COLUMN below re-applies it
    # unconditionally (comments also self-heal on the next Database open via
    # _apply_comments, but this keeps it immediate).
    conn.execute(  # type: ignore[union-attr]
        "COMMENT ON COLUMN app.user_categories.class IS 'Accounting class: income | expense | transfer | debt'"
    )

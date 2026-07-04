"""V032: add app.category_source_map, app.user_categories.class, and seeds.categories.class.

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

from moneybin.sql.category_class import CATEGORY_CLASS_FROM_ID_CASE_SQL

logger = logging.getLogger(__name__)


def migrate(conn: object) -> None:
    """Create app.category_source_map; add class to user_categories + seeds.categories. Idempotent."""
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
            f"UPDATE seeds.categories SET class = {CATEGORY_CLASS_FROM_ID_CASE_SQL}"  # noqa: S608  # category_class module constant, not user input
        )
        conn.execute(  # type: ignore[union-attr]
            "COMMENT ON COLUMN seeds.categories.class IS 'Accounting class: income | expense | transfer | debt'"
        )

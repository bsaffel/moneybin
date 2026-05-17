"""V015: drop the UNIQUE (category, subcategory) constraint on user_categories.

The constraint was a defensive measure against duplicate user-created
categories when text was the de-facto reference key. Now that V014 has
introduced category_id FK columns across every consumer, the text
columns are display snapshots only and the unique constraint actively
blocks legitimate cases (e.g., two users wanting "Hobbies / Music" with
different metadata, or future merge/split flows).

The category_id PK remains the uniqueness contract. The service layer
(MatchApplier.create_category) performs the text-duplicate check
explicitly for the cases that should remain disallowed today.

DuckDB does not support DROP CONSTRAINT directly; the migration uses the
documented workaround of rebuilding the table without the constraint.
"""

import logging
from typing import cast

logger = logging.getLogger(__name__)

_TARGET_UNIQUE_COLUMNS = sorted(["category", "subcategory"])


def migrate(conn: object) -> None:
    """Rebuild app.user_categories without UNIQUE (category, subcategory)."""
    constraints = cast(
        list[tuple[list[str]]],
        conn.execute(  # type: ignore[union-attr]
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE schema_name = 'app' AND table_name = 'user_categories' "
            "AND constraint_type = 'UNIQUE'"
        ).fetchall(),
    )
    # Sort both sides so the match doesn't depend on DuckDB's declaration-order
    # promise in duckdb_constraints.constraint_column_names.
    has_unique = any(sorted(cols) == _TARGET_UNIQUE_COLUMNS for (cols,) in constraints)
    if not has_unique:
        logger.info("V015: UNIQUE constraint absent; skipping rebuild")
        return

    conn.execute(  # type: ignore[union-attr]
        "CREATE TABLE app.user_categories__v015_tmp AS "
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
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(  # type: ignore[union-attr]
        "INSERT INTO app.user_categories SELECT * FROM app.user_categories__v015_tmp"
    )
    conn.execute("DROP TABLE app.user_categories__v015_tmp")  # type: ignore[union-attr]
    # Column comments on app.user_categories are not preserved through the
    # CREATE TABLE + INSERT SELECT rebuild; they self-heal on the next
    # Database(...) open via init_schemas -> _apply_comments.

    logger.info("V015 migration complete (UNIQUE constraint dropped)")

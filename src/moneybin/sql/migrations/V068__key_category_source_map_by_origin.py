"""V068: key app.category_source_map by the row's source_type and source_origin.

``source_type`` was filled from two unrelated name spaces: a provider tag for
provider rows and the import's ``source_origin`` slug for imported rows, so an
imported account whose slug is ``plaid`` collided with Plaid's own mappings.
The key becomes ``(source_type, source_origin, source_category_code,
source_subcategory_code)`` and ``source_type`` holds the transaction row's own
``source_type``. A blank ``source_origin`` marks a provider-wide row, like a
blank subcategory marks "none".

A ``plaid`` row is a provider row and backfills ``source_origin = ''``. Any
other row is an imported mapping whose old ``source_type`` is really an origin
slug; the old key applied it to every row of that origin, so it is re-keyed
once per source type the raw tables hold for that origin. An imported mapping
whose origin no raw row carries has no source type to take and is dropped —
its term returns to the pending inbox if those rows are imported again.

DuckDB cannot ``ALTER`` a primary key, so the table is rebuilt via a tmp-table
snapshot, mirroring V067.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE app.category_source_map (
    source_type VARCHAR NOT NULL,
    source_origin VARCHAR NOT NULL,
    source_category_code VARCHAR NOT NULL,
    source_subcategory_code VARCHAR NOT NULL DEFAULT '',
    code_level VARCHAR NOT NULL DEFAULT 'detailed'
        CHECK (code_level IN ('detailed', 'primary')),
    category_id VARCHAR NOT NULL,
    source_taxonomy_version VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        source_type, source_origin, source_category_code, source_subcategory_code
    )
)
"""

_TMP = "app.category_source_map__v068_tmp"

# Explicit column lists (not `SELECT *`) so the two shapes can't silently
# misalign — same reason as V067.
_CARRIED_COLUMNS = (
    "source_category_code, source_subcategory_code, code_level, category_id, "
    "source_taxonomy_version, created_at, updated_at"
)

# Raw tables that hold imported rows carrying their own category text.
_IMPORTED_RAW_TABLES = ("tabular_transactions", "manual_transactions")


def migrate(conn: object) -> None:
    """Rebuild app.category_source_map with the origin-keyed shape. Idempotent."""
    cols: list[tuple[str]] = conn.execute(  # type: ignore[attr-defined]
        """
        SELECT column_name FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'category_source_map'
        """
    ).fetchall()
    if "source_origin" in {name for (name,) in cols}:
        logger.debug("V068: source_origin already present, skipping")
        return

    logger.debug("V068: rebuild app.category_source_map keyed by source_origin")
    conn.execute(  # type: ignore[attr-defined]
        f"CREATE TABLE {_TMP} AS SELECT * FROM app.category_source_map"  # noqa: S608  # module constant, no user input
    )
    conn.execute("DROP TABLE app.category_source_map")  # type: ignore[attr-defined]
    conn.execute(_CREATE_TABLE)  # type: ignore[attr-defined]
    conn.execute(  # type: ignore[attr-defined]
        f"""
        INSERT INTO app.category_source_map
            (source_type, source_origin, {_CARRIED_COLUMNS})
        SELECT source_type, '', {_CARRIED_COLUMNS}
        FROM {_TMP}
        WHERE source_type = 'plaid'
        """  # noqa: S608  # module constants, no user input
    )

    present: list[tuple[str]] = conn.execute(  # type: ignore[attr-defined]
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'raw'"
    ).fetchall()
    carriers = " UNION ".join(
        f"SELECT source_type, source_origin FROM raw.{table}"  # noqa: S608  # hardcoded table names, no user input
        for table in _IMPORTED_RAW_TABLES
        if (table,) in present
    )
    if carriers:
        conn.execute(  # type: ignore[attr-defined]
            f"""
            INSERT INTO app.category_source_map
                (source_type, source_origin, {_CARRIED_COLUMNS})
            SELECT r.source_type, t.source_type, {_CARRIED_COLUMNS}
            FROM {_TMP} AS t
            JOIN ({carriers}) AS r ON r.source_origin = t.source_type
            WHERE t.source_type <> 'plaid'
            """  # noqa: S608  # module constants, no user input
        )
    conn.execute(f"DROP TABLE {_TMP}")  # type: ignore[attr-defined]
    logger.debug("V068: keyed app.category_source_map by source_origin")

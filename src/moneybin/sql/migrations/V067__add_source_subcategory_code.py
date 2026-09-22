"""V067: widen app.category_source_map's key with source_subcategory_code.

The table's key holds a provider/exporter (category, subcategory) pair, but
``source_category_code`` is a single string. The prior encoding packed the
pair into that one column via ``to_json({'category': ..., 'subcategory':
...})`` — but the predicate that reads it (``b.source_category_code =
to_json({...})``) compares a VARCHAR column against a JSON-typed literal,
and DuckDB resolves that comparison by casting the VARCHAR to JSON. Seeded
Plaid rows hold bare codes like ``INCOME``, which are not valid JSON, so the
cast raised ``ConversionException`` and broke every categorization run that
touched the bridge with seeded data present. A real second key column
replaces the encoding.

DuckDB rejects NULL in a primary key, so an absent subcategory cannot be
stored as NULL — ``source_subcategory_code`` uses ``''`` as the sentinel for
"the source supplied no subcategory," matching how this codebase already
treats blank taxonomy text (V054, V055, V056 backfill/clear blank category
strings).

DuckDB cannot ``ALTER`` a primary key, so the table is rebuilt wholesale,
mirroring V061's tmp-table precedent: snapshot to
``category_source_map__v067_tmp``, DROP, CREATE with the new shape, re-INSERT
via an explicit column list supplying ``''`` for the new column, DROP tmp.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE = """
CREATE TABLE app.category_source_map (
    source_type VARCHAR NOT NULL,
    source_category_code VARCHAR NOT NULL,
    source_subcategory_code VARCHAR NOT NULL DEFAULT '',
    code_level VARCHAR NOT NULL DEFAULT 'detailed'
        CHECK (code_level IN ('detailed', 'primary')),
    category_id VARCHAR NOT NULL,
    source_taxonomy_version VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_type, source_category_code, source_subcategory_code)
)
"""

# Explicit column list (not `SELECT *`) so a future column addition on one
# side can't silently reorder/miscount against the old shape — matches
# V066's precedent for the same reason.
_OLD_COLUMNS = (
    "source_type",
    "source_category_code",
    "code_level",
    "category_id",
    "source_taxonomy_version",
    "created_at",
    "updated_at",
)


def migrate(conn: object) -> None:
    """Rebuild app.category_source_map with the widened key. Idempotent."""
    cols: list[tuple[str]] = conn.execute(  # type: ignore[attr-defined]
        """
        SELECT column_name FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'category_source_map'
        """
    ).fetchall()
    existing_columns: set[str] = {name for (name,) in cols}
    if "source_subcategory_code" in existing_columns:
        logger.debug("V067: source_subcategory_code already present, skipping")
        return

    logger.debug("V067: widen app.category_source_map's key")
    conn.execute(  # type: ignore[attr-defined]
        "CREATE TABLE app.category_source_map__v067_tmp AS "
        "SELECT * FROM app.category_source_map"
    )
    conn.execute("DROP TABLE app.category_source_map")  # type: ignore[attr-defined]
    conn.execute(_CREATE_TABLE)  # type: ignore[attr-defined]
    old_column_list = ", ".join(_OLD_COLUMNS)
    conn.execute(  # type: ignore[attr-defined]  # allowlisted literal, no user input
        f"INSERT INTO app.category_source_map ({old_column_list}, "  # noqa: S608  # column_list built from the hardcoded _OLD_COLUMNS tuple, not user input
        "source_subcategory_code) "
        f"SELECT {old_column_list}, '' FROM app.category_source_map__v067_tmp"
    )
    conn.execute("DROP TABLE app.category_source_map__v067_tmp")  # type: ignore[attr-defined]
    logger.info(
        "V067: widened app.category_source_map's key with source_subcategory_code"
    )

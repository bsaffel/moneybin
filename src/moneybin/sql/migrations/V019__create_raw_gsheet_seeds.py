"""V019: create raw.gsheet_seeds.

Row-level storage for the gsheet seed (catch-all) adapter. One row per
(connection_id, row_hash); the JSON ``data`` column captures the source
row verbatim and per-connection auto-generated views in
``raw.gsheet_<alias>`` project the JSON paths into typed columns. Diff
semantics mirror the transactions adapter — recomputed row_hash on each
pull drives no-op / undelete / soft-delete / insert decisions.

The same DDL also ships in ``src/moneybin/sql/schema/raw_gsheet_seeds.sql``
which ``init_schemas`` runs on every Database open. Fresh installs get
the table at open time; pre-existing databases get it via this
migration. ``CREATE TABLE IF NOT EXISTS`` keeps both paths idempotent.

Pure additive DDL — no backfill, no reshape. The migration is a no-op
when the schema file has already created the table.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.gsheet_seeds (
    connection_id VARCHAR NOT NULL,
    spreadsheet_id VARCHAR NOT NULL,
    sheet_gid INTEGER NOT NULL,
    row_number INTEGER NOT NULL,
    row_hash VARCHAR NOT NULL,
    data JSON NOT NULL,
    deleted_from_source_at TIMESTAMP NULL,
    import_id VARCHAR NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (connection_id, row_hash)
)
"""

# (column_name, comment_text) — applied as COMMENT ON COLUMN after CREATE.
# COMMENT ON COLUMN replaces existing comments, so this is safe to re-run.
_COLUMN_COMMENTS: list[tuple[str, str]] = [
    (
        "connection_id",
        "FK reference to app.gsheet_connections.connection_id (logical FK; "
        "not enforced cross-schema)",
    ),
    (
        "spreadsheet_id",
        "Denormalized from app.gsheet_connections for query convenience",
    ),
    (
        "sheet_gid",
        "Denormalized tab gid",
    ),
    (
        "row_number",
        "1-based row index in the source sheet (informational; may shift on edits)",
    ),
    (
        "row_hash",
        "Content hash of the row's cell values (stable identity for the seed adapter)",
    ),
    (
        "data",
        "JSON object: column-name -> cell-value, captured verbatim from the sheet",
    ),
    (
        "deleted_from_source_at",
        "Set when the row is observed absent on a subsequent pull "
        "(soft-delete; mirrors the raw.tabular_transactions pattern)",
    ),
    (
        "import_id",
        "Import run that wrote this row (FK to import_runs)",
    ),
    (
        "loaded_at",
        "Timestamp this row was first observed (does NOT update on subsequent pulls)",
    ),
]


def migrate(conn: object) -> None:
    """Create raw.gsheet_seeds + apply column comments. Idempotent."""
    logger.info("V019: CREATE TABLE IF NOT EXISTS raw.gsheet_seeds")
    conn.execute(_CREATE_TABLE_SQL)  # type: ignore[union-attr]

    for column, comment in _COLUMN_COMMENTS:
        # COMMENT ON COLUMN does not accept parameterized values; inline a
        # single-quoted literal with standard SQL escaping (double the
        # single quote). column names come from the static _COLUMN_COMMENTS
        # list, not user input.
        escaped = comment.replace("'", "''")
        conn.execute(  # type: ignore[union-attr]
            f"COMMENT ON COLUMN raw.gsheet_seeds.{column} "  # noqa: S608  # static identifier + escaped literal
            f"IS '{escaped}'"
        )

    logger.info("V019: raw.gsheet_seeds ready")

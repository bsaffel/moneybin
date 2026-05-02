"""Add import_id, source_type, source_origin to raw.ofx_* tables.

Brings OFX raw lineage into parity with the tabular import-batch model.
Existing rows get source_type='ofx' backfilled (literal value) and
import_id/source_origin left NULL — these are 'pre-batch-tracking' rows
that cannot be reverted via import revert.

Idempotent: skips columns that already exist on a fresh install.
"""

_TABLE_COLUMNS = {
    "raw.ofx_transactions": ["import_id", "source_type", "source_origin"],
    "raw.ofx_accounts": ["import_id", "source_type"],
    "raw.ofx_balances": ["import_id", "source_type"],
    "raw.ofx_institutions": ["import_id", "source_type"],
}


def _column_exists(conn: object, schema: str, table: str, column: str) -> bool:
    return (
        conn.execute(  # type: ignore[union-attr]
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = ?
              AND table_name = ?
              AND column_name = ?
            """,
            [schema, table, column],
        ).fetchone()[0]
        > 0
    )


def migrate(conn: object) -> None:
    """Add the new columns to raw.ofx_* tables and backfill source_type='ofx'."""
    for qualified_table, columns in _TABLE_COLUMNS.items():
        schema, table = qualified_table.split(".", 1)
        for column in columns:
            if _column_exists(conn, schema, table, column):
                continue
            if column == "source_type":
                conn.execute(  # type: ignore[union-attr]
                    f"ALTER TABLE {qualified_table} ADD COLUMN {column} VARCHAR DEFAULT 'ofx'"  # noqa: S608  # identifiers from compile-time allowlist, not user input
                )
                # Backfill existing rows; DEFAULT only applies to new inserts.
                conn.execute(  # type: ignore[union-attr]
                    f"UPDATE {qualified_table} SET source_type = 'ofx' WHERE source_type IS NULL"  # noqa: S608  # identifiers from compile-time allowlist, not user input
                )
            else:
                conn.execute(  # type: ignore[union-attr]
                    f"ALTER TABLE {qualified_table} ADD COLUMN {column} VARCHAR"  # noqa: S608  # identifiers from compile-time allowlist, not user input
                )

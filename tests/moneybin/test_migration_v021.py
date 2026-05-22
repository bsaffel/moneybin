"""V021: create raw.gsheet_seeds.

Phase-2 schema work for connect-gsheet. This migration creates the
row-level storage table for the seed (catch-all) adapter — one row per
(connection_id, row_hash), with the JSON ``data`` column capturing the
source row verbatim and per-connection views projecting JSON paths into
typed columns.

The schema file (``raw_gsheet_seeds.sql``) is registered in
``init_schemas`` so fresh databases get the table at open time; the V021
migration is the backstop for pre-existing databases being upgraded.
Both paths produce the same shape — ``CREATE TABLE IF NOT EXISTS`` keeps
the migration idempotent.

Pure additive DDL (new table) — fixtures are not required by
``.claude/rules/database.md``, but PK/round-trip tests still need to
insert representative rows to verify the schema actually enforces
identity and accepts the expected types.
"""

from __future__ import annotations

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V021__create_raw_gsheet_seeds import migrate
from tests.moneybin.migration_helpers import column_exists, run_migration

_ALL_COLUMNS: tuple[str, ...] = (
    "connection_id",
    "spreadsheet_id",
    "sheet_gid",
    "row_number",
    "row_hash",
    "data",
    "deleted_from_source_at",
    "import_id",
    "loaded_at",
)


def _insert_seed(
    db: Database,
    *,
    connection_id: str,
    row_hash: str,
    spreadsheet_id: str = "1AbCDefGhIjKlMnOpQrStUvWxYz0123456789ABCDE",
    sheet_gid: int = 0,
    row_number: int = 1,
    data: str = '{"Name":"Netflix","Amount":"15.99"}',
    import_id: str = "imp-0001",
) -> None:
    """Insert a seed row with minimal required fields populated."""
    db.execute(
        "INSERT INTO raw.gsheet_seeds "
        "(connection_id, spreadsheet_id, sheet_gid, row_number, row_hash, "
        " data, import_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            connection_id,
            spreadsheet_id,
            sheet_gid,
            row_number,
            row_hash,
            data,
            import_id,
        ],
    )


@pytest.fixture()
def v019_db(db: Database) -> Database:
    """Database with V021 applied (idempotent on top of init_schemas)."""
    run_migration(db, migrate)
    return db


class TestV021CreateRawGsheetSeeds:
    """V021 creates raw.gsheet_seeds — table, columns, primary key."""

    def test_table_exists_after_migration(self, v019_db: Database) -> None:
        row = v019_db.execute(
            "SELECT 1 FROM duckdb_tables() "
            "WHERE schema_name = 'raw' AND table_name = 'gsheet_seeds'"
        ).fetchone()
        assert row is not None

    def test_all_columns_present(self, v019_db: Database) -> None:
        for col in _ALL_COLUMNS:
            assert column_exists(v019_db, "raw", "gsheet_seeds", col), (
                f"missing column: {col}"
            )
        # Sanity check: count matches the spec exactly (no extras, no drops).
        count_row = v019_db.execute(
            "SELECT COUNT(*) FROM duckdb_columns() "
            "WHERE schema_name = 'raw' AND table_name = 'gsheet_seeds'"
        ).fetchone()
        assert count_row is not None
        assert count_row[0] == len(_ALL_COLUMNS)

    def test_primary_key_is_connection_id_row_hash(self, v019_db: Database) -> None:
        row = v019_db.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE schema_name = 'raw' AND table_name = 'gsheet_seeds' "
            "AND constraint_type = 'PRIMARY KEY'"
        ).fetchone()
        assert row is not None
        (pk_cols,) = row
        assert list(pk_cols) == ["connection_id", "row_hash"]

    def test_pk_violation_on_duplicate_connection_id_row_hash(
        self, v019_db: Database
    ) -> None:
        _insert_seed(v019_db, connection_id="conn-a", row_hash="hash-1")
        with pytest.raises(duckdb.ConstraintException):
            _insert_seed(v019_db, connection_id="conn-a", row_hash="hash-1")

    def test_pk_allows_same_row_hash_across_connections(
        self, v019_db: Database
    ) -> None:
        """Same row_hash under different connection_id is allowed by the composite PK."""
        _insert_seed(v019_db, connection_id="conn-1", row_hash="hash-shared")
        _insert_seed(v019_db, connection_id="conn-2", row_hash="hash-shared")
        count_row = v019_db.execute(
            "SELECT COUNT(*) FROM raw.gsheet_seeds WHERE row_hash = ?",
            ["hash-shared"],
        ).fetchone()
        assert count_row is not None
        assert count_row[0] == 2

    def test_data_column_accepts_json(self, v019_db: Database) -> None:
        _insert_seed(
            v019_db,
            connection_id="conn-json",
            row_hash="hash-json",
            data='{"col":"val"}',
        )
        row = v019_db.execute(
            "SELECT data FROM raw.gsheet_seeds WHERE connection_id = ?",
            ["conn-json"],
        ).fetchone()
        assert row is not None
        # DuckDB JSON round-trips as a string; canonical form preserves the content.
        assert row[0] == '{"col":"val"}'

    def test_deleted_from_source_at_nullable(self, v019_db: Database) -> None:
        _insert_seed(v019_db, connection_id="conn-null", row_hash="hash-null")
        row = v019_db.execute(
            "SELECT deleted_from_source_at FROM raw.gsheet_seeds "
            "WHERE connection_id = ?",
            ["conn-null"],
        ).fetchone()
        assert row is not None
        assert row[0] is None

    def test_loaded_at_default_current_timestamp(self, v019_db: Database) -> None:
        _insert_seed(v019_db, connection_id="conn-ts", row_hash="hash-ts")
        row = v019_db.execute(
            "SELECT loaded_at FROM raw.gsheet_seeds WHERE connection_id = ?",
            ["conn-ts"],
        ).fetchone()
        assert row is not None
        assert row[0] is not None

    def test_idempotent(self, v019_db: Database) -> None:
        """Re-running the migration on an already-migrated DB is harmless."""
        run_migration(v019_db, migrate)
        assert column_exists(v019_db, "raw", "gsheet_seeds", "connection_id")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

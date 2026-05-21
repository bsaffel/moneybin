"""V020: create app.gsheet_connections.

Phase-2 schema work for connect-gsheet. This migration creates the
central state table that holds every gsheet connection's identity,
adapter choice, pinned column mapping, drift signature, and health
status.

The schema file (``app_gsheet_connections.sql``) is registered in
``init_schemas`` so fresh databases get the table at open time; the V020
migration is the backstop for pre-existing databases being upgraded.
Both paths produce the same shape — ``CREATE TABLE IF NOT EXISTS`` keeps
the migration idempotent.

Pure additive DDL (new table) — fixtures are not required by
``.claude/rules/database.md``, but constraint/check tests still need to
insert representative rows to verify the schema actually rejects invalid
inputs (not just that the CHECK syntax parses).
"""

from __future__ import annotations

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V020__create_app_gsheet_connections import migrate
from tests.moneybin.migration_helpers import column_exists, run_migration

_ALL_COLUMNS: tuple[str, ...] = (
    "connection_id",
    "spreadsheet_id",
    "sheet_gid",
    "sheet_name",
    "workbook_name",
    "adapter",
    "account_id",
    "account_name",
    "column_mapping",
    "header_signature",
    "date_format",
    "sign_convention",
    "number_format",
    "skip_rows",
    "skip_trailing_patterns",
    "status",
    "last_pull_at",
    "last_pull_import_id",
    "last_success_at",
    "last_drift_reason",
    "consecutive_failure_count",
    "alias",
    "created_at",
    "updated_at",
)

_VALID_STATUSES: tuple[str, ...] = (
    "healthy",
    "auth_expired",
    "unreachable",
    "drift_detected",
    "rate_limited",
    "disconnected",
)


def _insert_connection(
    db: Database,
    *,
    connection_id: str,
    spreadsheet_id: str = "1AbCDefGhIjKlMnOpQrStUvWxYz0123456789ABCDE",
    sheet_gid: int = 0,
    sheet_name: str = "Transactions",
    workbook_name: str = "Personal Finance",
    adapter: str = "transactions",
    column_mapping: str = '{"Date":"date","Amount":"amount"}',
    header_signature: str = '["Date","Amount","Description"]',
    alias: str | None = None,
    status: str | None = None,
) -> None:
    """Insert a connection row with minimal required fields populated."""
    cols = [
        "connection_id",
        "spreadsheet_id",
        "sheet_gid",
        "sheet_name",
        "workbook_name",
        "adapter",
        "column_mapping",
        "header_signature",
        "alias",
    ]
    vals: list[object] = [
        connection_id,
        spreadsheet_id,
        sheet_gid,
        sheet_name,
        workbook_name,
        adapter,
        column_mapping,
        header_signature,
        alias,
    ]
    if status is not None:
        cols.append("status")
        vals.append(status)
    placeholders = ", ".join("?" * len(cols))
    col_list = ", ".join(cols)
    db.execute(
        f"INSERT INTO app.gsheet_connections ({col_list}) VALUES ({placeholders})",  # noqa: S608  # test input, column list is allowlisted
        vals,
    )


@pytest.fixture()
def v018_db(db: Database) -> Database:
    """Database with V020 applied (idempotent on top of init_schemas)."""
    run_migration(db, migrate)
    return db


class TestV020CreateAppGsheetConnections:
    """V020 creates app.gsheet_connections — table, columns, constraints."""

    def test_table_exists_after_migration(self, v018_db: Database) -> None:
        row = v018_db.execute(
            "SELECT 1 FROM duckdb_tables() "
            "WHERE schema_name = 'app' AND table_name = 'gsheet_connections'"
        ).fetchone()
        assert row is not None

    def test_all_columns_present(self, v018_db: Database) -> None:
        for col in _ALL_COLUMNS:
            assert column_exists(v018_db, "app", "gsheet_connections", col), (
                f"missing column: {col}"
            )
        # Sanity check: count matches the spec exactly (no extras, no drops).
        count_row = v018_db.execute(
            "SELECT COUNT(*) FROM duckdb_columns() "
            "WHERE schema_name = 'app' AND table_name = 'gsheet_connections'"
        ).fetchone()
        assert count_row is not None
        assert count_row[0] == len(_ALL_COLUMNS)

    def test_connection_id_is_primary_key(self, v018_db: Database) -> None:
        row = v018_db.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE schema_name = 'app' AND table_name = 'gsheet_connections' "
            "AND constraint_type = 'PRIMARY KEY'"
        ).fetchone()
        assert row is not None
        (pk_cols,) = row
        assert list(pk_cols) == ["connection_id"]

    def test_unique_spreadsheet_id_sheet_gid(self, v018_db: Database) -> None:
        _insert_connection(v018_db, connection_id="conn-a", spreadsheet_id="ssid-1")
        with pytest.raises(duckdb.ConstraintException):
            _insert_connection(v018_db, connection_id="conn-b", spreadsheet_id="ssid-1")

    def test_unique_alias(self, v018_db: Database) -> None:
        _insert_connection(
            v018_db,
            connection_id="conn-c",
            spreadsheet_id="ssid-2",
            sheet_gid=1,
            alias="grocery-budget",
        )
        with pytest.raises(duckdb.ConstraintException):
            _insert_connection(
                v018_db,
                connection_id="conn-d",
                spreadsheet_id="ssid-3",
                sheet_gid=2,
                alias="grocery-budget",
            )

    def test_adapter_check_accepts_valid(self, v018_db: Database) -> None:
        """Both 'transactions' and 'seed' are accepted adapter values."""
        _insert_connection(
            v018_db,
            connection_id="conn-tx",
            spreadsheet_id="ssid-tx",
            adapter="transactions",
        )
        _insert_connection(
            v018_db,
            connection_id="conn-seed",
            spreadsheet_id="ssid-seed",
            adapter="seed",
            alias="seed-1",
        )

    def test_adapter_check_rejects_invalid(self, v018_db: Database) -> None:
        with pytest.raises(duckdb.ConstraintException):
            _insert_connection(
                v018_db,
                connection_id="conn-bad",
                spreadsheet_id="ssid-bad",
                adapter="other",
            )

    def test_status_check_accepts_all_six(self, v018_db: Database) -> None:
        for i, status in enumerate(_VALID_STATUSES):
            _insert_connection(
                v018_db,
                connection_id=f"conn-status-{i}",
                spreadsheet_id=f"ssid-status-{i}",
                status=status,
            )

    def test_status_check_rejects_invalid(self, v018_db: Database) -> None:
        with pytest.raises(duckdb.ConstraintException):
            _insert_connection(
                v018_db,
                connection_id="conn-bad-status",
                spreadsheet_id="ssid-bad-status",
                status="bogus",
            )

    def test_status_default_is_healthy(self, v018_db: Database) -> None:
        _insert_connection(
            v018_db, connection_id="conn-default", spreadsheet_id="ssid-default"
        )
        row = v018_db.execute(
            "SELECT status FROM app.gsheet_connections WHERE connection_id = ?",
            ["conn-default"],
        ).fetchone()
        assert row is not None
        assert row[0] == "healthy"

    def test_consecutive_failure_count_default_zero(self, v018_db: Database) -> None:
        _insert_connection(
            v018_db, connection_id="conn-failcnt", spreadsheet_id="ssid-failcnt"
        )
        row = v018_db.execute(
            "SELECT consecutive_failure_count FROM app.gsheet_connections "
            "WHERE connection_id = ?",
            ["conn-failcnt"],
        ).fetchone()
        assert row is not None
        assert row[0] == 0

    def test_skip_rows_default_zero(self, v018_db: Database) -> None:
        _insert_connection(
            v018_db, connection_id="conn-skiprows", spreadsheet_id="ssid-skiprows"
        )
        row = v018_db.execute(
            "SELECT skip_rows FROM app.gsheet_connections WHERE connection_id = ?",
            ["conn-skiprows"],
        ).fetchone()
        assert row is not None
        assert row[0] == 0

    def test_idempotent(self, v018_db: Database) -> None:
        """Re-running the migration on an already-migrated DB is harmless."""
        run_migration(v018_db, migrate)
        assert column_exists(v018_db, "app", "gsheet_connections", "connection_id")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

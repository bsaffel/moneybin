"""V022: create app.ai_consent_grants — table + columns + CHECK constraint."""

from __future__ import annotations

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V022__create_app_ai_consent_grants import migrate
from tests.moneybin.migration_helpers import column_exists, run_migration

_ALL_COLUMNS: tuple[str, ...] = (
    "grant_id",
    "feature_category",
    "backend",
    "consent_mode",
    "granted_at",
    "revoked_at",
    "grant_prompt",
)


@pytest.fixture()
def migrated_db(db: Database) -> Database:
    """Database with V022 applied (idempotent on top of init_schemas)."""
    run_migration(db, migrate)
    return db


class TestV022CreateAppAiConsentGrants:
    """V022 creates app.ai_consent_grants — table, columns, constraints."""

    def test_table_exists_after_migration(self, migrated_db: Database) -> None:
        row = migrated_db.execute(
            "SELECT 1 FROM duckdb_tables() "
            "WHERE schema_name = 'app' AND table_name = 'ai_consent_grants'"
        ).fetchone()
        assert row is not None

    def test_all_columns_present(self, migrated_db: Database) -> None:
        for col in _ALL_COLUMNS:
            assert column_exists(migrated_db, "app", "ai_consent_grants", col), (
                f"missing column: {col}"
            )
        count_row = migrated_db.execute(
            "SELECT COUNT(*) FROM duckdb_columns() "
            "WHERE schema_name = 'app' AND table_name = 'ai_consent_grants'"
        ).fetchone()
        assert count_row is not None
        assert count_row[0] == len(_ALL_COLUMNS)

    def test_grant_id_is_primary_key(self, migrated_db: Database) -> None:
        row = migrated_db.execute(
            "SELECT constraint_column_names FROM duckdb_constraints() "
            "WHERE schema_name = 'app' AND table_name = 'ai_consent_grants' "
            "AND constraint_type = 'PRIMARY KEY'"
        ).fetchone()
        assert row is not None
        (pk_cols,) = row
        assert list(pk_cols) == ["grant_id"]

    def test_consent_mode_check_rejects_invalid(self, migrated_db: Database) -> None:
        with pytest.raises(duckdb.ConstraintException):
            migrated_db.execute(
                "INSERT INTO app.ai_consent_grants "
                "(grant_id, feature_category, backend, consent_mode, grant_prompt) "
                "VALUES ('g1', 'mcp-data-sharing', 'anthropic', 'forever', 'prompt')"  # noqa: S608  # test input, not executing user SQL
            )

    def test_consent_mode_check_accepts_valid(self, migrated_db: Database) -> None:
        for i, mode in enumerate(("persistent", "one-time")):
            migrated_db.execute(
                "INSERT INTO app.ai_consent_grants "
                "(grant_id, feature_category, backend, consent_mode, grant_prompt) "
                "VALUES (?, 'mcp-data-sharing', 'anthropic', ?, 'prompt')",
                [f"g{i}", mode],
            )

    def test_idempotent(self, migrated_db: Database) -> None:
        run_migration(migrated_db, migrate)
        assert column_exists(migrated_db, "app", "ai_consent_grants", "grant_id")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

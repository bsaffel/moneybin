"""Tests for the database migration system."""

from moneybin.database import Database


class TestMigrationSchema:
    """Verify migration tracking tables are created by init_schemas."""

    def test_schema_migrations_table_exists(self, db: Database) -> None:
        """app.schema_migrations table is created during init."""
        result = db.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = 'schema_migrations' "
            "ORDER BY ordinal_position"
        ).fetchall()
        columns = {row[0]: row[1] for row in result}
        assert "version" in columns
        assert "filename" in columns
        assert "checksum" in columns
        assert "success" in columns
        assert "execution_ms" in columns
        assert "applied_at" in columns

    def test_versions_table_exists(self, db: Database) -> None:
        """app.versions table is created during init."""
        result = db.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = 'versions' "
            "ORDER BY ordinal_position"
        ).fetchall()
        columns = {row[0]: row[1] for row in result}
        assert "component" in columns
        assert "version" in columns
        assert "previous_version" in columns
        assert "updated_at" in columns
        assert "installed_at" in columns

    def test_analytics_schema_exists(self, db: Database) -> None:
        """Analytics schema is created during init."""
        result = db.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = 'analytics'"
        ).fetchall()
        assert len(result) == 1

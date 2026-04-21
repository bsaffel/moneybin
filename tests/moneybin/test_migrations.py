"""Tests for the database migration system."""

import hashlib
from pathlib import Path

import pytest

from moneybin.database import Database
from moneybin.migrations import Migration, discover_migrations


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


class TestMigrationDataclass:
    """Migration dataclass parsing and checksum computation."""

    def test_parse_sql_filename(self, tmp_path: Path) -> None:
        """Parse version and name from V###__name.sql filename."""
        sql_file = tmp_path / "V001__create_foo.sql"
        sql_file.write_text("CREATE TABLE foo (id INTEGER);")
        m = Migration.from_file(sql_file)
        assert m.version == 1
        assert m.name == "create_foo"
        assert m.filename == "V001__create_foo.sql"
        assert m.file_type == "sql"

    def test_parse_python_filename(self, tmp_path: Path) -> None:
        """Parse version and name from V###__name.py filename."""
        py_file = tmp_path / "V002__backfill_data.py"
        py_file.write_text("def migrate(conn):\n    pass\n")
        m = Migration.from_file(py_file)
        assert m.version == 2
        assert m.name == "backfill_data"
        assert m.file_type == "py"

    def test_checksum_is_deterministic(self, tmp_path: Path) -> None:
        """Same file content produces same checksum."""
        sql_file = tmp_path / "V001__test.sql"
        sql_file.write_text("SELECT 1;")
        m = Migration.from_file(sql_file)
        expected = hashlib.sha256(b"SELECT 1;").hexdigest()
        assert m.checksum == expected
        assert len(m.checksum) == 64

    def test_different_content_different_checksum(self, tmp_path: Path) -> None:
        """Different content produces different checksum."""
        f1 = tmp_path / "V001__a.sql"
        f1.write_text("SELECT 1;")
        f2 = tmp_path / "V002__b.sql"
        f2.write_text("SELECT 2;")
        assert Migration.from_file(f1).checksum != Migration.from_file(f2).checksum

    def test_rejects_malformed_name_no_prefix(self, tmp_path: Path) -> None:
        """Filenames without V### prefix are rejected."""
        bad = tmp_path / "create_foo.sql"
        bad.write_text("SELECT 1;")
        with pytest.raises(ValueError, match="does not match"):
            Migration.from_file(bad)

    def test_rejects_malformed_name_single_underscore(self, tmp_path: Path) -> None:
        """Filenames with single underscore separator are rejected."""
        bad = tmp_path / "V001_create_foo.sql"
        bad.write_text("SELECT 1;")
        with pytest.raises(ValueError, match="does not match"):
            Migration.from_file(bad)

    def test_rejects_unsupported_extension(self, tmp_path: Path) -> None:
        """Only .sql and .py extensions are supported."""
        bad = tmp_path / "V001__create_foo.txt"
        bad.write_text("data")
        with pytest.raises(ValueError, match="does not match"):
            Migration.from_file(bad)

    def test_multi_digit_version(self, tmp_path: Path) -> None:
        """Version numbers with more than 3 digits are accepted."""
        sql_file = tmp_path / "V1234__big_version.sql"
        sql_file.write_text("SELECT 1;")
        m = Migration.from_file(sql_file)
        assert m.version == 1234


class TestDiscoverMigrations:
    """discover_migrations finds and orders migration files."""

    def test_discovers_sql_and_py_files(self, tmp_path: Path) -> None:
        """Finds both .sql and .py migration files."""
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")
        (tmp_path / "V002__second.py").write_text("def migrate(conn): pass\n")
        migrations = discover_migrations(tmp_path)
        assert len(migrations) == 2
        assert migrations[0].version == 1
        assert migrations[1].version == 2

    def test_sorted_by_version(self, tmp_path: Path) -> None:
        """Migrations are returned sorted by version number."""
        (tmp_path / "V003__third.sql").write_text("SELECT 3;")
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")
        (tmp_path / "V002__second.sql").write_text("SELECT 2;")
        migrations = discover_migrations(tmp_path)
        assert [m.version for m in migrations] == [1, 2, 3]

    def test_ignores_non_migration_files(self, tmp_path: Path) -> None:
        """Non-migration files (README, __init__, etc.) are ignored."""
        (tmp_path / "V001__real.sql").write_text("SELECT 1;")
        (tmp_path / "README.md").write_text("# Migrations")
        (tmp_path / "__init__.py").write_text("")
        (tmp_path / "notes.txt").write_text("notes")
        migrations = discover_migrations(tmp_path)
        assert len(migrations) == 1

    def test_empty_directory(self, tmp_path: Path) -> None:
        """Empty directory returns empty list."""
        migrations = discover_migrations(tmp_path)
        assert migrations == []

    def test_nonexistent_directory(self, tmp_path: Path) -> None:
        """Nonexistent directory returns empty list."""
        migrations = discover_migrations(tmp_path / "nope")
        assert migrations == []

    def test_rejects_duplicate_versions(self, tmp_path: Path) -> None:
        """Duplicate version numbers raise an error."""
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")
        (tmp_path / "V001__also_first.sql").write_text("SELECT 2;")
        with pytest.raises(ValueError, match="Duplicate migration version"):
            discover_migrations(tmp_path)

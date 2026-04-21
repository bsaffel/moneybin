"""Tests for the database migration system."""

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from moneybin.database import Database
from moneybin.migrations import (
    Migration,
    MigrationError,
    MigrationRunner,
    discover_migrations,
    get_current_versions,
    record_version,
)


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


class TestMigrationRunnerAppliedVersions:
    """MigrationRunner.applied_versions() reads the tracking table."""

    def test_empty_tracking_table(self, db: Database) -> None:
        """Returns empty dict when no migrations have been applied."""
        runner = MigrationRunner(db)
        assert runner.applied_versions() == {}

    def test_returns_applied_versions_with_checksums(self, db: Database) -> None:
        """Returns {version: checksum} for applied migrations."""
        db.execute(
            "INSERT INTO app.schema_migrations (version, filename, checksum) "
            "VALUES (1, 'V001__test.sql', 'abc123')"
        )
        runner = MigrationRunner(db)
        applied = runner.applied_versions()
        assert applied == {1: "abc123"}

    def test_excludes_failed_migrations(self, db: Database) -> None:
        """Failed migrations (success=false) are still returned — they represent stuck state."""
        db.execute(
            "INSERT INTO app.schema_migrations (version, filename, checksum, success) "
            "VALUES (1, 'V001__ok.sql', 'aaa', TRUE), "
            "(2, 'V002__fail.sql', 'bbb', FALSE)"
        )
        runner = MigrationRunner(db)
        applied = runner.applied_versions()
        assert 1 in applied
        assert 2 in applied


class TestMigrationRunnerPending:
    """MigrationRunner.pending() computes unapplied migrations."""

    def test_all_pending_when_none_applied(self, db: Database, tmp_path: Path) -> None:
        """All discovered migrations are pending when tracking table is empty."""
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")
        (tmp_path / "V002__second.sql").write_text("SELECT 2;")
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        pending = runner.pending()
        assert [m.version for m in pending] == [1, 2]

    def test_excludes_already_applied(self, db: Database, tmp_path: Path) -> None:
        """Already-applied versions are excluded from pending."""
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")
        (tmp_path / "V002__second.sql").write_text("SELECT 2;")
        db.execute(
            "INSERT INTO app.schema_migrations (version, filename, checksum) "
            "VALUES (1, 'V001__first.sql', 'ignored')"
        )
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        pending = runner.pending()
        assert [m.version for m in pending] == [2]

    def test_empty_when_all_applied(self, db: Database, tmp_path: Path) -> None:
        """Returns empty list when all migrations are applied."""
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")
        db.execute(
            "INSERT INTO app.schema_migrations (version, filename, checksum) "
            "VALUES (1, 'V001__first.sql', 'ignored')"
        )
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        assert runner.pending() == []


class TestMigrationRunnerApplyOne:
    """MigrationRunner.apply_one() executes a single migration."""

    def test_applies_sql_migration(self, db: Database, tmp_path: Path) -> None:
        """SQL migration is executed and tracked."""
        sql_file = tmp_path / "V001__create_test.sql"
        sql_file.write_text(
            "CREATE TABLE IF NOT EXISTS app.migration_test (id INTEGER PRIMARY KEY);"
        )
        migration = Migration.from_file(sql_file)
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        runner.apply_one(migration)

        # Table was created
        result = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'migration_test'"
        ).fetchone()
        assert result[0] == 1

        # Tracking row was recorded
        row = db.execute(
            "SELECT version, filename, checksum, success FROM app.schema_migrations "
            "WHERE version = 1"
        ).fetchone()
        assert row is not None
        assert row[0] == 1
        assert row[1] == "V001__create_test.sql"
        assert row[2] == migration.checksum
        assert row[3] is True

    def test_applies_python_migration(self, db: Database, tmp_path: Path) -> None:
        """Python migration calls migrate(conn) and is tracked."""
        py_file = tmp_path / "V001__py_test.py"
        py_file.write_text(
            "def migrate(conn):\n"
            "    conn.execute('CREATE TABLE IF NOT EXISTS app.py_test (id INTEGER)')\n"
        )
        migration = Migration.from_file(py_file)
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        runner.apply_one(migration)

        result = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'app' AND table_name = 'py_test'"
        ).fetchone()
        assert result[0] == 1

        row = db.execute(
            "SELECT success FROM app.schema_migrations WHERE version = 1"
        ).fetchone()
        assert row[0] is True

    def test_records_execution_time(self, db: Database, tmp_path: Path) -> None:
        """Execution time is recorded in milliseconds."""
        sql_file = tmp_path / "V001__timed.sql"
        sql_file.write_text("SELECT 1;")
        migration = Migration.from_file(sql_file)
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        runner.apply_one(migration)

        row = db.execute(
            "SELECT execution_ms FROM app.schema_migrations WHERE version = 1"
        ).fetchone()
        assert row[0] is not None
        assert row[0] >= 0

    def test_rollback_on_sql_error(self, db: Database, tmp_path: Path) -> None:
        """Failed SQL migration rolls back and records success=false."""
        sql_file = tmp_path / "V001__bad.sql"
        sql_file.write_text("CREATE TABLE this is not valid SQL;")
        migration = Migration.from_file(sql_file)
        runner = MigrationRunner(db, migrations_dir=tmp_path)

        with pytest.raises(MigrationError):
            runner.apply_one(migration)

        row = db.execute(
            "SELECT success FROM app.schema_migrations WHERE version = 1"
        ).fetchone()
        assert row is not None
        assert row[0] is False

    def test_idempotent_on_rerun(self, db: Database, tmp_path: Path) -> None:
        """Re-running an already-applied migration is a silent no-op."""
        sql_file = tmp_path / "V001__test.sql"
        sql_file.write_text("SELECT 1;")
        migration = Migration.from_file(sql_file)
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        runner.apply_one(migration)
        runner.apply_one(migration)  # no error

        count = db.execute(
            "SELECT COUNT(*) FROM app.schema_migrations WHERE version = 1"
        ).fetchone()
        assert count[0] == 1


class TestMigrationRunnerApplyAll:
    """MigrationRunner.apply_all() runs pending migrations in order."""

    def test_applies_pending_in_order(self, db: Database, tmp_path: Path) -> None:
        """Pending migrations are applied in version order."""
        (tmp_path / "V001__first.sql").write_text(
            "CREATE TABLE IF NOT EXISTS app.first (id INTEGER);"
        )
        (tmp_path / "V002__second.sql").write_text(
            "CREATE TABLE IF NOT EXISTS app.second (id INTEGER);"
        )
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        result = runner.apply_all()
        assert result.applied_count == 2
        assert result.failed is False

    def test_skips_already_applied(self, db: Database, tmp_path: Path) -> None:
        """Already-applied migrations are not re-executed."""
        (tmp_path / "V001__first.sql").write_text("SELECT 1;")
        (tmp_path / "V002__second.sql").write_text("SELECT 2;")
        db.execute(
            "INSERT INTO app.schema_migrations (version, filename, checksum) "
            "VALUES (1, 'V001__first.sql', 'ignored')"
        )
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        result = runner.apply_all()
        assert result.applied_count == 1

    def test_stops_on_first_failure(self, db: Database, tmp_path: Path) -> None:
        """Stops executing after first failed migration."""
        (tmp_path / "V001__good.sql").write_text("SELECT 1;")
        (tmp_path / "V002__bad.sql").write_text("INVALID SQL HERE;")
        (tmp_path / "V003__never.sql").write_text("SELECT 3;")
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        result = runner.apply_all()
        assert result.applied_count == 1
        assert result.failed is True
        assert result.failed_migration == "V002__bad.sql"

        # V003 was never attempted
        row = db.execute(
            "SELECT COUNT(*) FROM app.schema_migrations WHERE version = 3"
        ).fetchone()
        assert row[0] == 0

    def test_no_pending_returns_zero(self, db: Database, tmp_path: Path) -> None:
        """Returns count=0 when nothing is pending."""
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        result = runner.apply_all()
        assert result.applied_count == 0
        assert result.failed is False


class TestMigrationRunnerDrift:
    """MigrationRunner.check_drift() detects modified migration files."""

    def test_detects_checksum_drift(self, db: Database, tmp_path: Path) -> None:
        """Warns when an applied migration's file has changed."""
        sql_file = tmp_path / "V001__drifted.sql"
        sql_file.write_text("SELECT 1;")
        migration = Migration.from_file(sql_file)
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        runner.apply_one(migration)

        # Modify the file after applying
        sql_file.write_text("SELECT 999;")
        warnings = runner.check_drift()
        assert len(warnings) == 1
        assert warnings[0].version == 1
        assert warnings[0].filename == "V001__drifted.sql"

    def test_no_drift_when_unchanged(self, db: Database, tmp_path: Path) -> None:
        """No warnings when file checksums match."""
        sql_file = tmp_path / "V001__stable.sql"
        sql_file.write_text("SELECT 1;")
        migration = Migration.from_file(sql_file)
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        runner.apply_one(migration)
        assert runner.check_drift() == []

    def test_ignores_unapplied_files(self, db: Database, tmp_path: Path) -> None:
        """Unapplied migration files are not checked for drift."""
        (tmp_path / "V001__pending.sql").write_text("SELECT 1;")
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        assert runner.check_drift() == []

    def test_detects_missing_file(self, db: Database, tmp_path: Path) -> None:
        """Warns when an applied migration's file has been deleted."""
        sql_file = tmp_path / "V001__deleted.sql"
        sql_file.write_text("SELECT 1;")
        migration = Migration.from_file(sql_file)
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        runner.apply_one(migration)

        sql_file.unlink()
        warnings = runner.check_drift()
        assert len(warnings) == 1
        assert "missing" in warnings[0].reason.lower()


class TestMigrationRunnerStuck:
    """MigrationRunner detects stuck migrations (success=false)."""

    def test_detects_stuck_migration(self, db: Database, tmp_path: Path) -> None:
        """Raises error when a failed migration exists in tracking table."""
        db.execute(
            "INSERT INTO app.schema_migrations (version, filename, checksum, success) "
            "VALUES (1, 'V001__stuck.sql', 'abc', FALSE)"
        )
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        with pytest.raises(MigrationError, match="stuck"):
            runner.check_stuck()

    def test_no_stuck_when_all_succeeded(self, db: Database, tmp_path: Path) -> None:
        """No error when all applied migrations succeeded."""
        db.execute(
            "INSERT INTO app.schema_migrations (version, filename, checksum, success) "
            "VALUES (1, 'V001__ok.sql', 'abc', TRUE)"
        )
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        runner.check_stuck()  # no exception

    def test_apply_all_checks_stuck_first(self, db: Database, tmp_path: Path) -> None:
        """apply_all() raises if there's a stuck migration, before running anything."""
        db.execute(
            "INSERT INTO app.schema_migrations (version, filename, checksum, success) "
            "VALUES (1, 'V001__stuck.sql', 'abc', FALSE)"
        )
        (tmp_path / "V002__new.sql").write_text("SELECT 1;")
        runner = MigrationRunner(db, migrations_dir=tmp_path)
        result = runner.apply_all()
        assert result.failed is True
        assert result.applied_count == 0


class TestVersionTracking:
    """Version recording and retrieval in app.versions."""

    def test_record_version_first_install(self, db: Database) -> None:
        """Records version with no previous_version on first install."""
        record_version(db, "test_component", "1.0.0")
        row = db.execute(
            "SELECT component, version, previous_version "
            "FROM app.versions WHERE component = 'test_component'"
        ).fetchone()
        assert row[0] == "test_component"
        assert row[1] == "1.0.0"
        assert row[2] is None

    def test_record_version_upgrade(self, db: Database) -> None:
        """Updates version and sets previous_version on upgrade."""
        record_version(db, "test_component", "1.0.0")
        record_version(db, "test_component", "2.0.0")
        row = db.execute(
            "SELECT version, previous_version FROM app.versions "
            "WHERE component = 'test_component'"
        ).fetchone()
        assert row[0] == "2.0.0"
        assert row[1] == "1.0.0"

    def test_record_version_same_is_noop(self, db: Database) -> None:
        """Recording the same version is a no-op."""
        record_version(db, "test_component", "1.0.0")
        record_version(db, "test_component", "1.0.0")
        count = db.execute(
            "SELECT COUNT(*) FROM app.versions WHERE component = 'test_component'"
        ).fetchone()
        assert count[0] == 1

    def test_get_current_versions_empty(self, db: Database) -> None:
        """Returns empty dict when no versions recorded for unknown component."""
        versions = get_current_versions(db)
        assert "test_never_recorded" not in versions

    def test_get_current_versions(self, db: Database) -> None:
        """Returns {component: version} mapping including recorded components."""
        record_version(db, "test_component_a", "1.0.0")
        record_version(db, "test_component_b", "0.130.0")
        versions = get_current_versions(db)
        assert versions["test_component_a"] == "1.0.0"
        assert versions["test_component_b"] == "0.130.0"


class TestAutoUpgrade:
    """Auto-upgrade runs migrations when package version changes."""

    def test_first_init_records_version(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        """First database init records the current package version."""
        db_path = tmp_path / "test.duckdb"
        with patch(
            "moneybin.database.importlib.metadata.version", return_value="1.0.0"
        ):
            database = Database(db_path, secret_store=mock_secret_store)
        try:
            row = database.execute(
                "SELECT version FROM app.versions WHERE component = 'moneybin'"
            ).fetchone()
            assert row is not None
            assert row[0] == "1.0.0"
        finally:
            database.close()

    def test_version_mismatch_runs_migrations(
        self, tmp_path: Path, mock_secret_store: MagicMock
    ) -> None:
        """When package version changes, pending migrations are applied."""
        db_path = tmp_path / "test.duckdb"
        # First init at version 1.0.0
        with patch(
            "moneybin.database.importlib.metadata.version", return_value="1.0.0"
        ):
            db1 = Database(db_path, secret_store=mock_secret_store)
            db1.close()

        # Second init at version 2.0.0 — should trigger migration sequence
        with patch(
            "moneybin.database.importlib.metadata.version", return_value="2.0.0"
        ):
            db2 = Database(db_path, secret_store=mock_secret_store)
        try:
            row = db2.execute(
                "SELECT version, previous_version FROM app.versions "
                "WHERE component = 'moneybin'"
            ).fetchone()
            assert row[0] == "2.0.0"
            assert row[1] == "1.0.0"
        finally:
            db2.close()

    def test_no_auto_upgrade_skips_migrations(
        self,
        tmp_path: Path,
        mock_secret_store: MagicMock,
    ) -> None:
        """no_auto_upgrade=True in settings skips migration sequence."""
        from moneybin.config import DatabaseConfig, MoneyBinSettings

        mock_settings = MagicMock(spec=MoneyBinSettings)
        mock_settings.database = DatabaseConfig(no_auto_upgrade=True)
        db_path = tmp_path / "test.duckdb"
        with (
            patch("moneybin.database.importlib.metadata.version", return_value="1.0.0"),
            patch("moneybin.database.get_settings", return_value=mock_settings),
        ):
            database = Database(db_path, secret_store=mock_secret_store)
        try:
            row = database.execute(
                "SELECT COUNT(*) FROM app.versions WHERE component = 'moneybin'"
            ).fetchone()
            assert row[0] == 0  # version not recorded when auto-upgrade is skipped
        finally:
            database.close()

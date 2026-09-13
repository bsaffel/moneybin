"""Tests for V060 display-currency-target persistence."""

from __future__ import annotations

from pathlib import Path

from moneybin.database import Database
from moneybin.migrations import Migration, MigrationRunner
from moneybin.sql.migrations.V060__add_profile_display_currency_targets import migrate
from tests.moneybin.migration_helpers import column_info, run_migration

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "moneybin"
    / "sql"
    / "migrations"
    / "V060__add_profile_display_currency_targets.py"
)
_V044_MIGRATION_PATH = _MIGRATION_PATH.with_name("V044__create_app_profile_settings.py")

_COLUMN_SHAPE_SQL = """
SELECT column_name, data_type, is_nullable
  FROM information_schema.columns
 WHERE table_schema = 'app' AND table_name = 'profile_settings'
 ORDER BY ordinal_position
"""


def test_v060_adds_an_empty_default_target_collection(db: Database) -> None:
    """The runner preserves a legacy home currency and backfills its targets."""
    db.execute("INSERT INTO app.profile_settings (home_currency) VALUES ('USD')")
    db.execute("ALTER TABLE app.profile_settings DROP COLUMN display_currency_targets")
    migration = Migration.from_file(_MIGRATION_PATH)

    MigrationRunner(db, migrations_dir=_MIGRATION_PATH.parent).apply_one(migration)

    data_type, nullable = column_info(
        db, "app", "profile_settings", "display_currency_targets"
    )
    assert data_type == "VARCHAR[]"
    assert nullable is False
    assert db.execute(
        "SELECT home_currency, display_currency_targets FROM app.profile_settings"
    ).fetchone() == ("USD", [])


def test_v060_add_column_applies_comment_and_classification_immediately(
    db: Database,
) -> None:
    """Catalog gets the human comment and `[class: currency]` immediately.

    Right after ADD COLUMN, not only after the next writable open.
    `init_schemas()` applies both from the schema file before migrations run
    and silently skips a column that does not exist yet, so V060 must set
    them itself (Codex CONSIDER on V060 line 32).
    """
    db.execute("ALTER TABLE app.profile_settings DROP COLUMN display_currency_targets")
    migration = Migration.from_file(_MIGRATION_PATH)

    MigrationRunner(db, migrations_dir=_MIGRATION_PATH.parent).apply_one(migration)

    comment = db.execute(
        """
        SELECT comment FROM duckdb_columns()
        WHERE schema_name = 'app' AND table_name = 'profile_settings'
          AND column_name = 'display_currency_targets'
        """
    ).fetchone()
    assert comment is not None
    assert comment[0] == (
        "Explicit read targets; empty leaves refresh cost unchanged [class: currency]"
    )


def test_v060_runs_through_the_migration_runner_transaction(db: Database) -> None:
    """The runner survives V060's required commit/reopen boundary and records it."""
    db.execute("ALTER TABLE app.profile_settings DROP COLUMN display_currency_targets")
    migration = Migration.from_file(_MIGRATION_PATH)

    MigrationRunner(db, migrations_dir=_MIGRATION_PATH.parent).apply_one(migration)

    assert column_info(db, "app", "profile_settings", "display_currency_targets") == (
        "VARCHAR[]",
        False,
    )
    assert db.execute(
        "SELECT success FROM app.schema_migrations WHERE version = 60"
    ).fetchone() == (True,)


def test_v060_runner_recovers_a_partial_column_without_a_failure_row(
    db: Database,
) -> None:
    """A crash after add can resume through the real runner on the next open."""
    db.execute("ALTER TABLE app.profile_settings DROP COLUMN display_currency_targets")
    db.execute(
        "ALTER TABLE app.profile_settings "
        "ADD COLUMN display_currency_targets VARCHAR[] DEFAULT []"
    )
    migration = Migration.from_file(_MIGRATION_PATH)

    MigrationRunner(db, migrations_dir=_MIGRATION_PATH.parent).apply_one(migration)

    assert column_info(db, "app", "profile_settings", "display_currency_targets") == (
        "VARCHAR[]",
        False,
    )
    assert db.execute(
        "SELECT success FROM app.schema_migrations WHERE version = 60"
    ).fetchone() == (True,)


def test_v044_to_v060_runner_chain_matches_fresh_profile_shape(
    db: Database, tmp_path: Path
) -> None:
    """The two real migrations produce the fresh schema and empty default."""
    fresh_shape = {row[0]: row[1:] for row in db.execute(_COLUMN_SHAPE_SQL).fetchall()}
    db.execute("DROP TABLE app.profile_settings")
    for path in (_V044_MIGRATION_PATH, _MIGRATION_PATH):
        (tmp_path / path.name).write_bytes(path.read_bytes())

    result = MigrationRunner(db, migrations_dir=tmp_path).apply_all()

    migrated_shape = {
        row[0]: row[1:] for row in db.execute(_COLUMN_SHAPE_SQL).fetchall()
    }
    assert result.applied_count == 2
    assert migrated_shape == fresh_shape
    db.execute("INSERT INTO app.profile_settings (home_currency) VALUES ('USD')")
    assert db.execute(
        "SELECT display_currency_targets FROM app.profile_settings"
    ).fetchone() == ([],)


def test_v060_rerun_and_partial_column_both_restore_the_empty_default(
    db: Database,
) -> None:
    """A crash after add leaves a nullable column that a later run can complete."""
    db.execute("ALTER TABLE app.profile_settings DROP COLUMN display_currency_targets")
    db.execute("INSERT INTO app.profile_settings (home_currency) VALUES ('USD')")
    db.execute(
        "ALTER TABLE app.profile_settings "
        "ADD COLUMN display_currency_targets VARCHAR[] DEFAULT []"
    )
    db.execute("UPDATE app.profile_settings SET display_currency_targets = NULL")

    run_migration(db, migrate)
    run_migration(db, migrate)

    assert column_info(db, "app", "profile_settings", "display_currency_targets") == (
        "VARCHAR[]",
        False,
    )
    assert db.execute(
        "SELECT display_currency_targets FROM app.profile_settings"
    ).fetchone() == ([],)

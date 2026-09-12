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


def test_v060_adds_an_empty_default_target_collection(db: Database) -> None:
    """Existing profiles gain an empty target list, preserving refresh cost."""
    db.execute("ALTER TABLE app.profile_settings DROP COLUMN display_currency_targets")

    run_migration(db, migrate)

    data_type, nullable = column_info(
        db, "app", "profile_settings", "display_currency_targets"
    )
    assert data_type == "VARCHAR[]"
    assert nullable is False
    db.execute("INSERT INTO app.profile_settings (home_currency) VALUES ('USD')")
    assert db.execute(
        "SELECT display_currency_targets FROM app.profile_settings"
    ).fetchone() == ([],)


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

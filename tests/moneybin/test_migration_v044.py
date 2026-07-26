"""Tests for V044 profile-settings persistence (multi-currency Requirement 4)."""

from __future__ import annotations

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V044__create_app_profile_settings import migrate
from tests.moneybin.migration_helpers import run_migration

pytestmark = pytest.mark.fresh_db

_COLUMN_SHAPE_SQL = """
SELECT column_name, data_type, is_nullable
  FROM information_schema.columns
 WHERE table_schema = 'app' AND table_name = 'profile_settings'
 ORDER BY ordinal_position
"""


def _drop_table(db: Database) -> None:
    """Simulate a pre-V044 database, which the schema file otherwise pre-creates."""
    db.execute("DROP TABLE IF EXISTS app.profile_settings")


def test_v044_creates_profile_settings_on_a_pre_v044_database(db: Database) -> None:
    """An existing database that predates the table gains it with the right shape."""
    _drop_table(db)

    run_migration(db, migrate)

    columns = db.execute(_COLUMN_SHAPE_SQL).fetchall()
    assert [row[0] for row in columns] == ["scope", "home_currency", "updated_at"]

    db.execute("INSERT INTO app.profile_settings (home_currency) VALUES ('EUR')")
    assert db.execute(
        "SELECT scope, home_currency FROM app.profile_settings"
    ).fetchall() == [("profile", "EUR")]


def test_v044_singleton_guard_survives_the_migration_path(db: Database) -> None:
    """The singleton CHECK ships in the migration, not just the schema file.

    Without it, an upgraded database could hold two settings rows while a fresh
    install could not, so the report guards would read a scan-order-dependent
    home currency on exactly the databases that already have user data.
    """
    _drop_table(db)
    run_migration(db, migrate)

    db.execute("INSERT INTO app.profile_settings (home_currency) VALUES ('EUR')")

    with pytest.raises(duckdb.ConstraintException, match="CHECK constraint"):
        db.execute(
            "INSERT INTO app.profile_settings (scope, home_currency) "
            "VALUES ('other', 'GBP')"
        )


def test_v044_produces_the_same_shape_as_a_fresh_install(db: Database) -> None:
    """Upgraded and fresh-installed databases agree on the table's shape.

    The DDL exists twice — `sql/schema/app_profile_settings.sql` for fresh
    installs and this migration for upgrades. Nothing but this test stops the
    two copies from drifting apart.
    """
    fresh_shape = db.execute(_COLUMN_SHAPE_SQL).fetchall()
    assert fresh_shape, "schema file should have created the table for a fresh install"

    _drop_table(db)
    run_migration(db, migrate)
    migrated_shape = db.execute(_COLUMN_SHAPE_SQL).fetchall()

    assert migrated_shape == fresh_shape


def test_v044_is_idempotent(db: Database) -> None:
    """Fresh installs and migration upgrades may both invoke the DDL."""
    _drop_table(db)
    run_migration(db, migrate)
    run_migration(db, migrate)

    assert db.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'app' AND table_name = 'profile_settings'
        """
    ).fetchone() == (1,)

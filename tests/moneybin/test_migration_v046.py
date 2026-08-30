"""Tests for V046 content-derived re-import detection."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.sql.migrations.V046__add_file_sha256_to_import_log import migrate
from tests.moneybin.migration_helpers import run_migration

_COLUMN_SHAPE_SQL = """
SELECT column_name, data_type, is_nullable
  FROM information_schema.columns
 WHERE table_schema = 'raw' AND table_name = 'import_log'
 ORDER BY ordinal_position
"""


def _drop_column(db: Database) -> None:
    """Simulate a pre-V046 database, which the schema file otherwise pre-creates."""
    db.execute("ALTER TABLE raw.import_log DROP COLUMN IF EXISTS file_sha256")


def test_v046_adds_file_sha256_on_a_pre_v046_database(db: Database) -> None:
    _drop_column(db)

    run_migration(db, migrate)

    columns = {row[0]: row[2] for row in db.execute(_COLUMN_SHAPE_SQL).fetchall()}
    assert columns["file_sha256"] == "YES"


def test_v046_leaves_existing_batches_matchable_by_path(db: Database) -> None:
    """Rows that predate the column keep NULL and keep their path behavior.

    A backfill is impossible here — the source file may be long gone — so the
    upgrade has to leave old batches on the path predicate alone.
    """
    _drop_column(db)
    db.execute(
        "INSERT INTO raw.import_log "
        "(import_id, source_file, source_type, source_origin, "
        " account_names, status) "
        "VALUES ('legacy-01', '/tmp/legacy.ofx', 'ofx', 'wells_fargo', "  # noqa: S108  # test fixture path
        " '[\"checking\"]', 'complete')"
    )

    run_migration(db, migrate)

    assert db.execute(
        "SELECT file_sha256 FROM raw.import_log WHERE import_id = 'legacy-01'"
    ).fetchone() == (None,)
    assert import_log.find_existing_import(db, "/tmp/legacy.ofx") == (  # noqa: S108  # test fixture path
        "legacy-01",
        "complete",
    )


def test_v046_produces_the_same_shape_as_a_fresh_install(db: Database) -> None:
    """Upgraded and fresh-installed databases agree on the table's shape.

    The DDL exists twice — `sql/schema/raw_import_log.sql` for fresh installs
    and this migration for upgrades. Nothing but this test stops the two copies
    from drifting apart.

    Ordinal position is part of the shape, hence ``ORDER BY ordinal_position``
    and an ordered comparison: ``ALTER TABLE ADD COLUMN`` can only append, so a
    schema file that declares the column anywhere but last would give fresh and
    upgraded databases different layouts under ``SELECT *``.
    """
    fresh_shape = db.execute(_COLUMN_SHAPE_SQL).fetchall()
    assert any(row[0] == "file_sha256" for row in fresh_shape), (
        "schema file should have created the column for a fresh install"
    )

    _drop_column(db)
    run_migration(db, migrate)
    migrated_shape = db.execute(_COLUMN_SHAPE_SQL).fetchall()

    assert migrated_shape == fresh_shape


def test_v046_is_idempotent(db: Database) -> None:
    """Fresh installs and migration upgrades may both invoke the DDL."""
    _drop_column(db)
    run_migration(db, migrate)
    run_migration(db, migrate)

    assert db.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND table_name = 'import_log'
          AND column_name = 'file_sha256'
        """
    ).fetchone() == (1,)

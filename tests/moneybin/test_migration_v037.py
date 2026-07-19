"""Tests for V037 import-preview trust-state persistence."""

from __future__ import annotations

import importlib

from moneybin.database import Database
from tests.moneybin.migration_helpers import run_migration


def test_v037_creates_import_preview_trust_state_table(db: Database) -> None:
    db.execute("DROP TABLE IF EXISTS app.import_previews")
    migration = importlib.import_module(
        "moneybin.sql.migrations.V037__create_import_previews"
    )

    run_migration(db, migration.migrate)

    columns = {
        row[0]
        for row in db.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'app' AND table_name = 'import_previews'
            """
        ).fetchall()
    }
    assert columns == {
        "preview_id",
        "file_path",
        "file_sha256",
        "file_size_bytes",
        "channel",
        "snapshot_json",
        "issued_at",
        "expires_at",
        "consumed_at",
        "import_id",
        "updated_at",
    }


def test_v037_is_idempotent(db: Database) -> None:
    migration = importlib.import_module(
        "moneybin.sql.migrations.V037__create_import_previews"
    )

    run_migration(db, migration.migrate)
    run_migration(db, migration.migrate)

    assert db.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'app' AND table_name = 'import_previews'
        """
    ).fetchone() == (1,)

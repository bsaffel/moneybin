"""Tests for V041 export-destination persistence."""

from __future__ import annotations

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V041__create_app_export_destinations import migrate
from tests.moneybin.migration_helpers import run_migration

pytestmark = pytest.mark.fresh_db


def test_v041_creates_export_destinations(db: Database) -> None:
    """The migration creates the exact cross-kind destination registry."""
    run_migration(db, migrate)

    db.execute("SELECT * FROM app.export_destinations LIMIT 0")
    columns = db.execute(
        """SELECT column_name FROM information_schema.columns
               WHERE table_schema = 'app'
                 AND table_name = 'export_destinations'
             ORDER BY ordinal_position"""
    ).fetchall()
    assert [row[0] for row in columns] == [
        "destination_id",
        "name",
        "kind",
        "local_path",
        "spreadsheet_id",
        "managed_tab_prefix",
        "created_at",
        "updated_at",
    ]

    db.execute(
        """
        INSERT INTO app.export_destinations (
            destination_id, name, kind, local_path
        ) VALUES (?, ?, ?, ?)
        """,
        ["destination01", "downloads", "local", "visible/exports"],
    )
    db.execute(
        """
        INSERT INTO app.export_destinations (
            destination_id, name, kind, spreadsheet_id, managed_tab_prefix
        ) VALUES (?, ?, ?, ?, ?)
        """,
        ["destination02", "planning", "sheets", "sheet_123", "MoneyBin"],
    )

    rows = db.execute(
        """
        SELECT kind, local_path, spreadsheet_id, managed_tab_prefix
        FROM app.export_destinations
        ORDER BY destination_id
        """
    ).fetchall()
    assert rows == [
        ("local", "visible/exports", None, None),
        ("sheets", None, "sheet_123", "MoneyBin"),
    ]

    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            """
            INSERT INTO app.export_destinations (
                destination_id, name, kind, local_path
            ) VALUES (?, ?, ?, ?)
            """,
            ["destination03", "planning", "local", "visible/another"],
        )

    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            """
            INSERT INTO app.export_destinations (destination_id, name, kind)
            VALUES (?, ?, ?)
            """,
            ["destination04", "invalid", "local"],
        )

    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            """
            INSERT INTO app.export_destinations (
                destination_id, name, kind, spreadsheet_id
            ) VALUES (?, ?, ?, ?)
            """,
            ["destination05", "incomplete-sheets", "sheets", "sheet_456"],
        )

    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            """
            INSERT INTO app.export_destinations (
                destination_id, name, kind, local_path
            ) VALUES (?, ?, ?, ?)
            """,
            ["destination06", "unknown-kind", "drive", "visible/drive"],
        )


def test_v041_is_idempotent(db: Database) -> None:
    """Fresh installs and migration upgrades may both invoke the DDL."""
    run_migration(db, migrate)
    run_migration(db, migrate)

    assert db.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'app' AND table_name = 'export_destinations'
        """
    ).fetchone() == (1,)

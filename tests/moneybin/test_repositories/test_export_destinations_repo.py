"""Tests for audited export-destination configuration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.repositories.export_destinations_repo import ExportDestinationsRepo
from moneybin.sql.migrations.V041__create_app_export_destinations import migrate
from tests.moneybin.migration_helpers import run_migration


@pytest.fixture()
def repo(db: Database) -> ExportDestinationsRepo:
    run_migration(db, migrate)
    return ExportDestinationsRepo(db)


def _audit_rows_for(db: Database, destination_id: str) -> list[tuple[Any, ...]]:
    return db.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [destination_id],
    ).fetchall()


def test_set_local_lists_and_resolves_exact_references(
    db: Database, repo: ExportDestinationsRepo
) -> None:
    """A local destination is saved once and resolves only by exact id or name."""
    event = repo.set_local(
        name="monthly-export",
        local_path=Path("visible/monthly-exports"),
        actor="cli",
    )
    destination_id = event.target_id
    assert destination_id is not None and len(destination_id) == 12

    listed = repo.list()
    assert [destination.name for destination in listed] == ["monthly-export"]
    assert listed[0].local_path == Path("visible/monthly-exports")
    assert listed[0].spreadsheet_id is None
    assert listed[0].managed_tab_prefix is None
    assert repo.resolve(destination_id) == listed[0]
    assert repo.resolve("monthly-export") == listed[0]
    assert repo.resolve("monthly") is None

    audit = _audit_rows_for(db, destination_id)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor = audit[0]
    assert action == "export_destination.set_local"
    assert (schema, table, target_id) == (
        "app",
        "export_destinations",
        destination_id,
    )
    assert before is None
    assert json.loads(after)["local_path"] == "visible/monthly-exports"
    assert actor == "cli"


def test_set_sheets_writes_complete_destination_and_one_audit_row(
    db: Database, repo: ExportDestinationsRepo
) -> None:
    """A Sheets destination persists its workbook and managed tab namespace."""
    event = repo.set_sheets(
        name="planning-workbook",
        spreadsheet_id="sheet_123",
        managed_tab_prefix="MoneyBin",
        actor="mcp",
    )
    destination_id = event.target_id
    assert destination_id is not None

    destination = repo.resolve("planning-workbook")
    assert destination is not None
    assert destination.kind == "sheets"
    assert destination.local_path is None
    assert destination.spreadsheet_id == "sheet_123"
    assert destination.managed_tab_prefix == "MoneyBin"

    audit = _audit_rows_for(db, destination_id)
    assert len(audit) == 1
    assert audit[0][0] == "export_destination.set_sheets"
    assert json.loads(audit[0][5])["spreadsheet_id"] == "sheet_123"


def test_set_replaces_a_destination_kind_with_one_paired_audit_row(
    db: Database, repo: ExportDestinationsRepo
) -> None:
    """Re-saving a name preserves its identity while updating its full shape."""
    created = repo.set_local(
        name="finance",
        local_path=Path("visible/finance"),
        actor="cli",
    )
    updated = repo.set_sheets(
        name="finance",
        spreadsheet_id="sheet_finance",
        managed_tab_prefix="Finance",
        actor="cli",
    )
    assert updated.target_id == created.target_id

    destination = repo.resolve("finance")
    assert destination is not None
    assert destination.kind == "sheets"
    assert destination.local_path is None

    audit = _audit_rows_for(db, created.target_id or "")
    assert len(audit) == 2
    before, after = json.loads(audit[-1][4]), json.loads(audit[-1][5])
    assert before["kind"] == "local"
    assert after["kind"] == "sheets"


def test_remove_deletes_configuration_only_and_emits_one_audit_row(
    db: Database, repo: ExportDestinationsRepo, tmp_path: Path
) -> None:
    """Removal unregisters a destination without deleting its visible directory."""
    visible_dir = tmp_path / "exports"
    visible_dir.mkdir()
    created = repo.set_local(
        name="removable",
        local_path=visible_dir,
        actor="cli",
    )
    destination_id = created.target_id
    assert destination_id is not None

    removed = repo.remove("removable", actor="cli")
    assert removed is not None
    assert removed.target_id == destination_id
    assert repo.resolve("removable") is None
    assert visible_dir.is_dir()

    audit = _audit_rows_for(db, destination_id)
    assert len(audit) == 2
    assert audit[-1][0] == "export_destination.remove"
    assert json.loads(audit[-1][4])["name"] == "removable"
    assert audit[-1][5] is None

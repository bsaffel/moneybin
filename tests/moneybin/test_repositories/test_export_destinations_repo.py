"""Tests for audited export-destination configuration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.exports.models import ExportDestination, ReservedExportDestinationError
from moneybin.repositories.export_destinations_repo import (
    ExportDestinationChangedError,
    ExportDestinationNamespaceConflictError,
    ExportDestinationSpreadsheetConflictError,
    ExportDestinationsRepo,
)
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo
from moneybin.services.entity_reference import AmbiguousEntity, MissingEntity
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
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
    assert repo.resolve("monthly") == MissingEntity(reference="monthly")

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
    assert isinstance(destination, ExportDestination)
    assert destination.kind == "sheets"
    assert destination.local_path is None
    assert destination.spreadsheet_id == "sheet_123"
    assert destination.managed_tab_prefix == "MoneyBin"

    audit = _audit_rows_for(db, destination_id)
    assert len(audit) == 1
    assert audit[0][0] == "export_destination.set_sheets"
    assert json.loads(audit[0][5])["spreadsheet_id"] == "sheet_123"


def test_set_sheets_rejects_a_duplicate_managed_namespace(
    repo: ExportDestinationsRepo,
) -> None:
    """One workbook prefix may belong to only one saved destination."""
    repo.set_sheets(
        name="dashboard",
        spreadsheet_id="sheet_123",
        managed_tab_prefix="MoneyBin",
        actor="cli",
    )

    with pytest.raises(ExportDestinationNamespaceConflictError):
        repo.set_sheets(
            name="other-dashboard",
            spreadsheet_id="sheet_123",
            managed_tab_prefix="MoneyBin",
            actor="cli",
        )


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
    assert isinstance(destination, ExportDestination)
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
    assert repo.resolve("removable") == MissingEntity(reference="removable")
    assert visible_dir.is_dir()

    audit = _audit_rows_for(db, destination_id)
    assert len(audit) == 2
    assert audit[-1][0] == "export_destination.remove"
    assert json.loads(audit[-1][4])["name"] == "removable"
    assert audit[-1][5] is None


def test_publication_recheck_rejects_a_repointed_sheets_destination(
    repo: ExportDestinationsRepo,
) -> None:
    """Publication may not use a workbook identity resolved before a repoint."""
    repo.set_sheets(
        name="dashboard",
        spreadsheet_id="sheet-original",
        managed_tab_prefix="MoneyBin",
        actor="cli",
    )
    resolved = repo.resolve("dashboard")
    assert isinstance(resolved, ExportDestination)
    repo.set_sheets(
        name="dashboard",
        spreadsheet_id="sheet-repointed",
        managed_tab_prefix="MoneyBin",
        actor="cli",
    )

    with pytest.raises(ExportDestinationChangedError):
        repo.assert_current_for_publication(resolved)


def test_publication_recheck_rejects_a_repointed_local_destination(
    repo: ExportDestinationsRepo,
) -> None:
    repo.set_local(name="archive", local_path=Path("original"), actor="cli")
    resolved = repo.resolve("archive")
    assert isinstance(resolved, ExportDestination)
    repo.set_local(name="archive", local_path=Path("repointed"), actor="cli")

    with pytest.raises(ExportDestinationChangedError):
        repo.assert_current_for_publication(resolved)


def test_undo_destination_restore_rechecks_inbound_workbook_role(
    db: Database, repo: ExportDestinationsRepo
) -> None:
    repo.set_sheets(
        name="dashboard",
        spreadsheet_id="shared-workbook",
        managed_tab_prefix="MB",
        actor="cli",
    )
    with operation() as removal_operation:
        repo.remove("dashboard", actor="cli")
    GSheetConnectionsRepo(db).insert(
        spreadsheet_id="shared-workbook",
        sheet_gid=0,
        sheet_name="Transactions",
        workbook_name="Inbound",
        adapter="transactions",
        alias=None,
        account_id=None,
        account_name=None,
        column_mapping={"Date": "date"},
        header_signature=["Date"],
        date_format=None,
        sign_convention=None,
        number_format=None,
        skip_rows=0,
        skip_trailing_patterns=None,
    )

    with pytest.raises(ExportDestinationSpreadsheetConflictError):
        UndoService(db).undo(removal_operation, actor="cli")


def test_list_and_resolve_treat_an_unmigrated_export_table_as_empty(
    db: Database,
) -> None:
    """Read-only first calls must not crash before V041 has applied."""
    repo = ExportDestinationsRepo(db)

    assert repo.list() == []
    assert repo.resolve("missing") == MissingEntity(reference="missing")


def test_set_sheets_rejects_an_inbound_connection_workbook(
    db: Database, repo: ExportDestinationsRepo
) -> None:
    """The same workbook cannot be both inbound connection and export target."""
    GSheetConnectionsRepo(db).insert(
        spreadsheet_id="inbound_sheet",
        sheet_gid=0,
        sheet_name="Transactions",
        workbook_name="Inbound ledger",
        adapter="transactions",
        alias=None,
        account_id=None,
        account_name=None,
        column_mapping={"Date": "transaction_date"},
        header_signature=["Date"],
        date_format=None,
        sign_convention=None,
        number_format=None,
        skip_rows=0,
        skip_trailing_patterns=None,
        actor="cli",
    )

    with pytest.raises(ExportDestinationSpreadsheetConflictError):
        repo.set_sheets(
            name="outbound",
            spreadsheet_id="inbound_sheet",
            managed_tab_prefix="MoneyBin",
            actor="cli",
        )

    assert repo.list() == []


@pytest.mark.parametrize(
    "name",
    ["exports", " exports ", " EXPORTS ", "\uff45\uff58\uff50\uff4f\uff52\uff54\uff53"],
)
def test_set_local_rejects_normalized_bare_exports_name(
    repo: ExportDestinationsRepo, name: str
) -> None:
    """Saved local names cannot shadow the derived local:exports target."""
    with pytest.raises(ReservedExportDestinationError):
        repo.set_local(
            name=name,
            local_path=Path("visible/exports"),
            actor="cli",
        )


@pytest.mark.parametrize("name", ["", "   ", "archive:monthly"])
@pytest.mark.parametrize("kind", ["local", "sheets"])
def test_set_rejects_unaddressable_destination_names(
    repo: ExportDestinationsRepo,
    name: str,
    kind: str,
) -> None:
    """Every accepted configured target can be represented as kind:name."""
    with pytest.raises(UserError) as exc_info:
        if kind == "local":
            repo.set_local(
                name=name,
                local_path=Path("visible/archive"),
                actor="cli",
            )
        else:
            repo.set_sheets(
                name=name,
                spreadsheet_id="sheet_789",
                managed_tab_prefix="MoneyBin",
                actor="cli",
            )

    assert exc_info.value.code == "mutation_invalid_input"


def test_resolve_prefers_id_then_exact_name_then_normalized_name(
    repo: ExportDestinationsRepo,
) -> None:
    """Resolution uses the shared deterministic entity-reference ladder."""
    id_owner = repo.set_local(
        name="first destination",
        local_path=Path("visible/first"),
        actor="cli",
    )
    destination_id = id_owner.target_id
    assert destination_id is not None
    repo.set_local(
        name=destination_id,
        local_path=Path("visible/name-collision"),
        actor="cli",
    )
    named = repo.set_local(
        name="Monthly Exports",
        local_path=Path("visible/monthly"),
        actor="cli",
    )

    id_result = repo.resolve(destination_id)
    exact_result = repo.resolve("MONTHLY EXPORTS")
    normalized_result = repo.resolve("  monthly\u3000exports  ")
    assert isinstance(id_result, ExportDestination)
    assert isinstance(exact_result, ExportDestination)
    assert isinstance(normalized_result, ExportDestination)
    assert id_result.destination_id == destination_id
    assert exact_result.destination_id == named.target_id
    assert normalized_result.destination_id == named.target_id


def test_resolve_returns_structured_ambiguity_for_normalized_name_collisions(
    repo: ExportDestinationsRepo,
) -> None:
    """Normalized matches never choose an arbitrary first destination."""
    first = repo.set_local(
        name=" Checking ",
        local_path=Path("visible/first"),
        actor="cli",
    )
    second = repo.set_local(
        name="checking  ",
        local_path=Path("visible/second"),
        actor="cli",
    )

    result = repo.resolve("checking")
    assert isinstance(result, AmbiguousEntity)
    assert first.target_id is not None
    assert second.target_id is not None
    assert result.candidate_ids == tuple(sorted([first.target_id, second.target_id]))


def test_repository_table_rejects_invalid_destination_shapes(
    db: Database, repo: ExportDestinationsRepo
) -> None:
    """Repository callers retain the migration's database-backed shape guard."""
    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            """
            INSERT INTO app.export_destinations (
                destination_id, name, kind, spreadsheet_id
            ) VALUES (?, ?, ?, ?)
            """,
            ["destination99", "bad-shape", "sheets", "sheet_999"],
        )

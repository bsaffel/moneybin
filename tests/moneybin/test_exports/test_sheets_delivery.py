"""Managed-tab Google Sheets export publication tests."""

from __future__ import annotations

import unicodedata
from typing import Never

import pytest

from moneybin.connectors.gsheet.errors import GSheetAPIError, GSheetAuthError
from moneybin.connectors.gsheet.testing.fake_oauth_client import TestOAuthClient
from moneybin.connectors.gsheet.testing.fake_sheets_client import (
    FakeSheetTab,
    FakeWorkbook,
    TestSheetsClient,
)
from moneybin.database import Database
from moneybin.exports.models import ExportDestination
from moneybin.exports.service import ExportService
from moneybin.exports.sheets import SheetsExportPublisher, SheetsPublishError
from moneybin.repositories.export_destinations_repo import (
    ExportDestinationSpreadsheetConflictError,
    ExportDestinationsRepo,
)
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo
from tests.moneybin.test_exports.test_renderers import make_snapshot, make_text_snapshot


def _destination(spreadsheet_id: str = "output-sheet") -> ExportDestination:
    return ExportDestination(
        destination_id="destination-1",
        name="finance-dashboard",
        kind="sheets",
        local_path=None,
        spreadsheet_id=spreadsheet_id,
        managed_tab_prefix="MB",
    )


def _publisher(
    db: Database,
    *,
    workbook: FakeWorkbook | None = None,
) -> tuple[SheetsExportPublisher, TestSheetsClient]:
    client = TestSheetsClient()
    client.register_workbook(
        "output-sheet", workbook or FakeWorkbook(title="Output workbook")
    )
    return SheetsExportPublisher(db=db, sheets_client=client), client


def _insert_inbound_connection(db: Database, spreadsheet_id: str) -> None:
    GSheetConnectionsRepo(db).insert(
        spreadsheet_id=spreadsheet_id,
        sheet_gid=0,
        sheet_name="Transactions",
        workbook_name="Inbound",
        adapter="transactions",
        alias=None,
        account_id=None,
        account_name=None,
        column_mapping={"Date": "date", "Amount": "amount"},
        header_signature=["Date", "Amount"],
        date_format=None,
        sign_convention="negative_is_expense",
        number_format=None,
        skip_rows=0,
        skip_trailing_patterns=None,
    )


def test_publish_rejects_inbound_spreadsheet_before_any_api_call(
    db: Database,
) -> None:
    _insert_inbound_connection(db, "output-sheet")
    publisher, client = _publisher(db)

    with pytest.raises(ExportDestinationSpreadsheetConflictError, match="inbound"):
        publisher.publish(make_snapshot(), _destination())

    assert client.requests == []


def test_successful_publish_stages_validates_and_atomically_promotes(
    db: Database,
) -> None:
    user_tab = FakeSheetTab("User Notes", 7, ["Note"], [["keep me"]])
    publisher, client = _publisher(db, workbook=FakeWorkbook("Output", [user_tab]))

    receipt = publisher.publish(make_snapshot(), _destination())

    metadata = client.get_workbook_metadata("output-sheet")
    names = {sheet.name for sheet in metadata.sheets}
    assert "User Notes" in names
    assert "MB Bundle 20260721T184233Z activity" in names
    assert "MB Manifest" in names
    assert "MB Dictionary" in names
    assert not any(" Staging " in name for name in names)
    assert client.read_sheet_values("output-sheet", "User Notes") == [
        ["Note"],
        ["keep me"],
    ]
    assert [operation for operation, _ in client.requests] == [
        "create",
        "write",
        "promote",
    ]
    assert receipt.sheets_identity == "MB:20260721T184233Z"
    assert receipt.row_counts == {"activity": 2}


def test_report_publication_cannot_select_or_replace_bundle_tabs(
    db: Database,
) -> None:
    publisher, client = _publisher(db)
    publisher.publish(make_snapshot(), _destination())
    bundle_before = {
        sheet.gid: sheet.name
        for sheet in client.get_workbook_metadata("output-sheet").sheets
        if sheet.name.startswith("MB Bundle ")
    }

    publisher.publish(make_snapshot(report=True), _destination())

    metadata = client.get_workbook_metadata("output-sheet")
    assert {
        sheet.gid: sheet.name
        for sheet in metadata.sheets
        if sheet.name.startswith("MB Bundle ")
    } == bundle_before
    assert any(sheet.name.startswith("MB Report ") for sheet in metadata.sheets)


def test_failed_promotion_preserves_last_good_tabs_and_reports_staging_ids(
    db: Database,
) -> None:
    publisher, client = _publisher(db)
    publisher.publish(make_snapshot(), _destination())
    before = {
        sheet.gid: sheet.name
        for sheet in client.get_workbook_metadata("output-sheet").sheets
        if " Staging " not in sheet.name
    }
    client.inject_error_for("promote", GSheetAPIError("promotion failed"))

    with pytest.raises(SheetsPublishError) as exc_info:
        publisher.publish(make_snapshot(), _destination())

    metadata = client.get_workbook_metadata("output-sheet")
    after_visible = {
        sheet.gid: sheet.name
        for sheet in metadata.sheets
        if " Staging " not in sheet.name
    }
    staging_ids = {
        sheet.gid for sheet in metadata.sheets if sheet.name.startswith("MB Staging ")
    }
    assert after_visible == before
    assert staging_ids
    assert exc_info.value.details == {"staging_sheet_ids": sorted(staging_ids)}
    assert exc_info.value.recovery_actions is None
    assert "output-sheet" not in str(exc_info.value)
    assert "café" not in str(exc_info.value)


def test_validation_failure_leaves_only_new_staging_tabs(
    db: Database,
) -> None:
    publisher, client = _publisher(db)
    client.inject_error_for("read", GSheetAPIError("validation failed"))

    with pytest.raises(SheetsPublishError) as exc_info:
        publisher.publish(make_snapshot(), _destination())

    metadata = client.get_workbook_metadata("output-sheet")
    staging = [sheet for sheet in metadata.sheets if " Staging " in sheet.name]
    assert staging
    assert exc_info.value.details == {
        "staging_sheet_ids": sorted(sheet.gid for sheet in staging)
    }


def test_ambiguous_create_recovers_only_exact_owned_staging_ids(
    db: Database,
) -> None:
    malicious = FakeSheetTab(
        "MB Staging 20260721T184233Z Bundle activity", 7, ["keep"], [], None
    )
    publisher, client = _publisher(db, workbook=FakeWorkbook("Output", [malicious]))
    client.inject_error_for("create_after", GSheetAPIError("malformed response"))

    with pytest.raises(SheetsPublishError) as exc_info:
        publisher.publish(make_snapshot(), _destination())

    metadata = client.get_workbook_metadata("output-sheet")
    recovered = {
        sheet.gid
        for sheet in metadata.sheets
        if sheet.managed_prefix == "MB" and " Staging " in sheet.name
    }
    assert recovered
    assert exc_info.value.details == {"staging_sheet_ids": sorted(recovered)}
    assert 7 not in recovered


def test_write_failure_preserves_old_visible_tabs_and_reports_new_staging(
    db: Database,
) -> None:
    publisher, client = _publisher(db)
    publisher.publish(make_snapshot(), _destination())
    before = {
        sheet.gid: sheet.name
        for sheet in client.get_workbook_metadata("output-sheet").sheets
        if " Staging " not in sheet.name
    }
    client.inject_error_for("write", GSheetAPIError("write failed"))

    with pytest.raises(SheetsPublishError) as exc_info:
        publisher.publish(make_snapshot(), _destination())

    metadata = client.get_workbook_metadata("output-sheet")
    after = {
        sheet.gid: sheet.name
        for sheet in metadata.sheets
        if " Staging " not in sheet.name
    }
    staging = {
        sheet.gid
        for sheet in metadata.sheets
        if sheet.managed_prefix == "MB" and " Staging " in sheet.name
    }
    assert after == before
    assert exc_info.value.details == {"staging_sheet_ids": sorted(staging)}


def test_user_lookalikes_and_stale_staging_are_never_replaced(
    db: Database,
) -> None:
    tabs = [
        FakeSheetTab("MB Bundle old activity", 1, ["keep"], [], None),
        FakeSheetTab("MB Manifest", 2, ["keep"], [], None),
        FakeSheetTab("mb bundle 20260721t184233z activity", 3, ["keep"], [], None),
        FakeSheetTab("MB Staging abandoned Bundle activity", 4, ["keep"], [], "MB"),
    ]
    publisher, client = _publisher(db, workbook=FakeWorkbook("Output", tabs))

    publisher.publish(make_snapshot(), _destination())

    by_gid = {
        sheet.gid: sheet.name
        for sheet in client.get_workbook_metadata("output-sheet").sheets
    }
    assert {1, 2, 3, 4}.issubset(by_gid)
    assert by_gid[3] == "mb bundle 20260721t184233z activity"
    assert any("20260721T184233Z-2" in name for name in by_gid.values())


def test_generated_titles_are_bounded_sanitized_and_unicode_case_unique(
    db: Database,
) -> None:
    snapshot = make_text_snapshot(
        ["value"],
        table_names=("A/[very-long]" * 20, "ａ_[VERY-LONG]" * 20),
    )
    publisher, client = _publisher(db)

    publisher.publish(snapshot, _destination())

    names = [
        sheet.name for sheet in client.get_workbook_metadata("output-sheet").sheets
    ]
    normalized = [unicodedata.normalize("NFKC", name).casefold() for name in names]
    assert len(normalized) == len(set(normalized))
    assert all(len(name) <= 100 for name in names)
    assert all(not any(character in name for character in "[]:*?/\\") for name in names)


def test_set_sheets_destination_explicitly_upgrades_write_scope(
    db: Database,
) -> None:
    oauth = TestOAuthClient(write_authorized=False)

    ExportService(db).set_sheets_destination(
        name="dashboard",
        spreadsheet_id="output-sheet",
        managed_tab_prefix="MB",
        actor="cli",
        oauth_client=oauth,
    )

    assert oauth.authorize_require_write == [True]
    assert oauth.is_authorized(require_write=True) is True
    destination = ExportDestinationsRepo(db).resolve("dashboard")
    assert isinstance(destination, ExportDestination)


def test_set_sheets_destination_rejects_inbound_overlap_before_oauth(
    db: Database,
) -> None:
    _insert_inbound_connection(db, "output-sheet")
    oauth = TestOAuthClient(write_authorized=False)

    with pytest.raises(
        ExportDestinationSpreadsheetConflictError, match="inbound connection"
    ):
        ExportService(db).set_sheets_destination(
            name="dashboard",
            spreadsheet_id="output-sheet",
            managed_tab_prefix="MB",
            actor="cli",
            oauth_client=oauth,
        )

    assert oauth.authorize_called == 0


def test_set_sheets_destination_does_not_persist_when_write_grant_fails(
    db: Database,
) -> None:
    class RejectingOAuth:
        def authorize(self, *, require_write: bool = False) -> Never:
            raise GSheetAuthError("write authorization declined")

    with pytest.raises(GSheetAuthError, match="declined"):
        ExportService(db).set_sheets_destination(
            name="dashboard",
            spreadsheet_id="output-sheet",
            managed_tab_prefix="MB",
            actor="cli",
            oauth_client=RejectingOAuth(),
        )

    assert ExportDestinationsRepo(db).list() == []


@pytest.mark.parametrize("prefix", ["", " MB", "MB/Finance", "ＭＢ", "x" * 41])
def test_set_sheets_destination_validates_managed_prefix_before_oauth(
    db: Database, prefix: str
) -> None:
    oauth = TestOAuthClient(write_authorized=False)

    with pytest.raises(ValueError, match="managed tab prefix"):
        ExportService(db).set_sheets_destination(
            name="dashboard",
            spreadsheet_id="output-sheet",
            managed_tab_prefix=prefix,
            actor="cli",
            oauth_client=oauth,
        )

    assert oauth.authorize_called == 0

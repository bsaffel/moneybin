"""Managed-tab Google Sheets export publication tests."""

from __future__ import annotations

import json
import unicodedata
from contextlib import contextmanager
from typing import Any, Never
from unittest.mock import MagicMock, patch

import pytest

from moneybin.connectors.gsheet.errors import (
    GSheetAPIError,
    GSheetAuthError,
    GSheetRateLimitError,
)
from moneybin.connectors.gsheet.sheets_api import SheetsClient, WorkbookMetadata
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
from moneybin.exports.snapshot import PreparedExport
from moneybin.exports.workbook_roles import workbook_role_lease
from moneybin.repositories.export_destinations_repo import (
    ExportDestinationSpreadsheetConflictError,
    ExportDestinationsRepo,
)
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo
from moneybin.services.request_lifetime import (
    PublicationCancelledError,
    RequestLifetime,
)
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
    return SheetsExportPublisher(sheets_client=client), client


def _publish(
    db: Database,
    publisher: SheetsExportPublisher,
    snapshot: PreparedExport,
    destination: ExportDestination | None = None,
    *,
    lifetime: RequestLifetime | None = None,
):
    selected = destination or _destination()
    spreadsheet_id = selected.spreadsheet_id
    assert spreadsheet_id is not None
    with workbook_role_lease(db.path, spreadsheet_id, lifetime=lifetime) as permit:
        ExportDestinationsRepo(db).assert_not_inbound_connection(spreadsheet_id)
        return publisher.publish(
            snapshot,
            selected,
            role_permit=permit,
            publication_lifetime=lifetime,
        )


@contextmanager
def _bound_database(db: Database):
    yield db


def _set_sheets_destination(db: Database, **kwargs: Any):
    def database_provider(*, read_only: bool):
        _ = read_only
        return _bound_database(db)

    with patch(
        "moneybin.database.get_database",
        side_effect=database_provider,
    ):
        return ExportService.set_sheets_destination(**kwargs)


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
        _publish(db, publisher, make_snapshot())

    assert client.requests == []


def test_successful_publish_stages_validates_and_atomically_promotes(
    db: Database,
) -> None:
    user_tab = FakeSheetTab("User Notes", 7, ["Note"], [["keep me"]])
    publisher, client = _publisher(db, workbook=FakeWorkbook("Output", [user_tab]))

    receipt = _publish(db, publisher, make_snapshot())

    metadata = client.get_workbook_metadata("output-sheet")
    names = {sheet.name for sheet in metadata.sheets}
    assert "User Notes" in names
    assert "MB Bundle 20260721T184233Z activity" in names
    assert "MB Bundle Manifest" in names
    assert "MB Bundle Dictionary" in names
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


def test_cancelled_request_cannot_promote_staged_sheets(db: Database) -> None:
    """Timeout cancellation leaves staging tabs but no new visible snapshot."""
    lifetime = RequestLifetime()

    class CancelBeforePromotion(TestSheetsClient):
        cancelled = False

        def read_sheet_values(
            self,
            spreadsheet_id: str,
            sheet_name: str,
            *,
            require_write: bool = False,
        ) -> list[list[str]]:
            values = super().read_sheet_values(
                spreadsheet_id,
                sheet_name,
                require_write=require_write,
            )
            if not self.cancelled:
                self.cancelled = True
                lifetime.cancel_and_wait()
            return values

    client = CancelBeforePromotion()
    client.register_workbook("output-sheet", FakeWorkbook("Output"))
    publisher = SheetsExportPublisher(sheets_client=client)

    with pytest.raises(PublicationCancelledError):
        _publish(db, publisher, make_snapshot(), lifetime=lifetime)

    names = {
        sheet.name for sheet in client.get_workbook_metadata("output-sheet").sheets
    }
    assert any(" Staging " in name for name in names)
    assert not any(name.startswith("MB Bundle ") for name in names)
    assert [operation for operation, _ in client.requests] == ["create", "write"]


def test_lost_promotion_response_is_reconciled_as_success(db: Database) -> None:
    """A committed Google batch with a lost response must not be reported stale."""

    class PromoteThenLoseResponse(TestSheetsClient):
        def promote_sheets(self, *args: Any, **kwargs: Any) -> None:
            super().promote_sheets(*args, **kwargs)
            raise GSheetAPIError("response lost")

    client = PromoteThenLoseResponse()
    client.register_workbook("output-sheet", FakeWorkbook("Output"))
    publisher = SheetsExportPublisher(sheets_client=client)

    receipt = _publish(db, publisher, make_snapshot())

    assert receipt.sheets_identity == "MB:20260721T184233Z"
    names = {
        sheet.name for sheet in client.get_workbook_metadata("output-sheet").sheets
    }
    assert "MB Bundle 20260721T184233Z activity" in names
    assert not any(" Staging " in name for name in names)


def test_publish_retries_a_transient_sheets_rate_limit(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One temporary 429 during validation does not abandon a staged export."""

    class RateLimitedReadTwice(TestSheetsClient):
        attempts = 0

        def read_sheet_values(self, *args: Any, **kwargs: Any) -> list[list[str]]:
            self.attempts += 1
            if self.attempts <= 2:
                raise GSheetRateLimitError("temporary")
            return super().read_sheet_values(*args, **kwargs)

    client = RateLimitedReadTwice()
    client.register_workbook("output-sheet", FakeWorkbook("Output"))
    delays: list[float] = []

    def capture_sleep(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr("moneybin.exports.sheets.time.sleep", capture_sleep)

    receipt = _publish(db, SheetsExportPublisher(sheets_client=client), make_snapshot())

    assert receipt.sheets_identity == "MB:20260721T184233Z"
    assert client.attempts == 5
    assert delays == [1.0, 1.5]


def test_publish_encodes_nulls_distinctly_from_empty_strings(db: Database) -> None:
    snapshot = make_text_snapshot((None, "", r"\N", r"\literal"))
    publisher, client = _publisher(db)

    _publish(db, publisher, snapshot)

    assert client.read_sheet_values(
        "output-sheet", "MB Bundle 20260721T184233Z activity"
    ) == [["note"], [r"\N"], [""], [r"\\N"], [r"\\literal"]]
    manifest = json.loads(
        client.read_sheet_values("output-sheet", "MB Bundle Manifest")[1][0]
    )
    assert manifest["sheets_encoding"] == {
        "scheme": "moneybin.sheets-cell",
        "version": 1,
        "null": r"\N",
        "escape": "\\",
    }


def test_publish_uses_write_capability_for_output_metadata_and_validation(
    db: Database,
) -> None:
    class WriteOnlyOutputSheets(TestSheetsClient):
        def get_workbook_metadata(
            self, spreadsheet_id: str, *, require_write: bool = False
        ) -> WorkbookMetadata:
            if not require_write:
                raise GSheetAuthError("read-only grant is intentionally absent")
            return super().get_workbook_metadata(spreadsheet_id)

        def read_sheet_values(
            self,
            spreadsheet_id: str,
            sheet_name: str,
            *,
            require_write: bool = False,
        ) -> list[list[str]]:
            if not require_write:
                raise GSheetAuthError("read-only grant is intentionally absent")
            return super().read_sheet_values(spreadsheet_id, sheet_name)

    client = WriteOnlyOutputSheets()
    client.register_workbook("output-sheet", FakeWorkbook("Output"))
    oauth = TestOAuthClient(authorized=False)
    _set_sheets_destination(
        db,
        name="dashboard",
        spreadsheet_id="output-sheet",
        managed_tab_prefix="MB",
        actor="test",
        oauth_client=oauth,
    )
    destination = ExportDestinationsRepo(db).resolve("dashboard")
    assert isinstance(destination, ExportDestination)
    publisher = SheetsExportPublisher(sheets_client=client)

    receipt = _publish(db, publisher, make_snapshot(), destination)

    assert oauth.authorize_require_write == [True]
    assert receipt.sheets_identity == "MB:20260721T184233Z"


def test_report_publication_cannot_select_or_replace_bundle_tabs(
    db: Database,
) -> None:
    publisher, client = _publisher(db)
    _publish(db, publisher, make_snapshot())
    bundle_before = {
        sheet.gid: sheet.name
        for sheet in client.get_workbook_metadata("output-sheet").sheets
        if sheet.name.startswith("MB Bundle ")
    }

    _publish(db, publisher, make_snapshot(report=True))

    metadata = client.get_workbook_metadata("output-sheet")
    assert {
        sheet.gid: sheet.name
        for sheet in metadata.sheets
        if sheet.name.startswith("MB Bundle ")
    } == bundle_before
    assert any(sheet.name.startswith("MB Report ") for sheet in metadata.sheets)


@pytest.mark.parametrize("first_report", [False, True])
def test_alternating_subjects_preserve_each_subjects_verifiable_metadata(
    db: Database,
    first_report: bool,
) -> None:
    """Bundle and report latest-state tabs retain independent receipts."""
    publisher, client = _publisher(db)
    first = make_snapshot(report=first_report)
    second = make_snapshot(report=not first_report)

    _publish(db, publisher, first)
    _publish(db, publisher, second)

    metadata = client.get_workbook_metadata("output-sheet")
    names = {sheet.name for sheet in metadata.sheets}
    assert {
        "MB Bundle Manifest",
        "MB Bundle Dictionary",
        "MB Report Manifest",
        "MB Report Dictionary",
    }.issubset(names)
    bundle_manifest = client.read_sheet_values("output-sheet", "MB Bundle Manifest")[1][
        0
    ]
    report_manifest = client.read_sheet_values("output-sheet", "MB Report Manifest")[1][
        0
    ]
    assert json.loads(bundle_manifest)["subject"]["kind"] == "bundle"
    assert json.loads(report_manifest)["subject"]["kind"] == "report"


def test_failed_promotion_preserves_last_good_tabs_and_reports_staging_ids(
    db: Database,
) -> None:
    publisher, client = _publisher(db)
    _publish(db, publisher, make_snapshot())
    before = {
        sheet.gid: sheet.name
        for sheet in client.get_workbook_metadata("output-sheet").sheets
        if " Staging " not in sheet.name
    }
    client.inject_error_for("promote", GSheetAPIError("promotion failed"))

    with pytest.raises(SheetsPublishError) as exc_info:
        _publish(db, publisher, make_snapshot())

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
        _publish(db, publisher, make_snapshot())

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
        _publish(db, publisher, make_snapshot())

    metadata = client.get_workbook_metadata("output-sheet")
    recovered = {
        sheet.gid
        for sheet in metadata.sheets
        if sheet.managed_prefix == "MB" and " Staging " in sheet.name
    }
    assert recovered
    assert exc_info.value.details == {"staging_sheet_ids": sorted(recovered)}
    assert 7 not in recovered
    assert [operation for operation, _ in client.requests] == ["create"]


def test_mismatched_create_response_reconciles_without_touching_user_tab(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = MagicMock()
    spreadsheets = service.spreadsheets.return_value
    created_tabs: list[dict[str, Any]] = []

    def execute_metadata() -> dict[str, Any]:
        user_tab = {
            "properties": {
                "title": "User Notes",
                "sheetId": 7,
                "gridProperties": {"rowCount": 1, "columnCount": 1},
            }
        }
        return {
            "properties": {"title": "Output"},
            "sheets": [user_tab, *created_tabs],
        }

    def execute_create() -> dict[str, Any]:
        requests = spreadsheets.batchUpdate.call_args.kwargs["body"]["requests"]
        replies: list[dict[str, Any]] = []
        for index in range(0, len(requests), 2):
            properties = requests[index]["addSheet"]["properties"]
            created_tabs.append({
                "properties": properties,
                "developerMetadata": [
                    {
                        "metadataKey": "moneybin.managed_prefix",
                        "metadataValue": "MB",
                        "visibility": "DOCUMENT",
                    }
                ],
            })
            replies.extend([
                {"addSheet": {"properties": dict(properties)}},
                {"createDeveloperMetadata": {"developerMetadata": {}}},
            ])
        replies[0] = {"addSheet": {"properties": {"title": "User Notes", "sheetId": 7}}}
        return {"replies": replies}

    spreadsheets.get.return_value.execute.side_effect = execute_metadata
    spreadsheets.batchUpdate.return_value.execute.side_effect = execute_create
    client = SheetsClient(oauth=TestOAuthClient(write_authorized=True))
    monkeypatch.setattr(client, "_build_service", MagicMock(return_value=service))

    with pytest.raises(SheetsPublishError) as exc_info:
        _publish(
            db,
            SheetsExportPublisher(sheets_client=client),
            make_snapshot(),
        )

    recovered = {int(tab["properties"]["sheetId"]) for tab in created_tabs}
    assert exc_info.value.details == {"staging_sheet_ids": sorted(recovered)}
    assert 7 not in recovered
    assert spreadsheets.batchUpdate.call_count == 1
    assert spreadsheets.values.return_value.batchUpdate.call_count == 0
    assert spreadsheets.values.return_value.get.call_count == 0


def test_write_failure_preserves_old_visible_tabs_and_reports_new_staging(
    db: Database,
) -> None:
    publisher, client = _publisher(db)
    _publish(db, publisher, make_snapshot())
    before = {
        sheet.gid: sheet.name
        for sheet in client.get_workbook_metadata("output-sheet").sheets
        if " Staging " not in sheet.name
    }
    client.inject_error_for("write", GSheetAPIError("write failed"))

    with pytest.raises(SheetsPublishError) as exc_info:
        _publish(db, publisher, make_snapshot())

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

    _publish(db, publisher, make_snapshot())

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

    _publish(db, publisher, snapshot)

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

    _set_sheets_destination(
        db,
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


def test_set_sheets_destination_releases_database_before_oauth(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Configuration reads close before the interactive external authorization."""
    active = False
    modes: list[bool] = []

    @contextmanager
    def database_context(*, read_only: bool):
        nonlocal active
        modes.append(read_only)
        active = True
        try:
            yield db
        finally:
            active = False

    class ObservedOAuth(TestOAuthClient):
        def authorize(self, *, require_write: bool = False):
            assert active is False
            return super().authorize(require_write=require_write)

    monkeypatch.setattr("moneybin.database.get_database", database_context)

    ExportService.set_sheets_destination(
        name="dashboard",
        spreadsheet_id="output-sheet",
        managed_tab_prefix="MB",
        actor="cli",
        oauth_client=ObservedOAuth(write_authorized=False),
    )

    assert modes == [True, False]


def test_set_sheets_destination_rejects_inbound_overlap_before_oauth(
    db: Database,
) -> None:
    _insert_inbound_connection(db, "output-sheet")
    oauth = TestOAuthClient(write_authorized=False)

    with pytest.raises(
        ExportDestinationSpreadsheetConflictError, match="inbound connection"
    ):
        _set_sheets_destination(
            db,
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
        _set_sheets_destination(
            db,
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
        _set_sheets_destination(
            db,
            name="dashboard",
            spreadsheet_id="output-sheet",
            managed_tab_prefix=prefix,
            actor="cli",
            oauth_client=oauth,
        )

    assert oauth.authorize_called == 0

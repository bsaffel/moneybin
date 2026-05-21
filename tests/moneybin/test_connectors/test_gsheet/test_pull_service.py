"""Tests for GSheetPullService."""

from __future__ import annotations

import pytest

from moneybin.connectors.gsheet.connection_service import (
    ConnectionRequest,
    GSheetConnectionService,
)
from moneybin.connectors.gsheet.errors import (
    GSheetAuthError,
    GSheetError,
    GSheetRateLimitError,
    GSheetUnreachableError,
)
from moneybin.connectors.gsheet.pull_service import GSheetPullService
from moneybin.connectors.gsheet.testing.fake_oauth_client import TestOAuthClient
from moneybin.connectors.gsheet.testing.fake_sheets_client import (
    FakeSheetTab,
    FakeWorkbook,
    TestSheetsClient,
)
from moneybin.database import Database
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo


def _tiller_workbook(spreadsheet_id: str = "ss1") -> FakeWorkbook:
    _ = spreadsheet_id  # name is bound at register_workbook time
    return FakeWorkbook(
        title="Tiller",
        tabs=[
            FakeSheetTab(
                name="Transactions",
                gid=0,
                headers=["Date", "Description", "Amount", "Account"],
                rows=[["2026-01-15", "WF", "-87.42", "Checking"]],
            )
        ],
    )


def _setup(
    db: Database, *, spreadsheet_id: str = "ss1", account_id: str = "acct_a"
) -> tuple[GSheetPullService, TestSheetsClient, str]:
    """Connect one transactions sheet without pulling; return service + cid."""
    oauth = TestOAuthClient(authorized=True)
    sheets = TestSheetsClient()
    sheets.register_workbook(spreadsheet_id, _tiller_workbook(spreadsheet_id))
    conn_svc = GSheetConnectionService(db=db, sheets_client=sheets, oauth_client=oauth)
    result = conn_svc.connect(
        ConnectionRequest(
            url=(f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid=0"),
            adapter="transactions",
            account_name="Checking",
            account_id=account_id,
            yes=True,
            no_initial_pull=True,
        )
    )
    pull_svc = GSheetPullService(db=db, sheets_client=sheets, oauth_client=oauth)
    return pull_svc, sheets, result.connection.connection_id


def test_pull_inserts_rows(in_memory_db: Database) -> None:
    pull_svc, _, cid = _setup(in_memory_db)
    result = pull_svc.pull_connection(cid)
    assert result.status == "complete"
    assert result.load_result is not None
    assert result.load_result.rows_inserted == 1


def test_pull_isolates_per_connection_failure(in_memory_db: Database) -> None:
    """One unhealthy connection in the batch doesn't crash the others.

    Both connections are healthy at scheduling time, but the fake's
    one-shot ``inject_error`` fires on the FIRST pull. The second
    connection should still succeed.
    """
    pull_svc, sheets, _cid_a = _setup(in_memory_db)
    # Add a second connection using a different spreadsheet.
    _pull_b, _, _cid_b = _setup(in_memory_db, spreadsheet_id="ss2", account_id="acct_b")
    sheets.register_workbook("ss2", _tiller_workbook("ss2"))
    # Inject a one-shot unreachable on the first call; the second should succeed.
    sheets.inject_error(GSheetUnreachableError("403 boom"))
    results = pull_svc.pull_all_healthy()
    statuses = sorted(r.status for r in results)
    # Exactly one unreachable and one complete — order may vary by repo's
    # created_at ordering.
    assert "complete" in statuses
    assert "unreachable" in statuses


def test_pull_marks_auth_expired(in_memory_db: Database) -> None:
    pull_svc, sheets, cid = _setup(in_memory_db)
    sheets.inject_error(GSheetAuthError("token revoked"))
    result = pull_svc.pull_connection(cid)
    assert result.status == "auth_expired"
    # Connection row reflects the new state.
    repo = GSheetConnectionsRepo(in_memory_db)
    conn_row = repo.get(cid)
    assert conn_row is not None
    assert conn_row["status"] == "auth_expired"
    assert conn_row["last_drift_reason"] == "token revoked"


def test_pull_handles_rate_limit_with_retry(in_memory_db: Database) -> None:
    pull_svc, sheets, cid = _setup(in_memory_db)
    # One rate-limit error → retry succeeds on the second attempt.
    sheets.inject_error(GSheetRateLimitError("429"))
    result = pull_svc.pull_connection(cid)
    assert result.status == "complete"


def test_pull_detects_drift_and_skips(in_memory_db: Database) -> None:
    pull_svc, sheets, cid = _setup(in_memory_db)
    # Remove the pinned "Amount" column — drift detector should refuse the load.
    sheets.mutate_tab("ss1", 0, headers=["Date", "Description"])
    result = pull_svc.pull_connection(cid)
    assert result.status == "drift_detected"
    assert result.drift_reason is not None
    assert "Amount" in result.drift_reason


def test_pull_increments_consecutive_failure_on_error(
    in_memory_db: Database,
) -> None:
    pull_svc, sheets, cid = _setup(in_memory_db)
    sheets.inject_error(GSheetAuthError("token revoked"))
    pull_svc.pull_connection(cid)
    repo = GSheetConnectionsRepo(in_memory_db)
    conn_row = repo.get(cid)
    assert conn_row is not None
    assert conn_row["consecutive_failure_count"] == 1


def test_pull_resets_failure_count_on_success(in_memory_db: Database) -> None:
    pull_svc, sheets, cid = _setup(in_memory_db)
    repo = GSheetConnectionsRepo(in_memory_db)
    # First pull fails with auth.
    sheets.inject_error(GSheetAuthError("token revoked"))
    pull_svc.pull_connection(cid)
    conn_row = repo.get(cid)
    assert conn_row is not None
    assert conn_row["consecutive_failure_count"] == 1

    # Status is now auth_expired — promote back to healthy so pull_connection
    # treats it as a regular pull (no need to be in pull_all_healthy here).
    repo.update_status(cid, status="healthy", reason=None)
    result = pull_svc.pull_connection(cid)
    assert result.status == "complete"
    conn_row = repo.get(cid)
    assert conn_row is not None
    assert conn_row["consecutive_failure_count"] == 0


def test_pull_connection_unknown_raises(in_memory_db: Database) -> None:
    oauth = TestOAuthClient(authorized=True)
    sheets = TestSheetsClient()
    pull_svc = GSheetPullService(
        db=in_memory_db, sheets_client=sheets, oauth_client=oauth
    )
    with pytest.raises(GSheetError, match="Unknown connection"):
        pull_svc.pull_connection("bogus")


def test_pull_all_healthy_skips_disconnected(in_memory_db: Database) -> None:
    pull_svc, _sheets, cid = _setup(in_memory_db)
    # Mark the only connection as disconnected.
    GSheetConnectionsRepo(in_memory_db).soft_disconnect(cid)
    results = pull_svc.pull_all_healthy()
    assert results == []

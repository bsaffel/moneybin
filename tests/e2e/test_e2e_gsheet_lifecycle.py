"""E2E lifecycle test for the ``moneybin gsheet`` CLI subgroup.

Drives the full ``connect → pull → drift → reconnect → disconnect`` flow
through the real Typer app (``CliRunner`` in-process) and real ``Database``,
swapping only the network-touching collaborators (``SheetsClient`` →
``TestSheetsClient``, ``GoogleOAuthClient`` → ``TestOAuthClient``) at the
``_build_*`` factory seams.

Why in-process rather than subprocess: the project has no subprocess HTTP-mock
precedent — ``googleapiclient`` uses ``httplib2``, not ``httpx``, so respx
doesn't apply — and ``test_e2e_help.py`` already exercises subprocess boot of
the ``gsheet`` subgroup. The marginal coverage of a subprocess lifecycle test
versus this in-process variant doesn't justify a production-side test-mode
flag (the equivalent of ``MONEYBIN_GSHEET__USE_TEST_CLIENT``, rejected as
test-mode-in-prod-wiring with no analogue in the codebase).
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.connectors.gsheet.connection_service import GSheetConnectionService
from moneybin.connectors.gsheet.pull_service import GSheetPullService
from moneybin.connectors.gsheet.testing import (
    FakeSheetTab,
    FakeWorkbook,
    TestOAuthClient,
    TestSheetsClient,
)
from moneybin.database import Database
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo

pytestmark = pytest.mark.e2e

_SPREADSHEET_ID = "ss_lifecycle"
_SHEET_URL = f"https://docs.google.com/spreadsheets/d/{_SPREADSHEET_ID}/edit#gid=0"
_HEADERS = ["Date", "Description", "Amount", "Account"]
_INITIAL_ROWS = [
    ["2026-01-15", "Whole Foods", "-42.50", "Checking"],
    ["2026-01-16", "Paycheck", "1500.00", "Checking"],
    ["2026-01-17", "Coffee", "-5.25", "Checking"],
]


def _build_workbook() -> FakeWorkbook:
    return FakeWorkbook(
        title="Family Budget",
        tabs=[
            FakeSheetTab(
                name="Transactions",
                gid=0,
                headers=list(_HEADERS),
                rows=[list(r) for r in _INITIAL_ROWS],
            ),
        ],
    )


@pytest.fixture()
def lifecycle_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Real Database (encrypted, mocked keyring) for the duration of the test.

    The ``tests/moneybin`` ``mock_secret_store`` fixture isn't visible from
    the e2e tier, so we build the same shape inline.
    """
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-e2e-lifecycle"
    db = Database(
        tmp_path / "gsheet-lifecycle.duckdb",
        secret_store=secret_store,
        no_auto_upgrade=True,
    )
    try:
        yield db
    finally:
        db.close()


@pytest.fixture()
def fakes() -> tuple[TestSheetsClient, TestOAuthClient]:
    """Shared TestSheetsClient + TestOAuthClient — state persists across calls."""
    sheets = TestSheetsClient()
    sheets.register_workbook(_SPREADSHEET_ID, _build_workbook())
    oauth = TestOAuthClient(authorized=True)
    return sheets, oauth


@contextmanager
def _patch_cli_builders(
    db: Database,
    sheets: TestSheetsClient,
    oauth: TestOAuthClient,
) -> Generator[None, None, None]:
    """Swap the gsheet CLI's two builder seams to use shared fakes + the real DB.

    ``_build_connection_service`` and ``_build_pull_service`` are context
    managers; patch them to yield services bound to the live shared
    ``TestSheetsClient`` so a single test sequence sees state mutate
    consistently. The real database is reused so the encrypted on-disk
    state survives across CLI invocations within the test.
    """

    @contextmanager
    def _connection_service() -> Generator[GSheetConnectionService, None, None]:
        yield GSheetConnectionService(db=db, sheets_client=sheets, oauth_client=oauth)

    @contextmanager
    def _pull_service() -> Generator[tuple[GSheetPullService, Database], None, None]:
        yield GSheetPullService(db=db, sheets_client=sheets, oauth_client=oauth), db

    with (
        patch(
            "moneybin.cli.commands.gsheet._build_connection_service",
            _connection_service,
        ),
        patch(
            "moneybin.cli.commands.gsheet._build_pull_service",
            _pull_service,
        ),
        # Suppress the refresh step — that pipeline is exercised in scenarios;
        # this test focuses on the gsheet lifecycle plumbing in isolation.
        patch("moneybin.services.refresh.refresh"),
    ):
        yield


def _raw_row_count(db: Database, connection_id: str) -> int:
    row = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_origin = ?",
        [connection_id],
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_gsheet_full_lifecycle(
    lifecycle_db: Database,
    fakes: tuple[TestSheetsClient, TestOAuthClient],
) -> None:
    """``connect → pull → drift → reconnect → disconnect --purge`` end-to-end.

    Each step asserts on the database state the CLI is supposed to leave
    behind, not on stdout phrasing. The CliRunner exit code asserts the
    Typer wiring (option parsing, error envelope) for each command in turn.
    """
    sheets, oauth = fakes
    runner = CliRunner()

    with _patch_cli_builders(lifecycle_db, sheets, oauth):
        # 1. connect — registers the connection and runs the initial pull.
        result = runner.invoke(
            app,
            [
                "gsheet",
                "connect",
                _SHEET_URL,
                "--adapter",
                "transactions",
                "--account-name",
                "Checking",
                "--account-id",
                "acct_checking",
                "--yes",
                "--output",
                "json",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        repo = GSheetConnectionsRepo(lifecycle_db)
        connections = repo.list_all()
        assert len(connections) == 1
        connection_id = connections[0]["connection_id"]
        # Three rows hand-counted from _INITIAL_ROWS.
        assert _raw_row_count(lifecycle_db, connection_id) == 3

        # 2. pull — idempotent: re-pulling the unchanged sheet inserts no
        # new rows (existing rows upsert in place).
        result = runner.invoke(
            app,
            ["gsheet", "pull", connection_id, "--no-refresh", "--output", "json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        assert _raw_row_count(lifecycle_db, connection_id) == 3

        # 3. drift — drop the pinned "Amount" column; the next pull must
        # refuse the load and mark the connection drift_detected.
        sheets.mutate_tab(_SPREADSHEET_ID, 0, headers=["Date", "Description"])
        result = runner.invoke(
            app,
            ["gsheet", "pull", connection_id, "--no-refresh", "--output", "json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        row = repo.get(connection_id)
        assert row is not None
        assert row["status"] == "drift_detected"
        # Still three rows — the load was refused.
        assert _raw_row_count(lifecycle_db, connection_id) == 3

        # 4. reconnect — restore the headers, then reconnect must re-pin the
        # mapping and flip status back to healthy.
        sheets.mutate_tab(
            _SPREADSHEET_ID,
            0,
            headers=list(_HEADERS),
            rows=[list(r) for r in _INITIAL_ROWS],
        )
        result = runner.invoke(
            app,
            ["gsheet", "reconnect", connection_id, "--yes", "--output", "json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        row = repo.get(connection_id)
        assert row is not None
        assert row["status"] == "healthy"

        # 5. disconnect --purge --yes — connection row deleted, raw rows wiped.
        result = runner.invoke(
            app,
            [
                "gsheet",
                "disconnect",
                connection_id,
                "--purge",
                "--yes",
                "--output",
                "json",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output
        assert repo.get(connection_id) is None
        assert _raw_row_count(lifecycle_db, connection_id) == 0

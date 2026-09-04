"""Tests for the `moneybin gsheet` CLI subgroup."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.connectors.gsheet.adapters.base import (
    DetectionResult,
    GSheetConnection,
    LoadResult,
)
from moneybin.connectors.gsheet.connection_service import ConnectResult
from moneybin.connectors.gsheet.pull_service import PullResult
from moneybin.privacy.classified_envelope import classify
from moneybin.privacy.payloads.gsheet import GsheetConnectionsPayload

runner = CliRunner()


def _make_connection(
    *,
    connection_id: str = "conn_abc123",
    adapter: str = "transactions",
    status: str = "healthy",
    last_status_reason: str | None = None,
) -> GSheetConnection:
    return GSheetConnection(
        connection_id=connection_id,
        spreadsheet_id="ssid_xyz",
        sheet_gid=0,
        sheet_name="Sheet1",
        workbook_name="My Budget",
        adapter=adapter,
        alias=None,
        account_id=None,
        account_name="Checking",
        column_mapping={"Date": "date", "Amount": "amount"},
        header_signature=["Date", "Amount"],
        date_format="%Y-%m-%d",
        sign_convention="negative_expense",
        number_format="plain",
        skip_rows=0,
        skip_trailing_patterns=[],
        status=status,
        last_pull_at=None,
        last_pull_import_id=None,
        last_success_at=None,
        last_status_reason=last_status_reason,
        consecutive_failure_count=0,
    )


def _make_detection() -> DetectionResult:
    return DetectionResult(
        confidence="high",
        column_mapping={"Date": "date", "Amount": "amount"},
        header_signature=["Date", "Amount"],
        date_format="%Y-%m-%d",
        sign_convention="negative_expense",
        number_format="plain",
        skip_rows=0,
        skip_trailing_patterns=[],
        notes=[],
    )


def _make_load_result(*, rows_inserted: int = 5, rows_upserted: int = 0) -> LoadResult:
    return LoadResult(
        rows_inserted=rows_inserted,
        rows_soft_deleted=0,
        rows_upserted=rows_upserted,
    )


# -------------------------------------------------------------------- auth ---


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_auth_command_runs_oauth(mock_build: MagicMock) -> None:
    client = MagicMock()
    client.is_authorized.return_value = False
    mock_build.return_value = client
    result = runner.invoke(app, ["gsheet", "auth"])
    assert result.exit_code == 0, result.output
    client.authorize.assert_called_once()


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_auth_short_circuits_when_already_authorized(
    mock_build: MagicMock,
) -> None:
    """Mirror of the MCP gsheet_auth short-circuit: don't re-open the browser."""
    client = MagicMock()
    client.is_authorized.return_value = True
    mock_build.return_value = client
    result = runner.invoke(app, ["gsheet", "auth"])
    assert result.exit_code == 0, result.output
    client.authorize.assert_not_called()
    assert "Already authorized" in result.stdout
    assert "--force" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_auth_force_reauthenticates_when_already_authorized(
    mock_build: MagicMock,
) -> None:
    """--force bypasses the short-circuit even with a refresh token on file."""
    client = MagicMock()
    client.is_authorized.return_value = True
    mock_build.return_value = client
    result = runner.invoke(app, ["gsheet", "auth", "--force"])
    assert result.exit_code == 0, result.output
    client.authorize.assert_called_once()


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_auth_json_output(mock_build: MagicMock) -> None:
    client = MagicMock()
    client.is_authorized.return_value = False
    mock_build.return_value = client
    result = runner.invoke(app, ["gsheet", "auth", "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)["data"]
    assert payload["status"] == "authorized"


# ----------------------------------------------------------------- connect ---


@pytest.mark.unit
def test_gsheet_connect_help_describes_role_derived_sign_behavior() -> None:
    result = runner.invoke(app, ["gsheet", "connect", "--help"])
    help_text = " ".join(result.output.split())

    assert result.exit_code == 0, result.output
    assert "positive_is_expense" not in help_text
    assert "detected debit or credit role" in help_text
    assert "cannot derive polarity" in help_text


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_connect_text_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.connect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection(),
        initial_pull=_make_load_result(),
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app,
        [
            "gsheet",
            "connect",
            "https://docs.google.com/spreadsheets/d/ssid_xyz/edit#gid=0",
            "--account-name",
            "Checking",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Connected" in result.stdout
    assert "conn_abc123" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_connect_json_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.connect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection(),
        initial_pull=_make_load_result(rows_inserted=3),
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app,
        [
            "gsheet",
            "connect",
            "https://docs.google.com/spreadsheets/d/ssid_xyz/edit#gid=0",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)["data"]
    assert payload["connection"]["connection_id"] == "conn_abc123"
    assert payload["initial_pull"]["rows_inserted"] == 3


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_connect_no_initial_pull(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.connect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection(),
        initial_pull=None,
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app,
        [
            "gsheet",
            "connect",
            "https://docs.google.com/spreadsheets/d/ssid_xyz/edit#gid=0",
            "--no-initial-pull",
        ],
    )
    assert result.exit_code == 0, result.output
    req = service.connect.call_args.args[0]
    assert req.no_initial_pull is True


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_connect_column_mapping_json(mock_build: MagicMock) -> None:
    """--column-mapping accepts JSON form."""
    service = MagicMock()
    service.connect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection(),
        initial_pull=None,
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app,
        [
            "gsheet",
            "connect",
            "https://docs.google.com/spreadsheets/d/ssid_xyz/edit#gid=0",
            "--column-mapping",
            '{"Date":"date","Amount":"amount"}',
            "--no-initial-pull",
        ],
    )
    assert result.exit_code == 0, result.output
    req = service.connect.call_args.args[0]
    assert req.column_mapping == {"Date": "date", "Amount": "amount"}


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_connect_column_mapping_kv(mock_build: MagicMock) -> None:
    """--column-mapping accepts comma-separated key=value pairs."""
    service = MagicMock()
    service.connect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection(),
        initial_pull=None,
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app,
        [
            "gsheet",
            "connect",
            "https://docs.google.com/spreadsheets/d/ssid_xyz/edit#gid=0",
            "--column-mapping",
            "Date=date,Amount=amount",
            "--no-initial-pull",
        ],
    )
    assert result.exit_code == 0, result.output
    req = service.connect.call_args.args[0]
    assert req.column_mapping == {"Date": "date", "Amount": "amount"}


# -------------------------------------------------------------------- pull ---


@pytest.mark.unit
@patch("moneybin.orchestration.refresh.refresh")
@patch("moneybin.database.get_database")
@patch("moneybin.connectors.gsheet.sheets_api.SheetsClient")
@patch("moneybin.connectors.gsheet.pull_service.GSheetPullService")
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_pull_single_connection_runs_refresh(
    mock_oauth: MagicMock,
    mock_service_cls: MagicMock,
    mock_sheets_cls: MagicMock,  # noqa: ARG001  # patched for namespace presence
    mock_get_db: MagicMock,
    mock_refresh: MagicMock,
) -> None:
    """Pull <connection_id> runs the refresh chain by default.

    The list is explicit, and an explicit list is never widened by a newly
    added canonical step — so every stage a pulled row needs has to be named
    here. `rates` is named for the same reason `transform` is: a Sheet can
    carry foreign-currency rows, and leaving the rate cache empty would defeat
    the offline-conversion guarantee until some later unrelated refresh.
    """
    service = MagicMock()
    service.pull_connection.return_value = PullResult(
        connection_id="conn_abc123",
        status="complete",
        load_result=_make_load_result(),
    )
    mock_service_cls.return_value = service
    mock_oauth.return_value = MagicMock()
    db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = db

    result = runner.invoke(app, ["gsheet", "pull", "conn_abc123"])
    assert result.exit_code == 0, result.output
    service.pull_connection.assert_called_once_with("conn_abc123")
    mock_refresh.assert_called_once()
    assert mock_refresh.call_args.kwargs == {
        "steps": ["match", "transform", "categorize", "rates"]
    }


@pytest.mark.unit
@patch("moneybin.orchestration.refresh.refresh")
@patch("moneybin.database.get_database")
@patch("moneybin.connectors.gsheet.sheets_api.SheetsClient")
@patch("moneybin.connectors.gsheet.pull_service.GSheetPullService")
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_pull_nonzero_exit_on_failed_pull(
    mock_oauth: MagicMock,
    mock_service_cls: MagicMock,
    mock_sheets_cls: MagicMock,  # noqa: ARG001
    mock_get_db: MagicMock,
    mock_refresh: MagicMock,  # noqa: ARG001  # --no-refresh isolates the pull-exit path
) -> None:
    """A non-complete pull status makes `gsheet pull` exit 1 (CI/agent signal)."""
    service = MagicMock()
    service.pull_connection.return_value = PullResult(
        connection_id="conn_abc123",
        status="auth_expired",
        error_message="OAuth token revoked.",
    )
    mock_service_cls.return_value = service
    mock_oauth.return_value = MagicMock()
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    result = runner.invoke(app, ["gsheet", "pull", "conn_abc123", "--no-refresh"])
    assert result.exit_code == 1, result.output


@pytest.mark.unit
@patch("moneybin.orchestration.refresh.refresh")
@patch("moneybin.database.get_database")
@patch("moneybin.connectors.gsheet.sheets_api.SheetsClient")
@patch("moneybin.connectors.gsheet.pull_service.GSheetPullService")
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_pull_no_refresh_skips_pipeline(
    mock_oauth: MagicMock,
    mock_service_cls: MagicMock,
    mock_sheets_cls: MagicMock,  # noqa: ARG001
    mock_get_db: MagicMock,
    mock_refresh: MagicMock,
) -> None:
    """--no-refresh skips the refresh pipeline call."""
    service = MagicMock()
    service.pull_all_healthy.return_value = []
    mock_service_cls.return_value = service
    mock_oauth.return_value = MagicMock()
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    result = runner.invoke(app, ["gsheet", "pull", "--no-refresh"])
    assert result.exit_code == 0, result.output
    service.pull_all_healthy.assert_called_once()
    mock_refresh.assert_not_called()


@pytest.mark.unit
@patch("moneybin.orchestration.refresh.refresh")
@patch("moneybin.database.get_database")
@patch("moneybin.connectors.gsheet.sheets_api.SheetsClient")
@patch("moneybin.connectors.gsheet.pull_service.GSheetPullService")
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_pull_json_output(
    mock_oauth: MagicMock,
    mock_service_cls: MagicMock,
    mock_sheets_cls: MagicMock,  # noqa: ARG001
    mock_get_db: MagicMock,
    mock_refresh: MagicMock,  # noqa: ARG001
) -> None:
    service = MagicMock()
    service.pull_connection.return_value = PullResult(
        connection_id="conn_abc123",
        status="complete",
        load_result=_make_load_result(rows_inserted=7),
    )
    mock_service_cls.return_value = service
    mock_oauth.return_value = MagicMock()
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    result = runner.invoke(
        app,
        ["gsheet", "pull", "conn_abc123", "--output", "json", "--no-refresh"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)["data"]
    # Payload shape: {"pulls": [...], "refresh_error": str | None}
    assert payload["refresh_error"] is None
    assert payload["pulls"][0]["connection_id"] == "conn_abc123"
    assert payload["pulls"][0]["status"] == "complete"
    assert payload["pulls"][0]["rows_inserted"] == 7


@pytest.mark.unit
@patch("moneybin.orchestration.refresh.refresh")
@patch("moneybin.database.get_database")
@patch("moneybin.connectors.gsheet.sheets_api.SheetsClient")
@patch("moneybin.connectors.gsheet.pull_service.GSheetPullService")
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_pull_reports_a_transfer_its_refresh_retired(
    mock_oauth: MagicMock,
    mock_service_cls: MagicMock,
    mock_sheets_cls: MagicMock,  # noqa: ARG001  # patched for namespace presence
    mock_get_db: MagicMock,
    mock_refresh: MagicMock,
) -> None:
    """A pull's refresh runs `match`, so it can reverse an accepted transfer.

    `gsheet pull` was the one embedded-refresh caller still reading only
    `error` off the RefreshResult, so a routine "✅ pulled N rows" could sit on
    top of a reversed user decision with nothing naming it or the way back.
    """
    from moneybin.orchestration.refresh import RefreshResult

    service = MagicMock()
    service.pull_connection.return_value = PullResult(
        connection_id="conn_abc123",
        status="complete",
        load_result=_make_load_result(),
    )
    mock_service_cls.return_value = service
    mock_oauth.return_value = MagicMock()
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_refresh.return_value = RefreshResult(
        applied=True, duration_seconds=0.05, transfers_retired=2
    )

    result = runner.invoke(app, ["gsheet", "pull", "conn_abc123"])
    assert result.exit_code == 0, result.output
    assert "Retired 2 previously accepted transfer(s)" in result.output
    assert "moneybin system audit undo" in result.output


@pytest.mark.unit
@patch("moneybin.orchestration.refresh.refresh")
@patch("moneybin.database.get_database")
@patch("moneybin.connectors.gsheet.sheets_api.SheetsClient")
@patch("moneybin.connectors.gsheet.pull_service.GSheetPullService")
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_pull_json_carries_transfers_retired(
    mock_oauth: MagicMock,
    mock_service_cls: MagicMock,
    mock_sheets_cls: MagicMock,  # noqa: ARG001
    mock_get_db: MagicMock,
    mock_refresh: MagicMock,
) -> None:
    """The agent parsing JSON is owed the same count the human is told."""
    from moneybin.orchestration.refresh import RefreshResult

    service = MagicMock()
    service.pull_connection.return_value = PullResult(
        connection_id="conn_abc123",
        status="complete",
        load_result=_make_load_result(),
    )
    mock_service_cls.return_value = service
    mock_oauth.return_value = MagicMock()
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_refresh.return_value = RefreshResult(
        applied=True, duration_seconds=0.05, transfers_retired=2
    )

    result = runner.invoke(app, ["gsheet", "pull", "conn_abc123", "--output", "json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["transfers_retired"] == 2


@pytest.mark.unit
@patch("moneybin.orchestration.refresh.refresh")
@patch("moneybin.database.get_database")
@patch("moneybin.connectors.gsheet.sheets_api.SheetsClient")
@patch("moneybin.connectors.gsheet.pull_service.GSheetPullService")
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_pull_reports_a_crashed_rates_step(
    mock_oauth: MagicMock,
    mock_service_cls: MagicMock,
    mock_sheets_cls: MagicMock,  # noqa: ARG001
    mock_get_db: MagicMock,
    mock_refresh: MagicMock,
) -> None:
    """A crashed rates step is the case `applied` cannot report.

    The pull names `rates` in its step list, so it reaches the network. SQLMesh
    still applies when the backfill crashes, so the command's own
    `applied`/`error` check stays silent and only this signal remains.
    """
    from moneybin.orchestration.refresh import RefreshResult

    service = MagicMock()
    service.pull_connection.return_value = PullResult(
        connection_id="conn_abc123",
        status="complete",
        load_result=_make_load_result(),
    )
    mock_service_cls.return_value = service
    mock_oauth.return_value = MagicMock()
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_refresh.return_value = RefreshResult(
        applied=True, duration_seconds=0.05, rate_backfill_error="provider timeout"
    )

    result = runner.invoke(app, ["gsheet", "pull", "conn_abc123"])
    assert result.exit_code == 0, result.output
    assert "Exchange rate backfill failed" in result.output


@pytest.mark.unit
@patch("moneybin.orchestration.refresh.refresh")
@patch("moneybin.database.get_database")
@patch("moneybin.connectors.gsheet.sheets_api.SheetsClient")
@patch("moneybin.connectors.gsheet.pull_service.GSheetPullService")
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_pull_names_an_unsupported_pair_and_its_remedy(
    mock_oauth: MagicMock,
    mock_service_cls: MagicMock,
    mock_sheets_cls: MagicMock,  # noqa: ARG001
    mock_get_db: MagicMock,
    mock_refresh: MagicMock,
) -> None:
    """A pair no provider publishes carries its own remedy, not a retry hint.

    Distinct from the crash above on purpose: this result has no
    ``rate_backfill_error``, so it isolates the per-pair lines rather than
    passing on the crash warning.
    """
    from moneybin.orchestration.refresh import RefreshResult
    from moneybin.services.rate_backfill import RateBackfillResult

    service = MagicMock()
    service.pull_connection.return_value = PullResult(
        connection_id="conn_abc123",
        status="complete",
        load_result=_make_load_result(),
    )
    mock_service_cls.return_value = service
    mock_oauth.return_value = MagicMock()
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_refresh.return_value = RefreshResult(
        applied=True,
        duration_seconds=0.05,
        rate_backfill=RateBackfillResult(
            rates_written=0,
            pairs_failed=(),
            pairs_unsupported=("EUR/XTS",),
        ),
    )

    result = runner.invoke(app, ["gsheet", "pull", "conn_abc123"])
    assert result.exit_code == 0, result.output
    assert "EUR/XTS" in result.output
    assert "moneybin fx set" in result.output


@pytest.mark.unit
@patch("moneybin.orchestration.refresh.refresh")
@patch("moneybin.database.get_database")
@patch("moneybin.connectors.gsheet.sheets_api.SheetsClient")
@patch("moneybin.connectors.gsheet.pull_service.GSheetPullService")
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_pull_json_carries_the_rate_backfill_outcome(
    mock_oauth: MagicMock,
    mock_service_cls: MagicMock,
    mock_sheets_cls: MagicMock,  # noqa: ARG001
    mock_get_db: MagicMock,
    mock_refresh: MagicMock,
) -> None:
    """The agent parsing JSON is owed the same outcome the human is told.

    Field names match ``refresh_envelope`` so a caller reading one surface does
    not have to learn a second spelling for the same outcome.
    """
    from moneybin.orchestration.refresh import RefreshResult
    from moneybin.services.rate_backfill import RateBackfillResult

    service = MagicMock()
    service.pull_connection.return_value = PullResult(
        connection_id="conn_abc123",
        status="complete",
        load_result=_make_load_result(),
    )
    mock_service_cls.return_value = service
    mock_oauth.return_value = MagicMock()
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_refresh.return_value = RefreshResult(
        applied=True,
        duration_seconds=0.05,
        rate_backfill=RateBackfillResult(
            rates_written=7,
            pairs_failed=("EUR/USD",),
            pairs_discarded=("GBP/USD",),
        ),
    )

    result = runner.invoke(app, ["gsheet", "pull", "conn_abc123", "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)["data"]
    assert payload["rates_written"] == 7
    assert payload["rate_pairs_failed"] == ["EUR/USD"]
    assert payload["rate_pairs_discarded"] == ["GBP/USD"]
    assert payload["rate_pairs_unsupported"] == []
    assert payload["rate_backfill_error"] is None


@pytest.mark.unit
@patch("moneybin.database.get_database")
@patch("moneybin.connectors.gsheet.sheets_api.SheetsClient")
@patch("moneybin.connectors.gsheet.pull_service.GSheetPullService")
@patch("moneybin.cli.commands.gsheet._build_oauth_client")
def test_gsheet_pull_json_says_null_when_the_rates_step_did_not_run(
    mock_oauth: MagicMock,
    mock_service_cls: MagicMock,
    mock_sheets_cls: MagicMock,  # noqa: ARG001
    mock_get_db: MagicMock,
) -> None:
    """``null`` and ``0`` are different answers, and only one of them is true here.

    ``rates_written`` is the sole did-it-run signal on this envelope — the three
    pair lists are empty whether the step ran clean or never ran at all. Under
    ``--no-refresh`` it did not run, so a ``0`` would tell a script that rate
    coverage was checked and found complete, and it would skip the refresh that
    is actually owed.
    """
    service = MagicMock()
    service.pull_connection.return_value = PullResult(
        connection_id="conn_abc123",
        status="complete",
        load_result=_make_load_result(),
    )
    mock_service_cls.return_value = service
    mock_oauth.return_value = MagicMock()
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    result = runner.invoke(
        app, ["gsheet", "pull", "conn_abc123", "--no-refresh", "--output", "json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)["data"]
    assert payload["rates_written"] is None
    assert payload["rate_backfill_error"] is None


# -------------------------------------------------------------------- list ---


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_list_empty_outputs_no_connections_message(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    service.list_connections.return_value = []
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["gsheet", "list"])
    assert result.exit_code == 0, result.output
    assert "No Google Sheets connections" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_list_text_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = [_make_connection()]
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["gsheet", "list"])
    assert result.exit_code == 0, result.output
    assert "conn_abc123" in result.stdout
    assert "My Budget" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_list_json_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = [_make_connection()]
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["gsheet", "list", "--output", "json"])
    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)["data"]["connections"]
    assert rows[0]["connection_id"] == "conn_abc123"


# ------------------------------------------------------------------ status ---


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_status_unknown_connection_exits_nonzero(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    service.get.return_value = None
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["gsheet", "status", "conn_missing"])
    assert result.exit_code == 1


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_status_unknown_connection_audits_as_gsheet_status(
    mock_build: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failure row names the same command and payload the success row does.

    `handle_cli_errors` defaults to `cli.unknown` at the conservative `high`
    tier with no classes; the success path below it records `cli.gsheet_status`
    off `GsheetConnectionsPayload`. Left unwired, one command wrote two
    different provenances into `privacy.log.jsonl` depending only on whether
    the id existed.
    """
    log_dir = tmp_path / "profile"
    log_dir.mkdir(mode=0o700)
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir",
        lambda: log_dir,
    )
    service = MagicMock()
    service.get.return_value = None
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(
        app, ["gsheet", "status", "conn_missing", "--output", "json"]
    )

    assert result.exit_code == 1
    expected = classify(GsheetConnectionsPayload)
    event = json.loads((log_dir / "privacy.log.jsonl").read_text().splitlines()[0])
    assert event["actor"] == "cli.gsheet_status"
    assert event["sensitivity"] == expected.sensitivity
    assert event["classes_returned"] == expected.classes_returned


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_status_single_connection(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.get.return_value = _make_connection(last_status_reason="header mismatch")
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["gsheet", "status", "conn_abc123"])
    assert result.exit_code == 0, result.output
    assert "conn_abc123" in result.stdout
    assert "header mismatch" in result.stdout


# --------------------------------------------------------------- reconnect ---


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_reconnect_command_invokes_service(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.reconnect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection(),
        initial_pull=_make_load_result(),
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["gsheet", "reconnect", "conn_abc123", "--yes"])
    assert result.exit_code == 0, result.output
    service.reconnect.assert_called_once_with(
        "conn_abc123", yes=True, sign=None, actor="cli"
    )
    assert "Reconnected" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_reconnect_forwards_sign_flag(mock_build: MagicMock) -> None:
    """--sign on reconnect threads through to the service call.

    Closes the unrecoverable wrong-default path the prior review flagged:
    a split→single mapping reconnect with positive_is_expense source data
    needs an explicit way to set the saved sign convention.
    """
    service = MagicMock()
    service.reconnect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection(),
        initial_pull=_make_load_result(),
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app,
        [
            "gsheet",
            "reconnect",
            "conn_abc123",
            "--yes",
            "--sign",
            "positive_is_expense",
        ],
    )
    # Typer rejects an enum value not in SignConventionType — assert one of the
    # accepted values reaches the service call.
    assert result.exit_code != 0, "positive_is_expense is not a valid choice"

    result = runner.invoke(
        app,
        [
            "gsheet",
            "reconnect",
            "conn_abc123",
            "--yes",
            "--sign",
            "negative_is_income",
        ],
    )
    assert result.exit_code == 0, result.output
    service.reconnect.assert_called_once_with(
        "conn_abc123", yes=True, sign="negative_is_income", actor="cli"
    )


# -------------------------------------------------------------- disconnect ---


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_disconnect_soft(mock_build: MagicMock) -> None:
    service = MagicMock()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["gsheet", "disconnect", "conn_abc123"])
    assert result.exit_code == 0, result.output
    service.disconnect.assert_called_once_with("conn_abc123", purge=False, actor="cli")
    assert "Disconnected" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet.sys")
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_disconnect_purge_requires_confirmation_or_yes(
    mock_build: MagicMock, mock_sys: MagicMock
) -> None:
    """In a TTY context, --purge without --yes prompts; declining aborts."""
    service = MagicMock()
    mock_build.return_value.__enter__.return_value = service
    mock_sys.stdin.isatty.return_value = True
    # Provide stdin "n\n" to decline the typer.confirm prompt.
    result = runner.invoke(
        app, ["gsheet", "disconnect", "conn_abc123", "--purge"], input="n\n"
    )
    assert result.exit_code == 0
    service.disconnect.assert_not_called()


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_disconnect_purge_with_yes_proceeds(mock_build: MagicMock) -> None:
    service = MagicMock()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app, ["gsheet", "disconnect", "conn_abc123", "--purge", "--yes"]
    )
    assert result.exit_code == 0, result.output
    service.disconnect.assert_called_once_with("conn_abc123", purge=True, actor="cli")
    assert "Purged" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet.sys")
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_disconnect_purge_non_tty_requires_yes(
    mock_build: MagicMock, mock_sys: MagicMock
) -> None:
    """In non-TTY (script/agent), --purge without --yes must fail loudly, not auto-confirm."""
    service = MagicMock()
    mock_build.return_value.__enter__.return_value = service
    mock_sys.stdin.isatty.return_value = False
    result = runner.invoke(app, ["gsheet", "disconnect", "conn_abc123", "--purge"])
    assert result.exit_code == 2
    service.disconnect.assert_not_called()
    assert "--yes" in result.stderr or "--yes" in result.output


def _make_detection_with_note() -> DetectionResult:
    detection = _make_detection()
    return replace(
        detection,
        notes=[
            "Duplicate header(s) renamed: Amount -> Amount_duplicated_0. "
            "Only the first column of each duplicated name is matched to a "
            "field; the renamed copies are not imported."
        ],
    )


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_connect_text_output_shows_detection_notes(
    mock_build: MagicMock,
) -> None:
    """Text is the default surface, so a note only in --output json is invisible."""
    service = MagicMock()
    service.connect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection_with_note(),
        initial_pull=_make_load_result(),
    )
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(
        app,
        [
            "gsheet",
            "connect",
            "https://docs.google.com/spreadsheets/d/ssid_xyz/edit#gid=0",
            "--account-name",
            "Checking",
        ],
    )

    assert result.exit_code == 0, result.output
    # cli.md routes diagnostics to stderr so stdout stays pure data: a warning
    # mixed into stdout is captured by a redirect as though it were output.
    assert "Amount_duplicated_0" in (result.stderr or "")
    assert "Amount_duplicated_0" not in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_reconnect_text_output_shows_detection_notes(
    mock_build: MagicMock,
) -> None:
    """Reconnect renames duplicates too, and its text output never said so."""
    service = MagicMock()
    service.reconnect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection_with_note(),
        initial_pull=_make_load_result(),
    )
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["gsheet", "reconnect", "conn_abc123"])

    assert result.exit_code == 0, result.output
    # cli.md routes diagnostics to stderr so stdout stays pure data: a warning
    # mixed into stdout is captured by a redirect as though it were output.
    assert "Amount_duplicated_0" in (result.stderr or "")
    assert "Amount_duplicated_0" not in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_reconnect_json_output_carries_detection_notes(
    mock_build: MagicMock,
) -> None:
    """The reconnect JSON payload omitted notes entirely; connect's carries them."""
    service = MagicMock()
    service.reconnect.return_value = ConnectResult(
        connection=_make_connection(),
        detection=_make_detection_with_note(),
        initial_pull=_make_load_result(),
    )
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(
        app, ["gsheet", "reconnect", "conn_abc123", "--output", "json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)["data"]
    notes = " ".join(payload["detection"]["detection_notes"])
    assert "Amount_duplicated_0" in notes

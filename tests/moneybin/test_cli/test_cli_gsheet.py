"""Tests for the `moneybin gsheet` CLI subgroup."""

from __future__ import annotations

import json
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
    payload = json.loads(result.stdout)
    assert payload["status"] == "authorized"


# ----------------------------------------------------------------- connect ---


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
    payload = json.loads(result.stdout)
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
@patch("moneybin.services.refresh.refresh")
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
    """Pull <connection_id> runs the refresh chain by default."""
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
        "steps": ["match", "transform", "categorize"]
    }


@pytest.mark.unit
@patch("moneybin.services.refresh.refresh")
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
@patch("moneybin.services.refresh.refresh")
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
    payload = json.loads(result.stdout)
    # Envelope shape: {"pulls": [...], "refresh_error": str | None}
    assert payload["refresh_error"] is None
    assert payload["pulls"][0]["connection_id"] == "conn_abc123"
    assert payload["pulls"][0]["status"] == "complete"
    assert payload["pulls"][0]["rows_inserted"] == 7


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
    payload = json.loads(result.stdout)
    assert payload[0]["connection_id"] == "conn_abc123"


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
    service.reconnect.assert_called_once_with("conn_abc123", yes=True)
    assert "Reconnected" in result.stdout


# -------------------------------------------------------------- disconnect ---


@pytest.mark.unit
@patch("moneybin.cli.commands.gsheet._build_connection_service")
def test_gsheet_disconnect_soft(mock_build: MagicMock) -> None:
    service = MagicMock()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["gsheet", "disconnect", "conn_abc123"])
    assert result.exit_code == 0, result.output
    service.disconnect.assert_called_once_with("conn_abc123", purge=False)
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
    service.disconnect.assert_called_once_with("conn_abc123", purge=True)
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

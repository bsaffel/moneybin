"""Tests for the `system status` CLI command."""

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
from moneybin.services.system_service import SystemStatus

runner = CliRunner()


def test_system_status_help() -> None:
    result = runner.invoke(app, ["system", "status", "--help"])
    assert result.exit_code == 0
    assert "--output" in result.output
    assert "--quiet" in result.output


@patch("moneybin.cli.commands.system.get_database")
def test_system_status_text_output(mock_get_db: MagicMock) -> None:
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    # SystemService runs 3 queries + 2 service-delegated queries; mock all to 0
    mock_db.execute.return_value.fetchone.return_value = (0, None, None)

    result = runner.invoke(app, ["system", "status"])
    assert result.exit_code == 0
    out = result.output.lower()
    assert "system status" in out
    assert "accounts" in out
    assert "transactions" in out
    assert "matches pending" in out
    assert "uncategorized" in out
    assert "local:exports" in out
    assert "ready" in out


def _fixed_status() -> SystemStatus:
    return SystemStatus(
        accounts_count=2,
        transactions_count=5,
        transactions_date_range=(date(2025, 1, 1), date(2025, 3, 31)),
        last_import_at=None,
        matches_pending=1,
        account_links_pending=0,
        merchant_links_pending=0,
        security_links_pending=0,
        categorize_pending=3,
        transforms_pending=False,
        transforms_last_apply_at=None,
        schema_drift={},
    )


@pytest.fixture
def _status_reads(monkeypatch: pytest.MonkeyPatch) -> MagicMock:  # pyright: ignore[reportUnusedFunction]  # fixture used by name
    """Stub the reads behind `system status --output json` with fixed values."""
    from moneybin.adapters import system_status_adapters
    from moneybin.privacy.payloads.system import (
        SystemStatusDatabaseConnectionsInfo,
        SystemStatusGsheetInfo,
    )

    def fixed_status(_self: object) -> SystemStatus:
        return _fixed_status()

    def no_gsheets(_db: object) -> SystemStatusGsheetInfo:
        return SystemStatusGsheetInfo(
            total_connections=0, by_status={}, needs_attention=[]
        )

    def no_connections(_path: object) -> SystemStatusDatabaseConnectionsInfo:
        return SystemStatusDatabaseConnectionsInfo(writers=[], readers=[])

    monkeypatch.setattr(
        "moneybin.services.system_service.SystemService.status", fixed_status
    )
    monkeypatch.setattr(system_status_adapters, "gsheet_info", no_gsheets)
    monkeypatch.setattr(
        system_status_adapters, "database_connections_info", no_connections
    )
    get_db = MagicMock()
    monkeypatch.setattr("moneybin.cli.commands.system.get_database", get_db)
    return get_db


@pytest.mark.usefixtures("_status_reads")
def test_system_status_json_output_is_the_mcp_sectioned_shape() -> None:
    """The CLI emits `system_status(sections=["overview", "exports"])`'s data."""
    result = runner.invoke(app, ["system", "status", "--output", "json"])
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.stdout)
    assert envelope["summary"]["sensitivity"] == "medium"
    assert envelope["summary"]["total_count"] == 2
    data = envelope["data"]
    assert data["kind"] == "sections"
    overview, exports = data["sections"]
    assert overview["kind"] == "overview"
    assert overview["overview"]["accounts"] == {"count": 2}
    assert overview["overview"]["transactions"]["date_range"] == [
        "2025-01-01",
        "2025-03-31",
    ]
    assert overview["overview"]["matches"] == {"pending_review": 1}
    assert overview["overview"]["categorization"] == {"uncategorized": 3}
    assert exports == {
        "kind": "exports",
        "destinations": [
            {
                "name": "local:exports",
                "kind": "local",
                "ready": True,
                "write_capable": True,
                "reasons": [],
            }
        ],
    }


@pytest.mark.usefixtures("_status_reads")
def test_system_status_json_uses_typed_privacy_and_redaction_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_event: dict[str, object] = {}
    redacted_payloads: list[object] = []

    monkeypatch.setattr(
        "moneybin.cli.output.write_privacy_event",
        captured_event.update,
    )

    def always_active(_payload_type: object) -> bool:
        return True

    monkeypatch.setattr(
        "moneybin.cli.output._has_active_transform",
        always_active,
    )

    def capture_redaction(payload: object, consent: object) -> object:
        redacted_payloads.append(payload)
        return payload

    monkeypatch.setattr("moneybin.cli.output.redact_typed", capture_redaction)

    result = runner.invoke(app, ["system", "status", "--output", "json"])

    assert result.exit_code == 0, result.output
    assert captured_event["sensitivity"] == "medium"
    assert "user_note" in captured_event["classes_returned"]  # type: ignore[operator]
    assert len(redacted_payloads) == 1
    assert not isinstance(redacted_payloads[0], dict)


@pytest.mark.usefixtures("_status_reads")
def test_system_status_text_and_json_share_export_readiness_reasons() -> None:
    from moneybin.exports.service import (
        ExportDestinationReadiness,
        ExportReadinessStatus,
    )

    readiness = ExportReadinessStatus(
        destinations=(
            ExportDestinationReadiness(
                name="dashboard",
                kind="sheets",
                ready=False,
                write_capable=False,
                reasons=(
                    "invalid_managed_tab_prefix",
                    "sheets_write_authorization_required",
                ),
            ),
        )
    )

    with patch(
        "moneybin.exports.service.ExportService.status",
        return_value=readiness,
    ):
        text_result = runner.invoke(app, ["system", "status"])
        json_result = runner.invoke(
            app,
            ["system", "status", "--output", "json"],
        )

    assert text_result.exit_code == 0
    assert "invalid_managed_tab_prefix" in text_result.stdout
    assert "sheets_write_authorization_required" in text_result.stdout
    exports = json.loads(json_result.stdout)["data"]["sections"][1]
    assert exports["destinations"][0]["reasons"] == [
        "invalid_managed_tab_prefix",
        "sheets_write_authorization_required",
    ]


@patch("moneybin.cli.commands.system.get_database")
def test_system_status_pages_all_configured_destinations_and_no_pager_keeps_them(
    mock_get_db: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Destination status is unbounded configuration, so it stays one pageable answer."""
    from moneybin.exports.service import (
        ExportDestinationReadiness,
        ExportReadinessStatus,
    )

    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_db.execute.return_value.fetchone.return_value = (0, None, None)
    readiness = ExportReadinessStatus(
        destinations=tuple(
            ExportDestinationReadiness(
                name=f"destination-{index}",
                kind="local",
                ready=True,
                write_capable=True,
                reasons=(),
            )
            for index in range(8)
        )
    )
    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=1,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.system.get_terminal_policy",
        lambda *, no_pager=False: policy,
    )
    pages: list[str] = []

    def capture_page(text: str, **_kwargs: object) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr("moneybin.cli.pager.page_text", capture_page)
    with patch("moneybin.exports.service.ExportService.status", return_value=readiness):
        paged = runner.invoke(app, ["system", "status"])
        direct = runner.invoke(app, ["system", "status", "--no-pager"])

    assert paged.exit_code == direct.exit_code == 0
    assert pages and "destination-7" in pages[0]
    assert "destination-7" in direct.stdout

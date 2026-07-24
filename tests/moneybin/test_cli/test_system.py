"""Tests for the `system status` CLI command."""

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app

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
    assert "accounts" in out
    assert "transactions" in out
    assert "matches pending" in out
    assert "uncategorized" in out
    assert "local:exports" in out
    assert "ready" in out


@patch("moneybin.cli.commands.system.get_database")
def test_system_status_json_output(mock_get_db: MagicMock) -> None:
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_db.execute.return_value.fetchone.return_value = (0, None, None)

    result = runner.invoke(app, ["system", "status", "--output", "json"])
    assert result.exit_code == 0
    envelope = json.loads(result.stdout)
    assert envelope["summary"]["sensitivity"] == "medium"
    payload = envelope["data"]
    assert "accounts_count" in payload
    assert "transactions_count" in payload
    assert "matches_pending" in payload
    assert "categorize_pending" in payload
    assert payload["exports"] == [
        {
            "name": "local:exports",
            "kind": "local",
            "ready": True,
            "write_capable": True,
            "reasons": [],
        }
    ]


@patch("moneybin.cli.commands.system.get_database")
def test_system_status_json_uses_typed_privacy_and_redaction_path(
    mock_get_db: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_db.execute.return_value.fetchone.return_value = (0, None, None)
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

    def capture_redaction(payload: object, consent: object) -> object:  # noqa: ARG001
        redacted_payloads.append(payload)
        return payload

    monkeypatch.setattr("moneybin.cli.output.redact_typed", capture_redaction)

    result = runner.invoke(app, ["system", "status", "--output", "json"])

    assert result.exit_code == 0, result.output
    assert captured_event["sensitivity"] == "medium"
    assert "user_note" in captured_event["classes_returned"]  # type: ignore[operator]
    assert len(redacted_payloads) == 1
    assert not isinstance(redacted_payloads[0], dict)


@patch("moneybin.cli.commands.system.get_database")
def test_system_status_text_and_json_share_export_readiness_reasons(
    mock_get_db: MagicMock,
) -> None:
    from moneybin.exports.service import (
        ExportDestinationReadiness,
        ExportReadinessStatus,
    )

    mock_db = MagicMock()
    mock_get_db.return_value.__enter__.return_value = mock_db
    mock_db.execute.return_value.fetchone.return_value = (0, None, None)
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
    payload = json.loads(json_result.stdout)["data"]
    assert payload["exports"][0]["reasons"] == [
        "invalid_managed_tab_prefix",
        "sheets_write_authorization_required",
    ]

"""Outcome receipts for the human ``moneybin sync pull`` surface."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.connectors.sync_models import InstitutionResult, PullResult

runner = CliRunner()


def _normalize_prose(text: str) -> str:
    """Compare wrapped receipt prose without weakening numeric or identity assertions."""
    return " ".join(text.split())


def _pull_result(*, partial: bool = False) -> PullResult:
    """A completed pull, optionally with one failed institution."""
    institutions = [
        InstitutionResult(
            provider_item_id="item_example_bank",
            institution_name="Example Bank",
            status="completed",
            transaction_count=28,
        )
    ]
    if partial:
        institutions.append(
            InstitutionResult(
                provider_item_id="item_example_investing",
                institution_name="Example Investing",
                status="failed",
                error="connection timed out",
                error_code="TIMEOUT",
            )
        )
    return PullResult(
        job_id="job-1",
        transactions_loaded=28,
        accounts_loaded=1,
        balances_loaded=1,
        transactions_removed=2,
        institutions=institutions,
        transforms_applied=True,
    )


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_receipt_keeps_requested_result_when_quiet(mock_build: MagicMock) -> None:
    """Removing the receipt from quiet output would hide saved sync work."""
    service = MagicMock()
    service.pull.return_value = _pull_result()
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "pull", "--quiet"])

    assert result.exit_code == 0, result.output
    assert "Sync complete" in result.stdout
    assert "28 transactions loaded from 1 institution" in result.stdout
    assert "Example Bank" in result.stdout
    assert "2 stale transactions" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_partial_sync_receipt_never_claims_overall_success(
    mock_build: MagicMock,
) -> None:
    """Treating a failed requested institution as complete hides stale data."""
    service = MagicMock()
    service.pull.return_value = _pull_result(partial=True)
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "pull"])

    assert result.exit_code != 0
    assert "Sync partially completed" in result.stdout
    assert "Sync complete" not in result.stdout
    assert "28 transactions loaded" in result.stdout
    assert "Example Investing" in result.stdout
    assert "connection timed out" in result.stdout
    assert (
        "Example Investing was not refreshed; previously available data may be stale"
        in _normalize_prose(result.stdout)
    )
    assert "Loaded transactions were saved" in result.stdout
    assert result.stderr == ""


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_transform_failure_receipt_names_saved_raw_data_and_recovery(
    mock_build: MagicMock,
) -> None:
    """Calling a failed refresh complete would hide stale derived tables."""
    service = MagicMock()
    pull = _pull_result()
    pull.transforms_applied = False
    pull.transforms_error = "SQLMesh apply failed"
    service.pull.return_value = pull
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "pull"])

    assert result.exit_code == 1
    assert "Sync partially completed" in result.stdout
    assert "Loaded transactions were saved" in result.stdout
    assert (
        "Core tables and reports may still reflect data before this pull"
        in result.stdout
    )
    assert "moneybin transform apply" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_security_identity_failure_receipt_names_incomplete_cost_basis(
    mock_build: MagicMock,
) -> None:
    """A failed identity pass must say which investment result is incomplete."""
    service = MagicMock()
    pull = _pull_result()
    pull.security_resolution_error = "resolver unavailable"
    service.pull.return_value = pull
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "pull"])

    assert result.exit_code == 1
    assert (
        "Investment transactions from this pull are not attributed to securities"
        in _normalize_prose(result.stdout)
    )
    assert "cost basis may be incomplete" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_refresh_stage_failure_is_partial_and_nonzero(mock_build: MagicMock) -> None:
    """A failed requested refresh stage must not leave a successful process status."""
    from moneybin.services.refresh_outcome import RefreshStepOutcome, StageOutcome

    service = MagicMock()
    pull = _pull_result()
    pull.refresh_steps = RefreshStepOutcome(
        stages=(StageOutcome(step="match", ran=True, error="matcher unavailable"),)
    )
    service.pull.return_value = pull
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "pull"])

    assert result.exit_code == 1
    assert "Sync partially completed" in result.stdout
    assert "Post-load refresh" in result.stdout
    assert (
        "Derived matching, categorization, identity, or exchange-rate results may be incomplete"
        in _normalize_prose(result.stdout)
    )
    assert "Matching step failed: matcher unavailable" in result.stderr


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_review_candidates_do_not_turn_successful_sync_into_partial(
    mock_build: MagicMock,
) -> None:
    """Review work is attention after completed work, not a failed sync."""
    service = MagicMock()
    pull = _pull_result()
    pull.security_resolution = {"proposed": 2, "pending": 1}
    service.pull.return_value = pull
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "pull"])

    assert result.exit_code == 0, result.output
    assert "Sync complete" in result.stdout
    assert "3 securities awaiting identity review" in result.stdout
    assert "moneybin investments securities links pending" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_interrupted_sync_receipt_leaves_saved_scope_unknown(
    mock_build: MagicMock,
) -> None:
    """Claiming no writes after Ctrl+C would be an unsupported rollback claim."""
    service = MagicMock()
    service.pull.side_effect = KeyboardInterrupt
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "pull"])

    assert result.exit_code != 0
    assert "Sync cancelled" in result.stdout
    assert "saved scope is unknown" in result.stdout.lower()
    assert "moneybin sync status" in result.stdout
    assert result.stderr == ""


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_interrupted_json_sync_emits_structured_unknown_saved_scope(
    mock_build: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dropping JSON on Ctrl+C would leave agents unable to classify the outcome."""
    service = MagicMock()
    service.pull.side_effect = KeyboardInterrupt
    mock_build.return_value.__enter__.return_value = service
    log_dir = tmp_path / "privacy"
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir", lambda: log_dir
    )

    result = runner.invoke(app, ["sync", "pull", "--output", "json"])

    assert result.exit_code == 130
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "sync_error"
    assert payload["error"]["details"]["saved_scope"] == "unknown"
    assert result.stderr == ""
    events = [
        json.loads(line)
        for line in (log_dir / "privacy.log.jsonl").read_text().splitlines()
    ]
    assert len(events) == 1
    event = events[0]
    assert event["actor"] == "cli.sync_pull"
    assert event["action"] == "tool_call"
    assert event["sensitivity"] == "high"
    assert event["classes_returned"] == []
    assert event["row_count"] == 0

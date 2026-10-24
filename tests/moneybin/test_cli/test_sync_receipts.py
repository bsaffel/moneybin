"""Outcome receipts for the human ``moneybin sync pull`` surface."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.connectors.sync_models import InstitutionResult, PullResult
from moneybin.logging.config import setup_logging

runner = CliRunner()


def _sync_app_with_real_logging(*, verbose: bool, log_path: Path | None) -> typer.Typer:
    """Invoke the real ``sync pull`` command beneath real CLI handlers."""
    from moneybin.cli.commands import sync

    wrapper = typer.Typer(no_args_is_help=False)

    @wrapper.callback()
    def configure_logging() -> None:
        setup_logging(
            stream="cli",
            verbose=verbose,
            log_to_file=log_path is not None,
            log_file_path=log_path,
        )

    _ = configure_logging
    wrapper.add_typer(sync.app, name="sync")
    return wrapper


def _pull_with_diagnostics(*_args: object, **_kwargs: object) -> PullResult:
    """Synthetic upstream seam that retains representative service records."""
    logging.getLogger("moneybin.services.synthetic_sync").info("sync diagnostic")
    logging.getLogger("moneybin.services.synthetic_refresh").info("refresh diagnostic")
    logging.getLogger("moneybin.services.synthetic_refresh").warning("refresh warning")
    return _pull_result()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_real_sync_command_normal_and_quiet_keep_receipt_warning_without_info_wall(
    mock_build: MagicMock,
) -> None:
    """The command path, handler setup, and synthetic service seam stay coherent."""
    service = MagicMock()
    service.pull.side_effect = _pull_with_diagnostics
    mock_build.return_value.__enter__.return_value = service

    for args in ([], ["--quiet"]):
        result = runner.invoke(
            _sync_app_with_real_logging(verbose=False, log_path=None),
            ["sync", "pull", *args],
        )
        assert result.exit_code == 0, result.output
        assert "Sync complete" in result.stdout
        assert "refresh warning" in result.stderr
        assert "sync diagnostic" not in result.stderr
        assert "refresh diagnostic" not in result.stderr


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_real_sync_command_verbose_and_file_keep_diagnostics_and_json_clean(
    mock_build: MagicMock, tmp_path: Path
) -> None:
    """Verbose exposes diagnostics; JSON stdout remains one structured document."""
    service = MagicMock()
    service.pull.side_effect = _pull_with_diagnostics
    mock_build.return_value.__enter__.return_value = service
    path = tmp_path / "moneybin.log"

    verbose = runner.invoke(
        _sync_app_with_real_logging(verbose=True, log_path=path), ["sync", "pull"]
    )
    assert verbose.exit_code == 0, verbose.output
    assert "sync diagnostic" in verbose.stderr
    assert "refresh diagnostic" in verbose.stderr
    log_path = next(tmp_path.glob("cli_*.log"))
    log_text = log_path.read_text()
    assert "sync diagnostic" in log_text
    assert "refresh diagnostic" in log_text

    service.pull.side_effect = _pull_with_diagnostics
    json_result = runner.invoke(
        _sync_app_with_real_logging(verbose=False, log_path=path),
        ["sync", "pull", "--output", "json"],
    )
    assert json_result.exit_code == 0, json_result.output
    assert json.loads(json_result.stdout)["status"] == "ok"
    assert "sync diagnostic" not in json_result.stderr


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
@pytest.mark.parametrize(
    ("partial", "expected_title"),
    [(False, "OK Sync complete"), (True, "! Sync partially completed")],
)
def test_sync_pull_receipt_uses_terminal_symbols_for_ascii_titles(
    partial: bool,
    expected_title: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Changing the title glyph must follow the terminal policy, not literals."""
    from moneybin.cli.commands.sync import (
        _render_sync_pull_receipt,  # pyright: ignore[reportPrivateUsage]  # pins the shared receipt title policy
    )
    from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

    terminal = TerminalPolicy(
        output="text",
        interactive=False,
        page=False,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=24,
        symbols=TerminalSymbols(success="OK", attention="!", failure="X", action=">"),
        minus="-",
    )

    _render_sync_pull_receipt(_pull_result(partial=partial), terminal=terminal)

    assert capsys.readouterr().out.splitlines()[0] == expected_title


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

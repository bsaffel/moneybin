"""Tests for CLI `moneybin sync login`, `moneybin sync logout`, `moneybin sync pull`, and link."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.output import OutputFormat
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
from moneybin.connectors.sync_models import (
    InstitutionResult,
    LinkResult,
    PullResult,
    SyncConnectionView,
)
from moneybin.services.refresh_outcome import RefreshStepOutcome

runner = CliRunner()


def _pager_policy(*, no_pager: bool = False, ascii: bool = False) -> TerminalPolicy:
    """Make a terminal answer pager-eligible unless the command opts out."""
    return TerminalPolicy(
        output="text",
        interactive=True,
        page=not no_pager,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=ascii,
        width=80,
        height=1,
        symbols=TerminalSymbols(
            "OK" if ascii else "✓", "!", "X" if ascii else "×", ">" if ascii else "›"
        ),
        minus="-" if ascii else "−",
    )


def _fake_pull_result(
    *,
    transforms_applied: bool = True,
    transforms_error: str | None = None,
    securities_loaded: int = 0,
    investment_transactions_loaded: int = 0,
    holdings_loaded: int = 0,
    opening_bootstrap_rows: int = 0,
    investment_source_overlap_accounts: list[str] | None = None,
    security_resolution: dict[str, int] | None = None,
    security_resolution_error: str | None = None,
    transfers_retired: int = 0,
    refresh_steps: RefreshStepOutcome | None = None,
) -> PullResult:
    return PullResult(
        job_id="job-xyz",
        transactions_loaded=10,
        accounts_loaded=2,
        balances_loaded=2,
        transactions_removed=0,
        securities_loaded=securities_loaded,
        investment_transactions_loaded=investment_transactions_loaded,
        holdings_loaded=holdings_loaded,
        institutions=[
            InstitutionResult(
                provider_item_id="item_chase",
                institution_name="Chase",
                status="completed",
                transaction_count=10,
            )
        ],
        transforms_applied=transforms_applied,
        transforms_duration_seconds=0.05 if transforms_applied else None,
        transforms_error=transforms_error,
        opening_bootstrap_rows=opening_bootstrap_rows,
        investment_source_overlap_accounts=investment_source_overlap_accounts or [],
        security_resolution=security_resolution or {},
        security_resolution_error=security_resolution_error,
        transfers_retired=transfers_retired,
        refresh_steps=refresh_steps,
    )


def _fake_failed_pull_result() -> PullResult:
    """Return a pull result whose institution failed to refresh."""
    result = _fake_pull_result()
    result.institutions = [
        InstitutionResult(
            provider_item_id="item_chase",
            institution_name="Chase",
            status="failed",
            transaction_count=0,
            error="provider unavailable",
        )
    ]
    return result


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_client")
def test_sync_login_invokes_client_login(mock_build: MagicMock) -> None:
    mock_client = MagicMock()
    mock_build.return_value = mock_client
    result = runner.invoke(app, ["sync", "login", "--no-browser"])
    assert result.exit_code == 0, result.output
    mock_client.login.assert_called_once_with(open_browser=False)
    assert "Logged in" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_client")
def test_sync_login_default_opens_browser(mock_build: MagicMock) -> None:
    mock_client = MagicMock()
    mock_build.return_value = mock_client
    result = runner.invoke(app, ["sync", "login"])
    assert result.exit_code == 0, result.output
    mock_client.login.assert_called_once_with(open_browser=True)


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_client")
def test_sync_logout_clears_tokens(mock_build: MagicMock) -> None:
    mock_client = MagicMock()
    mock_build.return_value = mock_client
    result = runner.invoke(app, ["sync", "logout"])
    assert result.exit_code == 0, result.output
    mock_client.logout.assert_called_once()
    assert "Logged out" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_text_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.pull.return_value = _fake_pull_result()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull"])
    assert result.exit_code == 0, result.output
    assert "Chase" in result.stdout
    assert "10" in result.stdout


@pytest.mark.unit
@pytest.mark.parametrize(
    ("result_factory", "action", "exit_code"),
    [
        (_fake_failed_pull_result, "moneybin sync status", 1),
        (
            lambda: _fake_pull_result(transforms_error="SQLMeshError"),
            "moneybin transform apply",
            1,
        ),
        (
            lambda: _fake_pull_result(security_resolution_error="resolver unavailable"),
            "moneybin sync pull",
            1,
        ),
        (
            lambda: _fake_pull_result(security_resolution={"pending": 1}),
            "moneybin investments securities links pending",
            0,
        ),
        (
            lambda: _fake_pull_result(investment_source_overlap_accounts=["account-1"]),
            "moneybin doctor",
            0,
        ),
    ],
    ids=(
        "failed-institution",
        "transform",
        "security-resolution",
        "identity",
        "overlap",
    ),
)
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_receipt_actions_use_ascii_terminal_symbol(
    mock_build: MagicMock,
    result_factory: Callable[[], PullResult],
    action: str,
    exit_code: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every text recovery action follows the active terminal symbol policy."""
    service = MagicMock()
    service.pull.return_value = result_factory()
    mock_build.return_value.__enter__.return_value = service
    monkeypatch.setattr(
        "moneybin.cli.commands.sync.get_terminal_policy",
        lambda: _pager_policy(ascii=True),
    )

    result = runner.invoke(app, ["sync", "pull"])

    assert result.exit_code == exit_code, result.output
    assert f"> {action}" in result.stdout
    assert "›" not in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_cancellation_uses_ascii_terminal_symbol(
    mock_build: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelled pulls keep their recovery action legible in ASCII terminals."""
    service = MagicMock()
    service.pull.side_effect = KeyboardInterrupt
    mock_build.return_value.__enter__.return_value = service
    monkeypatch.setattr(
        "moneybin.cli.commands.sync.get_terminal_policy",
        lambda: _pager_policy(ascii=True),
    )

    result = runner.invoke(app, ["sync", "pull"])

    assert result.exit_code == 130, result.output
    assert "> moneybin sync status" in result.stdout
    assert "›" not in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_json_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.pull.return_value = _fake_pull_result()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull", "--output", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert data["job_id"] == "job-xyz"
    assert data["transactions_loaded"] == 10
    assert data["institutions"][0]["institution_name"] == "Chase"


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_warns_about_a_retired_transfer_in_both_output_modes(
    mock_build: MagicMock,
) -> None:
    """The reversal is said aloud whichever output mode the caller picked.

    A pull runs the full refresh, so its match step can reverse a transfer the
    user accepted. The count reaches the JSON body either way, but the count
    alone does not name `system audit undo` — the warning is what carries the
    way back, and an agent driving `--output json` is exactly the caller least
    able to notice a reversal on its own. Gating it on text mode drops the
    recovery route from the surface, matching `gsheet pull`, which places the
    same call ahead of both branches for this reason.
    """
    for mode in (["sync", "pull"], ["sync", "pull", "--output", "json"]):
        service = MagicMock()
        service.pull.return_value = _fake_pull_result(transfers_retired=2)
        mock_build.return_value.__enter__.return_value = service
        result = runner.invoke(app, mode)
        assert result.exit_code == 0, result.output
        assert "Retired 2 previously accepted transfer(s)" in result.stderr, mode
        assert "moneybin system audit undo" in result.stderr, mode


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_with_institution_and_force(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.pull.return_value = _fake_pull_result()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull", "--institution", "Chase", "--force"])
    assert result.exit_code == 0, result.output
    service.pull.assert_called_once()
    assert service.pull.call_args.kwargs == {
        "institution": "Chase",
        "force": True,
        "refresh": True,
        "progress": service.pull.call_args.kwargs["progress"],
    }


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_no_refresh_flag(mock_build: MagicMock) -> None:
    """--no-refresh threads refresh=False to the service."""
    service = MagicMock()
    service.pull.return_value = _fake_pull_result()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull", "--no-refresh"])
    assert result.exit_code == 0, result.output
    service.pull.assert_called_once()
    assert service.pull.call_args.kwargs == {
        "institution": None,
        "force": False,
        "refresh": False,
        "progress": service.pull.call_args.kwargs["progress"],
    }


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_text_output_no_investments_stays_quiet(
    mock_build: MagicMock,
) -> None:
    """A pull with zero investment data prints no Investments/Securities lines."""
    service = MagicMock()
    service.pull.return_value = _fake_pull_result()
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull"])
    assert result.exit_code == 0, result.output
    assert "Investments:" not in result.stdout
    assert "Securities:" not in result.stdout
    assert "opening lot" not in result.stdout
    assert "manual and Plaid" not in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_text_output_shows_clean_investment_resolution(
    mock_build: MagicMock,
) -> None:
    """All-clean resolution (no proposed/pending) lists outcomes without a review nag."""
    service = MagicMock()
    service.pull.return_value = _fake_pull_result(
        securities_loaded=3,
        investment_transactions_loaded=4,
        holdings_loaded=3,
        security_resolution={"adopted": 1, "auto_bound": 1, "minted": 1},
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull"])
    assert result.exit_code == 0, result.output
    assert "3 new securities" in result.stdout
    assert "4 investment transactions" in result.stdout
    assert "3 holdings snapshots" in result.stdout
    assert "awaiting" not in result.stdout
    assert "Review:" not in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_text_output_names_review_command_when_awaiting(
    mock_build: MagicMock,
) -> None:
    """Any identity awaiting review must name the exact review command."""
    service = MagicMock()
    service.pull.return_value = _fake_pull_result(
        securities_loaded=3,
        investment_transactions_loaded=1,
        holdings_loaded=1,
        security_resolution={"proposed": 2, "pending": 1},
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull"])
    assert result.exit_code == 0, result.output
    assert "3 securities awaiting identity review" in result.stdout
    assert "moneybin investments securities links pending" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_text_output_shows_bootstrap_and_overlap(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    service.pull.return_value = _fake_pull_result(
        securities_loaded=1,
        investment_transactions_loaded=1,
        holdings_loaded=1,
        opening_bootstrap_rows=2,
        investment_source_overlap_accounts=["acc_1"],
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull"])
    assert result.exit_code == 0, result.output
    assert "2 cumulative lots seeded for pre-window positions" in result.stdout
    assert "1 accounts have both manual and Plaid history" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_surfaces_transforms_error(mock_build: MagicMock) -> None:
    """Transform failure on pull warns via logger and exits non-zero.

    Mirrors import_cmd.py: agents and scripts need an exit-code signal that
    core.* tables are stale, not just text on stdout. The warning text
    itself is not asserted here — ``setup_observability`` reconfigures
    logging via ``basicConfig(force=True)`` inside the Typer ``CliRunner``
    invocation, which wipes ``caplog``'s root handler.
    """
    service = MagicMock()
    service.pull.return_value = _fake_pull_result(
        transforms_applied=False,
        transforms_error="SQLMeshError",
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull"])
    assert result.exit_code == 1
    service.pull.assert_called_once()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_json_mode_exits_nonzero_on_transforms_error(
    mock_build: MagicMock,
) -> None:
    """JSON-mode `sync pull` must also exit non-zero when refresh failed.

    Agents that gate on process status (e.g., Claude Code, Codex) parse
    the JSON envelope on stdout but trust the exit code for branching.
    Without this, a SQLMesh failure during the post-pull refresh would
    return exit 0 with ``transforms_applied=false`` in the payload and
    automation would treat stale ``core.*`` state as success.
    """
    service = MagicMock()
    service.pull.return_value = _fake_pull_result(
        transforms_applied=False,
        transforms_error="SQLMeshError",
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull", "--output", "json"])
    assert result.exit_code == 1
    # JSON payload must still be emitted on stdout so agents can read the
    # structured error before observing the non-zero exit.
    payload = json.loads(result.stdout)["data"]
    assert payload["transforms_applied"] is False
    assert payload["transforms_error"] == "SQLMeshError"


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_surfaces_security_resolution_error(mock_build: MagicMock) -> None:
    """Security resolution failure on pull warns via logger and exits non-zero.

    Mirrors ``test_sync_pull_surfaces_transforms_error``: there is no
    staging fallback for an unresolved security_id (unlike accounts), so a
    swallowed resolution failure is exactly as silent a cost-basis
    corruption as a swallowed transform failure and must carry the same
    exit-code signal.
    """
    service = MagicMock()
    service.pull.return_value = _fake_pull_result(
        securities_loaded=3,
        security_resolution_error="boom",
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull"])
    assert result.exit_code == 1
    service.pull.assert_called_once()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_json_mode_exits_nonzero_on_security_resolution_error(
    mock_build: MagicMock,
) -> None:
    """JSON-mode `sync pull` must also exit non-zero on security resolution failure."""
    service = MagicMock()
    service.pull.return_value = _fake_pull_result(
        securities_loaded=3,
        security_resolution_error="boom",
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull", "--output", "json"])
    assert result.exit_code == 1
    payload = json.loads(result.stdout)["data"]
    assert payload["security_resolution_error"] == "boom"


@pytest.mark.unit
def test_sync_link_command_exists() -> None:
    """`moneybin sync link` is the new canonical command (renamed from sync connect)."""
    result = runner.invoke(app, ["sync", "link", "--help"])
    assert result.exit_code == 0
    assert "link" in result.stdout.lower()


@pytest.mark.unit
def test_sync_link_status_command_exists() -> None:
    """`moneybin sync link-status` is the new canonical status command."""
    result = runner.invoke(app, ["sync", "link-status", "--help"])
    assert result.exit_code == 0
    assert "link" in result.stdout.lower()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_new_institution(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = []
    service.link.return_value = LinkResult(
        provider_item_id="item_new",
        institution_name="Chase",
        pull_result=_fake_pull_result(),
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "link"])
    assert result.exit_code == 0, result.output
    service.link.assert_called_once()
    # auto_pull defaults to True
    assert service.link.call_args.kwargs.get("auto_pull", True) is True
    assert "Link complete" in result.stdout
    assert "Chase" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_surfaces_transforms_error_from_auto_pull(
    mock_build: MagicMock,
) -> None:
    """Link's auto-pull transform failure must warn and exit non-zero.

    Without this, the ✅ Connected line would hide stale core.* tables. See
    ``test_sync_pull_surfaces_transforms_error`` for why the warning text
    itself is not asserted.
    """
    service = MagicMock()
    service.list_connections.return_value = []
    service.link.return_value = LinkResult(
        provider_item_id="item_new",
        institution_name="Chase",
        pull_result=_fake_pull_result(
            transforms_applied=False,
            transforms_error="SQLMeshError",
        ),
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "link"])
    assert result.exit_code == 1
    service.link.assert_called_once()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_surfaces_security_resolution_error_from_auto_pull(
    mock_build: MagicMock,
) -> None:
    """Link's auto-pull security resolution failure must warn and exit non-zero.

    Mirrors ``test_sync_link_surfaces_transforms_error_from_auto_pull``: the
    same fail-loud contract applies to a swallowed resolution failure as to a
    swallowed transforms failure.
    """
    service = MagicMock()
    service.list_connections.return_value = []
    service.link.return_value = LinkResult(
        provider_item_id="item_new",
        institution_name="Chase",
        pull_result=_fake_pull_result(
            securities_loaded=3,
            security_resolution_error="boom",
        ),
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "link"])
    assert result.exit_code == 1
    service.link.assert_called_once()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_no_pull(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = []
    service.link.return_value = LinkResult(
        provider_item_id="item_new",
        institution_name="Chase",
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "link", "--no-pull"])
    assert result.exit_code == 0, result.output
    service.link.assert_called_once()
    assert service.link.call_args.kwargs["auto_pull"] is False


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_auto_pull_exception_receipt_exits_nonzero(
    mock_build: MagicMock,
) -> None:
    """A connected institution with a failed default follow-up is incomplete."""
    service = MagicMock()
    service.list_connections.return_value = []
    service.link.return_value = LinkResult(
        provider_item_id="item_new",
        institution_name="Chase",
    )
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "link"])

    assert result.exit_code == 1, result.output
    assert "Link partially completed" in result.stdout
    assert "Connected; auto-pull failed" in result.stdout
    assert "moneybin sync pull" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_busy_database_reports_recovery(mock_build: MagicMock) -> None:
    """A writer lock is a classified, actionable sync-pull failure."""
    from moneybin.database import DatabaseLockError

    service = MagicMock()
    service.pull.side_effect = DatabaseLockError("database busy")
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "pull"])

    assert result.exit_code == 1, result.output
    assert "database busy" in result.output
    assert "moneybin db ps" in result.output


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_busy_database_reports_recovery(mock_build: MagicMock) -> None:
    """A writer lock before linking is classified instead of leaking a traceback."""
    from moneybin.database import DatabaseLockError

    service = MagicMock()
    service.list_connections.side_effect = DatabaseLockError("database busy")
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "link"])

    assert result.exit_code == 1, result.output
    assert "database busy" in result.output
    assert "moneybin db ps" in result.output


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_explicit_institution(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.link.return_value = LinkResult(
        provider_item_id="item_x",
        institution_name="Schwab",
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app, ["sync", "link", "--institution", "Schwab", "--no-pull"]
    )
    assert result.exit_code == 0, result.output
    service.link.assert_called_once()
    assert service.link.call_args.kwargs["institution"] == "Schwab"


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
@patch("moneybin.cli.commands.sync.typer.confirm", return_value=False)
def test_sync_link_declined_reauth_performs_no_link(
    mock_confirm: MagicMock,
    mock_build: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Declining the explicit re-authentication prompt leaves the service untouched."""
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="error",
            last_sync=None,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            guidance=None,
        )
    ]
    mock_build.return_value.__enter__.return_value = service

    monkeypatch.setattr("moneybin.cli.utils.sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stdout.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stderr.isatty", lambda: True)
    from moneybin.cli.commands.sync import sync_link

    with pytest.raises(typer.Exit) as exit_info:
        sync_link(
            institution=None,
            no_pull=False,
            no_browser=False,
            yes=False,
            output=OutputFormat.TEXT,
        )

    assert exit_info.value.exit_code == 0
    mock_confirm.assert_called_once()
    service.link.assert_not_called()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
@patch("moneybin.cli.commands.sync.typer.confirm")
def test_sync_link_json_tty_refuses_ambiguous_reauth_without_prompt(
    mock_confirm: MagicMock,
    mock_build: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """JSON cannot use a TTY prompt to decide which errored connection to re-auth."""
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="error",
            last_sync=None,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            guidance=None,
        )
    ]
    mock_build.return_value.__enter__.return_value = service
    monkeypatch.setattr("moneybin.cli.utils.sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stdout.isatty", lambda: True)
    from moneybin.cli.commands.sync import sync_link

    with pytest.raises(typer.Exit) as exit_info:
        sync_link(
            institution=None,
            no_pull=False,
            no_browser=False,
            yes=False,
            output=OutputFormat.JSON,
        )

    assert exit_info.value.exit_code == 2
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["error"]["code"] == "mutation_confirmation_required"
    assert "--institution" in payload["error"]["hint"]
    assert captured.err == ""
    mock_confirm.assert_not_called()
    service.initiate_link.assert_not_called()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_json_refuses_multiple_reauth_choices_without_initiating(
    mock_build: MagicMock,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Multiple errored connections require an explicit JSON-safe target choice."""
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="error",
            last_sync=None,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            guidance=None,
        ),
        SyncConnectionView(
            id="u2",
            provider_item_id="item_b",
            institution_name="Schwab",
            provider="plaid",
            status="error",
            last_sync=None,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            guidance=None,
        ),
    ]
    mock_build.return_value.__enter__.return_value = service
    from moneybin.cli.commands.sync import sync_link

    with pytest.raises(typer.Exit) as exit_info:
        sync_link(
            institution=None,
            no_pull=False,
            no_browser=False,
            yes=False,
            output=OutputFormat.JSON,
        )

    assert exit_info.value.exit_code == 2
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["error"]["code"] == "mutation_confirmation_required"
    assert "--institution NAME with --yes" in payload["error"]["hint"]
    assert captured.err == ""
    service.initiate_link.assert_not_called()
    service.link.assert_not_called()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_status_command(mock_build: MagicMock) -> None:
    from moneybin.connectors.sync_client import SyncClient

    client = MagicMock(spec=SyncClient)
    # CLI link-status is single-shot via the public get_link_status,
    # not the blocking poll_link_status loop.
    client.get_link_status.return_value = MagicMock(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_new",
        institution_name="Chase",
        model_dump_json=lambda **k: '{"status": "linked"}',  # type: ignore[misc]
    )
    # link-status uses the client directly, not the service
    with patch("moneybin.cli.commands.sync._build_sync_client", return_value=client):
        result = runner.invoke(
            app,
            ["sync", "link-status", "--session-id", "sess_x", "--output", "json"],
        )
    assert result.exit_code == 0, result.output
    assert "linked" in result.stdout


@pytest.mark.unit
def test_sync_link_status_text_is_a_finite_unpaged_result() -> None:
    """Link status is a single read, so it may use the shared finite result path."""
    from moneybin.connectors.sync_client import SyncClient

    client = MagicMock(spec=SyncClient)
    client.get_link_status.return_value = MagicMock(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_new",
        institution_name="Chase",
    )
    with patch("moneybin.cli.commands.sync._build_sync_client", return_value=client):
        result = runner.invoke(
            app,
            ["sync", "link-status", "--session-id", "sess_x", "--no-pager"],
        )

    assert result.exit_code == 0, result.output
    assert "Link status" in result.stdout
    assert "linked" in result.stdout


@pytest.mark.unit
def test_sync_link_status_pages_complete_answer_and_no_pager_prints_same_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long finite link-status result pages without changing its returned answer."""
    from moneybin.cli import pager
    from moneybin.connectors.sync_client import SyncClient

    client = MagicMock(spec=SyncClient)
    client.get_link_status.return_value = MagicMock(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_new",
        institution_name="Chase",
    )
    monkeypatch.setattr("moneybin.cli.commands.sync.get_terminal_policy", _pager_policy)
    pages: list[str] = []

    def capture_page(text: str, *, color: bool, wide: bool) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr(pager, "page_text", capture_page)
    with patch("moneybin.cli.commands.sync._build_sync_client", return_value=client):
        paged = runner.invoke(app, ["sync", "link-status", "--session-id", "sess_x"])
        direct = runner.invoke(
            app, ["sync", "link-status", "--session-id", "sess_x", "--no-pager"]
        )

    assert paged.exit_code == 0, paged.output
    assert direct.exit_code == 0, direct.output
    assert len(pages) == 1
    assert (
        pages[0].replace("\n\nq return to shell\n", "").rstrip()
        == direct.stdout.rstrip()
    )


@pytest.mark.unit
@patch("moneybin.cli.commands.sync.logger")
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_connect_alias_warns_and_forwards(
    mock_build: MagicMock, mock_logger: MagicMock
) -> None:
    """The deprecated `sync connect` alias warns but still forwards to `sync link`."""
    service = MagicMock()
    service.list_connections.return_value = []
    service.link.return_value = LinkResult(
        provider_item_id="item_new",
        institution_name="Chase",
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "connect", "--no-pull"])
    assert result.exit_code == 0, result.output
    service.link.assert_called_once()
    # Deprecation warning fires via module logger; caplog can't observe it
    # because setup_observability reconfigures logging mid-invocation. Assert
    # against the patched logger instead.
    assert mock_logger.warning.called
    assert any(
        "deprecated" in str(call.args[0]).lower()
        for call in mock_logger.warning.call_args_list
    )


@pytest.mark.unit
@patch("moneybin.cli.commands.sync.logger")
def test_sync_connect_status_alias_warns_and_forwards(mock_logger: MagicMock) -> None:
    """The deprecated `sync connect-status` alias warns but still forwards."""
    from moneybin.connectors.sync_client import SyncClient

    client = MagicMock(spec=SyncClient)
    client.get_link_status.return_value = MagicMock(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_new",
        institution_name="Chase",
        model_dump_json=lambda **k: '{"status": "linked"}',  # type: ignore[misc]
    )
    with patch("moneybin.cli.commands.sync._build_sync_client", return_value=client):
        result = runner.invoke(
            app,
            ["sync", "connect-status", "--session-id", "sess_x", "--output", "json"],
        )
    assert result.exit_code == 0, result.output
    assert "linked" in result.stdout
    assert mock_logger.warning.called
    assert any(
        "deprecated" in str(call.args[0]).lower()
        for call in mock_logger.warning.call_args_list
    )


@pytest.mark.unit
@patch("moneybin.cli.commands.sync.logger")
def test_sync_connect_status_alias_forwards_presentation_flags(
    mock_logger: MagicMock,
) -> None:
    """The hidden alias delegates the same finite-result controls as link-status."""
    from moneybin.connectors.sync_client import SyncClient

    client = MagicMock(spec=SyncClient)
    client.get_link_status.return_value = MagicMock(
        session_id="sess_x",
        status="linked",
        provider_item_id="item_new",
        institution_name="Chase",
    )
    with patch("moneybin.cli.commands.sync._build_sync_client", return_value=client):
        result = runner.invoke(
            app,
            [
                "sync",
                "connect-status",
                "--session-id",
                "sess_x",
                "--quiet",
                "--no-pager",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "Link status" in result.stdout
    client.get_link_status.assert_called_once_with("sess_x")


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_disconnect_requires_yes_or_confirm(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.disconnect.return_value.institution_name = "Chase"
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app, ["sync", "disconnect", "--institution", "Chase", "--yes"]
    )
    assert result.exit_code == 0, result.output
    service.disconnect.assert_called_once_with(
        institution="Chase", provider_item_id=None
    )
    assert "Disconnected" in result.stdout
    assert "Chase" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_disconnect_by_provider_item_id(mock_build: MagicMock) -> None:
    """--provider-item-id targets one exact connection, bypassing name lookup."""
    from moneybin.connectors.sync_models import ConnectedInstitution

    service = MagicMock()
    service.disconnect.return_value = ConnectedInstitution(
        id="conn_b",
        provider_item_id="item_b",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 3, 15, tzinfo=UTC),
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app, ["sync", "disconnect", "--provider-item-id", "item_b", "--yes"]
    )
    assert result.exit_code == 0, result.output
    service.disconnect.assert_called_once_with(
        institution=None, provider_item_id="item_b"
    )
    assert "Chase" in result.output


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_disconnect_rejects_both_institution_and_provider_item_id(
    mock_build: MagicMock,
) -> None:
    service = MagicMock()
    service.disconnect.side_effect = ValueError(
        "institution and provider_item_id are mutually exclusive — pass exactly one"
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(
        app,
        [
            "sync",
            "disconnect",
            "--institution",
            "Chase",
            "--provider-item-id",
            "item_a",
            "--yes",
        ],
    )
    assert result.exit_code == 1


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_disconnect_requires_yes_without_an_interactive_terminal(
    mock_build: MagicMock,
) -> None:
    """Redirected text cannot silently authorize a destructive disconnect."""
    result = runner.invoke(app, ["sync", "disconnect", "--institution", "Chase"])

    assert result.exit_code == 2, result.output
    assert "--yes" in result.stderr
    mock_build.assert_not_called()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_disconnect_requires_a_target(mock_build: MagicMock) -> None:
    """Neither --institution nor --provider-item-id given must refuse, not delete."""
    service = MagicMock()
    service.disconnect.side_effect = ValueError(
        "institution or provider_item_id is required to disconnect"
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "disconnect", "--yes"])
    assert result.exit_code == 1


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_disconnect_json_requires_yes_without_an_interactive_terminal(
    mock_build: MagicMock,
) -> None:
    """JSON mode has no prompt path, so it requires the explicit confirmation flag."""
    result = runner.invoke(
        app, ["sync", "disconnect", "--institution", "Chase", "--output", "json"]
    )

    assert result.exit_code == 2, result.output
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "mutation_confirmation_required"
    assert "--yes" in payload["error"]["hint"]
    assert result.stderr == ""
    mock_build.assert_not_called()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
@patch("moneybin.cli.commands.sync.typer.confirm", return_value=False)
def test_sync_disconnect_refusal_performs_no_mutation(
    mock_confirm: MagicMock,
    mock_build: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Declining the confirmation must resolve the plan but never delete."""
    from moneybin.connectors.sync_models import ConnectedInstitution

    monkeypatch.setattr("moneybin.cli.utils.sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stdout.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stderr.isatty", lambda: True)
    from moneybin.cli.commands.sync import sync_disconnect

    service = MagicMock()
    service.plan_disconnect.return_value = ConnectedInstitution(
        id="conn_a",
        provider_item_id="item_a",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 3, 1, tzinfo=UTC),
    )
    mock_build.return_value.__enter__.return_value = service

    with pytest.raises(typer.Exit) as exit_info:
        sync_disconnect(institution="Chase", yes=False, output=OutputFormat.TEXT)

    assert exit_info.value.exit_code == 0
    from rich.text import Text

    receipt = Text.from_ansi(capsys.readouterr().out).plain
    assert "Disconnect cancelled" in receipt
    assert "Institution: Chase" in receipt
    assert "No connection was removed" in receipt
    mock_confirm.assert_called_once()
    service.disconnect.assert_not_called()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
@patch("moneybin.cli.commands.sync.typer.confirm", return_value=True)
def test_sync_disconnect_interactive_confirm_names_provider_item_id(
    mock_confirm: MagicMock,
    mock_build: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The interactive prompt must name the planned connection's exact id."""
    from moneybin.connectors.sync_models import ConnectedInstitution

    monkeypatch.setattr("moneybin.cli.utils.sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stdout.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stderr.isatty", lambda: True)
    from moneybin.cli.commands.sync import sync_disconnect

    service = MagicMock()
    service.plan_disconnect.return_value = ConnectedInstitution(
        id="conn_a",
        provider_item_id="item_a",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 3, 1, tzinfo=UTC),
    )
    service.disconnect.return_value = service.plan_disconnect.return_value
    mock_build.return_value.__enter__.return_value = service

    sync_disconnect(institution="Chase", yes=False, output=OutputFormat.TEXT)

    prompt = mock_confirm.call_args.args[0]
    assert "Chase" in prompt
    assert "provider_item_id=item_a" in prompt
    service.disconnect.assert_called_once_with(provider_item_id="item_a")


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
@patch("moneybin.cli.commands.sync.typer.confirm", return_value=True)
def test_sync_disconnect_interactive_confirm_targets_planned_connection(
    mock_confirm: MagicMock,
    mock_build: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirming deletes the planned connection, not a same-named replacement.

    Regression for a bug where confirmation re-resolved `institution` by name
    after the prompt closed: if the planned connection was removed or relinked
    while the prompt was open, a different sole item with the same name would
    be deleted instead of the one the user actually confirmed.
    """
    from moneybin.connectors.sync_models import ConnectedInstitution

    monkeypatch.setattr("moneybin.cli.utils.sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stdout.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stderr.isatty", lambda: True)
    from moneybin.cli.commands.sync import sync_disconnect

    item_a = ConnectedInstitution(
        id="conn_a",
        provider_item_id="item_a",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 3, 1, tzinfo=UTC),
    )
    item_b = ConnectedInstitution(
        id="conn_b",
        provider_item_id="item_b",
        provider="plaid",
        institution_name="Chase",
        status="active",
        created_at=datetime(2026, 3, 15, tzinfo=UTC),
    )
    service = MagicMock()
    service.plan_disconnect.return_value = item_a

    def _disconnect(
        *, institution: str | None = None, provider_item_id: str | None = None
    ) -> ConnectedInstitution:
        if institution is not None:
            # A relink while the prompt was open replaced item A with item B
            # under the same institution name — a name-based resolve here
            # would delete the wrong connection.
            return item_b
        assert provider_item_id == "item_a"
        return item_a

    service.disconnect.side_effect = _disconnect
    mock_build.return_value.__enter__.return_value = service

    sync_disconnect(institution="Chase", yes=False, output=OutputFormat.TEXT)

    service.disconnect.assert_called_once_with(provider_item_id="item_a")


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
@patch("moneybin.cli.commands.sync.typer.confirm")
def test_sync_disconnect_ambiguous_institution_fails_before_prompt(
    mock_confirm: MagicMock,
    mock_build: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ambiguous name must refuse before any prompt and point at the flag."""
    monkeypatch.setattr("moneybin.cli.utils.sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stdout.isatty", lambda: True)
    monkeypatch.setattr("moneybin.cli.utils.sys.stderr.isatty", lambda: True)

    service = MagicMock()
    service.plan_disconnect.side_effect = ValueError(
        "multiple connected institutions match 'Chase': "
        "item_a (linked 2026-01-05 09:00 UTC), "
        "item_b (linked 2026-02-10 14:30 UTC). "
        "Target one by provider_item_id; `moneybin sync status --wide` "
        "lists every connection's id."
    )
    mock_build.return_value.__enter__.return_value = service

    from moneybin.cli.commands.sync import sync_disconnect

    with pytest.raises(typer.Exit) as exit_info:
        sync_disconnect(institution="Chase", yes=False, output=OutputFormat.TEXT)

    assert exit_info.value.exit_code == 1
    mock_confirm.assert_not_called()
    service.disconnect.assert_not_called()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_status_text_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="active",
            last_sync=datetime(2026, 4, 7, 14, 30, tzinfo=UTC),
            created_at=datetime(2026, 1, 5, 9, 0, tzinfo=UTC),
            guidance=None,
        ),
        SyncConnectionView(
            id="u2",
            provider_item_id="item_b",
            institution_name="Schwab",
            provider="plaid",
            status="error",
            last_sync=None,
            created_at=datetime(2026, 2, 1, 9, 0, tzinfo=UTC),
            guidance="Schwab needs re-authentication — run `moneybin sync link --institution Schwab`",
        ),
    ]
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "status"])
    assert result.exit_code == 0, result.output
    assert "Chase" in result.stdout
    assert "Schwab" in result.stdout
    assert "needs re-authentication" in result.stdout.replace("\n", " ")
    assert "Connected institutions" in result.stdout
    # created_at (the link date) must reach text output — several items at one
    # institution otherwise render as identical, unorderable rows (issue #408).
    assert "2026-01-05" in result.stdout
    # Item ID is narrowed by default; --wide (or JSON) is required to see it.
    assert "item_a" not in result.stdout
    assert "Item ID" not in result.stdout
    assert "5 of 6 columns shown — --wide for all" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_status_wide_shows_item_id(mock_build: MagicMock) -> None:
    """`--wide` restores the `Item ID` column.

    An identically-named duplicate connection can then be targeted by
    `provider_item_id` (issue #408).
    """
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="active",
            last_sync=datetime(2026, 4, 7, 14, 30, tzinfo=UTC),
            created_at=datetime(2026, 1, 5, 9, 0, tzinfo=UTC),
            guidance=None,
        ),
    ]
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "status", "--wide"])
    assert result.exit_code == 0, result.output
    assert "Item ID" in result.stdout
    assert "item_a" in result.stdout
    assert "columns shown" not in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_status_empty_output_keeps_scope_and_next_action(
    mock_build: MagicMock,
) -> None:
    """An empty status result still names the searched scope and recovery path."""
    service = MagicMock()
    service.list_connections.return_value = []
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "status", "--quiet", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert "No connected institutions" in result.stdout
    assert "moneybin sync link" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_status_no_pager_bypasses_shared_pager(
    mock_build: MagicMock,
) -> None:
    """The explicit pager opt-out must reach the shared finite result boundary."""
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="active",
            last_sync=None,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            guidance=None,
        )
    ]
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "status", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert "Chase" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_status_pages_complete_answer_and_no_pager_prints_same_answer(
    mock_build: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The finite status answer pages once and --no-pager prints every same row."""
    from moneybin.cli import pager

    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id=f"u{number}",
            provider_item_id=f"item_{number}",
            institution_name=f"Institution {number}",
            provider="plaid",
            status="active",
            last_sync=None,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            guidance=None,
        )
        for number in range(12)
    ]
    mock_build.return_value.__enter__.return_value = service
    monkeypatch.setattr("moneybin.cli.commands.sync.get_terminal_policy", _pager_policy)
    pages: list[str] = []

    def capture_page(text: str, *, color: bool, wide: bool) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr(pager, "page_text", capture_page)

    paged = runner.invoke(app, ["sync", "status"])
    direct = runner.invoke(app, ["sync", "status", "--no-pager"])

    assert paged.exit_code == 0, paged.output
    assert direct.exit_code == 0, direct.output
    assert len(pages) == 1
    assert "Institution 11" in pages[0]
    assert (
        pages[0].replace("\n\nq return to shell\n", "").rstrip()
        == direct.stdout.rstrip()
    )


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_auto_pull_partial_receipt_preserves_saved_and_remaining_scope(
    mock_build: MagicMock,
) -> None:
    """A failed requested institution cannot be hidden behind a link success line."""
    service = MagicMock()
    service.list_connections.return_value = []
    pull = _fake_pull_result()
    pull.institutions.append(
        InstitutionResult(
            provider_item_id="item_schwab",
            institution_name="Schwab",
            status="failed",
            error="connection timed out",
        )
    )
    service.link.return_value = LinkResult(
        provider_item_id="item_new",
        institution_name="Chase",
        pull_result=pull,
    )
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "link"])

    assert result.exit_code == 1, result.output
    assert "Link partially completed" in result.stdout
    assert "Linked Chase" not in result.stdout
    assert "10 transactions loaded" in result.stdout
    assert "Schwab was not refreshed" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync.logger")
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_partial_receipt_does_not_duplicate_terminal_warning(
    mock_build: MagicMock,
    mock_logger: MagicMock,
) -> None:
    """The receipt owns incomplete auto-pull facts; logging must not echo them to users."""
    service = MagicMock()
    service.list_connections.return_value = []
    pull = _fake_pull_result(
        transforms_applied=False,
        transforms_error="SQLMesh apply failed",
    )
    service.link.return_value = LinkResult(
        provider_item_id="item_new", institution_name="Chase", pull_result=pull
    )
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "link"])

    assert result.exit_code == 1, result.output
    assert (
        "Core tables and reports may still reflect data before this pull"
        in result.stdout
    )
    assert not any(
        "transforms failed" in str(call.args[0])
        for call in mock_logger.warning.call_args_list
    )


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_partial_receipt_has_no_duplicate_terminal_diagnostic(
    mock_build: MagicMock,
) -> None:
    """The required stdout receipt is the only terminal account of an auto-pull failure."""
    service = MagicMock()
    service.list_connections.return_value = []
    service.link.return_value = LinkResult(
        provider_item_id="item_new",
        institution_name="Chase",
        pull_result=_fake_pull_result(
            transforms_applied=False,
            transforms_error="SQLMesh apply failed",
        ),
    )
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "link"])

    assert result.exit_code == 1, result.output
    assert (
        "Core tables and reports may still reflect data before this pull"
        in result.stdout
    )
    assert "transforms failed" not in result.stderr.lower()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
@pytest.mark.parametrize("no_pull", [False, True])
def test_sync_link_interrupt_reports_unknown_saved_state(
    mock_build: MagicMock,
    no_pull: bool,
) -> None:
    """An interrupted hosted-link wait cannot claim that no link state was saved."""
    service = MagicMock()
    service.list_connections.return_value = []
    service.link.side_effect = KeyboardInterrupt
    mock_build.return_value.__enter__.return_value = service

    args = ["sync", "link"]
    if no_pull:
        args.append("--no-pull")
    result = runner.invoke(app, args)

    assert result.exit_code == 130, result.output
    assert "Link cancelled" in result.stdout
    receipt = " ".join(result.stdout.split())
    if no_pull:
        assert "Saved state: Unknown — link connection may have changed" in receipt
        assert "auto-pull data" not in result.stdout
    else:
        assert (
            "Saved state: Unknown — link and any auto-pull data may have changed"
            in receipt
        )
        assert "Refresh and report freshness are not confirmed" in receipt
    assert "moneybin sync status" in result.stdout
    assert "No connection was removed" not in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_success_uses_ascii_policy_symbol(
    mock_build: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The successful link receipt takes its functional symbol from TerminalPolicy."""
    service = MagicMock()
    service.list_connections.return_value = []
    service.link.return_value = LinkResult(
        provider_item_id="item_new",
        institution_name="Chase",
        pull_result=_fake_pull_result(),
    )
    mock_build.return_value.__enter__.return_value = service

    def ascii_policy(*, no_pager: bool = False) -> TerminalPolicy:
        return _pager_policy(no_pager=no_pager, ascii=True)

    monkeypatch.setattr(
        "moneybin.cli.commands.sync.get_terminal_policy",
        ascii_policy,
    )

    result = runner.invoke(app, ["sync", "link"])

    assert result.exit_code == 0, result.output
    assert "OK Link complete: Chase" in result.stdout
    assert "✓" not in result.stdout


@pytest.mark.unit
def test_sync_action_receipts_never_invoke_the_pager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mutation receipts remain complete output even on a pager-sized terminal."""
    from moneybin.cli import pager

    monkeypatch.setattr("moneybin.cli.commands.sync.get_terminal_policy", _pager_policy)
    page_text = MagicMock(return_value=True)
    monkeypatch.setattr(pager, "page_text", page_text)
    client = MagicMock()
    service = MagicMock()
    service.list_connections.return_value = []
    service.link.return_value = LinkResult(
        provider_item_id="item_new", institution_name="Chase"
    )
    with (
        patch("moneybin.cli.commands.sync._build_sync_client", return_value=client),
        patch("moneybin.cli.commands.sync._build_sync_service") as mock_build,
        patch("moneybin.connectors.sync_auth.SyncAuthService.logout"),
    ):
        mock_build.return_value.__enter__.return_value = service
        login = runner.invoke(app, ["sync", "login"])
        logout = runner.invoke(app, ["sync", "logout"])
        disconnect = runner.invoke(
            app, ["sync", "disconnect", "--institution", "Chase", "--yes"]
        )
        link = runner.invoke(app, ["sync", "link", "--no-pull"])

    assert all(result.exit_code == 0 for result in (login, logout, disconnect, link))
    page_text.assert_not_called()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_link_json_returns_only_the_initiate_event(mock_build: MagicMock) -> None:
    """JSON link stays event-driven; it cannot claim a later auto-pull outcome."""
    from moneybin.connectors.sync_models import LinkInitiateResponse

    service = MagicMock()
    service.list_connections.return_value = []
    service.initiate_link.return_value = LinkInitiateResponse(
        session_id="session-1",
        link_url="https://example.test/link",
        link_type="widget_flow",
        expiration=datetime(2026, 4, 7, 15, 30, tzinfo=UTC),
    )
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(app, ["sync", "link", "--output", "json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)["data"]
    assert payload["session_id"] == "session-1"
    assert "transactions_loaded" not in payload
    service.link.assert_not_called()


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_status_json_output(mock_build: MagicMock) -> None:
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="active",
            last_sync=datetime(2026, 4, 7, 14, 30, tzinfo=UTC),
            created_at=datetime(2026, 1, 5, 9, 0, tzinfo=UTC),
            guidance=None,
        ),
    ]
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "status", "--output", "json"])
    assert result.exit_code == 0, result.output
    rows = json.loads(result.stdout)["data"]["connections"]
    assert rows[0]["institution_name"] == "Chase"
    assert rows[0]["status"] == "active"
    assert rows[0]["error_code"] is None
    # created_at must survive the projection to CLI JSON output.
    assert rows[0]["created_at"] == "2026-01-05T09:00:00+00:00"


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_status_json_fields_still_projects_after_the_envelope(
    mock_build: MagicMock,
) -> None:
    """The projection survived the move onto the typed payload.

    This command hand-rolled its own field filter over a bare list, which is
    the only reason the flag ever worked: `render_or_json` used to project
    exclusively into a `list` payload, so handing it a typed one would have
    left a documented flag silently inert.
    """
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="active",
            last_sync=datetime(2026, 4, 7, 14, 30, tzinfo=UTC),
            created_at=datetime(2026, 1, 5, 9, 0, tzinfo=UTC),
            guidance=None,
        ),
    ]
    mock_build.return_value.__enter__.return_value = service

    result = runner.invoke(
        app,
        ["sync", "status", "--output", "json", "--json-fields", "institution_name"],
    )

    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)
    assert body["data"]["connections"] == [{"institution_name": "Chase"}]


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_status_shows_error_code_when_present(mock_build: MagicMock) -> None:
    """Sync status text output shows the error_code when it is non-null."""
    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="u1",
            provider_item_id="item_a",
            institution_name="Chase",
            provider="plaid",
            status="error",
            last_sync=None,
            created_at=datetime(2026, 1, 5, 9, 0, tzinfo=UTC),
            error_code="ITEM_LOGIN_REQUIRED",
            guidance="Chase needs re-authentication",
        ),
    ]
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "status"])
    assert result.exit_code == 0, result.output
    assert "ITEM_LOGIN_REQUIRED" in result.stdout


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_reports_the_best_effort_steps_its_refresh_ran(
    mock_build: MagicMock,
) -> None:
    """A pull runs four best-effort steps; none of them used to reach the user.

    ``SyncService.pull`` closes with ``refresh(steps=None)``, so it executes a
    matcher, a categorizer, an identity pass and a network-touching rate
    backfill on the user's behalf. It reported only the SQLMesh apply, so a
    provider outage mid-pull looked exactly like a clean pull.
    """
    from moneybin.services.refresh_outcome import RefreshStepOutcome, StageOutcome

    service = MagicMock()
    service.pull.return_value = _fake_pull_result(
        refresh_steps=RefreshStepOutcome(
            stages=(
                StageOutcome(step="match", ran=True, error="matcher blew up"),
                StageOutcome(step="categorize", ran=True, error="categorizer blew up"),
                # ran=False: a rates step that crashed produced no backfill, so
                # it never got as far as naming a pair.
                StageOutcome(step="rates", ran=False, error="provider timeout"),
            ),
            identity_errors=("merchants",),
        )
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull"])

    assert "Matching step failed" in result.stderr
    assert "Categorization step failed" in result.stderr
    assert "Merchants identity backfill failed" in result.stderr
    assert "Exchange rate backfill failed" in result.stderr


@pytest.mark.unit
@patch("moneybin.cli.commands.sync._build_sync_service")
def test_sync_pull_json_carries_the_refresh_step_outcome(
    mock_build: MagicMock,
) -> None:
    """The agent parsing JSON is owed what the human at the terminal is told.

    Keys are spelled as ``refresh_envelope`` spells them, so a caller reading
    `sync pull` and `refresh_run` does not learn two names for one outcome.
    """
    from moneybin.services.refresh_outcome import RefreshStepOutcome, StageOutcome

    service = MagicMock()
    service.pull.return_value = _fake_pull_result(
        refresh_steps=RefreshStepOutcome(
            stages=(
                StageOutcome(
                    step="match",
                    ran=True,
                    counts={
                        "auto_merged": 0,
                        "pending_review": 0,
                        "pending_transfers": 0,
                    },
                ),
                StageOutcome(step="rates", ran=True, counts={"rates_written": 4}),
            ),
            rate_pairs_unsupported=("EUR/XTS",),
        )
    )
    mock_build.return_value.__enter__.return_value = service
    result = runner.invoke(app, ["sync", "pull", "--output", "json"])

    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)["data"]
    stages = {s["step"]: s for s in payload["stages"]}
    assert stages["rates"]["counts"]["rates_written"] == 4
    assert payload["rate_pairs_unsupported"] == ["EUR/XTS"]
    # A step that ran clean still gets a row carrying a null error — it is not
    # dropped, so an agent can tell it apart from one that was never asked for.
    assert stages["match"]["error"] is None

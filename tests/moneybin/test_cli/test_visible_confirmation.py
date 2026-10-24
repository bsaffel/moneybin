"""Regression coverage for confirmations that must be visible to the operator."""

from __future__ import annotations

import io
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
import typer

from moneybin.cli.output import OutputFormat
from moneybin.cli.utils import get_terminal_policy


class _Stream(io.StringIO):
    """A text stream with an explicit terminal capability."""

    def __init__(self, value: str = "", *, is_tty: bool) -> None:
        super().__init__(value)
        self._is_tty = is_tty

    def isatty(self) -> bool:
        return self._is_tty


@pytest.fixture
def stdin_tty_stdout_redirected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make input prompt-capable while its prompt output is not visible."""
    monkeypatch.setattr("moneybin.cli.utils.sys.stdin", _Stream(is_tty=True))
    monkeypatch.setattr("moneybin.cli.utils.sys.stdout", _Stream(is_tty=False))
    monkeypatch.setattr("moneybin.cli.utils.sys.stderr", _Stream(is_tty=True))


def test_sync_link_reauth_refuses_when_prompt_output_is_redirected(
    stdin_tty_stdout_redirected: None,
) -> None:
    """Re-authentication cannot consume a response to an unseen prompt."""
    from moneybin.cli.commands.sync import sync_link
    from moneybin.connectors.sync_models import SyncConnectionView

    service = MagicMock()
    service.list_connections.return_value = [
        SyncConnectionView(
            id="connection-1",
            provider_item_id="item-1",
            institution_name="Example Credit Union",
            provider="plaid",
            status="error",
            last_sync=None,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            guidance=None,
        )
    ]

    assert not get_terminal_policy().interactive
    with (
        patch("moneybin.cli.commands.sync._build_sync_service") as build_service,
        patch("moneybin.cli.commands.sync.typer.confirm") as confirm,
        pytest.raises(typer.Exit) as exit_info,
    ):
        build_service.return_value.__enter__.return_value = service
        sync_link(
            institution=None,
            no_pull=False,
            no_browser=False,
            yes=False,
            output=OutputFormat.TEXT,
        )

    assert exit_info.value.exit_code == 2
    confirm.assert_not_called()
    service.link.assert_not_called()


def test_sync_disconnect_refuses_when_prompt_output_is_redirected(
    stdin_tty_stdout_redirected: None,
) -> None:
    """Disconnect cannot consume a response to an unseen prompt."""
    from moneybin.cli.commands.sync import sync_disconnect

    assert not get_terminal_policy().interactive
    with (
        patch("moneybin.cli.commands.sync._build_sync_service") as build_service,
        patch("moneybin.cli.commands.sync.typer.confirm") as confirm,
        pytest.raises(typer.Exit) as exit_info,
    ):
        sync_disconnect(
            institution="Example Credit Union", yes=False, output=OutputFormat.TEXT
        )

    assert exit_info.value.exit_code == 2
    confirm.assert_not_called()
    build_service.assert_not_called()


def test_transform_restate_refuses_when_prompt_output_is_redirected(
    stdin_tty_stdout_redirected: None,
) -> None:
    """Restatement cannot consume a response to an unseen prompt."""
    from moneybin.cli.commands.transform import transform_restate

    assert not get_terminal_policy().interactive
    with (
        patch("moneybin.cli.commands.transform.typer.confirm") as confirm,
        patch("moneybin.cli.commands.transform.sqlmesh_command") as sqlmesh_command,
        pytest.raises(typer.Exit) as exit_info,
    ):
        transform_restate(
            model="core.fct_transactions",
            start="2026-01-01",
            end=None,
            yes=False,
        )

    assert exit_info.value.exit_code == 2
    confirm.assert_not_called()
    sqlmesh_command.assert_not_called()


def test_matches_undo_refuses_piped_yes_before_prompt_or_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Piped input cannot confirm a match reversal."""
    from moneybin.cli.commands.transactions.matches import matches_undo

    monkeypatch.setattr("moneybin.cli.utils.sys.stdin", _Stream("y\n", is_tty=False))
    monkeypatch.setattr("moneybin.cli.utils.sys.stdout", _Stream(is_tty=True))
    monkeypatch.setattr("moneybin.cli.utils.sys.stderr", _Stream(is_tty=True))

    assert not get_terminal_policy().interactive
    with (
        patch("moneybin.cli.commands.transactions.matches.typer.confirm") as confirm,
        patch(
            "moneybin.cli.commands.transactions.matches.get_database"
        ) as get_database,
        patch(
            "moneybin.cli.commands.transactions.matches.MatchingService.undo"
        ) as undo,
        pytest.raises(typer.Exit) as exit_info,
    ):
        matches_undo(match_id="match-1", yes=False)

    assert exit_info.value.exit_code == 2
    confirm.assert_not_called()
    get_database.assert_not_called()
    undo.assert_not_called()

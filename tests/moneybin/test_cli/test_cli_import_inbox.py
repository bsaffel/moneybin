"""CLI tests for `moneybin import inbox` subcommands."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.inbox_service import InboxListResult, InboxSyncResult


@pytest.fixture
def runner() -> CliRunner:
    """Return a Typer CliRunner for invoking the root app."""
    return CliRunner()


def test_inbox_drain_prints_summary(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Draining the inbox prints 'N imported, M failed' summary."""
    fake = MagicMock()
    fake.sync.return_value = InboxSyncResult(
        processed=[{"filename": "chase-checking/march.csv", "transactions": 47}],
        failed=[],
    )
    fake.root = tmp_path / "inbox-root"
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox._build_service", lambda: fake
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 0, result.stderr
    assert "1 imported" in result.stdout
    assert "0 failed" in result.stdout


def test_inbox_drain_failure_exits_zero_but_warns(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Failed files exit 0 but display error_code in output."""
    fake = MagicMock()
    fake.sync.return_value = InboxSyncResult(
        processed=[],
        failed=[
            {
                "filename": "x.csv",
                "error_code": "needs_account_name",
                "sidecar": "failed/2026-05/x.csv.error.yml",
            }
        ],
    )
    fake.root = tmp_path / "inbox-root"
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox._build_service", lambda: fake
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 0
    assert "needs_account_name" in result.stdout + result.stderr
    assert "0 imported" in result.stdout
    assert "1 failed" in result.stdout


def test_inbox_drain_json_output(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """--output json emits a JSON payload with processed list."""
    fake = MagicMock()
    fake.sync.return_value = InboxSyncResult(
        processed=[{"filename": "a.csv", "transactions": 3}],
    )
    fake.root = tmp_path / "inbox-root"
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox._build_service", lambda: fake
    )

    result = runner.invoke(app, ["import", "inbox", "--output", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["processed"][0]["filename"] == "a.csv"


def test_inbox_list_prints_would_process(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`inbox list` shows each file that would be processed."""
    fake = MagicMock()
    fake.enumerate.return_value = InboxListResult(
        would_process=[
            {"filename": "chase-checking/march.csv", "account_hint": "chase-checking"}
        ],
    )
    fake.root = tmp_path / "inbox-root"
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox._build_service", lambda: fake
    )

    result = runner.invoke(app, ["import", "inbox", "list"])

    assert result.exit_code == 0
    assert "chase-checking/march.csv" in result.stdout


def test_inbox_path_prints_active_profile_root(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`inbox path` prints the service root directory."""
    fake = MagicMock()
    fake.root = tmp_path / "MoneyBin" / "alice"
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox._build_service", lambda: fake
    )

    result = runner.invoke(app, ["import", "inbox", "path"])

    assert result.exit_code == 0
    assert str(fake.root) in result.stdout.strip()

"""CLI tests for `moneybin import inbox` subcommands."""

from __future__ import annotations

import json
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.inbox_service import (
    InboxListResult,
    InboxSyncResult,
)


@contextmanager
def _fake_db_ctx(**kwargs: object) -> Generator[object, None, None]:
    yield object()


@pytest.fixture
def runner() -> CliRunner:
    """Return a Typer CliRunner for invoking the root app."""
    return CliRunner()


@pytest.fixture
def patch_inbox(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> MagicMock:
    """Patch InboxService factories + handle_cli_errors to skip the real DB."""
    fake = MagicMock()
    fake.root = tmp_path / "inbox-root"

    # Patch get_database in the CLI module so the `with get_database() as db:` in
    # inbox_default doesn't open a real encrypted database.
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox.get_database",
        _fake_db_ctx,
    )

    # Patch InboxService in the CLI module so both constructor call and
    # for_active_profile_no_db return `fake`.
    fake_cls = MagicMock(return_value=fake)
    fake_cls.for_active_profile_no_db = MagicMock(return_value=fake)
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox.InboxService",
        fake_cls,
    )
    monkeypatch.setattr("moneybin.cli.utils.handle_cli_errors", _fake_db_ctx)
    return fake


def test_inbox_drain_prints_summary(runner: CliRunner, patch_inbox: MagicMock) -> None:
    """Draining the inbox prints 'N imported, M failed' summary."""
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[{"filename": "chase-checking/march.csv", "transactions": 47}],
        failed=[],
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 0, result.stderr
    # Per-file ✓ lines on stdout (data); summary on stderr (status).
    assert "chase-checking/march.csv" in result.stdout
    assert "1 imported" in result.stderr
    assert "0 failed" in result.stderr


def test_inbox_drain_failure_exits_zero_but_warns(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """Failed files exit 0 but display error_code in output."""
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[],
        failed=[
            {
                "filename": "x.csv",
                "error_code": "needs_account_name",
                "sidecar": "failed/2026-05/x.csv.error.yml",
            }
        ],
    )

    result = runner.invoke(app, ["import", "inbox"])

    assert result.exit_code == 0
    assert "needs_account_name" in result.stderr
    assert "0 imported" in result.stderr
    assert "1 failed" in result.stderr


def test_inbox_drain_json_output(runner: CliRunner, patch_inbox: MagicMock) -> None:
    """--output json emits a JSON envelope with sync payload."""
    patch_inbox.sync.return_value = InboxSyncResult(
        processed=[{"filename": "a.csv", "transactions": 3}],
    )

    result = runner.invoke(app, ["import", "inbox", "--output", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["processed"][0]["filename"] == "a.csv"


def test_inbox_list_prints_would_process(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """`inbox list` shows each file that would be processed."""
    patch_inbox.enumerate.return_value = InboxListResult(
        would_process=[
            {"filename": "chase-checking/march.csv", "account_hint": "chase-checking"}
        ],
    )

    result = runner.invoke(app, ["import", "inbox", "list"])

    assert result.exit_code == 0
    assert "chase-checking/march.csv" in result.stdout


def test_inbox_path_prints_active_profile_root(
    runner: CliRunner, patch_inbox: MagicMock
) -> None:
    """`inbox path` prints the service root directory."""
    result = runner.invoke(app, ["import", "inbox", "path"])

    assert result.exit_code == 0
    assert str(patch_inbox.root) in result.stdout.strip()

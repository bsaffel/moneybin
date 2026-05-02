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
    InboxService,
    InboxSyncResult,
)


@contextmanager
def _fake_db_ctx() -> Generator[object, None, None]:
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

    def _factory(cls: type[InboxService]) -> MagicMock:
        return fake

    monkeypatch.setattr(
        "moneybin.services.inbox_service.InboxService.for_active_profile",
        classmethod(_factory),
    )
    monkeypatch.setattr(
        "moneybin.services.inbox_service.InboxService.for_active_profile_no_db",
        classmethod(_factory),
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
    assert payload["sync"]["processed"][0]["filename"] == "a.csv"


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

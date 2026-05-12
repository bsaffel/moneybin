"""CLI tests for ``moneybin transactions audit`` (per-txn audit log view)."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import Database
from moneybin.services.transaction_service import TransactionService
from tests.moneybin.test_cli._curation_helpers import make_curation_db, patch_db


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Database, None, None]:
    database = make_curation_db(tmp_path)
    patch_db(monkeypatch, database)
    yield database
    database.close()


def test_transactions_audit_lists_events_for_txn(
    runner: CliRunner, db: Database
) -> None:
    svc = TransactionService(db)
    svc.add_note("T1", "hello", actor="cli")
    svc.add_tags("T1", ["food"], actor="cli")

    result = runner.invoke(app, ["transactions", "audit", "T1", "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    events = payload["data"]
    actions = {e["action"] for e in events}
    assert "note.add" in actions
    assert "tag.add" in actions
    assert all(e["target_id"] == "T1" for e in events)


def test_transactions_audit_empty_returns_empty_list(
    runner: CliRunner, db: Database
) -> None:
    result = runner.invoke(
        app, ["transactions", "audit", "NEVER_EXISTED", "--output", "json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    events = payload["data"]
    assert events == []

"""CLI tests for ``moneybin system audit`` (list, show)."""

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


def test_system_audit_list_filters_by_action(runner: CliRunner, db: Database) -> None:
    svc = TransactionService(db)
    svc.add_note("T1", "alpha", actor="cli")
    svc.add_tags("T1", ["food"], actor="cli")

    result = runner.invoke(
        app,
        ["system", "audit", "list", "--action", "note.%", "--output", "json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    events = payload["data"]
    assert events
    assert all(e["action"].startswith("note.") for e in events)


def test_system_audit_list_filter_by_target_id(runner: CliRunner, db: Database) -> None:
    TransactionService(db).add_note("T1", "x", actor="cli")
    result = runner.invoke(
        app,
        ["system", "audit", "list", "--target-id", "T1", "--output", "json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    events = payload["data"]
    assert all(e["target_id"] == "T1" for e in events)


def test_system_audit_show_returns_chain(runner: CliRunner, db: Database) -> None:
    """rename_tag emits parent + child events; ``show`` returns both."""
    TransactionService(db).add_tags("T1", ["old"], actor="cli")
    rename = TransactionService(db).rename_tag("old", "new", actor="cli")

    result = runner.invoke(
        app,
        ["system", "audit", "show", rename.parent_audit_id, "--output", "json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    chain = payload["data"]
    actions = {e["action"] for e in chain}
    assert "tag.rename" in actions
    assert "tag.rename_row" in actions


def test_system_audit_show_unknown_id_exits_1(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(app, ["system", "audit", "show", "deadbeef"])
    assert result.exit_code == 1

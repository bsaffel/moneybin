"""CLI tests for ``moneybin transactions tags``."""

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


def test_tags_add_and_list(runner: CliRunner, db: Database) -> None:
    add = runner.invoke(
        app,
        ["transactions", "tags", "add", "T1", "personal", "food", "--output", "json"],
    )
    assert add.exit_code == 0, add.output
    payload = json.loads(add.stdout)["data"]
    assert sorted(payload["added"]) == ["food", "personal"]

    listed = runner.invoke(
        app, ["transactions", "tags", "list", "T1", "--output", "json"]
    )
    assert listed.exit_code == 0
    body = json.loads(listed.stdout)["data"]
    assert sorted(body["tags"]) == ["food", "personal"]


def test_tags_remove(runner: CliRunner, db: Database) -> None:
    TransactionService(db).add_tags("T1", ["personal"], actor="cli")
    result = runner.invoke(
        app, ["transactions", "tags", "remove", "T1", "personal", "--output", "json"]
    )
    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)["data"]
    assert body["removed"] == ["personal"]
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_tags WHERE transaction_id = 'T1'"
    ).fetchone()
    assert rows is not None and rows[0] == 0


def test_tags_list_distinct(runner: CliRunner, db: Database) -> None:
    TransactionService(db).add_tags("T1", ["food", "fun"], actor="cli")
    result = runner.invoke(app, ["transactions", "tags", "list", "--output", "json"])
    assert result.exit_code == 0
    payload = json.loads(result.stdout)["data"]
    assert {entry["tag"] for entry in payload} == {"food", "fun"}


def test_tags_rename_emits_parent_audit(runner: CliRunner, db: Database) -> None:
    TransactionService(db).add_tags("T1", ["old"], actor="cli")
    result = runner.invoke(
        app,
        ["transactions", "tags", "rename", "old", "new", "--output", "json"],
    )
    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)["data"]
    assert body["row_count"] == 1
    assert body["parent_audit_id"]


def test_tags_add_invalid_slug_exits_1(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(app, ["transactions", "tags", "add", "T1", "Bad Tag!"])
    assert result.exit_code == 1

"""CLI tests for ``moneybin transactions create``."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import Database
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


def test_transactions_create_minimum(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(
        app,
        ["transactions", "create", "--account", "A1", "--", "-12.50", "Coffee"],
    )
    assert result.exit_code == 0, result.output
    rows = db.conn.execute(
        "SELECT description, amount FROM raw.manual_transactions WHERE account_id = ?",
        ["A1"],
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "Coffee"


def test_transactions_create_json_output(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(
        app,
        [
            "transactions",
            "create",
            "--account",
            "A1",
            "--output",
            "json",
            "--",
            "-12.50",
            "Coffee",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    body = payload["data"]
    assert body["transaction_id"]
    assert body["import_id"]
    assert body["source_transaction_id"].startswith("manual_")


def test_transactions_create_with_note_and_tags(
    runner: CliRunner, db: Database
) -> None:
    result = runner.invoke(
        app,
        [
            "transactions",
            "create",
            "--account",
            "A1",
            "--note",
            "morning latte",
            "--tag",
            "personal",
            "--tag",
            "food",
            "--output",
            "json",
            "--",
            "-12.50",
            "Coffee",
        ],
    )
    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)["data"]
    assert body["note_id"] is not None
    assert sorted(body["tags"]) == ["food", "personal"]
    row = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_notes WHERE transaction_id = ?",
        [body["transaction_id"]],
    ).fetchone()
    assert row is not None
    assert row[0] == 1


def test_transactions_create_invalid_amount_exits_2(
    runner: CliRunner, db: Database
) -> None:
    result = runner.invoke(
        app,
        ["transactions", "create", "abc", "Coffee", "--account", "A1"],
    )
    assert result.exit_code == 2


def test_transactions_create_unknown_account_exits_1(
    runner: CliRunner, db: Database
) -> None:
    result = runner.invoke(
        app,
        ["transactions", "create", "--account", "UNKNOWN", "--", "-12.50", "Coffee"],
    )
    assert result.exit_code == 1

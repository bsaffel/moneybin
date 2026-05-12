"""CLI tests for ``moneybin transactions splits``."""

from __future__ import annotations

import json
from collections.abc import Generator
from decimal import Decimal
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


def test_splits_add_balanced(runner: CliRunner, db: Database) -> None:
    """T1 amount = -100. One split of -100 leaves residual 0."""
    result = runner.invoke(
        app,
        [
            "transactions",
            "splits",
            "add",
            "--category",
            "Food",
            "--output",
            "json",
            "--",
            "T1",
            "-100.00",
        ],
    )
    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)["split"]
    assert body["split"]["amount"] == "-100.00"
    assert Decimal(body["residual"]) == Decimal("0")


def test_splits_add_unbalanced_warns(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(
        app,
        ["transactions", "splits", "add", "--output", "json", "--", "T1", "-25.00"],
    )
    assert result.exit_code == 0
    # T1 amount=-100, child=-25 → residual = -100 - (-25) = -75
    body = json.loads(result.stdout)["split"]
    assert Decimal(body["residual"]) == Decimal("-75.00")


def test_splits_list(runner: CliRunner, db: Database) -> None:
    TransactionService(db).add_split("T1", Decimal("-50"), category="Food", actor="cli")
    result = runner.invoke(
        app, ["transactions", "splits", "list", "T1", "--output", "json"]
    )
    assert result.exit_code == 0
    splits = json.loads(result.stdout)["splits"]
    assert len(splits) == 1
    assert splits[0]["category"] == "Food"


def test_splits_remove_with_yes(runner: CliRunner, db: Database) -> None:
    s = TransactionService(db).add_split("T1", Decimal("-50"), actor="cli")
    result = runner.invoke(
        app, ["transactions", "splits", "remove", s.split_id, "--yes"]
    )
    assert result.exit_code == 0
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_splits WHERE split_id = ?",
        [s.split_id],
    ).fetchone()
    assert rows is not None and rows[0] == 0


def test_splits_clear_with_yes(runner: CliRunner, db: Database) -> None:
    TransactionService(db).add_split("T1", Decimal("-50"), actor="cli")
    TransactionService(db).add_split("T1", Decimal("-25"), actor="cli")
    result = runner.invoke(app, ["transactions", "splits", "clear", "T1", "--yes"])
    assert result.exit_code == 0
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_splits WHERE transaction_id = 'T1'"
    ).fetchone()
    assert rows is not None and rows[0] == 0


def test_splits_add_invalid_amount_exits_1(runner: CliRunner, db: Database) -> None:
    # InvalidOperation is classified as invalid_input by handle_cli_errors → exit 1,
    # not exit 2 (usage error), because the error is a runtime data problem.
    result = runner.invoke(app, ["transactions", "splits", "add", "T1", "notanumber"])
    assert result.exit_code == 1

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
    body = json.loads(result.stdout)["data"]
    assert body["split"]["amount"] == "-100.00"
    assert Decimal(body["residual"]) == Decimal("0")


def test_splits_add_unbalanced_warns(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(
        app,
        ["transactions", "splits", "add", "--output", "json", "--", "T1", "-25.00"],
    )
    assert result.exit_code == 0
    # T1 amount=-100, child=-25 → residual = -100 - (-25) = -75
    body = json.loads(result.stdout)["data"]
    assert Decimal(body["residual"]) == Decimal("-75.00")


def test_splits_list(runner: CliRunner, db: Database) -> None:
    TransactionService(db).add_split("T1", Decimal("-50"), category="Food", actor="cli")
    result = runner.invoke(
        app, ["transactions", "splits", "list", "T1", "--output", "json"]
    )
    assert result.exit_code == 0
    body = json.loads(result.stdout)["data"]
    assert len(body["splits"]) == 1
    assert body["splits"][0]["category"] == "Food"


def test_splits_list_text_names_an_uncategorized_split(
    runner: CliRunner, db: Database
) -> None:
    """Requirement 30: one word for the absence, on every surface that shows it.

    This branch renders `-` before the change, which is a second placeholder
    for the condition `transactions list` already spells `Uncategorized` — the
    two-patterns-for-one-job the coherence rule prohibits. The JSON assertion
    above cannot see it: this line is the text branch, and nothing covered it.
    """
    svc = TransactionService(db)
    svc.add_split("T1", Decimal("-50"), category="Food", actor="cli")
    svc.add_split("T1", Decimal("-25"), actor="cli")

    result = runner.invoke(app, ["transactions", "splits", "list", "T1"])

    assert result.exit_code == 0
    assert "Uncategorized" in result.output
    # The categorised split keeps its own word, so the placeholder is standing
    # in for the absence rather than flattening the column.
    assert "Food" in result.output


def test_splits_add_refuses_a_blank_category(runner: CliRunner, db: Database) -> None:
    """The blank the renderer was told to distrust can no longer be stored.

    #515 restricted the placeholder to NULL because `add_split` took
    `--category ""` verbatim, so calling a stored blank absent would have
    reported a gap the curator never left. The blank is now refused at the
    service, matching the MCP write contracts, and the renderer's NULL-only
    rule holds without needing to defend against a value that cannot exist.
    """
    result = runner.invoke(
        app,
        ["transactions", "splits", "add", "--category", "   ", "--", "T1", "-75.00"],
    )

    assert result.exit_code == 1
    # A refusal, not a crash. An uncaught ValueError exits 1 under CliRunner
    # too, so exit code alone cannot tell the two apart; what distinguishes
    # them is which exception reached the runner.
    assert isinstance(result.exception, SystemExit)
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_splits WHERE transaction_id = 'T1'"
    ).fetchone()
    assert rows is not None and rows[0] == 0


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


def test_splits_remove_missing_exits_1(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(
        app, ["transactions", "splits", "remove", "doesnotexist", "--yes"]
    )
    assert result.exit_code == 1
    assert "not found" in result.output


def test_splits_clear_with_yes(runner: CliRunner, db: Database) -> None:
    TransactionService(db).add_split("T1", Decimal("-50"), actor="cli")
    TransactionService(db).add_split("T1", Decimal("-25"), actor="cli")
    result = runner.invoke(app, ["transactions", "splits", "clear", "T1", "--yes"])
    assert result.exit_code == 0
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_splits WHERE transaction_id = 'T1'"
    ).fetchone()
    assert rows is not None and rows[0] == 0


def test_splits_add_invalid_amount_exits_2(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(app, ["transactions", "splits", "add", "T1", "notanumber"])
    assert result.exit_code == 2

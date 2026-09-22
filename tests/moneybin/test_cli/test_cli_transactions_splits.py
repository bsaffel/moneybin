"""CLI tests for ``moneybin transactions splits``."""

from __future__ import annotations

import json
from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.cli import utils as cli_utils
from moneybin.cli.main import app
from moneybin.database import Database
from moneybin.services.transaction_service import TransactionService
from tests.moneybin.test_cli._curation_helpers import make_curation_db, patch_db


class _Stream:
    def __init__(self, tty: bool) -> None:
        self.tty = tty
        self.encoding = "utf-8"

    def isatty(self) -> bool:
        return self.tty


def _set_terminal_streams(
    monkeypatch: pytest.MonkeyPatch, *, stdin_tty: bool, stdout_tty: bool
) -> None:
    monkeypatch.setattr(
        cli_utils,
        "sys",
        SimpleNamespace(
            stdin=_Stream(stdin_tty),
            stdout=_Stream(stdout_tty),
            stderr=_Stream(True),
        ),
    )


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


def test_splits_mutations_emit_scoped_unpaged_receipts(
    runner: CliRunner, db: Database
) -> None:
    added = runner.invoke(app, ["transactions", "splits", "add", "T1", "--", "-25.00"])
    assert added.exit_code == 0, added.output
    assert "Split added" in added.stdout
    assert "T1" in added.stdout
    assert "Splits do not balance" in added.stdout

    split_id = TransactionService(db).list_splits("T1")[0].split_id
    removed = runner.invoke(
        app, ["transactions", "splits", "remove", split_id, "--yes"]
    )
    assert removed.exit_code == 0, removed.output
    assert "Split removed" in removed.stdout
    assert split_id in removed.stdout

    TransactionService(db).add_split("T1", Decimal("-25"), actor="cli")
    cleared = runner.invoke(app, ["transactions", "splits", "clear", "T1", "--yes"])
    assert cleared.exit_code == 0, cleared.output
    assert "Splits cleared" in cleared.stdout
    assert "1" in cleared.stdout


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


def test_splits_list_text_has_scope_and_a_safe_empty_action(
    runner: CliRunner, db: Database
) -> None:
    result = runner.invoke(app, ["transactions", "splits", "list", "T1", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert "Transaction splits" in result.stdout
    assert "No splits on this transaction." in result.stdout
    assert "moneybin transactions splits add --help" in result.stdout


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


@pytest.mark.parametrize("output_args", [[], ["--output", "json"]])
def test_splits_remove_with_yes(
    runner: CliRunner, db: Database, output_args: list[str]
) -> None:
    s = TransactionService(db).add_split("T1", Decimal("-50"), actor="cli")
    result = runner.invoke(
        app, ["transactions", "splits", "remove", s.split_id, "--yes", *output_args]
    )
    assert result.exit_code == 0
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_splits WHERE split_id = ?",
        [s.split_id],
    ).fetchone()
    assert rows is not None and rows[0] == 0


@pytest.mark.parametrize(
    ("output_args", "stdin_tty", "stdout_tty"),
    [([], False, True), ([], True, False), (["--output", "json"], True, True)],
)
def test_splits_remove_refuses_piped_confirmation_without_yes(
    runner: CliRunner,
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    output_args: list[str],
    stdin_tty: bool,
    stdout_tty: bool,
) -> None:
    """A redirected ``y`` must not authorize removing a split."""
    split = TransactionService(db).add_split("T1", Decimal("-50"), actor="cli")
    _set_terminal_streams(monkeypatch, stdin_tty=stdin_tty, stdout_tty=stdout_tty)

    result = runner.invoke(
        app,
        ["transactions", "splits", "remove", split.split_id, *output_args],
        input="y\n",
    )

    assert result.exit_code == 2, result.output
    if output_args:
        body = json.loads(result.stdout)
        assert body["error"]["code"] == error_codes.MUTATION_CONFIRMATION_REQUIRED
        assert "Remove split" not in result.output
    assert TransactionService(db).get_split(split.split_id) is not None


def test_splits_remove_cancellation_is_a_visible_receipt(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    split = TransactionService(db).add_split("T1", Decimal("-50"), actor="cli")
    _set_terminal_streams(monkeypatch, stdin_tty=True, stdout_tty=True)

    result = runner.invoke(
        app, ["transactions", "splits", "remove", split.split_id], input="n\n"
    )

    assert result.exit_code == 0, result.output
    assert "Split removal cancelled" in result.stdout
    assert "No split was removed" in result.stdout


def test_splits_clear_cancellation_is_a_visible_receipt(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A declined clear reports that the existing splits remain saved."""
    TransactionService(db).add_split("T1", Decimal("-50"), actor="cli")
    _set_terminal_streams(monkeypatch, stdin_tty=True, stdout_tty=True)

    result = runner.invoke(app, ["transactions", "splits", "clear", "T1"], input="n\n")

    assert result.exit_code == 0, result.output
    assert "Split clear cancelled" in result.stdout
    assert "No splits were removed" in result.stdout
    assert len(TransactionService(db).list_splits("T1")) == 1


def test_splits_remove_missing_exits_1(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(
        app, ["transactions", "splits", "remove", "doesnotexist", "--yes"]
    )
    assert result.exit_code == 1
    assert "not found" in result.output


@pytest.mark.parametrize("output_args", [[], ["--output", "json"]])
def test_splits_clear_with_yes(
    runner: CliRunner, db: Database, output_args: list[str]
) -> None:
    TransactionService(db).add_split("T1", Decimal("-50"), actor="cli")
    TransactionService(db).add_split("T1", Decimal("-25"), actor="cli")
    result = runner.invoke(
        app, ["transactions", "splits", "clear", "T1", "--yes", *output_args]
    )
    assert result.exit_code == 0
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_splits WHERE transaction_id = 'T1'"
    ).fetchone()
    assert rows is not None and rows[0] == 0


@pytest.mark.parametrize(
    ("output_args", "stdin_tty", "stdout_tty"),
    [([], False, True), ([], True, False), (["--output", "json"], True, True)],
)
def test_splits_clear_refuses_piped_confirmation_without_yes(
    runner: CliRunner,
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    output_args: list[str],
    stdin_tty: bool,
    stdout_tty: bool,
) -> None:
    """A redirected ``y`` must not authorize clearing every split."""
    TransactionService(db).add_split("T1", Decimal("-50"), actor="cli")
    _set_terminal_streams(monkeypatch, stdin_tty=stdin_tty, stdout_tty=stdout_tty)

    result = runner.invoke(
        app,
        ["transactions", "splits", "clear", "T1", *output_args],
        input="y\n",
    )

    assert result.exit_code == 2, result.output
    if output_args:
        body = json.loads(result.stdout)
        assert body["error"]["code"] == error_codes.MUTATION_CONFIRMATION_REQUIRED
        assert "Clear all splits" not in result.output
    assert len(TransactionService(db).list_splits("T1")) == 1


def test_splits_clear_receipt_uses_resolved_transaction_and_actual_count(
    runner: CliRunner, db: Database
) -> None:
    db.conn.execute(
        "INSERT INTO app.transaction_id_aliases "
        "(old_transaction_id, new_transaction_id, created_at) "
        "VALUES ('T1_OLD', 'T1', CURRENT_TIMESTAMP)"
    )
    TransactionService(db).add_split("T1", Decimal("-25"), actor="cli")

    result = runner.invoke(app, ["transactions", "splits", "clear", "T1_OLD", "--yes"])

    assert result.exit_code == 0, result.output
    assert "T1" in result.stdout
    assert "Splits cleared: 1" in result.stdout
    assert TransactionService(db).list_splits("T1") == []


def test_splits_clear_noop_receipt_reports_zero(
    runner: CliRunner, db: Database
) -> None:
    result = runner.invoke(app, ["transactions", "splits", "clear", "T1", "--yes"])

    assert result.exit_code == 0, result.output
    assert "Splits cleared: 0" in result.stdout


def test_splits_add_invalid_amount_exits_2(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(app, ["transactions", "splits", "add", "T1", "notanumber"])
    assert result.exit_code == 2

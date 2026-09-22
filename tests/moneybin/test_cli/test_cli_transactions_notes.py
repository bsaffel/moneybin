"""CLI tests for ``moneybin transactions notes``."""

from __future__ import annotations

import json
from collections.abc import Generator
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


def test_notes_add_then_list(runner: CliRunner, db: Database) -> None:
    add = runner.invoke(
        app, ["transactions", "notes", "add", "T1", "first", "--output", "json"]
    )
    assert add.exit_code == 0, add.output
    payload = json.loads(add.stdout)["data"]
    assert payload["text"] == "first"
    assert payload["transaction_id"] == "T1"

    listed = runner.invoke(
        app, ["transactions", "notes", "list", "T1", "--output", "json"]
    )
    assert listed.exit_code == 0
    notes = json.loads(listed.stdout)["data"]["notes"]
    assert len(notes) == 1
    assert notes[0]["text"] == "first"


def test_notes_edit_changes_text(runner: CliRunner, db: Database) -> None:
    note = TransactionService(db).add_note("T1", "before", actor="cli")
    result = runner.invoke(
        app,
        ["transactions", "notes", "edit", note.note_id, "after", "--output", "json"],
    )
    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)["data"]
    assert body["text"] == "after"


@pytest.mark.parametrize("output_args", [[], ["--output", "json"]])
def test_notes_delete_with_yes(
    runner: CliRunner, db: Database, output_args: list[str]
) -> None:
    note = TransactionService(db).add_note("T1", "doomed", actor="cli")
    result = runner.invoke(
        app, ["transactions", "notes", "delete", note.note_id, "--yes", *output_args]
    )
    assert result.exit_code == 0
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_notes WHERE note_id = ?",
        [note.note_id],
    ).fetchone()
    assert rows is not None and rows[0] == 0


@pytest.mark.parametrize(
    ("output_args", "stdin_tty", "stdout_tty"),
    [([], False, True), ([], True, False), (["--output", "json"], True, True)],
)
def test_notes_delete_refuses_piped_confirmation_without_yes(
    runner: CliRunner,
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    output_args: list[str],
    stdin_tty: bool,
    stdout_tty: bool,
) -> None:
    """A redirected ``y`` is input, not the explicit --yes mutation intent."""
    note = TransactionService(db).add_note("T1", "keep", actor="cli")
    _set_terminal_streams(monkeypatch, stdin_tty=stdin_tty, stdout_tty=stdout_tty)

    result = runner.invoke(
        app,
        ["transactions", "notes", "delete", note.note_id, *output_args],
        input="y\n",
    )

    assert result.exit_code == 2, result.output
    if output_args:
        body = json.loads(result.stdout)
        assert body["error"]["code"] == error_codes.MUTATION_CONFIRMATION_REQUIRED
        assert "Delete note" not in result.output
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_notes WHERE note_id = ?",
        [note.note_id],
    ).fetchone()
    assert rows is not None and rows[0] == 1


def test_notes_delete_cancellation_is_a_visible_receipt(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    note = TransactionService(db).add_note("T1", "keep", actor="cli")
    _set_terminal_streams(monkeypatch, stdin_tty=True, stdout_tty=True)

    result = runner.invoke(
        app, ["transactions", "notes", "delete", note.note_id], input="n\n"
    )

    assert result.exit_code == 0, result.output
    assert "Note deletion cancelled" in result.stdout
    assert "Note was not deleted" in result.stdout


def test_notes_edit_unknown_id_exits_1(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(
        app, ["transactions", "notes", "edit", "deadbeef", "anything"]
    )
    assert result.exit_code == 1


def test_notes_add_empty_text_fails(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(app, ["transactions", "notes", "add", "T1", ""])
    assert result.exit_code == 1

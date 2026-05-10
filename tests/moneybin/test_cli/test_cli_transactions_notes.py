"""CLI tests for ``moneybin transactions notes``."""

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


def test_notes_add_then_list(runner: CliRunner, db: Database) -> None:
    add = runner.invoke(
        app, ["transactions", "notes", "add", "T1", "first", "--output", "json"]
    )
    assert add.exit_code == 0, add.output
    payload = json.loads(add.stdout)["note"]
    assert payload["text"] == "first"
    assert payload["transaction_id"] == "T1"

    listed = runner.invoke(
        app, ["transactions", "notes", "list", "T1", "--output", "json"]
    )
    assert listed.exit_code == 0
    notes = json.loads(listed.stdout)["notes"]
    assert len(notes) == 1
    assert notes[0]["text"] == "first"


def test_notes_edit_changes_text(runner: CliRunner, db: Database) -> None:
    note = TransactionService(db).add_note("T1", "before", actor="cli")
    result = runner.invoke(
        app,
        ["transactions", "notes", "edit", note.note_id, "after", "--output", "json"],
    )
    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)["note"]
    assert body["text"] == "after"


def test_notes_delete_with_yes(runner: CliRunner, db: Database) -> None:
    note = TransactionService(db).add_note("T1", "doomed", actor="cli")
    result = runner.invoke(
        app, ["transactions", "notes", "delete", note.note_id, "--yes"]
    )
    assert result.exit_code == 0
    rows = db.conn.execute(
        "SELECT COUNT(*) FROM app.transaction_notes WHERE note_id = ?",
        [note.note_id],
    ).fetchone()
    assert rows is not None and rows[0] == 0


def test_notes_edit_unknown_id_exits_1(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(
        app, ["transactions", "notes", "edit", "deadbeef", "anything"]
    )
    assert result.exit_code == 1


def test_notes_add_empty_text_fails(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(app, ["transactions", "notes", "add", "T1", ""])
    assert result.exit_code == 1

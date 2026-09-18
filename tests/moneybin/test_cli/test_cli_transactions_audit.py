"""CLI tests for ``moneybin transactions audit`` (per-txn audit log view)."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from moneybin.cli import pager
from moneybin.cli.main import app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
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
    events: list[dict[str, Any]] = payload["data"]
    actions = {e["action"] for e in events}
    assert "note.add" in actions
    assert "tag.add" in actions
    # Row-grain: target_id is the entity PK (note_id / "T1:food"), but every event
    # relates to T1 via its captured row image.
    for e in events:
        row: dict[str, Any] = e.get("after_value") or e.get("before_value") or {}
        assert row.get("transaction_id") == "T1"


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


def test_transactions_audit_text_has_scope_and_a_safe_empty_action(
    runner: CliRunner, db: Database
) -> None:
    result = runner.invoke(
        app, ["transactions", "audit", "NEVER_EXISTED", "--quiet", "--no-pager"]
    )

    assert result.exit_code == 0, result.output
    assert "Transaction audit" in result.stdout
    assert "No audit events for this transaction." in result.stdout
    assert "moneybin transactions --help" in result.stdout


def test_transactions_audit_pages_and_no_pager_bypasses_it(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The command carries its complete human answer through the shared pager."""
    svc = TransactionService(db)
    svc.add_note("T1", "first", actor="cli")
    svc.add_tags("T1", ["food"], actor="cli")
    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=True,
        ascii=True,
        width=80,
        height=1,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.transactions.audit.get_terminal_policy",
        lambda *, no_pager=False: policy,
    )
    pages: list[str] = []

    def record_page(text: str, *, color: bool, wide: bool) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr(
        pager,
        "page_text",
        record_page,
    )

    result = runner.invoke(app, ["transactions", "audit", "T1"])

    assert result.exit_code == 0, result.output
    assert len(pages) == 1
    assert "Transaction audit" in pages[0]

    pages.clear()
    result = runner.invoke(app, ["transactions", "audit", "T1", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert pages == []
    assert "Transaction audit" in result.stdout

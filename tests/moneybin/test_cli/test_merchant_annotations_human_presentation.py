"""Human presentation coverage for merchant mappings and transaction annotations."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock, patch

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


def test_merchant_lists_offer_a_pager_escape_without_initializing_runtime() -> None:
    """Every finite merchant read exposes the shared pager control."""
    for command in (
        ["merchants", "list", "--help"],
        ["merchants", "links", "pending", "--help"],
        ["merchants", "links", "history", "--help"],
    ):
        result = CliRunner().invoke(app, command)
        assert result.exit_code == 0, result.output
        assert "--no-pager" in result.stdout


def test_notes_and_tags_lists_offer_a_pager_escape() -> None:
    """Annotation reads are pageable finite results."""
    for command in (
        ["transactions", "notes", "list", "--help"],
        ["transactions", "tags", "list", "--help"],
    ):
        result = CliRunner().invoke(app, command)
        assert result.exit_code == 0, result.output
        assert "--no-pager" in result.stdout


def test_note_list_quiet_empty_keeps_scope_and_a_safe_action(
    runner: CliRunner, db: Database
) -> None:
    """Quiet does not hide the requested empty answer or its next action."""
    result = runner.invoke(
        app,
        ["transactions", "notes", "list", "T1", "--quiet", "--no-pager"],
    )

    assert result.exit_code == 0, result.output
    assert "T1" in result.stdout
    assert "transactions notes add --help" in result.stdout


def test_tag_list_is_pageable_and_no_pager_prints_the_same_answer(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shared pager receives the full tag answer; --no-pager bypasses it."""
    from moneybin.cli import pager
    from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

    TransactionService(db).add_tags("T1", ["food", "personal"], actor="cli")
    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=True,
        ascii=True,
        width=40,
        height=1,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.transactions.tags.get_terminal_policy",
        lambda *, no_pager=False: policy,
    )
    pages: list[str] = []

    def capture_page(text: str, *, color: bool, wide: bool) -> bool:
        del color, wide
        pages.append(text)
        return True

    monkeypatch.setattr(
        pager,
        "page_text",
        capture_page,
    )

    paged = runner.invoke(app, ["transactions", "tags", "list"])
    bypass = runner.invoke(app, ["transactions", "tags", "list", "--no-pager"])

    assert paged.exit_code == 0, paged.output
    assert bypass.exit_code == 0, bypass.output
    assert len(pages) == 1
    assert "food" in pages[0]
    assert "food" in bypass.stdout


def test_annotation_mutations_render_unpaged_receipts(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Write receipts bypass paging and name the affected target and effect."""
    from moneybin.cli import pager
    from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=True,
        ascii=True,
        width=20,
        height=1,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.transactions.notes.get_terminal_policy",
        lambda: policy,
    )

    def unexpected_page(*args: object, **kwargs: object) -> bool:
        del args, kwargs
        pytest.fail("paged")

    monkeypatch.setattr(pager, "page_text", unexpected_page)

    result = runner.invoke(app, ["transactions", "notes", "add", "T1", "receipt"])

    assert result.exit_code == 0, result.output
    assert "T1" in result.stdout
    assert "Note added" in result.stdout


@patch("moneybin.cli.commands.merchants.links.get_database")
@patch("moneybin.services.merchant_links_service.MerchantLinksService.set")
def test_merchant_link_set_renders_a_receipt(
    set_link: MagicMock, database: MagicMock
) -> None:
    """A decision write gives the human the target and the applied outcome."""
    database.return_value.__enter__.return_value = MagicMock()

    result = CliRunner().invoke(
        app,
        ["merchants", "links", "set", "decision-1", "--into", "merchant-1"],
    )

    assert result.exit_code == 0, result.output
    assert "decision-1" in result.stdout
    assert "merchant-1" in result.stdout


def test_annotation_mutation_receipts_name_the_target_and_effect(
    runner: CliRunner, db: Database
) -> None:
    """Each annotation write returns a concise receipt on stdout."""
    added_note = runner.invoke(app, ["transactions", "notes", "add", "T1", "first"])
    assert added_note.exit_code == 0, added_note.output
    assert "Note added" in added_note.stdout
    note_id = added_note.stdout.split("Note ID:", maxsplit=1)[1].strip()

    edited_note = runner.invoke(
        app, ["transactions", "notes", "edit", note_id, "second"]
    )
    assert edited_note.exit_code == 0, edited_note.output
    assert "Note updated" in edited_note.stdout

    deleted_note = runner.invoke(
        app, ["transactions", "notes", "delete", note_id, "--yes"]
    )
    assert deleted_note.exit_code == 0, deleted_note.output
    assert "Note deleted" in deleted_note.stdout

    added_tags = runner.invoke(app, ["transactions", "tags", "add", "T1", "food"])
    assert added_tags.exit_code == 0, added_tags.output
    assert "Tags updated" in added_tags.stdout
    assert "T1" in added_tags.stdout

    removed_tags = runner.invoke(app, ["transactions", "tags", "remove", "T1", "food"])
    assert removed_tags.exit_code == 0, removed_tags.output
    assert "Tags removed" in removed_tags.stdout


def test_empty_merchant_history_keeps_a_nonrepeating_next_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The empty history tells the user how to generate reviewable results."""
    from moneybin.cli.commands.merchants import links

    database = MagicMock()
    database.return_value.__enter__.return_value = MagicMock()
    monkeypatch.setattr(links, "get_database", database)
    with patch(
        "moneybin.services.merchant_links_service.MerchantLinksService.history",
        return_value=[],
    ):
        normal = CliRunner().invoke(app, ["merchants", "links", "history"])
        quiet = CliRunner().invoke(app, ["merchants", "links", "history", "--quiet"])

    assert normal.exit_code == quiet.exit_code == 0
    assert "No merchant-link decisions found" in normal.stdout
    assert normal.stdout.count("merchants links run") == 1
    assert "No merchant-link decisions found" in quiet.stdout
    assert "merchants links run" not in quiet.stdout

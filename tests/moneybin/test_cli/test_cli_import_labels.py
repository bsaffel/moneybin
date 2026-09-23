"""CLI tests for ``moneybin import labels`` (add, remove, list)."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import Database
from moneybin.services.import_service import ImportService
from tests.moneybin.test_cli._curation_helpers import make_curation_db, patch_db


@pytest.fixture()
def db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Database, None, None]:
    database = make_curation_db(tmp_path)
    patch_db(monkeypatch, database)
    yield database
    database.close()


def _allocate_import(database: Database) -> str:
    return ImportService(database).allocate_import_log(
        source_type="manual", format_name="manual_entry", actor="cli"
    )


def test_import_labels_add_then_list_for_id(runner: CliRunner, db: Database) -> None:
    import_id = _allocate_import(db)
    result = runner.invoke(
        app,
        [
            "import",
            "labels",
            "add",
            import_id,
            "tax-2026",
            "personal",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)["data"]
    assert sorted(body["labels"]) == sorted(["tax-2026", "personal"])

    listed = runner.invoke(
        app,
        ["import", "labels", "list", "--import-id", import_id, "--output", "json"],
    )
    assert listed.exit_code == 0
    body = json.loads(listed.stdout)["data"]
    assert sorted(body["labels"]) == sorted(["tax-2026", "personal"])


def test_import_labels_remove(runner: CliRunner, db: Database) -> None:
    import_id = _allocate_import(db)
    ImportService(db).add_labels(import_id, ["x", "y"], actor="cli")
    result = runner.invoke(
        app,
        ["import", "labels", "remove", import_id, "x", "--output", "json"],
    )
    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)["data"]
    assert body["labels"] == ["y"]


def test_import_labels_add_text_receipt_names_the_import_and_returned_labels(
    runner: CliRunner, db: Database
) -> None:
    """A label write returns a factual stdout receipt rather than a log line."""
    import_id = _allocate_import(db)

    result = runner.invoke(app, ["import", "labels", "add", import_id, "tax-2026"])

    assert result.exit_code == 0, result.output
    assert "Labels added" in result.stdout
    assert import_id in result.stdout
    assert "tax-2026" in result.stdout


def test_import_labels_list_distinct_with_counts(
    runner: CliRunner, db: Database
) -> None:
    i1 = _allocate_import(db)
    i2 = _allocate_import(db)
    ImportService(db).add_labels(i1, ["shared", "only-i1"], actor="cli")
    ImportService(db).add_labels(i2, ["shared"], actor="cli")
    result = runner.invoke(app, ["import", "labels", "list", "--output", "json"])
    assert result.exit_code == 0
    body = json.loads(result.stdout)["data"]
    counts = {entry["label"]: entry["usage_count"] for entry in body}
    assert counts["shared"] == 2
    assert counts["only-i1"] == 1


def test_import_labels_list_quiet_empty_keeps_scope_and_recovery(
    runner: CliRunner, db: Database
) -> None:
    """An empty labels read says which scope was queried and what can populate it."""
    result = runner.invoke(app, ["import", "labels", "list", "--quiet"])

    assert result.exit_code == 0, result.output
    assert "Import labels" in result.stdout
    assert "No labels" in result.stdout
    assert "moneybin import labels add" in result.stdout


def test_import_labels_list_pages_complete_answer_unless_no_pager_is_requested(
    runner: CliRunner,
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The global finite label inventory pages and --no-pager bypasses it."""
    from moneybin.cli import pager
    from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

    import_id = _allocate_import(db)
    ImportService(db).add_labels(
        import_id, [f"label-{index}" for index in range(20)], actor="cli"
    )
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
        "moneybin.cli.commands.import_labels.get_terminal_policy",
        lambda *, no_pager=False: policy,
    )
    paged: list[str] = []

    def capture_page(text: str, *, color: bool, wide: bool) -> bool:
        del color, wide
        paged.append(text)
        return True

    monkeypatch.setattr(pager, "page_text", capture_page)

    normal = runner.invoke(app, ["import", "labels", "list"])
    bypass = runner.invoke(app, ["import", "labels", "list", "--no-pager"])

    assert normal.exit_code == 0, normal.output
    assert bypass.exit_code == 0, bypass.output
    assert len(paged) == 1
    assert "label-19" in paged[0]
    assert "label-19" in bypass.stdout


def test_import_labels_add_invalid_slug_exits_1(
    runner: CliRunner, db: Database
) -> None:
    import_id = _allocate_import(db)
    result = runner.invoke(
        app, ["import", "labels", "add", import_id, "Invalid Label!"]
    )
    assert result.exit_code == 1

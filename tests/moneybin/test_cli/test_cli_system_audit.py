"""CLI tests for ``moneybin system audit`` (list, show)."""

from __future__ import annotations

import json
from collections.abc import Generator, Iterable, Sequence
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
from moneybin.database import Database
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import operation
from moneybin.services.transaction_service import TransactionService
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_cli._curation_helpers import make_curation_db, patch_db


@pytest.fixture()
def db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Database, None, None]:
    database = make_curation_db(tmp_path)
    patch_db(monkeypatch, database)
    yield database
    database.close()


def test_system_audit_list_filters_by_action(runner: CliRunner, db: Database) -> None:
    svc = TransactionService(db)
    svc.add_note("T1", "alpha", actor="cli")
    svc.add_tags("T1", ["food"], actor="cli")

    result = runner.invoke(
        app,
        ["system", "audit", "list", "--action", "note.%", "--output", "json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    events = payload["data"]
    assert events
    assert all(e["action"].startswith("note.") for e in events)


def test_system_audit_list_filter_by_target_id(runner: CliRunner, db: Database) -> None:
    # Row-grain: a note's audit row is keyed by note_id, so --target-id filters on
    # the entity PK (an exact-match filter), not the parent transaction_id.
    note = TransactionService(db).add_note("T1", "x", actor="cli")
    result = runner.invoke(
        app,
        ["system", "audit", "list", "--target-id", note.note_id, "--output", "json"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    events = payload["data"]
    assert events and all(e["target_id"] == note.note_id for e in events)


def test_system_audit_history_serializes_recovery_actions(
    runner: CliRunner, db: Database
) -> None:
    # OperationSummary.recovery_actions are Pydantic RecoveryAction models;
    # dataclasses.asdict leaves them unconverted, so JSON output must serialize
    # them explicitly. A blocked op (add then edit same note) carries one.
    svc = TransactionService(db)
    note = svc.add_note("T1", "v1", actor="cli")  # op1
    svc.edit_note(note.note_id, "v2", actor="cli")  # op2 edits same row → blocks op1

    result = runner.invoke(app, ["system", "audit", "history", "--output", "json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    ops = payload["data"]
    blocked = [o for o in ops if not o["can_undo"] and o.get("recovery_actions")]
    assert blocked, "expected a blocked op carrying recovery_actions"
    action = blocked[0]["recovery_actions"][0]
    assert action["tool"] == "system_audit_undo"
    assert "operation_id" in action["arguments"]


def test_system_audit_history_names_unblocked_refusals_not_undoable(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An already-undone operation has no blocking operation to name."""
    note = TransactionService(db).add_note("T1", "alpha", actor="cli")
    operation_id = next(
        iter(AuditService(db).list_events(target_id=note.note_id))
    ).operation_id
    undone = runner.invoke(app, ["system", "audit", "undo", operation_id, "--yes"])
    assert undone.exit_code == 0, undone.output

    rendered_rows: list[tuple[object, ...]] = []

    def capture_rows(
        _columns: Sequence[str], rows: Iterable[Sequence[object]], **_kwargs: object
    ) -> str:
        rendered_rows.extend(tuple(row) for row in rows)
        return "history"

    monkeypatch.setattr("moneybin.cli.commands.system.audit.build_rows", capture_rows)
    result = runner.invoke(app, ["system", "audit", "history", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert rendered_rows
    assert rendered_rows[0][-1] == "not undoable"


def test_system_audit_show_returns_chain(runner: CliRunner, db: Database) -> None:
    """rename_tag emits parent + child events; ``show`` returns both."""
    TransactionService(db).add_tags("T1", ["old"], actor="cli")
    rename = TransactionService(db).rename_tag("old", "new", actor="cli")

    result = runner.invoke(
        app,
        ["system", "audit", "show", rename.parent_audit_id, "--output", "json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    chain = payload["data"]
    actions = {e["action"] for e in chain}
    assert "tag.rename" in actions
    assert "tag.rename_row" in actions


def test_system_audit_show_unknown_id_exits_1(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(app, ["system", "audit", "show", "deadbeef"])
    assert result.exit_code == 1


def test_system_audit_show_missing_id_has_a_structured_json_error(
    runner: CliRunner, db: Database
) -> None:
    result = runner.invoke(
        app, ["system", "audit", "show", "deadbeef", "--output", "json"]
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "audit_identifier_not_found"


def test_system_audit_list_pages_one_complete_answer_and_no_pager_prints_it(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Removing the shared result boundary would lose rows or ignore --no-pager."""
    for index in range(4):
        TransactionService(db).add_note("T1", f"note {index}", actor="cli")
    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=1,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.system.audit.get_terminal_policy",
        lambda *, no_pager=False: policy,
    )
    pages: list[str] = []

    def capture_page(text: str, **_kwargs: object) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr("moneybin.cli.pager.page_text", capture_page)

    paged = runner.invoke(app, ["system", "audit", "list"])
    direct = runner.invoke(app, ["system", "audit", "list", "--no-pager"])

    assert paged.exit_code == direct.exit_code == 0
    assert pages and "note.add" in pages[0]
    assert (
        pages[0].replace("\n\nq return to shell\n", "").rstrip()
        == direct.stdout.rstrip()
    )


def test_system_audit_get_text_keeps_before_after_and_context_readable(
    runner: CliRunner, db: Database
) -> None:
    """Detail output must not regress to a Python representation wall."""
    note = TransactionService(db).add_note("T1", "alpha", actor="cli")
    event = next(iter(AuditService(db).list_events(target_id=note.note_id)))

    result = runner.invoke(
        app, ["system", "audit", "get", event.operation_id, "--no-pager"]
    )

    assert result.exit_code == 0, result.output
    assert "Before:" in result.stdout
    assert "After:" in result.stdout
    assert "{'" not in result.stdout


def test_system_audit_show_history_and_get_page_complete_answers(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each remaining finite audit leaf shares the pager and --no-pager contract."""
    note = TransactionService(db).add_note("T1", "alpha", actor="cli")
    event = next(iter(AuditService(db).list_events(target_id=note.note_id)))
    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=1,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.system.audit.get_terminal_policy",
        lambda *, no_pager=False: policy,
    )
    pages: list[str] = []

    def capture_page(text: str, **_kwargs: object) -> bool:
        pages.append(text)
        return True

    monkeypatch.setattr("moneybin.cli.pager.page_text", capture_page)
    invocations = [
        (["system", "audit", "show", event.audit_id], event.audit_id),
        (["system", "audit", "history"], "note.add"),
        (["system", "audit", "get", event.operation_id], event.audit_id),
    ]
    for invocation, expected_id in invocations:
        paged = runner.invoke(app, invocation)
        direct = runner.invoke(app, [*invocation, "--no-pager"])
        assert paged.exit_code == direct.exit_code == 0
        assert expected_id in pages[-1]
        assert expected_id in direct.stdout


def test_system_audit_undo_json_refuses_without_explicit_confirmation(
    runner: CliRunner, db: Database
) -> None:
    """A machine-readable undo cannot read a prompt as authorization."""
    note = TransactionService(db).add_note("T1", "alpha", actor="cli")
    operation_id = next(
        iter(AuditService(db).list_events(target_id=note.note_id))
    ).operation_id

    result = runner.invoke(
        app, ["system", "audit", "undo", operation_id, "--output", "json"]
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "mutation_confirmation_required"
    assert db.execute(
        "SELECT text FROM app.transaction_notes WHERE note_id = ?", [note.note_id]
    ).fetchone() == ("alpha",)


def test_system_audit_undo_yes_reports_the_saved_receipt_without_paging(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A successful mutation prints only its actual result; a pager must never run."""
    note = TransactionService(db).add_note("T1", "alpha", actor="cli")
    operation_id = next(
        iter(AuditService(db).list_events(target_id=note.note_id))
    ).operation_id

    def unexpected_page(text: str, *, color: bool, wide: bool) -> bool:
        del text, color, wide
        pytest.fail("mutation receipt must not page")

    monkeypatch.setattr(
        "moneybin.cli.pager.page_text",
        unexpected_page,
    )

    result = runner.invoke(app, ["system", "audit", "undo", operation_id, "--yes"])

    assert result.exit_code == 0, result.output
    assert f"Reversed operation {operation_id}" in result.stdout
    assert "Rows reversed: 1" in result.stdout
    assert db.execute("SELECT COUNT(*) FROM app.transaction_notes").fetchone() == (0,)


def test_system_audit_undo_preview_shows_source_scope_and_binds_all_audit_ids(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Markers/no-ops are selected for drift detection but excluded from the effect."""
    with operation() as operation_id:
        TransactionService(db).add_note("T1", "alpha", actor="cli")
        note_event = AuditService(db).list_events(action_pattern="note.add")[0]
        AuditService(db).record_audit_event(
            action="note.marker",
            target=("app", "transaction_notes", None),
            before=None,
            after={"marker": True},
            actor="cli",
        )
        AuditService(db).record_audit_event(
            action="note.noop",
            target=("app", "transaction_notes", note_event.target_id),
            before=note_event.after_value,
            after=note_event.after_value,
            actor="cli",
        )
    expected_ids = tuple(
        event.audit_id for event in UndoService(db).get(operation_id).events
    )
    received: list[tuple[str, ...] | None] = []
    original_undo = UndoService.undo

    def record_undo(
        self: UndoService,
        requested_operation_id: str,
        *,
        actor: str,
        expected_audit_ids: tuple[str, ...] | None = None,
    ) -> object:
        received.append(expected_audit_ids)
        return original_undo(
            self,
            requested_operation_id,
            actor=actor,
            expected_audit_ids=expected_audit_ids,
        )

    from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=24,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.system.audit.get_terminal_policy", lambda: policy
    )
    monkeypatch.setattr(UndoService, "undo", record_undo)

    result = runner.invoke(app, ["system", "audit", "undo", operation_id], input="y\n")

    assert result.exit_code == 0, result.output
    assert "Selected source events: 1" in result.stdout
    assert "app.transaction_notes" in result.stdout
    assert "Restore the displayed source-state images." in result.stdout
    assert received == [expected_ids]


def test_system_audit_undo_interactive_uses_service_marker_refusal(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A known refusal is reported before asking for a confirmation."""
    with operation() as operation_id:
        AuditService(db).record_audit_event(
            action="tag.rename",
            target=("app", "transaction_tags", None),
            before={"old_tag": "ghost"},
            after={"new_tag": "x", "row_count": 0},
            actor="cli",
        )
    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=24,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.system.audit.get_terminal_policy", lambda: policy
    )

    def unexpected_confirmation(*args: object, **kwargs: object) -> bool:
        del args, kwargs
        pytest.fail("known refusal must not prompt")

    monkeypatch.setattr(
        "moneybin.cli.commands.system.audit.typer.confirm", unexpected_confirmation
    )

    result = runner.invoke(app, ["system", "audit", "undo", operation_id])

    assert result.exit_code == 1
    assert "has no reversible row mutations (only marker events)" in result.output


def test_system_audit_undo_interactive_uses_service_noop_refusal(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A no-op refusal keeps its distinct recovery explanation before prompting."""
    same = json.dumps({"transaction_id": "T1", "tag": "trip"})
    db.execute(
        "INSERT INTO app.audit_log "
        "(audit_id, actor, action, target_schema, target_table, target_id, "
        "before_value, after_value, operation_id) "
        "VALUES ('noop_cli','cli','tag.add','app','transaction_tags','T1:trip', "
        "?, ?, 'op_noop_cli')",
        [same, same],
    )
    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=24,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.system.audit.get_terminal_policy", lambda: policy
    )

    def unexpected_confirmation(*args: object, **kwargs: object) -> bool:
        del args, kwargs
        pytest.fail("known refusal must not prompt")

    monkeypatch.setattr(
        "moneybin.cli.commands.system.audit.typer.confirm", unexpected_confirmation
    )

    result = runner.invoke(app, ["system", "audit", "undo", "op_noop_cli"])

    assert result.exit_code == 1
    assert "has no net effect to reverse (all captured rows show before == after)" in (
        result.output
    )

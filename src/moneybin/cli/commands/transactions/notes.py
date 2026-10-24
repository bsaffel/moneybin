"""Transaction notes subgroup: add, list, edit, delete.

Thin wrappers over ``TransactionService`` note methods.
"""

from __future__ import annotations

import logging

import typer

from moneybin import error_codes
from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import abort_cli_error, get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.privacy.payloads.transactions import (
    NoteDeletePayload,
    NotePayload,
    NotesListPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.transaction_service import Note

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Notes attached to transactions (multi-note threads)",
    no_args_is_help=True,
)


def _note_payload(n: Note) -> NotePayload:
    return NotePayload(
        note_id=n.note_id,
        transaction_id=n.transaction_id,
        text=n.text,
        author=n.author,
        created_at=n.created_at,
    )


@app.command("add")
def transactions_notes_add(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    text: str = typer.Argument(..., help="Note text"),
    output: OutputFormat = output_option,
) -> None:
    """Add a new note to a transaction."""
    from moneybin.services.transaction_service import TransactionService

    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                note = TransactionService(db).add_note(
                    transaction_id, text, actor="cli"
                )
    except ValueError as e:
        abort_cli_error(
            e, output=output, exit_code=1, cli_actor="transactions_notes_add"
        )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=_note_payload(note), sensitivity="low"),
            output,
            cli_actor="transactions_notes_add",
        )
        return
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Transaction", transaction_id), ("Note ID", note.note_id)],
                title="Note added",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


@app.command("list")
def transactions_notes_list(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """List all notes on a transaction."""
    from moneybin.services.transaction_service import TransactionService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            notes = TransactionService(db).list_notes(transaction_id)

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=NotesListPayload(notes=[_note_payload(n) for n in notes])
            ),
            output,
            cli_actor="transactions_notes_list",
        )
        return

    policy = get_terminal_policy(no_pager=no_pager)
    parts: list[object] = [
        build_summary([("Transaction", transaction_id)], title="Notes")
    ]
    if notes:
        parts.append(
            build_rows(
                ["note id", "created", "author", "note"],
                [
                    (note.note_id, note.created_at, note.author, note.text)
                    for note in notes
                ],
                terminal=policy,
            )
        )
    else:
        parts.append(build_summary([("Result", "No notes on this transaction.")]))
    emit_human_result(
        compose_human_result(
            parts,
            disclosures=(
                () if notes else ("Next: moneybin transactions notes add --help",)
            ),
        ),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("edit")
def transactions_notes_edit(
    note_id: str = typer.Argument(..., help="Note ID"),
    text: str = typer.Argument(..., help="New note text"),
    output: OutputFormat = output_option,
) -> None:
    """Edit an existing note's text."""
    from moneybin.services.transaction_service import TransactionService

    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                note = TransactionService(db).edit_note(note_id, text, actor="cli")
    except LookupError as e:
        abort_cli_error(
            e, output=output, exit_code=1, cli_actor="transactions_notes_edit"
        )
    except ValueError as e:
        abort_cli_error(
            e, output=output, exit_code=1, cli_actor="transactions_notes_edit"
        )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=_note_payload(note), sensitivity="low"),
            output,
            cli_actor="transactions_notes_edit",
        )
        return
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Note ID", note.note_id), ("Result", "updated")],
                title="Note updated",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


@app.command("delete")
def transactions_notes_delete(
    note_id: str = typer.Argument(..., help="Note ID"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
) -> None:
    """Delete a note."""
    from moneybin.services.transaction_service import TransactionService

    if not yes:
        if output == OutputFormat.JSON or not get_terminal_policy().interactive:
            abort_cli_error(
                UserError(
                    "Explicit confirmation is required.",
                    code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
                    hint="Re-run with --yes after reviewing the requested change.",
                ),
                output=output,
                exit_code=2,
                cli_actor="transactions_notes_delete",
                payload_type=NoteDeletePayload,
            )
        if not typer.confirm(f"Delete note {note_id}?"):
            emit_human_result(
                compose_human_result([
                    build_summary(
                        [("Saved state", "Note was not deleted")],
                        title="Note deletion cancelled",
                    )
                ]),
                policy=get_terminal_policy(),
                finite_read=False,
                receipt=True,
            )
            raise typer.Exit(0)

    try:
        with handle_cli_errors():
            with get_database(read_only=False) as db:
                TransactionService(db).delete_note(note_id, actor="cli")
    except LookupError as e:
        abort_cli_error(
            e, output=output, exit_code=1, cli_actor="transactions_notes_delete"
        )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=NoteDeletePayload(note_id=note_id), sensitivity="low"),
            output,
            cli_actor="transactions_notes_delete",
        )
        return
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Note ID", note_id), ("Result", "deleted")],
                title="Note deleted",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )

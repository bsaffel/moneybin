"""Transaction notes subgroup: add, list, edit, delete.

Thin wrappers over ``TransactionService`` note methods.
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.services.transaction_service import Note

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Notes attached to transactions (multi-note threads)",
    no_args_is_help=True,
)


def _note_to_dict(n: Note) -> dict[str, str]:
    return {
        "note_id": n.note_id,
        "transaction_id": n.transaction_id,
        "text": n.text,
        "author": n.author,
        "created_at": n.created_at,
    }


@app.command("add")
def transactions_notes_add(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    text: str = typer.Argument(..., help="Note text"),
    output: OutputFormat = output_option,
) -> None:
    """Add a new note to a transaction."""
    from moneybin.services.transaction_service import TransactionService

    try:
        with handle_cli_errors(output=output) as db:
            note = TransactionService(db).add_note(transaction_id, text, actor="cli")
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    payload = _note_to_dict(note)
    if output == OutputFormat.JSON:
        emit_json("note", payload)
        return
    logger.info(f"✅ Added note {note.note_id} to {transaction_id}")


@app.command("list")
def transactions_notes_list(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List all notes on a transaction."""
    from moneybin.services.transaction_service import TransactionService

    with handle_cli_errors(output=output) as db:
        notes = TransactionService(db).list_notes(transaction_id)

    payload = [_note_to_dict(n) for n in notes]
    if output == OutputFormat.JSON:
        emit_json("notes", payload)
        return

    if not notes:
        if not quiet:
            logger.info(f"No notes for {transaction_id}")
        return
    for n in notes:
        typer.echo(f"  [{n.note_id}] {n.created_at} {n.author}: {n.text}")


@app.command("edit")
def transactions_notes_edit(
    note_id: str = typer.Argument(..., help="Note ID"),
    text: str = typer.Argument(..., help="New note text"),
    output: OutputFormat = output_option,
) -> None:
    """Edit an existing note's text."""
    from moneybin.services.transaction_service import TransactionService

    try:
        with handle_cli_errors(output=output) as db:
            note = TransactionService(db).edit_note(note_id, text, actor="cli")
    except LookupError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    payload = _note_to_dict(note)
    if output == OutputFormat.JSON:
        emit_json("note", payload)
        return
    logger.info(f"✅ Updated note {note.note_id}")


@app.command("delete")
def transactions_notes_delete(
    note_id: str = typer.Argument(..., help="Note ID"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
) -> None:
    """Delete a note."""
    from moneybin.services.transaction_service import TransactionService

    if not yes:
        if not typer.confirm(f"Delete note {note_id}?"):
            logger.info("Cancelled")
            raise typer.Exit(0)

    try:
        with handle_cli_errors(output=output) as db:
            TransactionService(db).delete_note(note_id, actor="cli")
    except LookupError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        emit_json("note_delete", {"note_id": note_id, "deleted": True})
        return
    logger.info(f"✅ Deleted note {note_id}")

"""`moneybin import inbox` — drain, list, and locate the watched inbox."""

from __future__ import annotations

import dataclasses
import json
import logging
from typing import TYPE_CHECKING

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.database import DatabaseKeyError
from moneybin.services.inbox_service import InboxSyncResult

if TYPE_CHECKING:
    from moneybin.services.inbox_service import InboxService

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Drop files into the inbox and drain them into MoneyBin.",
    no_args_is_help=False,
)


def _build_service() -> InboxService:
    """Build an InboxService bound to the active profile."""
    from moneybin.config import get_settings
    from moneybin.database import get_database
    from moneybin.services.inbox_service import InboxService

    return InboxService(db=get_database(), settings=get_settings())


def _print_sync_text(result: InboxSyncResult) -> None:
    """Render a sync result as human-readable text."""
    processed = result.processed
    failed = result.failed
    skipped = result.skipped

    if skipped and any(s.get("reason") == "inbox_busy" for s in skipped):
        typer.echo("⚠️  Another sync is in progress; nothing done.", err=True)
        return

    for item in processed:
        typer.echo(
            f"✓ {item['filename']}  →  imported "
            f"({item.get('transactions', 0)} transactions)"
        )
    for item in failed:
        typer.echo(f"✗ {item['filename']}  →  failed ({item['error_code']})")
        if "sidecar" in item:
            typer.echo(f"   See {item['sidecar']}", err=True)

    typer.echo(f"Done: {len(processed)} imported, {len(failed)} failed.")


@app.callback(invoke_without_command=True)
def inbox_default(
    ctx: typer.Context,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Default action: drain the inbox."""
    if ctx.invoked_subcommand is not None:
        return
    try:
        service = _build_service()
        result = service.sync()
    except DatabaseKeyError as e:
        typer.echo(f"❌ {e}. Run 'moneybin db unlock'.", err=True)
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        typer.echo(json.dumps(dataclasses.asdict(result), default=str))
        return
    if quiet:
        return
    _print_sync_text(result)


@app.command("list")
def inbox_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show what a sync would do, without moving anything."""
    try:
        service = _build_service()
        result = service.enumerate()
    except DatabaseKeyError as e:
        typer.echo(f"❌ {e}. Run 'moneybin db unlock'.", err=True)
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        typer.echo(json.dumps(dataclasses.asdict(result), default=str))
        return
    if quiet:
        return
    for item in result.would_process:
        hint = f"  [{item['account_hint']}]" if item.get("account_hint") else ""
        typer.echo(f"  {item['filename']}{hint}")
    if not result.would_process:
        typer.echo("(inbox empty)")


@app.command("path")
def inbox_path() -> None:
    """Print the active profile's inbox parent directory."""
    service = _build_service()
    typer.echo(str(service.root))

"""`moneybin import inbox` — drain, list, and locate the watched inbox."""

from __future__ import annotations

import dataclasses
import logging
from typing import TYPE_CHECKING

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
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
    from moneybin.cli.utils import handle_cli_errors

    with handle_cli_errors():
        result = _build_service().sync()

    if output == OutputFormat.JSON:
        from moneybin.cli.utils import emit_json

        emit_json("sync", dataclasses.asdict(result))
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
    from moneybin.cli.utils import handle_cli_errors

    with handle_cli_errors():
        result = _build_service().enumerate()

    if output == OutputFormat.JSON:
        from moneybin.cli.utils import emit_json

        emit_json("list", dataclasses.asdict(result))
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
    from moneybin.cli.utils import handle_cli_errors

    with handle_cli_errors():
        typer.echo(str(_build_service().root))

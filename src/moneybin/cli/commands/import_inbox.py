"""`moneybin import inbox` — drain, list, and locate the watched inbox."""

from __future__ import annotations

import dataclasses
import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.services.inbox_service import InboxService, InboxSyncResult

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Drop files into the inbox and drain them into MoneyBin.",
    no_args_is_help=False,
)


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
        typer.echo(f"✗ {item['filename']}  →  failed ({item['error_code']})", err=True)
        if "sidecar" in item:
            typer.echo(f"   See {item['sidecar']}", err=True)

    typer.echo(f"Done: {len(processed)} imported, {len(failed)} failed.", err=True)


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
        result = InboxService.for_active_profile().sync()

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json
        from moneybin.protocol.envelope import build_envelope

        render_or_json(
            build_envelope(data=dataclasses.asdict(result), sensitivity="low"), output
        )
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
    # No handle_cli_errors(): for_active_profile_no_db() exists precisely so
    # this command works when the DB is locked or its key is unavailable.
    try:
        result = InboxService.for_active_profile_no_db().enumerate()
    except (OSError, ValueError) as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json
        from moneybin.protocol.envelope import build_envelope

        render_or_json(
            build_envelope(data=dataclasses.asdict(result), sensitivity="low"), output
        )
        return
    if quiet:
        return
    for item in result.would_process:
        hint = f"  [{item['account_hint']}]" if item.get("account_hint") else ""
        typer.echo(f"  {item['filename']}{hint}")
    if not result.would_process:
        typer.echo("(inbox empty)")


@app.command("path")
def inbox_path(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Print the active profile's inbox parent directory."""
    # No handle_cli_errors(): printing a path doesn't need the DB.
    try:
        service = InboxService.for_active_profile_no_db()
        # Materialize the layout so users can immediately copy files into
        # `$(moneybin import inbox path)/inbox/...` on a fresh profile.
        service.ensure_layout()
    except (OSError, ValueError) as e:
        logger.error(f"❌ {e}")
        raise typer.Exit(1) from e

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json
        from moneybin.protocol.envelope import build_envelope

        render_or_json(
            build_envelope(
                data={"path": str(service.root), "inbox": str(service.inbox_dir)},
                sensitivity="low",
            ),
            output,
        )
        return
    if quiet:
        return
    typer.echo(str(service.root))

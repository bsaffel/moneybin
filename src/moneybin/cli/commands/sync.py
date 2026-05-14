"""Data synchronization commands for MoneyBin CLI."""

import json
import logging
from contextlib import contextmanager

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors

from .stubs import _not_implemented

app = typer.Typer(
    help="Sync financial data from external services",
    no_args_is_help=True,
)
key_app = typer.Typer(
    help="Manage the sync server's encryption key",
    no_args_is_help=True,
)
app.add_typer(key_app, name="key")
logger = logging.getLogger(__name__)


def _build_sync_client():
    """Construct a SyncClient from current settings. Extracted for test mocking."""
    from moneybin.config import get_settings  # noqa: PLC0415
    from moneybin.connectors.sync_client import SyncClient  # noqa: PLC0415

    settings = get_settings()
    if settings.sync.server_url is None:
        raise ValueError(
            "sync.server_url is not configured. "
            "Set MONEYBIN_SYNC__SERVER_URL in your environment."
        )
    return SyncClient(server_url=str(settings.sync.server_url))


@contextmanager
def _build_sync_service():
    """Yield a SyncService with an active Database connection (per ADR-010)."""
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.loaders.plaid_loader import PlaidLoader  # noqa: PLC0415
    from moneybin.services.sync_service import SyncService  # noqa: PLC0415

    client = _build_sync_client()
    with get_database(read_only=False) as db:
        loader = PlaidLoader(db)
        yield SyncService(client=client, db=db, loader=loader)


@app.command("login")
def sync_login(
    no_browser: bool = typer.Option(
        False, "--no-browser", help="Print URL only; don't try to open a browser."
    ),
) -> None:
    """Authenticate with moneybin-server via Device Authorization Flow."""
    with handle_cli_errors():
        client = _build_sync_client()
        client.login(open_browser=not no_browser)
        typer.echo("✅ Logged in.")


@app.command("logout")
def sync_logout() -> None:
    """Clear stored JWT from keychain (or fallback file)."""
    with handle_cli_errors():
        client = _build_sync_client()
        client.logout()
        typer.echo("✅ Logged out.")


@app.command("connect")
def sync_connect() -> None:
    """Connect a bank account."""
    _not_implemented("sync-overview.md")


@app.command("disconnect")
def sync_disconnect() -> None:
    """Remove an institution."""
    _not_implemented("sync-overview.md")


@app.command("pull")
def sync_pull(
    institution: str | None = typer.Option(
        None, "--institution", help="Sync specific institution by name."
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Reset cursor and re-fetch full history."
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Pull data from connected institutions."""
    with handle_cli_errors():
        with _build_sync_service() as service:
            if not quiet and output == OutputFormat.TEXT:
                typer.echo("⚙️  Syncing… (this may take up to 2 minutes)")
            result = service.pull(institution=institution, force=force)

    if output == OutputFormat.JSON:
        typer.echo(result.model_dump_json(indent=2))
        return

    for inst in result.institutions:
        icon = "✅" if inst.status == "completed" else "❌"
        count = inst.transaction_count or 0
        typer.echo(f"{icon} {inst.institution_name}: {count} transactions")
        if inst.status == "failed" and inst.error_code:
            typer.echo(f"   💡 error: {inst.error_code}")
    completed = sum(1 for i in result.institutions if i.status == "completed")
    typer.echo(
        f"✅ Loaded {result.transactions_loaded} transactions from "
        f"{completed} institutions."
    )
    if result.transactions_removed:
        typer.echo(f"   Removed {result.transactions_removed} stale transactions.")


@app.command("status")
def sync_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — placeholder; nothing to suppress yet
) -> None:
    """Show connected institutions and sync health."""
    if output == OutputFormat.JSON:
        typer.echo(
            json.dumps(
                {"status": "not_implemented", "spec": "docs/specs/sync-overview.md"},
                indent=2,
            )
        )
        return
    _not_implemented("sync-overview.md")


@key_app.command("rotate")
def sync_key_rotate() -> None:
    """Rotate E2E encryption key pair."""
    _not_implemented("sync-overview.md")


# sync schedule subgroup
schedule_app = typer.Typer(help="Manage scheduled sync jobs")
app.add_typer(schedule_app, name="schedule")


@schedule_app.command("set")
def sync_schedule_set() -> None:
    """Install daily sync schedule."""
    _not_implemented("sync-overview.md")


@schedule_app.command("show")
def sync_schedule_show() -> None:
    """Show current schedule details."""
    _not_implemented("sync-overview.md")


@schedule_app.command("remove")
def sync_schedule_remove() -> None:
    """Uninstall scheduled sync job."""
    _not_implemented("sync-overview.md")

"""Data synchronization commands for MoneyBin CLI."""

import json
import logging
from typing import Annotated, Literal

import typer

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


def _not_implemented(owning_spec: str) -> None:
    """Print a not-implemented message.

    Args:
        owning_spec: The spec filename under docs/specs/ that owns this feature.
    """
    logger.info("This command is not yet implemented.")
    logger.info(f"💡 See docs/specs/{owning_spec} for the design")


@app.command("login")
def sync_login() -> None:
    """Authenticate with moneybin-server."""
    _not_implemented("sync-overview.md")


@app.command("logout")
def sync_logout() -> None:
    """Clear stored JWT from keychain."""
    _not_implemented("sync-overview.md")


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
    force: bool = typer.Option(False, "--force", "-f", help="Force full sync"),
    institution: str | None = typer.Option(
        None, "--institution", help="Sync specific institution"
    ),
) -> None:
    """Pull data from connected institutions."""
    _not_implemented("sync-overview.md")


@app.command("status")
def sync_status(
    output: Annotated[
        Literal["text", "json"],
        typer.Option("-o", "--output", help="Output format: text or json"),
    ] = "text",
    quiet: Annotated[  # noqa: ARG001 — placeholder; nothing to suppress yet
        bool,
        typer.Option("-q", "--quiet", help="Suppress informational output"),
    ] = False,
) -> None:
    """Show connected institutions and sync health."""
    if output == "json":
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

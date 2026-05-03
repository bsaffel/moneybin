"""CLI commands for the v2 accounts namespace.

Owns:
  - Entity ops (list/show) — this spec
  - Balance subcommands (balance show/history/assert/list/delete/reconcile) —
    contributed by net-worth.md, also live in this module (added in Phase 7)

Per-spec ownership: see docs/specs/account-management.md and docs/specs/net-worth.md.
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.services.account_service import AccountService

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Account listing, settings, and lifecycle ops",
    no_args_is_help=True,
)


@app.command("list")
def list_cmd(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — list has no informational chatter; only data
    include_archived: bool = typer.Option(
        False, "--include-archived", help="Include archived accounts in the listing"
    ),
    type_filter: str | None = typer.Option(
        None,
        "--type",
        help="Filter by account_type or account_subtype (case-insensitive)",
    ),
) -> None:
    """List accounts. Hides archived accounts by default."""
    with handle_cli_errors() as db:
        result = AccountService(db).list_accounts(
            include_archived=include_archived, type_filter=type_filter
        )
    if output == OutputFormat.JSON:
        emit_json("data", result.accounts)
        return
    for acct in result.accounts:
        display = acct.get("display_name") or acct.get("account_id")
        institution = acct.get("institution_name", "")
        acct_type = acct.get("account_type", "")
        typer.echo(f"  {display}  [{institution}]  {acct_type}")


@app.command("show")
def show_cmd(
    account_id: str = typer.Argument(..., help="Account ID"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show one account's full settings + dim record."""
    with handle_cli_errors() as db:
        record = AccountService(db).get_account(account_id)
    if record is None:
        logger.error(f"❌ Account not found: {account_id}")
        raise typer.Exit(1)
    if output == OutputFormat.JSON:
        emit_json("account", record)
        return
    for k, v in record.items():
        typer.echo(f"  {k}: {v}")


@app.command("rename")
def rename_cmd(
    account_id: str = typer.Argument(..., help="Account ID"),
    display_name: str = typer.Argument(
        ..., help="New display name (empty string clears)"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),  # noqa: ARG001 — reserved for future confirmation prompt
) -> None:
    """Rename an account. Empty string clears the override."""
    with handle_cli_errors() as db:
        result = AccountService(db).rename(account_id, display_name)
    name = result.display_name or "<cleared>"
    typer.echo(f"✅ Renamed {account_id} → {name}", err=True)


@app.command("include")
def include_cmd(
    account_id: str = typer.Argument(..., help="Account ID"),
    no: bool = typer.Option(False, "--no", help="Set include_in_net_worth=FALSE"),
    yes: bool = typer.Option(False, "--yes", "-y"),  # noqa: ARG001 — reserved for future confirmation prompt
) -> None:
    """Toggle account inclusion in net worth (default TRUE; --no to exclude)."""
    include = not no
    with handle_cli_errors() as db:
        result = AccountService(db).set_include_in_net_worth(account_id, include)
    state = "included in" if result.include_in_net_worth else "excluded from"
    typer.echo(f"✅ Account {account_id} {state} net worth", err=True)


@app.command("archive")
def archive_cmd(
    account_id: str = typer.Argument(..., help="Account ID"),
    yes: bool = typer.Option(False, "--yes", "-y"),  # noqa: ARG001 — reserved for future confirmation prompt
) -> None:
    """Archive an account. Cascades exclude_from_net_worth in the same write."""
    with handle_cli_errors() as db:
        AccountService(db).archive(account_id)
    typer.echo(
        f"✅ Archived account {account_id} (also excluded from net worth)",
        err=True,
    )


@app.command("unarchive")
def unarchive_cmd(
    account_id: str = typer.Argument(..., help="Account ID"),
    yes: bool = typer.Option(False, "--yes", "-y"),  # noqa: ARG001 — reserved for future confirmation prompt
) -> None:
    """Unarchive an account. Does NOT restore include_in_net_worth."""
    with handle_cli_errors() as db:
        result = AccountService(db).unarchive(account_id)
    if not result.include_in_net_worth:
        typer.echo(
            f"✅ Unarchived account {account_id} "
            f"(still excluded from net worth — use 'moneybin accounts include' to re-enable)",
            err=True,
        )
    else:
        typer.echo(f"✅ Unarchived account {account_id}", err=True)

"""CLI commands for the v2 accounts namespace.

Owns:
  - Entity ops (list/show) — this spec
  - Balance subcommands (balance show/history/assert/list/delete/reconcile) —
    contributed by net-worth.md, also live in this module (added in Phase 7)

Per-spec ownership: see docs/specs/account-management.md and docs/specs/net-worth.md.
"""

from __future__ import annotations

import logging
import sys
from decimal import Decimal

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.services.account_service import (
    CLEAR,
    AccountService,
    is_canonical_holder_category,
    is_canonical_subtype,
    suggest_holder_category,
    suggest_subtype,
)

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


def _maybe_prompt_soft_validation(
    field_name: str,
    value: str,
    is_canonical: bool,
    suggestion: str | None,
    yes: bool,
) -> bool:
    """Return True if the write should proceed.

    TTY mode: print warning and prompt for confirmation.
    Non-TTY mode without --yes: refuse with warning (exit-2 caller).
    Either mode with --yes: print warning, proceed.
    """
    if is_canonical:
        return True
    msg = f"⚠️  '{value}' is not a known {field_name}"
    if suggestion:
        msg += f" (did you mean '{suggestion}'?)"
    if yes:
        typer.echo(msg, err=True)
        return True
    if sys.stdin.isatty():
        typer.echo(msg, err=True)
        return typer.confirm("Proceed anyway?", default=False)
    # Non-TTY without --yes: refuse.
    typer.echo(msg, err=True)
    typer.echo(
        "Refusing to write a non-canonical value in non-interactive mode without --yes.",
        err=True,
    )
    return False


@app.command("set")
def set_cmd(
    account_id: str = typer.Argument(..., help="Account ID"),
    official_name: str | None = typer.Option(
        None, "--official-name", help="Institution's formal account name"
    ),
    last_four: str | None = typer.Option(
        None, "--last-four", help="Last 4 digits of account number"
    ),
    subtype: str | None = typer.Option(
        None,
        "--subtype",
        help="Plaid-style account subtype (e.g., checking, savings, credit card)",
    ),
    holder_category: str | None = typer.Option(
        None, "--holder-category", help="Account holder type (personal/business/joint)"
    ),
    currency: str | None = typer.Option(
        None, "--currency", help="ISO-4217 currency code (e.g., USD)"
    ),
    credit_limit: float | None = typer.Option(
        None, "--credit-limit", help="Credit limit (for credit cards / lines)"
    ),
    clear_official_name: bool = typer.Option(False, "--clear-official-name"),
    clear_last_four: bool = typer.Option(False, "--clear-last-four"),
    clear_subtype: bool = typer.Option(False, "--clear-subtype"),
    clear_holder_category: bool = typer.Option(False, "--clear-holder-category"),
    clear_currency: bool = typer.Option(False, "--clear-currency"),
    clear_credit_limit: bool = typer.Option(False, "--clear-credit-limit"),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip soft-validation prompt for non-canonical values",
    ),
) -> None:
    """Bulk update structural metadata fields. At least one --field flag required."""
    diff: dict[str, object] = {}

    def _add(field: str, value: object | None, clear: bool) -> None:
        if clear:
            diff[field] = CLEAR
        elif value is not None:
            diff[field] = value

    _add("official_name", official_name, clear_official_name)
    _add("last_four", last_four, clear_last_four)
    _add("account_subtype", subtype, clear_subtype)
    _add("holder_category", holder_category, clear_holder_category)
    _add("iso_currency_code", currency, clear_currency)
    _add(
        "credit_limit",
        Decimal(str(credit_limit)) if credit_limit is not None else None,
        clear_credit_limit,
    )

    if not diff:
        typer.echo(
            "error: at least one --field flag is required (or use --clear-FIELD)",
            err=True,
        )
        raise typer.Exit(2)

    # Soft-validation BEFORE writing
    if "account_subtype" in diff and isinstance(diff["account_subtype"], str):
        ok = _maybe_prompt_soft_validation(
            "Plaid subtype",
            diff["account_subtype"],
            is_canonical_subtype(diff["account_subtype"]),
            suggest_subtype(diff["account_subtype"]),
            yes,
        )
        if not ok:
            raise typer.Exit(2)
    if "holder_category" in diff and isinstance(diff["holder_category"], str):
        ok = _maybe_prompt_soft_validation(
            "holder category",
            diff["holder_category"],
            is_canonical_holder_category(diff["holder_category"]),
            suggest_holder_category(diff["holder_category"]),
            yes,
        )
        if not ok:
            raise typer.Exit(2)

    with handle_cli_errors() as db:
        AccountService(db).settings_update(account_id, **diff)  # type: ignore[arg-type]  # dynamic settings_update kwargs
    typer.echo(
        f"✅ Updated settings for {account_id}: fields={sorted(diff.keys())}",
        err=True,
    )

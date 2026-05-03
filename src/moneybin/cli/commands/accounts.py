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
from datetime import date as _date
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
from moneybin.services.balance_service import BalanceService

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


# ---------------------------------------------------------------------------
# Balance sub-app
# ---------------------------------------------------------------------------

balance_app = typer.Typer(
    help="Balance assertions, history, and reconciliation",
    no_args_is_help=True,
)
app.add_typer(balance_app, name="balance")


@balance_app.command("show")
def balance_show_cmd(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — show has no informational chatter
    account: str | None = typer.Option(
        None, "--account", help="Filter to a single account_id"
    ),
    as_of: str | None = typer.Option(
        None, "--as-of", help="ISO date (YYYY-MM-DD); shows balance on or before"
    ),
) -> None:
    """Show current or as-of balances per account."""
    as_of_date = _date.fromisoformat(as_of) if as_of else None
    account_ids = [account] if account else None
    with handle_cli_errors() as db:
        observations = BalanceService(db).current_balances(
            account_ids=account_ids, as_of_date=as_of_date
        )
    if output == OutputFormat.JSON:
        emit_json("balances", [o.to_dict() for o in observations])
        return
    for obs in observations:
        d = obs.to_dict()
        typer.echo(
            f"  {d['account_id']}  {d['balance_date']}  {d['balance']}"
            f"  observed={d['is_observed']}  source={d['observation_source']}"
            f"  delta={d['reconciliation_delta']}"
        )


@balance_app.command("history")
def balance_history_cmd(
    account: str = typer.Option(..., "--account", help="Account ID (required)"),
    from_date: str | None = typer.Option(None, "--from"),
    to_date: str | None = typer.Option(None, "--to"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — history has no informational chatter
) -> None:
    """Per-account balance history (daily series)."""
    from_d = _date.fromisoformat(from_date) if from_date else None
    to_d = _date.fromisoformat(to_date) if to_date else None
    with handle_cli_errors() as db:
        observations = BalanceService(db).history(
            account, from_date=from_d, to_date=to_d
        )
    if output == OutputFormat.JSON:
        emit_json("history", [o.to_dict() for o in observations])
        return
    for obs in observations:
        d = obs.to_dict()
        typer.echo(
            f"  {d['balance_date']}  {d['balance']}"
            f"  observed={d['is_observed']}  source={d['observation_source']}"
            f"  delta={d['reconciliation_delta']}"
        )


@balance_app.command("assert")
def balance_assert_cmd(
    account_id: str = typer.Argument(...),
    assertion_date: str = typer.Argument(..., help="ISO date (YYYY-MM-DD)"),
    amount: str = typer.Argument(..., help="Balance amount as decimal"),
    notes: str | None = typer.Option(None, "--notes"),
    yes: bool = typer.Option(False, "--yes", "-y"),  # noqa: ARG001 — reserved for future confirmation prompt
) -> None:
    """Assert a balance for an account on a specific date."""
    parsed_date = _date.fromisoformat(assertion_date)
    parsed_amount = Decimal(amount)
    with handle_cli_errors() as db:
        result = BalanceService(db).assert_balance(
            account_id=account_id,
            assertion_date=parsed_date,
            balance=parsed_amount,
            notes=notes,
        )
    typer.echo(
        f"✅ Asserted balance for {account_id} on {parsed_date}: {result.balance}",
        err=True,
    )


@balance_app.command("list")
def balance_list_cmd(
    account: str | None = typer.Option(None, "--account"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — list has no informational chatter
) -> None:
    """List balance assertions, optionally filtered by account."""
    with handle_cli_errors() as db:
        assertions = BalanceService(db).list_assertions(account)
    if output == OutputFormat.JSON:
        emit_json("assertions", [a.to_dict() for a in assertions])
        return
    for assertion in assertions:
        d = assertion.to_dict()
        typer.echo(
            f"  {d['account_id']}  {d['assertion_date']}  {d['balance']}  notes={d['notes']}"
        )


@balance_app.command("delete")
def balance_delete_cmd(
    account_id: str = typer.Argument(...),
    assertion_date: str = typer.Argument(..., help="ISO date (YYYY-MM-DD)"),
    yes: bool = typer.Option(False, "--yes", "-y"),  # noqa: ARG001 — reserved for future confirmation prompt
) -> None:
    """Delete a balance assertion. Silent no-op if no row exists."""
    parsed_date = _date.fromisoformat(assertion_date)
    with handle_cli_errors() as db:
        BalanceService(db).delete_assertion(account_id, parsed_date)
    typer.echo(
        f"✅ Deleted balance assertion for {account_id} on {parsed_date}",
        err=True,
    )


@balance_app.command("reconcile")
def balance_reconcile_cmd(
    account: str | None = typer.Option(None, "--account"),
    threshold: str = typer.Option("0.01", "--threshold"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — reconcile has no informational chatter
) -> None:
    """Show observed balance days with non-zero reconciliation delta."""
    parsed_threshold = Decimal(threshold)
    account_ids = [account] if account else None
    with handle_cli_errors() as db:
        observations = BalanceService(db).reconcile(
            account_ids=account_ids, threshold=parsed_threshold
        )
    if output == OutputFormat.JSON:
        emit_json("reconcile", [o.to_dict() for o in observations])
        return
    for obs in observations:
        d = obs.to_dict()
        typer.echo(
            f"  {d['account_id']}  {d['balance_date']}  {d['balance']}"
            f"  source={d['observation_source']}  delta={d['reconciliation_delta']}"
        )

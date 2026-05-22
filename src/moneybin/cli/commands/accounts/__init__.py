"""Accounts top-level command group.

Owns account entity operations (list, get, set, resolve) and per-account
workflows (balance, investments) per moneybin-cli.md v2 +
account-management.md. `set` is the single partial-update entry point —
display_name, include_in_net_worth, and is_archived fold in via flags
(see `accounts set --help`).
"""

from __future__ import annotations

import dataclasses
import logging
import sys
from collections.abc import Callable
from decimal import Decimal

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import emit_json as emit_json
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.accounts import (
    AccountResolvePayload as AccountResolvePayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.account_service import (
    CLEAR,
    AccountService,
    is_canonical_holder_category,
    is_canonical_subtype,
    suggest_holder_category,
    suggest_subtype,
)
from moneybin.services.balance_service import (
    BalanceService,  # noqa: F401 — re-exported for patch targets in tests  # type: ignore[reportUnusedImport]
)

from . import balance, investments

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Account listing, settings, and lifecycle ops",
    no_args_is_help=True,
)


@app.command("list")
def accounts_list(
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
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            result = AccountService(db).list_accounts(
                include_archived=include_archived, type_filter=type_filter
            )
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=result, sensitivity="medium"),
            output,
            cli_actor="accounts_list",
        )
        return
    for acct in result.rows:
        display = acct.display_name or acct.account_id
        institution = acct.institution_name or ""
        acct_type = acct.account_type
        typer.echo(f"  {display}  [{institution}]  {acct_type}")


@app.command("get")
def accounts_get(
    account_id: str = typer.Argument(..., help="Account ID"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show one account's full settings + dim record."""
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            record = AccountService(db).get_account(account_id)
    if record is None:
        logger.error(f"❌ Account not found: {account_id}")
        raise typer.Exit(1)
    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=record, sensitivity="medium"),
            output,
            cli_actor="accounts_get",
        )
        return
    for k, v in dataclasses.asdict(record).items():
        typer.echo(f"  {k}: {v}")


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


_SOFT_VALIDATED_FIELDS: dict[
    str, tuple[str, Callable[[str], bool], Callable[[str], str | None]]
] = {
    "account_subtype": ("Plaid subtype", is_canonical_subtype, suggest_subtype),
    "holder_category": (
        "holder category",
        is_canonical_holder_category,
        suggest_holder_category,
    ),
}


@app.command("set")
def accounts_set(
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
    credit_limit: str | None = typer.Option(
        None, "--credit-limit", help="Credit limit (for credit cards / lines)"
    ),
    display_name: str | None = typer.Option(
        None,
        "--display-name",
        help="Custom display name override (use --clear-display-name to clear)",
    ),
    include_in_net_worth: bool | None = typer.Option(
        None,
        "--include/--exclude",
        help="Include or exclude this account from net worth",
    ),
    is_archived: bool | None = typer.Option(
        None,
        "--archive/--unarchive",
        help="Archive (cascades --exclude) or unarchive (does not auto-restore include)",
    ),
    clear_official_name: bool = typer.Option(False, "--clear-official-name"),
    clear_last_four: bool = typer.Option(False, "--clear-last-four"),
    clear_subtype: bool = typer.Option(False, "--clear-subtype"),
    clear_holder_category: bool = typer.Option(False, "--clear-holder-category"),
    clear_currency: bool = typer.Option(False, "--clear-currency"),
    clear_credit_limit: bool = typer.Option(False, "--clear-credit-limit"),
    clear_display_name: bool = typer.Option(False, "--clear-display-name"),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip soft-validation prompt for non-canonical values",
    ),
) -> None:
    """Update account settings (structural + behavioral fields).

    Structural: --official-name, --last-four, --subtype, --holder-category,
    --currency, --credit-limit (each clearable via --clear-FIELD).
    Behavioral: --display-name, --include/--exclude, --archive/--unarchive.
    Archive cascades --exclude in the same write; unarchive does NOT restore
    include. At least one field flag required.
    """
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
    _add("display_name", display_name, clear_display_name)
    if include_in_net_worth is not None:
        diff["include_in_net_worth"] = include_in_net_worth
    if is_archived is not None:
        diff["archived"] = is_archived

    if not diff and credit_limit is None and not clear_credit_limit:
        typer.echo(
            "error: at least one --field flag is required (or use --clear-FIELD)",
            err=True,
        )
        raise typer.Exit(2)

    # Soft-validation BEFORE writing
    for field_key, (
        label,
        is_canonical_fn,
        suggest_fn,
    ) in _SOFT_VALIDATED_FIELDS.items():
        value = diff.get(field_key)
        if isinstance(value, str):
            ok = _maybe_prompt_soft_validation(
                label, value, is_canonical_fn(value), suggest_fn(value), yes
            )
            if not ok:
                raise typer.Exit(2)

    with handle_cli_errors():
        with get_database() as db:
            # Decimal conversion inside the handler so InvalidOperation surfaces
            # via classify_user_error rather than as a raw traceback.
            _add(
                "credit_limit",
                Decimal(credit_limit) if credit_limit is not None else None,
                clear_credit_limit,
            )
            _, warnings = AccountService(db).settings_update(account_id, **diff)  # type: ignore[arg-type]  # dynamic settings_update kwargs
    for w in warnings:
        typer.echo(f"⚠️  {w.get('message', w)}", err=True)
    cascade_note = " (also excluded from net worth)" if is_archived is True else ""
    typer.echo(
        f"✅ Updated settings for {account_id}: fields={sorted(diff.keys())}{cascade_note}",
        err=True,
    )


@app.command("resolve")
def accounts_resolve(
    query: str = typer.Argument(
        ..., help="Free-text account reference (e.g., 'my Chase account')"
    ),
    limit: int = typer.Option(
        5,
        "--limit",
        "-n",
        min=1,
        help="Maximum number of candidates to return",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Resolve a free-text account reference to ranked account_id candidates.

    Fuzzy-matches against display_name, account_subtype, and institution_name.
    Use this before commands that need an account_id when you only have a
    natural-language reference.
    """
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            payload = AccountService(db).resolve(query=query, limit=limit)

    if output == OutputFormat.JSON:
        # No explicit sensitivity: AccountResolvePayload carries
        # ACCOUNT_IDENTIFIER (CRITICAL), and render_or_json derives the real
        # tier via _derive_log_sensitivity. A literal "low" here understates it
        # at the call site even though the emitted value is corrected.
        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="accounts_resolve",
        )
        return

    if not payload.matches:
        if not quiet:
            typer.echo(f"No accounts matched '{query}'.", err=True)
        return
    for m in payload.matches:
        subtype = m.account_subtype or "-"
        institution = m.institution_name or "-"
        typer.echo(
            f"{m.account_id}\t{m.display_name}\t{subtype}\t{institution}\t"
            f"{round(m.confidence, 3):.3f}"
        )


app.add_typer(balance.app, name="balance")
app.add_typer(investments.app, name="investments")

"""Accounts top-level command group.

Owns account entity operations (list, get, set, resolve) and per-account
workflows (balance, links) per moneybin-cli.md v2 + account-management.md.
`set` is the single partial-update entry point — display_name,
include_in_net_worth, and is_archived fold in via flags (see
`accounts set --help`). Investment holdings live under the top-level
`investments` group (see `cli/commands/investments.py`), not here.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from decimal import Decimal

import typer

from moneybin.cli.output import (
    OutputFormat,
    currency_label,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import (
    build_rows,
    build_summary,
    compose_human_result,
    render_note,
)
from moneybin.cli.utils import (
    abort_cli_error,
    format_cli_attention,
    generated_cli_command,
    get_terminal_policy,
    handle_cli_errors,
)
from moneybin.database import get_database
from moneybin.privacy.payloads.accounts import (
    AccountDetail,
    AccountListPayload,
    AccountResolvePayload,
    AccountSummary,
    AccountSummaryStats,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.account_resolution_types import (
    UNNAMED_ACCOUNT_LABEL,
    is_a_name,
)
from moneybin.services.account_service import (
    CLEAR,
    AccountService,
    is_canonical_holder_category,
    is_canonical_subtype,
    normalize_setting_text,
    suggest_holder_category,
    suggest_subtype,
)
from moneybin.services.balance_service import (
    BalanceService,  # noqa: F401  # re-exported for patch targets in tests  # type: ignore[reportUnusedImport]
)

from . import balance, links

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Account listing, settings, and lifecycle ops",
    no_args_is_help=True,
)


@app.command("list")
def accounts_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # list has no informational chatter; only data
    include_archived: bool = typer.Option(
        False, "--include-archived", help="Include archived accounts in the listing"
    ),
    type_filter: str | None = typer.Option(
        None,
        "--type",
        help=(
            "Filter by account_type (canonical: depository, credit, loan, "
            "investment, other) or account_subtype (source detail: checking, "
            "savings, credit card, ...); case-insensitive"
        ),
    ),
    no_pager: bool = no_pager_option,
) -> None:
    """List accounts. Hides archived accounts by default."""
    with handle_cli_errors(cli_actor="accounts_list", payload_type=AccountListPayload):
        with get_database(read_only=True) as db:
            result = AccountService(db).list_accounts(
                include_archived=include_archived, type_filter=type_filter
            )
    if output == OutputFormat.JSON:
        # No explicit sensitivity: AccountSummary carries ACCOUNT_IDENTIFIER
        # (CRITICAL); render_or_json derives the real tier. A literal "medium"
        # understates it at the call site.
        render_or_json(
            build_envelope(data=result),
            output,
            cli_actor="accounts_list",
        )
        return

    def _display(acct: AccountSummary) -> str:
        """The account's name, or the shared label when it has none.

        The id no longer rides this cell. Requirement 26 gives it a column on
        every row, which serves the same end more evenly: `set` takes an id,
        and a display name is not unique — two cards at one institution
        routinely share one, so a named row needs its selector just as much as
        a nameless one did.
        """
        if is_a_name(acct.display_name):
            return acct.display_name
        return UNNAMED_ACCOUNT_LABEL

    if result.rows:
        policy = get_terminal_policy(no_pager=no_pager)
        human = build_rows(
            # `account_id` is named identically in `transactions list` and
            # holds equal values, so the two outputs join on it (requirement
            # 28). That shared key is the whole of the fix; the display name
            # beside it is an ergonomic extra.
            ["account", "account_id", "institution", "type"],
            [
                (
                    _display(acct),
                    acct.account_id,
                    acct.institution_name or "",
                    acct.account_type or "",
                )
                for acct in result.rows
            ],
            terminal=policy,
        )
        emit_human_result(
            human,
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )


@app.command("summary")
def accounts_summary(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Summarize account counts, lifecycle state, and recent activity."""
    with handle_cli_errors(
        cli_actor="accounts_summary", payload_type=AccountSummaryStats
    ):
        with get_database(read_only=True) as db:
            result = AccountService(db).summary()

    def _render_text(_: object) -> None:
        emit_human_result(
            compose_human_result([
                build_summary(
                    [
                        ("Accounts", str(result.total_accounts)),
                        ("Archived", str(result.count_archived)),
                        (
                            "Excluded from net worth",
                            str(result.count_excluded_from_net_worth),
                        ),
                        (
                            "With recent activity",
                            str(result.count_with_recent_activity),
                        ),
                    ],
                    title="Accounts",
                )
            ]),
            policy=get_terminal_policy(no_pager=no_pager),
            finite_read=True,
            no_pager=no_pager,
        )

    render_or_json(
        build_envelope(data=result),
        output,
        render_fn=_render_text,
        cli_actor="accounts_summary",
    )


@app.command("get")
def accounts_get(
    account_id: str = typer.Argument(..., help="Account ID"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Show one account's identity and lifecycle summary."""
    with handle_cli_errors(cli_actor="accounts_get", payload_type=AccountDetail):
        with get_database(read_only=True) as db:
            record = AccountService(db).get_account(account_id)
    if record is None:
        abort_cli_error(
            LookupError(f"Account not found: {account_id}"),
            output=output,
            exit_code=1,
            cli_actor="accounts_get",
            payload_type=AccountDetail,
        )
    if output == OutputFormat.JSON:
        # AccountDetail carries CRITICAL fields; render_or_json derives the tier.
        render_or_json(
            build_envelope(data=record),
            output,
            cli_actor="accounts_get",
        )
        return
    pairs = [
        ("Account ID", record.account_id),
        ("Name", record.display_name or UNNAMED_ACCOUNT_LABEL),
        ("Institution", record.institution_name or "-"),
        ("Type", record.account_type or "-"),
        ("Subtype", record.account_subtype or "-"),
        ("Currency", currency_label(record.currency_code)),
        ("Included in net worth", "yes" if record.include_in_net_worth else "no"),
        ("Status", "archived" if record.archived else "active"),
    ]
    emit_human_result(
        compose_human_result([build_summary(pairs, title="Account")]),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )


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
    msg = format_cli_attention(f"'{value}' is not a known {field_name}")
    if suggestion:
        msg += f" (did you mean '{suggestion}'?)"
    if yes:
        typer.echo(msg, err=True)
        return True
    if get_terminal_policy().interactive:
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
    default_cost_basis_method: str | None = typer.Option(
        None,
        "--default-cost-basis-method",
        help="Per-account cost-basis default: fifo, hifo, specific, or average "
        "(NULL falls back to the global FIFO default)",
    ),
    include_in_net_worth: bool | None = typer.Option(
        None,
        "--include/--exclude",
        help="Include or exclude this account from net worth",
    ),
    is_archived: bool | None = typer.Option(
        None,
        "--archive/--unarchive",
        help="Archive or unarchive this account (does not change --include/--exclude)",
    ),
    clear_official_name: bool = typer.Option(False, "--clear-official-name"),
    clear_last_four: bool = typer.Option(False, "--clear-last-four"),
    clear_subtype: bool = typer.Option(False, "--clear-subtype"),
    clear_holder_category: bool = typer.Option(False, "--clear-holder-category"),
    clear_currency: bool = typer.Option(False, "--clear-currency"),
    clear_credit_limit: bool = typer.Option(False, "--clear-credit-limit"),
    clear_display_name: bool = typer.Option(False, "--clear-display-name"),
    clear_default_cost_basis_method: bool = typer.Option(
        False, "--clear-default-cost-basis-method"
    ),
    yes: bool = typer.Option(
        False,
        "--yes",
        "-y",
        help="Skip soft-validation prompt for non-canonical values",
    ),
) -> None:
    """Update account settings (structural + behavioral fields).

    Structural: --official-name, --last-four, --subtype, --holder-category,
    --currency, --credit-limit, --default-cost-basis-method (each clearable
    via --clear-FIELD). --default-cost-basis-method must be one of fifo,
    hifo, specific, average — an invalid value is rejected before any write.
    Behavioral: --display-name, --include/--exclude, --archive/--unarchive.
    --archive/--unarchive and --include/--exclude are independent flags with
    different jobs. --archive today excludes the account from net worth
    entirely, history included — the archive date is recorded so a future
    release can make that exclusion date-scoped instead of retroactive, but
    no report reads it that way yet. Use --exclude to exclude an account
    from net worth regardless of its archived status, including one that
    stays active and listed. At least one field flag required.
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
    _add("currency_code", currency, clear_currency)
    _add("display_name", display_name, clear_display_name)
    _add(
        "default_cost_basis_method",
        default_cost_basis_method,
        clear_default_cost_basis_method,
    )
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

    # Ask about the spelling the service will store. Settings are trimmed on
    # the way in, and a non-TTY run without --yes refuses anything
    # non-canonical outright, so checking the raw flag turns one stray space
    # into a hard refusal of a subtype MoneyBin recognizes.
    for field_key in _SOFT_VALIDATED_FIELDS:
        pending = diff.get(field_key)
        if isinstance(pending, str):
            diff[field_key] = normalize_setting_text(pending)

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
        with get_database(read_only=False) as db:
            # Decimal conversion inside the handler so InvalidOperation surfaces
            # via classify_user_error rather than as a raw traceback.
            _add(
                "credit_limit",
                Decimal(credit_limit) if credit_limit is not None else None,
                clear_credit_limit,
            )
            _, warnings = AccountService(db).settings_update(
                account_id,
                actor="cli",
                **diff,  # type: ignore[arg-type]  # dynamic settings_update kwargs
            )
    emit_human_result(
        compose_human_result(
            [
                build_summary(
                    [
                        ("Account ID", account_id),
                        ("Updated fields", ", ".join(sorted(diff))),
                    ],
                    title="Account settings updated",
                )
            ],
            disclosures=[str(w.get("message", w)) for w in warnings],
        ),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )
    if "currency_code" in diff:
        # Rule 34: `core.dim_accounts` is a FULL SQLMesh model reading
        # `app.account_settings`, so a currency change is invisible to
        # reports until the next `refresh --step transform` — same claim,
        # same remedy, as `accounts balance assert`.
        render_note(
            format_cli_attention(
                "Reports read this after a rebuild: "
                f"{generated_cli_command('refresh', '--step', 'transform')}"
            ),
            warn=True,
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
    no_pager: bool = no_pager_option,
) -> None:
    """Resolve a free-text account reference to ranked account_id candidates.

    Fuzzy-matches against display_name, account_subtype, and institution_name.
    Use this before commands that need an account_id when you only have a
    natural-language reference.
    """
    with handle_cli_errors(
        cli_actor="accounts_resolve", payload_type=AccountResolvePayload
    ):
        with get_database(read_only=True) as db:
            payload = AccountService(db).resolve(query=query, limit=limit)

    if output == OutputFormat.JSON:
        # No explicit sensitivity: AccountResolvePayload carries
        # ACCOUNT_IDENTIFIER (CRITICAL), and render_or_json derives the real
        # tier via derive_log_sensitivity. A literal "low" here understates it
        # at the call site even though the emitted value is corrected.
        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="accounts_resolve",
        )
        return

    if not payload.matches:
        policy = get_terminal_policy(no_pager=no_pager)
        emit_human_result(
            compose_human_result(
                [
                    build_summary([
                        ("Account resolution", f"No accounts match '{query}'.")
                    ])
                ],
                disclosures=["Try: moneybin accounts list"],
            ),
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )
        return
    policy = get_terminal_policy(no_pager=no_pager)
    emit_human_result(
        build_rows(
            ["account_id", "account", "subtype", "institution", "confidence"],
            [
                (
                    match.account_id,
                    match.display_name or UNNAMED_ACCOUNT_LABEL,
                    match.account_subtype or "-",
                    match.institution_name or "-",
                    Decimal(str(match.confidence)).quantize(Decimal("0.001")),
                )
                for match in payload.matches
            ],
            numeric=("confidence",),
            terminal=policy,
        ),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


app.add_typer(balance.app, name="balance")
app.add_typer(links.app, name="links")

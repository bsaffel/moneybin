"""Categorize transactions: rules, bulk apply, auto-rules.

Per-transaction categorization workflow — rules, bulk apply from JSON,
auto-rule review/confirm, and stats. The matcher itself (rules + merchants)
runs locally with no LLM dependency; LLM-assist for uncategorized rows is
available via the MCP server. Category taxonomy and merchant mappings live
in the top-level `categories` and `merchants` groups respectively.
"""

import logging
from decimal import Decimal, InvalidOperation
from typing import Literal, cast

import typer

from moneybin import error_codes
from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import (
    Money,
    MoneyWithCurrency,
    build_rows,
    build_summary,
    compose_human_result,
    format_money,
)
from moneybin.cli.utils import abort_cli_error, get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import ErrorDetail

from . import auto, ml, rules
from .commit_from_file import categorize_commit_from_file
from .export import categorize_export_uncategorized

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Categorization workflow + rules (taxonomy under top-level `categories`)",
    no_args_is_help=True,
)

app.add_typer(rules.app, name="rules")
app.add_typer(auto.app, name="auto")
app.add_typer(ml.app, name="ml", hidden=True)

app.command("export-uncategorized")(categorize_export_uncategorized)
app.command("commit-from-file")(categorize_commit_from_file)


@app.command("pending")
def categorize_pending(
    limit: int = typer.Option(
        50, "--limit", help="Maximum rows (default 50, max 1000)."
    ),
    sort: str = typer.Option(
        "date",
        "--sort",
        help="Sort order: 'date' (most recent first) or 'impact' (ABS(amount)*age_days).",
    ),
    min_amount: str = typer.Option(
        "0",
        "--min-amount",
        help="Filter to ABS(amount) >= this value.",
    ),
    account: str | None = typer.Option(
        None,
        "--account",
        help="Filter to an account: accepts account_id or display_name (ambiguous matches error).",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """List uncategorized transactions.

    Excludes transfer pairs and archived accounts.

      moneybin transactions categorize pending
      moneybin transactions categorize pending --sort impact
      moneybin transactions categorize pending --min-amount 20 --output json
    """
    from moneybin.privacy.payloads.categorize import CatPendingPayload, PendingTxnRow
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.account_service import AccountService
    from moneybin.services.categorization import CategorizationService

    try:
        min_amount_dec = Decimal(min_amount)
    except InvalidOperation as e:
        abort_cli_error(
            e,
            output=output,
            exit_code=2,
            cli_actor="categorize_pending",
            payload_type=CatPendingPayload,
            message=f"Invalid --min-amount: {min_amount}",
        )

    if sort not in {"date", "impact"}:
        abort_cli_error(
            ValueError("--sort must be 'date' or 'impact'."),
            output=output,
            exit_code=2,
            cli_actor="categorize_pending",
            payload_type=CatPendingPayload,
        )

    with handle_cli_errors(cli_actor="categorize_pending"):
        with get_database(read_only=True) as db:
            account_id: str | None = None
            if account is not None:
                account_id = AccountService(db).resolve_strict(account)
            records = CategorizationService(db).list_uncategorized_transactions(
                limit=min(limit, 1000),
                sort=cast(Literal["date", "impact"], sort),
                min_amount=min_amount_dec,
                account_id=account_id,
            )

    if records is None:
        typer.echo("No data — import transactions first.", err=True)
        raise typer.Exit(0)

    effective_limit = min(limit, 1000)
    scope_terms = ["active, non-transfer transactions"]
    if account_id is not None:
        scope_terms.append(f"account {account_id}")
    if min_amount_dec:
        scope_terms.append(f"minimum amount {min_amount_dec}")
    if sort != "date":
        scope_terms.append(f"sort {sort}")
    scope = "Scope: " + "; ".join(scope_terms) + "."
    has_filter = account_id is not None or bool(min_amount_dec) or sort != "date"
    next_action = (
        "Next: moneybin transactions categorize pending --min-amount 0"
        if min_amount_dec
        else "Next: moneybin transactions categorize pending"
        if has_filter
        else "Next: moneybin transactions categorize stats"
    )

    # Wrap raw rows in the typed CatPendingPayload so the JSON path sees the
    # account_id's active transform and redact_typed masks it — a bare list[dict]
    # short-circuits _has_active_transform(list) → False and would emit account_id raw.
    payload = CatPendingPayload(
        transactions=[
            PendingTxnRow(
                transaction_id=r["transaction_id"],
                transaction_date=str(r["txn_date"])
                if r.get("txn_date") is not None
                else None,
                amount=float(r["amount"]) if r.get("amount") is not None else None,
                currency_code=r.get("currency_code"),
                description=r.get("description"),
                memo=r.get("memo"),
                account_id=r.get("account_id"),
                age_days=int(r["age_days"]) if r.get("age_days") is not None else None,
                pending_transfer_match=bool(r.get("pending_transfer_match", False)),
            )
            for r in records
        ]
    )
    envelope = build_envelope(data=payload)

    def _render_table(_: object) -> None:
        policy = get_terminal_policy(no_pager=no_pager)
        if not records:
            emit_human_result(
                compose_human_result(
                    [build_summary([("Result", "No uncategorized transactions.")])],
                    disclosures=(
                        scope,
                        next_action,
                    ),
                ),
                policy=policy,
                finite_read=True,
                no_pager=no_pager,
            )
            return
        # A selection queue's mandatory text facts are the actionable transaction
        # ID and its complete monetary value. Measure the complete, curated
        # projection before choosing its compact table; otherwise use stacked
        # records so no terminal width can elide either fact.
        columns = (
            "Transaction ID",
            "Amount",
            "Date",
            "Description",
            "Account",
            "Age days",
        )
        table_rows = [
            (
                str(r["transaction_id"]),
                MoneyWithCurrency(
                    r.get("amount"), str(r.get("currency_code") or "n/a")
                ),
                str(r.get("txn_date") or "-"),
                str(r.get("description") or "-"),
                str(r.get("account_id") or "-"),
                str(r.get("age_days") or "-"),
            )
            for r in records
        ]
        measured_rows = [
            (
                transaction_id,
                f"{format_money(amount.amount, 'flow', minus=policy.minus)} "
                f"{amount.currency}",
                date,
                description,
                account,
                age_days,
            )
            for transaction_id, amount, date, description, account, age_days in table_rows
        ]
        # UTF-8 length is a conservative bound for terminal cell width and is
        # exact for the common ASCII identifiers, dates, amounts, and codes.
        # Rich adds one padding cell either side of every column plus its frame.
        table_width = (
            sum(
                max(
                    len(column.encode()),
                    *(len(str(value).encode()) for value in column_values),
                )
                for column, column_values in zip(
                    columns, zip(*measured_rows, strict=True), strict=True
                )
            )
            + 3 * len(columns)
            + 1
        )
        queue_part = (
            build_rows(
                columns,
                table_rows,
                money={"Amount": Money("flow")},
                terminal=policy,
            )
            if table_width <= policy.width
            else None
        )
        stacked_records = [
            build_summary(
                [
                    ("Transaction ID", str(r["transaction_id"])),
                    (
                        "Amount",
                        f"{format_money(r.get('amount'), 'flow', minus=policy.minus)} "
                        f"{r.get('currency_code') or 'n/a'}",
                    ),
                    ("Date", str(r.get("txn_date") or "-")),
                    ("Description", str(r.get("description") or "-")),
                    ("Account", str(r.get("account_id") or "-")),
                    ("Age days", str(r.get("age_days") or "-")),
                ],
                title=f"Transaction {index}",
            )
            for index, r in enumerate(records, start=1)
        ]
        emit_human_result(
            compose_human_result(
                [
                    build_summary(
                        [("Transactions", f"{len(records):,}")],
                        title="Uncategorized queue",
                    ),
                    *([queue_part] if queue_part is not None else stacked_records),
                ],
                disclosures=(
                    scope,
                    f"Showing {len(records):,} (limit {effective_limit:,}; total unknown).",
                ),
            ),
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )

    render_or_json(
        envelope, output, render_fn=_render_table, cli_actor="categorize_pending"
    )


@app.command("commit")
def categorize_commit(
    stdin_sentinel: str | None = typer.Argument(
        None,
        help="Pass '-' to read JSON from stdin.",
    ),
    input_path: str | None = typer.Option(
        None, "--input", help="Path to a JSON file with categorization items."
    ),
    output: OutputFormat = output_option,
) -> None:
    """Commit externally-decided categorizations from a JSON array.

    Each item has transaction_id and category, with optional subcategory and
    canonical_merchant_name. A canonical merchant name teaches the exact-match
    merchant exemplar set while preserving the AI-source categorization.

    Read from a file:

      moneybin transactions categorize commit --input cats.json

    Or from stdin:

      cat cats.json | moneybin transactions categorize commit -

    Per-item validation: failures are reported in the result without aborting
    the batch. Exit code is 1 if any item failed.
    """
    import json
    import sys
    from pathlib import Path

    from moneybin.cli.output import render_or_json
    from moneybin.services.categorization import (
        CategorizationResult,
        CategorizationService,
        validate_items,
    )

    use_stdin = stdin_sentinel == "-"

    if input_path is not None and use_stdin:
        typer.echo(
            "Provide either --input <path> or '-' to read from stdin (not both).",
            err=True,
        )
        raise typer.Exit(2)

    if input_path is None and not use_stdin:
        typer.echo(
            "Provide either --input <path> or '-' to read JSON from stdin.",
            err=True,
        )
        raise typer.Exit(2)

    try:
        if input_path is not None:
            with Path(input_path).open(encoding="utf-8") as f:
                raw = json.load(f)
        else:
            raw = json.load(sys.stdin)
    except FileNotFoundError as e:
        abort_cli_error(
            e,
            output=output,
            exit_code=2,
            cli_actor="categorize_commit",
            message=f"File not found: {input_path}",
        )
    except json.JSONDecodeError as e:
        abort_cli_error(
            e,
            output=output,
            exit_code=1,
            cli_actor="categorize_commit",
            message=f"Invalid JSON: {e}",
        )

    try:
        items, parse_errors = validate_items(raw)
    except ValueError as e:
        abort_cli_error(e, output=output, exit_code=1, cli_actor="categorize_commit")

    if items:
        with handle_cli_errors(cli_actor="categorize_commit"):
            with get_database(read_only=False) as db:
                result = CategorizationService(db).categorize_items(items)
    else:
        result = CategorizationResult(applied=0, skipped=0, errors=0, error_details=[])
    result.merge_parse_errors(parse_errors)

    def _render_table(_: object) -> None:
        details = [
            ("Applied", str(result.applied)),
            ("Skipped", str(result.skipped)),
            ("Failed", str(result.errors)),
        ]
        if result.merchants_created:
            details.append(("Merchant mappings created", str(result.merchants_created)))
        disclosures = tuple(
            f"{err['transaction_id']}: {err['reason']}" for err in result.error_details
        )
        emit_human_result(
            compose_human_result(
                [
                    build_summary(
                        details,
                        title=(
                            "Categorization partially completed"
                            if result.errors or result.skipped
                            else "Categorizations committed"
                        ),
                    )
                ],
                disclosures=disclosures,
            ),
            policy=get_terminal_policy(),
            finite_read=False,
            receipt=True,
        )

    from moneybin.protocol.envelope import build_envelope

    envelope = build_envelope(
        data=result.to_payload(),
        sensitivity="medium",
        actions=[
            "Use `moneybin transactions categorize rules list` to review "
            "auto-created rules",
            "Use `moneybin transactions categorize pending` to fetch the next batch",
        ],
    )
    if result.errors > 0:
        envelope = envelope.with_error(
            ErrorDetail(
                message=f"{result.errors} item(s) failed to categorize",
                code=error_codes.TRANSACTION_CATEGORIZATION_ERRORS,
            )
        )
    render_or_json(
        envelope, output, render_fn=_render_table, cli_actor="categorize_commit"
    )

    if result.errors > 0 or result.skipped > 0:
        raise typer.Exit(1)


@app.command("run")
def categorize_run(
    methods: str = typer.Option(
        "rules,merchants",
        "--methods",
        help="Comma-separated engines to run in order. Default: rules,merchants.",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Run the categorization engine cascade over uncategorized transactions.

    Engines available today: ``rules``, ``merchants``. Methods cascade in
    order — a rule write blocks a merchant write at the same priority.

      moneybin transactions categorize run
      moneybin transactions categorize run --methods rules
      moneybin transactions categorize run --methods rules,merchants --output json
    """
    from typing import Literal

    from moneybin.privacy.payloads.categorize import CategorizeRunPayload
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.categorization import CategorizationService

    valid: set[str] = {"rules", "merchants"}
    typed_methods: list[Literal["rules", "merchants"]] = []
    bad: list[str] = []
    for raw in methods.split(","):
        name = raw.strip()
        if not name:
            continue
        if name == "rules" or name == "merchants":
            typed_methods.append(name)
        else:
            bad.append(name)
    if bad:
        abort_cli_error(
            ValueError(
                f"Unknown method(s): {', '.join(bad)}. Valid: {', '.join(sorted(valid))}."
            ),
            output=output,
            exit_code=2,
            cli_actor="categorize_run",
        )

    with handle_cli_errors(cli_actor="categorize_run"):
        with get_database(read_only=False) as db:
            data = CategorizationService(db).categorize_run(methods=typed_methods)

    payload = CategorizeRunPayload(
        applied_by_method=data["applied_by_method"],
        total_applied=data["total_applied"],
    )
    envelope = build_envelope(data=payload, sensitivity="medium")

    def _render_table(_: object) -> None:
        emit_human_result(
            compose_human_result([
                build_summary(
                    [
                        (method, f"{count:,}")
                        for method, count in payload.applied_by_method.items()
                    ]
                    + [("Total applied", f"{payload.total_applied:,}")],
                    title="Categorization run complete",
                )
            ]),
            policy=get_terminal_policy(),
            finite_read=False,
            receipt=True,
        )

    render_or_json(
        envelope, output, render_fn=_render_table, cli_actor="categorize_run"
    )


@app.command("improve-ai")
def categorize_improve_ai(output: OutputFormat = output_option) -> None:
    """Re-categorize AI-guessed transactions to confident Plaid provider categories.

    Reverse-looks-up every transaction currently categorized by
    ``categorized_by='ai'`` against the Plaid category bridge; upgrades it to
    ``provider_native`` only when the bridge match is at MEDIUM confidence or
    higher. Only touches ``ai``-guessed rows — manual (``user``) and rule
    (``rule``/``auto_rule``) categorizations are never overwritten. Prints the
    count of transactions upgraded.

      moneybin transactions categorize improve-ai
      moneybin transactions categorize improve-ai --output json
    """
    from moneybin.privacy.payloads.categorize import ImproveAiPayload
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors(cli_actor="categorize_improve_ai"):
        with get_database(read_only=False) as db:
            count = CategorizationService(db).improve_ai_categories()

    payload = ImproveAiPayload(upgraded_count=count)
    envelope = build_envelope(data=payload, sensitivity="low")

    def _render_table(_: object) -> None:
        emit_human_result(
            compose_human_result([
                build_summary(
                    [
                        ("Upgraded", f"{payload.upgraded_count:,}"),
                        ("Method", "provider-native"),
                    ],
                    title="AI categorizations improved",
                )
            ]),
            policy=get_terminal_policy(),
            finite_read=False,
            receipt=True,
        )

    render_or_json(
        envelope, output, render_fn=_render_table, cli_actor="categorize_improve_ai"
    )


@app.command("assist")
def categorize_assist(
    limit: int = typer.Option(
        100, "--limit", help="Maximum number of records to return (default 100)."
    ),
    account_filter: str | None = typer.Option(
        None,
        "--account-filter",
        help="Comma-separated account IDs to restrict to.",
    ),
    date_range: str | None = typer.Option(
        None,
        "--date-range",
        help="Date range as START,END (ISO dates, inclusive).",
    ),
    output: OutputFormat = output_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Return uncategorized transactions as PII-scrubbed records for LLM categorization.

    Outputs the same shape as the MCP tool transactions_categorize_assist: merchant
    text (description/memo) is sent in full — it's the categorization signal — with
    only embedded PII (e.g. account numbers) masked. No amount, date, or account
    ID is included.

      moneybin transactions categorize assist --limit 50 --output json | jq '.data[0]'
      moneybin transactions categorize assist --account-filter acct_a,acct_b --output json

    Pipe the JSON output into an LLM workflow; commit decisions back via
    `moneybin transactions categorize commit`.
    """
    from moneybin.metrics.registry import CATEGORIZE_ASSIST_CALLS_TOTAL
    from moneybin.privacy.payloads.categorize import AssistRow, CatAssistPayload
    from moneybin.privacy.sensitivity import audit_log
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.categorization import CategorizationService

    accounts: list[str] | None = (
        [a.strip() for a in account_filter.split(",") if a.strip()]
        if account_filter
        else None
    )
    date_tuple: tuple[str, str] | None = None
    if date_range:
        parts = [p.strip() for p in date_range.split(",")]
        if len(parts) != 2:
            abort_cli_error(
                ValueError("--date-range must be START,END (ISO dates)."),
                output=output,
                exit_code=2,
                cli_actor="categorize_assist",
            )
        date_tuple = (parts[0], parts[1])

    with handle_cli_errors(cli_actor="categorize_assist"):
        with get_database(read_only=True) as db:
            redacted = CategorizationService(db).categorize_assist(
                limit=limit,
                account_filter=accounts,
                date_range=date_tuple,
            )

    CATEGORIZE_ASSIST_CALLS_TOTAL.labels(surface="cli").inc()
    audit_log(
        tool="transactions_categorize_assist",
        sensitivity="medium",
        metadata={"txn_count": len(redacted), "account_filter": accounts},
    )

    payload = CatAssistPayload(
        transactions=[
            AssistRow(
                transaction_id=r.transaction_id,
                description_scrubbed=r.description_scrubbed,
                memo_scrubbed=r.memo_scrubbed,
                source_type=r.source_type,
                transaction_type=r.transaction_type,
                check_number=r.check_number,
                is_transfer=r.is_transfer,
                transfer_pair_id=r.transfer_pair_id,
                payment_channel=r.payment_channel,
                amount_sign=r.amount_sign,
            )
            for r in redacted
        ]
    )
    envelope = build_envelope(data=payload, sensitivity="medium")

    def _render_table(_: object) -> None:
        policy = get_terminal_policy(no_pager=no_pager)
        if not payload.transactions:
            parts = [
                build_summary([
                    (
                        "Result",
                        "No uncategorized transactions are available for assist.",
                    )
                ])
            ]
            disclosures = ("Next: moneybin transactions categorize run",)
        else:
            parts = [
                build_summary(
                    [("Redacted records", f"{len(payload.transactions):,}")],
                    title="Categorization assist",
                )
            ]
            disclosures = (
                f"Showing {len(payload.transactions):,} (limit {limit:,}; total unknown).",
                "Use --output json when sending this data to an LLM.",
            )
        emit_human_result(
            compose_human_result(parts, disclosures=disclosures),
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )

    render_or_json(
        envelope, output, render_fn=_render_table, cli_actor="categorize_assist"
    )


@app.command("stats")
def stats(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # summary has no informational chatter; only data
    no_pager: bool = no_pager_option,
) -> None:
    """Show categorization coverage summary."""
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors(cli_actor="categorize_stats"):
        with get_database(read_only=True) as db:
            # `stats()` rather than `categorization_stats()`: the typed result
            # already knows how to become the payload the MCP tool returns for
            # the same numbers, so both surfaces report one shape.
            coverage = CategorizationService(db).stats()

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(data=coverage.to_payload()),
            output,
            cli_actor="categorize_stats",
        )
        return

    # The scope belongs in the header, not left for the reader to infer from a
    # total that no longer counts every transaction: these figures cover what
    # `moneybin review` will offer, so transfer legs and archived accounts are
    # out. Without it "Transactions" reads as the whole ledger and the number
    # looks wrong.
    pairs = [
        ("Transactions", f"{coverage.total:,}"),
        (
            "Categorized",
            f"{coverage.categorized:,} ({coverage.percent_categorized:.1f}%)",
        ),
        ("Uncategorized", f"{coverage.uncategorized:,}"),
        *[
            (f"By {source}", f"{value:,}")
            for source, value in coverage.by_source.items()
        ],
    ]
    if coverage.plaid_unmapped is not None:
        pairs.append(("Plaid unmapped", f"{coverage.plaid_unmapped:,}"))
    emit_human_result(
        compose_human_result(
            [build_summary(pairs, title="Categorization coverage")],
            disclosures=(
                "Scope: excludes transfers, archived and unresolved accounts.",
            ),
        ),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )

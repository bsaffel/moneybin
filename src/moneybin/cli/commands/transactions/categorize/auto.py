"""Auto-rule proposal workflow (review, confirm, stats, rules)."""

import logging
from typing import cast

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import (
    format_cli_failure,
    get_terminal_policy,
    handle_cli_errors,
)
from moneybin.database import get_database
from moneybin.privacy.payloads.categorize import (
    AutoRuleRow,
    AutoRulesPayload,
    AutoStatsPayload,
)
from moneybin.protocol.envelope import build_envelope

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Auto-rule proposal workflow",
    no_args_is_help=True,
)


@app.command("review")
def review(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Maximum number of proposals to display (defaults to configured limit)",
    ),
) -> None:
    """List pending auto-rule proposals with sample transactions and trigger counts."""
    from moneybin.adapters.categorize_adapters import auto_review_envelope
    from moneybin.services.auto_rule_service import AutoRuleService

    with handle_cli_errors(cli_actor="categorize_auto_review"):
        with get_database(read_only=True) as db:
            result = AutoRuleService(db).review(limit=limit)

    proposals = result.proposals
    if output == OutputFormat.JSON:
        envelope = auto_review_envelope(
            result,
            # An agent reading a CLI envelope can only run commands.
            accept_action=(
                "Use `moneybin transactions categorize auto accept` to accept "
                "or reject proposals"
            ),
        )
        render_or_json(envelope, output, cli_actor="categorize_auto_review")
        return

    policy = get_terminal_policy(no_pager=no_pager)
    if not proposals:
        parts = [build_summary([("Result", "No pending auto-rule proposals.")])]
        disclosures = ("Next: moneybin transactions categorize auto rules",)
    else:
        parts = [
            build_summary(
                [("Pending proposals", f"{len(proposals):,}")],
                title="Auto-rule review",
            ),
            build_rows(
                [
                    "proposal id",
                    "pattern",
                    "category",
                    "triggers",
                    "estimated matches",
                    "review",
                ],
                [
                    (
                        p["proposed_rule_id"],
                        p["merchant_pattern"],
                        " / ".join(
                            part for part in (p["category"], p["subcategory"]) if part
                        ),
                        p["trigger_count"],
                        p["estimated_match_count"],
                        "Broad — requires --allow-broad" if p["is_broad"] else "Ready",
                    )
                    for p in proposals
                ],
                numeric=("triggers", "estimated matches"),
                terminal=policy,
            ),
        ]
        disclosures = (
            f"Showing {len(proposals):,} of {result.total_count:,} proposals."
            if result.total_count > len(proposals)
            else "",
            "Broad proposals require --allow-broad to accept."
            if any(bool(p["is_broad"]) for p in proposals)
            else "",
            "Next: moneybin transactions categorize auto accept --accept <proposal-id>",
        )
    emit_human_result(
        compose_human_result(parts, disclosures=tuple(d for d in disclosures if d)),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("accept")
def categorize_auto_accept(
    accept: list[str] = typer.Option(None, "--accept", help="Proposal IDs to accept"),
    reject: list[str] = typer.Option(None, "--reject", help="Proposal IDs to reject"),
    accept_all: bool = typer.Option(
        False, "--accept-all", help="Accept all pending proposals"
    ),
    reject_all: bool = typer.Option(
        False, "--reject-all", help="Reject all pending proposals"
    ),
    allow_broad: bool = typer.Option(
        False,
        "--allow-broad",
        help=(
            "Accept proposals flagged broad (match count far exceeds evidence). "
            "Review the estimated match count first — a broad rule recategorizes "
            "many transactions at once."
        ),
    ),
    output: OutputFormat = output_option,
) -> None:
    """Batch accept/reject auto-rule proposals."""
    from moneybin.services.auto_rule_service import AutoRuleService

    if accept_all and reject_all:
        logger.error(
            format_cli_failure("--accept-all and --reject-all are mutually exclusive")
        )
        raise typer.Exit(2)

    with handle_cli_errors(cli_actor="transactions_categorize_auto_accept"):
        with get_database(read_only=False) as db:
            svc = AutoRuleService(db)
            if accept_all or reject_all:
                pending_ids = [
                    cast(str, p["proposed_rule_id"])
                    for p in svc.list_pending_proposals()
                ]
                if accept_all:
                    accept = (accept or []) + pending_ids
                if reject_all:
                    reject = (reject or []) + pending_ids

            # Explicit reject wins over --accept-all: a user passing
            # --accept-all --reject <id> means "accept all except <id>".
            accept_set = set(accept or [])
            reject_set = set(reject or [])
            accept_set -= reject_set
            result = svc.accept(
                accept=sorted(accept_set),
                reject=sorted(reject_set),
                actor="cli",
                allow_broad=allow_broad,
            )

    if output == OutputFormat.JSON:
        from moneybin import error_codes
        from moneybin.adapters.categorize_adapters import auto_accept_envelope
        from moneybin.errors import ErrorDetail

        envelope = auto_accept_envelope(result)
        if result.skipped:
            envelope = envelope.with_error(
                ErrorDetail(
                    message=f"{result.skipped} proposal(s) were skipped.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            )
        render_or_json(
            envelope,
            output,
            cli_actor="transactions_categorize_auto_accept",
        )
    else:
        emit_human_result(
            compose_human_result([
                build_summary(
                    [
                        ("Accepted", str(result.approved)),
                        ("Rejected", str(result.rejected)),
                        ("Skipped", str(result.skipped)),
                        (
                            "Existing transactions categorized",
                            str(result.newly_categorized),
                        ),
                    ],
                    title=(
                        "Auto-rule decision partially completed"
                        if result.skipped
                        else "Auto-rule decisions applied"
                    ),
                )
            ]),
            policy=get_terminal_policy(),
            finite_read=False,
            receipt=True,
        )
    if result.skipped:
        raise typer.Exit(1)


@app.command("stats")
def stats(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # stats has no informational chatter; only data
    no_pager: bool = no_pager_option,
) -> None:
    """Show auto-rule health: active rules, pending proposals, transactions categorized."""
    from moneybin.services.auto_rule_service import AutoRuleService

    with handle_cli_errors(cli_actor="categorize_auto_stats"):
        with get_database(read_only=True) as db:
            result = AutoRuleService(db).stats()

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=AutoStatsPayload(
                    active_auto_rules=result.active_auto_rules,
                    pending_proposals=result.pending_proposals,
                    transactions_categorized=result.transactions_categorized,
                )
            ),
            output,
            cli_actor="categorize_auto_stats",
        )
        return

    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    ("Active auto-rules", f"{result.active_auto_rules:,}"),
                    ("Pending proposals", f"{result.pending_proposals:,}"),
                    (
                        "Transactions categorized",
                        f"{result.transactions_categorized:,}",
                    ),
                ],
                title="Auto-rule health",
            )
        ]),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("rules")
def rules(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Maximum number of auto-rules to display (defaults to configured limit)",
    ),
) -> None:
    """List active auto-rules (rules with created_by='auto_rule')."""
    from moneybin.services.auto_rule_service import AutoRuleService

    with handle_cli_errors(cli_actor="categorize_auto_rules"):
        with get_database(read_only=True) as db:
            svc = AutoRuleService(db)
            active_rules = svc.list_active_rules(limit=limit)
            total = svc.count_active_rules()

    if output == OutputFormat.JSON:
        # `total` was a sibling key of `rules`; the envelope already has a slot
        # for "how many exist beyond this page", so it rides there instead.
        render_or_json(
            build_envelope(
                data=AutoRulesPayload(
                    rules=[
                        AutoRuleRow(
                            rule_id=cast(str, r["rule_id"]),
                            merchant_pattern=cast(str | None, r["merchant_pattern"]),
                            match_type=cast(str | None, r["match_type"]),
                            category=cast(str | None, r["category"]),
                            subcategory=cast(str | None, r["subcategory"]),
                            priority=cast(int | None, r["priority"]),
                        )
                        for r in active_rules
                    ]
                ),
                total_count=total,
                returned_count=len(active_rules),
            ),
            output,
            cli_actor="categorize_auto_rules",
        )
        return

    policy = get_terminal_policy(no_pager=no_pager)
    if not active_rules:
        parts = [build_summary([("Result", "No active auto-rules.")])]
        disclosures = ("Next: moneybin transactions categorize auto review",)
    else:
        rule_rows: list[tuple[object, object, object, str, object]] = [
            (
                r["rule_id"],
                r["merchant_pattern"],
                r["match_type"],
                " / ".join(
                    str(part)
                    for part in (r["category"], r["subcategory"])
                    if part is not None
                ),
                r["priority"],
            )
            for r in active_rules
        ]
        parts = [
            build_summary(
                [("Active rules", f"{len(active_rules):,}")], title="Auto-rules"
            ),
            build_rows(
                ["rule id", "pattern", "match", "category", "priority"],
                rule_rows,
                numeric=("priority",),
                terminal=policy,
            ),
        ]
        disclosures = (
            (
                f"Showing {len(active_rules):,} of {total:,} active rules."
                if total > len(active_rules)
                else ""
            ),
        )
    emit_human_result(
        compose_human_result(parts, disclosures=tuple(d for d in disclosures if d)),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )

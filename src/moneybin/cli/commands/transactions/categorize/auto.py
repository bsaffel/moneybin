"""Auto-rule proposal workflow (review, confirm, stats, rules)."""

import json
import logging
from typing import cast

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
)
from moneybin.cli.utils import emit_json, handle_cli_errors

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Auto-rule proposal workflow",
    no_args_is_help=True,
)


@app.command("review")
def review(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Maximum number of proposals to display (defaults to configured limit)",
    ),
) -> None:
    """List pending auto-rule proposals with sample transactions and trigger counts."""
    from moneybin.mcp.adapters.categorize_adapters import auto_review_envelope
    from moneybin.services.auto_rule_service import AutoRuleService

    with handle_cli_errors() as db:
        result = AutoRuleService(db).review(limit=limit)

    proposals = result.proposals
    if output == OutputFormat.JSON:
        typer.echo(json.dumps(auto_review_envelope(result).to_dict(), indent=2))
        return

    if not proposals:
        if not quiet:
            logger.info("No pending auto-rule proposals.")
        return

    if not quiet:
        logger.info("👀 Pending auto-rule proposals:")
    for p in proposals:
        sub = f" / {p['subcategory']}" if p["subcategory"] else ""
        samples = cast(list[str], p["sample_txn_ids"])
        sample_str = f" samples: {','.join(samples)}" if samples else ""
        logger.info(
            f"  [{p['proposed_rule_id']}] '{p['merchant_pattern']}' "
            f"({p['match_type']}) -> {p['category']}{sub} "
            f"(×{p['trigger_count']}){sample_str}"
        )
    if not quiet and result.total_count > len(proposals):
        logger.info(
            f"💡 Showing {len(proposals)} of {result.total_count} pending proposals "
            f"— increase --limit to see more"
        )


@app.command("confirm")
def confirm(
    approve: list[str] = typer.Option(
        None, "--approve", help="Proposal IDs to approve"
    ),
    reject: list[str] = typer.Option(None, "--reject", help="Proposal IDs to reject"),
    approve_all: bool = typer.Option(
        False, "--approve-all", help="Approve all pending proposals"
    ),
    reject_all: bool = typer.Option(
        False, "--reject-all", help="Reject all pending proposals"
    ),
) -> None:
    """Batch approve/reject auto-rule proposals."""
    from moneybin.services.auto_rule_service import AutoRuleService

    if approve_all and reject_all:
        logger.error("❌ --approve-all and --reject-all are mutually exclusive")
        raise typer.Exit(2)

    with handle_cli_errors() as db:
        svc = AutoRuleService(db)
        if approve_all or reject_all:
            pending_ids = [
                cast(str, p["proposed_rule_id"]) for p in svc.list_pending_proposals()
            ]
            if approve_all:
                approve = (approve or []) + pending_ids
            if reject_all:
                reject = (reject or []) + pending_ids

        # Explicit reject wins over --approve-all: a user passing
        # --approve-all --reject <id> means "approve all except <id>".
        approve_set = set(approve or [])
        reject_set = set(reject or [])
        approve_set -= reject_set
        result = svc.accept(accept=sorted(approve_set), reject=sorted(reject_set))

    logger.info(
        f"✅ Approved {result.approved} "
        f"(categorized {result.newly_categorized} existing); "
        f"rejected {result.rejected}; "
        f"skipped {result.skipped}"
    )


@app.command("stats")
def stats(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — stats has no informational chatter; only data
) -> None:
    """Show auto-rule health: active rules, pending proposals, transactions categorized."""
    from moneybin.services.auto_rule_service import AutoRuleService

    with handle_cli_errors() as db:
        result = AutoRuleService(db).stats()

    if output == OutputFormat.JSON:
        emit_json(
            "stats",
            {
                "active_auto_rules": result.active_auto_rules,
                "pending_proposals": result.pending_proposals,
                "transactions_categorized": result.transactions_categorized,
            },
        )
        return

    logger.info("Auto-rule health:")
    logger.info(f"  Active auto-rules:        {result.active_auto_rules}")
    logger.info(f"  Pending proposals:        {result.pending_proposals}")
    logger.info(f"  Transactions auto-ruled:  {result.transactions_categorized}")


@app.command("rules")
def rules(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    limit: int | None = typer.Option(
        None,
        "--limit",
        min=1,
        help="Maximum number of auto-rules to display (defaults to configured limit)",
    ),
) -> None:
    """List active auto-rules (rules with created_by='auto_rule')."""
    from moneybin.services.auto_rule_service import AutoRuleService

    with handle_cli_errors() as db:
        svc = AutoRuleService(db)
        active_rules = svc.list_active_rules(limit=limit)
        total = svc.count_active_rules()

    if output == OutputFormat.JSON:
        emit_json("rules", {"rules": active_rules, "total": total})
        return

    if not active_rules:
        if not quiet:
            logger.info("No active auto-rules.")
        return

    if not quiet:
        logger.info("Active auto-rules:")
    for r in active_rules:
        sub = f" / {r['subcategory']}" if r["subcategory"] else ""
        logger.info(
            f"  [{r['rule_id']}] '{r['merchant_pattern']}' "
            f"({r['match_type']}) -> {r['category']}{sub} "
            f"(priority: {r['priority']})"
        )
    if not quiet and total > len(active_rules):
        logger.info(
            f"💡 Showing {len(active_rules)} of {total} active auto-rules "
            f"— increase --limit to see more"
        )

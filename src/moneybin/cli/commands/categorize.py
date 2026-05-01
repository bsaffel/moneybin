"""Categorization CLI commands for MoneyBin.

Deterministic categorization operations — no LLM dependency.
LLM-based auto-categorization is available through the MCP server.
"""

import json
import logging
from typing import cast

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.protocol.envelope import ResponseEnvelope

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage transaction categories, rules, and merchants",
    no_args_is_help=True,
)

auto_app = typer.Typer(
    help="Auto-categorization workflows: review, confirm, stats, rules",
    no_args_is_help=True,
)
app.add_typer(auto_app, name="auto")


@app.command("apply-rules")
def apply_rules_cmd() -> None:
    """Run all active rules and merchant mappings against uncategorized transactions."""
    from moneybin.services.categorization_service import CategorizationService

    with handle_cli_errors() as db:
        stats = CategorizationService(db).apply_deterministic()
        if stats["total"] > 0:
            logger.info(
                f"\u2705 Categorized {stats['total']} transactions "
                f"({stats['merchant']} merchant, {stats['rule']} rule)"
            )
        else:
            logger.info(
                "\u2705 No uncategorized transactions matched rules or merchants"
            )


@app.command("summary")
def summary_cmd(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — summary has no informational chatter; only data
) -> None:
    """Show categorization coverage summary."""
    from moneybin.services.categorization_service import CategorizationService

    with handle_cli_errors() as db:
        stats = CategorizationService(db).categorization_stats()

    if output == "json":
        emit_json("summary", stats)
        return

    total = stats["total"]
    categorized = stats["categorized"]
    uncategorized = stats["uncategorized"]
    pct = stats["pct_categorized"]

    logger.info("Categorization coverage:")
    logger.info(f"  Total transactions:   {total}")
    logger.info(f"  Categorized:          {categorized} ({pct:.1f}%)")
    logger.info(f"  Uncategorized:        {uncategorized}")

    # Show breakdown by source
    for key, value in stats.items():
        if key.startswith("by_"):
            source = key[3:]
            logger.info(f"  By {source}:  {value}")


@app.command("list-rules")
def list_rules_cmd(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Display all active categorization rules."""
    from moneybin.tables import CATEGORIZATION_RULES

    with handle_cli_errors() as db:
        rows = db.execute(
            f"""
            SELECT rule_id, name, merchant_pattern, match_type,
                   category, subcategory, priority
            FROM {CATEGORIZATION_RULES.full_name}
            WHERE is_active = true
            ORDER BY priority ASC, name
            """
        ).fetchall()

    if output == "json":
        rules = [
            {
                "rule_id": r[0],
                "name": r[1],
                "merchant_pattern": r[2],
                "match_type": r[3],
                "category": r[4],
                "subcategory": r[5],
                "priority": r[6],
            }
            for r in rows
        ]
        emit_json("rules", rules)
        return

    if not rows:
        if not quiet:
            logger.info("No active categorization rules.")
        return

    if not quiet:
        logger.info("Active categorization rules:")
    for rule_id, name, pattern, match_type, cat, subcat, priority in rows:
        sub = f" / {subcat}" if subcat else ""
        logger.info(
            f"  [{rule_id}] {name}: '{pattern}' ({match_type}) -> {cat}{sub} (priority: {priority})"
        )


@auto_app.command("review")
def auto_review_cmd(
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
    if output == "json":
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


@auto_app.command("confirm")
def auto_confirm_cmd(
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
        result = svc.confirm(approve=sorted(approve_set), reject=sorted(reject_set))

    logger.info(
        f"✅ Approved {result.approved} "
        f"(categorized {result.newly_categorized} existing); "
        f"rejected {result.rejected}; "
        f"skipped {result.skipped}"
    )


@auto_app.command("stats")
def auto_stats_cmd(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — stats has no informational chatter; only data
) -> None:
    """Show auto-rule health: active rules, pending proposals, transactions categorized."""
    from moneybin.services.auto_rule_service import AutoRuleService

    with handle_cli_errors() as db:
        stats = AutoRuleService(db).stats()

    if output == "json":
        emit_json(
            "stats",
            {
                "active_auto_rules": stats.active_auto_rules,
                "pending_proposals": stats.pending_proposals,
                "transactions_categorized": stats.transactions_categorized,
            },
        )
        return

    logger.info("Auto-rule health:")
    logger.info(f"  Active auto-rules:        {stats.active_auto_rules}")
    logger.info(f"  Pending proposals:        {stats.pending_proposals}")
    logger.info(f"  Transactions auto-ruled:  {stats.transactions_categorized}")


@app.command("bulk")
def bulk_cmd(
    stdin_sentinel: str | None = typer.Argument(
        None,
        help="Pass '-' to read JSON from stdin.",
    ),
    input_path: str | None = typer.Option(
        None, "--input", help="Path to a JSON file with categorization items."
    ),
    output: OutputFormat = output_option,
) -> None:
    """Bulk-assign categories to transactions from a JSON array.

    Read from a file:

      moneybin categorize bulk --input cats.json

    Or from stdin:

      cat cats.json | moneybin categorize bulk -

    Per-item validation: failures are reported in the result without aborting
    the batch. Exit code is 1 if any item failed.
    """
    import json
    import sys
    from pathlib import Path

    from moneybin.services.categorization_service import (
        BulkCategorizationResult,
        CategorizationService,
        validate_bulk_items,
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
        typer.echo(f"❌ File not found: {input_path}", err=True)
        raise typer.Exit(2) from e
    except json.JSONDecodeError as e:
        typer.echo(f"❌ Invalid JSON: {e}", err=True)
        raise typer.Exit(1) from e

    try:
        items, parse_errors = validate_bulk_items(raw)
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    if items:
        with handle_cli_errors() as db:
            result = CategorizationService(db).bulk_categorize(items)
    else:
        result = BulkCategorizationResult(
            applied=0, skipped=0, errors=0, error_details=[]
        )
    result.merge_parse_errors(parse_errors)

    input_count = len(items) + len(parse_errors)

    def _render_table(_: ResponseEnvelope) -> None:
        logger.info(
            f"✅ Applied {result.applied} | skipped {result.skipped} | errors {result.errors}"
        )
        if result.merchants_created:
            logger.info(f"   Created {result.merchants_created} merchant mappings")
        for err in result.error_details:
            logger.warning(f"⚠️  {err['transaction_id']}: {err['reason']}")

    render_or_json(result.to_envelope(input_count), output, render_fn=_render_table)

    if result.errors > 0 or result.skipped > 0:
        raise typer.Exit(1)


@auto_app.command("rules")
def auto_rules_cmd(
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
        rules = svc.list_active_rules(limit=limit)
        total = svc.count_active_rules()

    if output == "json":
        emit_json("rules", {"rules": rules, "total": total})
        return

    if not rules:
        if not quiet:
            logger.info("No active auto-rules.")
        return

    if not quiet:
        logger.info("Active auto-rules:")
    for r in rules:
        sub = f" / {r['subcategory']}" if r["subcategory"] else ""
        logger.info(
            f"  [{r['rule_id']}] '{r['merchant_pattern']}' "
            f"({r['match_type']}) -> {r['category']}{sub} "
            f"(priority: {r['priority']})"
        )
    if not quiet and total > len(rules):
        logger.info(
            f"💡 Showing {len(rules)} of {total} active auto-rules "
            f"— increase --limit to see more"
        )

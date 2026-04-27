"""Categorization CLI commands for MoneyBin.

Deterministic categorization operations — no LLM dependency.
LLM-based auto-categorization is available through the MCP server.
"""

import logging
from typing import cast

import typer

from moneybin.cli.utils import handle_database_errors

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage transaction categories, rules, and merchants",
    no_args_is_help=True,
)


@app.command("apply-rules")
def apply_rules_cmd() -> None:
    """Run all active rules and merchant mappings against uncategorized transactions."""
    from moneybin.services.categorization_service import CategorizationService

    try:
        with handle_database_errors() as db:
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
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e


@app.command("seed")
def seed_cmd() -> None:
    """Initialize default categories from Plaid PFCv2 taxonomy.

    Requires SQLMesh transforms to have been run at least once.
    Safe to run multiple times — existing categories are not overwritten.
    """
    from moneybin.services.categorization_service import CategorizationService

    try:
        with handle_database_errors() as db:
            count = CategorizationService(db).seed()
            logger.info(f"\u2705 Seeded {count} new categories")
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e


@app.command("stats")
def stats_cmd() -> None:
    """Show categorization coverage statistics."""
    from moneybin.services.categorization_service import CategorizationService

    try:
        with handle_database_errors() as db:
            stats = CategorizationService(db).categorization_stats()
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e

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
def list_rules_cmd() -> None:
    """Display all active categorization rules."""
    from moneybin.tables import CATEGORIZATION_RULES

    try:
        with handle_database_errors() as db:
            rows = db.execute(
                f"""
                SELECT rule_id, name, merchant_pattern, match_type,
                       category, subcategory, priority
                FROM {CATEGORIZATION_RULES.full_name}
                WHERE is_active = true
                ORDER BY priority ASC, name
                """
            ).fetchall()
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e

    if not rows:
        logger.info("No active categorization rules.")
        return

    logger.info("Active categorization rules:")
    for rule_id, name, pattern, match_type, cat, subcat, priority in rows:
        sub = f" / {subcat}" if subcat else ""
        logger.info(
            f"  [{rule_id}] {name}: '{pattern}' ({match_type}) -> {cat}{sub} (priority: {priority})"
        )


@app.command("auto-review")
def auto_review_cmd(
    output: str = typer.Option(
        "table", "--output", help="Output format: table or json"
    ),
) -> None:
    """List pending auto-rule proposals with sample transactions and trigger counts."""
    import json

    from moneybin.services.categorization_service import CategorizationService

    try:
        with handle_database_errors() as db:
            proposals = CategorizationService(db).auto_review()
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e

    if output == "json":
        typer.echo(json.dumps(proposals))
        return

    if not proposals:
        logger.info("No pending auto-rule proposals.")
        return

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


@app.command("auto-confirm")
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
    from moneybin.services.categorization_service import CategorizationService

    if approve_all and reject_all:
        logger.error("❌ --approve-all and --reject-all are mutually exclusive")
        raise typer.Exit(2)

    try:
        with handle_database_errors() as db:
            svc = CategorizationService(db)
            if approve_all or reject_all:
                pending_ids = [
                    cast(str, p["proposed_rule_id"]) for p in svc.auto_review()
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
            result = svc.auto_confirm(
                approve=sorted(approve_set), reject=sorted(reject_set)
            )
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e

    logger.info(
        f"✅ Approved {result['approved']} "
        f"(categorized {result['newly_categorized']} existing); "
        f"rejected {result['rejected']}; "
        f"skipped {result['skipped']}"
    )


@app.command("auto-stats")
def auto_stats_cmd() -> None:
    """Show auto-rule health: active rules, pending proposals, transactions categorized."""
    from moneybin.services.categorization_service import CategorizationService

    try:
        with handle_database_errors() as db:
            stats = CategorizationService(db).auto_stats()
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e

    logger.info("Auto-rule health:")
    logger.info(f"  Active auto-rules:        {stats['active_auto_rules']}")
    logger.info(f"  Pending proposals:        {stats['pending_proposals']}")
    logger.info(f"  Transactions auto-ruled:  {stats['transactions_categorized']}")


@app.command("auto-rules")
def auto_rules_cmd() -> None:
    """List active auto-rules (rules with created_by='auto_rule')."""
    from moneybin.services.categorization_service import CategorizationService

    try:
        with handle_database_errors() as db:
            rules = CategorizationService(db).list_auto_rules()
    except FileNotFoundError as e:
        logger.error(f"{e}")
        raise typer.Exit(1) from e

    if not rules:
        logger.info("No active auto-rules.")
        return

    logger.info("Active auto-rules:")
    for r in rules:
        sub = f" / {r['subcategory']}" if r["subcategory"] else ""
        logger.info(
            f"  [{r['rule_id']}] '{r['merchant_pattern']}' "
            f"({r['match_type']}) -> {r['category']}{sub} "
            f"(priority: {r['priority']})"
        )

"""Categorization CLI commands for MoneyBin.

Deterministic categorization operations — no LLM dependency.
LLM-based auto-categorization is available through the MCP server.
"""

import logging

import typer

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage transaction categories, rules, and merchants",
    no_args_is_help=True,
)


@app.command("apply-rules")
def apply_rules_cmd() -> None:
    """Run all active rules and merchant mappings against uncategorized transactions."""
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.services.categorization_service import (
        apply_deterministic_categorization,
    )

    try:
        db = get_database()
        stats = apply_deterministic_categorization(db)
        if stats["total"] > 0:
            logger.info(
                "\u2705 Categorized %d transactions (%d merchant, %d rule)",
                stats["total"],
                stats["merchant"],
                stats["rule"],
            )
        else:
            logger.info(
                "\u2705 No uncategorized transactions matched rules or merchants"
            )
    except FileNotFoundError as e:
        logger.error("%s", e)
        raise typer.Exit(1) from e
    except DatabaseKeyError:
        logger.error(
            "Database is locked. Run 'moneybin db unlock' "
            "or set MONEYBIN_DATABASE__ENCRYPTION_KEY."
        )
        raise typer.Exit(1) from None


@app.command("seed")
def seed_cmd() -> None:
    """Initialize default categories from Plaid PFCv2 taxonomy.

    Requires SQLMesh transforms to have been run at least once.
    Safe to run multiple times — existing categories are not overwritten.
    """
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.services.categorization_service import seed_categories

    try:
        db = get_database()
        count = seed_categories(db)
        logger.info("\u2705 Seeded %d new categories", count)
    except FileNotFoundError as e:
        logger.error("%s", e)
        raise typer.Exit(1) from e
    except DatabaseKeyError:
        logger.error(
            "Database is locked. Run 'moneybin db unlock' "
            "or set MONEYBIN_DATABASE__ENCRYPTION_KEY."
        )
        raise typer.Exit(1) from None


@app.command("stats")
def stats_cmd() -> None:
    """Show categorization coverage statistics."""
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.services.categorization_service import get_categorization_stats

    try:
        db = get_database()
        stats = get_categorization_stats(db)
    except FileNotFoundError as e:
        logger.error("%s", e)
        raise typer.Exit(1) from e
    except DatabaseKeyError:
        logger.error(
            "Database is locked. Run 'moneybin db unlock' "
            "or set MONEYBIN_DATABASE__ENCRYPTION_KEY."
        )
        raise typer.Exit(1) from None

    total = stats["total"]
    categorized = stats["categorized"]
    uncategorized = stats["uncategorized"]
    pct = stats["pct_categorized"]

    logger.info("Categorization coverage:")
    logger.info("  Total transactions:   %d", total)
    logger.info("  Categorized:          %d (%.1f%%)", categorized, pct)
    logger.info("  Uncategorized:        %d", uncategorized)

    # Show breakdown by source
    for key, value in stats.items():
        if key.startswith("by_"):
            source = key[3:]
            logger.info("  By %s:  %s", source, value)


@app.command("list-rules")
def list_rules_cmd() -> None:
    """Display all active categorization rules."""
    from moneybin.database import DatabaseKeyError, get_database
    from moneybin.tables import CATEGORIZATION_RULES

    try:
        db = get_database()
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
        logger.error("%s", e)
        raise typer.Exit(1) from e
    except DatabaseKeyError:
        logger.error(
            "Database is locked. Run 'moneybin db unlock' "
            "or set MONEYBIN_DATABASE__ENCRYPTION_KEY."
        )
        raise typer.Exit(1) from None

    if not rows:
        logger.info("No active categorization rules.")
        return

    logger.info("Active categorization rules:")
    for rule_id, name, pattern, match_type, cat, subcat, priority in rows:
        sub = f" / {subcat}" if subcat else ""
        logger.info(
            "  [%s] %s: '%s' (%s) -> %s%s (priority: %d)",
            rule_id,
            name,
            pattern,
            match_type,
            cat,
            sub,
            priority,
        )

"""Categorization CLI commands for MoneyBin.

Deterministic categorization operations — no LLM dependency.
LLM-based auto-categorization is available through the MCP server.
"""

import logging

import duckdb
import typer

from ...config import get_database_path
from ...logging import setup_logging

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Manage transaction categories, rules, and merchants",
    no_args_is_help=True,
)


@app.command("apply-rules")
def apply_rules_cmd() -> None:
    """Run all active rules and merchant mappings against uncategorized transactions."""
    setup_logging(cli_mode=True)

    from moneybin.services.categorization_service import (
        apply_deterministic_categorization,
    )

    db_path = get_database_path()
    try:
        conn = duckdb.connect(str(db_path), read_only=False)
        try:
            stats = apply_deterministic_categorization(conn)
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
        finally:
            conn.close()
    except FileNotFoundError as e:
        logger.error("%s", e)
        raise typer.Exit(1) from e
    except duckdb.IOException as e:
        logger.error("Database error: %s", e)
        raise typer.Exit(1) from e


@app.command("seed")
def seed_cmd() -> None:
    """Initialize default categories from Plaid PFCv2 taxonomy.

    Requires SQLMesh transforms to have been run at least once.
    Safe to run multiple times — existing categories are not overwritten.
    """
    setup_logging(cli_mode=True)

    from moneybin.services.categorization_service import seed_categories

    db_path = get_database_path()
    try:
        conn = duckdb.connect(str(db_path), read_only=False)
        try:
            count = seed_categories(conn)
            logger.info("\u2705 Seeded %d new categories", count)
        finally:
            conn.close()
    except FileNotFoundError as e:
        logger.error("%s", e)
        raise typer.Exit(1) from e
    except duckdb.CatalogException as e:
        logger.error(
            "Seed table not found. Run SQLMesh transforms first: "
            "moneybin data transform apply\n%s",
            e,
        )
        raise typer.Exit(1) from e


@app.command("stats")
def stats_cmd() -> None:
    """Show categorization coverage statistics."""
    setup_logging(cli_mode=True)

    from moneybin.services.categorization_service import get_categorization_stats

    db_path = get_database_path()
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            stats = get_categorization_stats(conn)
        finally:
            conn.close()
    except FileNotFoundError as e:
        logger.error("%s", e)
        raise typer.Exit(1) from e

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
    setup_logging(cli_mode=True)

    from moneybin.tables import CATEGORIZATION_RULES

    db_path = get_database_path()
    try:
        conn = duckdb.connect(str(db_path), read_only=True)
        try:
            rows = conn.execute(
                f"""
                SELECT rule_id, name, merchant_pattern, match_type,
                       category, subcategory, priority
                FROM {CATEGORIZATION_RULES.full_name}
                WHERE is_active = true
                ORDER BY priority ASC, name
                """
            ).fetchall()
        finally:
            conn.close()
    except FileNotFoundError as e:
        logger.error("%s", e)
        raise typer.Exit(1) from e
    except duckdb.CatalogException:
        logger.info("No categorization rules found.")
        return

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

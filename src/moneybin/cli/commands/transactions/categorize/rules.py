"""Rule management for categorization (list, apply)."""

import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.protocol.envelope import build_envelope

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Rule management (list, apply)",
    no_args_is_help=True,
)


@app.command("list")
def rules_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Display all active categorization rules."""
    from moneybin.tables import CATEGORIZATION_RULES

    with handle_cli_errors(output=output) as db:
        rows = db.execute(
            f"""
            SELECT rule_id, name, merchant_pattern, match_type,
                   category, subcategory, priority
            FROM {CATEGORIZATION_RULES.full_name}
            WHERE is_active = true
            ORDER BY priority ASC, name
            """  # noqa: S608  # TableRef compile-time constant, not user input
        ).fetchall()

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

    def _render_text(_: object) -> None:
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

    render_or_json(
        build_envelope(data=rules, sensitivity="low"),
        output,
        render_fn=_render_text,
    )


@app.command("apply")
def rules_apply() -> None:
    """Run all active rules and merchant mappings against uncategorized transactions."""
    from moneybin.services.categorization_service import CategorizationService

    with handle_cli_errors() as db:
        stats = CategorizationService(db).categorize_pending()
        if stats["total"] > 0:
            logger.info(
                f"✅ Categorized {stats['total']} transactions "
                f"({stats['merchant']} merchant, {stats['rule']} rule)"
            )
        else:
            logger.info("✅ No uncategorized transactions matched rules or merchants")

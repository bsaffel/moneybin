"""Rule management for categorization (list, apply, create, delete)."""

import logging
from enum import StrEnum
from pathlib import Path

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
)
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.database import get_database

logger = logging.getLogger(__name__)


class MatchTypeChoice(StrEnum):
    """Mirrors `services.categorization._shared.MatchType` for Typer choice validation."""

    EXACT = "exact"
    CONTAINS = "contains"
    REGEX = "regex"


app = typer.Typer(
    help="Rule management (list, apply, create, delete)",
    no_args_is_help=True,
)


@app.command("list")
def rules_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Display all active categorization rules."""
    from moneybin.tables import CATEGORIZATION_RULES

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            rows = db.execute(
                f"""
                SELECT rule_id, name, merchant_pattern, match_type,
                       category, subcategory, priority
                FROM {CATEGORIZATION_RULES.full_name}
                WHERE is_active = true
                ORDER BY priority ASC, name
                """  # noqa: S608  # TableRef compile-time constant, not user input
            ).fetchall()

    if output == OutputFormat.JSON:
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


@app.command("apply")
def rules_apply() -> None:
    """Run all active rules and merchant mappings against uncategorized transactions."""
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database() as db:
            stats = CategorizationService(db).categorize_pending()
            if stats["total"] > 0:
                logger.info(
                    f"✅ Categorized {stats['total']} transactions "
                    f"({stats['merchant']} merchant, {stats['rule']} rule)"
                )
            else:
                logger.info(
                    "✅ No uncategorized transactions matched rules or merchants"
                )


@app.command("create")
def rules_create(
    name: str | None = typer.Argument(
        None, help="Rule name (omit when --from-file is used)"
    ),
    pattern: str | None = typer.Option(
        None, "--pattern", help="Merchant pattern to match"
    ),
    category: str | None = typer.Option(None, "--category", help="Target category"),
    subcategory: str | None = typer.Option(
        None, "--subcategory", help="Optional target subcategory"
    ),
    match_type: MatchTypeChoice = typer.Option(
        MatchTypeChoice.CONTAINS, "--match-type", help="Pattern match strategy"
    ),
    priority: int = typer.Option(100, "--priority", help="Lower runs first"),
    min_amount: float | None = typer.Option(None, "--min-amount"),
    max_amount: float | None = typer.Option(None, "--max-amount"),
    account_id: str | None = typer.Option(
        None, "--account-id", help="Restrict to one account"
    ),
    from_file: Path | None = typer.Option(
        None, "--from-file", help="JSON file with a list of rule dicts"
    ),
    reapply: bool = typer.Option(
        False,
        "--reapply",
        help="Apply newly-created rules to uncategorized rows after insert",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Create one or more categorization rules.

    Single rule: pass NAME positionally with --pattern and --category.
    Batch: pass --from-file pointing at a JSON list of rule dicts.
    """
    import json  # noqa: PLC0415 — defer import; CLI cold-start hygiene

    from moneybin.services.categorization import (  # noqa: PLC0415 — defer import; CLI cold-start hygiene
        CategorizationService,
        validate_rule_items,
    )

    if from_file is not None:
        try:
            with from_file.open(encoding="utf-8") as f:
                loaded = json.load(f)
        except FileNotFoundError as e:
            typer.echo(f"❌ File not found: {from_file}", err=True)
            raise typer.Exit(2) from e
        except json.JSONDecodeError as e:
            typer.echo(f"❌ Invalid JSON in {from_file}: {e}", err=True)
            raise typer.Exit(1) from e
        if not isinstance(loaded, list):
            raise typer.BadParameter(
                "--from-file must point at a JSON list of rule dicts"
            )
        rules: list[dict[str, object]] = loaded
    else:
        if not (name and pattern and category):
            raise typer.BadParameter(
                "Single-rule mode requires NAME + --pattern + --category, "
                "or use --from-file for batch."
            )
        rules = [
            {
                "name": name,
                "merchant_pattern": pattern,
                "category": category,
                "subcategory": subcategory,
                "match_type": match_type.value,
                "priority": priority,
                "min_amount": min_amount,
                "max_amount": max_amount,
                "account_id": account_id,
            }
        ]

    with handle_cli_errors():
        validated, parse_errors = validate_rule_items(rules)
        with get_database() as db:
            result = CategorizationService(db).create_rules(validated, reapply=reapply)
        result.merge_parse_errors(parse_errors)

    if output == OutputFormat.JSON:
        emit_json("rules_create", result.to_envelope(len(rules)).data)
        return

    if not quiet:
        logger.info(
            f"✅ Created {result.created} rule(s); "
            f"existing {result.existing}, skipped {result.skipped}"
        )


@app.command("delete")
def rules_delete(
    rule_id: str = typer.Argument(..., help="Rule ID to deactivate (soft-delete)"),
    reapply: bool = typer.Option(
        False,
        "--reapply",
        help="Re-evaluate transactions previously categorized by this rule",
    ),
    output: OutputFormat = output_option,
) -> None:
    """Soft-delete (deactivate) a categorization rule by ID.

    The rule remains in the database with is_active=false. Use --reapply to
    strip categorizations written by this rule and re-evaluate those rows
    against remaining active matchers.
    """
    from moneybin.errors import (
        UserError,  # noqa: PLC0415 — defer import; CLI cold-start hygiene
    )
    from moneybin.services.categorization import (  # noqa: PLC0415 — defer import; CLI cold-start hygiene
        CategorizationService,
    )

    with handle_cli_errors():
        with get_database() as db:
            deactivated = CategorizationService(db).deactivate_rule(
                rule_id, reapply=reapply
            )
        if not deactivated:
            raise UserError(f"Rule {rule_id} not found", code="RULE_NOT_FOUND")

    if output == OutputFormat.JSON:
        emit_json("rules_delete", {"rule_id": rule_id, "action": "deactivated"})
        return

    logger.info(f"✅ Rule {rule_id} deactivated")

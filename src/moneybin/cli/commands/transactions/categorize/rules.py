"""Rule management for categorization (list, apply, create, delete, resolve)."""

import json
import logging
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import typer

from moneybin import error_codes
from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.protocol.envelope import build_envelope

if TYPE_CHECKING:
    from moneybin.services.categorization import ConflictDecision

logger = logging.getLogger(__name__)


class MatchTypeChoice(StrEnum):
    """Mirrors `services.categorization._shared.MatchType` for Typer choice validation."""

    EXACT = "exact"
    CONTAINS = "contains"
    REGEX = "regex"


app = typer.Typer(
    help="Rule management (list, apply, create, delete, resolve conflicts)",
    no_args_is_help=True,
)


@app.command("list")
def rules_list(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Display all active categorization rules."""
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors(cli_actor="rules_list"):
        with get_database(read_only=True) as db:
            rows = [
                row
                for row in CategorizationService(db).list_rules().rules
                if row.is_active
            ]

    if output == OutputFormat.JSON:
        rules = [
            {
                "rule_id": row.rule_id,
                "name": row.name,
                "merchant_pattern": row.merchant_pattern,
                "match_type": row.match_type,
                "category": row.category,
                "subcategory": row.subcategory,
                "priority": row.priority,
            }
            for row in rows
        ]
        render_or_json(
            build_envelope(data=rules, sensitivity="low"),
            output,
            cli_actor="rules_list",
        )
        return

    if not rows:
        if not quiet:
            logger.info("No active categorization rules.")
        return

    if not quiet:
        logger.info("Active categorization rules:")
    for row in rows:
        sub = f" / {row.subcategory}" if row.subcategory else ""
        logger.info(
            f"  [{row.rule_id}] {row.name}: '{row.merchant_pattern}' "
            f"({row.match_type}) -> {row.category}{sub} "
            f"(priority: {row.priority})"
        )


@app.command("apply")
def rules_apply() -> None:
    """Run all active rules against uncategorized transactions."""
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            result = CategorizationService(db).categorize_run(methods=["rules"])
            applied = result["total_applied"]
            if applied > 0:
                logger.info(f"✅ Categorized {applied} transactions by rule")
            else:
                logger.info("✅ No uncategorized transactions matched active rules")


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
    match_type: MatchTypeChoice | None = typer.Option(
        None, "--match-type", help="Pattern match strategy (default: contains)"
    ),
    priority: int | None = typer.Option(
        None, "--priority", help="Lower runs first (default: 100)"
    ),
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
    allow_broad: bool = typer.Option(
        False,
        "--allow-broad",
        help=(
            "Allow a 'contains' rule whose pattern is too short to "
            "discriminate (e.g. 'TO', which matches STORE/AUTO/TOTAL). "
            "Without this flag such rules are refused, not created."
        ),
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Create one or more categorization rules.

    Single rule: pass NAME positionally with --pattern and --category.
    Batch: pass --from-file pointing at a JSON list of rule dicts.

    A 'contains' rule whose pattern is too short to discriminate is refused
    unless --allow-broad is passed — see --allow-broad help.
    """
    from moneybin.services.categorization import (  # noqa: PLC0415 — defer import; CLI cold-start hygiene
        CategorizationService,
        validate_rule_items,
    )

    if from_file is not None:
        single_rule_flags = {
            "NAME": name,
            "--pattern": pattern,
            "--category": category,
            "--subcategory": subcategory,
            "--match-type": match_type,
            "--priority": priority,
            "--min-amount": min_amount,
            "--max-amount": max_amount,
            "--account-id": account_id,
        }
        conflicting = [
            flag for flag, val in single_rule_flags.items() if val is not None
        ]
        if conflicting:
            raise typer.BadParameter(
                f"--from-file is mutually exclusive with single-rule flags: "
                f"{', '.join(conflicting)}"
            )
        try:
            with from_file.open(encoding="utf-8") as f:
                loaded = json.load(f)
        except FileNotFoundError as e:
            typer.echo(f"❌ File not found: {from_file}", err=True)
            raise typer.Exit(2) from e
        except json.JSONDecodeError as e:
            typer.echo(f"❌ Invalid JSON in {from_file}: {e}", err=True)
            raise typer.Exit(1) from e
        except OSError as e:
            # PermissionError, IsADirectoryError, broken-mount OSError, etc.
            typer.echo(f"❌ Cannot read {from_file}: {e}", err=True)
            raise typer.Exit(2) from e
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
                "match_type": (match_type or MatchTypeChoice.CONTAINS).value,
                "priority": priority if priority is not None else 100,
                "min_amount": min_amount,
                "max_amount": max_amount,
                "account_id": account_id,
            }
        ]

    with handle_cli_errors(cli_actor="rules_create"):
        validated, parse_errors = validate_rule_items(rules)
        with get_database(read_only=False) as db:
            result = CategorizationService(db).create_rules(
                validated, reapply=reapply, actor="cli", allow_broad=allow_broad
            )
        result.merge_parse_errors(parse_errors)

    if output == OutputFormat.JSON:
        actions = [
            "Use `moneybin transactions categorize rules list` to review all rules"
        ]
        if result.conflicts:
            actions.insert(
                0,
                "Use `moneybin transactions categorize rules resolve` to decide "
                "the refused rule(s)",
            )
        envelope = build_envelope(
            data=result.to_payload(),
            sensitivity="low",
            total_count=len(rules),
            conflict=result.conflicts > 0,
            actions=actions,
        )
        render_or_json(envelope, output, cli_actor="rules_create")
    elif not quiet:
        logger.info(
            f"✅ Created {result.created} rule(s); "
            f"existing {result.existing}, skipped {result.skipped}, "
            f"conflicts {result.conflicts}"
        )

    # Per-row failure warnings always surface — they're diagnostic, not informational.
    for err in result.error_details:
        logger.warning(
            f"⚠️  {err.get('name', '(unknown)')}: {err.get('reason', 'failed')}"
        )
    for conflict in result.conflict_details:
        logger.warning(
            f"👀 {conflict['name']}: {conflict['reason']} "
            f"Decide it with `moneybin transactions categorize rules resolve "
            f"{conflict['conflict_id']} --replace|--reprioritize N|--cancel`."
        )

    if result.skipped > 0:
        raise typer.Exit(1)


@app.command("delete")
def rules_delete(
    rule_id: str = typer.Argument(..., help="Rule ID to deactivate (soft-delete)"),
    reapply: bool = typer.Option(
        False,
        "--reapply",
        help="Re-evaluate transactions previously categorized by this rule",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Soft-delete (deactivate) a categorization rule by ID.

    The rule remains in the database with is_active=false. Use --reapply to
    strip categorizations written by this rule and re-evaluate those rows
    against remaining active matchers.
    """
    from moneybin.services.categorization import (  # noqa: PLC0415 — defer import; CLI cold-start hygiene
        CategorizationService,
    )

    with handle_cli_errors(cli_actor="rules_delete"):
        with get_database(read_only=False) as db:
            deactivated = CategorizationService(db).deactivate_rule(
                rule_id, reapply=reapply, actor="cli"
            )
        if not deactivated:
            raise UserError(
                f"Rule {rule_id} not found", code=error_codes.TAXONOMY_RULE_NOT_FOUND
            )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data={"rule_id": rule_id, "action": "deactivated"},
                sensitivity="low",
            ),
            output,
            cli_actor="rules_delete",
        )
        return

    if not quiet:
        logger.info(f"✅ Rule {rule_id} deactivated")


@app.command("list-conflicts")
def rules_list_conflicts(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Show categorization rules refused because another rule owns the matcher."""
    from moneybin.services.categorization import (  # noqa: PLC0415 — defer import; CLI cold-start hygiene
        CategorizationService,
    )

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            conflicts = CategorizationService(db).list_rule_conflicts()

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=[
                    {
                        "conflict_id": row["conflict_id"],
                        "merchant_pattern": row["proposed_merchant_pattern"],
                        "match_type": row["proposed_match_type"],
                        "existing_rule_id": row["existing_rule_id"],
                        "existing_category": row["existing_category"],
                        "existing_subcategory": row["existing_subcategory"],
                        "proposed_name": row["proposed_name"],
                        "proposed_category": row["proposed_category"],
                        "proposed_subcategory": row["proposed_subcategory"],
                        "proposed_priority": row["proposed_priority"],
                    }
                    for row in conflicts
                ],
                sensitivity="medium",
            ),
            output,
        )
        return

    if not conflicts:
        if not quiet:
            logger.info("No rule conflicts awaiting a decision.")
        return

    if not quiet:
        logger.info("Rule conflicts awaiting a decision:")
    for row in conflicts:
        logger.info(
            f"  👀 [{row['conflict_id']}] '{row['proposed_merchant_pattern']}' "
            f"({row['proposed_match_type']}): rule {row['existing_rule_id']} "
            f"assigns {_label(row['existing_category'], row['existing_subcategory'])}, "
            f"'{row['proposed_name']}' wanted "
            f"{_label(row['proposed_category'], row['proposed_subcategory'])}"
        )


def _label(category: object, subcategory: object) -> str:
    """Render a category pair the way the rest of the surface shows it."""
    return f"{category} / {subcategory}" if subcategory else str(category)


def _load_resolution_file(from_file: Path) -> list[object]:
    """Read a batch resolution file, mapping every read failure to an exit code."""
    try:
        with from_file.open(encoding="utf-8") as f:
            loaded = json.load(f)
    except FileNotFoundError as e:
        typer.echo(f"❌ File not found: {from_file}", err=True)
        raise typer.Exit(2) from e
    except json.JSONDecodeError as e:
        typer.echo(f"❌ Invalid JSON in {from_file}: {e}", err=True)
        raise typer.Exit(1) from e
    except OSError as e:
        # PermissionError, IsADirectoryError, broken-mount OSError, etc.
        typer.echo(f"❌ Cannot read {from_file}: {e}", err=True)
        raise typer.Exit(2) from e
    if not isinstance(loaded, list):
        raise typer.BadParameter(
            "--from-file must point at a JSON list of resolution dicts"
        )
    return cast("list[object]", loaded)


_RESOLUTIONS: tuple[str, ...] = ("replace", "reprioritize", "cancel")


def _decision_from_row(index: int, row: object) -> "ConflictDecision":
    """Validate one batch-file row into a typed decision, naming its position."""
    from moneybin.services.categorization import (  # noqa: PLC0415 — defer import; CLI cold-start hygiene
        ConflictDecision,
    )

    if not isinstance(row, dict):
        raise typer.BadParameter(f"Row {index} is not an object")
    fields = cast("dict[str, object]", row)
    conflict_id = fields.get("conflict_id")
    resolution = fields.get("resolution")
    priority = fields.get("priority")
    if not isinstance(conflict_id, str) or not conflict_id:
        raise typer.BadParameter(f"Row {index} is missing a conflict_id")
    if resolution not in _RESOLUTIONS:
        raise typer.BadParameter(
            f"Row {index} resolution must be one of {', '.join(_RESOLUTIONS)}"
        )
    narrowed = cast('Literal["replace", "reprioritize", "cancel"]', resolution)
    if priority is not None and not isinstance(priority, int):
        raise typer.BadParameter(f"Row {index} priority must be an integer")
    return ConflictDecision(
        conflict_id=conflict_id,
        resolution=narrowed,
        priority=priority,
    )


def _resolution_from_flags(
    replace: bool, reprioritize: int | None, cancel: bool
) -> tuple[Literal["replace", "reprioritize", "cancel"], int | None]:
    """Narrow the three mutually exclusive resolution flags to one decision."""
    chosen = [
        flag
        for flag, given in (
            ("--replace", replace),
            ("--reprioritize", reprioritize is not None),
            ("--cancel", cancel),
        )
        if given
    ]
    if len(chosen) != 1:
        raise typer.BadParameter(
            "Choose exactly one of --replace, --reprioritize N, or --cancel."
        )
    if replace:
        return "replace", None
    if cancel:
        return "cancel", None
    return "reprioritize", reprioritize


@app.command("resolve")
def rules_resolve(
    conflict_id: str | None = typer.Argument(
        None, help="Conflict ID to resolve (omit when --from-file is used)"
    ),
    replace: bool = typer.Option(
        False,
        "--replace",
        help="Deactivate the existing rule and activate the refused one",
    ),
    reprioritize: int | None = typer.Option(
        None,
        "--reprioritize",
        help=(
            "Activate the refused rule beside the existing one at this priority "
            "(lower runs first)"
        ),
    ),
    cancel: bool = typer.Option(
        False, "--cancel", help="Discard the refused rule and keep live state"
    ),
    from_file: Path | None = typer.Option(
        None,
        "--from-file",
        help=(
            'JSON file with a list of {"conflict_id", "resolution", "priority"} dicts'
        ),
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Resolve categorization rule conflicts.

    Single conflict: pass CONFLICT_ID with exactly one of --replace,
    --reprioritize N, or --cancel. Batch: pass --from-file pointing at a JSON
    list of resolution dicts; the whole batch applies atomically or not at all.

    A conflict recorded against a rule that has since been edited is refused
    as stale — re-read the queue with `rules list-conflicts` and decide again.
    """
    from moneybin.services.categorization import (  # noqa: PLC0415 — defer import; CLI cold-start hygiene
        CategorizationService,
        ConflictDecision,
    )

    if from_file is not None:
        single_flags = {
            "CONFLICT_ID": conflict_id,
            "--replace": replace or None,
            "--reprioritize": reprioritize,
            "--cancel": cancel or None,
        }
        conflicting = [flag for flag, val in single_flags.items() if val is not None]
        if conflicting:
            raise typer.BadParameter(
                f"--from-file is mutually exclusive with single-conflict flags: "
                f"{', '.join(conflicting)}"
            )
        decisions = [
            _decision_from_row(index, row)
            for index, row in enumerate(_load_resolution_file(from_file))
        ]
    else:
        if not conflict_id:
            raise typer.BadParameter(
                "Single-conflict mode requires CONFLICT_ID, or use --from-file "
                "for a batch."
            )
        resolution, priority = _resolution_from_flags(replace, reprioritize, cancel)
        decisions = [
            ConflictDecision(
                conflict_id=conflict_id,
                resolution=resolution,
                priority=priority,
            )
        ]

    if not yes:
        verbs = ", ".join(sorted({d.resolution for d in decisions}))
        confirmed = typer.confirm(
            f"Apply {len(decisions)} rule-conflict resolution(s) ({verbs})?"
        )
        if not confirmed:
            logger.info("Resolution cancelled")
            raise typer.Exit(0)

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            results = CategorizationService(db).resolve_rule_conflicts(
                decisions, actor="cli"
            )

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data=[
                    {
                        "conflict_id": item.conflict_id,
                        "resolution": item.resolution,
                        "rule_id": item.rule_id,
                        "superseded_rule_id": item.superseded_rule_id,
                    }
                    for item in results
                ],
                sensitivity="low",
                actions=[
                    "Use `moneybin transactions categorize rules list` to review "
                    "the active rules"
                ],
            ),
            output,
        )
        return

    if not quiet:
        activated = sum(1 for item in results if item.rule_id is not None)
        superseded = sum(1 for item in results if item.superseded_rule_id is not None)
        logger.info(
            f"✅ Resolved {len(results)} conflict(s); "
            f"activated {activated}, superseded {superseded}"
        )

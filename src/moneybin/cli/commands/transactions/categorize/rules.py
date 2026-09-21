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
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
    wide_option,
)
from moneybin.cli.render import (
    build_rows,
    build_summary,
    column_view,
    compose_human_result,
    render_note,
)
from moneybin.cli.utils import abort_cli_error, get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.limits import RULE_PRIORITY_MAX, RULE_PRIORITY_MIN
from moneybin.protocol.envelope import build_envelope

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from moneybin.services.categorization import ConflictDecision, RuleCreationResult

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
    no_pager: bool = no_pager_option,
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

    policy = get_terminal_policy(no_pager=no_pager)
    if not rows:
        parts = [build_summary([("Result", "No active categorization rules.")])]
        disclosures = (
            "Next: moneybin transactions categorize rules create <name> --pattern <pattern> --category <category>",
        )
    else:
        parts = [
            build_summary(
                [("Active rules", f"{len(rows):,}")], title="Categorization rules"
            ),
            build_rows(
                ["rule id", "name", "pattern", "match", "category", "priority"],
                [
                    (
                        row.rule_id,
                        row.name,
                        row.merchant_pattern,
                        row.match_type,
                        _label(row.category, row.subcategory),
                        row.priority,
                    )
                    for row in rows
                ],
                numeric=("priority",),
                terminal=policy,
            ),
        ]
        disclosures = ()
    emit_human_result(
        compose_human_result(parts, disclosures=disclosures),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("apply")
def rules_apply() -> None:
    """Run all active rules against uncategorized transactions."""
    from moneybin.services.categorization import CategorizationService

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            result = CategorizationService(db).categorize_run(methods=["rules"])
            applied = result["total_applied"]
    emit_human_result(
        compose_human_result([
            build_summary(
                [("Categorized", f"{applied:,}"), ("Method", "rules")],
                title="Rule application complete",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


def _warn_rule_create_rows(result: "RuleCreationResult") -> None:
    """Report each failed and each refused row on stderr.

    Per-row warnings always surface — they're diagnostic, not informational —
    so neither takes `quiet`. Both go to stderr through `render_note` rather
    than the logger: each names the rule its author named, and the log pipeline
    persists to disk where `SanitizedLogFormatter` cannot recognize
    user-authored text.
    """
    for err in result.error_details:
        render_note(
            f"Attention: {err.get('name', '(unknown)')}: {err.get('reason', 'failed')}",
            warn=True,
        )
    for conflict in result.conflict_details:
        render_note(
            f"Attention: {conflict.name}: {conflict.reason} "
            f"Decide it with `moneybin transactions categorize rules resolve "
            f"{conflict.conflict_id} --replace|--reprioritize N|--cancel`.",
            warn=True,
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
    # defer import; CLI cold-start hygiene
    from moneybin.services.categorization import (
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
            abort_cli_error(
                e,
                output=output,
                exit_code=2,
                cli_actor="rules_create",
                message=f"File not found: {from_file}",
            )
        except json.JSONDecodeError as e:
            abort_cli_error(
                e,
                output=output,
                exit_code=1,
                cli_actor="rules_create",
                message=f"Invalid JSON in {from_file}: {e}",
            )
        except OSError as e:
            # PermissionError, IsADirectoryError, broken-mount OSError, etc.
            abort_cli_error(
                e,
                output=output,
                exit_code=2,
                cli_actor="rules_create",
                message=f"Cannot read {from_file}: {e}",
            )
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
        if result.conflicts > 0 and result.created == 0:
            # An error promises the call changed nothing. This batch routes
            # each row independently, so one call can create a rule *and*
            # refuse another; only a batch that wrote nothing fails, and
            # `data.conflicts` reports the refusals either way.
            #
            # The notes go out first: the refusal below names no rule — a rule
            # name is its author's text and this message reaches the logger —
            # so they carry the conflict id the resolve command needs.
            _warn_rule_create_rows(result)
            raise UserError(
                "A rule in this batch matches the same transactions as an "
                "active rule and assigns a different category.",
                code=error_codes.TAXONOMY_RULE_CONFLICT,
                details={"conflict_ids": list(result.conflict_ids)},
            )

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
        payload = result.to_payload()
        envelope = build_envelope(
            data=payload,
            sensitivity="low",
            # No total_count: this is a completed, non-paginated write, not a
            # partial page. len(rules) counts the submitted batch, not "how
            # many more exist" — pairing it with total_count made has_more=True
            # whenever a rule was skipped/refused rather than written (MB-175
            # review, matches the MCP tool's fix).
            returned_count=len(payload.rule_ids),
            actions=actions,
        )
        render_or_json(envelope, output, cli_actor="rules_create")
    else:
        emit_human_result(
            compose_human_result([
                build_summary(
                    [
                        ("Created", str(result.created)),
                        ("Existing", str(result.existing)),
                        ("Skipped", str(result.skipped)),
                        ("Conflicts", str(result.conflicts)),
                    ],
                    title=(
                        "Rules partially created"
                        if result.skipped or result.conflicts
                        else "Rules created"
                    ),
                )
            ]),
            policy=get_terminal_policy(),
            finite_read=False,
            receipt=True,
        )

    _warn_rule_create_rows(result)

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
    # defer import; CLI cold-start hygiene
    from moneybin.services.categorization import (
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

    emit_human_result(
        compose_human_result([
            build_summary(
                [("Rule ID", rule_id), ("Action", "Deactivated")], title="Rule deleted"
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


_CONFLICT_COLUMNS: tuple[
    tuple[str, "Callable[[Mapping[str, object]], object]"], ...
] = (
    ("conflict", lambda row: str(row["conflict_id"])),
    ("pattern", lambda row: str(row["proposed_merchant_pattern"])),
    ("match", lambda row: str(row["proposed_match_type"])),
    ("existing rule", lambda row: str(row["existing_rule_id"])),
    (
        "assigns",
        lambda row: _label(row["existing_category"], row["existing_subcategory"]),
    ),
    ("proposed rule", lambda row: str(row["proposed_name"])),
    (
        "wants",
        lambda row: _label(row["proposed_category"], row["proposed_subcategory"]),
    ),
    ("priority", lambda row: row["proposed_priority"]),
)

_CONFLICT_DEFAULT = ("conflict", "pattern", "assigns", "wants")
"""The id is what `rules resolve` takes, so it is never the column dropped.

A conflict id is 21 characters, which leaves an 80-column terminal room for
the pattern and the two categories in disagreement — the question the queue
exists to answer. Which rule and which proposal follow under `--wide`.
"""


@app.command("list-conflicts")
def rules_list_conflicts(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    wide: bool = wide_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Show categorization rules refused because another rule owns the matcher."""
    # defer import; CLI cold-start hygiene
    from moneybin.services.categorization import (
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

    policy = get_terminal_policy(no_pager=no_pager)
    if not conflicts:
        emit_human_result(
            compose_human_result(
                [build_summary([("Result", "No rule conflicts awaiting a decision.")])],
                disclosures=("Next: moneybin transactions categorize rules list",),
            ),
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
        )
        return

    # Rows go to stdout through `render_rows`, never through `logger`: the
    # pattern is merchant text and the proposed name is text its author wrote,
    # and the log pipeline persists to disk where `SanitizedLogFormatter`
    # cannot recognize either.
    view = column_view(
        _CONFLICT_COLUMNS, conflicts, default=_CONFLICT_DEFAULT, wide=wide
    )
    emit_human_result(
        compose_human_result(
            [
                build_summary(
                    [("Conflicts", f"{len(conflicts):,}")], title="Rule conflicts"
                ),
                build_rows(
                    view.names,
                    view.rows,
                    numeric=("priority",),
                    total_columns=view.total,
                    terminal=policy,
                ),
            ],
            disclosures=(
                "Next: moneybin transactions categorize rules resolve <conflict-id> --replace",
            ),
        ),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
        wide=wide,
    )


def _label(category: object, subcategory: object) -> str:
    """Render a category pair the way the rest of the surface shows it."""
    return f"{category} / {subcategory}" if subcategory else str(category)


def _load_resolution_file(from_file: Path, *, output: OutputFormat) -> list[object]:
    """Read a batch resolution file, mapping every read failure to an exit code."""
    try:
        with from_file.open(encoding="utf-8") as f:
            loaded = json.load(f)
    except FileNotFoundError as e:
        abort_cli_error(
            e, output=output, exit_code=2, message=f"File not found: {from_file}"
        )
    except json.JSONDecodeError as e:
        abort_cli_error(
            e, output=output, exit_code=1, message=f"Invalid JSON in {from_file}: {e}"
        )
    except OSError as e:
        # PermissionError, IsADirectoryError, broken-mount OSError, etc.
        abort_cli_error(
            e, output=output, exit_code=2, message=f"Cannot read {from_file}: {e}"
        )
    if not isinstance(loaded, list):
        raise typer.BadParameter(
            "--from-file must point at a JSON list of resolution dicts"
        )
    return cast("list[object]", loaded)


_RESOLUTIONS: tuple[str, ...] = ("replace", "reprioritize", "cancel")


def _decision_from_row(index: int, row: object) -> "ConflictDecision":
    """Validate one batch-file row into a typed decision, naming its position."""
    # defer import; CLI cold-start hygiene
    from moneybin.services.categorization import (
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
    # `isinstance(True, int)` is True, so the bool arm has to come first or a
    # JSON `true` reaches the repository as priority 1.
    if priority is not None and (
        isinstance(priority, bool) or not isinstance(priority, int)
    ):
        raise typer.BadParameter(f"Row {index} priority must be an integer")
    if priority is not None and not (
        RULE_PRIORITY_MIN <= priority <= RULE_PRIORITY_MAX
    ):
        raise typer.BadParameter(
            f"Row {index} priority must be between {RULE_PRIORITY_MIN} and "
            f"{RULE_PRIORITY_MAX}"
        )
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
        min=RULE_PRIORITY_MIN,
        max=RULE_PRIORITY_MAX,
        help=(
            "Activate the refused rule beside the existing one at this priority "
            f"({RULE_PRIORITY_MIN}-{RULE_PRIORITY_MAX}, lower runs first)"
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
    # defer import; CLI cold-start hygiene
    from moneybin.services.categorization import (
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
            for index, row in enumerate(_load_resolution_file(from_file, output=output))
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
            emit_human_result(
                compose_human_result([
                    build_summary(
                        [("Saved state", "No rule conflicts were changed")],
                        title="Rule conflict resolution cancelled",
                    )
                ]),
                policy=get_terminal_policy(),
                finite_read=False,
                receipt=True,
            )
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
                        "superseded_rule_ids": item.superseded_rule_ids,
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

    activated = sum(1 for item in results if item.rule_id is not None)
    superseded = sum(len(item.superseded_rule_ids) for item in results)
    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    ("Resolved", str(len(results))),
                    ("Activated", str(activated)),
                    ("Superseded", str(superseded)),
                ],
                title="Rule conflicts resolved",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )

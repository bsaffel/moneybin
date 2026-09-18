"""Data transformation commands for MoneyBin CLI.

This module provides commands for running SQLMesh transformations on the
loaded DuckDB data. ``plan``/``apply``/``status``/``validate``/``audit``
route through ``TransformService`` so the CLI and MCP layers share the same
business logic and the same response envelope. ``restate`` keeps the direct
``sqlmesh_context()`` path — it's operator-only and has no MCP equivalent.
"""

import sys
from collections.abc import Sequence

import typer

from moneybin import error_codes
from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
)
from moneybin.cli.progress import operation_progress
from moneybin.cli.render import build_summary, compose_human_result
from moneybin.cli.utils import (
    emit_json_failure,
    get_terminal_policy,
    handle_cli_errors,
    sqlmesh_command,
)
from moneybin.database import sqlmesh_context
from moneybin.errors import UserError
from moneybin.progress import ProgressEvent

app = typer.Typer(help="Run data transformations", no_args_is_help=True)


def _emit_transform_text(
    title: str,
    pairs: Sequence[tuple[str, object]],
    *,
    finite_read: bool,
    no_pager: bool = False,
) -> None:
    """Emit the command result through the shared human-result boundary."""
    emit_human_result(
        compose_human_result([
            build_summary([(key, str(value)) for key, value in pairs], title=title)
        ]),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=finite_read,
        no_pager=no_pager,
        receipt=not finite_read,
    )


@app.command("plan")
def transform_plan(
    auto_apply: bool = typer.Option(
        False, "--apply", "-a", help="Automatically apply the plan"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Preview pending transform changes (and optionally apply them).

    Shows which models would be rebuilt based on changes since the last run.
    Use --apply to apply the plan immediately.
    """
    if auto_apply:
        # Delegate so source-priority seeding (run_transforms) happens before
        # ctx.plan; calling ctx.plan(auto_apply=True) directly would skip
        # seeding and risk NULL-winning merges in core fields.
        transform_apply(output=output, quiet=quiet)
        return

    from moneybin.cli.output import render_or_json
    from moneybin.database import get_database
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.transform_service import TransformService

    with handle_cli_errors(), get_database(read_only=True) as db:
        plan = TransformService(db).plan()

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data={
                    "has_changes": plan.has_changes,
                    "directly_modified": plan.directly_modified,
                    "indirectly_modified": plan.indirectly_modified,
                    "added": plan.added,
                    "removed": plan.removed,
                },
                sensitivity="low",
            ),
            output,
            cli_actor="transform_plan",
        )
        return

    if not plan.has_changes:
        _emit_transform_text(
            "Transform plan",
            [("Result", "No pending changes")],
            finite_read=False,
        )
        return
    pairs: list[tuple[str, object]] = []
    if plan.directly_modified:
        pairs.append(("Directly modified", ", ".join(plan.directly_modified)))
    if plan.indirectly_modified:
        pairs.append(("Indirectly modified", ", ".join(plan.indirectly_modified)))
    if plan.added:
        pairs.append(("Added", ", ".join(plan.added)))
    if plan.removed:
        pairs.append(("Removed", ", ".join(plan.removed)))
    pairs.append(("Next step", "`moneybin transform apply`"))
    _emit_transform_text("Pending transform changes", pairs, finite_read=False)


@app.command("apply")
def transform_apply(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Apply all pending transform changes.

    Equivalent to 'moneybin transform plan --apply'. Rebuilds only changed
    models since the last run.
    """
    from moneybin.cli.output import render_or_json
    from moneybin.database import get_database
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.transform_service import TransformService

    terminal = get_terminal_policy()
    try:
        with (
            handle_cli_errors(cli_actor="transform_apply"),
            get_database(read_only=False, operation_type="transform_apply") as db,
        ):
            with operation_progress(terminal, quiet=quiet) as report:
                report(ProgressEvent("Applying transforms"))
                result = TransformService(db).apply()
    except KeyboardInterrupt:
        if output == OutputFormat.JSON:
            emit_json_failure(
                UserError(
                    "Transform apply cancelled; saved scope is unknown",
                    code=error_codes.REFRESH_MODEL_FAILED,
                    hint="Run 'moneybin transform status' to inspect derived data.",
                    details={"saved_scope": "unknown", "outcome": "cancelled"},
                ),
                cli_actor="transform_apply",
            )
        else:
            emit_human_result(
                compose_human_result([
                    build_summary(
                        [
                            ("Saved state", "Saved scope is unknown"),
                            ("Next step", "`moneybin transform status`"),
                        ],
                        title="Transform apply cancelled",
                    )
                ]),
                policy=terminal,
                finite_read=False,
                receipt=True,
            )
        raise typer.Exit(130) from None

    if output == OutputFormat.JSON:
        data: dict[str, object] = {
            "applied": result.applied,
            "duration_seconds": result.duration_seconds,
        }
        if result.error is not None:
            data["error"] = result.error
        render_or_json(
            build_envelope(data=data, sensitivity="low"),
            output,
            cli_actor="transform_apply",
        )
        # JSON mode must still exit non-zero on failure so scripts can detect
        # it from $?; the envelope alone isn't enough for shell pipelines.
        if not result.applied:
            raise typer.Exit(1)
        return

    if result.applied:
        _emit_transform_text(
            "Transforms applied",
            [("Outcome", "Derived tables rebuilt")],
            finite_read=False,
        )
    else:
        _emit_transform_text(
            "Transforms failed",
            [("Failure", result.error or "Unknown failure")],
            finite_read=False,
        )
        raise typer.Exit(1)


@app.command("seed")
def transform_seed() -> None:
    """Materialize seed models and propagate to app tables.

    Re-runs the seed step in isolation — useful after editing a seed CSV
    or restoring deleted defaults. ``moneybin db init`` and ``moneybin
    transform apply`` already run this implicitly.
    """
    from moneybin.seeds import materialize_seeds

    with sqlmesh_command("Seed materialization", success="Seeds materialized") as db:
        materialize_seeds(db)


@app.command("status")
def transform_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Show current model state and environment."""
    from moneybin.cli.output import render_or_json
    from moneybin.database import get_database
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.transform_service import TransformService

    with handle_cli_errors(), get_database(read_only=True) as db:
        status = TransformService(db).status()

    if output == OutputFormat.JSON:
        actions: list[str] = []
        if status.pending:
            actions.append(
                "Run `moneybin refresh --step transform` "
                "(or `refresh_run(steps=['transform'])` on MCP) to refresh derived tables"
            )
        render_or_json(
            build_envelope(
                data={
                    "environment": status.environment,
                    "initialized": status.initialized,
                    "last_apply_at": (
                        status.last_apply_at.isoformat()
                        if status.last_apply_at is not None
                        else None
                    ),
                    "pending": status.pending,
                    "latest_import_at": (
                        status.latest_import_at.isoformat()
                        if status.latest_import_at is not None
                        else None
                    ),
                },
                sensitivity="low",
                actions=actions,
            ),
            output,
            cli_actor="transform_status",
        )
        return

    if not status.initialized:
        _emit_transform_text(
            "Transform status",
            [
                ("Result", "No transform environment initialized yet"),
                ("Next step", "`moneybin transform apply`"),
            ],
            finite_read=True,
            no_pager=no_pager,
        )
        return
    pairs = [
        ("Environment", status.environment),
        (
            "Last apply",
            status.last_apply_at.strftime("%Y-%m-%d %H:%M:%S")
            if status.last_apply_at
            else "never finalized",
        ),
        ("Pending", status.pending),
    ]
    if status.pending:
        pairs.append(("Next step", "`moneybin transform apply`"))
    _emit_transform_text("Transform status", pairs, finite_read=True, no_pager=no_pager)


@app.command("validate")
def transform_validate(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Check that model SQL parses and resolves without errors."""
    from moneybin.cli.output import render_or_json
    from moneybin.database import get_database
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.transform_service import TransformService

    with handle_cli_errors(), get_database(read_only=True) as db:
        result = TransformService(db).validate()

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data={"valid": result.valid, "errors": result.errors},
                sensitivity="low",
            ),
            output,
            cli_actor="transform_validate",
        )
        if not result.valid:
            raise typer.Exit(1)
        return

    if result.valid:
        _emit_transform_text(
            "Transform validation",
            [("Result", "All models valid")],
            finite_read=True,
            no_pager=no_pager,
        )
        return
    _emit_transform_text(
        "Transform validation failed",
        [
            (str(err.get("model", "<unknown>")), str(err.get("message", "")))
            for err in result.errors
        ],
        finite_read=True,
        no_pager=no_pager,
    )
    raise typer.Exit(1)


@app.command("audit")
def transform_audit(
    start: str = typer.Option(
        ..., "--start", help="Start date for audit window (YYYY-MM-DD)"
    ),
    end: str = typer.Option(
        ..., "--end", help="End date for audit window (YYYY-MM-DD)"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Run data quality assertions defined in transform models."""
    from moneybin.cli.output import render_or_json
    from moneybin.database import get_database
    from moneybin.protocol.envelope import build_envelope
    from moneybin.services.transform_service import TransformService

    with handle_cli_errors(), get_database(read_only=False) as db:
        result = TransformService(db).audit(start, end)

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data={
                    "passed": result.passed,
                    "failed": result.failed,
                    "audits": result.audits,
                },
                sensitivity="low",
            ),
            output,
            cli_actor="transform_audit",
        )
        if result.failed:
            raise typer.Exit(1)
        return

    pairs: list[tuple[str, object]] = [
        ("Passed", result.passed),
        ("Failed", result.failed),
    ]
    pairs.extend(
        (
            str(row.get("name", "<unknown>")),
            f"{row.get('status', 'unknown')}: {row.get('detail', '')}",
        )
        for row in result.audits
    )
    _emit_transform_text("Transform audit", pairs, finite_read=True, no_pager=no_pager)
    if result.failed:
        raise typer.Exit(1)


@app.command("restate")
def transform_restate(
    model: str = typer.Option(
        ..., "--model", help="Model name (e.g., core.fct_transactions)"
    ),
    start: str = typer.Option(
        ..., "--start", help="Start date for restatement (YYYY-MM-DD)"
    ),
    end: str | None = typer.Option(None, "--end", help="End date (defaults to today)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
) -> None:
    """Force recompute a model for a date range."""
    if output == OutputFormat.JSON or not yes and not sys.stdin.isatty():
        message = "Restate requires an interactive confirmation or --yes."
        if output == OutputFormat.JSON:
            from moneybin.cli.utils import emit_json_failure

            emit_json_failure(
                UserError(
                    "Restate confirmation is required",
                    code=error_codes.MUTATION_CONFIRMATION_REQUIRED,
                    hint=message,
                ),
                cli_actor="transform_restate",
            )
        else:
            typer.echo(message, err=True)
        raise typer.Exit(2)
    if not yes:
        confirm = typer.confirm(
            f"Restate {model} from {start}? This will recompute all affected data."
        )
        if not confirm:
            _emit_transform_text(
                "Restate cancelled",
                [("Outcome", "No restatement was requested")],
                finite_read=False,
            )
            return
    with (
        sqlmesh_command(f"Restating {model}", success=f"Restated {model}") as db,
        sqlmesh_context(db) as ctx,
    ):
        ctx.plan(
            restate_models=[model],
            start=start,
            end=end,
            auto_apply=True,
            no_prompts=True,
        )

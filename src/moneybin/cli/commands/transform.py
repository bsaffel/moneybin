"""Data transformation commands for MoneyBin CLI.

This module provides commands for running SQLMesh transformations on the
loaded DuckDB data. ``plan``/``apply``/``status``/``validate``/``audit``
route through ``TransformService`` so the CLI and MCP layers share the same
business logic and the same response envelope. ``restate`` keeps the direct
``sqlmesh_context()`` path — it's operator-only and has no MCP equivalent.
"""

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors, sqlmesh_command
from moneybin.database import sqlmesh_context

app = typer.Typer(help="Run data transformations using SQLMesh", no_args_is_help=True)
logger = logging.getLogger(__name__)


@app.command("plan")
def transform_plan(
    auto_apply: bool = typer.Option(
        False, "--apply", "-a", help="Automatically apply the plan"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Preview pending SQLMesh changes (and optionally apply them).

    Shows which models would be rebuilt based on changes since the last run.
    Use --apply to apply the plan immediately.
    """
    if auto_apply:
        # Delegate so source-priority seeding (run_transforms) happens before
        # ctx.plan; calling ctx.plan(auto_apply=True) directly would skip
        # seeding and risk NULL-winning merges in core fields.
        transform_apply(output=output, quiet=quiet)
        return

    from moneybin.cli.output import render_or_json  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.protocol.envelope import build_envelope  # noqa: PLC0415
    from moneybin.services.transform_service import TransformService  # noqa: PLC0415

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
        )
        return

    if quiet:
        return
    if not plan.has_changes:
        logger.info("No pending changes")
        return
    logger.info("Pending SQLMesh changes:")
    if plan.directly_modified:
        logger.info(f"  Directly modified: {', '.join(plan.directly_modified)}")
    if plan.indirectly_modified:
        logger.info(f"  Indirectly modified: {', '.join(plan.indirectly_modified)}")
    if plan.added:
        logger.info(f"  Added: {', '.join(plan.added)}")
    if plan.removed:
        logger.info(f"  Removed: {', '.join(plan.removed)}")
    logger.info("💡 Run 'moneybin transform apply' to apply these changes")


@app.command("apply")
def transform_apply(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Apply all pending SQLMesh changes.

    Equivalent to 'moneybin transform plan --apply'. Rebuilds only changed
    models since the last run.
    """
    from moneybin.cli.output import render_or_json  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.protocol.envelope import build_envelope  # noqa: PLC0415
    from moneybin.services.transform_service import TransformService  # noqa: PLC0415

    with handle_cli_errors(), get_database() as db:
        result = TransformService(db).apply()

    if output == OutputFormat.JSON:
        data: dict[str, object] = {
            "applied": result.applied,
            "duration_seconds": result.duration_seconds,
        }
        if result.error is not None:
            data["error"] = result.error
        render_or_json(build_envelope(data=data, sensitivity="low"), output)
        # JSON mode must still exit non-zero on failure so scripts can detect
        # it from $?; the envelope alone isn't enough for shell pipelines.
        if not result.applied:
            raise typer.Exit(1)
        return

    if quiet:
        if not result.applied:
            raise typer.Exit(1)
        return
    if result.applied:
        logger.info(f"✅ SQLMesh transforms applied in {result.duration_seconds:.2f}s")
    else:
        logger.error(f"❌ SQLMesh transforms failed: {result.error}")
        raise typer.Exit(1)


@app.command("seed")
def transform_seed() -> None:
    """Materialize SQLMesh seed models and propagate to app tables.

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
) -> None:
    """Show current model state and environment."""
    from moneybin.cli.output import render_or_json  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.protocol.envelope import build_envelope  # noqa: PLC0415
    from moneybin.services.transform_service import TransformService  # noqa: PLC0415

    with handle_cli_errors(), get_database(read_only=True) as db:
        status = TransformService(db).status()

    if output == OutputFormat.JSON:
        actions: list[str] = []
        if status.pending:
            actions.append("Run transform_apply to refresh derived tables")
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
        )
        return

    if quiet:
        return
    if not status.initialized:
        logger.info("No SQLMesh environment initialized yet")
        logger.info("💡 Run 'moneybin transform apply' to initialize")
        return
    logger.info(f"Environment: {status.environment}")
    if status.last_apply_at is not None:
        logger.info(f"  Last apply: {status.last_apply_at:%Y-%m-%d %H:%M:%S}")
    else:
        logger.info("  Last apply: never finalized")
    logger.info(f"  Pending: {status.pending}")
    if status.pending:
        logger.info("💡 Run 'moneybin transform apply' to refresh derived tables")


@app.command("validate")
def transform_validate(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Check that model SQL parses and resolves without errors."""
    from moneybin.cli.output import render_or_json  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.protocol.envelope import build_envelope  # noqa: PLC0415
    from moneybin.services.transform_service import TransformService  # noqa: PLC0415

    with handle_cli_errors(), get_database(read_only=True) as db:
        result = TransformService(db).validate()

    if output == OutputFormat.JSON:
        render_or_json(
            build_envelope(
                data={"valid": result.valid, "errors": result.errors},
                sensitivity="low",
            ),
            output,
        )
        if not result.valid:
            raise typer.Exit(1)
        return

    if result.valid:
        if not quiet:
            logger.info("✅ All models valid")
        return
    for err in result.errors:
        logger.error(f"❌ {err.get('model', '<unknown>')}: {err.get('message', '')}")
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
) -> None:
    """Run data quality assertions defined in SQLMesh models."""
    from moneybin.cli.output import render_or_json  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.protocol.envelope import build_envelope  # noqa: PLC0415
    from moneybin.services.transform_service import TransformService  # noqa: PLC0415

    with handle_cli_errors(), get_database() as db:
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
        )
        if result.failed:
            raise typer.Exit(1)
        return

    if not quiet:
        logger.info(f"Audits: {result.passed} passed, {result.failed} failed")
        for audit_row in result.audits:
            status_str = audit_row.get("status", "")
            name = audit_row.get("name", "<unknown>")
            detail = audit_row.get("detail")
            if status_str == "failed":
                logger.error(f"❌ {name}: {detail}")
            elif not quiet:
                logger.info(f"  ✅ {name}")
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
) -> None:
    """Force recompute a model for a date range."""
    if not yes:
        confirm = typer.confirm(
            f"Restate {model} from {start}? This will recompute all affected data."
        )
        if not confirm:
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

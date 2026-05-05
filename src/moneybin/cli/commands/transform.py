"""Data transformation commands for MoneyBin CLI.

This module provides commands for running SQLMesh transformations on the
loaded DuckDB data.
"""

import logging
from datetime import UTC, datetime

import typer

from moneybin.cli.utils import sqlmesh_command
from moneybin.database import sqlmesh_context

app = typer.Typer(help="Run data transformations using SQLMesh", no_args_is_help=True)
logger = logging.getLogger(__name__)


@app.command("plan")
def transform_plan(
    auto_apply: bool = typer.Option(
        False, "--apply", "-a", help="Automatically apply the plan"
    ),
) -> None:
    """Preview pending SQLMesh changes (and optionally apply them).

    Shows which models would be rebuilt based on changes since the last run.
    Use --apply to apply the plan immediately.
    """
    if auto_apply:
        # Delegate so source-priority seeding (run_transforms) happens before
        # ctx.plan; calling ctx.plan(auto_apply=True) directly would skip
        # seeding and risk NULL-winning merges in core fields.
        transform_apply()
        return

    with sqlmesh_command("SQLMesh plan"), sqlmesh_context() as ctx:
        ctx.plan(auto_apply=False, no_prompts=False)


@app.command("apply")
def transform_apply() -> None:
    """Apply all pending SQLMesh changes.

    Equivalent to 'moneybin transform plan --apply'. Rebuilds only changed
    models since the last run.
    """
    from moneybin.services.import_service import ImportService

    with sqlmesh_command("SQLMesh apply", success="SQLMesh transforms applied") as db:
        ImportService(db).run_transforms()


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
def transform_status() -> None:
    """Show current model state and environment."""
    with sqlmesh_command("SQLMesh status check"), sqlmesh_context() as ctx:
        env = ctx.state_reader.get_environment("prod")
        if env:
            logger.info("Environment: prod")
            if env.finalized_ts is not None:
                finalized = datetime.fromtimestamp(
                    env.finalized_ts / 1000, tz=UTC
                ).astimezone()
                logger.info(f"  Last updated: {finalized:%Y-%m-%d %H:%M:%S %Z}")
            else:
                logger.info("  Last updated: never finalized")
        else:
            logger.info("No SQLMesh environment initialized yet")
            logger.info("💡 Run 'moneybin transform apply' to initialize")


@app.command("validate")
def transform_validate() -> None:
    """Check that model SQL parses and resolves without errors."""
    with (
        sqlmesh_command("SQLMesh validation", success="All models valid"),
        sqlmesh_context() as ctx,
    ):
        ctx.plan(no_prompts=True, auto_apply=False)


@app.command("audit")
def transform_audit(
    start: str = typer.Option(
        ..., "--start", help="Start date for audit window (YYYY-MM-DD)"
    ),
    end: str = typer.Option(
        ..., "--end", help="End date for audit window (YYYY-MM-DD)"
    ),
) -> None:
    """Run data quality assertions defined in SQLMesh models."""
    with (
        sqlmesh_command("SQLMesh audit", success="All audits passed"),
        sqlmesh_context() as ctx,
    ):
        ctx.audit(start=start, end=end)


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
        sqlmesh_command(f"Restating {model}", success=f"Restated {model}"),
        sqlmesh_context() as ctx,
    ):
        ctx.plan(
            restate_models=[model],
            start=start,
            end=end,
            auto_apply=True,
            no_prompts=True,
        )

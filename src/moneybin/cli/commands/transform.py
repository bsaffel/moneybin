"""Data transformation commands for MoneyBin CLI.

This module provides commands for running SQLMesh transformations on the
loaded DuckDB data.
"""

import logging

import typer

from moneybin.cli.utils import handle_database_errors
from moneybin.database import sqlmesh_context

app = typer.Typer(help="Run data transformations using SQLMesh", no_args_is_help=True)
logger = logging.getLogger(__name__)


@app.command("plan")
def plan_transforms(
    auto_apply: bool = typer.Option(
        False, "--apply", "-a", help="Automatically apply the plan"
    ),
) -> None:
    """Preview pending SQLMesh changes (and optionally apply them).

    Shows which models would be rebuilt based on changes since the last run.
    Use --apply to apply the plan immediately.
    """
    logger.info("⚙️  Running SQLMesh plan...")

    try:
        with handle_database_errors():
            with sqlmesh_context() as ctx:
                ctx.plan(auto_apply=auto_apply, no_prompts=auto_apply)
        logger.info("✅ SQLMesh plan completed")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error(f"❌ SQLMesh plan failed: {e}")
        raise typer.Exit(1) from e


@app.command("apply")
def apply_transforms() -> None:
    """Apply all pending SQLMesh changes.

    Equivalent to 'moneybin transform plan --apply'. Rebuilds only changed
    models since the last run.
    """
    logger.info("⚙️  Applying SQLMesh transforms...")

    try:
        with handle_database_errors():
            with sqlmesh_context() as ctx:
                ctx.plan(auto_apply=True, no_prompts=True)
        logger.info("✅ SQLMesh transforms applied")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error(f"❌ SQLMesh apply failed: {e}")
        raise typer.Exit(1) from e


@app.command("status")
def transform_status() -> None:
    """Show current model state and environment."""
    logger.info("⚙️  Checking SQLMesh status...")
    try:
        with handle_database_errors():
            with sqlmesh_context() as ctx:
                env = ctx.state_reader.get_environment("prod")
                if env:
                    logger.info("Environment: prod")
                    logger.info(f"  Last updated: {env.expiration_ts}")
                else:
                    logger.info("No SQLMesh environment initialized yet")
                    logger.info("💡 Run 'moneybin transform apply' to initialize")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error(f"❌ SQLMesh status failed: {e}")
        raise typer.Exit(1) from e


@app.command("validate")
def transform_validate() -> None:
    """Check that model SQL parses and resolves without errors."""
    logger.info("⚙️  Validating SQLMesh models...")
    try:
        with handle_database_errors():
            with sqlmesh_context() as ctx:
                ctx.plan(no_prompts=True, auto_apply=False)
        logger.info("✅ All models valid")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error(f"❌ Validation failed: {e}")
        raise typer.Exit(1) from e


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
    logger.info("⚙️  Running SQLMesh audits...")
    try:
        with handle_database_errors():
            with sqlmesh_context() as ctx:
                ctx.audit(start=start, end=end)
        logger.info("✅ All audits passed")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error(f"❌ Audit failed: {e}")
        raise typer.Exit(1) from e


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
    logger.info(f"⚙️  Restating {model} from {start}...")
    try:
        with handle_database_errors():
            with sqlmesh_context() as ctx:
                ctx.plan(
                    restate_models=[model],
                    start=start,
                    end=end,
                    auto_apply=True,
                    no_prompts=True,
                )
        logger.info(f"✅ Restated {model}")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error(f"❌ Restatement failed: {e}")
        raise typer.Exit(1) from e

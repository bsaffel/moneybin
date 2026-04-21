"""Data transformation commands for MoneyBin CLI.

This module provides commands for running SQLMesh transformations on the
loaded DuckDB data.
"""

import logging
from pathlib import Path

import typer

from moneybin.config import get_database_path

# Context is imported at module level so tests can patch
# moneybin.cli.commands.transform.Context. SQLMesh has no type stubs.
from sqlmesh import Context  # type: ignore[import-untyped] — sqlmesh has no type stubs

app = typer.Typer(help="Run data transformations using SQLMesh", no_args_is_help=True)
logger = logging.getLogger(__name__)

_SQLMESH_ROOT = Path(__file__).resolve().parents[4] / "sqlmesh"


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
    db_path = get_database_path()
    logger.info("Running SQLMesh plan against %s", db_path)

    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        ctx.plan(auto_apply=auto_apply, no_prompts=auto_apply)
        logger.info("✅ SQLMesh plan completed")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error("❌ SQLMesh plan failed: %s", e)
        raise typer.Exit(1) from e


@app.command("apply")
def apply_transforms() -> None:
    """Apply all pending SQLMesh changes.

    Equivalent to 'moneybin transform plan --apply'. Rebuilds only changed
    models since the last run.
    """
    db_path = get_database_path()
    logger.info("Applying SQLMesh transforms against %s", db_path)

    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        ctx.plan(auto_apply=True, no_prompts=True)
        logger.info("✅ SQLMesh transforms applied")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error("❌ SQLMesh apply failed: %s", e)
        raise typer.Exit(1) from e


@app.command("status")
def transform_status() -> None:
    """Show current model state and environment."""
    logger.info("⚙️  Checking SQLMesh status...")
    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        env = ctx.state_reader.get_environment("prod")
        if env:
            logger.info("Environment: prod")
            logger.info("  Last updated: %s", env.expiration_ts)
        else:
            logger.info("No SQLMesh environment initialized yet")
            logger.info("💡 Run 'moneybin transform apply' to initialize")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error("❌ SQLMesh status failed: %s", e)
        raise typer.Exit(1) from e


@app.command("validate")
def transform_validate() -> None:
    """Check that model SQL parses and resolves without errors."""
    logger.info("⚙️  Validating SQLMesh models...")
    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        ctx.plan(no_prompts=True, auto_apply=False)
        logger.info("✅ All models valid")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error("❌ Validation failed: %s", e)
        raise typer.Exit(1) from e


@app.command("audit")
def transform_audit() -> None:
    """Run data quality assertions defined in SQLMesh models."""
    logger.info("⚙️  Running SQLMesh audits...")
    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        ctx.audit()
        logger.info("✅ All audits passed")
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error("❌ Audit failed: %s", e)
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
    logger.info("⚙️  Restating %s from %s...", model, start)
    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        ctx.restate_model(model, start=start, end=end)
        ctx.plan(auto_apply=True, no_prompts=True)
        logger.info("✅ Restated %s", model)
    except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
        logger.error("❌ Restatement failed: %s", e)
        raise typer.Exit(1) from e

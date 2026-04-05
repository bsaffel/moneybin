"""Data transformation commands for MoneyBin CLI.

This module provides commands for running SQLMesh transformations on the
loaded DuckDB data.
"""

import logging
from pathlib import Path

import typer

from moneybin.config import get_database_path

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
    from sqlmesh import Context  # type: ignore[import-untyped]

    db_path = get_database_path()
    logger.info("Running SQLMesh plan against %s", db_path)

    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        ctx.plan(auto_apply=auto_apply, no_prompts=auto_apply)
        logger.info("✅ SQLMesh plan completed")
    except Exception as e:
        logger.error("❌ SQLMesh plan failed: %s", e)
        raise typer.Exit(1) from e


@app.command("apply")
def apply_transforms() -> None:
    """Apply all pending SQLMesh changes.

    Equivalent to 'moneybin transform plan --apply'. Rebuilds only changed
    models since the last run.
    """
    from sqlmesh import Context  # type: ignore[import-untyped]

    db_path = get_database_path()
    logger.info("Applying SQLMesh transforms against %s", db_path)

    try:
        ctx = Context(paths=str(_SQLMESH_ROOT))
        ctx.plan(auto_apply=True, no_prompts=True)
        logger.info("✅ SQLMesh transforms applied")
    except Exception as e:
        logger.error("❌ SQLMesh apply failed: %s", e)
        raise typer.Exit(1) from e

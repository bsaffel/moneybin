"""Refresh command for MoneyBin CLI.

CLI peer of the ``refresh_run`` MCP tool. Runs the post-load refresh
pipeline (matching → SQLMesh apply → categorization) via
``moneybin.services.refresh.refresh()``. Idempotent — safe to retry
after a failure.
"""

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors

logger = logging.getLogger(__name__)


def refresh_command(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    step: list[str] = typer.Option(
        None,
        "--step",
        help=(
            "Limit the cascade to one or more steps "
            "(repeatable; choose from match, transform, categorize). "
            "Default: full cascade. Steps always run in canonical order "
            "(match → transform → categorize) regardless of flag order."
        ),
    ),
) -> None:
    """Run the post-load refresh pipeline: matching, SQLMesh apply, categorization.

    Single user-facing entry point for refreshing derived state from raw
    inputs. Idempotent. Matching and categorization steps are best-effort
    and log-only on failure — only SQLMesh apply errors fail the command.
    """
    from moneybin.cli.output import render_or_json  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.mcp.adapters.refresh_adapters import (  # noqa: PLC0415
        refresh_envelope,
    )
    from moneybin.services.refresh import expand_steps, refresh  # noqa: PLC0415

    steps: list[str] | None = step if step else None

    with handle_cli_errors(), get_database() as db:
        result = refresh(db, steps=steps)

    if output == OutputFormat.JSON:
        render_or_json(refresh_envelope(result, requested=expand_steps(steps)), output)
        if result.error is not None:
            raise typer.Exit(1)
        return

    if quiet:
        if result.error is not None:
            raise typer.Exit(1)
        return
    if result.applied:
        duration = result.duration_seconds or 0.0
        logger.info(f"✅ Refresh complete in {duration:.2f}s")
        return
    if result.error is not None:
        logger.error(f"❌ Refresh failed: {result.error}")
        raise typer.Exit(1)
    logger.info("✅ Partial refresh complete (transform skipped)")

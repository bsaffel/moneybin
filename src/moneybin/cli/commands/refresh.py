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
) -> None:
    """Run the post-load refresh pipeline: matching, SQLMesh apply, categorization.

    Single user-facing entry point for refreshing derived state from raw
    inputs. Idempotent. Matching and categorization steps are best-effort
    and log-only on failure — only SQLMesh apply errors fail the command.
    """
    from moneybin.cli.output import render_or_json  # noqa: PLC0415
    from moneybin.database import get_database  # noqa: PLC0415
    from moneybin.protocol.envelope import build_envelope  # noqa: PLC0415
    from moneybin.services.refresh import refresh  # noqa: PLC0415

    with handle_cli_errors(), get_database() as db:
        result = refresh(db)

    if output == OutputFormat.JSON:
        data: dict[str, object] = {
            "applied": result.applied,
            "duration_seconds": result.duration_seconds,
        }
        if result.error is not None:
            data["error"] = result.error
        actions: list[str] = []
        if not result.applied and result.error is not None:
            actions.append(
                "SQLMesh apply failed — call transform_plan to inspect, "
                "or refresh_run to retry."
            )
        render_or_json(
            build_envelope(data=data, sensitivity="low", actions=actions),
            output,
        )
        if not result.applied:
            raise typer.Exit(1)
        return

    if quiet:
        if not result.applied:
            raise typer.Exit(1)
        return
    if result.applied:
        duration = result.duration_seconds or 0.0
        logger.info(f"✅ Refresh complete in {duration:.2f}s")
    else:
        logger.error(f"❌ Refresh failed: {result.error}")
        raise typer.Exit(1)

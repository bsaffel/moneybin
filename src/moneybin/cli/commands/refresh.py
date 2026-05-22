"""Refresh command for MoneyBin CLI.

CLI peer of the ``refresh_run`` MCP tool. Runs the post-load refresh
pipeline (matching → SQLMesh apply → categorization) via
``moneybin.services.refresh.refresh()``. Idempotent — safe to retry
after a failure.
"""

import logging
from enum import StrEnum

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors

logger = logging.getLogger(__name__)


class RefreshStepChoice(StrEnum):
    """Mirrors ``services.refresh.RefreshStep`` for Typer choice validation.

    Rejecting invalid step names at parse time surfaces a usage error
    (exit code 2) rather than a runtime UserError (exit code 1). The
    service-layer ``UNKNOWN_REFRESH_STEP`` check remains as
    defense-in-depth for programmatic callers.
    """

    MATCH = "match"
    TRANSFORM = "transform"
    CATEGORIZE = "categorize"


def refresh_command(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    step: list[RefreshStepChoice] = typer.Option(
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

    # StrEnum members compare equal to their string values, so downstream
    # service code that accepts ``list[str]`` works unchanged.
    steps: list[str] | None = [s.value for s in step] if step else None

    with handle_cli_errors(), get_database() as db:
        result = refresh(db, steps=steps)
    requested = expand_steps(steps)

    if output == OutputFormat.JSON:
        render_or_json(refresh_envelope(result, requested=requested), output)
        if result.error is not None:
            raise typer.Exit(1)
        return

    if quiet:
        if result.error is not None:
            raise typer.Exit(1)
        return

    # Best-effort step crashes (matcher/categorizer) don't fail the command,
    # but surface them so a partially-refreshed pipeline isn't silent.
    if result.matching_error is not None:
        logger.warning(f"⚠️  Matching step failed: {result.matching_error}")
    if result.categorization_error is not None:
        logger.warning(f"⚠️  Categorization step failed: {result.categorization_error}")
    if result.matching_error is not None or result.categorization_error is not None:
        logger.info(
            "💡 Re-run the failed step (e.g. `moneybin refresh --step match`) "
            "or run `moneybin system doctor` to diagnose."
        )

    if result.applied:
        duration = result.duration_seconds or 0.0
        logger.info(f"✅ Refresh complete in {duration:.2f}s")
        return
    if result.error is not None:
        logger.error(f"❌ Refresh failed: {result.error}")
        raise typer.Exit(1)
    logger.info(f"✅ Partial refresh complete (steps: {', '.join(sorted(requested))})")

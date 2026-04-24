# src/moneybin/cli/output.py
"""CLI output format support.

Provides ``--output json`` on all CLI commands that have a corresponding
MCP tool. When ``json`` is selected, the command returns the same
``{summary, data, actions}`` response envelope as the MCP tool.

Usage in a CLI command::

    from moneybin.cli.output import OutputFormat, output_option, render_or_json

    @app.command("summary")
    def summary_cmd(
        months: int = typer.Option(3),
        output: OutputFormat = output_option,
    ) -> None:
        service = SpendingService(get_database())
        result = service.summary(months=months)
        render_or_json(result.to_envelope(), output, render_fn=_render_table)
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from enum import StrEnum

import typer

from moneybin.mcp.envelope import ResponseEnvelope

logger = logging.getLogger(__name__)


class OutputFormat(StrEnum):
    """CLI output format."""

    TABLE = "table"
    JSON = "json"


output_option: OutputFormat = typer.Option(
    OutputFormat.TABLE,
    "--output",
    "-o",
    help="Output format: 'table' (human-readable) or 'json' (response envelope).",
)


def render_or_json(
    envelope: ResponseEnvelope,
    output: OutputFormat,
    render_fn: Callable[[ResponseEnvelope], None] | None = None,
) -> None:
    """Render a response envelope as a table or JSON.

    Args:
        envelope: The response envelope to render.
        output: The output format.
        render_fn: Function to render the envelope as a human-readable table.
            If None, falls back to printing the JSON.
    """
    if output == OutputFormat.JSON:
        typer.echo(envelope.to_json())
    elif render_fn is not None:
        render_fn(envelope)
    else:
        typer.echo(envelope.to_json())

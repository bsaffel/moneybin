# src/moneybin/cli/output.py
"""Shared output-format options for read-only CLI commands.

`-o/--output` and `-q/--quiet` are required on every read-only command per
`.claude/rules/cli.md`. Importing the shared options keeps the surface
consistent and avoids 6-line copy-paste at every call site.

Usage::

    from moneybin.cli.output import OutputFormat, output_option, quiet_option

    @app.command("summary")
    def summary_cmd(
        output: OutputFormat = output_option,
        quiet: bool = quiet_option,
    ) -> None:
        ...
        if output == OutputFormat.JSON:
            ...
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from enum import StrEnum

import typer

from moneybin.protocol.envelope import ResponseEnvelope

logger = logging.getLogger(__name__)


class OutputFormat(StrEnum):
    """CLI output format for read-only commands."""

    TEXT = "text"
    JSON = "json"


output_option: OutputFormat = typer.Option(
    OutputFormat.TEXT,
    "-o",
    "--output",
    help="Output format: 'text' (human-readable) or 'json' (machine-readable).",
)


quiet_option: bool = typer.Option(
    False,
    "-q",
    "--quiet",
    help="Suppress informational output (status lines, progress, ✅).",
)


def render_or_json(
    envelope: ResponseEnvelope,
    output: OutputFormat,
    render_fn: Callable[[ResponseEnvelope], None] | None = None,
) -> None:
    """Render a response envelope as text or JSON."""
    if output == OutputFormat.JSON:
        typer.echo(envelope.to_json())
    elif render_fn is not None:
        render_fn(envelope)
    else:
        typer.echo(envelope.to_json())

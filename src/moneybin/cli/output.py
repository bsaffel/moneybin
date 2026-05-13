"""Shared output-format options for read-only CLI commands.

`-o/--output`, `-q/--quiet`, and `--json-fields` are required on every
read-only command per `.claude/rules/cli.md`. Importing the shared options
keeps the surface consistent and avoids copy-paste at every call site.

Usage::

    from moneybin.cli.output import (
        OutputFormat, output_option, quiet_option, json_fields_option,
    )

    @app.command("list")
    def list_cmd(
        output: OutputFormat = output_option,
        quiet: bool = quiet_option,
        json_fields: str | None = json_fields_option,
    ) -> None:
        ...
        render_or_json(envelope, output, json_fields=json_fields)
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Callable
from enum import StrEnum

import typer

from moneybin.errors import UserError
from moneybin.protocol.envelope import ResponseEnvelope, build_error_envelope

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

json_fields_option: str | None = typer.Option(
    None,
    "--json-fields",
    help=(
        "Comma-separated fields to include in JSON output (e.g. id,date,amount). "
        "Only applies with --output json. "
        "Available fields are documented in each command's --help text."
    ),
)


def render_or_json(
    envelope: ResponseEnvelope,
    output: OutputFormat,
    render_fn: Callable[[ResponseEnvelope], None] | None = None,
    json_fields: str | None = None,
) -> None:
    """Render a response envelope as text or JSON.

    When ``json_fields`` is supplied (and non-empty) and ``output`` is JSON,
    only those comma-separated keys are kept in each ``data`` list item.
    An empty string ``""`` is treated the same as ``None`` — no filtering.
    Silently skipped when ``data`` is a dict (write-result shape).
    Leading/trailing whitespace around each field name is stripped; empty
    segments (e.g. from ``"id,,amount"``) are silently ignored.
    """
    if output == OutputFormat.TEXT:
        if render_fn is not None:
            render_fn(envelope)
        return
    if json_fields and isinstance(envelope.data, list):
        fields = {f.strip() for f in json_fields.split(",") if f.strip()}
        filtered = [
            {k: v for k, v in row.items() if k in fields} for row in envelope.data
        ]
        envelope = dataclasses.replace(envelope, data=filtered)
    typer.echo(envelope.to_json())


def emit_json_error(user_error: UserError) -> None:
    """Emit a structured error envelope to stdout for --output json failure paths."""
    typer.echo(build_error_envelope(error=user_error).to_json())

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
from typing import Any

import typer

from moneybin.errors import UserError
from moneybin.privacy.introspection import derive_tier, extract_data_classes
from moneybin.privacy.log import build_tool_call_event, write_privacy_event
from moneybin.privacy.redaction import redact_typed
from moneybin.privacy.taxonomy import Tier
from moneybin.protocol.envelope import ResponseEnvelope, build_error_envelope

logger = logging.getLogger(__name__)


class OutputFormat(StrEnum):
    """CLI output format for read-only commands."""

    TEXT = "text"
    JSON = "json"


def _set_output_flag(value: OutputFormat) -> OutputFormat:
    from moneybin.cli.utils import (
        set_output_flag,  # noqa: PLC0415 — defer to break import cycle
    )

    return set_output_flag(value)


output_option: OutputFormat = typer.Option(
    OutputFormat.TEXT,
    "-o",
    "--output",
    help="Output format: 'text' (human-readable) or 'json' (machine-readable).",
    callback=_set_output_flag,
    is_eager=False,
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
    envelope: ResponseEnvelope[Any],
    output: OutputFormat,
    render_fn: Callable[[ResponseEnvelope[Any]], None] | None = None,
    json_fields: str | None = None,
    cli_actor: str | None = None,
) -> None:
    """Render a response envelope as text or JSON.

    TEXT path: delegates to ``render_fn`` (caller owns text formatting and
    is expected to display only appropriate fields such as last_4). No
    redaction and no privacy.log event.

    JSON path:
    - Applies ``redact_typed`` to mask CRITICAL fields (e.g. ACCOUNT_IDENTIFIER)
      before serialising, mirroring the ``@mcp_tool`` decorator's behaviour.
    - When ``cli_actor`` is provided, writes a ``privacy.log.jsonl`` event with
      ``actor="cli.<cli_actor>"`` and ``action="tool_call"``.
    - ``json_fields`` field-filter (``--fields`` flag) runs post-redaction on
      ``list`` payloads; typed dataclass payloads skip this filter (no-op
      because Phase 5 migrated CLI commands to typed payloads).

    When ``json_fields`` is supplied (and non-empty) and ``output`` is JSON,
    only those comma-separated keys are kept in each ``data`` list item.
    An empty string ``""`` is treated the same as ``None`` — no filtering.
    Silently skipped when ``data`` is not a list.
    Leading/trailing whitespace around each field name is stripped; empty
    segments (e.g. from ``"id,,amount"``) are silently ignored.
    """
    if output == OutputFormat.TEXT:
        if render_fn is not None:
            render_fn(envelope)
        return

    # Capture the payload's declared classes BEFORE the json_fields filter
    # mutates envelope.data into a bare list[dict] — otherwise the privacy
    # log records classes_returned=[] for filtered responses, losing the
    # audit signal.
    original_data_type = (
        type(envelope.data) if envelope.data is not None else type(None)
    )  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]

    # Redact CRITICAL fields before serialising. Skip the walk for non-CRITICAL
    # payloads — the result would be value-identical and the cost is real.
    # Derive from the payload TYPE (same source the MCP decorator uses) rather
    # than envelope.summary.sensitivity, which CLI commands set manually and
    # often understate (e.g. accounts_resolve passes "low" but its payload
    # contains ACCOUNT_IDENTIFIER → tier CRITICAL).
    if (
        envelope.error is None
        and envelope.data is not None  # pyright: ignore[reportUnknownMemberType]
        and _has_critical(original_data_type)  # pyright: ignore[reportUnknownArgumentType]
    ):
        redacted_data = redact_typed(envelope.data, consent=None)  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        envelope = dataclasses.replace(envelope, data=redacted_data)  # pyright: ignore[reportUnknownArgumentType]

    if json_fields and isinstance(envelope.data, list):  # pyright: ignore[reportUnknownMemberType]
        fields = {f.strip() for f in json_fields.split(",") if f.strip()}
        filtered = [
            {k: v for k, v in row.items() if k in fields}  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            for row in envelope.data  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        ]
        envelope = dataclasses.replace(envelope, data=filtered)  # pyright: ignore[reportUnknownArgumentType]

    if cli_actor is not None:
        classes_returned = [
            c.value
            for c in sorted(extract_data_classes(original_data_type))  # pyright: ignore[reportUnknownArgumentType]
        ]
        write_privacy_event(
            build_tool_call_event(
                actor=f"cli.{cli_actor}",
                sensitivity=envelope.summary.sensitivity,
                classes_returned=classes_returned,
                row_count=envelope.summary.returned_count,
            )
        )

    typer.echo(envelope.to_json())


def _has_critical(payload_type: type) -> bool:
    """Return True if ``payload_type`` carries any CRITICAL-tier field.

    Used by the JSON output path to skip ``redact_typed`` for payloads
    that would pass through unchanged (every non-CRITICAL tier is
    pass-through in PR 2). Mirrors the equivalent check in the
    ``@mcp_tool`` decorator's wrapper.
    """
    try:
        return derive_tier(payload_type) == Tier.CRITICAL
    except Exception:  # noqa: BLE001 — bare types (list/dict) raise; treat as non-critical
        return False


def emit_json_error(user_error: UserError) -> None:
    """Emit a structured error envelope to stdout for --output json failure paths."""
    typer.echo(build_error_envelope(error=user_error).to_json())

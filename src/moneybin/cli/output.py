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
        # Always pass cli_actor — it gates the privacy.log audit event
        # (the logging branch is `if cli_actor is not None`). Omitting it
        # silently drops the audit row for this command's JSON output.
        render_or_json(
            envelope, output, json_fields=json_fields, cli_actor="entity_list"
        )
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Any, Literal

import typer

from moneybin.errors import UserError
from moneybin.privacy.introspection import derive_tier, extract_data_classes
from moneybin.privacy.log import build_tool_call_event, write_privacy_event
from moneybin.privacy.redaction import has_active_transform, redact_typed
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)

if TYPE_CHECKING:
    from moneybin.exports.models import ExportReceipt

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


@dataclass(frozen=True, slots=True)
class ExportDestinationOutput:
    """Privacy-classified destination identity safe for CLI JSON output."""

    destination_id: Annotated[str | None, DataClass.RECORD_ID]
    name: Annotated[str, DataClass.USER_NOTE]
    kind: Annotated[Literal["local", "sheets"], DataClass.TXN_TYPE]
    local_path: Annotated[str | None, DataClass.USER_NOTE]


@dataclass(frozen=True, slots=True)
class ExportReceiptOutput:
    """Typed transport projection of a completed export receipt."""

    subject: Annotated[dict[str, object], DataClass.USER_NOTE]
    format: Annotated[Literal["csv", "parquet", "xlsx", "sheets"], DataClass.TXN_TYPE]
    redaction_mode: Annotated[Literal["redacted", "unredacted"], DataClass.TXN_TYPE]
    destination: ExportDestinationOutput
    artifact_path: Annotated[str | None, DataClass.USER_NOTE]
    compressed_artifact_path: Annotated[str | None, DataClass.USER_NOTE]
    sheets_identity: Annotated[str | None, DataClass.RECORD_ID]
    row_counts: Annotated[dict[str, int], DataClass.AGGREGATE]
    output_classes: Annotated[dict[str, dict[str, str]], DataClass.AGGREGATE]
    checksums: Annotated[dict[str, str], DataClass.RECORD_ID]


@dataclass(frozen=True, slots=True)
class ExportDestinationStatusOutput:
    """One saved export destination without a Sheets source identity."""

    destination_id: Annotated[str | None, DataClass.RECORD_ID]
    name: Annotated[str, DataClass.USER_NOTE]
    kind: Annotated[Literal["local", "sheets"], DataClass.TXN_TYPE]
    local_path: Annotated[str | None, DataClass.USER_NOTE]
    ready: Annotated[bool, DataClass.TXN_TYPE]
    write_capable: Annotated[bool, DataClass.TXN_TYPE]
    reasons: Annotated[list[str], DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class ExportDestinationsOutput:
    """Typed wrapper retaining destination privacy metadata in JSON mode."""

    destinations: list[ExportDestinationStatusOutput]


def render_or_json(
    envelope: ResponseEnvelope[Any],
    output: OutputFormat,
    render_fn: Callable[[ResponseEnvelope[Any]], None] | None = None,
    json_fields: str | None = None,
    cli_actor: str | None = None,
    classes_returned: list[str] | None = None,
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

    ``classes_returned`` overrides the audit event's data classes. Provide it
    for dynamic-classification commands (``sql query``) whose classes come from
    SQL lineage rather than the payload type; for typed payloads leave it
    ``None`` and the classes are derived from the payload's annotations.
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

    # Redact fields with an active transform before serialising. Skip the walk
    # for payloads with none — the result would be value-identical and the cost
    # is real. Derive from the payload TYPE (same source the MCP decorator uses)
    # rather than envelope.summary.sensitivity, which CLI commands set manually
    # and often understate (e.g. accounts_resolve passes "low" but its payload
    # contains ACCOUNT_IDENTIFIER → an active transform).
    if (
        envelope.error is None
        and envelope.data is not None  # pyright: ignore[reportUnknownMemberType]
        and _has_active_transform(original_data_type)  # pyright: ignore[reportUnknownArgumentType]
    ):
        redacted_data = redact_typed(envelope.data, consent=None)  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        envelope = dataclasses.replace(envelope, data=redacted_data)  # pyright: ignore[reportUnknownArgumentType]

    # Stamp summary.sensitivity from the derived tier so the emitted envelope's
    # summary matches what was actually returned. Mirrors the MCP decorator's
    # post-call correction. Agents using `summary.sensitivity` to decide trust
    # level would otherwise underestimate the tier whenever a CLI command
    # passes a too-low value to build_envelope().
    derived_sensitivity = derive_log_sensitivity(
        original_data_type,  # pyright: ignore[reportUnknownArgumentType]
        envelope.summary.sensitivity,
    )
    if derived_sensitivity != envelope.summary.sensitivity:
        updated_summary = dataclasses.replace(
            envelope.summary,
            sensitivity=derived_sensitivity,  # pyright: ignore[reportArgumentType]
        )
        envelope = dataclasses.replace(envelope, summary=updated_summary)  # pyright: ignore[reportUnknownArgumentType]

    if json_fields and isinstance(envelope.data, list):  # pyright: ignore[reportUnknownMemberType]
        fields = {f.strip() for f in json_fields.split(",") if f.strip()}
        filtered = [
            {k: v for k, v in row.items() if k in fields}  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            for row in envelope.data  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        ]
        envelope = dataclasses.replace(envelope, data=filtered)  # pyright: ignore[reportUnknownArgumentType]

    if cli_actor is not None:
        # Dynamic-classification commands (sql query) resolve classes via SQL
        # lineage, not the static payload type, and pass them explicitly — a
        # bare list[dict] payload carries no Annotated metadata to derive from.
        event_classes = (
            classes_returned
            if classes_returned is not None
            else [
                c.value
                for c in sorted(extract_data_classes(original_data_type))  # pyright: ignore[reportUnknownArgumentType]
            ]
        )
        # envelope.summary.sensitivity is the derived value (stamped above) for
        # typed payloads, or the command's declared value for bare list/dict
        # payloads — either way it's the authoritative tier for the audit log.
        write_privacy_event(
            build_tool_call_event(
                actor=f"cli.{cli_actor}",
                sensitivity=envelope.summary.sensitivity,
                classes_returned=event_classes,
                row_count=envelope.summary.returned_count,
            )
        )

    typer.echo(envelope.to_json())


def render_export_receipt(
    receipt: ExportReceipt,
    output: OutputFormat,
    *,
    cli_actor: str,
) -> None:
    """Render one export receipt through the standard typed envelope path."""
    payload = ExportReceiptOutput(
        subject=dict(receipt.subject),
        format=receipt.format,
        redaction_mode=receipt.redaction_mode,
        destination=ExportDestinationOutput(
            destination_id=receipt.destination.destination_id,
            name=receipt.destination.name,
            kind=receipt.destination.kind,
            local_path=(
                str(receipt.destination.local_path.resolve())
                if receipt.destination.local_path is not None
                else None
            ),
        ),
        artifact_path=(
            str(receipt.artifact_path.resolve())
            if receipt.artifact_path is not None
            else None
        ),
        compressed_artifact_path=(
            str(receipt.compressed_artifact_path.resolve())
            if receipt.compressed_artifact_path is not None
            else None
        ),
        sheets_identity=receipt.sheets_identity,
        row_counts=dict(receipt.row_counts),
        output_classes={
            table: dict(classes) for table, classes in receipt.output_classes.items()
        },
        checksums=dict(receipt.checksums),
    )

    def _render_text(_: ResponseEnvelope[Any]) -> None:
        if payload.artifact_path is not None:
            typer.echo(f"Exported artifact: {payload.artifact_path}")
            if payload.compressed_artifact_path is not None:
                typer.echo(f"Compressed artifact: {payload.compressed_artifact_path}")
        else:
            typer.echo(
                f"Exported to sheets:{payload.destination.name} "
                f"(identity={payload.sheets_identity})"
            )
        typer.echo("✅ Export complete.")

    render_or_json(
        build_envelope(
            data=payload,
            recovery_actions=list(receipt.recovery_actions),
        ),
        output,
        render_fn=_render_text,
        cli_actor=cli_actor,
    )


def derive_log_sensitivity(payload_type: type, envelope_sensitivity: str) -> str:
    """Return the audit-log sensitivity string derived from ``payload_type``.

    For bare list/dict/None payloads (legacy CLI commands not yet migrated to
    typed payloads), falls back to ``envelope_sensitivity`` — the command's
    own declaration is the only signal we have when the type carries no
    class metadata. ``db_key_show`` passes ``{"key": ...}`` with
    ``sensitivity="high"``; the audit log must preserve that, not flatten
    every dict payload to ``"low"``.
    """
    if payload_type in (list, dict, tuple, set, type(None)):
        return envelope_sensitivity
    return derive_tier(payload_type).name.lower()


def _has_active_transform(payload_type: type) -> bool:
    """Return True if ``payload_type`` carries any field with an active transform.

    Used by the JSON output path to skip ``redact_typed`` for payloads that
    would pass through unchanged. Delegates to the same
    ``has_active_transform`` gate the ``@mcp_tool`` decorator's wrapper uses
    (``decorator.py``), so the CLI and MCP redaction paths stay coherent:
    when PR3 wires HIGH/MEDIUM transforms (hash-placeholder for MERCHANT_NAME,
    date-shifting for TXN_DATE), both paths begin redacting those fields
    together. A ``tier == CRITICAL`` check here would be the "CRITICAL-only
    trap" — it would leave the CLI ``--output json`` path leaking MEDIUM/HIGH
    fields the MCP path masks.

    ``PrivacyContractError`` deliberately propagates: a typed payload
    missing ``Annotated[T, DataClass]`` metadata is a contract bug, not
    a "non-critical" case. The MCP path fails the same way at
    registration time; the CLI has no equivalent gate so this is the
    only place the violation can surface.
    """
    # Bare builtin containers (legacy CLI commands still passing dict/list
    # payloads pre-typed-payload migration) have no field annotations.
    # Short-circuit so we don't conflate "no annotation possible" with
    # "annotation missing on a typed payload".
    if payload_type in (list, dict, tuple, set):
        return False
    return has_active_transform(payload_type)


def emit_json_error(user_error: UserError) -> None:
    """Emit a structured error envelope to stdout for --output json failure paths."""
    typer.echo(build_error_envelope(error=user_error).to_json())

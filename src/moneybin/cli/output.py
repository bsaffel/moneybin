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
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast

import typer

from moneybin.cli.render import render_note
from moneybin.errors import UserError
from moneybin.privacy.classified_envelope import classify
from moneybin.privacy.log import build_tool_call_event, write_privacy_event
from moneybin.privacy.redaction import has_active_transform, redact_typed
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import (
    AUXILIARY_LIST_FIELDS,
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
    serialize_payload,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from moneybin.exports.models import ExportReceipt
    from moneybin.services.currency_service import ResolvedRate

logger = logging.getLogger(__name__)

CLI_MAX_ROWS = 1_000_000
"""Rows a CLI report run may return.

The CLI is an operator/agent surface and a report's own LIMIT params (`top`,
etc.) are what bound a result in practice, so this sits far above anything a
terminal renders. It is a cap all the same: `reports run --help` names it, built
from this constant, and the text renderer says so whenever it bites.

Here rather than in `cli_register`, which is the module that generates report
commands: the help string is built at import time, and `reports run` defers that
import to keep sqlglot off the CLI cold-start path. It had been defined twice
with `networth` reaching across for the copy it liked.
"""

UNKNOWN_CURRENCY = "n/a"
"""Printed in the currency slot when the ledger does not know the currency.

One token everywhere, because the two slots are read together: `reports
networth` uses the currency as a heading (`n/a: 1234.56`) and `accounts
balance` puts it after the amount (`1234.56 n/a`). `?` is cryptic as a
heading and `unknown` reads as though the *amount* were unknown, so each
position had grown its own spelling.
"""


def currency_label(value: object) -> str:
    """Render a currency code, naming the unknown case instead of showing it raw.

    An unguarded `!s` prints a NULL as the literal "None", which an agent
    parsing the text surface can take for a denomination.
    """
    return str(value) if value else UNKNOWN_CURRENCY


def echo_applied_rates(
    applied_rates: Sequence[ResolvedRate], target_currency: str | None
) -> None:
    """State the rates behind a converted figure, on stderr.

    Requirement 10 on the surfaces that cannot read `applied_rates` off an
    envelope. Shared by every terminal path that prints a converted number —
    the registered reports and the investments portfolio total — because two
    renderings of one disclosure drift apart, and the one that drifts is the
    one nobody is looking at.

    Summarized rather than enumerated: a twelve-month rollup in three
    currencies applies thirty-six rates, and thirty-six lines under a table is
    a wall nobody reads. The exact rate prints when there is exactly one — the
    common case, and the only one a single line can state without choosing
    which rate to favour. The full set rides the JSON envelope.

    stderr per `cli.md` "Exit Codes & stderr": this is a diagnostic about the
    answer, not the answer, and redirecting to a file or a downstream parser
    must not append prose to the data stream.
    """
    if not applied_rates:
        return
    if len(applied_rates) == 1:
        rate = applied_rates[0]
        priced_on = (
            f"{rate.rate_date}"
            if rate.rate_date == rate.requested_date
            # A weekend or holiday prices at the previous published day, and
            # Requirement 10 wants that visible rather than smoothed over.
            else f"{rate.rate_date}, for {rate.requested_date}"
        )
        render_note(
            f"💱 Converted from {rate.from_currency} at {rate.rate} "
            f"({priced_on}, {rate.source})"
        )
        return
    sources = sorted({rate.from_currency for rate in applied_rates})
    render_note(
        f"💱 Converted from {', '.join(sources)} using "
        f"{len(applied_rates)} stored rates; run "
        f"'moneybin fx rate <from> {currency_label(target_currency)} <date>' "
        "for one of them, or --output json for all"
    )


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

wide_option: bool = typer.Option(
    False,
    "--wide",
    help="Render every column, not just the report's default set.",
)

display_currency_option: str | None = typer.Option(
    None,
    "--display-currency",
    help=(
        "ISO-4217 code to price every amount in (e.g. EUR). Each row converts at "
        "its own date. Amounts stay in their original currency — and the result "
        "says so — when any row has no rate. 'moneybin refresh' stores rates "
        "into your home currency only, so any other target falls back until its "
        "own rates are stored."
    ),
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
    export_id: Annotated[str, DataClass.RECORD_ID]


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


@dataclass(frozen=True, slots=True)
class ExportDestinationStateOutput:
    """Observed target state after one destination mutation."""

    destination_id: Annotated[str | None, DataClass.RECORD_ID]
    kind: Annotated[Literal["local", "sheets"], DataClass.TXN_TYPE]
    name: Annotated[str, DataClass.USER_NOTE]
    state: Annotated[Literal["present", "absent"], DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class ExportDestinationSetOutput:
    """Audited receipt for one destination target-state assertion."""

    destination: ExportDestinationStateOutput
    operation_id: Annotated[str, DataClass.RECORD_ID]


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
    - ``json_fields`` field-filter (``--json-fields`` flag) runs post-redaction,
      on a bare ``list`` payload or on the single list-valued field of a typed
      payload. See ``_project_fields`` for why exactly one is the condition.

    When ``json_fields`` is supplied (and non-empty) and ``output`` is JSON,
    only those comma-separated keys are kept in each row.
    An empty string ``""`` is treated the same as ``None`` — no filtering.
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
    #
    # Deliberately NOT gated on `envelope.error is None`. That gate was only
    # safe while every error envelope came from build_error_envelope, which
    # forces data=[]. A caller may now attach `error` to a payload-carrying
    # envelope when the payload explains the failure, so skipping on `error`
    # would leave real payload data unwalked. Walking a data=[] error envelope
    # is nearly free. Mirrors the same removal in the @mcp_tool decorator.
    if (
        envelope.data is not None  # pyright: ignore[reportUnknownMemberType]
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

    if json_fields:
        fields = {f.strip() for f in json_fields.split(",") if f.strip()}
        projected = _project_fields(envelope.data, fields)  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        if projected is not None:
            envelope = dataclasses.replace(envelope, data=projected)  # pyright: ignore[reportUnknownArgumentType]

    if cli_actor is not None:
        # Dynamic-classification commands (sql query) resolve classes via SQL
        # lineage, not the static payload type, and pass them explicitly — a
        # bare list[dict] payload carries no Annotated metadata to derive from.
        event_classes = (
            classes_returned
            if classes_returned is not None
            else classify(original_data_type).classes_returned  # pyright: ignore[reportUnknownArgumentType]
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


def _project_rows(rows: list[Any], fields: set[str]) -> list[dict[str, Any]] | None:
    """Keep only ``fields`` on each already-serialized row.

    None when any element is not a keyed row — a list of scalars has no fields
    to name, and dropping or passing through the odd one out would return a
    collection the caller cannot reason about. Empty projects to empty.
    """
    if not all(isinstance(row, dict) for row in rows):
        return None
    return [
        {
            key: value
            for key, value in cast("dict[str, Any]", row).items()
            if key in fields
        }
        for row in rows
    ]


def _project_fields(data: Any, fields: set[str]) -> Any | None:
    """Apply the ``--json-fields`` projection, or return None to leave data alone.

    Two shapes carry rows. A bare ``list`` payload is the rows themselves
    (``sql query``). A typed payload wraps them in a named field beside its
    counts — ``SyncStatusPayload(connections=[...])`` — which is what every
    command migrated off a hand-rolled JSON path now returns.

    Descending into a typed payload is deliberately limited to the case where
    exactly ONE field holds a row collection. With two there is no way to tell
    which the caller meant, and projecting one of them would return a payload
    that is narrowed in a place the caller cannot see; a payload with none has
    nothing to narrow. Both no-op, matching the flag's documented "silently
    ignored where it does not apply" behaviour.

    ``AUXILIARY_LIST_FIELDS`` is excluded from that count for the same reason
    ``_count_primary_lists`` excludes it — a refresh's diagnostic lists describe
    the rows rather than being a second set of them. Sharing the one set is what
    keeps "the payload's collection" from meaning one field to the counter and
    another to the projection: ``GsheetPullPayload`` carries four diagnostics
    beside its ``pulls``, and counting them made the flag a silent no-op there.

    Runs on the *serialized* payload, after ``redact_typed`` has walked the
    original: a projection naming a CRITICAL field must hand back the mask, not
    the value. Returning a plain dict rather than a rebuilt dataclass is what
    makes that safe — a rebuilt payload would have to hold `dict` rows in a
    field typed for row objects.
    """
    if isinstance(data, list):
        return _project_rows(cast("list[Any]", data), fields)
    if data is None or isinstance(data, (str, bytes, int, float, bool)):
        return None
    serialized = serialize_payload(data)
    if not isinstance(serialized, dict):
        return None
    body = cast("dict[str, Any]", serialized)
    list_keys = [
        key
        for key, value in body.items()
        if isinstance(value, list) and key not in AUXILIARY_LIST_FIELDS
    ]
    if len(list_keys) != 1:
        return None
    key = list_keys[0]
    projected = _project_rows(cast("list[Any]", body[key]), fields)
    return None if projected is None else {**body, key: projected}


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
        export_id=receipt.export_id,
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
    return classify(payload_type).sensitivity


def _has_active_transform(payload_type: type) -> bool:
    """Return True if ``payload_type`` carries any field with an active transform.

    Used by the JSON output path to skip ``redact_typed`` for payloads that
    would pass through unchanged. Delegates to the same
    ``has_active_transform`` gate the ``@mcp_tool`` decorator's wrapper uses
    (``decorator.py``) — and the call site pairs it with the same
    data-presence-only check, never an ``envelope.error is None`` clause — so
    the CLI and MCP redaction paths stay coherent:
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

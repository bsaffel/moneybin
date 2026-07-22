"""Thin MCP adapters for export delivery and destination target state."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, Self, cast

from fastmcp import FastMCP
from fastmcp.server.dependencies import get_context
from fastmcp.server.elicitation import AcceptedElicitation
from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from moneybin import error_codes
from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.exports.models import (
    ExportCommand,
    ExportReceipt,
    RedactionMode,
)
from moneybin.exports.service import ExportService
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.elicitation import supports_elicitation
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.repositories.export_destinations_repo import ExportDestinationsRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.services.entity_reference import AmbiguousEntity, MissingEntity

_ACTOR = "mcp"
_SHEETS_TAB_PREFIX = "MoneyBin"
RedactionChoice = Literal["redacted", "unredacted"]


class _StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class BundleExportSubject(_StrictRequest):
    """The closed canonical portability bundle."""

    kind: Literal["bundle"]


class ReportExportSubject(_StrictRequest):
    """One registered report and typed parameter binding."""

    kind: Literal["report"]
    report_id: Annotated[str, Field(min_length=1)]
    parameters: dict[str, JsonValue] = Field(default_factory=dict)


ExportSubjectRequest = Annotated[
    BundleExportSubject | ReportExportSubject,
    Field(discriminator="kind"),
]


class LocalExportDestination(_StrictRequest):
    """One named local delivery target and its local-only options."""

    kind: Literal["local"]
    name: Annotated[str, Field(min_length=1)]
    format: Literal["csv", "parquet", "xlsx"] = "csv"
    compression: Literal["zip"] | None = None

    @model_validator(mode="after")
    def _validate_compression(self) -> Self:
        if self.format == "xlsx" and self.compression is not None:
            raise ValueError("XLSX is already compressed and rejects ZIP compression")
        return self


class SheetsExportDestination(_StrictRequest):
    """One named Sheets delivery target with native format semantics."""

    kind: Literal["sheets"]
    name: Annotated[str, Field(min_length=1)]


ExportDestinationRequest = Annotated[
    LocalExportDestination | SheetsExportDestination,
    Field(discriminator="kind"),
]


def _state_schema(required: str, *forbidden: str) -> dict[str, JsonValue]:
    return {
        "allOf": [
            {
                "if": {
                    "properties": {"state": {"const": "present"}},
                    "required": ["state"],
                },
                "then": {
                    "required": [required],
                    "properties": {required: {"not": {"type": "null"}}},
                },
                "else": {
                    "not": {"anyOf": [{"required": [name]} for name in forbidden]}
                },
            }
        ]
    }


class LocalDestinationTarget(_StrictRequest):
    """Present or absent target state for one named local destination."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        json_schema_extra=_state_schema("local_path", "local_path"),
    )

    kind: Literal["local"]
    state: Literal["present", "absent"]
    name: Annotated[str, Field(min_length=1)]
    local_path: Annotated[str, Field(min_length=1)] | None = None

    @model_validator(mode="after")
    def _validate_state(self) -> Self:
        if self.state == "present" and self.local_path is None:
            raise ValueError("Present local destinations require local_path")
        if self.state == "absent" and "local_path" in self.model_fields_set:
            raise ValueError("Absent local destinations forbid local_path")
        if self.local_path is not None and not Path(self.local_path).is_absolute():
            raise ValueError("Local destination paths must be absolute")
        return self


class SheetsDestinationTarget(_StrictRequest):
    """Present or absent target state for one named Sheets destination."""

    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        json_schema_extra=_state_schema(
            "spreadsheet_id",
            "spreadsheet_id",
            "managed_tab_prefix",
        ),
    )

    kind: Literal["sheets"]
    state: Literal["present", "absent"]
    name: Annotated[str, Field(min_length=1)]
    spreadsheet_id: Annotated[str, Field(min_length=1)] | None = None
    managed_tab_prefix: Annotated[str, Field(min_length=1)] | None = None

    @model_validator(mode="after")
    def _validate_state(self) -> Self:
        if self.state == "present" and self.spreadsheet_id is None:
            raise ValueError("Present Sheets destinations require spreadsheet_id")
        configured = {"spreadsheet_id", "managed_tab_prefix"} & self.model_fields_set
        if self.state == "absent" and configured:
            raise ValueError("Absent Sheets destinations forbid target fields")
        return self


ExportDestinationTarget = Annotated[
    LocalDestinationTarget | SheetsDestinationTarget,
    Field(discriminator="kind"),
]


@dataclass(frozen=True, slots=True)
class ExportDestinationOutput:
    """Privacy-classified destination identity in an export receipt."""

    destination_id: Annotated[str | None, DataClass.RECORD_ID]
    name: Annotated[str, DataClass.USER_NOTE]
    kind: Annotated[Literal["local", "sheets"], DataClass.TXN_TYPE]
    local_path: Annotated[str | None, DataClass.USER_NOTE]


@dataclass(frozen=True, slots=True)
class ExportReceiptOutput:
    """Transport projection of a completed export receipt."""

    subject: Annotated[dict[str, object], DataClass.USER_NOTE]
    format: Annotated[Literal["csv", "parquet", "xlsx", "sheets"], DataClass.TXN_TYPE]
    redaction_mode: Annotated[RedactionChoice, DataClass.TXN_TYPE]
    destination: ExportDestinationOutput
    artifact_path: Annotated[str | None, DataClass.USER_NOTE]
    compressed_artifact_path: Annotated[str | None, DataClass.USER_NOTE]
    sheets_identity: Annotated[str | None, DataClass.RECORD_ID]
    row_counts: Annotated[dict[str, int], DataClass.AGGREGATE]
    output_classes: Annotated[dict[str, dict[str, str]], DataClass.AGGREGATE]
    checksums: Annotated[dict[str, str], DataClass.RECORD_ID]


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


def _redaction_refusal(reason: str) -> UserError:
    return UserError(
        "Choose redacted or unredacted output explicitly before exporting. "
        "Nothing was written.",
        code="redaction_choice_required",
        hint="Retry export_run with redaction_mode='redacted' or 'unredacted'.",
        details={
            "default": "redacted",
            "options": ["redacted", "unredacted"],
            "reason": reason,
        },
    )


async def _select_redaction_mode(explicit: RedactionMode | None) -> RedactionMode:
    """Require one current-run choice, using elicitation only when omitted."""
    if explicit is not None:
        return explicit
    try:
        ctx = get_context()
    except RuntimeError as exc:
        raise _redaction_refusal("no_session") from exc
    if not supports_elicitation(ctx):
        raise _redaction_refusal("client_unsupported")
    result = await ctx.elicit(
        "Choose the redaction policy for this export run.",
        response_type=["redacted", "unredacted"],
        response_title="Export redaction mode",
        response_description=(
            "Choose redacted (the safe default) or explicitly choose unredacted."
        ),
    )
    if not isinstance(result, AcceptedElicitation):
        raise _redaction_refusal("declined")
    if result.data not in {"redacted", "unredacted"}:
        raise _redaction_refusal("invalid_response")
    return cast(RedactionMode, result.data)


def _receipt_output(receipt: ExportReceipt) -> ExportReceiptOutput:
    return ExportReceiptOutput(
        subject=dict(receipt.subject),
        format=receipt.format,
        redaction_mode=receipt.redaction_mode,
        destination=ExportDestinationOutput(
            destination_id=receipt.destination.destination_id,
            name=receipt.destination.name,
            kind=receipt.destination.kind,
            local_path=(
                str(receipt.destination.local_path)
                if receipt.destination.local_path is not None
                else None
            ),
        ),
        artifact_path=(
            str(receipt.artifact_path) if receipt.artifact_path is not None else None
        ),
        compressed_artifact_path=(
            str(receipt.compressed_artifact_path)
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


def _run_export(
    subject: BundleExportSubject | ReportExportSubject,
    destination_request: LocalExportDestination | SheetsExportDestination,
    redaction_mode: RedactionMode,
) -> ExportReceipt:
    reference = f"{destination_request.kind}:{destination_request.name}"
    format_ = (
        destination_request.format
        if isinstance(destination_request, LocalExportDestination)
        else "sheets"
    )
    compress_zip = (
        destination_request.compression == "zip"
        if isinstance(destination_request, LocalExportDestination)
        else False
    )
    try:
        return ExportService.run(
            ExportCommand(
                subject_kind=subject.kind,
                report_id=(
                    subject.report_id
                    if isinstance(subject, ReportExportSubject)
                    else None
                ),
                report_parameters=(
                    subject.parameters
                    if isinstance(subject, ReportExportSubject)
                    else {}
                ),
                destination_reference=reference,
                format=format_,
                redaction_mode=redaction_mode,
                compress_zip=compress_zip,
            ),
            actor=_ACTOR,
        )
    except OSError as exc:
        if destination_request.kind != "local":
            raise
        raise UserError(
            "Local export could not be published.",
            code=error_codes.INFRA_IO_ERROR,
        ) from exc


@mcp_tool(
    domain="exports",
    read_only=False,
    idempotent=False,
    open_world=True,
    timeout_seconds=300.0,
)
async def export_run(
    subject: ExportSubjectRequest,
    destination: ExportDestinationRequest,
    redaction_mode: RedactionMode | None = None,
) -> ResponseEnvelope[ExportReceiptOutput]:
    """Publish one bundle or registered-report delivery event."""
    selected_redaction = await _select_redaction_mode(redaction_mode)
    receipt = await asyncio.to_thread(
        _run_export,
        subject,
        destination,
        selected_redaction,
    )
    return build_envelope(
        data=_receipt_output(receipt),
        recovery_actions=list(receipt.recovery_actions),
    )


def _missing_destination() -> UserError:
    return UserError(
        "Export destination not found.",
        code=error_codes.MUTATION_NOT_FOUND,
    )


def _remove_destination(
    repo: ExportDestinationsRepo,
    target: LocalDestinationTarget | SheetsDestinationTarget,
) -> AuditEvent:
    resolved = repo.resolve(target.name)
    if isinstance(resolved, MissingEntity):
        raise _missing_destination()
    if isinstance(resolved, AmbiguousEntity):
        raise UserError(
            "Export destination reference is ambiguous.",
            code=error_codes.MUTATION_AMBIGUOUS,
            details={"candidate_ids": list(resolved.candidate_ids)},
        )
    if resolved.kind != target.kind:
        raise UserError(
            f"Export destination is configured as {resolved.kind}, not {target.kind}.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    event = repo.remove(target.name, actor=_ACTOR)
    if event is None:
        raise _missing_destination()
    return event


def _set_destination(
    target: LocalDestinationTarget | SheetsDestinationTarget,
) -> AuditEvent:
    if target.state == "present" and isinstance(target, SheetsDestinationTarget):
        return ExportService.set_sheets_destination(
            name=target.name,
            spreadsheet_id=cast(str, target.spreadsheet_id),
            managed_tab_prefix=target.managed_tab_prefix or _SHEETS_TAB_PREFIX,
            actor=_ACTOR,
        )
    with get_database(read_only=False) as db:
        repo = ExportDestinationsRepo(db)
        if target.state == "absent":
            return _remove_destination(repo, target)
        if isinstance(target, LocalDestinationTarget):
            return repo.set_local(
                name=target.name,
                local_path=Path(cast(str, target.local_path)),
                actor=_ACTOR,
            )
        raise RuntimeError("Unsupported export destination target")


@mcp_tool(
    domain="exports",
    read_only=False,
    destructive=True,
    idempotent=True,
    open_world=True,
    timeout_seconds=300.0,
)
async def exports_set(
    target: ExportDestinationTarget,
) -> ResponseEnvelope[ExportDestinationSetOutput]:
    """Assert one named local or Sheets destination's target state."""
    event = await asyncio.to_thread(_set_destination, target)
    return build_envelope(
        data=ExportDestinationSetOutput(
            destination=ExportDestinationStateOutput(
                destination_id=event.target_id,
                kind=target.kind,
                name=target.name,
                state=target.state,
            ),
            operation_id=event.operation_id,
        ),
        actions=[
            "Inspect destination readiness with system_status(sections=['exports'])."
        ],
    )


def register_export_tools(mcp: FastMCP) -> None:
    """Register the two admitted export write identities.

    ``export_run`` is one discrete delivery event; ``exports_set`` is one
    idempotent destination target-state mutation. Readiness stays on
    ``system_status(sections=['exports'])`` rather than consuming a third slot.
    """
    register(
        mcp,
        export_run,
        "export_run",
        "Publish one canonical bundle or registered report to a named local or "
        "Sheets destination. This is one discrete delivery event, so retries may "
        "create another immutable local artifact. Redaction must be selected for "
        "this run; omission elicits redacted (default) versus unredacted and "
        "refuses when elicitation is unavailable. Returns receipt identity, row "
        "counts, output classes, and checksums. Writes no app.* state; recovery "
        "uses the returned artifact or Sheets receipt.",
        privacy_actor="export_run",
    )
    register(
        mcp,
        exports_set,
        "exports_set",
        "Assert one named export destination's local or Sheets target state. "
        "state='present' upserts the typed target; state='absent' removes only "
        "MoneyBin configuration and never deletes artifacts, workbooks, or tabs. "
        "Writes app.export_destinations through its audited repository; revert "
        "with system_audit_undo(operation_id). Read readiness with "
        "system_status(sections=['exports']).",
        privacy_actor="exports_set",
    )

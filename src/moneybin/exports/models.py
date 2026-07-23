"""Typed service-boundary contracts for export delivery."""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from pydantic import JsonValue

from moneybin import error_codes
from moneybin.errors import UserError

if TYPE_CHECKING:
    from moneybin.errors import RecoveryAction

type ExportSubjectKind = Literal["bundle", "report"]
type ExportFormat = Literal["csv", "parquet", "xlsx", "sheets"]
type DestinationKind = Literal["local", "sheets"]
type RedactionMode = Literal["redacted", "unredacted"]


class InvalidExportDestinationNameError(UserError):
    """Raised when a saved destination cannot be addressed as ``kind:name``."""

    def __init__(self) -> None:
        """Build the stable invalid-input error."""
        super().__init__(
            "Export destination names must be nonblank and cannot contain ':'.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )


class ReservedExportDestinationError(UserError):
    """Raised when a local name would shadow the derived exports target."""

    def __init__(self) -> None:
        """Build the stable reserved-name error."""
        super().__init__(
            "local:exports is a built-in derived destination and cannot be saved.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )


def local_export_publish_error() -> UserError:
    """Return the shared public error for a failed local publication."""
    return UserError(
        "Local export could not be published.",
        code=error_codes.INFRA_IO_ERROR,
    )


def normalize_export_destination_name(name: str) -> str:
    """Normalize destination names with the shared reference-resolution rules."""
    return " ".join(unicodedata.normalize("NFKC", name).casefold().split())


def validate_export_destination_name(
    name: str,
    *,
    kind: DestinationKind,
    allow_builtin_local: bool = False,
) -> str:
    """Return ``name`` when it is addressable and does not shadow a built-in."""
    normalized = normalize_export_destination_name(name)
    if not normalized or ":" in name:
        raise InvalidExportDestinationNameError()
    if kind == "local" and normalized == "exports" and not allow_builtin_local:
        raise ReservedExportDestinationError()
    return name


@dataclass(frozen=True, slots=True)
class ExportDestination:
    """A saved export target, or the derived local exports directory."""

    destination_id: str | None
    name: str
    kind: DestinationKind
    local_path: Path | None
    spreadsheet_id: str | None
    managed_tab_prefix: str | None


@dataclass(frozen=True, slots=True)
class ExportRequest:
    """A validated export operation for the delivery service."""

    subject_kind: ExportSubjectKind
    report_id: str | None
    report_parameters: Mapping[str, JsonValue]
    destination: ExportDestination
    format: ExportFormat
    redaction_mode: RedactionMode
    compress_zip: bool = False


@dataclass(frozen=True, slots=True)
class ExportCommand:
    """Unresolved export operation accepted by the shared orchestration boundary."""

    subject_kind: ExportSubjectKind
    report_id: str | None
    report_parameters: Mapping[str, JsonValue]
    destination_reference: str
    format: ExportFormat
    redaction_mode: RedactionMode
    compress_zip: bool = False

    def resolve(self, destination: ExportDestination) -> ExportRequest:
        """Bind the destination resolved under the snapshot database context."""
        return ExportRequest(
            subject_kind=self.subject_kind,
            report_id=self.report_id,
            report_parameters=self.report_parameters,
            destination=destination,
            format=self.format,
            redaction_mode=self.redaction_mode,
            compress_zip=self.compress_zip,
        )


@dataclass(frozen=True, slots=True)
class ExportReceipt:
    """One adapter-ready receipt for a completed export publication."""

    subject: Mapping[str, object]
    format: ExportFormat
    redaction_mode: RedactionMode
    destination: ExportDestination
    artifact_path: Path | None
    compressed_artifact_path: Path | None
    sheets_identity: str | None
    row_counts: Mapping[str, int]
    output_classes: Mapping[str, Mapping[str, str]]
    checksums: Mapping[str, str]
    recovery_actions: tuple[RecoveryAction, ...]


@dataclass(frozen=True, slots=True)
class ReportExportReceipt:
    """Catalog report metadata retained beside one prepared snapshot."""

    report_id: str
    parameters: Mapping[str, object]
    parameter_classes: Mapping[str, str]
    sql: str | None
    lineage: tuple[str, ...]
    output_classes: Mapping[str, str]
    freshness: Mapping[str, object] | None
    graduation_eligibility: bool | None
    semantics: Mapping[str, object]

    def as_mapping(self) -> dict[str, object]:
        """Return metadata for the snapshot's deep-freezing receipt boundary."""
        return {
            "report_id": self.report_id,
            "parameters": dict(self.parameters),
            "parameter_classes": dict(self.parameter_classes),
            "sql": self.sql,
            "lineage": self.lineage,
            "output_classes": dict(self.output_classes),
            "freshness": self.freshness,
            "graduation_eligibility": self.graduation_eligibility,
            "semantics": dict(self.semantics),
        }

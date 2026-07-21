"""Typed service-boundary contracts for export delivery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import JsonValue

type ExportSubjectKind = Literal["bundle", "report"]
type ExportFormat = Literal["csv", "parquet", "xlsx", "sheets"]
type DestinationKind = Literal["local", "sheets"]
type RedactionMode = Literal["redacted", "unredacted"]


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

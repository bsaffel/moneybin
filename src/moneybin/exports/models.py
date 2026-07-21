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

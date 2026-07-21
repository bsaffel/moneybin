"""Safe staged publication into a configured Google Sheets tab namespace."""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from dataclasses import dataclass
from typing import Protocol

from moneybin.connectors.gsheet.errors import GSheetAPIError, GSheetError
from moneybin.connectors.gsheet.sheets_api import (
    SheetCreate,
    SheetIdentity,
    SheetInfo,
    SheetRename,
    SheetsAPI,
    SheetValueWrite,
)
from moneybin.database import Database
from moneybin.exports.models import ExportDestination, ExportReceipt
from moneybin.exports.renderers import normalize_tabular_cell
from moneybin.exports.snapshot import PreparedExport, PreparedTable
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo

_MAX_SHEET_TITLE_LENGTH = 100
_MAX_MANAGED_PREFIX_LENGTH = 40
_SAFE_PREFIX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _-]*[A-Za-z0-9]$|^[A-Za-z0-9]$")
_INVALID_TITLE_CHARACTERS = re.compile(r"[\[\]:*?/\\]")


class OAuthGrantLike(Protocol):
    """Capability result needed by export-destination setup."""

    @property
    def can_write(self) -> bool:
        """Return whether Sheets writes were granted."""
        ...


class SheetsAuthorization(Protocol):
    """OAuth interaction required to configure a Sheets destination."""

    def authorize(self, *, require_write: bool = False) -> OAuthGrantLike:
        """Establish or upgrade the persisted grant."""
        ...


class SheetsPublishError(GSheetAPIError):
    """A staged publish failed and left exact temporary tab identities."""

    def __init__(self, *, staging_sheet_ids: tuple[int, ...]) -> None:
        """Build a sanitized failure with identifier-only recovery context."""
        super().__init__(
            "Google Sheets publication failed; any prior visible output was preserved."
        )
        identifiers = sorted(staging_sheet_ids)
        self.details = {"staging_sheet_ids": identifiers}


class SheetsExportPublisher:
    """Publish immutable snapshots through a managed, staged tab namespace."""

    def __init__(self, *, db: Database, sheets_client: SheetsAPI) -> None:
        """Bind collision state and the typed Sheets API boundary."""
        self._db = db
        self._sheets = sheets_client

    def publish(
        self, snapshot: PreparedExport, destination: ExportDestination
    ) -> ExportReceipt:
        """Stage, validate, and atomically promote one latest-state snapshot."""
        spreadsheet_id, prefix = _validate_destination(destination)
        self._reject_inbound_collision(spreadsheet_id)
        metadata = self._sheets.get_workbook_metadata(spreadsheet_id)
        run_id = _available_run_id(snapshot, prefix, metadata.sheets)
        planned = _planned_sheets(snapshot, prefix, run_id)
        creates = tuple(
            SheetCreate(
                name=item.staging_name,
                row_count=max(1, len(item.values)),
                col_count=max(1, len(item.values[0]) if item.values else 0),
            )
            for item in planned
        )
        staged = self._sheets.create_sheets(spreadsheet_id, creates)
        if len(staged) != len(planned):
            raise GSheetAPIError("Google Sheets returned incomplete staging identities")

        try:
            writes = tuple(
                SheetValueWrite(sheet=identity, values=item.values)
                for identity, item in zip(staged, planned, strict=True)
            )
            self._sheets.write_sheet_values(spreadsheet_id, writes)
            for identity, item in zip(staged, planned, strict=True):
                actual = self._sheets.read_sheet_values(spreadsheet_id, identity.name)
                _validate_values(actual, item.values)

            deletes = _managed_replacements(
                metadata.sheets,
                prefix=prefix,
                subject_kind=snapshot.subject.kind,
            )
            renames = tuple(
                SheetRename(sheet=identity, new_name=item.visible_name)
                for identity, item in zip(staged, planned, strict=True)
            )
            self._sheets.promote_sheets(
                spreadsheet_id,
                managed_prefix=prefix,
                renames=renames,
                deletes=deletes,
            )
        except GSheetError as exc:
            raise SheetsPublishError(
                staging_sheet_ids=tuple(sheet.gid for sheet in staged),
            ) from exc

        return ExportReceipt(
            subject=snapshot.subject.as_manifest(),
            redaction_mode=snapshot.redaction_mode,
            destination=destination,
            artifact_path=None,
            compressed_artifact_path=None,
            sheets_identity=f"{prefix}:{run_id}",
            row_counts={table.name: len(table.rows) for table in snapshot.tables},
            checksums={table.name: table.checksum_sha256 for table in snapshot.tables},
            recovery_actions=(),
        )

    def _reject_inbound_collision(self, spreadsheet_id: str) -> None:
        """Reject every inbound/output workbook overlap before API access."""
        if any(
            row["spreadsheet_id"] == spreadsheet_id
            for row in GSheetConnectionsRepo(self._db).list_all()
        ):
            raise ValueError(
                "A Google Sheets export destination cannot also be an inbound "
                "spreadsheet."
            )


@dataclass(frozen=True, slots=True)
class _PlannedSheet:
    """Compact immutable staging-to-visible publication record."""

    staging_name: str
    visible_name: str
    values: tuple[tuple[object, ...], ...]


def validate_managed_tab_prefix(prefix: str) -> str:
    """Validate the configured ownership prefix once at destination setup."""
    if (
        not prefix
        or len(prefix) > _MAX_MANAGED_PREFIX_LENGTH
        or prefix != prefix.strip()
        or _SAFE_PREFIX.fullmatch(prefix) is None
    ):
        raise ValueError(
            "managed tab prefix must be 1-40 letters, numbers, spaces, hyphens, "
            "or underscores without leading or trailing whitespace"
        )
    return prefix


def _validate_destination(destination: ExportDestination) -> tuple[str, str]:
    if destination.kind != "sheets":
        raise ValueError("Sheets publisher requires a Sheets destination")
    if destination.spreadsheet_id is None or destination.managed_tab_prefix is None:
        raise ValueError("Sheets destination is missing its workbook or managed prefix")
    return destination.spreadsheet_id, validate_managed_tab_prefix(
        destination.managed_tab_prefix
    )


def _available_run_id(
    snapshot: PreparedExport, prefix: str, sheets: tuple[SheetInfo, ...]
) -> str:
    base = snapshot.created_at.strftime("%Y%m%dT%H%M%SZ")
    candidate = base
    suffix = 2
    names = {sheet.name for sheet in sheets}
    while any(
        name.startswith(f"{prefix} ") and f" {candidate} " in f" {name} "
        for name in names
    ):
        candidate = f"{base}-{suffix}"
        suffix += 1
    return candidate


def _planned_sheets(
    snapshot: PreparedExport, prefix: str, run_id: str
) -> tuple[_PlannedSheet, ...]:
    kind = "Bundle" if snapshot.subject.kind == "bundle" else "Report"
    result: list[_PlannedSheet] = []
    for table in snapshot.tables:
        subject = table.name
        if snapshot.subject.kind == "report" and snapshot.subject.report_id is not None:
            subject = snapshot.subject.report_id
        visible = _managed_title(prefix, kind, run_id, subject)
        staging = _managed_title(prefix, "Staging", run_id, kind, subject)
        result.append(_PlannedSheet(staging, visible, _table_values(table)))

    manifest = deepcopy(snapshot.manifest)
    manifest["format"] = "sheets"
    manifest["destination_kind"] = "sheets"
    manifest_values = (("JSON",), (_json_text(manifest),))
    dictionary_values = (("JSON",), (_json_text(snapshot.data_dictionary),))
    result.append(
        _PlannedSheet(
            _managed_title(prefix, "Staging", run_id, "Manifest"),
            _managed_title(prefix, "Manifest"),
            manifest_values,
        )
    )
    result.append(
        _PlannedSheet(
            _managed_title(prefix, "Staging", run_id, "Dictionary"),
            _managed_title(prefix, "Dictionary"),
            dictionary_values,
        )
    )
    return tuple(result)


def _table_values(table: PreparedTable) -> tuple[tuple[object, ...], ...]:
    header = tuple(column.name for column in table.columns)
    rows = tuple(
        tuple(normalize_tabular_cell(value) for value in row) for row in table.rows
    )
    return (header, *rows)


def _managed_title(prefix: str, *segments: str) -> str:
    cleaned = [
        _INVALID_TITLE_CHARACTERS.sub("_", segment).strip(" '") or "Sheet"
        for segment in segments
    ]
    title = " ".join((prefix, *cleaned))
    if len(title) <= _MAX_SHEET_TITLE_LENGTH:
        return title
    digest = hashlib.sha256(title.encode("utf-8")).hexdigest()[:8]
    return f"{title[: _MAX_SHEET_TITLE_LENGTH - len(digest) - 1]}-{digest}"


def _managed_replacements(
    sheets: tuple[SheetInfo, ...], *, prefix: str, subject_kind: str
) -> tuple[SheetIdentity, ...]:
    subject_namespace = (
        f"{prefix} Bundle " if subject_kind == "bundle" else f"{prefix} Report "
    )
    exact_metadata = {f"{prefix} Manifest", f"{prefix} Dictionary"}
    return tuple(
        SheetIdentity(name=sheet.name, gid=sheet.gid)
        for sheet in sheets
        if sheet.name.startswith(subject_namespace) or sheet.name in exact_metadata
    )


def _validate_values(
    actual: list[list[str]], expected: tuple[tuple[object, ...], ...]
) -> None:
    expected_text = [
        ["" if cell is None else str(cell) for cell in row] for row in expected
    ]
    width = max((len(row) for row in expected_text), default=0)
    normalized_actual = [row + [""] * (width - len(row)) for row in actual]
    while normalized_actual and not any(normalized_actual[-1]):
        normalized_actual.pop()
    normalized_expected = [row + [""] * (width - len(row)) for row in expected_text]
    while normalized_expected and not any(normalized_expected[-1]):
        normalized_expected.pop()
    if normalized_actual != normalized_expected:
        raise GSheetAPIError("Google Sheets staging validation failed")


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

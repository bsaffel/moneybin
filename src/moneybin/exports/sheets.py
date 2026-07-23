"""Safe staged publication into a configured Google Sheets tab namespace."""

from __future__ import annotations

import hashlib
import json
import re
import time
import unicodedata
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import Protocol, TypeVar

from moneybin.connectors.gsheet.errors import (
    GSheetAPIError,
    GSheetError,
    GSheetRateLimitError,
)
from moneybin.connectors.gsheet.sheets_api import (
    SheetCreate,
    SheetIdentity,
    SheetInfo,
    SheetRename,
    SheetsAPI,
    SheetValueWrite,
)
from moneybin.exports.models import ExportDestination, ExportReceipt
from moneybin.exports.renderers import normalize_tabular_cell
from moneybin.exports.snapshot import PreparedExport, PreparedTable
from moneybin.exports.workbook_roles import WorkbookRolePermit
from moneybin.services.request_lifetime import (
    RequestLifetime,
    current_request_lifetime,
    publication_barrier,
)

_MAX_SHEET_TITLE_LENGTH = 100
_MAX_MANAGED_PREFIX_LENGTH = 40
_SAFE_PREFIX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _-]*[A-Za-z0-9]$|^[A-Za-z0-9]$")
_INVALID_TITLE_CHARACTERS = re.compile(r"[\[\]:*?/\\]")
_RETRY_MAX = 3
_RETRY_BACKOFF_BASE_SECONDS = 1.5
_SHEETS_NULL = r"\N"
_SHEETS_ESCAPE = "\\"
SHEETS_ENCODING = {
    "scheme": "moneybin.sheets-cell",
    "version": 1,
    "null": _SHEETS_NULL,
    "escape": _SHEETS_ESCAPE,
}
_T = TypeVar("_T")


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

    def __init__(self, *, sheets_client: SheetsAPI) -> None:
        """Bind the typed Sheets API boundary."""
        self._sheets = sheets_client

    def publish(
        self,
        snapshot: PreparedExport,
        destination: ExportDestination,
        *,
        role_permit: WorkbookRolePermit,
        publication_lifetime: RequestLifetime | None = None,
    ) -> ExportReceipt:
        """Stage, validate, and atomically promote one latest-state snapshot."""
        spreadsheet_id, prefix = _validate_destination(destination)
        role_permit.assert_for(spreadsheet_id)
        lifetime = publication_lifetime or current_request_lifetime()
        if lifetime is not None:
            lifetime.raise_if_cancelled()
        metadata = self._with_rate_limit_retry(
            lambda: self._sheets.get_workbook_metadata(
                spreadsheet_id, require_write=True
            ),
            lifetime=lifetime,
        )
        run_id = _available_run_id(snapshot, prefix, metadata.sheets)
        replacements = _managed_replacements(
            metadata.sheets,
            prefix=prefix,
            subject_kind=snapshot.subject.kind,
        )
        reserved_names = {
            _normalized_title(sheet.name)
            for sheet in metadata.sheets
            if sheet.gid not in {identity.gid for identity in replacements}
        }
        planned = _planned_sheets(snapshot, prefix, run_id, reserved_names)
        used_gids = {sheet.gid for sheet in metadata.sheets}
        creates: list[SheetCreate] = []
        for item in planned:
            gid = _available_sheet_gid(item.staging_name, used_gids)
            used_gids.add(gid)
            creates.append(
                SheetCreate(
                    name=item.staging_name,
                    row_count=max(1, len(item.values)),
                    col_count=max(1, len(item.values[0]) if item.values else 0),
                    gid=gid,
                    managed_prefix=prefix,
                )
            )
        try:
            if lifetime is not None:
                lifetime.raise_if_cancelled()
            staged = self._with_rate_limit_retry(
                lambda: self._sheets.create_sheets(spreadsheet_id, tuple(creates)),
                lifetime=lifetime,
            )
        except GSheetError as exc:
            recovered = self._recover_created_sheets(
                spreadsheet_id,
                prefix=prefix,
                staging_names={item.staging_name for item in planned},
            )
            raise SheetsPublishError(
                staging_sheet_ids=tuple(sheet.gid for sheet in recovered)
            ) from exc
        if len(staged) != len(planned):
            raise SheetsPublishError(
                staging_sheet_ids=tuple(sheet.gid for sheet in staged)
            )

        try:
            if lifetime is not None:
                lifetime.raise_if_cancelled()
            writes = tuple(
                SheetValueWrite(sheet=identity, values=item.values)
                for identity, item in zip(staged, planned, strict=True)
            )
            self._with_rate_limit_retry(
                lambda: self._sheets.write_sheet_values(spreadsheet_id, writes),
                lifetime=lifetime,
            )
            for identity, item in zip(staged, planned, strict=True):
                if lifetime is not None:
                    lifetime.raise_if_cancelled()
                actual = self._with_rate_limit_retry(
                    lambda sheet_name=identity.name: self._sheets.read_sheet_values(
                        spreadsheet_id, sheet_name, require_write=True
                    ),
                    lifetime=lifetime,
                )
                _validate_values(actual, item.values)

            renames = tuple(
                SheetRename(sheet=identity, new_name=item.visible_name)
                for identity, item in zip(staged, planned, strict=True)
            )
            with publication_barrier(lifetime):
                self._with_rate_limit_retry(
                    lambda: self._sheets.promote_sheets(
                        spreadsheet_id,
                        managed_prefix=prefix,
                        renames=renames,
                        deletes=replacements,
                    ),
                    lifetime=lifetime,
                )
        except GSheetError as exc:
            if self._promotion_completed(
                spreadsheet_id,
                staged=staged,
                planned=planned,
                replacements=replacements,
            ):
                return self._receipt(snapshot, destination, prefix, run_id)
            raise SheetsPublishError(
                staging_sheet_ids=tuple(sheet.gid for sheet in staged),
            ) from exc

        return self._receipt(snapshot, destination, prefix, run_id)

    @staticmethod
    def _receipt(
        snapshot: PreparedExport,
        destination: ExportDestination,
        prefix: str,
        run_id: str,
    ) -> ExportReceipt:
        """Build the stable receipt for a completed staged promotion."""
        return ExportReceipt(
            subject=snapshot.subject.as_manifest(),
            format="sheets",
            redaction_mode=snapshot.redaction_mode,
            destination=destination,
            artifact_path=None,
            compressed_artifact_path=None,
            sheets_identity=f"{prefix}:{run_id}",
            row_counts={table.name: len(table.rows) for table in snapshot.tables},
            output_classes={
                table.name: {
                    column.name: column.data_class.value for column in table.columns
                }
                for table in snapshot.tables
            },
            checksums={table.name: table.checksum_sha256 for table in snapshot.tables},
            recovery_actions=(),
            export_id=snapshot.export_id,
        )

    def _promotion_completed(
        self,
        spreadsheet_id: str,
        *,
        staged: tuple[SheetIdentity, ...],
        planned: tuple[_PlannedSheet, ...],
        replacements: tuple[SheetIdentity, ...],
    ) -> bool:
        """Recognize a server-side promotion whose response was lost."""
        try:
            metadata = self._sheets.get_workbook_metadata(
                spreadsheet_id, require_write=True
            )
        except GSheetError:
            return False
        names_by_gid = {sheet.gid: sheet.name for sheet in metadata.sheets}
        return all(
            names_by_gid.get(identity.gid) == item.visible_name
            for identity, item in zip(staged, planned, strict=True)
        ) and all(identity.gid not in names_by_gid for identity in replacements)

    def _with_rate_limit_retry(
        self,
        operation: Callable[[], _T],
        *,
        lifetime: RequestLifetime | None,
    ) -> _T:
        """Retry only transient 429s, respecting request cancellation."""
        for attempt in range(_RETRY_MAX):
            if lifetime is not None:
                lifetime.raise_if_cancelled()
            try:
                return operation()
            except GSheetRateLimitError:
                if attempt + 1 == _RETRY_MAX:
                    raise
                time.sleep(_RETRY_BACKOFF_BASE_SECONDS**attempt)
        raise RuntimeError("Sheets rate-limit retry loop unexpectedly exhausted")

    def _recover_created_sheets(
        self, spreadsheet_id: str, *, prefix: str, staging_names: set[str]
    ) -> tuple[SheetInfo, ...]:
        """Reconcile only exact, ownership-marked tabs after ambiguous create."""
        try:
            metadata = self._sheets.get_workbook_metadata(
                spreadsheet_id, require_write=True
            )
        except GSheetError:
            return ()
        return tuple(
            sheet
            for sheet in metadata.sheets
            if sheet.name in staging_names and sheet.managed_prefix == prefix
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
    names = {_normalized_title(sheet.name) for sheet in sheets}
    while any(
        name.startswith(_normalized_title(f"{prefix} "))
        and f" {_normalized_title(candidate)} " in f" {name} "
        for name in names
    ):
        candidate = f"{base}-{suffix}"
        suffix += 1
    return candidate


def _planned_sheets(
    snapshot: PreparedExport,
    prefix: str,
    run_id: str,
    reserved_names: set[str] | None = None,
) -> tuple[_PlannedSheet, ...]:
    kind = "Bundle" if snapshot.subject.kind == "bundle" else "Report"
    result: list[_PlannedSheet] = []
    used_names = set(reserved_names or ())
    for table in snapshot.tables:
        subject = table.name
        if snapshot.subject.kind == "report" and snapshot.subject.report_id is not None:
            subject = snapshot.subject.report_id
        visible = _unique_managed_title(used_names, prefix, kind, run_id, subject)
        staging = _unique_managed_title(
            used_names, prefix, "Staging", run_id, kind, subject
        )
        result.append(_PlannedSheet(staging, visible, _table_values(table)))

    manifest = deepcopy(snapshot.manifest)
    manifest["format"] = "sheets"
    manifest["destination_kind"] = "sheets"
    manifest["sheets_encoding"] = deepcopy(SHEETS_ENCODING)
    manifest_values = (("JSON",), (_json_text(manifest),))
    dictionary_values = (("JSON",), (_json_text(snapshot.data_dictionary),))
    result.append(
        _PlannedSheet(
            _unique_managed_title(
                used_names, prefix, "Staging", run_id, kind, "Manifest"
            ),
            _unique_managed_title(used_names, prefix, kind, "Manifest"),
            manifest_values,
        )
    )
    result.append(
        _PlannedSheet(
            _unique_managed_title(
                used_names, prefix, "Staging", run_id, kind, "Dictionary"
            ),
            _unique_managed_title(used_names, prefix, kind, "Dictionary"),
            dictionary_values,
        )
    )
    return tuple(result)


def _table_values(table: PreparedTable) -> tuple[tuple[object, ...], ...]:
    header = tuple(_sheets_payload_cell(column.name) for column in table.columns)
    rows = tuple(
        tuple(_sheets_payload_cell(value) for value in row) for row in table.rows
    )
    return (header, *rows)


def _sheets_payload_cell(value: object) -> object:
    """Encode NULL and escaped text distinctly in the Sheets cell surface."""
    if value is None:
        return _SHEETS_NULL
    normalized = normalize_tabular_cell(value)
    if isinstance(normalized, str) and normalized.startswith(_SHEETS_ESCAPE):
        return f"{_SHEETS_ESCAPE}{normalized}"
    return normalized


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


def _unique_managed_title(used: set[str], prefix: str, *segments: str) -> str:
    """Return a valid title unique under Sheets' Unicode/case semantics."""
    base = _managed_title(prefix, *segments)
    title = base
    attempt = 1
    while _normalized_title(title) in used:
        digest = hashlib.sha256(f"{base}\x1f{attempt}".encode()).hexdigest()[:8]
        title = f"{base[: _MAX_SHEET_TITLE_LENGTH - 9]}-{digest}"
        attempt += 1
    used.add(_normalized_title(title))
    return title


def _normalized_title(name: str) -> str:
    return unicodedata.normalize("NFKC", name).casefold()


def _available_sheet_gid(name: str, used: set[int]) -> int:
    candidate = int.from_bytes(hashlib.sha256(name.encode("utf-8")).digest()[:4])
    candidate &= 0x7FFFFFFF
    while candidate in used:
        candidate = (candidate + 1) & 0x7FFFFFFF
    return candidate


def _managed_replacements(
    sheets: tuple[SheetInfo, ...], *, prefix: str, subject_kind: str
) -> tuple[SheetIdentity, ...]:
    subject_namespace = (
        f"{prefix} Bundle " if subject_kind == "bundle" else f"{prefix} Report "
    )
    return tuple(
        SheetIdentity(
            name=sheet.name,
            gid=sheet.gid,
            managed_prefix=sheet.managed_prefix,
        )
        for sheet in sheets
        if sheet.managed_prefix == prefix and sheet.name.startswith(subject_namespace)
    )


def _validate_values(
    actual: list[list[str]], expected: tuple[tuple[object, ...], ...]
) -> None:
    expected_text = [[str(cell) for cell in row] for row in expected]
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

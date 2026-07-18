"""Typed payload dataclasses for the import surface.

Covers: import_files, import_preview, import_status, import_revert,
import_formats, import_inbox_sync, import_inbox_pending,
import_labels_set (curation tool migrated from batch 4 stopgap),
and import_confirm.

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

Tier derivation summary:
  - ``ImportPerFileRow``           → Tier.MEDIUM (error = DESCRIPTION)
  - ``ImportFilesPayload``         → Tier.MEDIUM (transforms_error = DESCRIPTION;
                                     contains ImportPerFileRow list)
  - ``ImportFormatInfoPayload``    → Tier.LOW (file metadata only)
  - ``ImportPreviewPayload``       → Tier.MEDIUM (sample_values = DESCRIPTION —
                                     raw file content may contain PII)
  - ``ImportStatusPayload``        → Tier.LOW (AGGREGATE wrapper over opaque rows)
  - ``ImportRevertPayload``        → Tier.LOW (RECORD_ID + TXN_TYPE)
  - ``ImportFormatRow``            → Tier.LOW (format metadata only)
  - ``ImportFormatsPayload``       → Tier.LOW (list of ImportFormatRow)
  - ``ImportInboxSyncPayload``     → Tier.MEDIUM (transforms_error = DESCRIPTION;
                                     failed list may contain error strings)
  - ``ImportInboxPendingPayload``  → Tier.LOW (filename/account metadata only)
  - ``ImportLabelsSetPayload``     → Tier.MEDIUM (labels = USER_NOTE)
  - ``ImportConfirmPayload``       → Tier.MEDIUM (sample_values = DESCRIPTION —
                                     raw file content may contain PII)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from moneybin.privacy.taxonomy import DataClass

# ---------------------------------------------------------------------------
# import_files — per-file result row
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImportPerFileRow:
    """Per-file outcome inside ImportFilesPayload.files."""

    path: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    source_type: Annotated[str | None, DataClass.TXN_TYPE]
    rows_loaded: Annotated[int | None, DataClass.AGGREGATE]
    import_id: Annotated[str | None, DataClass.RECORD_ID]
    error: Annotated[str | None, DataClass.DESCRIPTION]
    sign_correction_suggested: Annotated[bool, DataClass.TXN_TYPE] = False
    # True when a saved `sign=` override replayed onto this PDF, bypassing the
    # credit-card marker detector for its format.
    sign_override_replayed: Annotated[bool, DataClass.TXN_TYPE] = False
    # Populated only when status == "confirmation_required": detector
    # proposal + samples + flagged + missing_required so the agent can
    # call `import_confirm` per file. Sample values are row-shaped
    # (DESCRIPTION / MEDIUM).
    confirmation_payload: Annotated[dict[str, object] | None, DataClass.DESCRIPTION] = (
        None
    )


# ---------------------------------------------------------------------------
# import_files — top-level payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImportFilesPayload:
    """Payload for ``import_files`` — batch import result."""

    imported_count: Annotated[int, DataClass.AGGREGATE]
    failed_count: Annotated[int, DataClass.AGGREGATE]
    total_count: Annotated[int, DataClass.AGGREGATE]
    transforms_applied: Annotated[bool, DataClass.TXN_TYPE]
    transforms_duration_seconds: Annotated[float | None, DataClass.AGGREGATE]
    transforms_error: Annotated[str | None, DataClass.DESCRIPTION]
    files: list[ImportPerFileRow]


# ---------------------------------------------------------------------------
# import_preview — nested format info + top-level payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImportFormatInfoPayload:
    """File format metadata sub-object inside ImportPreviewPayload."""

    file_type: Annotated[str, DataClass.TXN_TYPE]
    delimiter: Annotated[str | None, DataClass.TXN_TYPE]
    encoding: Annotated[str | None, DataClass.TXN_TYPE]
    file_size_bytes: Annotated[int | None, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class ImportPreviewPayload:
    """Payload for ``import_preview`` — structure and sample of a file.

    ``sample_values`` carries raw file content that may include PII
    (merchant names, description text); annotated as DESCRIPTION (MEDIUM)
    so the middleware applies the appropriate consent gate.
    """

    file: Annotated[str, DataClass.RECORD_ID]
    format: ImportFormatInfoPayload
    # column mapping fields — all metadata / structure, Tier.LOW
    mapping: Annotated[dict[str, Any], DataClass.TXN_TYPE]
    confidence: Annotated[str | None, DataClass.AGGREGATE]
    date_format: Annotated[str | None, DataClass.TXN_TYPE]
    number_format: Annotated[str | None, DataClass.TXN_TYPE]
    sign_convention: Annotated[str | None, DataClass.TXN_TYPE]
    is_multi_account: Annotated[bool | None, DataClass.TXN_TYPE]
    unmapped_columns: Annotated[list[str], DataClass.TXN_TYPE]
    flagged_fields: Annotated[list[str], DataClass.TXN_TYPE]
    # raw file content — may contain PII → DESCRIPTION (MEDIUM)
    sample_values: Annotated[dict[str, Any], DataClass.DESCRIPTION]
    rows_read: Annotated[int, DataClass.AGGREGATE]
    rows_skipped_trailing: Annotated[int, DataClass.AGGREGATE]
    skip_rows: Annotated[int, DataClass.AGGREGATE]
    has_header: Annotated[bool, DataClass.AGGREGATE]
    rows_in_file: Annotated[int, DataClass.AGGREGATE]
    header_row_looks_like_data: Annotated[bool, DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# import_status — top-level payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImportStatusPayload:
    """Payload for ``import_status`` — list of past import log records.

    ``records`` is ``list[dict[str, Any]]`` (opaque DB rows) annotated as
    AGGREGATE so the walker does not recurse into potentially mixed-tier dict
    fields, keeping the payload at Tier.LOW — matching the tool's
    ``sensitivity="low"`` declaration.
    """

    records: Annotated[list[dict[str, Any]], DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# import_revert — top-level payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImportRevertPayload:
    """Payload for ``import_revert`` — revert confirmation."""

    import_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    rows_deleted: Annotated[int | None, DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# import_formats — per-format row + top-level payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImportFormatRow:
    """One format entry in ImportFormatsPayload.formats."""

    name: Annotated[str, DataClass.RECORD_ID]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    file_type: Annotated[str, DataClass.TXN_TYPE]
    sign_convention: Annotated[str | None, DataClass.TXN_TYPE]
    date_format: Annotated[str | None, DataClass.TXN_TYPE]
    number_format: Annotated[str | None, DataClass.TXN_TYPE]
    multi_account: Annotated[bool, DataClass.TXN_TYPE]
    header_signature: Annotated[list[str] | None, DataClass.DESCRIPTION]


@dataclass(frozen=True, slots=True)
class ImportPdfFormatRow:
    """One PDF format entry in ImportFormatsPayload.pdf_formats (Phase 2a)."""

    name: Annotated[str, DataClass.RECORD_ID]
    institution_name: Annotated[str, DataClass.INSTITUTION]
    document_kind: Annotated[str, DataClass.TXN_TYPE]
    routing: Annotated[str, DataClass.TXN_TYPE]
    front_end: Annotated[str, DataClass.TXN_TYPE]
    version: Annotated[int, DataClass.AGGREGATE]
    times_used: Annotated[int, DataClass.AGGREGATE]
    last_used_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]


@dataclass(frozen=True, slots=True)
class ImportFormatsPayload:
    """Payload for ``import_formats`` — list of available tabular + PDF formats."""

    formats: list[ImportFormatRow]
    pdf_formats: list[ImportPdfFormatRow] = field(default_factory=list)


# ---------------------------------------------------------------------------
# import_inbox_sync — top-level payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImportInboxSyncPayload:
    """Payload for ``import_inbox_sync`` — drain result.

    ``processed``, ``skipped``, ``ignored`` are opaque per-file metadata dicts
    with no PII fields; annotated as AGGREGATE (LOW).
    ``failed`` may contain error strings (DESCRIPTION, MEDIUM) — pushed by
    the service layer and user-visible in the tool's action hints.
    ``transforms_error`` is a free-text error string (DESCRIPTION, MEDIUM).
    """

    processed: Annotated[list[dict[str, object]], DataClass.AGGREGATE]
    failed: Annotated[list[dict[str, object]], DataClass.DESCRIPTION]
    pending: Annotated[list[dict[str, object]], DataClass.DESCRIPTION]
    skipped: Annotated[list[dict[str, object]], DataClass.AGGREGATE]
    ignored: Annotated[list[dict[str, object]], DataClass.AGGREGATE]
    transforms_applied: Annotated[bool, DataClass.TXN_TYPE]
    transforms_duration_seconds: Annotated[float | None, DataClass.AGGREGATE]
    transforms_error: Annotated[str | None, DataClass.DESCRIPTION]


# ---------------------------------------------------------------------------
# import_inbox_pending — top-level payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImportInboxPendingPayload:
    """Payload for ``import_inbox_pending`` — preview of pending inbox files.

    Both lists are opaque per-file metadata dicts (filenames, account hints);
    annotated as AGGREGATE (LOW) — no PII fields at this level.
    """

    would_process: Annotated[list[dict[str, object]], DataClass.AGGREGATE]
    ignored: Annotated[list[dict[str, object]], DataClass.AGGREGATE]


# ---------------------------------------------------------------------------
# Dormant consolidated import_status — selected discriminated sections
# ---------------------------------------------------------------------------


class ImportStatusImportsSection(BaseModel):
    """Paginated import-log rows inside the dormant consolidated status read."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["imports"], DataClass.TXN_TYPE] = "imports"
    records: Annotated[list[dict[str, Any]], DataClass.AGGREGATE]


class ImportStatusFormatsSection(BaseModel):
    """Available tabular and PDF formats inside consolidated import status."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["formats"], DataClass.TXN_TYPE] = "formats"
    formats: list[ImportFormatRow]
    pdf_formats: list[ImportPdfFormatRow] = Field(default_factory=list)


class ImportStatusInboxSection(BaseModel):
    """Pending inbox files inside consolidated import status."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["inbox"], DataClass.TXN_TYPE] = "inbox"
    would_process: Annotated[list[dict[str, object]], DataClass.AGGREGATE]
    ignored: Annotated[list[dict[str, object]], DataClass.AGGREGATE]


ImportStatusSection = Annotated[
    ImportStatusImportsSection | ImportStatusFormatsSection | ImportStatusInboxSection,
    Field(discriminator="kind"),
]


class ImportStatusCoarsePayload(BaseModel):
    """Selected import status sections in deterministic request order."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["sections"], DataClass.TXN_TYPE] = "sections"
    sections: list[ImportStatusSection]


# ---------------------------------------------------------------------------
# import_labels_set — top-level payload (migrated from batch 4 stopgap)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImportLabelsSetPayload:
    """Payload for ``import_labels_set`` — label update confirmation.

    ``labels`` is the final set of labels after the diff is applied;
    annotated as USER_NOTE (MEDIUM) since labels are user-authored strings
    that can contain arbitrary text.
    """

    import_id: Annotated[str, DataClass.RECORD_ID]
    labels: Annotated[list[str], DataClass.USER_NOTE]


# ---------------------------------------------------------------------------
# import_confirm — confirmation outcome payload
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ImportConfirmPayload:
    """Payload for ``import_confirm`` — post-confirmation import result.

    ``status`` is always ``"imported"`` here — the discriminant mirrors the
    ``confirmation_required`` envelope's top-level status field, so callers
    can branch on a single discriminant regardless of whether ``import_confirm``
    succeeded or re-surfaced ConfirmationRequired (a re-surface emits a raw
    dict with ``status="confirmation_required"``; see ``import_tools.py``).
    ``merged_mapping`` is the authoritative destination → source column
    mapping the load actually used (threaded from ``ImportResult.field_mapping``
    populated inside ``_import_tabular`` from the ``resolve_or_confirm`` outcome,
    NOT re-derived from a post-hoc detection pass — those can diverge on
    ambiguous headers).
    ``sample_values`` carries raw file content that may include PII
    (merchant names, description text); annotated as DESCRIPTION (MEDIUM).
    Sample values are populated best-effort by re-reading the file post-load
    for display; they're informational, not load-state.
    """

    import_id: Annotated[str | None, DataClass.RECORD_ID]
    rows_loaded: Annotated[int, DataClass.AGGREGATE]
    merged_mapping: Annotated[dict[str, Any], DataClass.TXN_TYPE]
    # raw file content — may contain PII → DESCRIPTION (MEDIUM)
    sample_values: Annotated[dict[str, Any], DataClass.DESCRIPTION]
    sign_correction_suggested: Annotated[bool, DataClass.TXN_TYPE] = False
    status: Annotated[str, DataClass.TXN_TYPE] = "imported"

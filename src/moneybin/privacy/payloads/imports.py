"""Typed payload dataclasses for the import surface.

Covers: import_files, import_preview, import_status, import_revert,
import_formats, import_inbox_sync, import_inbox_pending,
and import_labels_set (curation tool migrated from batch 4 stopgap).

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
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any

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
    confidence: Annotated[str | None, DataClass.TXN_TYPE]
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

    name: Annotated[str, DataClass.TXN_TYPE]
    institution_name: Annotated[str | None, DataClass.INSTITUTION]
    file_type: Annotated[str, DataClass.TXN_TYPE]
    sign_convention: Annotated[str | None, DataClass.TXN_TYPE]
    date_format: Annotated[str | None, DataClass.TXN_TYPE]
    number_format: Annotated[str | None, DataClass.TXN_TYPE]
    multi_account: Annotated[bool, DataClass.TXN_TYPE]
    header_signature: Annotated[list[str] | None, DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class ImportFormatsPayload:
    """Payload for ``import_formats`` — list of available tabular formats."""

    formats: list[ImportFormatRow]


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

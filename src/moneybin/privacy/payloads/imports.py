"""Typed payload dataclasses for the import surface.

Covers: import_files, import_preview, import_status, import_revert,
import_formats, import_inbox_sync, import_inbox_pending,
import_labels_set (curation tool migrated from batch 4 stopgap),
and import_confirm_coarse (registered as ``import_confirm``).

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

Tier derivation summary:
  - ``ImportPerFileRow``           → Tier.CRITICAL (a PDF bridge confirmation can
                                     contain raw statement account identifiers)
  - ``ImportFilesPayload``         → Tier.CRITICAL (contains ImportPerFileRow list)
  - ``ImportFormatInfoPayload``    → Tier.LOW (file metadata only)
  - ``ImportPreviewPayload``       → Tier.MEDIUM (sample_values = DESCRIPTION —
                                     raw file content may contain PII)
  - ``ImportStatusPayload``        → Tier.LOW (AGGREGATE wrapper over opaque rows)
  - ``ImportRevertPayload``        → Tier.LOW (RECORD_ID + TXN_TYPE)
  - ``ImportFormatRow``            → Tier.LOW (format metadata only)
  - ``ImportFormatsPayload``       → Tier.LOW (list of ImportFormatRow)
  - ``ImportInboxSyncPayload``     → Tier.CRITICAL (pending[].account_proposals[]
                                     .source_account_key = ACCOUNT_IDENTIFIER;
                                     transforms_error = DESCRIPTION and the
                                     failed list may contain error strings)
  - ``ImportInboxPendingPayload``  → Tier.LOW (filename/account metadata only)
  - ``ImportLabelsSetPayload``     → Tier.MEDIUM (labels = USER_NOTE)
  - ``ImportConfirmCoarsePayload`` → Tier.MEDIUM (union max; reject_reason on
                                     ImportPdfBridgeInvalidPayload = DESCRIPTION
                                     and accounts_created[].display_name =
                                     USER_NOTE; every other member field is LOW);
                                     confirmation_required re-surfaces build a
                                     raw dict envelope instead (see
                                     import_tools.py)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, Literal, TypedDict

from pydantic import BaseModel, ConfigDict, Field

from moneybin.privacy.taxonomy import DataClass

# ---------------------------------------------------------------------------
# import_files — per-file result row
# ---------------------------------------------------------------------------


class ImportConfirmationBridgeTable(TypedDict, total=False):
    """One raw bridge table inside an import-files confirmation proposal."""

    page: Annotated[int, DataClass.AGGREGATE]
    header: Annotated[list[str], DataClass.TXN_TYPE]
    rows: list[list[Annotated[str, DataClass.DESCRIPTION]]]


class ImportConfirmationBridgePayload(TypedDict, total=False):
    """Typed PDF bridge content shared with the import-preview classification."""

    transparency_notice: Annotated[str, DataClass.DESCRIPTION]
    source_file: Annotated[str, DataClass.RECORD_ID]
    document_text: Annotated[str, DataClass.DESCRIPTION]
    tables_preview: list[ImportConfirmationBridgeTable]
    fingerprint: Annotated[dict[str, Any], DataClass.DESCRIPTION]
    request_kind: Annotated[str, DataClass.TXN_TYPE]
    saved_recipe_for_re_derive: Annotated[
        dict[str, Any] | None,
        DataClass.DESCRIPTION,
    ]


class ImportConfirmationSignSample(TypedDict, total=False):
    """One printed-versus-recorded row inside a sign proposal."""

    description: Annotated[str, DataClass.DESCRIPTION]
    as_printed: Annotated[str, DataClass.TXN_AMOUNT]
    as_recorded: Annotated[str, DataClass.TXN_AMOUNT]


class ImportConfirmationAccountCandidate(TypedDict, total=False):
    """One existing-account candidate inside an import proposal."""

    account_id: Annotated[str, DataClass.RECORD_ID]
    display_name: Annotated[str, DataClass.USER_NOTE]
    confidence: Annotated[float, DataClass.AGGREGATE]
    signal: Annotated[str, DataClass.TXN_TYPE]


class ImportCreatedAccount(TypedDict, total=False):
    """One canonical account an import minted.

    Same two fields, and the same classes, as an existing-account candidate: a
    minted account is the account a candidate would have been, and USER_NOTE is
    what ``core.dim_accounts.display_name`` carries everywhere else.

    There is no ``source_account_key`` field, because that is the file's native
    key (an OFX ``<ACCTID>`` is an account number) and the opaque ``account_id``
    already names the row. That is a statement about the schema, not about the
    values: on the PDF channel ``display_name`` is the document alias, which for
    a statement with no account anchor is also the derived source key. The alias
    is ``slugify(file_path.stem)`` — a path the caller supplied and, in
    ``import_files`` responses, already readable in the same row's ``path``.
    """

    account_id: Annotated[str, DataClass.RECORD_ID]
    display_name: Annotated[str, DataClass.USER_NOTE]


class ImportConfirmationAccountProposal(TypedDict, total=False):
    """One source-account resolution proposal."""

    source_account_key: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    # The one key in this proposal a masking surface leaves readable, and
    # deliberately so: source_account_key is CRITICAL, so an agent reading a
    # gated response has no other way to name which account it is answering.
    # A positional referent ("@0") discloses nothing beyond how many accounts
    # the file the caller already holds contains.
    proposal_ref: Annotated[str, DataClass.RECORD_ID]
    proposed_account_id: Annotated[str | None, DataClass.RECORD_ID]
    is_new: Annotated[bool, DataClass.TXN_TYPE]
    adopted_via: Annotated[str | None, DataClass.TXN_TYPE]
    requires_confirm: Annotated[bool, DataClass.TXN_TYPE]
    candidates: list[ImportConfirmationAccountCandidate]


class ImportConfirmationPayload(TypedDict, total=False):
    """Typed detector proposal carried by an ``import_files`` result row."""

    channel: Annotated[str, DataClass.TXN_TYPE]
    tier: Annotated[str, DataClass.AGGREGATE]
    score: Annotated[float, DataClass.AGGREGATE]
    reason: Annotated[str, DataClass.TXN_TYPE]
    error_message: Annotated[str, DataClass.DESCRIPTION]
    proposed_mapping: Annotated[dict[str, str], DataClass.TXN_TYPE]
    samples: Annotated[dict[str, list[str]], DataClass.DESCRIPTION]
    flagged: Annotated[list[str], DataClass.TXN_TYPE]
    missing_required: Annotated[list[str], DataClass.TXN_TYPE]
    unmapped_columns: Annotated[list[str], DataClass.TXN_TYPE]
    bridge_payload: ImportConfirmationBridgePayload | None
    sign_convention: Annotated[str | None, DataClass.TXN_TYPE]
    sign_prior_convention: Annotated[str | None, DataClass.TXN_TYPE]
    sign_evidence: Annotated[list[str], DataClass.DESCRIPTION]
    sign_sample_rows: list[ImportConfirmationSignSample]
    account_proposals: list[ImportConfirmationAccountProposal]


@dataclass(frozen=True, slots=True)
class CLIConfirmationRequiredPayload:
    """The CLI's ``confirmation_required`` envelope ``data``.

    A dataclass rather than the ``ImportConfirmationPayload`` TypedDict it
    otherwise mirrors, because ``render_or_json`` gates the redaction walk on
    ``type(envelope.data)`` and a TypedDict instance is a bare ``dict`` at
    runtime — it carries no annotation to find, so the walk is skipped and
    ``account_proposals[].source_account_key`` (the institution's own account
    number on the OFX channel) ships in cleartext on the surface built to be
    machine-read.

    Distinct from MCP's ``ImportConfirmRequiredPayload``, which requires a
    ``preview_id`` the CLI never mints — previews are an MCP concept. The nested
    types are shared, so both surfaces mask the same fields by the same
    declarations rather than by two hand-maintained lists.

    Every field is emitted unconditionally, matching the dict this replaced:
    ``confirmation_payload_dict`` returns all keys on every channel, so the
    serialized shape is unchanged.
    """

    status: Annotated[str, DataClass.TXN_TYPE]
    channel: Annotated[str, DataClass.TXN_TYPE]
    tier: Annotated[str, DataClass.AGGREGATE]
    score: Annotated[float, DataClass.AGGREGATE]
    reason: Annotated[str, DataClass.TXN_TYPE]
    error_message: Annotated[str | None, DataClass.DESCRIPTION]
    proposed_mapping: Annotated[dict[str, str], DataClass.TXN_TYPE]
    samples: Annotated[dict[str, list[str]], DataClass.DESCRIPTION]
    flagged: Annotated[list[str], DataClass.TXN_TYPE]
    missing_required: Annotated[list[str], DataClass.TXN_TYPE]
    unmapped_columns: Annotated[list[str], DataClass.TXN_TYPE]
    bridge_payload: ImportConfirmationBridgePayload | None
    sign_convention: Annotated[str | None, DataClass.TXN_TYPE]
    sign_prior_convention: Annotated[str | None, DataClass.TXN_TYPE]
    sign_evidence: Annotated[list[str], DataClass.DESCRIPTION]
    sign_sample_rows: list[ImportConfirmationSignSample]
    account_proposals: list[ImportConfirmationAccountProposal]


@dataclass(frozen=True, slots=True)
class ImportPerFileRow:
    """Per-file outcome inside ImportFilesPayload.files."""

    path: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]
    source_type: Annotated[str | None, DataClass.TXN_TYPE]
    rows_loaded: Annotated[int | None, DataClass.AGGREGATE]
    import_id: Annotated[str | None, DataClass.RECORD_ID]
    error: Annotated[str | None, DataClass.DESCRIPTION]
    # Stable classification of `error` (e.g. infra_permission_denied), or None
    # when the exception was unclassified and `error` is just its class name.
    # A closed vocabulary of codes, never free text → TXN_TYPE (LOW).
    error_code: Annotated[str | None, DataClass.TXN_TYPE] = None
    # Actionable recovery advice paired with `error_code` (e.g. the chmod/chown
    # or macOS Full-Disk-Access instruction). Prose that may name the failing
    # path, exactly like `error` → DESCRIPTION (MEDIUM).
    hint: Annotated[str | None, DataClass.DESCRIPTION] = None
    # Structured facts behind `error_code` — errno, platform, and
    # protected_root on the macOS permission branch — so an agent branches on
    # data instead of matching `hint` prose. DESCRIPTION, not TXN_TYPE:
    # `error_code` is a closed vocabulary, but this is an open-shaped dict
    # mirroring whatever `UserError.details` carries, so it is classified by
    # its worst plausible content rather than today's. It only ever appears
    # alongside `error`, so the row's effective tier is unchanged.
    details: Annotated[dict[str, Any] | None, DataClass.DESCRIPTION] = None
    sign_correction_suggested: Annotated[bool, DataClass.TXN_TYPE] = False
    # True when a saved `sign=` override replayed onto this PDF, bypassing the
    # credit-card marker detector for its format.
    sign_override_replayed: Annotated[bool, DataClass.TXN_TYPE] = False
    # Accounts this file minted. A first-contact mint no longer raises a
    # confirmation, so this row is where an agent learns an account came into
    # existence; empty on every re-import, which is the common case.
    accounts_created: list[ImportCreatedAccount] = field(default_factory=list)
    # Populated only when status == "confirmation_required": typed detector
    # proposal + samples + flagged + missing_required so nested CRITICAL PDF
    # bridge values receive the same redaction as import_preview.
    confirmation_payload: ImportConfirmationPayload | None = None


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


class ImportTabularPreviewCoarsePayload(BaseModel):
    """Persisted tabular preview plus its confirmable trust-state handle."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["tabular"], DataClass.TXN_TYPE] = "tabular"
    preview_id: Annotated[str, DataClass.RECORD_ID]
    expires_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    file: Annotated[str, DataClass.RECORD_ID]
    format: ImportFormatInfoPayload
    mapping: Annotated[dict[str, Any], DataClass.TXN_TYPE]
    confidence: Annotated[str | None, DataClass.AGGREGATE]
    date_format: Annotated[str | None, DataClass.TXN_TYPE]
    number_format: Annotated[str | None, DataClass.TXN_TYPE]
    sign_convention: Annotated[str | None, DataClass.TXN_TYPE]
    is_multi_account: Annotated[bool | None, DataClass.TXN_TYPE]
    unmapped_columns: Annotated[list[str], DataClass.TXN_TYPE]
    flagged_fields: Annotated[list[str], DataClass.TXN_TYPE]
    sample_values: Annotated[dict[str, Any], DataClass.DESCRIPTION]
    rows_read: Annotated[int, DataClass.AGGREGATE]
    rows_skipped_trailing: Annotated[int, DataClass.AGGREGATE]
    skip_rows: Annotated[int, DataClass.AGGREGATE]
    has_header: Annotated[bool, DataClass.AGGREGATE]
    rows_in_file: Annotated[int, DataClass.AGGREGATE]
    header_row_looks_like_data: Annotated[bool, DataClass.AGGREGATE]


class ImportBridgeTablePreview(BaseModel):
    """One bridge table whose cells remain usable for recipe generation."""

    model_config = ConfigDict(frozen=True)

    page: Annotated[int, DataClass.AGGREGATE]
    header: Annotated[list[str], DataClass.TXN_TYPE]
    rows: list[list[Annotated[str, DataClass.DESCRIPTION]]]


class ImportBridgeStatementPayload(BaseModel):
    """Raw PDF bridge request whose statement content must remain usable."""

    model_config = ConfigDict(frozen=True)

    transparency_notice: Annotated[str, DataClass.DESCRIPTION]
    source_file: Annotated[str, DataClass.RECORD_ID]
    document_text: Annotated[str, DataClass.DESCRIPTION]
    tables_preview: list[ImportBridgeTablePreview]
    fingerprint: Annotated[dict[str, Any], DataClass.DESCRIPTION]
    request_kind: Annotated[str, DataClass.TXN_TYPE]
    saved_recipe_for_re_derive: Annotated[
        dict[str, Any] | None,
        DataClass.DESCRIPTION,
    ] = None


class ImportPdfBridgePreviewPayload(BaseModel):
    """Confirmable PDF bridge preview."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["pdf_bridge"], DataClass.TXN_TYPE] = "pdf_bridge"
    preview_id: Annotated[str, DataClass.RECORD_ID]
    expires_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    status: Annotated[str, DataClass.TXN_TYPE]
    channel: Annotated[str, DataClass.TXN_TYPE]
    file: Annotated[str, DataClass.RECORD_ID]
    tier: Annotated[str, DataClass.AGGREGATE]
    score: Annotated[float, DataClass.AGGREGATE]
    reason: Annotated[str, DataClass.TXN_TYPE]
    bridge_payload: ImportBridgeStatementPayload


class ImportPdfDirectPreviewPayload(BaseModel):
    """Deterministic or seed PDF preview routed directly to import_files."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[
        Literal["pdf_deterministic", "pdf_seed"],
        DataClass.TXN_TYPE,
    ]
    preview_id: Annotated[str, DataClass.RECORD_ID]
    expires_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    status: Annotated[str, DataClass.TXN_TYPE]
    file: Annotated[str, DataClass.RECORD_ID]
    channel: Annotated[str, DataClass.TXN_TYPE]
    deterministic: Annotated[bool, DataClass.TXN_TYPE]
    decision_reason: Annotated[str, DataClass.TXN_TYPE]
    confidence: Annotated[float, DataClass.AGGREGATE]
    row_count: Annotated[int, DataClass.AGGREGATE]
    fingerprint: Annotated[dict[str, Any] | None, DataClass.DESCRIPTION]


class ImportPdfSignSample(BaseModel):
    """Printed-versus-recorded sign sample."""

    model_config = ConfigDict(frozen=True)

    description: Annotated[str, DataClass.DESCRIPTION]
    as_printed: Annotated[str, DataClass.TXN_AMOUNT]
    as_recorded: Annotated[str, DataClass.TXN_AMOUNT]


class ImportPdfSignPreviewPayload(BaseModel):
    """Human-confirmable credit-card sign inversion preview."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["pdf_sign"], DataClass.TXN_TYPE] = "pdf_sign"
    preview_id: Annotated[str, DataClass.RECORD_ID]
    expires_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    status: Annotated[str, DataClass.TXN_TYPE]
    channel: Annotated[str, DataClass.TXN_TYPE]
    file: Annotated[str, DataClass.RECORD_ID]
    tier: Annotated[str, DataClass.AGGREGATE]
    score: Annotated[float, DataClass.AGGREGATE]
    reason: Annotated[str, DataClass.TXN_TYPE]
    error_message: Annotated[str, DataClass.DESCRIPTION]
    sign_convention: Annotated[str, DataClass.TXN_TYPE]
    sign_evidence: Annotated[list[str], DataClass.DESCRIPTION]
    sign_sample_rows: list[ImportPdfSignSample]


ImportPreviewCoarsePayload = Annotated[
    ImportTabularPreviewCoarsePayload
    | ImportPdfBridgePreviewPayload
    | ImportPdfDirectPreviewPayload
    | ImportPdfSignPreviewPayload,
    Field(discriminator="kind"),
]


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


@dataclass(frozen=True, slots=True)
class ImportSavedFormatDeletePayload:
    """Payload for audited user-saved format deletion via ``import_revert``."""

    format_name: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[Literal["deleted"], DataClass.TXN_TYPE]
    operation_id: Annotated[str, DataClass.RECORD_ID]


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


class ImportInboxProcessedEntry(TypedDict, total=False):
    """One file the inbox drain imported successfully.

    Declared rather than opaque for the same reason as
    ``ImportInboxPendingEntry``: ``accounts_created[].display_name`` is the
    source's own label for an account it just minted (USER_NOTE → MEDIUM), and
    an opaque ``dict[str, object]`` would have handed the whole row one class.
    Key omission is the contract — ``accounts_created`` is absent when the file
    adopted every account, which is the common re-import case.
    """

    filename: Annotated[str, DataClass.RECORD_ID]
    moved_to: Annotated[str, DataClass.RECORD_ID]
    transactions: Annotated[int, DataClass.AGGREGATE]
    file_type: Annotated[str, DataClass.TXN_TYPE]
    sign_correction_suggested: Annotated[bool, DataClass.TXN_TYPE]
    sign_override_replayed: Annotated[bool, DataClass.TXN_TYPE]
    accounts_created: list[ImportCreatedAccount]


class ImportInboxPendingEntry(TypedDict, total=False):
    """One file the inbox drain routed to ``pending/`` awaiting confirmation.

    A TypedDict rather than a dataclass because key omission is this entry's
    contract: ``moved_to``, ``sidecar``, and ``account_proposals`` are absent
    when the file never moved or the gate carried no account proposal, and a
    dataclass would start emitting them as ``null`` on every row.

    Declared at all so the redaction walk can descend. The field this exists for
    is ``account_proposals[].source_account_key``, which on the OFX channel is
    the ``<ACCTID>`` the institution issued (ACCOUNT_IDENTIFIER → CRITICAL). An
    opaque ``dict[str, object]`` gives the walk one class for the whole entry,
    so the nested key inherited the entry's tier instead of its own.
    """

    filename: Annotated[str, DataClass.RECORD_ID]
    channel: Annotated[str, DataClass.TXN_TYPE]
    tier: Annotated[str, DataClass.AGGREGATE]
    score: Annotated[float, DataClass.AGGREGATE]
    reason: Annotated[str, DataClass.TXN_TYPE]
    moved_to: Annotated[str, DataClass.RECORD_ID]
    sidecar: Annotated[str, DataClass.RECORD_ID]
    account_proposals: list[ImportConfirmationAccountProposal]


@dataclass(frozen=True, slots=True)
class ImportInboxSyncPayload:
    """Payload for ``import_inbox_sync`` — drain result.

    ``skipped`` and ``ignored`` are opaque per-file metadata dicts with no PII
    fields; annotated as AGGREGATE (LOW).
    ``failed`` may contain error strings (DESCRIPTION, MEDIUM) — pushed by
    the service layer and user-visible in the tool's action hints.
    ``processed`` names the accounts a drain minted, so it is typed per-entry
    rather than opaque; see ``ImportInboxProcessedEntry``.
    ``pending`` carries the account-confirmation gate's proposals, so it is
    typed per-entry rather than opaque; see ``ImportInboxPendingEntry``.
    ``transforms_error`` is a free-text error string (DESCRIPTION, MEDIUM).
    """

    processed: list[ImportInboxProcessedEntry]
    failed: Annotated[list[dict[str, object]], DataClass.DESCRIPTION]
    pending: list[ImportInboxPendingEntry]
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


class ImportTabularConfirmCoarsePayload(BaseModel):
    """Successful tabular preview confirmation."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["tabular_complete"], DataClass.TXN_TYPE] = (
        "tabular_complete"
    )
    preview_id: Annotated[str, DataClass.RECORD_ID]
    import_id: Annotated[str, DataClass.RECORD_ID]
    rows_loaded: Annotated[int, DataClass.AGGREGATE]
    merged_mapping: Annotated[dict[str, str], DataClass.TXN_TYPE]
    status: Annotated[Literal["complete"], DataClass.TXN_TYPE] = "complete"
    format_name: Annotated[str | None, DataClass.RECORD_ID] = None
    accounts_created: list[ImportCreatedAccount] = Field(default_factory=list)
    """Accounts this confirmed import minted; empty when it adopted existing ones."""


class ImportPdfBridgeAppliedPayload(BaseModel):
    """Successful PDF bridge confirmation."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["pdf_bridge_applied"], DataClass.TXN_TYPE] = (
        "pdf_bridge_applied"
    )
    preview_id: Annotated[str, DataClass.RECORD_ID]
    import_id: Annotated[str, DataClass.RECORD_ID]
    rows_loaded: Annotated[int, DataClass.AGGREGATE]
    merged_mapping: Annotated[dict[str, str], DataClass.TXN_TYPE]
    status: Annotated[Literal["applied"], DataClass.TXN_TYPE] = "applied"
    format_name: Annotated[str | None, DataClass.RECORD_ID]
    accounts_created: list[ImportCreatedAccount] = Field(default_factory=list)
    """Accounts this confirmed import minted; empty when it adopted existing ones."""


class ImportPdfSignAppliedPayload(BaseModel):
    """Successful human-confirmed PDF sign inversion."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["pdf_sign_applied"], DataClass.TXN_TYPE] = (
        "pdf_sign_applied"
    )
    preview_id: Annotated[str, DataClass.RECORD_ID]
    import_id: Annotated[str, DataClass.RECORD_ID]
    rows_loaded: Annotated[int, DataClass.AGGREGATE]
    status: Annotated[Literal["applied"], DataClass.TXN_TYPE] = "applied"
    format_name: Annotated[str | None, DataClass.RECORD_ID]
    accounts_created: list[ImportCreatedAccount] = Field(default_factory=list)
    """Accounts this confirmed import minted; empty when it adopted existing ones."""


class ImportPdfBridgeInvalidPayload(BaseModel):
    """Retryable invalid PDF bridge response."""

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["pdf_bridge_invalid"], DataClass.TXN_TYPE] = (
        "pdf_bridge_invalid"
    )
    preview_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[Literal["invalid"], DataClass.TXN_TYPE] = "invalid"
    reject_reason: Annotated[str | None, DataClass.DESCRIPTION]
    expected_row_count: Annotated[int, DataClass.AGGREGATE]
    actual_row_count: Annotated[int, DataClass.AGGREGATE]
    rows_diverged: Annotated[bool, DataClass.TXN_TYPE]


class ImportConfirmRequiredPayload(BaseModel):
    """A non-sign confirmation the confirm call could not resolve on its own.

    Typed rather than a raw dict because ``import_confirm`` is
    ``dynamic_classification=True``: the decorator does not redact on the
    tool's behalf, so an untyped envelope would ship
    ``account_proposals[].source_account_key`` (ACCOUNT_IDENTIFIER → CRITICAL)
    unmasked and record no ``classes_returned``.
    """

    model_config = ConfigDict(frozen=True)

    kind: Annotated[Literal["confirmation_required"], DataClass.TXN_TYPE] = (
        "confirmation_required"
    )
    preview_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[Literal["confirmation_required"], DataClass.TXN_TYPE] = (
        "confirmation_required"
    )
    channel: Annotated[str, DataClass.TXN_TYPE]
    tier: Annotated[str, DataClass.AGGREGATE]
    score: Annotated[float, DataClass.AGGREGATE]
    reason: Annotated[str, DataClass.TXN_TYPE]
    error_message: Annotated[str | None, DataClass.DESCRIPTION] = None
    proposed_mapping: Annotated[dict[str, str], DataClass.TXN_TYPE] = Field(
        default_factory=dict
    )
    samples: Annotated[dict[str, list[str]], DataClass.DESCRIPTION] = Field(
        default_factory=dict
    )
    flagged: Annotated[list[str], DataClass.TXN_TYPE] = Field(default_factory=list)
    missing_required: Annotated[list[str], DataClass.TXN_TYPE] = Field(
        default_factory=list
    )
    unmapped_columns: Annotated[list[str], DataClass.TXN_TYPE] = Field(
        default_factory=list
    )
    bridge_payload: ImportConfirmationBridgePayload | None = None
    account_proposals: list[ImportConfirmationAccountProposal] = Field(
        default_factory=list
    )

    # No sign_* fields: this payload is built only where the reason is NOT
    # sign_convention, so the proposal is never a SignConventionProposal and
    # every sign field would be a constant empty. Declaring sign_sample_rows
    # here would also enroll import_confirm in the money surface on TXN_AMOUNT
    # leaves it can never emit.


ImportConfirmCoarsePayload = Annotated[
    ImportTabularConfirmCoarsePayload
    | ImportPdfBridgeAppliedPayload
    | ImportPdfSignAppliedPayload
    | ImportPdfBridgeInvalidPayload
    | ImportConfirmRequiredPayload,
    Field(discriminator="kind"),
]

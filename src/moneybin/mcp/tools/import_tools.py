# src/moneybin/mcp/tools/import_tools.py
"""Import namespace tools — file import, preview, status, revert, format listing.

Tools:
    - import_files — Import files (critical when a PDF bridge proposal is returned)
    - import_preview — Stage a preview, optionally overriding the column mapping
    - import_status — List past import batches (low sensitivity)
    - import_revert — Undo an import batch by import_id (low sensitivity)
    - import_formats — List available tabular import formats (low sensitivity)
    - import_confirm — Ratify one unchanged staged preview and load the file
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import shlex
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast

from fastmcp import FastMCP
from pydantic import Field, JsonValue, TypeAdapter

from moneybin import error_codes

if TYPE_CHECKING:
    from moneybin.services.import_confirmation import ConfirmationRequired
    from moneybin.services.import_service import SavedFormatDeletePlan

from moneybin.config import get_settings
from moneybin.database import get_database
from moneybin.errors import RecoveryAction, UserError
from moneybin.mcp._registration import register
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import Sensitivity, tier_to_sensitivity
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.imports import (
    ImportConfirmationPayload,
    ImportConfirmCoarsePayload,
    ImportConfirmRequiredPayload,
    ImportFilesPayload,
    ImportFormatInfoPayload,
    ImportFormatRow,
    ImportFormatsPayload,
    ImportInboxSyncPayload,
    ImportLabelsSetPayload,
    ImportPdfBridgeAppliedPayload,
    ImportPdfBridgeInvalidPayload,
    ImportPdfBridgePreviewPayload,
    ImportPdfDirectPreviewPayload,
    ImportPdfFormatRow,
    ImportPdfSignAppliedPayload,
    ImportPdfSignPreviewPayload,
    ImportPerFileRow,
    ImportPreviewCoarsePayload,
    ImportPreviewPayload,
    ImportRevertPayload,
    ImportSavedFormatDeletePayload,
    ImportStatusCoarsePayload,
    ImportStatusFormatsSection,
    ImportStatusImportsSection,
    ImportStatusInboxSection,
    ImportStatusPayload,
    ImportTabularConfirmCoarsePayload,
    ImportTabularPreviewCoarsePayload,
)
from moneybin.privacy.redaction import redact_typed
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)
from moneybin.protocol.pagination import (
    KeysetPosition,
    compare_keyset,
    decode_keyset_cursor,
    encode_keyset_cursor,
)
from moneybin.services.import_confirmation import sign_convention_effect
from moneybin.utils.file import file_sha256

logger = logging.getLogger(__name__)

_IMPORT_STATUS_SECTION_ORDER: tuple[Literal["imports", "formats", "inbox"], ...] = (
    "imports",
    "formats",
    "inbox",
)
_IMPORT_REVERT_INPUT_SCHEMA_EXTRA: dict[str, Any] = {
    "allOf": [
        {
            "if": {
                "properties": {"operation": {"const": "delete_saved_format"}},
                "required": ["operation"],
            },
            "then": {
                "properties": {"format_name": {"type": "string", "minLength": 1}},
                "required": ["format_name"],
                "not": {"anyOf": [{"required": ["import_id"]}]},
            },
        },
        {
            "if": {
                "not": {
                    "properties": {
                        "operation": {"const": "delete_saved_format"},
                    },
                    "required": ["operation"],
                }
            },
            "then": {
                "properties": {"import_id": {"type": "string", "minLength": 1}},
                "required": ["import_id"],
                "not": {
                    "anyOf": [
                        {"required": ["format_name"]},
                        {"required": ["confirmation_token"]},
                    ]
                },
            },
        },
    ]
}


def _validate_file_path(file_path: str) -> Path:
    """Validate and resolve a file path, raising UserError if invalid."""
    resolved = Path(file_path).expanduser().resolve()
    if not resolved.is_relative_to(Path.home()):
        raise UserError(
            "file_path must be within the user's home directory. "
            "Path traversal and symlinks that escape the home directory "
            "are not allowed.",
            code=error_codes.IMPORT_INVALID_FILE_PATH,
        )
    return resolved


def _reject_unsupported_pdf_account_signals(
    *,
    account_name: str | None,
    account_metadata: dict[str, dict[str, str]] | None,
) -> None:
    """Refuse account-selection signals no PDF channel can honor.

    Both PDF entry points still take only `account_id` for *naming* an account —
    `_import_pdf` for the deterministic/sign channel and
    `apply_pdf_bridge_response` for the bridge — so `account_name` and
    `account_metadata` remain tabular-only and forwarding them is impossible
    without a service-layer change. Accepting one silently would bind the rows
    to a statement- or filename-derived account while the caller believes they
    chose one — the failure is invisible at the call site and expensive to
    notice later, which is exactly when a loud refusal is worth more than a
    best-effort guess. Shared by both channels so a new signal cannot be
    rejected on one and dropped on the other.

    ``account_bindings`` is deliberately NOT here: PDF now stops before load on
    an unratified account identity, and a binding is the answer. Refusing it
    would leave the caller in a loop the tool that raised the gate cannot exit.
    """
    unsupported = next(
        (
            name
            for name, value in (
                ("account_name", account_name),
                ("account_metadata", account_metadata),
            )
            if value
        ),
        None,
    )
    if unsupported is None:
        return
    raise UserError(
        f"{unsupported} is not supported for a PDF — PDF rows resolve the "
        "account from the statement; pass account_id to pin rows to an existing "
        "account when there is no anchor.",
        code=error_codes.IMPORT_PDF_ACCOUNT_SIGNAL_UNSUPPORTED,
    )


def _bridge_confirm_action(*, payload_ref: str) -> str:
    """The agent-facing hint for a PDF bridge confirmation_required.

    `payload_ref` names where the bridge payload sits in *that* envelope —
    top-level `bridge_payload` for the preview envelope.
    """
    return (
        f"This PDF needs agent extraction. Read {payload_ref} (note its "
        "transparency_notice — proceeding surfaces the document to you), "
        "propose a recipe + rows, then call import_confirm(preview_id=..., "
        "bridge_response={'recipe': ..., 'rows': [...]}) to reconcile and load."
    )


def _sign_confirm_actions(
    file_path: str,
    error_message: str,
    *,
    channel: str,
    proposed_sign: str | None = None,
    prior_sign: str | None = None,
) -> list[str]:
    """The agent-facing hints for a sign-convention confirmation_required.

    Ratifying an inferred convention inverts every amount, and a wrong flip
    corrupts the ledger on this import and on every future replay of the format.
    An agent cannot ratify it — every channel routes the decision to a human, in
    place via elicitation, with the terminal as the fallback.

    ``prior_sign`` switches the framing for the same reason it does in the CLI's
    ``_sign_recovery_commands`` (which this mirrors): a first-contact inference
    always proposes ``negative_is_income``, so "is this a credit card?" is
    accurate, but a self-healed recipe can re-derive to *either* polarity, and
    the card framing would then name the wrong direction and offer no command
    that keeps the convention already in force.
    """
    quoted = shlex.quote(file_path)
    # Only the PDF channel populates sign_sample_rows; the tabular gate has no
    # parsed amounts at gate time and always sends an empty list, so naming it
    # there would send the agent to show the human nothing.
    evidence_pointer = (
        "sign_evidence (the amount header that triggered the inference)"
        if channel == "tabular"
        else "sign_sample_rows (what the statement printed vs what MoneyBin "
        "would record)"
    )
    head = [
        error_message,
        (
            "Stage a preview with import_preview(file_path=...), then "
            "import_confirm(preview_id=...) so MoneyBin can show the human the "
            "sign-inversion approval. Do not answer it yourself — show them "
            f"{evidence_pointer} so THEY decide."
        ),
    ]
    if prior_sign is not None:
        accepted = proposed_sign or "the re-derived convention"
        return [
            *head,
            f"Accept the change — {sign_convention_effect(accepted)}: "
            f"moneybin import files {quoted} --confirm.",
            f"Keep the previous convention — {sign_convention_effect(prior_sign)}: "
            f"moneybin import files {quoted} --sign {prior_sign}.",
        ]
    return [
        *head,
        (
            f"If it IS a credit card: moneybin import confirm {quoted} --accept "
            "--confirm-sign (records charges as expenses)."
            if channel == "tabular"
            else f"To decide in a terminal instead: moneybin import files {quoted} "
            "--confirm (records charges as expenses)."
        ),
        f"If it is NOT a credit card: moneybin import files {quoted} "
        "--sign negative_is_expense (records amounts exactly as printed).",
    ]


def import_files(
    paths: list[str],
    refresh: bool = True,
    force: bool = False,
    account_bindings: dict[str, str] | None = None,
) -> ResponseEnvelope[ImportFilesPayload]:
    """Import one or more financial data files into MoneyBin.

    Supported formats (auto-detected by extension):
      - .ofx / .qfx / .qbo -- OFX/Quicken bank statements
      - .csv / .tsv / .xlsx / .parquet / .feather -- tabular transaction exports

    Per-file failures do not abort the batch. The post-load refresh pipeline
    (matching + SQLMesh apply + categorization) runs once at end of batch by
    default; pass refresh=False to defer.

    Args:
        paths: One or more absolute file paths to import. Each path must
            be within the user's home directory.
        refresh: Run the refresh pipeline once after the batch completes.
            Defaults to True. Pass False to import without refreshing core
            tables; the transforms_pending signal in system_status will
            indicate the pending state, and a later refresh_run or
            refresh call will catch the data up.
        force: If True, re-import files already in the import log.
        account_bindings: Answer to an account_confirmation this tool
            previously returned, mapping each proposed source_account_key to
            an existing account_id, or the literal "new" to mint a fresh
            account. Single-path only. This is how OFX and PDF imports ratify
            an account identity; a tabular file answers through import_confirm
            instead, because its confirmation also stages a column mapping.

    Returns:
        Envelope with data containing imported/failed/total counts,
        transforms state, and a "files" list of per-file results.
        Amounts use accounting convention: negative=expense,
        positive=income; transfers exempt. Display currency is set
        in summary.display_currency.
    """
    from moneybin.protocol.import_envelope import mark_total_failure
    from moneybin.services.import_confirmation import ImportConfirmationRequiredError
    from moneybin.services.import_service import ImportService

    # Validate all paths upfront so a bad path fails before any service call.
    validated = [_validate_file_path(p) for p in paths]

    # A source_account_key is only unambiguous within one file, and the batch
    # path can't route bindings per-file. Refuse rather than drop them: an agent
    # whose answer is silently ignored gets the same account_confirmation back
    # and loops.
    if account_bindings and len(validated) != 1:
        raise UserError(
            "account_bindings answers one file's account confirmation; call "
            "import_files with a single path.",
            code=error_codes.INFRA_INVALID_INPUT,
        )

    # For single-path batches, call import_file (not import_files) so
    # ImportConfirmationRequiredError bubbles up — the batch variant catches
    # all per-file exceptions and loses the proposal payload.
    #
    # Refresh is run separately (not via import_file's built-in refresh) so a
    # refresh failure after a successful raw load preserves the import_id —
    # the agent needs it to call import_revert on the orphaned-from-core data.
    # The multi-file path has the same structure (see ImportService.import_files).
    if len(validated) == 1:
        transforms_error: str | None = None
        transforms_applied = False
        # Track import_id BEFORE refresh so a hard exception from _refresh
        # (anything other than the soft refresh_result.applied=False path)
        # still surfaces the import_id to the failure handler — without
        # this, the agent loses the revert handle for the orphaned raw
        # load. The soft-failure path already preserves it via the else
        # branch's transforms_error wiring.
        loaded_import_id: str | None = None
        try:
            with get_database(read_only=False) as db:
                one = ImportService(db).import_file(
                    validated[0],
                    refresh=False,
                    force=force,
                    actor_kind="agent",
                    account_bindings=account_bindings,
                )
                loaded_import_id = one.import_id
                # Include PDFs only when the deterministic path produced rows —
                # seed-path PDFs write nothing tabular, so refresh would run
                # the full SQLMesh apply for no purpose. Mirrors
                # ImportService.import_file and import_files: gate on
                # transactions_extracted (deterministic path produced rows),
                # not transactions (newly-inserted count). raw inserts use
                # INSERT OR IGNORE on the (transaction_id, account_id,
                # source_file) PK so a re-import after a prior refresh
                # failure reports transactions == 0 even though every row is
                # present — gating on insert count would skip refresh and
                # leave those rows invisible in core/reports.
                if refresh and (
                    one.file_type in ("ofx", "tabular")
                    or (
                        one.file_type == "pdf"
                        and one.details.get("transactions_extracted", 0) > 0
                    )
                ):
                    from moneybin.services.refresh import refresh as _refresh

                    refresh_result = _refresh(db)
                    transforms_applied = refresh_result.applied
                    if not refresh_result.applied:
                        transforms_error = refresh_result.error or (
                            "SQLMesh transforms failed (no error detail)"
                        )
        except ImportConfirmationRequiredError as e:
            # Match the multi-file path's shape: a confirmation-required
            # outcome on a single file lands as one entry in batch.per_file
            # with status="confirmation_required" and the proposal in
            # confirmation_payload. Callers parse data.files[i].status
            # regardless of path count.
            from moneybin.services.import_confirmation import (
                confirmation_payload_dict,
            )
            from moneybin.services.import_service import (
                BatchImportResult,
                PerFileResult,
            )

            file_path = str(validated[0])
            # Same shape as the batch service path — see confirmation_payload_dict.
            confirmation_payload = confirmation_payload_dict(e.outcome)
            batch = BatchImportResult(
                per_file=[
                    PerFileResult(
                        path=file_path,
                        status="confirmation_required",
                        source_type=None,
                        rows_loaded=0,
                        import_id=None,
                        confirmation_payload=confirmation_payload,
                    )
                ],
                transforms_applied=False,
                transforms_duration_seconds=None,
            )
            # Drop through to the shared envelope assembly below — the
            # `actions[]` builder below picks up the per-file
            # confirmation_payload state and surfaces the same
            # `_confirmation_actions` hints the legacy flat envelope had.
        except Exception as e:  # noqa: BLE001 — surface as per-file failure
            # Single-file path bypasses BatchImportResult's per-file catch-
            # all, so non-confirmation exceptions (FileNotFoundError,
            # ValueError, schema mismatches, …) would propagate as a
            # generic MCP error envelope and lose the per-file failure
            # shape callers expect. Synthesize the same batch-style
            # failure record the multi-path branch produces.
            #
            # When the raw load already succeeded but a hard exception
            # propagated from _refresh, loaded_import_id is non-None and
            # the agent can still call import_revert on the orphaned raw
            # rows. status stays "failed" because the user-visible operation
            # didn't complete end-to-end.
            from moneybin.services.import_service import (
                BatchImportResult,
                PerFileResult,
                per_file_failure,
            )

            error_message, error_code, error_hint, error_details = per_file_failure(e)
            if error_code is None and loaded_import_id is not None:
                # The rows landed, so nothing here was a parse failure — the
                # only work after the load is the refresh block above. Left
                # unclassified, `mark_total_failure` falls back to
                # IMPORT_PARSE_ERROR at the envelope level, which tells an
                # agent to fix the file and re-import when the actual
                # recoveries are retry-refresh or `import_revert` on the
                # orphaned load (the `import_id` kept on the row below).
                error_code = error_codes.REFRESH_MODEL_FAILED
            # Log the class name, never the message: a classified message is
            # user-safe but still names the path, and logs stay PII-free.
            logger.warning(f"Import failed for {validated[0]}: {type(e).__name__}")
            batch = BatchImportResult(
                per_file=[
                    PerFileResult(
                        path=str(validated[0]),
                        status="failed",
                        source_type=None,
                        rows_loaded=0,
                        import_id=loaded_import_id,
                        error=error_message,
                        error_code=error_code,
                        hint=error_hint,
                        details=error_details,
                    )
                ],
                transforms_applied=False,
                transforms_duration_seconds=None,
            )
        else:
            # Wrap successful single-file result in BatchImportResult shape
            # so the downstream envelope-builder doesn't branch on path count.
            # transforms_error is populated above when refresh ran but failed;
            # the import_id stays attached so the agent can revert the raw
            # load even though core wasn't refreshed.
            from moneybin.services.import_service import (
                BatchImportResult,
                PerFileResult,
            )

            batch = BatchImportResult(
                per_file=[
                    PerFileResult(
                        path=str(validated[0]),
                        status="imported",
                        source_type=one.file_type,
                        rows_loaded=one.rows_loaded,
                        import_id=one.import_id,
                        sign_correction_suggested=one.sign_correction_suggested,
                        sign_override_replayed=one.sign_override_replayed,
                    )
                ],
                transforms_applied=transforms_applied,
                transforms_duration_seconds=None,
                transforms_error=transforms_error,
            )
    else:
        with get_database(read_only=False) as db:
            batch = ImportService(db).import_files(
                [str(p) for p in validated],
                refresh=refresh,
                force=force,
                actor_kind="agent",
            )

    files = [
        ImportPerFileRow(
            path=r.path,
            status=r.status,
            source_type=r.source_type,
            rows_loaded=r.rows_loaded,
            import_id=r.import_id,
            error=r.error,
            error_code=r.error_code,
            hint=r.hint,
            details=r.details,
            sign_correction_suggested=r.sign_correction_suggested,
            sign_override_replayed=r.sign_override_replayed,
            confirmation_payload=cast(
                ImportConfirmationPayload | None, r.confirmation_payload
            ),
        )
        for r in batch.per_file
    ]

    actions: list[str] = []
    # When a file lands in confirmation_required, surface the per-file
    # accept/override hints so callers see how to re-drive the load even
    # when only one file in the batch needs confirmation. Mirrors the
    # tier-gated logic in _confirmation_actions; inlined here so we don't
    # need to reconstruct a ConfirmationRequired from the payload dict.
    pending_files = [r for r in batch.per_file if r.status == "confirmation_required"]
    for pending in pending_files:
        payload = pending.confirmation_payload or {}
        raw_err = payload.get("error_message")
        err_msg = raw_err if isinstance(raw_err, str) else ""
        # PDF sign-convention channel (credit-card inversion): the agent must NOT
        # ratify it — a wrong flip reverses every amount. MCP can't confirm the
        # inversion in place yet, so _sign_confirm_actions points at the terminal
        # recovery. It owns err_msg as its lead line, so skip the generic
        # "Validation failed" prefix (this is a proposal, not a validation error).
        if payload.get("reason") == "sign_convention":
            channel = payload.get("channel")
            proposed_sign = payload.get("sign_convention")
            prior_sign = payload.get("sign_prior_convention")
            actions.extend(
                _sign_confirm_actions(
                    pending.path,
                    err_msg,
                    channel=channel if isinstance(channel, str) else "tabular",
                    proposed_sign=proposed_sign
                    if isinstance(proposed_sign, str)
                    else None,
                    prior_sign=prior_sign if isinstance(prior_sign, str) else None,
                )
            )
            continue
        # Every remaining channel resolves through the staged-preview workflow:
        # import_preview stages an immutable proposal, import_confirm(preview_id)
        # ratifies it. import_files_coarse appends that hint per pending row, so
        # naming a correction here would only duplicate it.
        if err_msg:
            actions.append(f"Validation failed: {err_msg}")
    if any(r.sign_correction_suggested for r in batch.per_file):
        actions.append(
            "Sign convention may be inverted for one or more imports — "
            "re-run via CLI with `moneybin import files <path> "
            "--sign negative_is_income` (or another SignConventionType "
            "value) to override. MCP import_files does not accept a "
            "sign parameter today."
        )
    if any(r.sign_override_replayed for r in batch.per_file):
        actions.append(
            "One or more statements took their sign convention from a saved "
            "`--sign` override on that statement format — the credit-card "
            "detector was not consulted, so amounts follow the human's earlier "
            "decision. Tell the user; change it by re-running via CLI with "
            "`moneybin import files <path> --sign <SignConventionType>`."
        )
    if not batch.transforms_applied and batch.imported_count > 0:
        actions.append("Run refresh_run when ready to refresh derived tables")
    if batch.transforms_error:
        actions.append("Refresh failed after import — call refresh_run to retry")
    actions.append("Use system_status to confirm refreshed counts")

    envelope = build_envelope(
        data=ImportFilesPayload(
            imported_count=batch.imported_count,
            failed_count=batch.failed_count,
            total_count=batch.total_count,
            transforms_applied=batch.transforms_applied,
            transforms_duration_seconds=batch.transforms_duration_seconds,
            transforms_error=batch.transforms_error,
            files=files,
        ),
        # confirmation_required entries carry sample rows + proposed mapping
        # (DataClass.DESCRIPTION, MEDIUM). Per moneybin-mcp.md the envelope's
        # summary.sensitivity must reflect that — agents read summary.sensitivity
        # to drive consent prompts, not the per-field annotations directly.
        # A failed row's `error`/`hint` are DESCRIPTION-tier for the same reason
        # (`ImportPerFileRow`), so they raise the tier too; deriving from
        # `pending_files` alone under-declared every failed-file batch.
        sensitivity=(
            "medium"
            if pending_files or any(r.error or r.hint for r in files)
            else "low"
        ),
        actions=actions,
    )
    return mark_total_failure(envelope, batch)


def _import_preview_pdf(
    path: Path,
    *,
    source_bytes: bytes | None = None,
) -> ResponseEnvelope[dict[str, Any]]:
    """Preview a PDF via ImportService.pdf_preview (deterministic, bridge, or sign).

    Returns a deterministic preview dict, or a ``confirmation_required`` envelope
    — carrying the bridge payload when the layout is bridge-eligible (and writing
    the Req 14 egress audit row, so a writable DB is required), or the proposed
    sign inversion when the statement names itself a credit card.
    """
    from moneybin.services.import_confirmation import (
        BridgePayload,
        ImportConfirmationRequiredError,
        SignConventionProposal,
        confirmation_payload_dict,
    )
    from moneybin.services.import_service import ImportService

    try:
        with get_database(read_only=False) as db:
            preview = ImportService(db).pdf_preview(path, source_bytes=source_bytes)
    except ImportConfirmationRequiredError as e:
        proposed = e.outcome.proposed
        if isinstance(proposed, SignConventionProposal):
            # A credit-card statement: every amount's sign is about to be
            # inverted. Serialize the proposal (evidence + printed-vs-recorded
            # samples) so the agent can show the user what the flip does before
            # ratifying it.
            payload = confirmation_payload_dict(e.outcome)
            return build_envelope(
                sensitivity="medium",
                data={
                    "status": "confirmation_required",
                    "channel": e.outcome.channel,
                    "file": path.name,
                    "tier": e.outcome.confidence.tier,
                    "score": e.outcome.confidence.score,
                    "reason": e.outcome.reason,
                    "error_message": e.outcome.error_message,
                    "sign_convention": payload["sign_convention"],
                    "sign_prior_convention": payload["sign_prior_convention"],
                    "sign_evidence": payload["sign_evidence"],
                    "sign_sample_rows": payload["sign_sample_rows"],
                },
                actions=_sign_confirm_actions(
                    str(path),
                    e.outcome.error_message,
                    channel=e.outcome.channel,
                    proposed_sign=proposed.sign_convention,
                    prior_sign=proposed.prior_sign_convention,
                ),
            )
        # Otherwise pdf_preview escalated via _raise_pdf_bridge_escalation, which
        # always constructs a BridgePayload — so proposed is never the tabular
        # ProposedMapping here. Fail loudly on a contract break rather than carry
        # a dead `else None` that would emit bridge_payload=null while actions[]
        # still tells the agent to "Read bridge_payload".
        if not isinstance(proposed, BridgePayload):
            raise RuntimeError(
                "pdf_preview escalation must carry a BridgePayload or a "
                f"SignConventionProposal, got {type(proposed).__name__}"
            ) from e
        bridge_payload = proposed.payload
        return build_envelope(
            sensitivity="medium",
            data={
                "status": "confirmation_required",
                "channel": e.outcome.channel,
                "file": path.name,
                "tier": e.outcome.confidence.tier,
                "score": e.outcome.confidence.score,
                "reason": e.outcome.reason,
                "bridge_payload": bridge_payload,
            },
            actions=[_bridge_confirm_action(payload_ref="bridge_payload")],
        )

    return build_envelope(
        sensitivity="medium",
        data={
            "status": "preview",
            "file": preview.file_path,
            "channel": "pdf",
            "deterministic": preview.deterministic,
            "decision_reason": preview.decision_reason,
            "confidence": preview.confidence,
            "row_count": preview.row_count,
            "fingerprint": preview.fingerprint,
        },
        actions=(
            ["Use import_files to import this PDF (loads to transactions)."]
            if preview.deterministic
            else [
                "The deterministic rung could not structure this PDF as "
                "transactions; import_files will store it as a queryable seed "
                "(raw.pdf_<alias>). A scanned/image PDF has no text to extract."
            ]
        ),
    )


def import_preview(file_path: str) -> ResponseEnvelope[ImportPreviewPayload]:
    """Preview a file's structure without importing.

    Tabular files: runs the first 3 stages of the tabular pipeline (detect,
    read, map) and returns format info, column mapping, sample values, and
    confidence. ``has_header``/``skip_rows``/``rows_in_file`` let you
    reconcile row accounting (``rows_in_file == skip_rows + (1 if has_header
    else 0) + rows_read + rows_skipped_trailing``); ``header_row_looks_like_data``
    flags a row consumed as the header that also parses as a transaction — a
    likely misdetection. Any structural red flag forces ``confidence`` to
    ``low`` regardless of column-name/content score, so a suspicious layout
    routes to the confirm gate instead of being self-accepted.

    PDF files: runs the deterministic extraction rung. A clean native-text
    statement returns its routing outcome (row count, confidence,
    fingerprint). A bridge-eligible layout (low confidence, failed
    reconciliation, …) returns ``confirmation_required`` carrying the bridge
    payload — the document text + table preview + a transparency notice (this
    surfaces the document's content to you) — so you can propose a recipe +
    rows and ratify via ``import_confirm(bridge_response=...)``. Because the
    PDF branch can return row-level document content, this tool's sensitivity
    is ``medium`` for PDFs.

    Args:
        file_path: Absolute path to the file to preview.
    """
    validated = _validate_file_path(file_path)

    if validated.suffix.lower() == ".pdf":
        return cast(
            ResponseEnvelope[ImportPreviewPayload],
            _import_preview_pdf(validated),
        )

    return _import_preview_tabular(validated)


def _import_preview_tabular(
    path: Path,
    *,
    source_bytes: bytes | None = None,
    mapping: dict[str, str] | None = None,
) -> ResponseEnvelope[ImportPreviewPayload]:
    """Build a tabular preview from one optional immutable byte object.

    ``mapping`` is an optional partial-merge field->column override, validated
    the same way the confirm-time Override signal used to be for a
    first-encounter file (Req 11 in smart-import-confirmation.md). Staged
    previews are immutable (Req 8), so this is the only place a mapping
    correction can be made — a validated override reports ``confidence:
    "high"``, matching how ``resolve_or_confirm`` treats a ratified Override
    as ``Resolved`` rather than leaving it at whatever tier the raw detector
    scored. A structural red flag still pins the tier to ``low``: the override
    answers a column question, not a "the header row is a transaction" one.
    """
    from moneybin.config import get_settings
    from moneybin.extractors.tabular.column_mapper import collect_samples, map_columns
    from moneybin.extractors.tabular.field_aliases import FIELD_ALIASES
    from moneybin.extractors.tabular.format_detector import detect_format
    from moneybin.extractors.tabular.readers import read_file
    from moneybin.services.import_confirmation import (
        MappingValidationError,
        coerce_confidence_tier,
        coerce_number_format,
        coerce_sign_convention,
        tabular_required_fields,
        validate_partial_mapping,
    )

    bands = get_settings().import_.confidence
    try:
        format_info = detect_format(path, source_bytes=source_bytes)
        read_result = read_file(path, format_info, source_bytes=source_bytes)
        mapping_result = map_columns(
            read_result.df,
            overrides=mapping,
            t_high=bands.t_high,
            t_med=bands.t_med,
            structural_red_flag=read_result.header_row_looks_like_data,
        )
    except ValueError as e:
        raise UserError(str(e), code=error_codes.IMPORT_PREVIEW_ERROR) from e

    field_mapping = mapping_result.field_mapping
    unmapped_columns = mapping_result.unmapped_columns
    confidence = mapping_result.confidence
    sample_values = mapping_result.sample_values
    sign_convention = mapping_result.sign_convention
    number_format = mapping_result.number_format
    if mapping:
        required_fields = tabular_required_fields(
            proposed_keys=set(field_mapping.keys()),
            override_keys=set(mapping.keys()),
        )
        try:
            field_mapping = validate_partial_mapping(
                proposed=field_mapping,
                override=mapping,
                available_columns=tuple(read_result.df.columns),
                required_fields=required_fields,
                valid_destinations=tuple(FIELD_ALIASES.keys()),
            )
        except MappingValidationError as e:
            raise UserError(
                str(e), code=error_codes.IMPORT_PREVIEW_MAPPING_INVALID
            ) from e
        unmapped_columns = [
            c for c in read_result.df.columns if c not in field_mapping.values()
        ]
        # An override can swap the amount shape, which retires the losing
        # destination — keep samples and sign in step with the merged mapping
        # or the plan advertises a column it no longer loads and a split rule
        # against a single amount (which rejects every row).
        sample_values = {
            dest: values
            for dest, values in sample_values.items()
            if dest in field_mapping
        }
        for dest, column in field_mapping.items():
            if dest not in sample_values:
                sample_values[dest] = [
                    value
                    for value in collect_samples(read_result.df, column)
                    if value is not None
                ]
        sign_convention = coerce_sign_convention(
            field_mapping=field_mapping, detected=sign_convention
        )
        number_format = coerce_number_format(
            field_mapping=field_mapping,
            sample_values=sample_values,
            detected=number_format,
        )
        # A caller-validated override resolves the ambiguity the detector
        # couldn't (Req 11), but only for the fields it actually names. Re-score
        # and re-band through the canonical path rather than asserting a tier:
        # a hand-kept list of "facts an override cannot answer" was reported
        # incomplete twice — first missing the structural red flag and the
        # unreadable date, then missing a *second* required field still weakly
        # matched, which promoted an untouched weak match to `high` and made it
        # eligible for agent self-accept. score_mapping and resolve_tier already
        # know all three: a red flag forces low, an unread date scores 0.75, and
        # a surviving flag scores 0.85.
        confidence = coerce_confidence_tier(
            field_mapping=field_mapping,
            detected_flagged=mapping_result.flagged_fields,
            override_keys=set(mapping.keys()),
            date_format=mapping_result.date_format,
            structural_red_flag=read_result.header_row_looks_like_data,
            t_high=bands.t_high,
            t_med=bands.t_med,
        )

    return build_envelope(
        data=ImportPreviewPayload(
            file=path.name,
            format=ImportFormatInfoPayload(
                file_type=format_info.file_type,
                delimiter=format_info.delimiter,
                encoding=format_info.encoding,
                file_size_bytes=format_info.file_size,
            ),
            mapping=field_mapping,
            confidence=confidence,
            date_format=mapping_result.date_format,
            number_format=number_format,
            sign_convention=sign_convention,
            is_multi_account=mapping_result.is_multi_account,
            unmapped_columns=unmapped_columns,
            flagged_fields=mapping_result.flagged_fields,
            sample_values=sample_values,
            rows_read=len(read_result.df),
            rows_skipped_trailing=read_result.rows_skipped_trailing,
            skip_rows=read_result.skip_rows,
            has_header=read_result.has_header,
            rows_in_file=read_result.rows_in_file,
            header_row_looks_like_data=read_result.header_row_looks_like_data,
        ),
        # Consistent with the PDF branches; the @mcp_tool decorator also stamps
        # medium from ImportPreviewPayload (sample_values is row-level content).
        sensitivity="medium",
        actions=[
            "Use import_files to import after reviewing the preview",
            "Use import_status(sections=['formats']) for available named formats",
        ],
    )


def _file_identity(path: Path) -> tuple[str, int]:
    """Return the immutable content identity bound into an import preview."""
    return file_sha256(path), path.stat().st_size


def _bytes_identity(source_bytes: bytes) -> tuple[str, int]:
    """Return the exact identity of an already materialized source object."""
    return hashlib.sha256(source_bytes).hexdigest(), len(source_bytes)


def _read_tabular_preview_bytes(path: Path) -> bytes:
    """Enforce the tabular size limit before materializing one exact snapshot."""
    from moneybin.extractors.tabular.format_detector import detect_format

    try:
        format_info = detect_format(path)
    except ValueError as exc:
        raise UserError(str(exc), code=error_codes.IMPORT_PREVIEW_ERROR) from exc

    with path.open("rb") as handle:
        source_bytes = handle.read(format_info.file_size + 1)
    if len(source_bytes) != format_info.file_size:
        raise UserError(
            "The file changed while MoneyBin was preparing its preview. Retry.",
            code=error_codes.IMPORT_PREVIEW_CHANGED,
        )
    return source_bytes


def _read_pdf_preview_bytes(path: Path) -> bytes:
    """Enforce the PDF snapshot limit before materializing one exact object."""
    from moneybin.config import get_settings

    file_size = path.stat().st_size
    limit_mb = get_settings().import_.pdf_preview_size_limit_mb
    if file_size > limit_mb * 1024 * 1024:
        size_mb = file_size / (1024 * 1024)
        raise UserError(
            f"PDF is {size_mb:.1f} MB, exceeding the configured "
            f"{limit_mb} MB preview limit.",
            code=error_codes.IMPORT_PREVIEW_ERROR,
        )

    with path.open("rb") as handle:
        source_bytes = handle.read(file_size + 1)
    if len(source_bytes) != file_size:
        raise UserError(
            "The file changed while MoneyBin was preparing its preview. Retry.",
            code=error_codes.IMPORT_PREVIEW_CHANGED,
        )
    return source_bytes


def _import_dynamic_envelope[T](
    data: T,
    *,
    actions: list[str],
) -> ResponseEnvelope[T]:
    """Build one typed, redacted, dynamically classified import envelope."""
    classes = extract_data_classes(type(data))
    tier = max(data_class.tier for data_class in classes)
    redacted = cast(T, redact_typed(data, None))
    return build_envelope(
        data=redacted,
        sensitivity=cast(Any, tier_to_sensitivity(tier).value),
        actions=actions,
        classes_returned=sorted(data_class.value for data_class in classes),
    )


@mcp_tool(
    read_only=False,
    idempotent=False,
    dynamic_classification=True,
    maximum_sensitivity=Sensitivity.CRITICAL,
)
def import_preview_coarse(
    file_path: str,
    mapping: dict[str, str] | None = None,
) -> ResponseEnvelope[ImportPreviewCoarsePayload]:
    """Persist a complete staged-import preview for one exact file snapshot.

    ``mapping`` is a partial field->column override for tabular files —
    supply only the destination fields being corrected; the rest fall back to
    the detected proposal. Unrecognized destination fields or absent source
    columns raise. Staged previews are immutable, so a mapping correction is
    made here, at preview time, never at ``import_confirm``.
    """
    from moneybin.config import get_settings
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    path = _validate_file_path(file_path)
    if path.suffix.lower() in {".ofx", ".qfx", ".qbo"}:
        raise UserError(
            "OFX/QFX/QBO files import directly with import_files; their "
            "structured financial format has no column-mapping preview.",
            code=error_codes.IMPORT_PREVIEW_DIRECT_IMPORT_REQUIRED,
        )
    response: ResponseEnvelope[ImportPreviewPayload] | ResponseEnvelope[dict[str, Any]]
    if path.suffix.lower() == ".pdf":
        if mapping is not None:
            raise UserError(
                "mapping is valid only for a tabular preview — a PDF has no "
                "column-mapping proposal to override.",
                code=error_codes.IMPORT_PREVIEW_CHANNEL_CONFLICT,
            )
        source_bytes = _read_pdf_preview_bytes(path)
        response = _import_preview_pdf(path, source_bytes=source_bytes)
    else:
        source_bytes = _read_tabular_preview_bytes(path)
        response = _import_preview_tabular(
            path, source_bytes=source_bytes, mapping=mapping
        )
    reviewed_plan: dict[str, Any] | None = None
    if isinstance(response.data, ImportPreviewPayload):
        from moneybin.services.import_service import ReviewedTabularPlan

        data = response.data
        reviewed_plan = ReviewedTabularPlan(
            file_type=data.format.file_type,
            delimiter=data.format.delimiter,
            encoding=data.format.encoding or "utf-8",
            file_size=data.format.file_size_bytes or len(source_bytes),
            field_mapping=dict(data.mapping),
            # Persist the detector's answer verbatim, including None. Defaulting
            # here would replay a format the detector never read and drop every
            # row at parse time under a "complete" status.
            date_format=data.date_format,
            sign_convention=cast(Any, data.sign_convention or "negative_is_expense"),
            number_format=cast(Any, data.number_format or "us"),
            is_multi_account=bool(data.is_multi_account),
            confidence=data.confidence or "low",
            skip_rows=data.skip_rows,
            has_header=data.has_header,
            rows_in_file=data.rows_in_file,
            rows_skipped_trailing=data.rows_skipped_trailing,
            header_row_looks_like_data=data.header_row_looks_like_data,
            header_signature=sorted({
                *data.mapping.values(),
                *data.unmapped_columns,
            }),
            # Carried so a later refusal re-scores against the same evidence the
            # caller reviewed, instead of a clean mapping it never saw.
            flagged_fields=list(data.flagged_fields),
        ).to_dict()
    sha256, size = _bytes_identity(source_bytes)
    issued_at = datetime.now(UTC)
    expires_at = issued_at + timedelta(
        seconds=get_settings().mcp.confirmation_ttl_seconds
    )
    wire = TypeAdapter(dict[str, Any]).validate_json(response.to_json())
    data = TypeAdapter(dict[str, Any]).validate_python(wire["data"])
    pending_id = "pending"
    expires_wire = expires_at.isoformat()
    # Confirm rejects both of these outright, so the correction hint below has
    # to survive the preview_id rewrite at the end of this function.
    from moneybin.services.import_confirmation import (
        classify_unconfirmable_plan,
        header_row_consumed_recovery_mcp,
        unreadable_date_recovery_mcp,
    )

    plan_is_unconfirmable = reviewed_plan is not None and (
        reviewed_plan["confidence"] == "low" or reviewed_plan["date_format"] is None
    )
    # One classifier, shared with both service branches: which recovery a plan
    # needs was decided three separate ways and corrected one site at a time,
    # and each divergence shipped a hint that could not resolve the refusal it
    # accompanied.
    plan_reason = (
        classify_unconfirmable_plan(
            header_row_looks_like_data=bool(
                reviewed_plan["header_row_looks_like_data"]
            ),
            date_format=reviewed_plan["date_format"],
            field_mapping=dict(reviewed_plan["field_mapping"]),
            flagged_fields=list(reviewed_plan["flagged_fields"]),
        )
        if reviewed_plan is not None
        else None
    )
    if isinstance(response.data, ImportPreviewPayload):
        payload: ImportPreviewCoarsePayload = ImportTabularPreviewCoarsePayload(
            **data,
            preview_id=pending_id,
            expires_at=expires_wire,
        )
        # A low tier (or an unread date column) is rejected at confirm time by
        # design, so pointing there would burn a guaranteed round trip plus a
        # full re-parse. The tier is already known here — name the correction.
        if not plan_is_unconfirmable:
            actions = [
                "Use import_confirm(preview_id=...) before the preview expires.",
            ]
        elif plan_reason == "header_row_consumed":
            actions = [header_row_consumed_recovery_mcp()]
        elif plan_reason == "unreadable_date":
            actions = [unreadable_date_recovery_mcp(file_path)]
        else:
            actions = [
                "This mapping is not confirmable as staged. Use import_preview("
                f"file_path={file_path!r}, mapping={{'<dest_field>': "
                "'<source_column>'}) to correct it; data.sample_values and "
                "data.unmapped_columns name the file's columns.",
            ]
    elif data.get("status") == "confirmation_required" and "bridge_payload" in data:
        payload = ImportPdfBridgePreviewPayload(
            **data,
            preview_id=pending_id,
            expires_at=expires_wire,
        )
        actions = [
            "Use import_confirm(preview_id=..., bridge_response={...}) before "
            "the preview expires.",
        ]
    elif data.get("reason") == "sign_convention":
        payload = ImportPdfSignPreviewPayload(
            **data,
            preview_id=pending_id,
            expires_at=expires_wire,
        )
        actions = [
            "Use import_confirm(preview_id=...) before the preview expires; "
            "MoneyBin will ask the human to approve the sign inversion.",
        ]
    else:
        payload = ImportPdfDirectPreviewPayload(
            **data,
            kind=(
                "pdf_deterministic" if bool(data.get("deterministic")) else "pdf_seed"
            ),
            preview_id=pending_id,
            expires_at=expires_wire,
        )
        actions = list(response.actions)
    redacted_payload = redact_typed(payload, None)
    persisted_data = redacted_payload.model_dump(
        mode="json",
        exclude={"preview_id", "expires_at"},
    )
    snapshot: dict[str, Any] = {
        "data": persisted_data,
        "actions": actions,
        "sensitivity": tier_to_sensitivity(
            max(data_class.tier for data_class in extract_data_classes(type(payload)))
        ).value,
        "plan": reviewed_plan,
    }
    channel: Literal["tabular", "pdf", "ofx"] = (
        "pdf" if path.suffix.lower() == ".pdf" else "tabular"
    )
    with get_database(read_only=False) as db:
        repo = ImportPreviewsRepo(db)
        repo.purge_expired(now=issued_at, actor="system")
        preview_id = repo.issue(
            file_path=str(path),
            file_sha256=sha256,
            file_size_bytes=size,
            channel=channel,
            source_bytes=source_bytes,
            snapshot=snapshot,
            issued_at=issued_at,
            expires_at=expires_at,
            actor="mcp",
        )
    final_payload = redacted_payload.model_copy(
        update={"preview_id": preview_id},
    )
    if isinstance(final_payload, ImportTabularPreviewCoarsePayload):
        # Only a confirmable plan gains anything from naming the real
        # preview_id; overwriting unconditionally discarded the correction hint
        # and sent the agent to a confirm call guaranteed to be refused.
        if not plan_is_unconfirmable:
            actions = [
                f"Use import_confirm(preview_id='{preview_id}') before the preview "
                "expires.",
            ]
    elif isinstance(final_payload, ImportPdfBridgePreviewPayload):
        actions = [
            f"Use import_confirm(preview_id='{preview_id}', "
            "bridge_response={...}) before the preview expires.",
        ]
    elif isinstance(final_payload, ImportPdfSignPreviewPayload):
        actions = [
            f"Use import_confirm(preview_id='{preview_id}') before the preview "
            "expires; MoneyBin will ask the human to approve the sign inversion.",
        ]
    return cast(
        ResponseEnvelope[ImportPreviewCoarsePayload],
        _import_dynamic_envelope(final_payload, actions=actions),
    )


def import_status(
    limit: int = 20, import_id: str | None = None
) -> ResponseEnvelope[ImportStatusPayload]:
    """List past import batches with status and row counts.

    Returns import ID, source file, status, row counts, and detection
    confidence for each completed import batch.

    Args:
        limit: Maximum number of records to return (default 20).
        import_id: Filter to a specific import ID for full details.
    """
    from moneybin.loaders import import_log

    with get_database(read_only=True) as db:
        records = import_log.get_import_history(
            db, limit=min(limit, 200), import_id=import_id
        )
    return build_envelope(
        data=ImportStatusPayload(records=records),
        actions=[
            "Use import_files to import a new file",
        ],
    )


# Why a table and not f"revert_{status}": a code built from a runtime value is
# invisible to TestWireCodes (an f-string is an ast.JoinedStr, not a literal),
# so four undeclared codes reached agents unnoticed. An unknown status falls
# back to a declared code rather than minting one.
_REVERT_FAILURE_CODES = {
    "not_found": error_codes.IMPORT_REVERT_NOT_FOUND,
    "already_reverted": error_codes.IMPORT_REVERT_ALREADY_REVERTED,
    "unsupported": error_codes.IMPORT_REVERT_UNSUPPORTED,
    "superseded": error_codes.IMPORT_REVERT_SUPERSEDED,
}


def import_revert(import_id: str) -> ResponseEnvelope[ImportRevertPayload]:
    """Undo an import batch by deleting all rows it produced.

    Looks up source_type from raw.import_log and deletes rows tagged with
    import_id from the matching raw tables (raw.tabular_* or raw.ofx_*).
    Updates the import_log row's status to 'reverted'.

    Args:
        import_id: UUID of the import batch to revert. Get it from
            import_files's response or from import_status.
    """
    from moneybin.services.import_service import ImportService  # noqa: PLC0415

    with get_database(read_only=False) as db:
        result = ImportService(db).revert(import_id)
    status = result.get("status")

    if status == "reverted":
        return build_envelope(
            data=ImportRevertPayload(
                import_id=import_id,
                status="reverted",
                rows_deleted=int(result["rows_deleted"])
                if "rows_deleted" in result
                else None,
            ),
            actions=[
                "Use import_status to confirm the batch shows status='reverted'",
            ],
        )
    return build_error_envelope(
        error=UserError(
            str(result.get("reason") or f"Cannot revert (status={status})"),
            code=_REVERT_FAILURE_CODES.get(
                str(status), error_codes.IMPORT_REVERT_UNSUPPORTED
            ),
        )
    )


def _saved_format_delete_binding(
    plan: SavedFormatDeletePlan,
) -> ConfirmationBinding:
    """Bind deletion approval to one saved format's exact live row state."""
    return ConfirmationBinding(
        arguments={
            "operation": "delete_saved_format",
            "format_name": plan.format_name,
            "state_sha256": plan.state_sha256,
        },
        resolved_ids=(plan.format_name,),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="saved_format_delete",
        blast_radius=plan.blast_radius,
    )


async def _delete_saved_format(
    format_name: str,
    *,
    confirmation_token: str | None,
) -> ResponseEnvelope[ImportSavedFormatDeletePayload]:
    """Confirm and audit-delete one exact user-saved tabular format."""
    from moneybin.services.import_service import ImportService  # noqa: PLC0415

    with get_database(read_only=True) as db:
        plan = ImportService(db).plan_saved_format_delete(format_name)
    binding = _saved_format_delete_binding(plan)
    grant: ConfirmationGrant = await grant_confirmation_or_raise(
        binding=binding if confirmation_token is None else None,
        message=(
            f"Delete the exact saved import format {format_name!r}? "
            "The audited row can be restored with "
            "system_audit_undo(operation_id)."
        ),
        confirmation_token=confirmation_token,
    )
    with get_database(read_only=False) as db:
        operation_id = ImportService(db).delete_saved_format_confirmed(
            format_name,
            actor="mcp",
            verify=lambda live: grant.verify(_saved_format_delete_binding(live)),
        )
    return build_envelope(
        data=ImportSavedFormatDeletePayload(
            format_name=format_name,
            status="deleted",
            operation_id=operation_id,
        ),
        actions=["Use import_status(sections=['formats']) to verify the format list."],
        recovery_actions=[
            RecoveryAction(
                tool="system_audit",
                arguments={"view": "detail", "operation_id": operation_id},
                rationale="Inspect the exact saved-format deletion and undoability.",
                confidence="suggested",
                idempotent=True,
            ),
            RecoveryAction(
                tool="system_audit_undo",
                arguments={"operation_id": operation_id},
                rationale="Restore the audited saved-format row.",
                confidence="certain",
                idempotent=False,
            ),
        ],
    )


def import_formats() -> ResponseEnvelope[ImportFormatsPayload]:
    """List all available import formats (tabular + PDF, built-in and user-saved).

    The ``formats`` list holds tabular formats (CSV/Excel/etc.) with column
    mappings, sign convention, and header signature. The ``pdf_formats`` list
    (Phase 2a) holds auto-derived PDF recipes keyed by layout fingerprint:
    institution, document kind, routing target, and replay statistics. Use
    ``import_preview`` to test a tabular format against a specific file.
    """
    from moneybin.services.import_service import ImportService  # noqa: PLC0415

    pdf_format_rows: list[ImportPdfFormatRow] = []
    try:
        with get_database(read_only=True) as db:
            formats, _, pdf_formats = ImportService(db).list_formats()
            for pf in pdf_formats:
                pdf_format_rows.append(
                    ImportPdfFormatRow(
                        name=pf.name,
                        institution_name=pf.institution_name,
                        document_kind=pf.document_kind,
                        routing=pf.routing,
                        front_end=pf.front_end,
                        version=pf.version,
                        times_used=pf.times_used,
                        last_used_at=pf.last_used_at.isoformat()
                        if pf.last_used_at is not None
                        else None,
                    )
                )
    except Exception:  # noqa: BLE001 -- DB may not exist; fall back to built-in only
        from moneybin.extractors.tabular.formats import (  # noqa: PLC0415
            load_builtin_formats,
        )

        formats = load_builtin_formats()

    format_rows = [
        ImportFormatRow(
            name=fmt.name,
            institution_name=fmt.institution_name,
            file_type=fmt.file_type,
            sign_convention=fmt.sign_convention,
            date_format=fmt.date_format,
            number_format=fmt.number_format,
            multi_account=fmt.multi_account,
            header_signature=fmt.header_signature,
        )
        for fmt in sorted(formats.values(), key=lambda f: f.name)
    ]
    return build_envelope(
        data=ImportFormatsPayload(formats=format_rows, pdf_formats=pdf_format_rows),
        actions=[
            "Use import_preview to test a tabular format against a file",
            "Use import_files to import a file once you have the format name available",
        ],
    )


def _import_status_position(
    cursor: str | None,
    *,
    sections: list[Literal["imports", "formats", "inbox"]],
) -> KeysetPosition | None:
    """Decode and type-check one imports-view keyset before data access."""
    if cursor is None:
        return None
    try:
        position = decode_keyset_cursor(
            cursor,
            namespace="import_status.imports",
            scope={"import_id": None, "sections": sections},
        )
        if (
            len(position.snapshot) != 3
            or len(position.after) != 3
            or not all(
                isinstance(value, str)
                for value in (
                    position.snapshot[0],
                    position.snapshot[1],
                    position.after[0],
                    position.after[1],
                )
            )
            or not position.snapshot[1]
            or not position.after[1]
            or type(position.snapshot[2]) is not int
            or type(position.after[2]) is not int
            or position.snapshot[2] < 0
            or position.snapshot[2] != position.after[2]
            or position.snapshot[2] > position.total
        ):
            raise ValueError("invalid import keyset shape")
        snapshot = cast(tuple[str, str], position.snapshot[:2])
        after = cast(tuple[str, str], position.after[:2])
        datetime.fromisoformat(snapshot[0])
        datetime.fromisoformat(after[0])
        if compare_keyset(snapshot, after, ("desc", "desc")) > 0:
            raise ValueError("import continuation precedes its snapshot")
        return position
    except ValueError as exc:
        raise UserError(
            "Invalid import pagination cursor.",
            code=error_codes.IMPORT_CURSOR_INVALID,
        ) from exc


def _import_status_envelope(
    data: ImportStatusCoarsePayload,
    *,
    contract_types: list[type[Any]],
    total_count: int,
    returned_count: int,
    next_cursor: str | None,
    actions: list[str],
) -> ResponseEnvelope[ImportStatusCoarsePayload]:
    """Build and redact a dynamically classified import-status envelope."""
    classes = {
        data_class
        for contract_type in contract_types
        for data_class in extract_data_classes(contract_type)
    }
    tier = max(data_class.tier for data_class in classes)
    redacted = cast(ImportStatusCoarsePayload, redact_typed(data, None))
    envelope = cast(
        ResponseEnvelope[ImportStatusCoarsePayload],
        build_envelope(
            data=redacted,
            sensitivity=cast(Any, tier_to_sensitivity(tier).value),
            total_count=total_count,
            returned_count=returned_count,
            next_cursor=next_cursor,
            actions=actions,
            classes_returned=sorted(data_class.value for data_class in classes),
        ),
    )
    return replace(
        envelope,
        summary=replace(envelope.summary, has_more=next_cursor is not None),
    )


@mcp_tool(dynamic_classification=True, maximum_sensitivity=Sensitivity.MEDIUM)
def import_status_coarse(
    sections: list[Literal["imports", "formats", "inbox"]] | None = None,
    import_id: str | None = None,
    limit: Annotated[int, Field(strict=True, ge=1, le=200)] = 100,
    cursor: str | None = None,
) -> ResponseEnvelope[ImportStatusCoarsePayload]:
    """Return selected import history, format, and inbox status sections."""
    supplied: list[Literal["imports", "formats", "inbox"]] = (
        ["imports", "formats", "inbox"] if sections is None else sections
    )
    if not supplied:
        raise UserError(
            "At least one import status section is required.",
            code=error_codes.IMPORT_SECTIONS_REQUIRED,
        )
    if len(set(supplied)) != len(supplied):
        raise UserError(
            "Import status sections must not contain duplicates.",
            code=error_codes.IMPORT_SECTIONS_DUPLICATE,
        )
    requested: list[Literal["imports", "formats", "inbox"]] = [
        section for section in _IMPORT_STATUS_SECTION_ORDER if section in supplied
    ]
    if import_id is not None and requested != ["imports"]:
        raise UserError(
            "import_id is valid only when imports is the sole section.",
            code=error_codes.IMPORT_ID_NOT_ALLOWED,
        )
    if import_id is not None and (limit != 100 or cursor is not None):
        raise UserError(
            "A single import lookup does not accept pagination overrides.",
            code=error_codes.IMPORT_PAGINATION_NOT_ALLOWED,
        )
    if "imports" not in requested and (limit != 100 or cursor is not None):
        raise UserError(
            "Pagination is valid only when imports is selected.",
            code=error_codes.IMPORT_PAGINATION_NOT_ALLOWED,
        )

    position = (
        _import_status_position(cursor, sections=requested)
        if import_id is None and "imports" in requested
        else None
    )
    selected: list[
        ImportStatusImportsSection
        | ImportStatusFormatsSection
        | ImportStatusInboxSection
    ] = []
    contract_types: list[type[Any]] = []
    total_count = 0
    returned_count = 0
    next_cursor: str | None = None
    pending_cursor: tuple[tuple[str, str, int], tuple[str, str, int]] | None = None
    actions: list[str] = []

    for section in requested:
        if section == "imports":
            from moneybin.loaders import import_log

            with get_database(read_only=True) as db:
                if import_id is not None:
                    records = import_log.get_import_history(
                        db,
                        limit=1,
                        import_id=import_id,
                    )
                    count = len(records)
                    page = None
                else:
                    page = import_log.get_import_history_page(
                        db,
                        limit=limit,
                        snapshot_started_at=(
                            cast(str, position.snapshot[0])
                            if position is not None
                            else None
                        ),
                        snapshot_import_id=(
                            cast(str, position.snapshot[1])
                            if position is not None
                            else None
                        ),
                        after_started_at=(
                            cast(str, position.after[0])
                            if position is not None
                            else None
                        ),
                        after_import_id=(
                            cast(str, position.after[1])
                            if position is not None
                            else None
                        ),
                        snapshot_total=(
                            cast(int, position.snapshot[2])
                            if position is not None
                            else None
                        ),
                    )
                    records = page.records
                    count = page.total_count
            selected.append(ImportStatusImportsSection(records=records))
            contract_types.append(ImportStatusImportsSection)
            total_count += count
            returned_count += len(records)
            if import_id is None and page is not None and page.has_more:
                if (
                    not records
                    or page.snapshot_started_at is None
                    or page.snapshot_import_id is None
                ):
                    raise RuntimeError("non-empty import snapshot is missing its head")
                last = records[-1]
                pending_cursor = (
                    (
                        page.snapshot_started_at,
                        page.snapshot_import_id,
                        count,
                    ),
                    (
                        str(last["started_at"]),
                        str(last["import_id"]),
                        count,
                    ),
                )
        elif section == "formats":
            body = inspect.unwrap(import_formats)
            response = cast(ResponseEnvelope[ImportFormatsPayload], body())
            if response.error is not None:
                return cast(ResponseEnvelope[ImportStatusCoarsePayload], response)
            formats = ImportStatusFormatsSection(
                formats=response.data.formats,
                pdf_formats=response.data.pdf_formats,
            )
            selected.append(formats)
            contract_types.append(ImportStatusFormatsSection)
            count = len(formats.formats) + len(formats.pdf_formats)
            total_count += count
            returned_count += count
            actions.extend(response.actions)
        else:
            from moneybin.mcp.tools.import_inbox import read_import_inbox_pending

            pending = read_import_inbox_pending()
            inbox = ImportStatusInboxSection(
                would_process=pending.would_process,
                ignored=pending.ignored,
            )
            selected.append(inbox)
            contract_types.append(ImportStatusInboxSection)
            count = len(inbox.would_process) + len(inbox.ignored)
            total_count += count
            returned_count += count
            actions.append("Use import_inbox_sync to drain the inbox")

    stable_total = position.total if position is not None else total_count
    if pending_cursor is not None:
        next_cursor = encode_keyset_cursor(
            namespace="import_status.imports",
            scope={"import_id": None, "sections": requested},
            snapshot=pending_cursor[0],
            after=pending_cursor[1],
            total=stable_total,
        )
        actions.append(
            f"Continue with import_status(sections={requested!r}, limit={limit}, "
            f"cursor={next_cursor!r})"
        )
    if "imports" in requested:
        actions.append("Use import_files to import a new file")
    payload = ImportStatusCoarsePayload(sections=selected)
    return _import_status_envelope(
        payload,
        contract_types=contract_types,
        total_count=stable_total,
        returned_count=returned_count,
        next_cursor=next_cursor,
        actions=list(dict.fromkeys(actions)),
    )


def _sign_confirmation_message(payload: dict[str, Any], *, source: str) -> str:
    """Describe the exact ledger-wide change a human must ratify.

    `source` is named by the caller rather than derived from `payload["channel"]`:
    the bridge and deterministic-PDF paths both carry channel="pdf", and telling
    a human their deterministic statement is a "bridge recipe" describes a step
    that never ran.

    This is the last thing a human sees before a ledger-wide sign change is
    applied, so it branches on `sign_prior_convention` for the same reason the
    CLI and `actions[]` renderers do — and this is the costliest place to get it
    wrong. A first-contact inference always proposes `negative_is_income`, where
    "is this a credit card?" is the accurate and answerable question. A
    self-healed recipe (Req 9a) can re-derive to the *opposite* polarity, and
    the card wording would then describe the reverse of what approving does.
    """
    evidence = ", ".join(str(item) for item in payload["sign_evidence"])
    sample_rows = payload["sign_sample_rows"][:3]
    samples = (
        f"Sample rows (printed amount → MoneyBin amount): {sample_rows}\n\n"
        if sample_rows
        else "\n"
    )
    prior = payload.get("sign_prior_convention")
    proposed = payload.get("sign_convention")
    if prior:
        lead = (
            f"The saved layout for this {source} stopped reading it correctly "
            f"and was re-derived. It recorded amounts as {prior!r} before "
            f"({sign_convention_effect(str(prior))}); the re-derived version "
            f"records them as {proposed!r} "
            f"({sign_convention_effect(str(proposed))}). Every amount's sign "
            f"flips relative to earlier imports of this format.\n\n"
        )
        question = f"Approve this change from {prior!r} to {proposed!r}?"
    else:
        lead = (
            f"This {source} identifies the file as a credit card and will "
            "reverse every amount: charges become negative expenses and "
            "payments become positive income.\n\n"
        )
        question = "Approve this sign inversion?"
    return f"{lead}Evidence from the file: {evidence}.\n{samples}{question}"


def _load_import_confirm_preview(
    preview_id: str,
) -> tuple[dict[str, Any], Path, bytes]:
    """Load one persisted preview and its immutable source object."""
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    with get_database(read_only=True) as db:
        repo = ImportPreviewsRepo(db)
        preview = repo.get(preview_id)
        if preview is None:
            raise UserError(
                "Import preview was not found.",
                code=error_codes.IMPORT_PREVIEW_NOT_FOUND,
            )
        path = _validate_file_path(preview["file_path"])
        source_bytes = repo.get_source_bytes(preview_id)
        if source_bytes is None:
            raise UserError(
                "The immutable import preview snapshot is unavailable.",
                code=error_codes.IMPORT_PREVIEW_SNAPSHOT_MISSING,
            )
    return preview, path, source_bytes


def _canonical_json_sha256(value: object) -> str:
    """Hash one JSON value using the confirmation broker's canonical form."""
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _import_sign_confirmation_binding(
    *,
    preview_id: str,
    path: Path,
    source_bytes: bytes,
    channel: str,
    proposal: dict[str, object],
    bridge_response: dict[str, Any] | None,
    save_format: bool,
    account_id: str | None,
    account_name: str | None,
    account_bindings: dict[str, str] | None,
    account_metadata: dict[str, dict[str, str]] | None,
) -> ConfirmationBinding:
    """Bind one sign approval to the exact preview, proposal, and request."""
    source_sha256 = hashlib.sha256(source_bytes).hexdigest()
    arguments = TypeAdapter(dict[str, JsonValue]).validate_python({
        "preview_id": preview_id,
        "path": str(path),
        "channel": channel,
        "source_sha256": source_sha256,
        "source_size_bytes": len(source_bytes),
        "proposal_sha256": _canonical_json_sha256(proposal),
        "bridge_response_sha256": (
            _canonical_json_sha256(bridge_response)
            if bridge_response is not None
            else None
        ),
        "save_format": save_format,
        "account_id": account_id,
        "account_name": account_name,
        "account_bindings": account_bindings,
        "account_metadata": account_metadata,
    })
    return ConfirmationBinding(
        arguments=arguments,
        resolved_ids=(preview_id, source_sha256),
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="import_sign_confirmation",
        blast_radius={"imports": 1, "source_snapshots": 1},
    )


def _run_import_confirm_attempt(
    *,
    preview_id: str,
    path: Path,
    source_bytes: bytes,
    channel: str,
    pdf_sign: bool,
    bridge_response: dict[str, Any] | None,
    save_format: bool,
    account_id: str | None,
    account_name: str | None,
    account_bindings: dict[str, str] | None,
    account_metadata: dict[str, dict[str, str]] | None,
    approved_sign_proposal: dict[str, object] | None,
    confirmation_grant: ConfirmationGrant | None,
    started: float,
) -> tuple[Any | None, Any | None, str] | ResponseEnvelope[ImportConfirmCoarsePayload]:
    """Run one atomic import attempt off the MCP event loop."""
    from moneybin.metrics.observations import MetricObservations
    from moneybin.metrics.registry import IMPORT_DURATION_SECONDS, IMPORT_RECORDS_TOTAL
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo
    from moneybin.services.import_confirmation import (
        ImportConfirmationRequiredError,
        confirmation_payload_dict,
    )
    from moneybin.services.import_service import ImportService

    observations = MetricObservations()
    bridge_result: Any | None = None
    result: Any | None = None
    with get_database(read_only=False) as db:
        repo = ImportPreviewsRepo(db)
        live_identity = _file_identity(path)
        db.begin()
        try:
            consumed = repo.consume(
                preview_id,
                file_sha256=live_identity[0],
                file_size_bytes=live_identity[1],
                now=datetime.now(UTC),
                actor="mcp",
                in_outer_txn=True,
            )
            service = ImportService(db)
            reviewed_plan: Any | None = None
            if channel != "pdf":
                from moneybin.services.import_service import ReviewedTabularPlan

                plan_value = consumed["snapshot_json"].get("plan")
                if not isinstance(plan_value, dict):
                    raise UserError(
                        "The persisted preview lacks its normalized import plan.",
                        code=error_codes.IMPORT_PREVIEW_PLAN_MISSING,
                    )
                reviewed_plan = ReviewedTabularPlan.from_dict(
                    cast(dict[str, Any], plan_value)
                )

            def run_channel(
                *,
                confirm_sign: bool,
                channel_observations: MetricObservations,
            ) -> tuple[Any | None, Any | None, str | None]:
                """Run the selected channel without crossing the transaction boundary."""
                if channel == "pdf" and pdf_sign:
                    channel_result = service.import_file(
                        path,
                        source_bytes=source_bytes,
                        save_format=save_format,
                        account_id=account_id,
                        account_bindings=account_bindings,
                        actor_kind="agent",
                        confirm=confirm_sign,
                        refresh=False,
                        in_outer_txn=True,
                        emit_metrics=False,
                        observations=channel_observations,
                    )
                    return None, channel_result, channel_result.import_id
                if channel == "pdf":
                    channel_bridge_result = service.apply_pdf_bridge_response(
                        path,
                        cast(dict[str, Any], bridge_response),
                        save_format=save_format,
                        account_id=account_id,
                        account_bindings=account_bindings,
                        source_bytes=source_bytes,
                        in_outer_txn=True,
                        emit_metrics=False,
                        observations=channel_observations,
                        confirm=confirm_sign,
                    )
                    return (
                        channel_bridge_result,
                        None,
                        channel_bridge_result.import_id,
                    )
                channel_result = service.import_file(
                    path,
                    source_bytes=source_bytes,
                    reviewed_plan=reviewed_plan,
                    save_format=save_format,
                    account_id=account_id,
                    account_name=account_name,
                    account_bindings=account_bindings,
                    account_metadata=account_metadata,
                    actor_kind="agent",
                    human_sign_confirmation=confirm_sign,
                    refresh=False,
                    in_outer_txn=True,
                    emit_metrics=False,
                    observations=channel_observations,
                )
                return None, channel_result, channel_result.import_id

            if approved_sign_proposal is not None:
                if confirmation_grant is None:
                    raise RuntimeError("approved sign proposal lacks a grant")
                try:
                    run_channel(
                        confirm_sign=False,
                        channel_observations=MetricObservations(),
                    )
                except ImportConfirmationRequiredError as current_confirmation:
                    current = current_confirmation.outcome
                    if (
                        current.reason == "sign_convention"
                        and confirmation_payload_dict(current) == approved_sign_proposal
                    ):
                        pass
                    else:
                        raise UserError(
                            "The sign proposal changed after approval; re-run the "
                            "preview and review the current proposal.",
                            code=error_codes.IMPORT_SIGN_PROPOSAL_CHANGED,
                        ) from current_confirmation
                else:
                    raise UserError(
                        "The approved sign proposal is no longer pending; re-run the "
                        "preview before importing.",
                        code=error_codes.IMPORT_SIGN_PROPOSAL_CHANGED,
                    )
                confirmation_grant.verify(
                    _import_sign_confirmation_binding(
                        preview_id=preview_id,
                        path=path,
                        source_bytes=source_bytes,
                        channel=channel,
                        proposal=approved_sign_proposal,
                        bridge_response=bridge_response,
                        save_format=save_format,
                        account_id=account_id,
                        account_name=account_name,
                        account_bindings=account_bindings,
                        account_metadata=account_metadata,
                    )
                )

            channel_observations = (
                MetricObservations()
                if confirmation_grant is not None and approved_sign_proposal is None
                else observations
            )
            bridge_result, result, import_id = run_channel(
                confirm_sign=approved_sign_proposal is not None,
                channel_observations=channel_observations,
            )
            if confirmation_grant is not None and approved_sign_proposal is None:
                raise UserError(
                    "The confirmation token does not match a pending sign proposal.",
                    code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
                )
            if bridge_result is not None and bridge_result.outcome == "invalid":
                db.rollback()
                observations.flush("rollback")
                return cast(
                    ResponseEnvelope[ImportConfirmCoarsePayload],
                    _import_dynamic_envelope(
                        ImportPdfBridgeInvalidPayload(
                            kind="pdf_bridge_invalid",
                            preview_id=preview_id,
                            status="invalid",
                            reject_reason=bridge_result.reject_reason,
                            expected_row_count=bridge_result.expected_row_count,
                            actual_row_count=bridge_result.actual_row_count,
                            rows_diverged=bridge_result.rows_diverged,
                        ),
                        actions=[
                            "Revise the bridge recipe or extracted rows and retry "
                            "this same preview before it expires.",
                        ],
                    ),
                )
            if import_id is None:
                raise RuntimeError("confirmed import completed without an import ID")
            repo.record_result(
                preview_id,
                import_id=import_id,
                actor="mcp",
                in_outer_txn=True,
            )
            db.commit()
            source_type = "pdf" if channel == "pdf" else "tabular"
            if bridge_result is not None:
                committed_rows = bridge_result.rows_loaded
            elif result is not None:
                committed_rows = result.transactions
            else:
                raise RuntimeError("confirmed import produced no channel result")
            observations.counter(
                IMPORT_RECORDS_TOTAL,
                labels={"source_type": source_type},
                amount=committed_rows,
            )
            observations.observe(
                IMPORT_DURATION_SECONDS,
                time.monotonic() - started,
                labels={"source_type": source_type},
            )
            observations.flush("commit")
        except BaseException:
            db.rollback()
            observations.flush("rollback")
            raise

    from moneybin.services.inbox_service import InboxService

    InboxService.for_active_profile_no_db().archive_confirmed_file(path)
    return bridge_result, result, import_id


def _import_confirm_coarse_confirmation_actions(
    preview_id: str,
    file_path: str,
    outcome: ConfirmationRequired,
    *,
    sign_already_approved: bool = False,
) -> list[str]:
    """Build the actions[] hints for a non-sign confirmation_required envelope.

    Staged previews are immutable (Req 8 in smart-import-confirmation.md): a
    column-mapping correction requires a fresh import_preview, not a mutated
    confirm call. Only ``account_confirmation`` is resolvable by re-calling
    import_confirm on the SAME preview_id — the column mapping is already
    settled and only the account identity is open.

    ``sign_already_approved`` says the human ratified a sign inversion earlier
    in this same call. That approval died with the rolled-back transaction, so
    the next call re-enters the sign gate — say so, or the second prompt reads
    as a bug and the agent reports one remaining step when there are two.
    """
    actions: list[str] = []
    if outcome.error_message:
        actions.append(f"Validation failed: {outcome.error_message}")
    if sign_already_approved:
        actions.append(
            "The sign-inversion approval just given is NOT persisted — it was "
            "rolled back with this attempt. The next import_confirm call will "
            "ask the human to approve the inversion again."
        )
    if outcome.reason == "account_confirmation":
        # Never interpolate source_account_key here. It is the native OFX/Plaid
        # identifier (ACCOUNT_IDENTIFIER -> CRITICAL), and _import_dynamic_envelope
        # redacts `data` only — a key named in this prose ships unmasked.
        # account_bindings still matches on the raw key, which the agent cannot
        # see, so multi-account binding is a CLI capability until a non-sensitive
        # proposal handle exists.
        actions.append(
            f"Use import_confirm(preview_id={preview_id!r}, "
            "account_name='<name>') to bind the single account this file "
            f"belongs to. {len(outcome.account_proposals)} source account(s) "
            "were detected; data.account_proposals[] describes them with "
            "identifiers masked. Binding several accounts at once keys on the "
            "unmasked source key, so it runs from the CLI: `moneybin import "
            "confirm <file> --account-binding <source_key>=<account_id|new>`."
        )
        return actions
    # Every reason the service narrows gets the same text the preview-side
    # builder uses. Two hand-kept branch lists over one set of states is what
    # let these drift apart for three rounds; one helper per state is what
    # keeps them together.
    if outcome.reason == "header_row_consumed":
        from moneybin.services.import_confirmation import (
            header_row_consumed_recovery_mcp,
        )

        actions.append(header_row_consumed_recovery_mcp())
        return actions
    if outcome.reason == "unreadable_date":
        # The generic hint below prescribes a mapping= retry, which cannot
        # change how the mapped column's values parse — for the half of this
        # reason where the column is right, it stages the same unconfirmable
        # preview forever.
        from moneybin.services.import_confirmation import (
            unreadable_date_recovery_mcp,
        )

        actions.append(unreadable_date_recovery_mcp(file_path))
        return actions
    actions.append(
        f"This preview's mapping cannot be corrected in place — staged "
        "previews are immutable. Use import_preview(file_path="
        f"{file_path!r}, mapping={{'<dest_field>': '<source_column>'}}) to "
        "build a corrected preview, then import_confirm(preview_id=...) on "
        "the new preview_id."
    )
    return actions


@mcp_tool(
    read_only=False,
    idempotent=False,
    timeout_seconds=180.0,
    dynamic_classification=True,
    # CRITICAL, matching import_preview_coarse: the account-confirmation branch
    # returns account_proposals[].source_account_key (ACCOUNT_IDENTIFIER). It is
    # redacted before it ships, but the declared ceiling names what the tool can
    # classify, not what survives masking.
    maximum_sensitivity=Sensitivity.CRITICAL,
)
async def import_confirm_coarse(
    preview_id: str,
    *,
    bridge_response: dict[str, Any] | None = None,
    save_format: bool = True,
    account_id: str | None = None,
    account_name: str | None = None,
    account_bindings: dict[str, str] | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
    confirmation_token: str | None = None,
) -> ResponseEnvelope[ImportConfirmCoarsePayload]:
    """Consume one unchanged preview, eliciting human approval for sign inversion."""
    from moneybin.services.import_confirmation import (
        ConfirmationRequired,
        ImportConfirmationRequiredError,
        confirmation_payload_dict,
    )

    confirmation_grant: ConfirmationGrant | None = None
    if confirmation_token is not None:
        confirmation_grant = await grant_confirmation_or_raise(
            binding=None,
            message="",
            confirmation_token=confirmation_token,
        )
    started = time.monotonic()
    preview, path, source_bytes = await asyncio.to_thread(
        _load_import_confirm_preview,
        preview_id,
    )
    snapshot_data = cast(
        dict[str, Any],
        preview["snapshot_json"]["data"],
    )
    channel = preview["channel"]
    if channel == "ofx":
        raise UserError(
            "OFX/QFX/QBO files import directly with import_files.",
            code=error_codes.IMPORT_PREVIEW_DIRECT_IMPORT_REQUIRED,
        )
    pdf_sign = channel == "pdf" and snapshot_data.get("reason") == "sign_convention"
    if channel == "pdf":
        _reject_unsupported_pdf_account_signals(
            account_name=account_name,
            account_metadata=account_metadata,
        )
        if pdf_sign:
            if bridge_response is not None:
                raise UserError(
                    "A PDF sign preview does not accept bridge_response.",
                    code=error_codes.IMPORT_PREVIEW_CHANNEL_CONFLICT,
                )
        elif (
            snapshot_data.get("status") != "confirmation_required"
            or "bridge_payload" not in snapshot_data
        ):
            raise UserError(
                "This PDF preview does not require a bridge response; use "
                "import_files to run its deterministic or seed path.",
                code=error_codes.IMPORT_PREVIEW_DIRECT_IMPORT_REQUIRED,
            )
        elif bridge_response is None:
            raise UserError(
                "A PDF bridge preview requires bridge_response={recipe, rows}.",
                code=error_codes.IMPORT_PREVIEW_BRIDGE_RESPONSE_REQUIRED,
            )
    elif bridge_response is not None:
        raise UserError(
            "bridge_response is valid only for a PDF bridge preview.",
            code=error_codes.IMPORT_PREVIEW_CHANNEL_CONFLICT,
        )

    bridge_result: Any | None = None
    result: Any | None = None
    import_id: str | None = None
    approved_sign_proposal: dict[str, object] | None = None
    while True:
        try:
            attempt = await asyncio.to_thread(
                _run_import_confirm_attempt,
                preview_id=preview_id,
                path=path,
                source_bytes=source_bytes,
                channel=channel,
                pdf_sign=pdf_sign,
                bridge_response=bridge_response,
                save_format=save_format,
                account_id=account_id,
                account_name=account_name,
                account_bindings=account_bindings,
                account_metadata=account_metadata,
                approved_sign_proposal=approved_sign_proposal,
                confirmation_grant=confirmation_grant,
                started=started,
            )
        except ImportConfirmationRequiredError as confirmation:
            raw_outcome = getattr(cast(object, confirmation), "outcome", None)
            if not isinstance(raw_outcome, ConfirmationRequired):
                raise RuntimeError(
                    "confirmation error lacks a typed outcome"
                ) from confirmation
            outcome = raw_outcome
            if outcome.reason != "sign_convention":
                wire = confirmation_payload_dict(outcome)
                # Constant-empty on every non-sign reason; see the payload.
                for sign_key in (
                    "sign_convention",
                    "sign_prior_convention",
                    "sign_evidence",
                    "sign_sample_rows",
                ):
                    wire.pop(sign_key, None)
                return cast(
                    ResponseEnvelope[ImportConfirmCoarsePayload],
                    _import_dynamic_envelope(
                        ImportConfirmRequiredPayload(
                            preview_id=preview_id,
                            **cast(Any, wire),
                        ),
                        actions=_import_confirm_coarse_confirmation_actions(
                            preview_id,
                            str(path),
                            outcome,
                            sign_already_approved=approved_sign_proposal is not None,
                        ),
                    ),
                )
            if approved_sign_proposal is not None:
                raise
        else:
            if isinstance(attempt, ResponseEnvelope):
                return attempt
            bridge_result, result, import_id = attempt
            break
        source = (
            "PDF bridge recipe"
            if bridge_response is not None
            else "PDF statement"
            if channel == "pdf"
            else "tabular import"
        )
        approved_sign_proposal = confirmation_payload_dict(outcome)
        binding = _import_sign_confirmation_binding(
            preview_id=preview_id,
            path=path,
            source_bytes=source_bytes,
            channel=channel,
            proposal=approved_sign_proposal,
            bridge_response=bridge_response,
            save_format=save_format,
            account_id=account_id,
            account_name=account_name,
            account_bindings=account_bindings,
            account_metadata=account_metadata,
        )
        if confirmation_grant is None:
            confirmation_grant = await grant_confirmation_or_raise(
                binding=binding,
                message=_sign_confirmation_message(
                    approved_sign_proposal,
                    source=source,
                ),
                confirmation_token=None,
            )
        started = time.monotonic()

    if bridge_result is None and result is None:
        raise RuntimeError("confirmed import produced no channel result")
    if bridge_result is not None:
        rows_loaded = bridge_result.rows_loaded
        merged_mapping: dict[str, str] = {}
    else:
        if result is None:
            raise RuntimeError("tabular import produced no result")
        rows_loaded = result.transactions
        merged_mapping = dict(result.field_mapping or {})
    if bridge_result is not None:
        payload: Any = ImportPdfBridgeAppliedPayload(
            preview_id=preview_id,
            import_id=import_id,
            rows_loaded=rows_loaded,
            merged_mapping=merged_mapping,
            format_name=bridge_result.format_name,
        )
    elif channel == "pdf":
        if result is None:
            raise RuntimeError("PDF sign import produced no result")
        payload = ImportPdfSignAppliedPayload(
            preview_id=preview_id,
            import_id=import_id,
            rows_loaded=rows_loaded,
            format_name=result.pdf_format_name,
        )
    else:
        payload = ImportTabularConfirmCoarsePayload(
            preview_id=preview_id,
            import_id=import_id,
            rows_loaded=rows_loaded,
            merged_mapping=merged_mapping,
        )
    actions = [
        f"Use import_revert(import_id='{import_id}') to undo this import.",
        "Use import_status(sections=['imports'], "
        f"import_id='{import_id}') to verify it.",
    ]
    if bridge_result is not None and bridge_result.rows_diverged:
        actions.insert(
            0,
            f"The bridge recipe reproduced {bridge_result.actual_row_count} rows "
            f"instead of the {bridge_result.expected_row_count} returned by the "
            "agent; the reconciled reproduced rows were loaded.",
        )
    if result is not None and result.sign_correction_suggested:
        actions.insert(
            0,
            "Sign convention may be inverted — inspect the imported amounts.",
        )
    return cast(
        ResponseEnvelope[ImportConfirmCoarsePayload],
        _import_dynamic_envelope(
            payload,
            actions=actions,
        ),
    )


def register_import_coarse_reads(mcp: FastMCP) -> None:
    """Register the standard import status read."""
    register(
        mcp,
        import_status_coarse,
        "import_status",
        "Read selected import history, available format, and pending inbox "
        "sections. Import history has exact cursor pagination; import_id is "
        "valid only when imports is the sole section.",
        privacy_actor="import_status",
    )


@mcp_tool(read_only=False, idempotent=False)
def import_files_coarse(
    paths: list[str],
    refresh: bool = True,
    force: bool = False,
) -> ResponseEnvelope[ImportFilesPayload]:
    """Import files while keeping actions inside the staged-import cohort."""
    response = import_files(
        paths=paths,
        refresh=refresh,
        force=force,
    )
    actions = list(response.actions)
    for row in response.data.files:
        if row.status == "confirmation_required":
            actions.append(
                f"Use import_preview(file_path={row.path!r}) to begin the "
                "reviewed confirmation workflow."
            )
        elif row.import_id is not None:
            actions.append(
                "Use import_status(sections=['imports'], "
                f"import_id='{row.import_id}') to verify the result."
            )
    return replace(response, actions=list(dict.fromkeys(actions)))


@mcp_tool(
    read_only=False,
    destructive=True,
    idempotent=False,
    timeout_seconds=180.0,
)
async def import_revert_coarse(
    import_id: str | None = None,
    operation: Literal["revert_import", "delete_saved_format"] = "revert_import",
    format_name: str | None = None,
    confirmation_token: str | None = None,
) -> ResponseEnvelope[ImportRevertPayload | ImportSavedFormatDeletePayload]:
    """Revert one import or audit-delete one user-saved format."""
    if operation == "revert_import":
        if not import_id or format_name is not None or confirmation_token is not None:
            raise UserError(
                "operation='revert_import' requires exactly import_id and does "
                "not accept confirmation_token.",
                code=error_codes.IMPORT_REVERT_INVALID_TARGET,
            )
    elif import_id is not None or not format_name:
        raise UserError(
            "operation='delete_saved_format' requires exactly format_name.",
            code=error_codes.IMPORT_REVERT_INVALID_TARGET,
        )

    if operation == "delete_saved_format":
        return cast(
            ResponseEnvelope[ImportRevertPayload | ImportSavedFormatDeletePayload],
            await _delete_saved_format(
                cast(str, format_name),
                confirmation_token=confirmation_token,
            ),
        )

    response = import_revert(import_id=cast(str, import_id))
    if response.error is not None:
        return cast(
            ResponseEnvelope[ImportRevertPayload | ImportSavedFormatDeletePayload],
            response,
        )
    return cast(
        ResponseEnvelope[ImportRevertPayload | ImportSavedFormatDeletePayload],
        replace(
            response,
            actions=list(
                dict.fromkeys([
                    *response.actions,
                    "Use import_status(sections=['imports'], "
                    f"import_id='{import_id}') to verify the reverted state.",
                ])
            ),
        ),
    )


@mcp_tool(read_only=False, idempotent=False)
def import_inbox_sync_coarse(
    refresh: bool = True,
) -> ResponseEnvelope[ImportInboxSyncPayload]:
    """Drain the inbox with staged-import-only next actions."""
    from moneybin.mcp.tools.import_inbox import import_inbox_sync

    response = inspect.unwrap(import_inbox_sync)(refresh=refresh)
    return replace(
        response,
        actions=[
            "Use import_status(sections=['inbox', 'imports']) to inspect the "
            "result and any remaining pending files."
        ],
    )


@mcp_tool(read_only=False)
def import_labels_set_coarse(
    import_id: str,
    labels: list[str],
) -> ResponseEnvelope[ImportLabelsSetPayload]:
    """Set labels with an isolated status follow-up."""
    from moneybin.mcp.tools.curation import import_labels_set

    response = inspect.unwrap(import_labels_set)(import_id=import_id, labels=labels)
    return replace(
        response,
        actions=[
            "Use import_status(sections=['imports'], "
            f"import_id='{import_id}') to inspect the import."
        ],
    )


def register_import_workflow_tools(mcp: FastMCP) -> None:
    """Register the standard seven-boundary staged-import workflow."""
    for callback, name, description in (
        (import_files_coarse, "import_files", "Import one or more files."),
        (
            import_preview_coarse,
            "import_preview",
            "Persist an exact, expiring staged-import preview. For a tabular "
            "file, mapping={destination_field: source_column} corrects the "
            "detected column mapping — it merges onto the proposal rather than "
            "replacing it, so name only the fields to change. Destinations are "
            "transaction_date, description, and either amount or the "
            "debit_amount/credit_amount pair (never both shapes). A staged "
            "preview is immutable, so this is the only place to correct a "
            "mapping; import_confirm ratifies the preview unchanged.",
        ),
        (
            import_confirm_coarse,
            "import_confirm",
            "Atomically consume an unchanged preview and import its file, "
            "eliciting human approval before any sign inversion. When the "
            "file's accounts are unresolved it imports nothing and returns "
            "account proposals whose source_account_key is masked; bind a "
            "single account with account_name, or several with "
            "moneybin import confirm <file> --account-binding.",
        ),
        (
            import_status_coarse,
            "import_status",
            "Read import history, formats, and inbox state.",
        ),
        (
            import_revert_coarse,
            "import_revert",
            "Revert one completed import or delete one user-saved format. Import "
            "reversion permanently removes its raw rows; saved-format deletion "
            "writes app.tabular_formats, requires exact payload-bound confirmation, "
            "and is recoverable with system_audit_undo.",
        ),
        (
            import_inbox_sync_coarse,
            "import_inbox_sync",
            "Synchronize the import inbox.",
        ),
        (
            import_labels_set_coarse,
            "import_labels_set",
            "Set labels on one import.",
        ),
    ):
        register(
            mcp,
            callback,
            name,
            description,
            privacy_actor=name,
            input_schema_extra=(
                _IMPORT_REVERT_INPUT_SCHEMA_EXTRA if name == "import_revert" else None
            ),
        )


def register_import_tools(mcp: FastMCP) -> None:
    """Register the standard staged-import workflow."""
    register_import_workflow_tools(mcp)


_LEGACY_INTERNAL_CALLBACKS = (
    import_status,
    import_formats,
)

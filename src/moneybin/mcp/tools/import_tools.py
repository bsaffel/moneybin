# src/moneybin/mcp/tools/import_tools.py
"""Import namespace tools — file import, preview, status, revert, format listing.

Tools:
    - import_files — Import one or more financial data files (medium sensitivity)
    - import_preview — Preview a file's structure without importing (medium sensitivity)
    - import_status — List past import batches (low sensitivity)
    - import_revert — Undo an import batch by import_id (low sensitivity)
    - import_formats — List available tabular import formats (low sensitivity)
    - import_confirm — Confirm or override a proposed column mapping and load the file
"""

from __future__ import annotations

import base64
import binascii
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
from pydantic import Field, TypeAdapter

if TYPE_CHECKING:
    from moneybin.services.import_confirmation import ConfirmationRequired

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import tier_to_sensitivity
from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.payloads.imports import (
    ImportConfirmCoarsePayload,
    ImportConfirmPayload,
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
    ImportPdfSignPreviewPayload,
    ImportPerFileRow,
    ImportPreviewCoarsePayload,
    ImportPreviewPayload,
    ImportRevertPayload,
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

logger = logging.getLogger(__name__)

_IMPORT_STATUS_SECTION_ORDER: tuple[Literal["imports", "formats", "inbox"], ...] = (
    "imports",
    "formats",
    "inbox",
)


def _validate_file_path(file_path: str) -> Path:
    """Validate and resolve a file path, raising UserError if invalid."""
    resolved = Path(file_path).expanduser().resolve()
    if not resolved.is_relative_to(Path.home()):
        raise UserError(
            "file_path must be within the user's home directory. "
            "Path traversal and symlinks that escape the home directory "
            "are not allowed.",
            code="invalid_file_path",
        )
    return resolved


def _confirmation_actions(file_path: str, outcome: ConfirmationRequired) -> list[str]:
    """Build the actions[] hints for a confirmation_required envelope.

    Omits the `accept=True` suggestion on `low`-tier proposals because
    `resolve_or_confirm` rejects Accept on low (the detector couldn't
    form a complete mapping); recovery requires a partial-merge
    `mapping=...` override.
    """
    actions: list[str] = []
    if outcome.error_message:
        # Surface validation_failure detail first so the agent / human
        # sees WHY their last attempt was rejected (which override key
        # was unknown, which source column was missing, etc.) before
        # the generic recovery hints.
        actions.append(f"Validation failed: {outcome.error_message}")
    if outcome.reason == "account_confirmation":
        # The column mapping is settled; only the account identity is open.
        # accept=True ratifies the mapping and account_bindings answers the
        # account in one call — a bare accept (no binding) loops back to the
        # account gate and a mapping override is irrelevant. Bind every proposal
        # (the gate is all-or-nothing).
        keys = [
            str(p.get("source_account_key", "")) for p in outcome.account_proposals
        ] or ["<source_key>"]
        binding_map = ", ".join(f"'{k}': '<account_id|new>'" for k in keys)
        actions.append(
            f"Use import_confirm(file_path='{file_path}', accept=True, "
            f"account_bindings={{{binding_map}}}) to ratify the mapping and bind "
            "every account; source keys are in "
            "data.account_proposals[].source_account_key."
        )
        return actions
    if outcome.confidence.tier != "low":
        actions.append(
            f"Use import_confirm(file_path='{file_path}', accept=True) "
            "to accept the proposed mapping as-is."
        )
    actions.append(
        f"Use import_confirm(file_path='{file_path}', "
        "mapping={'<dest_field>': '<source_column>'}) "
        "to override specific fields (required on low-tier proposals)."
    )
    actions.append(
        f"Use import_preview(file_path='{file_path}') "
        "to inspect the proposal and samples in detail."
    )
    return actions


def _bridge_confirm_action(file_path: str, *, payload_ref: str) -> str:
    """The agent-facing hint for a PDF bridge confirmation_required.

    Shared by the `import_files` actions builder and `_import_preview_pdf` so the
    bridge workflow instruction stays identical across both entry points.
    `payload_ref` names where the bridge payload sits in *that* envelope —
    nested under `confirmation_payload.bridge_payload` for `import_files`,
    top-level `bridge_payload` for the preview envelope.
    """
    return (
        f"This PDF needs agent extraction. Read {payload_ref} (note its "
        "transparency_notice — proceeding surfaces the document to you), "
        "propose a recipe + rows, then call import_confirm("
        # repr() so a path containing a quote stays a valid string literal in
        # the suggested call (e.g. /home/alice/O'Brien/statement.pdf).
        f"file_path={file_path!r}, bridge_response={{'recipe': ..., "
        "'rows': [...]}) to reconcile and load."
    )


def _sign_confirm_actions(file_path: str, error_message: str) -> list[str]:
    """The agent-facing hints for a PDF sign-convention confirmation_required.

    Ratifying a card statement inverts every amount, and a wrong flip corrupts the
    ledger on this import and on every future replay of the format — so the agent
    must NOT ratify it. MCP has no in-place path to confirm a sign inversion yet
    (elicitation-based confirm is planned); until then the human resolves it in a
    terminal, where inverting every amount is a deliberate, visible act. The hints
    therefore point at the CLI, not at an `import_confirm` call that cannot ratify
    a sign flip.
    """
    quoted = shlex.quote(file_path)
    return [
        error_message,
        "Show the user sign_sample_rows — what the statement printed vs what "
        "MoneyBin would record — so THEY decide. MCP cannot ratify a sign "
        "inversion in place yet; resolve it in a terminal:",
        f"If it IS a credit card: moneybin import files {quoted} --confirm "
        "(records charges as expenses).",
        f"If it is NOT a credit card: moneybin import files {quoted} "
        "--sign negative_is_expense (records amounts exactly as printed).",
    ]


@mcp_tool(read_only=False, idempotent=False)
def import_files(
    paths: list[str], refresh: bool = True, force: bool = False
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

    Returns:
        Envelope with data containing imported/failed/total counts,
        transforms state, and a "files" list of per-file results.
        Amounts use accounting convention: negative=expense,
        positive=income; transfers exempt. Display currency is set
        in summary.display_currency.
    """
    from moneybin.services.import_confirmation import ImportConfirmationRequiredError
    from moneybin.services.import_service import ImportService

    # Validate all paths upfront so a bad path fails before any service call.
    validated = [_validate_file_path(p) for p in paths]

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
            )

            error_type = type(e).__name__
            logger.warning(f"Import failed for {validated[0]}: {error_type}")
            batch = BatchImportResult(
                per_file=[
                    PerFileResult(
                        path=str(validated[0]),
                        status="failed",
                        source_type=None,
                        rows_loaded=0,
                        import_id=loaded_import_id,
                        error=error_type,
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
            sign_correction_suggested=r.sign_correction_suggested,
            sign_override_replayed=r.sign_override_replayed,
            confirmation_payload=r.confirmation_payload,
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
        raw_tier = payload.get("tier")
        tier = raw_tier if isinstance(raw_tier, str) else "low"
        raw_err = payload.get("error_message")
        err_msg = raw_err if isinstance(raw_err, str) else ""
        # PDF sign-convention channel (credit-card inversion): the agent must NOT
        # ratify it — a wrong flip reverses every amount. MCP can't confirm the
        # inversion in place yet, so _sign_confirm_actions points at the terminal
        # recovery. It owns err_msg as its lead line, so skip the generic
        # "Validation failed" prefix (this is a proposal, not a validation error).
        if payload.get("reason") == "sign_convention":
            actions.extend(_sign_confirm_actions(pending.path, err_msg))
            continue
        if err_msg:
            actions.append(f"Validation failed: {err_msg}")
        # PDF bridge channel: the agent must read confirmation_payload's
        # bridge_payload (document text + transparency notice), extract rows
        # per the recipe request, and ratify with bridge_response — there is
        # no column mapping to accept/override.
        if payload.get("channel") == "pdf":
            actions.append(
                _bridge_confirm_action(
                    pending.path, payload_ref="confirmation_payload.bridge_payload"
                )
            )
            continue
        if payload.get("reason") == "account_confirmation":
            # The column layout is settled; only the account identity is open.
            # accept=True ratifies the mapping and account_bindings answers the
            # account (accept alone, with no binding, loops back to the account
            # gate; a mapping override is irrelevant). One call must carry a
            # binding for every proposal — the gate is all-or-nothing.
            # account_proposals is always a list of serialized dicts here (built
            # by confirmation_payload_dict); typed Any to read keys under strict.
            raw_props: Any = payload.get("account_proposals")
            props: list[Any] = raw_props if isinstance(raw_props, list) else []
            keys = [str(p.get("source_account_key", "")) for p in props] or [
                "<source_key>"
            ]
            binding_map = ", ".join(f"'{k}': '<account_id|new>'" for k in keys)
            actions.append(
                f"Use import_confirm(file_path='{pending.path}', accept=True, "
                f"account_bindings={{{binding_map}}}) to ratify the mapping and "
                "bind every account; source keys are in "
                "confirmation_payload.account_proposals[].source_account_key."
            )
            continue
        if tier != "low":
            actions.append(
                f"Use import_confirm(file_path='{pending.path}', accept=True) "
                "to accept the proposed mapping as-is."
            )
        actions.append(
            f"Use import_confirm(file_path='{pending.path}', "
            "mapping={'<dest_field>': '<source_column>'}) to override "
            "specific fields (required on low-tier proposals)."
        )
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

    return build_envelope(
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
        sensitivity="medium" if pending_files else "low",
        actions=actions,
    )


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
                    "sign_evidence": payload["sign_evidence"],
                    "sign_sample_rows": payload["sign_sample_rows"],
                },
                actions=_sign_confirm_actions(str(path), e.outcome.error_message),
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
            actions=[_bridge_confirm_action(str(path), payload_ref="bridge_payload")],
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


@mcp_tool(read_only=False, idempotent=False)
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
) -> ResponseEnvelope[ImportPreviewPayload]:
    """Build a tabular preview from one optional immutable byte object."""
    from moneybin.config import get_settings
    from moneybin.extractors.tabular.column_mapper import map_columns
    from moneybin.extractors.tabular.format_detector import detect_format
    from moneybin.extractors.tabular.readers import read_file

    bands = get_settings().import_.confidence
    try:
        format_info = detect_format(path, source_bytes=source_bytes)
        read_result = read_file(path, format_info, source_bytes=source_bytes)
        mapping_result = map_columns(
            read_result.df,
            t_high=bands.t_high,
            t_med=bands.t_med,
            structural_red_flag=read_result.header_row_looks_like_data,
        )
    except ValueError as e:
        raise UserError(str(e), code="preview_error") from e

    return build_envelope(
        data=ImportPreviewPayload(
            file=path.name,
            format=ImportFormatInfoPayload(
                file_type=format_info.file_type,
                delimiter=format_info.delimiter,
                encoding=format_info.encoding,
                file_size_bytes=format_info.file_size,
            ),
            mapping=mapping_result.field_mapping,
            confidence=mapping_result.confidence,
            date_format=mapping_result.date_format,
            number_format=mapping_result.number_format,
            sign_convention=mapping_result.sign_convention,
            is_multi_account=mapping_result.is_multi_account,
            unmapped_columns=mapping_result.unmapped_columns,
            flagged_fields=mapping_result.flagged_fields,
            sample_values=mapping_result.sample_values,
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
            "Use import_formats for available named formats",
        ],
    )


def _file_identity(path: Path) -> tuple[str, int]:
    """Return the immutable content identity bound into an import preview."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest(), path.stat().st_size


def _bytes_identity(source_bytes: bytes) -> tuple[str, int]:
    """Return the exact identity of an already materialized source object."""
    return hashlib.sha256(source_bytes).hexdigest(), len(source_bytes)


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


@mcp_tool(read_only=False, idempotent=False, dynamic_classification=True)
def import_preview_coarse(
    file_path: str,
) -> ResponseEnvelope[ImportPreviewCoarsePayload]:
    """Persist a complete staged-import preview for one exact file snapshot."""
    from moneybin.config import get_settings
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo

    path = _validate_file_path(file_path)
    if path.suffix.lower() in {".ofx", ".qfx", ".qbo"}:
        raise UserError(
            "OFX/QFX/QBO files import directly with import_files; their "
            "structured financial format has no column-mapping preview.",
            code="IMPORT_PREVIEW_DIRECT_IMPORT_REQUIRED",
        )
    source_bytes = path.read_bytes()
    response: ResponseEnvelope[ImportPreviewPayload] | ResponseEnvelope[dict[str, Any]]
    if path.suffix.lower() == ".pdf":
        response = _import_preview_pdf(path, source_bytes=source_bytes)
    else:
        response = _import_preview_tabular(path, source_bytes=source_bytes)
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
            date_format=data.date_format or "%Y-%m-%d",
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
    if isinstance(response.data, ImportPreviewPayload):
        payload: ImportPreviewCoarsePayload = ImportTabularPreviewCoarsePayload(
            **data,
            preview_id=pending_id,
            expires_at=expires_wire,
        )
        actions = [
            "Use import_confirm(preview_id=...) before the preview expires.",
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
        actions = list(response.actions)
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
        actions = [
            f"Use import_confirm(preview_id='{preview_id}') before the preview "
            "expires.",
        ]
    elif isinstance(final_payload, ImportPdfBridgePreviewPayload):
        actions = [
            f"Use import_confirm(preview_id='{preview_id}', "
            "bridge_response={...}) before the preview expires.",
        ]
    return cast(
        ResponseEnvelope[ImportPreviewCoarsePayload],
        _import_dynamic_envelope(final_payload, actions=actions),
    )


@mcp_tool()
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


@mcp_tool(read_only=False, destructive=True, idempotent=False)
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
            code=f"revert_{status}",
        )
    )


@mcp_tool()
def import_formats() -> ResponseEnvelope[ImportFormatsPayload]:
    """List all available import formats (tabular + PDF, built-in and user-saved).

    The ``formats`` list holds tabular formats (CSV/Excel/etc.) with column
    mappings, sign convention, and header signature. The ``pdf_formats`` list
    (Phase 2a) holds auto-derived PDF recipes keyed by layout fingerprint:
    institution, document kind, routing target, and replay statistics. Use
    ``import_preview`` to test a tabular format against a specific file.
    """
    from moneybin.extractors.tabular.formats import (
        load_builtin_formats,
        load_formats_from_db,
        merge_formats,
    )
    from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo

    builtin = load_builtin_formats()
    pdf_format_rows: list[ImportPdfFormatRow] = []
    try:
        with get_database(read_only=True) as db:
            formats = merge_formats(builtin, load_formats_from_db(db))
            # Independent try/except: app.pdf_formats may be absent on
            # pre-V027 DBs. A failure here must not clobber the tabular
            # formats already merged above.
            try:
                for pf in PdfFormatsRepo(db).list_all():
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
            except Exception:  # noqa: BLE001 -- pre-V027 DB; fall back to empty
                pdf_format_rows = []
    except Exception:  # noqa: BLE001 -- DB may not exist; fall back to built-in only
        formats = builtin

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


def _import_status_cursor(
    offset: int,
    *,
    sections: list[Literal["imports", "formats", "inbox"]],
    snapshot_digest: str,
    snapshot_head_import_id: str,
    snapshot_head_started_at: str,
    snapshot_total: int,
) -> str:
    """Encode an import cursor bound to its exact selected sections."""
    raw = json.dumps(
        {
            "filters": {"import_id": None, "sections": sections},
            "offset": offset,
            "snapshot": {
                "digest": snapshot_digest,
                "head": [snapshot_head_started_at, snapshot_head_import_id],
                "total": snapshot_total,
            },
            "tool": "import_status",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(raw).decode()


def _import_status_offset(
    cursor: str | None,
    *,
    sections: list[Literal["imports", "formats", "inbox"]],
) -> tuple[int, int | None, str | None, str | None, str | None]:
    """Decode an import cursor and reject malformed or cross-section reuse."""
    if cursor is None:
        return 0, None, None, None, None
    try:
        decoded = base64.b64decode(cursor.encode(), altchars=b"-_", validate=True)
        value = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UserError(
            "Invalid import pagination cursor.",
            code="IMPORT_CURSOR_INVALID",
        ) from exc
    if not isinstance(value, dict):
        raise UserError(
            "Invalid import pagination cursor.",
            code="IMPORT_CURSOR_INVALID",
        )
    payload = cast(dict[str, Any], value)
    offset = payload.get("offset")
    snapshot = payload.get("snapshot")
    if not isinstance(snapshot, dict):
        raise UserError(
            "Invalid import pagination cursor.",
            code="IMPORT_CURSOR_INVALID",
        )
    snapshot_payload = cast(dict[str, Any], snapshot)
    snapshot_digest = snapshot_payload.get("digest")
    snapshot_head = snapshot_payload.get("head")
    snapshot_head_items = (
        cast(list[Any], snapshot_head) if isinstance(snapshot_head, list) else None
    )
    snapshot_total = snapshot_payload.get("total")
    if (
        set(payload) != {"filters", "offset", "snapshot", "tool"}
        or payload.get("filters") != {"import_id": None, "sections": sections}
        or payload.get("tool") != "import_status"
        or set(snapshot_payload) != {"digest", "head", "total"}
        or isinstance(offset, bool)
        or not isinstance(offset, int)
        or offset < 0
        or not isinstance(snapshot_digest, str)
        or len(snapshot_digest) != 43
        or snapshot_head_items is None
        or len(snapshot_head_items) != 2
        or not all(isinstance(value, str) for value in snapshot_head_items)
        or isinstance(snapshot_total, bool)
        or not isinstance(snapshot_total, int)
        or snapshot_total < 0
    ):
        raise UserError(
            "Invalid import pagination cursor.",
            code="IMPORT_CURSOR_INVALID",
        )
    head_started_at, head_import_id = cast(list[str], snapshot_head_items)
    try:
        datetime.fromisoformat(head_started_at)
        base64.b64decode(
            f"{snapshot_digest}=".encode(),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, binascii.Error) as exc:
        raise UserError(
            "Invalid import pagination cursor.",
            code="IMPORT_CURSOR_INVALID",
        ) from exc
    return (
        offset,
        snapshot_total,
        snapshot_digest,
        head_started_at,
        head_import_id,
    )


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


@mcp_tool(dynamic_classification=True)
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
            code="IMPORT_SECTIONS_REQUIRED",
        )
    if len(set(supplied)) != len(supplied):
        raise UserError(
            "Import status sections must not contain duplicates.",
            code="IMPORT_SECTIONS_DUPLICATE",
        )
    requested: list[Literal["imports", "formats", "inbox"]] = [
        section for section in _IMPORT_STATUS_SECTION_ORDER if section in supplied
    ]
    if import_id is not None and requested != ["imports"]:
        raise UserError(
            "import_id is valid only when imports is the sole section.",
            code="IMPORT_ID_NOT_ALLOWED",
        )
    if import_id is not None and (limit != 100 or cursor is not None):
        raise UserError(
            "A single import lookup does not accept pagination overrides.",
            code="IMPORT_PAGINATION_NOT_ALLOWED",
        )
    if "imports" not in requested and (limit != 100 or cursor is not None):
        raise UserError(
            "Pagination is valid only when imports is selected.",
            code="IMPORT_PAGINATION_NOT_ALLOWED",
        )

    (
        offset,
        snapshot_total,
        snapshot_digest,
        snapshot_head_started_at,
        snapshot_head_import_id,
    ) = (
        _import_status_offset(cursor, sections=requested)
        if import_id is None and "imports" in requested
        else (0, None, None, None, None)
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
                        offset=offset,
                        head_started_at=snapshot_head_started_at,
                        head_import_id=snapshot_head_import_id,
                    )
                    records = page.records
                    count = page.total_count
                if snapshot_total is not None and (
                    page is None
                    or page.total_count != snapshot_total
                    or page.snapshot_digest != snapshot_digest
                ):
                    raise UserError(
                        "Invalid import pagination cursor.",
                        code="IMPORT_CURSOR_INVALID",
                    )
            selected.append(ImportStatusImportsSection(records=records))
            contract_types.append(ImportStatusImportsSection)
            total_count += count
            returned_count += len(records)
            if import_id is None and offset + len(records) < count:
                if (
                    page is None
                    or page.head_started_at is None
                    or page.head_import_id is None
                ):
                    raise RuntimeError("non-empty import snapshot is missing its head")
                next_cursor = _import_status_cursor(
                    offset + len(records),
                    sections=requested,
                    snapshot_digest=page.snapshot_digest,
                    snapshot_head_import_id=page.head_import_id,
                    snapshot_head_started_at=page.head_started_at,
                    snapshot_total=count,
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

    if next_cursor is not None:
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
        total_count=total_count,
        returned_count=returned_count,
        next_cursor=next_cursor,
        actions=list(dict.fromkeys(actions)),
    )


def _import_confirm_bridge(
    file_path: str,
    bridge_response: dict[str, Any],
    *,
    save_format: bool,
    account_id: str | None,
) -> ResponseEnvelope[ImportConfirmPayload]:
    """Apply a PDF bridge response via ImportService.apply_pdf_bridge_response.

    Returns an ``applied`` envelope (with import_id + divergence report) when
    the re-executed rows reconcile, or an ``invalid`` envelope (nothing loaded,
    carrying the reject reason) when they don't. A malformed response or a
    recipe that fails the security bounds raises ``UserError``.
    """
    from moneybin.extractors.pdf.bridge import BridgeResponseError
    from moneybin.services.import_service import ImportService

    path = _validate_file_path(file_path)
    try:
        with get_database(read_only=False) as db:
            result = ImportService(db).apply_pdf_bridge_response(
                path,
                bridge_response,
                save_format=save_format,
                account_id=account_id,
            )
    except BridgeResponseError as e:
        # Only a bad response shape / out-of-bounds recipe is bridge_response_
        # invalid. A ValueError raised later (PDF extraction, load) is NOT
        # caught here so it isn't mislabeled — it surfaces as a generic error.
        raise UserError(str(e), code="bridge_response_invalid") from e

    if result.outcome == "invalid":
        return build_envelope(
            sensitivity="medium",
            data={
                "status": "invalid",
                "reject_reason": result.reject_reason,
                "expected_row_count": result.expected_row_count,
                "actual_row_count": result.actual_row_count,
                "rows_diverged": result.rows_diverged,
            },
            actions=[
                "The recipe's re-executed rows did not reconcile against the "
                "statement balances — nothing was loaded. Re-inspect the "
                "document via import_preview and propose a corrected recipe "
                "(check row-region anchors, sign convention, and that no "
                "summary/subtotal line is captured as a transaction).",
            ],
        )

    actions = [
        f"Use import_revert(import_id='{result.import_id}') to undo this import.",
        "Use refresh_run() to rebuild derived tables and apply categorization.",
        "Use system_status to confirm refreshed counts.",
    ]
    if result.rows_diverged:
        actions.insert(
            0,
            f"Note: you returned {result.expected_row_count} rows but the "
            f"recipe reproduced {result.actual_row_count} when re-run against "
            f"the document; the {result.actual_row_count} reconciled rows were "
            f"loaded. Inspect the recipe if the difference is unexpected.",
        )

    # Complete the pending-file lifecycle for a bridge-confirmed inbox PDF the
    # same way the tabular confirm path does — otherwise a PDF that escalated
    # through the inbox lingers in pending/ after a successful load. Only on
    # the success path: an "invalid" outcome (handled above) loaded nothing, so
    # the file must stay in pending/ for another attempt. No-op for a PDF
    # passed directly to import_files (it never entered the inbox buckets).
    from moneybin.services.inbox_service import (
        InboxService,  # noqa: PLC0415 — defer import
    )

    InboxService.for_active_profile_no_db().archive_confirmed_file(path)
    return build_envelope(
        sensitivity="medium",
        data={
            "status": "applied",
            "import_id": result.import_id,
            "rows_loaded": result.rows_loaded,
            "format_name": result.format_name,
            "expected_row_count": result.expected_row_count,
            "actual_row_count": result.actual_row_count,
            "rows_diverged": result.rows_diverged,
        },
        actions=actions,
    )


@mcp_tool(read_only=False, idempotent=False)
def import_confirm(
    file_path: str,
    *,
    accept: bool = False,
    mapping: dict[str, str] | None = None,
    bridge_response: dict[str, Any] | None = None,
    save_format: bool = True,
    account_id: str | None = None,
    account_name: str | None = None,
    account_bindings: dict[str, str] | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
) -> ResponseEnvelope[ImportConfirmPayload]:
    """Confirm or override a proposed mapping, or apply a PDF bridge response.

    Terminal ``_confirm`` step of the propose -> review -> confirm workflow.
    Two channels:

    - **Tabular** — ``import_files`` returned ``confirmation_required`` for an
      unknown column layout; ratify with ``accept=True`` or a partial
      ``mapping=`` override. ``mapping`` is partial-merge: supply only the
      destination fields being corrected; the rest fall back to the detected
      proposal. Unrecognized destination fields or absent source columns raise.
    - **PDF bridge** — ``import_files``/``import_preview`` returned a
      ``confirmation_required`` whose ``confirmation_payload.bridge_payload``
      asked you to extract a native-text PDF. Pass ``bridge_response={'recipe':
      <recipe>, 'rows': [...]}``. MoneyBin re-runs your recipe against the
      document, reconciles the re-executed rows against the statement balances
      (the authority — your returned rows are verified against it, and a row-
      count divergence is reported back), persists the recipe, and loads the
      transactions. A response that fails reconciliation is rejected
      (``status='invalid'``) and nothing loads.

    Single-account tabular files (CSVs without an embedded account identifier)
    require ``account_id`` or ``account_name``. PDF rows resolve the account
    from the statement; pass ``account_id`` only to pin rows to an existing
    account when the statement carries no account anchor.

    Mutation surface: writes to ``raw.tabular_transactions`` (data load),
    ``app.tabular_formats`` / ``app.pdf_formats`` when ``save_format=True``, and
    ``app.account_settings`` when ``account_metadata`` captures fields for a
    newly-minted account. Data load is reversible via ``import_revert`` with the
    returned ``import_id``; format save and the settings write can be undone via
    ``system_audit_undo``.

    Amounts use the accounting convention: negative = expense, positive =
    income; transfers exempt.

    Args:
        file_path: Absolute path to the file to import. Must be within the
            user's home directory.
        accept: Accept the proposed mapping as-is (no overrides). Tabular only.
        mapping: Partial field→column override dict. Tabular only.
        bridge_response: PDF bridge reply ``{'recipe': ..., 'rows': [...]}``.
            Mutually exclusive with ``accept``/``mapping``.
        save_format: Auto-save the confirmed mapping/recipe as a named format
            for future imports. Defaults to True.
        account_id: Existing account id to associate single-account rows with.
        account_name: Existing account name to look up; resolves to account_id.
        account_bindings: Ratify an ``account_confirmation``: a map of
            ``source_account_key`` -> existing ``account_id`` (adopt) or
            ``"new"`` (mint a distinct new account). The keys come from the
            ``confirmation_payload.account_proposals[].source_account_key`` of a
            prior ``confirmation_required`` response. Use this for multi-account
            files; ``account_id``/``account_name`` cover the single-account case.
            On retry, re-supply ALL bindings — the gate re-evaluates every
            account and persists no partial state between calls.
        account_metadata: For accounts bound ``"new"``, a map of
            ``source_account_key`` -> ``{display_name, account_subtype,
            last_four, iso_currency_code}`` captured into the minted account's
            settings. Unknown fields raise. Ignored for adopted accounts.
    """
    from moneybin.services.import_confirmation import (
        ImportConfirmationRequiredError,
        ProposedMapping,
    )
    from moneybin.services.import_service import ImportService

    # Validate the path up front so an invalid path surfaces as
    # invalid_file_path before any channel/argument guard below (otherwise a
    # bad path combined with e.g. account_name would mask the path error).
    path = _validate_file_path(file_path)

    # PDF bridge channel — apply the agent's recipe + rows. Mutually exclusive
    # with the tabular accept/mapping signals.
    if bridge_response is not None:
        if accept or mapping:
            raise UserError(
                "bridge_response cannot be combined with accept= or mapping= "
                "(those are the tabular column-mapping channel).",
                code="confirm_channel_conflict",
            )
        if account_name is not None:
            # PDF rows resolve their account from the statement; account_name is
            # a tabular-only signal. Reject it explicitly so it isn't silently
            # dropped — pin a no-anchor PDF with account_id instead.
            raise UserError(
                "account_name is not supported with bridge_response — PDF rows "
                "resolve the account from the statement; pass account_id to pin "
                "rows to an existing account when there is no anchor.",
                code="bridge_account_name_unsupported",
            )
        return _import_confirm_bridge(
            str(path),
            bridge_response,
            save_format=save_format,
            account_id=account_id,
        )

    if path.suffix.lower() == ".pdf":
        # A PDF reached the tabular confirm channel (bridge_response is None here
        # but accept=/mapping= may be set). accept=/mapping= never ratify a PDF —
        # two kinds of PDF confirmation land here, each with a different recovery,
        # and both are surfaced honestly rather than routing-to-detect (which would
        # re-extract the document and, for a bridge PDF, write a spurious egress
        # audit row):
        #   * Bridge (native-text extraction) — re-call with bridge_response=.
        #   * Sign convention (credit-card inversion) — MCP cannot ratify a sign
        #     flip in place yet (elicitation-based confirm is planned); the human
        #     resolves it in a terminal, where inverting every amount is a
        #     deliberate, visible act. accept=True must never silently invert the
        #     ledger, and the tabular catch below only serializes ProposedMapping,
        #     so running the tabular path here would loop the agent instead.
        quoted = shlex.quote(str(path))
        raise UserError(
            "A PDF confirmation cannot be ratified with accept=/mapping= over MCP. "
            "If import_files/import_preview returned a bridge_payload (native-text "
            "extraction), call import_confirm(file_path=..., bridge_response="
            "{'recipe': ..., 'rows': [...]}). If it returned a sign-convention "
            "confirmation (a credit-card statement — confirming inverts every "
            "amount's sign), MCP cannot ratify the inversion in place yet; resolve "
            f"it in a terminal: `moneybin import files {quoted} --confirm` if it IS "
            f"a credit card, or `moneybin import files {quoted} --sign "
            "negative_is_expense` if it is not.",
            code="confirm_channel_conflict",
        )

    if not accept and not mapping:
        raise UserError(
            "import_confirm requires accept=True to ratify the proposed mapping, "
            "or mapping={'<dest_field>': '<source_column>'} to override specific fields.",
            code="confirm_requires_signal",
        )

    try:
        with get_database(read_only=False) as db:
            result = ImportService(db).import_file(
                path,
                confirm=accept,
                overrides=mapping,
                save_format=save_format,
                account_id=account_id,
                account_name=account_name,
                account_bindings=account_bindings,
                account_metadata=account_metadata,
                actor_kind="agent",
                refresh=False,  # caller can run refresh_run separately
            )
    except ImportConfirmationRequiredError as e:
        # An override that names an unknown source column, or an Accept against
        # a low-tier proposal where required fields remain missing, re-surfaces
        # ConfirmationRequired. Mirror import_files' envelope so the agent
        # sees the validator's error_message and actions[] instead of an
        # opaque server error.
        proposed_mapping = (
            e.outcome.proposed.field_mapping
            if isinstance(e.outcome.proposed, ProposedMapping)
            else {}
        )
        unmapped = (
            list(e.outcome.proposed.unmapped_columns)
            if isinstance(e.outcome.proposed, ProposedMapping)
            else []
        )
        return build_envelope(
            sensitivity="medium",
            data={
                "status": "confirmation_required",
                "channel": e.outcome.channel,
                "tier": e.outcome.confidence.tier,
                "score": e.outcome.confidence.score,
                "reason": e.outcome.reason,
                "error_message": e.outcome.error_message,
                "proposed_mapping": proposed_mapping,
                "samples": e.outcome.samples,
                "flagged": list(e.outcome.confidence.flagged),
                "missing_required": list(e.outcome.confidence.missing_required),
                "unmapped_columns": unmapped,
                # Carry account_proposals so an account_confirmation re-prompt
                # surfaces the source keys the agent needs for account_bindings.
                "account_proposals": list(e.outcome.account_proposals),
            },
            actions=_confirmation_actions(str(path), e.outcome),
        )

    actions: list[str] = [
        f"Use import_revert(import_id='{result.import_id}') to undo this import.",
        "Use refresh_run() to rebuild derived tables and apply categorization.",
        "Use system_status to confirm refreshed counts.",
    ]
    if result.sign_correction_suggested:
        actions.insert(
            0,
            "Sign convention may be inverted — inspect amounts and re-import with "
            "mapping={'amount': '<column>'} corrected if needed.",
        )

    # Authoritative mapping comes from ImportService — what actually loaded.
    # sample_values are populated best-effort by re-reading the file so the
    # agent sees the same per-column previews import_preview would emit;
    # failure to re-read does not affect the load and is logged at debug.
    merged_mapping: dict[str, str] = dict(result.field_mapping or {})
    sample_values: dict[str, list[str]] = {}
    try:
        from moneybin.config import get_settings
        from moneybin.extractors.tabular.column_mapper import map_columns
        from moneybin.extractors.tabular.format_detector import detect_format
        from moneybin.extractors.tabular.readers import read_file

        bands = get_settings().import_.confidence
        format_info = detect_format(path)
        read_result = read_file(path, format_info)
        # Drive the re-detection with the AUTHORITATIVE merged mapping
        # (what actually loaded) rather than the caller's raw partial
        # override — otherwise the re-detection picks columns by detector
        # heuristics for any field the override didn't name, and
        # sample_values could point at different source columns than
        # merged_mapping for those fields. Agents comparing the two would
        # see a misleading mismatch.
        mapping_result = map_columns(
            read_result.df,
            overrides=merged_mapping,
            t_high=bands.t_high,
            t_med=bands.t_med,
        )
        sample_values = {k: list(v) for k, v in mapping_result.sample_values.items()}
    except Exception:  # noqa: BLE001,S110 — samples are informational; load already succeeded
        logger.debug(
            "Could not build sample_values for import_confirm response",
            exc_info=True,
        )

    # Complete the pending-file lifecycle: a file confirmed out of the inbox's
    # pending/ bucket moves to processed/ and its .pending.yml sidecar is
    # dropped (no-op for a path that never entered the inbox). Done after the
    # sample_values re-read above, which still reads the file at `path`.
    from moneybin.services.inbox_service import (
        InboxService,  # noqa: PLC0415 — defer import
    )

    InboxService.for_active_profile_no_db().archive_confirmed_file(path)

    return build_envelope(
        sensitivity="medium",
        data=ImportConfirmPayload(
            import_id=result.import_id,
            rows_loaded=result.transactions,
            merged_mapping=merged_mapping,
            sample_values=sample_values,
            sign_correction_suggested=result.sign_correction_suggested,
        ),
        actions=actions,
    )


@mcp_tool(read_only=False, idempotent=False, dynamic_classification=True)
def import_confirm_coarse(
    preview_id: str,
    *,
    bridge_response: dict[str, Any] | None = None,
    save_format: bool = True,
    account_id: str | None = None,
    account_name: str | None = None,
    account_bindings: dict[str, str] | None = None,
    account_metadata: dict[str, dict[str, str]] | None = None,
) -> ResponseEnvelope[ImportConfirmCoarsePayload]:
    """Atomically consume one unchanged tabular or PDF-bridge preview."""
    from moneybin.metrics.observations import MetricObservations
    from moneybin.metrics.registry import IMPORT_DURATION_SECONDS, IMPORT_RECORDS_TOTAL
    from moneybin.repositories.import_previews_repo import ImportPreviewsRepo
    from moneybin.services.import_service import ImportService

    started = time.monotonic()
    observations = MetricObservations()
    bridge_result: Any | None = None
    result: Any | None = None
    with get_database(read_only=False) as db:
        repo = ImportPreviewsRepo(db)
        preview = repo.get(preview_id)
        if preview is None:
            raise UserError(
                "Import preview was not found.",
                code="IMPORT_PREVIEW_NOT_FOUND",
            )
        path = _validate_file_path(preview["file_path"])
        source_bytes = repo.get_source_bytes(preview_id)
        if source_bytes is None:
            raise UserError(
                "The immutable import preview snapshot is unavailable.",
                code="IMPORT_PREVIEW_SNAPSHOT_MISSING",
            )
        snapshot_data = cast(
            dict[str, Any],
            preview["snapshot_json"]["data"],
        )
        channel = preview["channel"]
        if channel == "ofx":
            raise UserError(
                "OFX/QFX/QBO files import directly with import_files.",
                code="IMPORT_PREVIEW_DIRECT_IMPORT_REQUIRED",
            )
        if channel == "pdf":
            if snapshot_data.get("reason") == "sign_convention":
                raise UserError(
                    "Credit-card PDF sign confirmation must be completed in the "
                    "CLI so the human explicitly approves the whole-statement "
                    "sign inversion.",
                    code="IMPORT_PREVIEW_SIGN_CONFIRMATION_CLI_REQUIRED",
                )
            if (
                snapshot_data.get("status") != "confirmation_required"
                or "bridge_payload" not in snapshot_data
            ):
                raise UserError(
                    "This PDF preview does not require a bridge response; use "
                    "import_files to run its deterministic or seed path.",
                    code="IMPORT_PREVIEW_DIRECT_IMPORT_REQUIRED",
                )
            if bridge_response is None:
                raise UserError(
                    "A PDF bridge preview requires bridge_response={recipe, rows}.",
                    code="IMPORT_PREVIEW_BRIDGE_RESPONSE_REQUIRED",
                )
        elif bridge_response is not None:
            raise UserError(
                "bridge_response is valid only for a PDF bridge preview.",
                code="IMPORT_PREVIEW_CHANNEL_CONFLICT",
            )
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
            if channel == "pdf":
                bridge_result = service.apply_pdf_bridge_response(
                    path,
                    cast(dict[str, Any], bridge_response),
                    save_format=save_format,
                    account_id=account_id,
                    source_bytes=source_bytes,
                    in_outer_txn=True,
                    emit_metrics=False,
                    observations=observations,
                )
                if bridge_result.outcome == "invalid":
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
                                "Revise the bridge recipe or extracted rows and "
                                "retry this same preview before it expires.",
                            ],
                        ),
                    )
                import_id = bridge_result.import_id
            else:
                from moneybin.services.import_service import ReviewedTabularPlan

                plan_value = consumed["snapshot_json"].get("plan")
                if not isinstance(plan_value, dict):
                    raise UserError(
                        "The persisted preview lacks its normalized import plan.",
                        code="IMPORT_PREVIEW_PLAN_MISSING",
                    )
                reviewed_plan = ReviewedTabularPlan.from_dict(
                    cast(dict[str, Any], plan_value)
                )
                result = service.import_file(
                    path,
                    source_bytes=source_bytes,
                    reviewed_plan=reviewed_plan,
                    save_format=save_format,
                    account_id=account_id,
                    account_name=account_name,
                    account_bindings=account_bindings,
                    account_metadata=account_metadata,
                    actor_kind="agent",
                    refresh=False,
                    in_outer_txn=True,
                    emit_metrics=False,
                    observations=observations,
                )
                import_id = result.import_id
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

    source_type = "pdf" if channel == "pdf" else "tabular"
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
    from moneybin.services.inbox_service import InboxService

    InboxService.for_active_profile_no_db().archive_confirmed_file(path)
    if bridge_result is not None:
        payload: Any = ImportPdfBridgeAppliedPayload(
            preview_id=preview_id,
            import_id=import_id,
            rows_loaded=rows_loaded,
            merged_mapping=merged_mapping,
            format_name=bridge_result.format_name,
        )
    else:
        payload = ImportTabularConfirmCoarsePayload(
            preview_id=preview_id,
            import_id=import_id,
            rows_loaded=rows_loaded,
            merged_mapping=merged_mapping,
        )
    return cast(
        ResponseEnvelope[ImportConfirmCoarsePayload],
        _import_dynamic_envelope(
            payload,
            actions=[
                f"Use import_revert(import_id='{import_id}') to undo this import.",
                "Use import_status(sections=['imports'], "
                f"import_id='{import_id}') to verify it.",
            ],
        ),
    )


def register_import_coarse_reads(mcp: FastMCP) -> None:
    """Register the dormant Plan 6 replacement import status read."""
    register(
        mcp,
        import_status_coarse,
        "import_status",
        "Read selected import history, available format, and pending inbox "
        "sections. Import history has exact cursor pagination; import_id is "
        "valid only when imports is the sole section.",
        privacy_actor="import_status",
    )
    # Plan 6 cutover removals: import_formats and import_inbox_pending. Their
    # live registrations remain untouched until the atomic registry swap.


@mcp_tool(read_only=False, idempotent=False)
def import_files_coarse(
    paths: list[str],
    refresh: bool = True,
    force: bool = False,
) -> ResponseEnvelope[ImportFilesPayload]:
    """Import files while keeping actions inside the staged-import cohort."""
    response = import_files.__wrapped__(  # type: ignore[attr-defined]
        paths=paths,
        refresh=refresh,
        force=force,
    )
    actions: list[str] = []
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


@mcp_tool(read_only=False, destructive=True, idempotent=False)
def import_revert_coarse(
    import_id: str,
) -> ResponseEnvelope[ImportRevertPayload]:
    """Revert one import with an isolated status recovery action."""
    response = import_revert.__wrapped__(import_id=import_id)  # type: ignore[attr-defined]
    return replace(
        response,
        actions=[
            "Use import_status(sections=['imports'], "
            f"import_id='{import_id}') to verify the reverted state."
        ],
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
    """Register the dormant seven-boundary staged-import workflow."""
    for callback, name, description in (
        (import_files_coarse, "import_files", "Import one or more files."),
        (
            import_preview_coarse,
            "import_preview",
            "Persist an exact, expiring staged-import preview.",
        ),
        (
            import_confirm_coarse,
            "import_confirm",
            "Atomically consume an unchanged preview and import its file.",
        ),
        (
            import_status_coarse,
            "import_status",
            "Read import history, formats, and inbox state.",
        ),
        (import_revert_coarse, "import_revert", "Revert one completed import."),
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
        )


def register_import_tools(mcp: FastMCP) -> None:
    """Register all import namespace tools with the FastMCP server."""
    register(
        mcp,
        import_files,
        "import_files",
        "Import one or more financial data files (OFX, QFX, QBO, CSV, TSV, "
        "Excel, Parquet, PDF) into MoneyBin. Per-file failures do not abort "
        "the batch; transforms run once at end-of-batch unless deferred. "
        "Writes raw.* source tables and raw.import_log; revert each import "
        "via import_revert with the returned import_id. "
        "Amounts use the accounting convention: negative=expense, "
        "positive=income; transfers exempt.",
    )
    register(
        mcp,
        import_preview,
        "import_preview",
        "Preview a file's structure without importing. Tabular files: detected "
        "format, column mapping, and sample values. PDF files: the deterministic "
        "extraction outcome, or — for a bridge-eligible layout (low confidence, "
        "failed reconciliation, …) — a confirmation_required envelope carrying "
        "the bridge_payload (document text + table preview) for you to propose a "
        "recipe + rows and ratify via import_confirm(bridge_response=...). The "
        "PDF bridge branch writes an app.audit_log egress row (not "
        "side-effect-free) and can return row-level document content (medium "
        "sensitivity).",
    )
    register(
        mcp,
        import_status,
        "import_status",
        "List past import batches with status, row counts, and detection confidence.",
    )
    register(
        mcp,
        import_revert,
        "import_revert",
        "Undo an import batch by import_id (deletes the rows it produced and "
        "marks the batch as reverted). "
        "Hard-deletes from raw.* source tables and updates raw.import_log.status='reverted'; the deletion is permanent — re-import the original file via import_files to restore the rows.",
    )
    register(
        mcp,
        import_formats,
        "import_formats",
        "List all available tabular import formats (built-in and user-saved).",
    )
    register(
        mcp,
        import_confirm,
        "import_confirm",
        "Confirm or override a proposed column mapping, or apply a PDF bridge "
        "response. Terminal step of the propose->review->confirm workflow: call "
        "after import_files / import_preview returns confirmation_required. "
        "Tabular: pass accept=True to ratify the proposal as-is, or "
        "mapping={'<dest_field>': '<source_column>'} for a partial override "
        "(unspecified fields fall back to the detected proposal); writes "
        "raw.tabular_transactions + app.tabular_formats (when save_format=True). "
        "PDF bridge: pass bridge_response={'recipe': ..., 'rows': [...]} — "
        "MoneyBin re-runs your recipe against the document, reconciles, and "
        "loads, writing raw.tabular_transactions + app.pdf_formats (when "
        "save_format=True); a response that fails reconciliation is rejected and "
        "nothing loads. Data load is reversible via import_revert; format save "
        "can be undone via system_audit_undo. "
        "Amounts use the accounting convention: negative=expense, "
        "positive=income; transfers exempt.",
    )

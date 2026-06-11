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

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastmcp import FastMCP

if TYPE_CHECKING:
    from moneybin.services.import_confirmation import ConfirmationRequired

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.imports import (
    ImportConfirmPayload,
    ImportFilesPayload,
    ImportFormatInfoPayload,
    ImportFormatRow,
    ImportFormatsPayload,
    ImportPdfFormatRow,
    ImportPerFileRow,
    ImportPreviewPayload,
    ImportRevertPayload,
    ImportStatusPayload,
)
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
)

logger = logging.getLogger(__name__)


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


def _import_preview_pdf(path: Path) -> ResponseEnvelope[ImportPreviewPayload]:
    """Preview a PDF via ImportService.pdf_preview (deterministic or bridge).

    Returns a deterministic preview dict, or — when the layout is bridge-
    eligible — a ``confirmation_required`` envelope carrying the bridge payload
    (and writing the Req 14 egress audit row, so a writable DB is required).
    """
    from moneybin.services.import_confirmation import (
        BridgePayload,
        ImportConfirmationRequiredError,
    )
    from moneybin.services.import_service import ImportService

    try:
        with get_database(read_only=False) as db:
            preview = ImportService(db).pdf_preview(path)
    except ImportConfirmationRequiredError as e:
        # pdf_preview only escalates via _raise_pdf_bridge_escalation, which
        # always constructs a BridgePayload — so proposed is never the tabular
        # ProposedMapping here. Fail loudly on a contract break rather than carry
        # a dead `else None` that would emit bridge_payload=null while actions[]
        # still tells the agent to "Read bridge_payload".
        proposed = e.outcome.proposed
        if not isinstance(proposed, BridgePayload):
            raise RuntimeError(
                "pdf_preview escalation must carry a BridgePayload, "
                f"got {type(proposed).__name__}"
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
    confidence.

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
        return _import_preview_pdf(validated)

    from moneybin.config import get_settings
    from moneybin.extractors.tabular.column_mapper import map_columns
    from moneybin.extractors.tabular.format_detector import detect_format
    from moneybin.extractors.tabular.readers import read_file

    bands = get_settings().import_.confidence
    try:
        format_info = detect_format(validated)
        read_result = read_file(validated, format_info)
        mapping_result = map_columns(
            read_result.df, t_high=bands.t_high, t_med=bands.t_med
        )
    except ValueError as e:
        raise UserError(str(e), code="preview_error") from e

    return build_envelope(
        data=ImportPreviewPayload(
            file=validated.name,
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
        ),
        # Consistent with the PDF branches; the @mcp_tool decorator also stamps
        # medium from ImportPreviewPayload (sample_values is row-level content).
        sensitivity="medium",
        actions=[
            "Use import_files to import after reviewing the preview",
            "Use import_formats for available named formats",
        ],
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

    Mutation surface: writes to ``raw.tabular_transactions`` (data load) and
    ``app.tabular_formats`` / ``app.pdf_formats`` when ``save_format=True``.
    Data load is reversible via ``import_revert`` with the returned
    ``import_id``; format save can be undone via ``system_audit_undo``.

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
        # but accept=/mapping= may be set). There is no valid tabular confirm for
        # a PDF: re-importing with actor_kind="agent" would re-raise the bridge
        # escalation, but this tool's catch below only serializes ProposedMapping
        # — the agent would get accept/mapping actions again and loop. Direct it
        # to the bridge channel instead of running the tabular path.
        raise UserError(
            "PDF confirmations use the bridge channel, not accept=/mapping=. "
            "Read the confirmation_required bridge_payload and call "
            "import_confirm(file_path=..., bridge_response={'recipe': ..., "
            "'rows': [...]}).",
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
        "Preview a tabular file's structure and detected column "
        "mapping without importing.",
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
        "Confirm or override a proposed column mapping and load the tabular file. "
        "Terminal step of the propose->review->confirm workflow: call after "
        "import_files returns confirmation_required. Pass accept=True to ratify "
        "the proposal as-is, or mapping={'<dest_field>': '<source_column>'} for a "
        "partial override (unspecified fields fall back to the detected proposal). "
        "Writes raw.tabular_transactions (data load) and app.tabular_formats "
        "(when save_format=True). Data load is reversible via import_revert; "
        "format save can be undone via system_audit_undo. "
        "Amounts use the accounting convention: negative=expense, "
        "positive=income; transfers exempt.",
    )

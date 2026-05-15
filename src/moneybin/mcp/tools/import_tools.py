# src/moneybin/mcp/tools/import_tools.py
"""Import namespace tools — file import, preview, status, revert, format listing.

Tools:
    - import_files — Import one or more financial data files (low sensitivity)
    - import_preview — Preview a tabular file without importing (low sensitivity)
    - import_status — List past import batches (low sensitivity)
    - import_revert — Undo an import batch by import_id (low sensitivity)
    - import_list_formats — List available tabular import formats (low sensitivity)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import (
    ResponseEnvelope,
    build_envelope,
    build_error_envelope,
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


@mcp_tool(sensitivity="low", read_only=False, idempotent=False)
def import_files(
    paths: list[str],
    apply_transforms: bool = True,
    force: bool = False,
) -> ResponseEnvelope:
    """Import one or more financial data files into MoneyBin.

    Supported formats (auto-detected by extension):
      - .ofx / .qfx / .qbo -- OFX/Quicken bank statements
      - .pdf -- W-2 tax forms
      - .csv / .tsv / .xlsx / .parquet / .feather -- tabular transaction exports

    Per-file failures do not abort the batch. Transforms run once at end
    of batch by default; pass apply_transforms=False to defer.

    Args:
        paths: One or more absolute file paths to import. Each path must
            be within the user's home directory.
        apply_transforms: Run SQLMesh transforms once after the batch
            completes. Defaults to True. Pass False to import without
            refreshing core tables; the transforms_pending signal in
            system_status will indicate the pending state, and a later
            transform_apply call will catch the data up.
        force: If True, re-import files already in the import log.

    Returns:
        Envelope with data containing imported/failed/total counts,
        transforms state, and a "files" list of per-file results.
        Amounts use accounting convention: negative=expense,
        positive=income; transfers exempt. Display currency is set
        in summary.display_currency.
    """
    from moneybin.services.import_service import ImportService

    validated = [_validate_file_path(p) for p in paths]
    with get_database() as db:
        batch = ImportService(db).import_files(
            [str(p) for p in validated],
            apply_transforms=apply_transforms,
            force=force,
        )

    files = [
        {
            "path": r.path,
            "status": r.status,
            "source_type": r.source_type,
            "rows_loaded": r.rows_loaded,
            "import_id": r.import_id,
            **({"error": r.error} if r.error else {}),
        }
        for r in batch.per_file
    ]

    actions: list[str] = []
    if not batch.transforms_applied and batch.imported_count > 0:
        actions.append(
            "Run transform_apply when ready to refresh derived tables"
        )
    if batch.transforms_error:
        actions.append(
            "Transform apply failed after import — call transform_apply to retry"
        )
    actions.append("Use system_status to confirm refreshed counts")

    data: dict[str, Any] = {
        "imported_count": batch.imported_count,
        "failed_count": batch.failed_count,
        "total_count": batch.total_count,
        "transforms_applied": batch.transforms_applied,
        "transforms_duration_seconds": batch.transforms_duration_seconds,
        "files": files,
    }
    if batch.transforms_error:
        data["transforms_error"] = batch.transforms_error

    return build_envelope(
        data=data,
        sensitivity="low",
        actions=actions,
    )


@mcp_tool(sensitivity="low")
def import_preview(file_path: str) -> ResponseEnvelope:
    """Preview a tabular file's structure and detected column mapping.

    Runs the first 3 stages of the tabular pipeline (detect, read, map)
    without importing. Returns format info, column mapping, sample values,
    and confidence. Use this to understand an unknown file before importing.

    Args:
        file_path: Absolute path to the file to preview.
    """
    validated = _validate_file_path(file_path)
    from moneybin.extractors.tabular.column_mapper import map_columns
    from moneybin.extractors.tabular.format_detector import detect_format
    from moneybin.extractors.tabular.readers import read_file

    try:
        format_info = detect_format(validated)
        read_result = read_file(validated, format_info)
        mapping_result = map_columns(read_result.df)
    except ValueError as e:
        raise UserError(str(e), code="preview_error") from e

    preview = {
        "file": validated.name,
        "format": {
            "file_type": format_info.file_type,
            "delimiter": format_info.delimiter,
            "encoding": format_info.encoding,
            "file_size_bytes": format_info.file_size,
        },
        "columns": {
            "mapping": mapping_result.field_mapping,
            "confidence": mapping_result.confidence,
            "date_format": mapping_result.date_format,
            "number_format": mapping_result.number_format,
            "sign_convention": mapping_result.sign_convention,
            "is_multi_account": mapping_result.is_multi_account,
            "unmapped_columns": mapping_result.unmapped_columns,
            "flagged_fields": mapping_result.flagged_fields,
        },
        "sample_values": mapping_result.sample_values,
        "rows_read": len(read_result.df),
        "rows_skipped_trailing": read_result.rows_skipped_trailing,
    }
    return build_envelope(
        data=preview,
        sensitivity="low",
        actions=[
            "Use import_files to import after reviewing the preview",
            "Use import_list_formats for available named formats",
        ],
    )


@mcp_tool(sensitivity="low")
def import_status(
    limit: int = 20,
    import_id: str | None = None,
) -> ResponseEnvelope:
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
            db,
            limit=min(limit, 200),
            import_id=import_id,
        )
    return build_envelope(
        data=records,
        sensitivity="low",
        actions=[
            "Use import_files to import a new file",
        ],
    )


@mcp_tool(sensitivity="low", read_only=False, destructive=True, idempotent=False)
def import_revert(import_id: str) -> ResponseEnvelope:
    """Undo an import batch by deleting all rows it produced.

    Looks up source_type from raw.import_log and deletes rows tagged with
    import_id from the matching raw tables (raw.tabular_* or raw.ofx_*).
    Updates the import_log row's status to 'reverted'.

    Args:
        import_id: UUID of the import batch to revert. Get it from
            import_files's response or from import_status.
    """
    from moneybin.loaders import import_log

    with get_database() as db:
        result = import_log.revert_import(db, import_id)
    status = result.get("status")

    if status == "reverted":
        return build_envelope(
            data=result,
            sensitivity="low",
            actions=[
                "Use import_status to confirm the batch shows status='reverted'",
            ],
        )
    return build_error_envelope(
        error=UserError(
            str(result.get("reason") or f"Cannot revert (status={status})"),
            code=f"revert_{status}",
        ),
        sensitivity="low",
    )


@mcp_tool(sensitivity="low")
def import_list_formats() -> ResponseEnvelope:
    """List all available tabular import formats (built-in and user-saved).

    Returns format name, institution, sign convention, date format, and
    header signature for each format. Use ``import_preview`` to test
    a format against a specific file.
    """
    from moneybin.extractors.tabular.formats import (
        load_builtin_formats,
        load_formats_from_db,
        merge_formats,
    )

    builtin = load_builtin_formats()
    try:
        with get_database(read_only=True) as db:
            formats = merge_formats(builtin, load_formats_from_db(db))
    except Exception:  # noqa: BLE001 -- DB may not exist; fall back to built-in
        formats = builtin

    format_list = [
        {
            "name": fmt.name,
            "institution_name": fmt.institution_name,
            "file_type": fmt.file_type,
            "sign_convention": fmt.sign_convention,
            "date_format": fmt.date_format,
            "number_format": fmt.number_format,
            "multi_account": fmt.multi_account,
            "header_signature": fmt.header_signature,
        }
        for fmt in sorted(formats.values(), key=lambda f: f.name)
    ]
    return build_envelope(
        data=format_list,
        sensitivity="low",
        actions=[
            "Use import_preview to test a format against a file",
            "Use import_files to import a file once you have the format name available",
        ],
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
        "Amounts use accounting convention: negative=expense, positive=income.",
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
        import_list_formats,
        "import_list_formats",
        "List all available tabular import formats (built-in and user-saved).",
    )

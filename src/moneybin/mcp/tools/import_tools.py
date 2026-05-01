# src/moneybin/mcp/tools/import_tools.py
"""Import namespace tools — file import, preview, status, format listing.

Tools:
    - import.file — Import a financial data file (low sensitivity)
    - import.csv_preview — Preview a tabular file without importing (low sensitivity)
    - import.status — List past import batches (low sensitivity)
    - import.list_formats — List available tabular import formats (low sensitivity)
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.errors import UserError
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope

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


@mcp_tool(sensitivity="low")
def import_file(
    file_path: str,
    account_id: str | None = None,
    account_name: str | None = None,
    institution: str | None = None,
    format_name: str | None = None,
) -> ResponseEnvelope:
    """Import a financial data file into MoneyBin.

    Supported formats (detected automatically by extension):
      - .ofx / .qfx -- OFX/Quicken bank statements
      - .pdf -- W-2 tax forms
      - .csv / .tsv / .xlsx / .parquet / .feather -- tabular transaction exports

    For single-account tabular files, provide ``account_name``.
    Multi-account files (Tiller, Mint, etc.) are detected automatically.

    Args:
        file_path: Absolute path to the file to import.
        account_id: Explicit account identifier (bypasses name matching).
        account_name: Account name for single-account tabular files.
        institution: Institution name (OFX only).
        format_name: Use a specific named format (bypass auto-detection).
    """
    from moneybin.services.import_service import ImportService

    validated = _validate_file_path(file_path)
    try:
        result = ImportService(get_database()).import_file(
            str(validated),
            account_id=account_id,
            account_name=account_name,
            institution=institution,
            format_name=format_name,
        )
    except ValueError as e:
        raise UserError(str(e), code="import_error") from e
    return build_envelope(
        data={
            "message": result.summary(),
            "file_type": result.file_type,
            "transactions": result.transactions,
            "accounts": result.accounts,
            "date_range": result.date_range,
            "core_tables_rebuilt": result.core_tables_rebuilt,
        },
        sensitivity="low",
        actions=[
            "Use transactions.search to view imported transactions",
            "Use categorize.uncategorized to categorize new transactions",
        ],
    )
    # FileNotFoundError propagates — mcp_tool decorator converts it to an error envelope.
    # Other unclassified exceptions propagate to fastmcp's mask_error_details.


@mcp_tool(sensitivity="low")
def import_csv_preview(file_path: str) -> ResponseEnvelope:
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
            "Use import.file to import after reviewing the preview",
            "Use import.list_formats for available named formats",
        ],
    )
    # FileNotFoundError propagates — mcp_tool decorator converts it to an error envelope.
    # Other unclassified exceptions propagate to fastmcp's mask_error_details.


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
    from moneybin.loaders.tabular_loader import TabularLoader

    db = get_database()
    loader = TabularLoader(db)
    records = loader.get_import_history(
        limit=min(limit, 200),
        import_id=import_id,
    )
    return build_envelope(
        data=records,
        sensitivity="low",
        actions=[
            "Use import.file to import a new file",
        ],
    )


@mcp_tool(sensitivity="low")
def import_list_formats() -> ResponseEnvelope:
    """List all available tabular import formats (built-in and user-saved).

    Returns format name, institution, sign convention, date format, and
    header signature for each format. Use ``import.csv_preview`` to test
    a format against a specific file.
    """
    from moneybin.extractors.tabular.formats import (
        load_builtin_formats,
        load_formats_from_db,
        merge_formats,
    )

    builtin = load_builtin_formats()
    try:
        db = get_database()
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
            "Use import.csv_preview to test a format against a file",
            "Use import.file with format_name to import using a specific format",
        ],
    )


def register_import_tools(mcp: FastMCP) -> None:
    """Register all import namespace tools with the FastMCP server."""
    register(
        mcp,
        import_file,
        "import.file",
        "Import a financial data file (OFX, CSV, TSV, Excel, "
        "Parquet, PDF) into MoneyBin.",
    )
    register(
        mcp,
        import_csv_preview,
        "import.csv_preview",
        "Preview a tabular file's structure and detected column "
        "mapping without importing.",
    )
    register(
        mcp,
        import_status,
        "import.status",
        "List past import batches with status, row counts, and detection confidence.",
    )
    register(
        mcp,
        import_list_formats,
        "import.list_formats",
        "List all available tabular import formats (built-in and user-saved).",
    )

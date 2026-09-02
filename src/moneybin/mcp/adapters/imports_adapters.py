"""Adapters that project stored import formats into typed payload rows.

`moneybin import formats list` and the `import_formats` MCP tool answer the
same question and now return the same rows, so the projection lives once here
rather than being spelled twice with drifting field names.

Pure: no I/O, no side-effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from moneybin.privacy.payloads.imports import (
    ImportFormatDetail,
    ImportFormatRow,
    ImportPdfFormatDetail,
    ImportPdfFormatRow,
)

if TYPE_CHECKING:
    from collections.abc import Container
    from datetime import datetime

    from moneybin.extractors.tabular.formats import TabularFormat
    from moneybin.repositories.pdf_formats_repo import PdfFormat


def _iso_timestamp(value: datetime | None) -> str | None:
    """The stored timestamp in full, or None when the format was never used.

    Full precision, not the date half: the MCP `import_formats` tool has always
    emitted `last_used_at.isoformat()`, so truncating here to match the CLI
    table's date-only column would narrow a shipped contract field. The CLI's
    text branch does its own truncation for display and is unaffected.
    """
    return None if value is None else value.isoformat()


def tabular_format_row(
    fmt: TabularFormat, *, builtin: Container[str]
) -> ImportFormatRow:
    """Project one tabular format for the catalogue listing.

    ``builtin`` is the set of shipped format names, so ``source`` distinguishes
    a format MoneyBin ships from one the user saved — the field a caller needs
    before deciding whether deleting it is safe.
    """
    return ImportFormatRow(
        name=fmt.name,
        institution_name=fmt.institution_name,
        file_type=fmt.file_type,
        sign_convention=fmt.sign_convention,
        date_format=fmt.date_format,
        number_format=fmt.number_format,
        multi_account=fmt.multi_account,
        header_signature=fmt.header_signature,
        source="builtin" if fmt.name in builtin else "user",
    )


def pdf_format_row(pdf_format: PdfFormat) -> ImportPdfFormatRow:
    """Project one saved PDF format for the catalogue listing."""
    return ImportPdfFormatRow(
        name=pdf_format.name,
        institution_name=pdf_format.institution_name,
        document_kind=pdf_format.document_kind,
        routing=pdf_format.routing,
        front_end=pdf_format.front_end,
        version=pdf_format.version,
        times_used=pdf_format.times_used,
        last_used_at=_iso_timestamp(pdf_format.last_used_at),
    )


def tabular_format_detail(fmt: TabularFormat) -> ImportFormatDetail:
    """Project everything stored about one tabular format."""
    return ImportFormatDetail(
        name=fmt.name,
        institution_name=fmt.institution_name,
        file_type=fmt.file_type,
        delimiter=fmt.delimiter,
        encoding=fmt.encoding,
        skip_rows=fmt.skip_rows,
        sheet=fmt.sheet,
        sign_convention=fmt.sign_convention,
        date_format=fmt.date_format,
        number_format=fmt.number_format,
        multi_account=fmt.multi_account,
        header_signature=fmt.header_signature,
        field_mapping=dict(fmt.field_mapping),
        skip_trailing_patterns=fmt.skip_trailing_patterns,
    )


def pdf_format_detail(pdf_format: PdfFormat) -> ImportPdfFormatDetail:
    """Project everything stored about one PDF format, recipe included."""
    return ImportPdfFormatDetail(
        name=pdf_format.name,
        institution_name=pdf_format.institution_name,
        document_kind=pdf_format.document_kind,
        routing=pdf_format.routing,
        front_end=pdf_format.front_end,
        sign_convention=pdf_format.sign_convention,
        date_format=pdf_format.date_format,
        number_format=pdf_format.number_format,
        version=pdf_format.version,
        times_used=pdf_format.times_used,
        last_used_at=_iso_timestamp(pdf_format.last_used_at),
        source=pdf_format.source,
        extraction_recipe=cast("dict[str, Any] | None", pdf_format.extraction_recipe),
    )

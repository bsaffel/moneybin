"""Deterministic native-text PDF front-end (Phase 1): pdfplumber -> IR."""

from __future__ import annotations

import logging
from io import BytesIO
from pathlib import Path

import pdfplumber

from moneybin.extractors.pdf.ir import PdfDocument, PdfTable

logger = logging.getLogger(__name__)


class PDFExtractor:
    """Extract native-text tables + text lines from a PDF into the IR.

    Phase 1 is deterministic only: pdfplumber's table detection plus raw text
    lines. No recipe, no bridge. Tables with no detectable header row are
    skipped (their rows still appear via text_lines for power-user SQL later).
    """

    def extract(
        self,
        file_path: str | Path,
        *,
        source_bytes: bytes | None = None,
    ) -> PdfDocument:
        """Extract tables and text lines from a native-text PDF into the IR."""
        path = Path(file_path)
        if source_bytes is None and not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        tables: list[PdfTable] = []
        text_lines: list[str] = []
        with pdfplumber.open(
            path if source_bytes is None else BytesIO(source_bytes)
        ) as pdf:
            page_count = len(pdf.pages)
            for page_no, page in enumerate(pdf.pages, start=1):
                # layout=True preserves horizontal column gaps as spaces, enabling
                # the recipe executor's \s{2,} row-splitter to separate columns
                # (e.g. "01/02/2024   COFFEE SHOP   -4.50" → 3 tokens).
                page_text = page.extract_text(layout=True) or ""
                text_lines.extend(
                    line.strip() for line in page_text.splitlines() if line.strip()
                )
                for raw_table in page.extract_tables() or []:
                    parsed = _to_table(page_no, raw_table)
                    if parsed is not None:
                        tables.append(parsed)

        logger.info(
            f"pdf extract: file={path.name} pages={page_count} "
            f"tables={len(tables)} text_lines={len(text_lines)}"
        )
        return PdfDocument(
            source_file=path.name,
            tables=tables,
            text_lines=text_lines,
            page_count=page_count,
        )


def _to_table(page_no: int, raw: list[list[str | None]]) -> PdfTable | None:
    """Normalize a pdfplumber table (list of cell rows) into a PdfTable.

    The first non-empty row is the header. Cells are coerced to stripped
    strings (None -> ""). Rows whose width differs from the header are padded
    or truncated to header width so a stray merged cell doesn't abort the page.
    Returns None if fewer than 2 rows (header + >=1 data row).
    """
    cleaned = [[(c or "").strip() for c in row] for row in raw if any(row)]
    if len(cleaned) < 2:
        return None
    header = cleaned[0]
    # Reject tables with duplicate or empty headers — dict(zip(...)) downstream
    # silently dedupes and overwrites, causing permanent data loss. pdfplumber
    # sometimes emits these for merged-cell or section-title rows; skip the
    # table cleanly so the rest of the page still imports.
    if "" in header or len(set(header)) != len(header):
        logger.warning(
            f"pdf extract: skipping table on page {page_no} — "
            f"header has duplicate or empty cells: {header}"
        )
        return None
    width = len(header)
    rows: list[list[str]] = []
    for row in cleaned[1:]:
        if len(row) < width:
            row = row + [""] * (width - len(row))
        elif len(row) > width:
            row = row[:width]
        rows.append(row)
    return PdfTable(page=page_no, header=header, rows=rows)

"""Deterministic native-text PDF front-end (Phase 1): pdfplumber -> IR."""

from __future__ import annotations

import logging
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

    def extract(self, file_path: str | Path) -> PdfDocument:
        """Extract tables and text lines from a native-text PDF into the IR."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        tables: list[PdfTable] = []
        text_lines: list[str] = []
        page_no = 0
        with pdfplumber.open(path) as pdf:
            for page_no, page in enumerate(pdf.pages, start=1):
                page_text = page.extract_text() or ""
                text_lines.extend(
                    line.strip() for line in page_text.splitlines() if line.strip()
                )
                for raw_table in page.extract_tables() or []:
                    parsed = _to_table(page_no, raw_table)
                    if parsed is not None:
                        tables.append(parsed)

        logger.info(
            f"pdf extract: file={path.name} pages={page_no} "
            f"tables={len(tables)} text_lines={len(text_lines)}"
        )
        return PdfDocument(source_file=path.name, tables=tables, text_lines=text_lines)


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
    width = len(header)
    rows: list[list[str]] = []
    for row in cleaned[1:]:
        if len(row) < width:
            row = row + [""] * (width - len(row))
        elif len(row) > width:
            row = row[:width]
        rows.append(row)
    return PdfTable(page=page_no, header=header, rows=rows)

"""Tests for the deterministic pdfplumber front-end (PDFExtractor)."""

from pathlib import Path

import pytest

from moneybin.extractors.pdf.extractor import PDFExtractor


def test_extracts_table_rows_and_text(simple_statement_pdf: Path) -> None:
    doc = PDFExtractor().extract(simple_statement_pdf)
    assert doc.source_file == "simple_statement.pdf"
    # The transaction table is detected; the header + 3 rows survive.
    all_rows = [cells for _page, cells in doc.iter_rows()]
    descriptions = [r.get("Description", "") for r in all_rows]
    assert any("COFFEE SHOP" in d for d in descriptions)
    assert len(all_rows) == 3
    # Header-block text is captured in text_lines (used by later phases).
    assert any("Account Number" in line for line in doc.text_lines)


def test_extract_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"nope\.pdf"):
        PDFExtractor().extract(tmp_path / "nope.pdf")

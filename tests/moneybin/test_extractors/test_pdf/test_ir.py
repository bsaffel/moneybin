"""Tests for PDF extraction intermediate representation."""

from moneybin.extractors.pdf.ir import PdfDocument, PdfTable


def test_document_rows_flattens_tables_with_page() -> None:
    doc = PdfDocument(
        source_file="stmt.pdf",
        tables=[
            PdfTable(page=1, header=["Date", "Amount"], rows=[["2024-01-02", "10.00"]]),
            PdfTable(page=2, header=["Date", "Amount"], rows=[["2024-02-02", "20.00"]]),
        ],
        text_lines=["Account Number: ****1234"],
    )
    rows = list(doc.iter_rows())
    assert rows == [
        (1, {"Date": "2024-01-02", "Amount": "10.00"}),
        (2, {"Date": "2024-02-02", "Amount": "20.00"}),
    ]


def test_table_rejects_row_wider_than_header() -> None:
    import pytest

    with pytest.raises(ValueError):
        PdfTable(page=1, header=["A"], rows=[["x", "y"]])

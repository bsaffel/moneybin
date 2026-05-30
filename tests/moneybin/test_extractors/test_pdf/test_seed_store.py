"""Tests for PDF seed store: hashed JSON rows → raw.pdf_seeds + typed view."""

import pytest

from moneybin.database import Database
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.extractors.pdf.seed_store import write_pdf_seed


def _doc() -> PdfDocument:
    return PdfDocument(
        source_file="acme.pdf",
        tables=[
            PdfTable(
                page=1,
                header=["Date", "Description", "Amount"],
                rows=[
                    ["2024-01-02", "COFFEE", "-4.50"],
                    ["2024-01-05", "PAYROLL", "2000.00"],
                ],
            )
        ],
    )


@pytest.mark.integration
def test_write_seed_creates_view_and_rows(db: Database) -> None:
    n = write_pdf_seed(db, _doc(), alias="acme", import_id="imp-1")
    assert n == 2
    rows = db.execute(
        'SELECT "date", "description", "amount", page FROM raw.pdf_acme ORDER BY "date"'
    ).fetchall()
    assert rows[0][1] == "COFFEE"
    assert rows[1][2] == 2000.00


@pytest.mark.integration
def test_reimport_same_doc_is_idempotent(db: Database) -> None:
    write_pdf_seed(db, _doc(), alias="acme", import_id="imp-1")
    write_pdf_seed(db, _doc(), alias="acme", import_id="imp-2")
    row = db.execute(
        "SELECT COUNT(*) FROM raw.pdf_seeds WHERE alias = 'acme'"
    ).fetchone()
    assert row is not None
    assert row[0] == 2  # dedup by content hash; second import is a no-op


@pytest.mark.parametrize(
    ("samples", "expected"),
    [
        ([], "VARCHAR"),
        (["1", "2", "3"], "BIGINT"),
        (["1.5", "2.0"], "DECIMAL(18,2)"),
        (["1", "2.5"], "DECIMAL(18,2)"),
        (["2024-01-02", "2024-01-05"], "DATE"),
        (["2024-01-02", "5"], "VARCHAR"),
        (["inf"], "VARCHAR"),
        (["nan"], "VARCHAR"),
        (["1e5"], "VARCHAR"),
        (["COFFEE"], "VARCHAR"),
    ],
    ids=[
        "empty",
        "all_ints_BIGINT",
        "all_decimals_DECIMAL",
        "mixed_numeric_DECIMAL",
        "all_dates_DATE",
        "mixed_date_and_int_VARCHAR",
        "inf_VARCHAR",
        "nan_VARCHAR",
        "scientific_VARCHAR",
        "free_text_VARCHAR",
    ],
)
def test_infer_type_branches(samples: list[str], expected: str) -> None:
    from moneybin.extractors.pdf.seed_store import (
        _infer_type,  # type: ignore[reportPrivateUsage]
    )

    assert _infer_type(samples) == expected

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
    extracted, inserted = write_pdf_seed(db, _doc(), alias="acme", import_id="imp-1")
    assert extracted == 2
    assert inserted == 2
    rows = db.execute(
        'SELECT "date", "description", "amount", "_page" '
        'FROM raw.pdf_acme ORDER BY "date"'
    ).fetchall()
    assert rows[0][1] == "COFFEE"
    assert rows[1][2] == 2000.00


@pytest.mark.integration
def test_reimport_same_doc_is_idempotent(db: Database) -> None:
    extracted_1, inserted_1 = write_pdf_seed(
        db, _doc(), alias="acme", import_id="imp-1"
    )
    extracted_2, inserted_2 = write_pdf_seed(
        db, _doc(), alias="acme", import_id="imp-2"
    )
    # Second import extracts the same 2 rows but inserts 0 (content hashes
    # already present from imp-1; on_conflict='ignore' keeps the original).
    assert extracted_1 == 2
    assert inserted_1 == 2
    assert extracted_2 == 2
    assert inserted_2 == 0
    row = db.execute(
        "SELECT COUNT(*) FROM raw.pdf_seeds WHERE alias = 'acme'"
    ).fetchone()
    assert row is not None
    assert row[0] == 2


@pytest.mark.integration
def test_duplicate_rows_within_doc_are_preserved(db: Database) -> None:
    """Two rows with identical cell values must both persist (legitimate dupes).

    The pre-fix content-hash included only ``alias|json(row)``, collapsing
    identical-cell rows even when they represented distinct transactions
    (e.g. two same-day same-amount coffee purchases). The position-aware
    hash basis (``alias|p<page>r<idx>|json(row)``) preserves them.
    """
    doc = PdfDocument(
        source_file="dupes.pdf",
        tables=[
            PdfTable(
                page=1,
                header=["Date", "Description", "Amount"],
                rows=[
                    ["2024-01-02", "COFFEE", "-4.50"],
                    ["2024-01-02", "COFFEE", "-4.50"],  # legit duplicate
                ],
            )
        ],
    )
    extracted, inserted = write_pdf_seed(db, doc, alias="dupes", import_id="imp-1")
    assert extracted == 2
    assert inserted == 2
    row = db.execute(
        "SELECT COUNT(*) FROM raw.pdf_seeds WHERE alias = 'dupes'"
    ).fetchone()
    assert row is not None
    assert row[0] == 2


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
        # BIGINT overflow guard: >18 digits → VARCHAR to avoid CAST failure at query time.
        (["1234567890123456789"], "VARCHAR"),  # 19 digits — fallback to VARCHAR
        (["999999999999999999"], "BIGINT"),  # 18 nines — fits BIGINT
        (["-999999999999999999"], "BIGINT"),  # negative, 18 digits without sign — fits
        (
            ["-1234567890123456789"],
            "VARCHAR",
        ),  # negative, 19 digits without sign — fallback
        # Unicode digit codepoints (Arabic-Indic, Devanagari, full-width)
        # must NOT be mis-inferred as numeric — DuckDB can't CAST them to
        # BIGINT/DECIMAL, which would silently break view queries.
        (["٤٢"], "VARCHAR"),  # Arabic-Indic "٤٢"
        (["२०"], "VARCHAR"),  # Devanagari "२०"
        (["１２３"], "VARCHAR"),  # full-width "１２３"
        (["१३११-०१-०ॢ"], "VARCHAR"),
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
        "19_digit_int_falls_to_VARCHAR",
        "18_digit_int_stays_BIGINT",
        "negative_18_digit_int_stays_BIGINT",
        "negative_19_digit_int_falls_to_VARCHAR",
        "arabic_indic_digits_VARCHAR",
        "devanagari_digits_VARCHAR",
        "fullwidth_digits_VARCHAR",
        "devanagari_date_pattern_VARCHAR",
    ],
)
def test_infer_type_branches(samples: list[str], expected: str) -> None:
    from moneybin.extractors.pdf.seed_store import (
        _infer_type,  # type: ignore[reportPrivateUsage]
    )

    assert _infer_type(samples) == expected

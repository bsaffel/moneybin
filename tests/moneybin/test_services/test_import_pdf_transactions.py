"""Integration tests for ImportService PDF → tabular_transactions path (Phase 2a).

Tests verify the routing dispatch: high-confidence, reconciling PDFs land in
raw.tabular_transactions and save their recipe to app.pdf_formats; everything
else falls back to the Phase 1 raw.pdf_seeds path.

Mock strategy: stub PDFExtractor.extract() to return a hand-built PdfDocument
(no real PDF parsing), so the routing pipeline exercises end-to-end without I/O.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from moneybin.database import Database
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo
from moneybin.services.import_service import ImportService

# ---------------------------------------------------------------------------
# Shared fixtures / helpers (mirrors test_routing.py)
# ---------------------------------------------------------------------------

_HEADERS = ["Date", "Description", "Amount"]
_ROW_REGION_START = "Date  Description  Amount"
_ROW_REGION_END = "Total:"


def _make_doc(
    text_lines: list[str] | None = None,
    tables: list[PdfTable] | None = None,
) -> PdfDocument:
    return PdfDocument(
        source_file="stmt.pdf",
        text_lines=text_lines or [],
        tables=tables or [],
    )


def _standard_table(rows: list[list[str]] | None = None) -> PdfTable:
    return PdfTable(
        page=1,
        header=_HEADERS,
        rows=rows
        or [
            ["01/15/2024", "Coffee Shop", "-50.00"],
            ["01/20/2024", "Paycheck", "150.00"],
        ],
    )


def _standard_text_lines(
    opening: str = "1000.00", closing: str = "1100.00"
) -> list[str]:
    return [
        "Chase Bank Statement",
        "Account Number: 1234",
        "Statement Period: 01/01/2024",
        "To: 01/31/2024",
        f"Beginning Balance: ${opening}",
        f"Ending Balance: ${closing}",
        _ROW_REGION_START,
        "01/15/2024  Coffee Shop  -50.00",
        "01/20/2024  Paycheck  150.00",
        _ROW_REGION_END,
    ]


def _standard_doc(
    opening: str = "1000.00",
    closing: str = "1100.00",
) -> PdfDocument:
    """Full Chase statement doc — reconciliation passes with opening/closing = 100 delta."""
    return _make_doc(
        text_lines=_standard_text_lines(opening, closing),
        tables=[_standard_table()],
    )


def _valid_recipe_dict() -> dict[str, Any]:
    return {
        "metadata_anchors": [],
        "row_region": {
            "start_anchor": _ROW_REGION_START,
            "end_anchor": _ROW_REGION_END,
        },
        "row_split": r"\s{2,}",
        "fields": [
            {
                "name": "Date",
                "pattern": r"\d{2}/\d{2}/\d{4}",
                "cast": "date",
                "date_format": "%m/%d/%Y",
            },
            {"name": "Description", "pattern": r".+", "cast": "str"},
            {"name": "Amount", "pattern": r"-?\$?[\d,]+\.\d{2}", "cast": "decimal"},
        ],
        "sign_convention": "negative_is_expense",
        "routing": "transactions",
    }


def _save_chase_format(
    db: Database,
    recipe: dict[str, Any] | None = None,
    *,
    name: str = "chase_checking_pdf",
) -> None:
    """Insert a Chase format row into app.pdf_formats so fingerprint lookup hits."""
    from moneybin.extractors.pdf.fingerprint import compute_fingerprint

    repo = PdfFormatsRepo(db)
    fp = compute_fingerprint(_standard_doc())
    repo.save_new(
        name,
        recipe if recipe is not None else _valid_recipe_dict(),
        fingerprint=fp,
        institution_name="Chase",
        document_kind="checking_statement",
        front_end="text",
        routing="transactions",
        actor="test",
    )


def _service_with_fake_pdf(
    db: Database, doc: PdfDocument, tmp_path: Path
) -> tuple[ImportService, Path]:
    """Return (ImportService, fake_pdf_path) patched so PDFExtractor.extract returns doc."""
    fake_pdf = tmp_path / "statement.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4 fake")  # non-empty so Path.exists() passes
    svc = ImportService(db)
    return svc, fake_pdf


# ---------------------------------------------------------------------------
# Test 1: First contact — auto-derive, routes to tabular_transactions, saves format
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_first_contact_routes_to_transactions_and_saves_format(
    db: Database, tmp_path: Path
) -> None:
    """Auto-derive path: rows land in tabular_transactions; format saved to pdf_formats."""
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        result = svc.import_file(fake_pdf, refresh=False)

    assert result.file_type == "pdf"
    assert result.import_id is not None
    assert result.transactions > 0

    # Rows landed in raw.tabular_transactions
    row = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone()
    assert row is not None
    assert row[0] == result.transactions

    # Format was saved to app.pdf_formats
    formats = db.execute("SELECT COUNT(*) FROM app.pdf_formats").fetchone()
    assert formats is not None
    assert formats[0] == 1


# ---------------------------------------------------------------------------
# Test 2: Replay — saved format found, rows land in tabular_transactions, no new format
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_replay_uses_saved_format(db: Database, tmp_path: Path) -> None:
    """Saved format matched → replay path; rows land in tabular_transactions; no new format row."""
    doc = _standard_doc()
    _save_chase_format(db)

    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        result = svc.import_file(fake_pdf, refresh=False)

    assert result.file_type == "pdf"
    assert result.transactions > 0

    txn_count = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE import_id = ?",
        [result.import_id],
    ).fetchone()
    assert txn_count is not None
    assert txn_count[0] == result.transactions

    # Still exactly one format row (the pre-populated one; no new one created)
    formats = db.execute("SELECT COUNT(*) FROM app.pdf_formats").fetchone()
    assert formats is not None
    assert formats[0] == 1


# ---------------------------------------------------------------------------
# Test 3: Replay reconciliation fail → seed fallback
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_replay_reconciliation_fail_falls_back_to_seed(
    db: Database, tmp_path: Path
) -> None:
    """Saved recipe fails reconciliation → seed path taken; no tabular_transactions rows."""
    # Wrong balances: rows sum to 100 but delta is 8999
    doc = _standard_doc(opening="1000.00", closing="9999.00")
    _save_chase_format(db)

    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        result = svc.import_file(fake_pdf, refresh=False)

    assert result.file_type == "pdf"
    # Seed path: details has seed_rows key
    assert "seed_rows" in result.details
    assert result.details["seed_rows"] > 0

    # No tabular_transactions rows for this import
    txn_count = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone()
    assert txn_count is not None
    assert txn_count[0] == 0


# ---------------------------------------------------------------------------
# Test 4: No transaction table → seed fallback, no format saved
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_low_confidence_or_no_table_falls_back_to_seed(
    db: Database, tmp_path: Path
) -> None:
    """Doc with no transaction table → seed path; no format saved; zero tabular rows."""
    doc = _make_doc(
        text_lines=["Just a header page", "No transactions here"],
    )

    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        # No tables → write_pdf_seed gets called; but no selectable tables either.
        # write_pdf_seed will produce 0 rows, which raises ValueError.
        with pytest.raises(ValueError, match="No tables extracted"):
            svc.import_file(fake_pdf, refresh=False)

    # No tabular_transactions rows
    txn_count = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone()
    assert txn_count is not None
    assert txn_count[0] == 0

    # No format saved
    formats = db.execute("SELECT COUNT(*) FROM app.pdf_formats").fetchone()
    assert formats is not None
    assert formats[0] == 0


# ---------------------------------------------------------------------------
# Test 5: Revert clears both raw.tabular_transactions AND raw.pdf_seeds
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_revert_clears_tabular_transactions(db: Database, tmp_path: Path) -> None:
    """Importing a PDF that routes to transactions then reverting removes the rows."""
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        result = svc.import_file(fake_pdf, refresh=False)

    assert result.import_id is not None
    assert result.transactions > 0

    # Rows present before revert
    before = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE import_id = ?",
        [result.import_id],
    ).fetchone()
    assert before is not None
    assert before[0] > 0

    # Revert
    out = svc.revert(result.import_id)
    assert out["status"] == "reverted"

    # Rows gone after revert
    after = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE import_id = ?",
        [result.import_id],
    ).fetchone()
    assert after is not None
    assert after[0] == 0

    # pdf_seeds is vacuously empty (nothing was written there)
    seeds = db.execute(
        "SELECT COUNT(*) FROM raw.pdf_seeds WHERE import_id = ?",
        [result.import_id],
    ).fetchone()
    assert seeds is not None
    assert seeds[0] == 0

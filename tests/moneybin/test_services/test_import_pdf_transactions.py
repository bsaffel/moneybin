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
    # `metadata_anchors` omitted → None → routing falls back to DEFAULT_ANCHORS
    # for capture_metadata, so opening/closing balance anchors find values and
    # reconciliation passes. An explicit `[]` would mean "deliberately decline
    # metadata capture" and route to seed with reason=metadata_incomplete.
    return {
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

    # Account row gone after revert — REVERT_TABLES["pdf"] includes
    # TABULAR_ACCOUNTS so the account row written alongside the transactions
    # gets cleared. Without this assertion a regression that drops
    # TABULAR_ACCOUNTS from REVERT_TABLES would leave orphan account rows.
    accounts_after = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_accounts WHERE import_id = ?",
        [result.import_id],
    ).fetchone()
    assert accounts_after is not None
    assert accounts_after[0] == 0

    # pdf_seeds is vacuously empty (nothing was written there)
    seeds = db.execute(
        "SELECT COUNT(*) FROM raw.pdf_seeds WHERE import_id = ?",
        [result.import_id],
    ).fetchone()
    assert seeds is not None
    assert seeds[0] == 0


# ---------------------------------------------------------------------------
# Test 6: rows_inserted matches the table's conflict key
# (regression for the codex finding that pre-count by transaction_id alone
# under-reported when source_file differed — tabular_transactions PK is
# (transaction_id, account_id, source_file), so a same-content import from a
# different path genuinely inserts new rows. The count now reflects that.)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_reimport_count_matches_conflict_key(db: Database, tmp_path: Path) -> None:
    """Re-importing same content from a NEW path inserts and counts the rows.

    Different source_file ⇒ different PK row in tabular_transactions, so
    the INSERT genuinely lands and rows_inserted reports the true count.
    The pre-count was changed to match the table's (transaction_id,
    account_id, source_file) key so reporting and storage agree.
    """
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        first = svc.import_file(fake_pdf, refresh=False)

    assert first.transactions == 2
    assert first.details["transactions"] == 2
    assert first.details["transactions_extracted"] == 2

    fake_pdf_2 = tmp_path / "statement_again.pdf"
    fake_pdf_2.write_bytes(b"%PDF-1.4 fake")

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        second = svc.import_file(fake_pdf_2, refresh=False)

    # Second import from a different path: rows DO land (PK includes
    # source_file). The honest count is 2 inserted, 2 extracted.
    assert second.details["transactions_extracted"] == 2
    assert second.transactions == 2
    assert second.details["transactions"] == 2

    # Both imports landed rows — total rows in the table reflects both.
    row_count = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone()
    assert row_count is not None
    assert row_count[0] == 4  # 2 from first import + 2 from second


# ---------------------------------------------------------------------------
# Test 7: Duplicate format name (hash collision / race) is non-fatal
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_duplicate_format_name_is_swallowed(db: Database, tmp_path: Path) -> None:
    """Second import with a pre-existing format name (same fingerprint) is a no-op."""
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    # First import creates the format.
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        svc.import_file(fake_pdf, refresh=False)

    # Delete the saved-format fingerprint from the routing side so the second
    # import takes the "auto-derive again, try save_new" path instead of replay.
    # The format row itself stays — so save_new raises ConstraintException.
    db.execute("UPDATE app.pdf_formats SET layout_fingerprint = '{}'::JSON")

    fake_pdf_2 = tmp_path / "statement_again.pdf"
    fake_pdf_2.write_bytes(b"%PDF-1.4 fake")

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        # Should not raise — ConstraintException on save_new is logged and skipped.
        result = svc.import_file(fake_pdf_2, refresh=False)

    assert result.file_type == "pdf"
    # Still exactly one format row (no duplicate save).
    formats = db.execute("SELECT COUNT(*) FROM app.pdf_formats").fetchone()
    assert formats is not None
    assert formats[0] == 1


# ---------------------------------------------------------------------------
# Test 8: Failure during ingest cleans up tabular rows + finalizes as failed
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_transactions_path_cleanup_on_ingest_failure(
    db: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ingest_dataframe raising mid-import → DELETE + finalize_import(failed)."""
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    # Patch ingest_dataframe to raise AFTER rows have landed, so the failure-cleanup
    # path (DELETE + finalize "failed") executes against real DB state. `db` here
    # is the same connection ImportService holds — patching it here patches both.
    original_ingest = db.ingest_dataframe

    def _flaky_ingest(*args: Any, **kwargs: Any) -> None:
        original_ingest(*args, **kwargs)
        raise RuntimeError("simulated mid-ingest failure")

    monkeypatch.setattr(db, "ingest_dataframe", _flaky_ingest)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        with pytest.raises(RuntimeError, match="simulated"):
            svc.import_file(fake_pdf, refresh=False)

    # Cleanup ran: no tabular_transactions rows survive the failure
    rows = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone()
    assert rows is not None
    assert rows[0] == 0

    # The import_log row was finalized as "failed", not left in "importing"
    log_status = db.execute(
        "SELECT status FROM raw.import_log WHERE source_type = 'pdf' "
        "ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    assert log_status is not None
    assert log_status[0] == "failed"


# ---------------------------------------------------------------------------
# Test 9: _to_account_number_mask covers every branch of the privacy boundary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # None / empty / whitespace-only → None
        (None, None),
        ("", None),
        ("   ", None),
        # Already-masked tokens (any supported prefix) → stripped, unchanged
        ("****1234", "****1234"),
        ("xxxx1234", "xxxx1234"),
        ("XXXX1234", "XXXX1234"),
        ("  ****1234  ", "****1234"),
        # Multi-digit raw values reduce to ****<last4>
        ("123456789", "****6789"),
        ("Account Number: 5678", "****5678"),
        ("1234", "****1234"),
        # No-digits branch returns the captured value verbatim (stripped),
        # never silently dropped — the column stays observable to the operator
        # even if the captured token is something exotic.
        ("ABC-XYZ", "ABC-XYZ"),
        ("  ABC  ", "ABC"),
    ],
)
def test_to_account_number_mask_covers_every_branch(
    raw: str | None, expected: str | None
) -> None:
    """Exercise every branch of _to_account_number_mask (privacy boundary)."""
    from moneybin.services.import_service import (
        _to_account_number_mask,  # pyright: ignore[reportPrivateUsage]
    )

    assert _to_account_number_mask(raw) == expected


# ---------------------------------------------------------------------------
# Test 10: save_format=False suppresses first-contact recipe persistence
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_first_contact_save_format_false_suppresses_recipe(
    db: Database, tmp_path: Path
) -> None:
    """save_format=False routes to transactions but skips app.pdf_formats save.

    Mirrors the tabular ``--no-save-format`` semantics: rows still land,
    but no layout fingerprint persists, so the same statement format
    will re-derive from scratch on every future import. The corresponding
    import_log row carries format_source='detected' (auto-derive ran)
    but format_name=NULL (no persistence to point at).
    """
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        result = svc.import_file(fake_pdf, refresh=False, save_format=False)

    assert result.file_type == "pdf"
    assert result.transactions > 0
    # Rows landed in raw.tabular_transactions
    rows = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE import_id = ?",
        [result.import_id],
    ).fetchone()
    assert rows is not None
    assert rows[0] == result.transactions

    # No format saved
    formats = db.execute("SELECT COUNT(*) FROM app.pdf_formats").fetchone()
    assert formats is not None
    assert formats[0] == 0

    # Import_log format columns reflect "ran auto-derive but did not persist"
    log = db.execute(
        "SELECT format_name, format_source FROM raw.import_log WHERE import_id = ?",
        [result.import_id],
    ).fetchone()
    assert log is not None
    assert log[0] is None
    assert log[1] == "detected"


# ---------------------------------------------------------------------------
# Test 11: Broken-recipe ConstraintException — auto-derive re-derives + auto-bumps
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_replay_invalid_recipe_auto_bumps_format(
    db: Database, tmp_path: Path
) -> None:
    """A saved recipe that fails model_validate is re-derived and auto-bumped (Req 9a).

    Routing falls through to auto-derive (the saved recipe can't validate;
    test_replay_invalid_recipe_falls_through_to_auto_derive covers that side),
    the re-derived recipe reconciles, and save_new collides with the stale row
    on its fingerprint-derived primary key. Instead of leaving the broken recipe
    stuck (the old Phase 2a dead end), the service bumps it to a new version so
    the next statement of this layout replays the corrected recipe rather than
    re-deriving forever.
    """
    import json as _json

    from moneybin.extractors.pdf.fingerprint import compute_fingerprint
    from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo

    # First contact: auto-derive persists a valid format (version 1) under its
    # fingerprint-derived name.
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        svc.import_file(fake_pdf, refresh=False)

    fp = compute_fingerprint(doc)
    saved = PdfFormatsRepo(db).get_by_fingerprint(fp)
    assert saved is not None
    format_name = saved.name

    # Simulate recipe drift: corrupt the stored recipe so the next replay fails
    # model_validate (missing required fields), routing back through auto-derive.
    db.execute(
        "UPDATE app.pdf_formats SET extraction_recipe = ?::JSON WHERE name = ?",
        [
            _json.dumps({
                "row_region": {
                    "start_anchor": _ROW_REGION_START,
                    "end_anchor": _ROW_REGION_END,
                }
            }),
            format_name,
        ],
    )

    # Re-import the same layout: replay loads the broken recipe → model_validate
    # fails → auto-derive → save_new collides → bump restores a valid recipe.
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        result = svc.import_file(fake_pdf, refresh=False)
    assert result.file_type == "pdf"  # import did not dead-end

    row = db.execute(
        "SELECT version, extraction_recipe FROM app.pdf_formats WHERE name = ?",
        [format_name],
    ).fetchone()
    assert row is not None
    assert row[0] == 2  # bumped from the version-1 stale row
    stored_recipe = _json.loads(row[1])
    # The stored recipe is now the valid auto-derived one (has the fields the
    # corrupted stub lacked), not the broken stub.
    assert "row_split" in stored_recipe
    assert "fields" in stored_recipe

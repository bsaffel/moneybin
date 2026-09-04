"""Integration tests for ImportService PDF → tabular_transactions path (Phase 2a).

Tests verify the routing dispatch: high-confidence, reconciling PDFs land in
raw.tabular_transactions and save their recipe to app.pdf_formats; everything
else falls back to the Phase 1 raw.pdf_seeds path.

Mock strategy: stub PDFExtractor.extract() to return a hand-built PdfDocument
(no real PDF parsing), so the routing pipeline exercises end-to-end without I/O.
The sign-convention gate tests are the exception — they import committed
statement PDFs through the real extractor, because the evidence the gate acts on
is text the extractor has to actually surface.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.metrics.observations import MetricObservations
from moneybin.orchestration.refresh import RefreshResult
from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo
from moneybin.services.import_confirmation import (
    ImportConfirmationRequiredError,
    SignConventionProposal,
)
from moneybin.services.import_service import ImportService
from tests.import_helpers import import_answering_gate
from tests.moneybin.db_helpers import create_core_tables
from tests.moneybin.pdf_statement_fixtures import (
    write_card_statement_pdf,
    write_checking_statement_pdf,
)

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


def _anchorless_doc(opening: str = "1000.00", closing: str = "1100.00") -> PdfDocument:
    """The standard statement with its account-number line removed.

    Everything else still reads — issuer, balances, rows — so routing reaches
    ``transactions`` exactly as usual. Only cross-document account evidence is
    missing, which isolates the path where an opaque document digest preserves
    exact-file identity but cannot identify another statement for the account.
    """
    return _make_doc(
        text_lines=[
            line
            for line in _standard_text_lines(opening, closing)
            if not line.startswith("Account Number:")
        ],
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


def _seed_chase_twin(db: Database, account_id: str = "acct_existing01") -> None:
    """An existing Chase ...1234 account, so the statement's identity is a question.

    Candidates are what make the account gate fire: a mint with nothing to merge
    into proceeds and is reported instead. Every test below that asserts on the
    gate has to give the resolver something the statement could plausibly BE,
    which is the case the gate exists for — a weak institution+last4 merge.
    """
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts "  # noqa: S608  # test fixture
        "(account_id, display_name, institution_slug, last_four) "
        "VALUES (?, ?, ?, ?)",
        [account_id, "Chase Card", "chase", "1234"],
    )


def _count(db: Database, query: str) -> int:
    """Return one COUNT(*) result."""
    row = db.execute(query).fetchone()
    assert row is not None
    return int(row[0])


@pytest.mark.integration
def test_pdf_transaction_import_joins_outer_transaction_and_buffers_metrics(
    db: Database,
    tmp_path: Path,
) -> None:
    from moneybin.metrics.registry import PDF_IMPORT_TOTAL

    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)
    observations = MetricObservations()
    metric = PDF_IMPORT_TOTAL.labels(outcome="transactions", rung="deterministic")
    before = metric._value.get()  # type: ignore[reportPrivateUsage]

    db.begin()
    try:
        with patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ):
            result = import_answering_gate(
                svc,
                fake_pdf,
                refresh=False,
                in_outer_txn=True,
                emit_metrics=False,
                observations=observations,
            )
        assert result.transactions == 2
        assert _count(db, "SELECT COUNT(*) FROM raw.tabular_transactions") == 2
    finally:
        db.rollback()

    assert _count(db, "SELECT COUNT(*) FROM raw.tabular_transactions") == 0
    assert _count(db, "SELECT COUNT(*) FROM raw.tabular_accounts") == 0
    assert _count(db, "SELECT COUNT(*) FROM raw.import_log") == 0
    assert _count(db, "SELECT COUNT(*) FROM app.pdf_formats") == 0
    assert metric._value.get() == before  # type: ignore[reportPrivateUsage]
    observations.flush("rollback")
    assert metric._value.get() == before  # type: ignore[reportPrivateUsage]


@pytest.mark.integration
def test_pdf_seed_import_joins_outer_transaction_and_buffers_metrics(
    db: Database,
    tmp_path: Path,
) -> None:
    from moneybin.metrics.registry import PDF_IMPORT_TOTAL, PDF_SEED_ROWS_TOTAL

    doc = _standard_doc(opening="1000.00", closing="9999.00")
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)
    observations = MetricObservations()
    import_metric = PDF_IMPORT_TOTAL.labels(outcome="seed", rung="deterministic")
    seed_metric = PDF_SEED_ROWS_TOTAL.labels(alias="statement")
    import_before = import_metric._value.get()  # type: ignore[reportPrivateUsage]
    seed_before = seed_metric._value.get()  # type: ignore[reportPrivateUsage]

    db.begin()
    try:
        with patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ):
            result = import_answering_gate(
                svc,
                fake_pdf,
                refresh=False,
                in_outer_txn=True,
                emit_metrics=False,
                observations=observations,
            )
        assert result.details["seed_rows"] == 2
        assert _count(db, "SELECT COUNT(*) FROM raw.pdf_seeds") == 2
    finally:
        db.rollback()

    assert _count(db, "SELECT COUNT(*) FROM raw.pdf_seeds") == 0
    assert _count(db, "SELECT COUNT(*) FROM raw.import_log") == 0
    assert import_metric._value.get() == import_before  # type: ignore[reportPrivateUsage]
    assert seed_metric._value.get() == seed_before  # type: ignore[reportPrivateUsage]
    observations.flush("rollback")
    assert import_metric._value.get() == import_before  # type: ignore[reportPrivateUsage]
    assert seed_metric._value.get() == seed_before  # type: ignore[reportPrivateUsage]


@pytest.mark.integration
def test_pdf_extraction_failure_metric_can_be_buffered(
    db: Database,
    tmp_path: Path,
) -> None:
    from moneybin.metrics.registry import PDF_IMPORT_TOTAL

    svc, fake_pdf = _service_with_fake_pdf(db, _standard_doc(), tmp_path)
    observations = MetricObservations()
    metric = PDF_IMPORT_TOTAL.labels(outcome="failed", rung="deterministic")
    before = metric._value.get()  # type: ignore[reportPrivateUsage]

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            side_effect=ValueError("bad PDF"),
        ),
        pytest.raises(ValueError, match="bad PDF"),
    ):
        import_answering_gate(
            svc,
            fake_pdf,
            refresh=False,
            emit_metrics=False,
            observations=observations,
        )

    assert metric._value.get() == before  # type: ignore[reportPrivateUsage]
    observations.flush("rollback")
    assert metric._value.get() == before + 1  # type: ignore[reportPrivateUsage]


@pytest.mark.integration
def test_pdf_read_failure_bumps_failed_metric(
    db: Database,
    tmp_path: Path,
) -> None:
    from moneybin.metrics.registry import PDF_IMPORT_TOTAL

    svc, fake_pdf = _service_with_fake_pdf(db, _standard_doc(), tmp_path)
    metric = PDF_IMPORT_TOTAL.labels(outcome="failed", rung="deterministic")
    before = metric._value.get()  # type: ignore[reportPrivateUsage]

    with (
        patch.object(Path, "read_bytes", side_effect=OSError("PDF disappeared")),
        pytest.raises(OSError, match="disappeared"),
    ):
        svc.import_file(fake_pdf, refresh=False)

    assert metric._value.get() == before + 1  # type: ignore[reportPrivateUsage]


@pytest.mark.integration
def test_pdf_sign_proposal_metric_is_buffered_once(
    db: Database,
    tmp_path: Path,
) -> None:
    from moneybin.metrics.registry import PDF_SIGN_GATE_TOTAL

    observations = MetricObservations()
    metric = PDF_SIGN_GATE_TOTAL.labels(outcome="proposed")
    before = metric._value.get()  # type: ignore[reportPrivateUsage]

    with pytest.raises(ImportConfirmationRequiredError):
        ImportService(db).import_file(
            write_card_statement_pdf(tmp_path),
            refresh=False,
            save_format=False,
            emit_metrics=False,
            observations=observations,
        )

    assert metric._value.get() == before  # type: ignore[reportPrivateUsage]
    observations.flush("rollback")
    assert metric._value.get() == before + 1  # type: ignore[reportPrivateUsage]


@pytest.mark.parametrize(
    "helper_name",
    ["_persist_replayed_sign_override", "_persist_self_healed_recipe"],
)
def test_pdf_saved_recipe_bookkeeping_re_raises_inside_outer_transaction(
    db: Database,
    helper_name: str,
) -> None:
    recipe = MagicMock()
    recipe.model_dump.return_value = {"sign_convention": "negative_is_expense"}
    decision = SimpleNamespace(
        matched_format_name="saved_pdf",
        recipe=recipe,
        fp={"issuer": "Example"},
        rederived_reason="saved recipe stopped reconciling",
    )
    service = ImportService(db)

    with (
        patch.object(
            PdfFormatsRepo,
            "get_by_fingerprint",
            return_value=None,
        ),
        patch.object(
            PdfFormatsRepo,
            "bump_version",
            side_effect=RuntimeError("bookkeeping failed"),
        ) as bump,
        pytest.raises(RuntimeError, match="bookkeeping failed"),
    ):
        getattr(service, helper_name)(
            decision,
            import_id="imp_outer",
            in_outer_txn=True,
        )

    assert bump.call_args.kwargs["in_outer_txn"] is True


def test_pdf_self_heal_persists_the_reason_the_decision_carries(db: Database) -> None:
    """The durable audit reason names the trigger that actually fired.

    ``app.audit_log`` is what an operator reads back through ``system audit``, so
    a repair triggered by a digit-free account id must not record that the recipe
    stopped reconciling — that replay reconciled to the cent.
    """
    recipe = MagicMock()
    recipe.model_dump.return_value = {"sign_convention": "negative_is_expense"}
    decision = SimpleNamespace(
        matched_format_name="saved_pdf",
        recipe=recipe,
        fp={"issuer": "Example"},
        rederived_reason="saved recipe read the account number as a digit-free mask",
    )

    service = ImportService(db)

    with patch.object(PdfFormatsRepo, "bump_version") as bump:
        service._persist_self_healed_recipe(  # type: ignore[reportPrivateUsage]
            decision,  # type: ignore[reportArgumentType]  # stands in for the fields read
            import_id="imp_reason",
        )

    assert bump.call_args.kwargs["reason"] == (
        "saved recipe read the account number as a digit-free mask"
    )


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
        result = import_answering_gate(svc, fake_pdf, refresh=False)

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


@pytest.mark.integration
def test_pdf_import_extracts_supplied_immutable_bytes(
    db: Database,
    tmp_path: Path,
) -> None:
    """The PDF path parses the previewed object instead of reopening live content."""
    doc = _standard_doc()
    source_bytes = b"%PDF immutable preview object"
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ) as extract:
        svc.import_file(
            fake_pdf,
            source_bytes=source_bytes,
            refresh=False,
        )

    extract.assert_called_once_with(
        fake_pdf.resolve(),
        source_bytes=source_bytes,
    )


@pytest.mark.integration
def test_pdf_import_reads_path_once_for_hash_and_extraction(
    db: Database,
    tmp_path: Path,
) -> None:
    """A path import hashes and parses the same immutable byte snapshot."""
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)
    source_bytes = fake_pdf.read_bytes()

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ) as extract:
        svc.import_file(fake_pdf, refresh=False)

    extract.assert_called_once_with(
        fake_pdf.resolve(),
        source_bytes=source_bytes,
    )


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
        result = import_answering_gate(svc, fake_pdf, refresh=False)

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
        result = import_answering_gate(svc, fake_pdf, refresh=False)

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
            import_answering_gate(svc, fake_pdf, refresh=False)

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
        result = import_answering_gate(svc, fake_pdf, refresh=False)

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
    out = svc.revert_confirmed(result.import_id, verify=lambda _live: None)
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
        first = import_answering_gate(svc, fake_pdf, refresh=False)

    assert first.transactions == 2
    assert first.details["transactions"] == 2
    assert first.details["transactions_extracted"] == 2

    fake_pdf_2 = tmp_path / "statement_again.pdf"
    fake_pdf_2.write_bytes(b"%PDF-1.4 fake")

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        second = import_answering_gate(svc, fake_pdf_2, refresh=False)

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


@pytest.mark.integration
def test_pdf_reimport_retries_refresh_after_prior_refresh_failure(
    db: Database, tmp_path: Path
) -> None:
    """An idempotent raw reload must still retry a failed transform."""
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)
    # The real dataclass, not a SimpleNamespace: the import path reads fields
    # beyond applied/error (transfers_retired, so a reversal committed by the
    # match step is disclosed even when the transform then fails), and a
    # hand-rolled stand-in silently lacks whichever field it was not updated
    # for. Constructing the real thing means a field added later arrives with
    # its default instead of an AttributeError from inside production code.
    failed_refresh = RefreshResult(
        applied=False, duration_seconds=0.0, error="test refresh failure"
    )

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        patch("moneybin.services.import_service._refresh", return_value=failed_refresh),
        pytest.raises(RuntimeError, match="test refresh failure"),
    ):
        svc.import_file(fake_pdf, refresh=True)

    successful_refresh = RefreshResult(applied=True, duration_seconds=0.0)
    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        patch(
            "moneybin.services.import_service._refresh",
            return_value=successful_refresh,
        ) as refresh_mock,
    ):
        repeated = svc.import_file(fake_pdf, refresh=True)

    refresh_mock.assert_called_once_with(db)
    assert repeated.transactions == 0
    assert repeated.details["transactions_extracted"] == 2
    assert repeated.core_tables_rebuilt is True


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
        import_answering_gate(svc, fake_pdf, refresh=False)

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
        result = import_answering_gate(svc, fake_pdf_2, refresh=False)

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
            import_answering_gate(svc, fake_pdf, refresh=False)

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
# Test 8b: AccountResolver failure finalizes the import instead of stranding it
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_resolver_failure_finalizes_import_and_records_the_failure_metric(
    db: Database, tmp_path: Path
) -> None:
    """A raise inside resolve() must not strand import_log at "importing".

    The resolve() call sits after begin_import() but outside the ingestion
    try/except below it, so without its own guard an unhandled raise there
    leaves the row "importing" forever and never emits the failure metric —
    the import looks perpetually in-flight rather than failed.
    """
    from moneybin.metrics.registry import PDF_IMPORT_TOTAL

    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)
    observations = MetricObservations()
    metric = PDF_IMPORT_TOTAL.labels(outcome="failed", rung="deterministic")
    before = metric._value.get()  # type: ignore[reportPrivateUsage]

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        patch(
            "moneybin.services.import_service.AccountResolver.resolve",
            side_effect=RuntimeError("simulated resolver failure"),
        ),
        pytest.raises(RuntimeError, match="simulated resolver failure"),
    ):
        import_answering_gate(
            svc,
            fake_pdf,
            refresh=False,
            emit_metrics=False,
            observations=observations,
        )

    # Finalized as "failed" — not left at "importing".
    log_status = db.execute(
        "SELECT status FROM raw.import_log WHERE source_type = 'pdf' "
        "ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    assert log_status is not None
    assert log_status[0] == "failed"

    # The failure metric is buffered, then emitted on flush.
    assert metric._value.get() == before  # type: ignore[reportPrivateUsage]
    observations.flush("rollback")
    assert metric._value.get() == before + 1  # type: ignore[reportPrivateUsage]


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
        # Already-masked tokens normalise to one canonical shape regardless of
        # the glyphs the statement used. These previously round-tripped
        # verbatim, which made ONE card key two different ways: a statement
        # printing "****1234" produced chase_1234 while the same card printing
        # "xxxx1234" produced chase_xxxx1234, so consecutive statements never
        # matched each other.
        ("****1234", "****1234"),
        ("xxxx1234", "****1234"),
        ("XXXX1234", "****1234"),
        ("  ****1234  ", "****1234"),
        # Space- and hyphen-grouped card numbers keep the TRAILING group.
        ("XXXX XXXX XXXX 1234", "****1234"),
        ("**** **** **** 1234", "****1234"),
        ("1234-5678-9012-3456", "****3456"),
        # Multi-digit raw values reduce to ****<last4>
        ("123456789", "****6789"),
        ("Account Number: 5678", "****5678"),
        ("1234", "****1234"),
        # Fewer-than-4-digits returns the captured value verbatim (stripped),
        # never silently dropped — the column stays observable to the operator
        # even if the captured token is something exotic. Crucially it is NOT
        # padded into a short "****12": a fabricated last4 reads as
        # authoritative to the institution+last4 merge signal.
        ("ABC-XYZ", "ABC-XYZ"),
        ("  ABC  ", "ABC"),
        ("xxxx", "xxxx"),
        ("12", "12"),
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
# Test 9b: PDF account identity is minted by AccountResolver, not string-built
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_fully_masked_pdf_requires_explicit_binding_before_loading(
    db: Database, tmp_path: Path
) -> None:
    doc = _make_doc(
        text_lines=[
            line.replace("Account Number: 1234", "Account Number: xxxx")
            for line in _standard_text_lines()
        ],
        tables=[_standard_table()],
    )
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        svc.import_file(fake_pdf, refresh=False, confirm=True, actor_kind="human")

    [proposal] = exc.value.outcome.account_proposals
    assert proposal["candidates"] == []
    assert proposal["is_new"] is True
    assert _count(db, "SELECT COUNT(*) FROM raw.tabular_transactions") == 0
    assert _count(db, "SELECT COUNT(*) FROM raw.import_log") == 0
    assert _count(db, "SELECT COUNT(*) FROM app.account_links") == 0


@pytest.mark.integration
def test_consecutive_statements_of_one_card_share_one_account(
    db: Database, tmp_path: Path
) -> None:
    """Two statements of the same card must not split across two accounts.

    The native key is issuer+last4, identical for every statement of a card.
    What varies is the filename — so scoping the link to a per-file alias makes
    the strong-ref lookup miss its own prior link, minting a fresh canonical
    account and a fresh review candidate every month, even after a human
    accepted last month's. The link scope must be the issuer/format identity,
    stable across statements, exactly as the tabular path derives it.
    """
    doc = _make_doc(text_lines=_standard_text_lines(), tables=[_standard_table()])
    svc = ImportService(db)

    for stem in ("chase_january_2024", "chase_february_2024"):
        pdf = tmp_path / f"{stem}.pdf"
        pdf.write_bytes(b"%PDF-1.4 fake")
        with patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ):
            import_answering_gate(svc, pdf, refresh=False)

    accounts = db.execute(
        "SELECT DISTINCT account_id FROM app.account_links "
        "WHERE status = 'accepted' AND ref_kind = 'source_native' "
        "AND source_type = 'pdf'"
    ).fetchall()
    assert len(accounts) == 1, (
        f"one card resolved to {len(accounts)} accounts across two statements"
    )


@pytest.mark.integration
def test_pdf_batch_surfaces_prior_partial_account_before_refresh(
    db: Database, tmp_path: Path
) -> None:
    """Later statements see accounts minted earlier in the unrefreshed batch."""
    doc = _make_doc(text_lines=_standard_text_lines(), tables=[_standard_table()])
    january = tmp_path / "january.pdf"
    january.write_bytes(b"%PDF-1.4 january rendering")
    february = tmp_path / "february.pdf"
    february.write_bytes(b"%PDF-1.4 february rendering")

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        result = ImportService(db).import_files([january, february], refresh=False)

    assert [row.status for row in result.per_file] == [
        "imported",
        "confirmation_required",
    ]
    [created] = result.per_file[0].accounts_created
    payload = result.per_file[1].confirmation_payload
    assert payload is not None
    [proposal] = cast(list[dict[str, Any]], payload["account_proposals"])
    [candidate] = proposal["candidates"]
    assert (candidate["account_id"], candidate["signal"]) == (
        created.account_id,
        "institution_last4",
    )
    assert candidate["display_name"]


@pytest.mark.integration
def test_pdf_batch_rejects_mask_only_identity_before_loading(
    db: Database, tmp_path: Path
) -> None:
    doc = _make_doc(
        text_lines=[
            line.replace("Account Number: 1234", "Account Number: XXXX")
            for line in _standard_text_lines()
        ],
        tables=[_standard_table()],
    )
    january = tmp_path / "january.pdf"
    january.write_bytes(b"%PDF-1.4 january rendering")
    february = tmp_path / "february.pdf"
    february.write_bytes(b"%PDF-1.4 february rendering")

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        result = ImportService(db).import_files([january, february], refresh=False)

    assert [row.status for row in result.per_file] == [
        "confirmation_required",
        "confirmation_required",
    ]
    assert _count(db, "SELECT COUNT(*) FROM raw.tabular_transactions") == 0
    assert _count(db, "SELECT COUNT(*) FROM raw.import_log") == 0
    assert _count(db, "SELECT COUNT(*) FROM app.account_links") == 0


@pytest.mark.integration
def test_regenerated_pdf_reuses_transactions_when_document_bytes_change(
    db: Database, tmp_path: Path
) -> None:
    """Document-native idempotency must not leak into transaction identity."""
    doc = _make_doc(
        text_lines=[
            line.replace("Account Number: 1234", "Account Number: 001234567890")
            for line in _standard_text_lines()
        ]
        + ["Routing Number: 021000021"],
        tables=[_standard_table()],
    )
    svc = ImportService(db)
    pdf = tmp_path / "statement.pdf"

    pdf.write_bytes(b"%PDF-1.4 first rendering")
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        first = svc.import_file(pdf, refresh=False)

    pdf.write_bytes(b"%PDF-1.4 regenerated rendering")
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        second = svc.import_file(pdf, refresh=False)

    def transaction_ids(import_id: str | None) -> list[str]:
        assert import_id is not None
        return [
            str(row[0])
            for row in db.execute(
                "SELECT transaction_id FROM raw.tabular_transactions "
                "WHERE import_id = ? ORDER BY row_number",
                [import_id],
            ).fetchall()
        ]

    assert transaction_ids(first.import_id)
    assert transaction_ids(second.import_id) == []
    assert second.transactions == 0
    assert db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone() == (2,)


@pytest.mark.integration
def test_reimport_retires_pre_document_identity_pdf_transaction_hashes(
    db: Database, tmp_path: Path
) -> None:
    """A same-path post-upgrade reimport must not double-count legacy PDF rows."""
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    doc = _standard_doc()
    svc, pdf = _service_with_fake_pdf(db, doc, tmp_path)
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.dim_accounts "
        "(account_id, display_name, institution_slug, last_four) "
        "VALUES ('acct_legacy_pdf', 'Chase account', 'chase', '1234')"
    )
    AccountLinksRepo(db).insert(
        link_id="legacy_pdf_link",
        account_id="acct_legacy_pdf",
        ref_kind="source_native",
        ref_value="unknown_1234",
        source_type="pdf",
        source_origin="unknown",
        decided_by="auto",
        actor="system",
    )
    period = "2024-01-01-2024-01-31"
    for row_number, (day, description, amount) in enumerate(
        (("2024-01-15", "Coffee Shop", "-50.00"), ("2024-01-20", "Paycheck", "150.00")),
        start=1,
    ):
        content_key = f"{period}|{day}|{amount}|0|0|{description}|unknown_1234"
        transaction_id = f"pdf_{hashlib.sha256(content_key.encode()).hexdigest()[:16]}"
        db.execute(
            "INSERT INTO raw.tabular_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, "
            "source_file, source_type, source_origin, import_id, row_number) "
            "VALUES (?, 'unknown_1234', ?, ?, ?, ?, 'pdf', 'unknown', "
            "'legacy_import', ?)",
            [
                transaction_id,
                day,
                amount,
                description,
                str(pdf),
                row_number,
            ],
        )

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        svc.import_file(pdf, refresh=False)
    [proposal] = exc.value.outcome.account_proposals
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        result = svc.import_file(
            pdf,
            refresh=False,
            account_bindings={proposal["source_account_key"]: "acct_legacy_pdf"},
        )

    assert result.transactions == 0
    assert db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone() == (2,)


@pytest.mark.integration
def test_reimport_keeps_new_rows_beside_superseded_legacy_pdf_rows(
    db: Database, tmp_path: Path
) -> None:
    """Legacy retirement is per row; it cannot discard new statement content."""
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    doc = _standard_doc()
    svc, pdf = _service_with_fake_pdf(db, doc, tmp_path)
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.dim_accounts "
        "(account_id, display_name, institution_slug, last_four) "
        "VALUES ('acct_legacy_pdf', 'Chase account', 'chase', '1234')"
    )
    AccountLinksRepo(db).insert(
        link_id="legacy_pdf_partial_link",
        account_id="acct_legacy_pdf",
        ref_kind="source_native",
        ref_value="unknown_1234",
        source_type="pdf",
        source_origin="unknown",
        decided_by="auto",
        actor="system",
    )
    content_key = "2024-01-01-2024-01-31|2024-01-15|-50.00|0|0|Coffee Shop|unknown_1234"
    transaction_id = f"pdf_{hashlib.sha256(content_key.encode()).hexdigest()[:16]}"
    db.execute(
        "INSERT INTO raw.tabular_transactions "
        "(transaction_id, account_id, transaction_date, amount, description, "
        "source_file, source_type, source_origin, import_id, row_number) "
        "VALUES (?, 'unknown_1234', '2024-01-15', -50, 'Coffee Shop', ?, "
        "'pdf', 'unknown', 'legacy_import', 1)",
        [transaction_id, str(pdf)],
    )

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        svc.import_file(pdf, refresh=False)
    [proposal] = exc.value.outcome.account_proposals
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        result = svc.import_file(
            pdf,
            refresh=False,
            account_bindings={proposal["source_account_key"]: "acct_legacy_pdf"},
        )

    assert result.transactions == 1
    assert db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone() == (2,)


@pytest.mark.integration
def test_filename_alias_collision_cannot_retire_account_identifier_rows(
    db: Database, tmp_path: Path
) -> None:
    """An unproven filename key cannot suppress a current account's transaction."""
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    doc = _make_doc(
        text_lines=[
            line.replace("Account Number: 1234", "Account Number: 2024")
            for line in _standard_text_lines()
        ],
        tables=[_standard_table()],
    )
    svc, pdf = _service_with_fake_pdf(db, doc, tmp_path)
    create_core_tables(db)
    db.execute(
        "INSERT INTO core.dim_accounts "
        "(account_id, display_name, institution_slug, last_four) "
        "VALUES ('acct_existing', 'Chase account', 'chase', '2024')"
    )
    AccountLinksRepo(db).insert(
        link_id="filename_alias_collision",
        account_id="acct_existing",
        ref_kind="source_native",
        ref_value="chase_2024",
        source_type="pdf",
        source_origin="chase",
        decided_by="auto",
        actor="system",
    )
    content_key = "2024-01-01-2024-01-31|2024-01-15|-50.00|0|0|Coffee Shop|chase_2024"
    transaction_id = f"pdf_{hashlib.sha256(content_key.encode()).hexdigest()[:16]}"
    db.execute(
        "INSERT INTO raw.tabular_transactions "
        "(transaction_id, account_id, transaction_date, amount, description, "
        "source_file, source_type, source_origin, import_id, row_number) "
        "VALUES (?, 'chase_2024', '2024-01-15', -50, 'Coffee Shop', "
        "'/unrelated/chase_2024.pdf', 'pdf', 'chase', 'legacy_import', 1)",
        [transaction_id],
    )

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        svc.import_file(pdf, refresh=False)
    [proposal] = exc.value.outcome.account_proposals
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        result = svc.import_file(
            pdf,
            refresh=False,
            account_bindings={proposal["source_account_key"]: "acct_existing"},
        )

    assert result.transactions == 2
    assert db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone() == (3,)


@pytest.mark.integration
def test_reimport_after_account_merge_keeps_existing_pdf_transaction_ids(
    db: Database, tmp_path: Path
) -> None:
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    doc = _make_doc(
        text_lines=[
            line.replace("Account Number: 1234", "Account Number: 001234567890")
            for line in _standard_text_lines()
        ]
        + ["Routing Number: 021000021"],
        tables=[_standard_table()],
    )
    svc, pdf = _service_with_fake_pdf(db, doc, tmp_path)
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        first = svc.import_file(pdf, refresh=False)
    [original_account] = first.accounts_created
    links = db.execute(
        "SELECT link_id FROM app.account_links "
        "WHERE status = 'accepted' AND account_id = ?",
        [original_account.account_id],
    ).fetchall()
    for (link_id,) in links:
        AccountLinksRepo(db).repoint(
            link_id=str(link_id),
            new_account_id="acct_merged_target",
            decided_by="user",
            actor="system",
        )

    moved_pdf = tmp_path / "moved-regenerated-statement.pdf"
    moved_pdf.write_bytes(b"%PDF-1.4 regenerated rendering")
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        repeated = svc.import_file(moved_pdf, refresh=False)

    assert repeated.transactions == 0
    assert db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone() == (2,)


@pytest.mark.integration
def test_distinct_full_pdf_account_numbers_with_same_last_four_do_not_collide(
    db: Database, tmp_path: Path
) -> None:
    """Validated routing and complete identifiers distinguish similar accounts."""
    svc = ImportService(db)
    account_numbers = ("001234567890", "991234567890")

    first_doc = _make_doc(
        text_lines=[
            line.replace(
                "Account Number: 1234", f"Account Number: {account_numbers[0]}"
            )
            for line in _standard_text_lines()
        ]
        + ["Routing Number: 021000021"],
        tables=[_standard_table()],
    )
    first_pdf = tmp_path / "first.pdf"
    first_pdf.write_bytes(b"%PDF-1.4 first account")
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=first_doc
    ):
        first_result = svc.import_file(first_pdf, refresh=False)
    assert first_result.transactions == 2
    first_link = db.execute(
        "SELECT account_id FROM app.account_links "
        "WHERE ref_kind = 'source_native' AND status = 'accepted'"
    ).fetchone()
    assert first_link is not None
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts "  # noqa: S608  # test fixture
        "(account_id, display_name, institution_slug, last_four) "
        "VALUES (?, ?, ?, ?)",
        [first_link[0], "First Chase account", "chase", "7890"],
    )

    second_doc = _make_doc(
        text_lines=[
            line.replace(
                "Account Number: 1234", f"Account Number: {account_numbers[1]}"
            )
            for line in _standard_text_lines()
        ]
        + ["Routing Number: 021000021"],
        tables=[_standard_table()],
    )
    second_pdf = tmp_path / "second.pdf"
    second_pdf.write_bytes(b"%PDF-1.4 second account")
    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=second_doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        svc.import_file(second_pdf, refresh=False)

    [proposal] = exc.value.outcome.account_proposals
    assert proposal["source_account_key"].startswith("pdf_doc_")
    assert account_numbers[1] not in proposal["source_account_key"]
    assert proposal["candidates"]

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=second_doc,
    ):
        second_result = svc.import_file(
            second_pdf,
            refresh=False,
            account_bindings={proposal["source_account_key"]: "new"},
        )
    assert second_result.transactions == 2

    full_number_links = db.execute(
        "SELECT ref_value, account_id FROM app.account_links "
        "WHERE ref_kind = 'full_number' AND status = 'accepted'"
    ).fetchall()
    assert {row[0] for row in full_number_links} == {
        f"021000021:{number}" for number in account_numbers
    }
    assert len({row[1] for row in full_number_links}) == 2


def test_account_id_override_pins_identity_through_the_resolver(
    db: Database, tmp_path: Path
) -> None:
    """`--account-id` must bind explicitly rather than mint from the statement.

    This branch skips mask derivation entirely (`masked_acct = None`), so it
    reaches the resolver with `last_four=None` and an explicit binding. It
    previously short-circuited before any resolver contact existed, so nothing
    covered it once the resolver call went in.
    """
    doc = _make_doc(text_lines=_standard_text_lines(), tables=[_standard_table()])
    svc = ImportService(db)
    pdf = tmp_path / "statement.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        result = import_answering_gate(
            svc, pdf, refresh=False, account_id="my_pinned_account"
        )

    assert result.transactions == 2
    # The rows carry the document's OWN key. A pin says which account the
    # statement belongs to; it does not rename the statement.
    raw_ids = [
        str(r[0])
        for r in db.execute(
            "SELECT DISTINCT account_id FROM raw.tabular_transactions "
            "WHERE source_type = 'pdf'"
        ).fetchall()
    ]
    [raw_id] = raw_ids
    assert raw_id.startswith("pdf_doc_"), raw_id
    # And the explicit binding is registered against that key, so a later
    # statement of the same card adopts it instead of minting beside it.
    link = db.execute(
        "SELECT account_id FROM app.account_links "
        "WHERE status = 'accepted' AND ref_kind = 'source_native' "
        "AND source_type = 'pdf' AND ref_value = ?",
        [raw_id],
    ).fetchone()
    assert link is not None
    # The explicit binding is honoured rather than minting a fresh canonical id
    # beside it -- one link, from the document's key to the pinned account.
    assert str(link[0]) == "my_pinned_account"


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
        result = import_answering_gate(svc, fake_pdf, refresh=False, save_format=False)

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
        import_answering_gate(svc, fake_pdf, refresh=False)

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
        result = import_answering_gate(svc, fake_pdf, refresh=False)
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


# ---------------------------------------------------------------------------
# Test 12: Scanned / image-only PDF (no text layer) — explicit unsupported (Req 5)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_pdf_scanned_no_text_layer_raises_unsupported(
    db: Database, tmp_path: Path
) -> None:
    """A scanned/image-only PDF (no text layer) raises an explicit unsupported error.

    Nothing to structure, nothing to seed, and the text bridge can't read a page
    image — so the import surfaces a clear 'needs a vision-capable backend'
    UserError (Req 5 no-agent degradation) rather than a generic 'No tables
    extracted' failure or a silent empty seed. Raised before begin_import, so no
    import_log row or orphan seed view is left behind.
    """
    from moneybin import error_codes
    from moneybin.errors import UserError
    from moneybin.metrics.registry import PDF_IMPORT_TOTAL

    scanned = _make_doc()  # text_lines=[] and tables=[] → no extractable text layer
    svc, fake_pdf = _service_with_fake_pdf(db, scanned, tmp_path)

    before = PDF_IMPORT_TOTAL.labels(
        outcome="unsupported", rung="deterministic"
    )._value.get()  # type: ignore[reportPrivateUsage]
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=scanned,
    ):
        with pytest.raises(UserError) as exc_info:
            import_answering_gate(svc, fake_pdf, refresh=False)

    assert exc_info.value.code == error_codes.IMPORT_PDF_NO_TEXT_LAYER
    assert "vision-capable" in exc_info.value.message
    after = PDF_IMPORT_TOTAL.labels(
        outcome="unsupported", rung="deterministic"
    )._value.get()  # type: ignore[reportPrivateUsage]
    assert after == before + 1

    # Raised before begin_import — no import_log row, no orphan seed view.
    log_rows = db.execute("SELECT COUNT(*) FROM raw.import_log").fetchone()
    assert log_rows is not None and log_rows[0] == 0
    views = db.execute(
        "SELECT COUNT(*) FROM duckdb_views() "
        "WHERE schema_name = 'raw' AND view_name LIKE 'pdf_%'"
    ).fetchone()
    assert views is not None and views[0] == 0


# ---------------------------------------------------------------------------
# Tests 13-17: the sign-convention gate (auto-derived inversion needs a confirm)
# ---------------------------------------------------------------------------


def _amounts(db: Database) -> list[Decimal]:
    return sorted(
        r[0]
        for r in db.execute(
            "SELECT amount FROM raw.tabular_transactions WHERE source_type = 'pdf'"
        ).fetchall()
    )


def _row_count(db: Database) -> int:
    row = db.execute("SELECT COUNT(*) FROM raw.tabular_transactions").fetchone()
    assert row is not None
    return int(row[0])


@pytest.mark.integration
def test_card_statement_import_requires_confirmation(
    db: Database, tmp_path: Path
) -> None:
    """An auto-derived inversion never lands rows unratified."""
    pdf = write_card_statement_pdf(tmp_path)
    svc = ImportService(db)

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        import_answering_gate(svc, pdf, refresh=False)

    outcome = exc.value.outcome
    assert outcome.channel == "pdf"
    assert outcome.reason == "sign_convention"
    proposed = outcome.proposed
    assert isinstance(proposed, SignConventionProposal)
    assert proposed.sign_convention == "negative_is_income"
    assert "minimum payment" in proposed.evidence
    # The samples show the flip concretely: printed +150.00 → recorded -150.00.
    assert proposed.sample_rows
    assert proposed.sample_rows[0]["as_printed"] == "150.00"
    assert proposed.sample_rows[0]["as_recorded"] == "-150.00"
    # `medium`, never `high`: `high` is the tier an agent may self-accept at.
    assert outcome.confidence.tier == "medium"

    assert _row_count(db) == 0


@pytest.mark.integration
def test_confirmed_card_statement_records_charges_as_expenses(
    db: Database, tmp_path: Path
) -> None:
    """The whole point: a +150 charge is an EXPENSE; a -50 payment is a credit."""
    pdf = write_card_statement_pdf(tmp_path)
    svc = ImportService(db)

    import_answering_gate(svc, pdf, refresh=False, confirm=True)

    assert _amounts(db) == [Decimal("-150.00"), Decimal("50.00")]


@pytest.mark.integration
def test_sign_override_overrules_the_card_detector(
    db: Database, tmp_path: Path
) -> None:
    """A false-positive detection must be recoverable in-band, not by editing the PDF."""
    pdf = write_card_statement_pdf(tmp_path)
    svc = ImportService(db)

    import_answering_gate(svc, pdf, refresh=False, sign="negative_is_expense")

    assert _amounts(db) == [Decimal("-50.00"), Decimal("150.00")]  # as printed


@pytest.mark.integration
def test_replayed_card_format_needs_no_second_confirmation(
    db: Database, tmp_path: Path
) -> None:
    """The confirm is once per FORMAT, not once per statement."""
    svc = ImportService(db)
    import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="01"),
        refresh=False,
        confirm=True,
        save_format=True,
    )

    # Second month, same layout -> replays the saved recipe, no confirm.
    import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="02"), refresh=False
    )

    assert _row_count(db) == 4
    # Both statements inverted — every charge an expense, every payment a credit.
    assert _amounts(db) == [
        Decimal("-150.00"),
        Decimal("-150.00"),
        Decimal("50.00"),
        Decimal("50.00"),
    ]


@pytest.mark.integration
def test_checking_statement_imports_without_a_sign_confirm(
    db: Database, tmp_path: Path
) -> None:
    """The gate's precision guard: the card twin with no disclosures never asks.

    Same issuer, same columns, same balances, same two amounts — only the
    disclosures differ. A gate that fired here would invert a real checking
    ledger (every paycheck an expense), which is the cost this test pins down.
    """
    pdf = write_checking_statement_pdf(tmp_path)
    svc = ImportService(db)

    result = import_answering_gate(svc, pdf, refresh=False)

    assert result.transactions == 2
    assert _amounts(db) == [Decimal("-50.00"), Decimal("150.00")]  # as printed


@pytest.mark.integration
def test_sign_override_shape_mismatch_names_the_shape_the_recipe_extracts(
    db: Database, tmp_path: Path
) -> None:
    """The shape-guard error must name what the recipe ACTUALLY extracts.

    The card statement's recipe extracts a single amount column. Overriding
    with `split_debit_credit` (a shape this recipe does not have) must fail
    with a message naming "single amount column" — not "debit/credit pair",
    which is what an inverted ternary said before this fix. This is the
    user's only in-band recovery path from a false-positive card detection;
    a misdirecting message sends them to fix the wrong thing.
    """
    pdf = write_card_statement_pdf(tmp_path)
    svc = ImportService(db)

    with pytest.raises(UserError) as exc:
        import_answering_gate(svc, pdf, refresh=False, sign="split_debit_credit")

    assert exc.value.code == "import_invalid_sign_convention"
    assert "single amount column" in exc.value.message
    assert "debit/credit pair" not in exc.value.message
    assert _row_count(db) == 0


@pytest.mark.integration
def test_sign_override_replays_without_asking_again(
    db: Database, tmp_path: Path
) -> None:
    """A `sign=` override must survive into every future statement of the format.

    The override's whole purpose is recovering from a false-positive card
    detection — and a false positive is, by construction, a document that CARRIES
    card markers. Without ``sign_ratified`` on the recipe, the polarity guard
    disowns the saved recipe on the next statement (a ``negative_is_expense``
    recipe replayed onto a marker-bearing document), derivation re-runs,
    re-proposes the inversion, and the gate raises again — forever. The user would
    have to re-override every month and the saved format would be dead weight.
    """
    svc = ImportService(db)

    first = import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="01"),
        refresh=False,
        sign="negative_is_expense",
    )
    # First contact: the user typed `sign=` themselves — nothing to re-surface.
    assert first.sign_override_replayed is False

    # Next month, same layout, no flags. The saved override must replay.
    second = import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="02"), refresh=False
    )

    assert second.transactions == 2
    # Both statements loaded as printed — the override held on the replay.
    assert _amounts(db) == [
        Decimal("-50.00"),
        Decimal("-50.00"),
        Decimal("150.00"),
        Decimal("150.00"),
    ]
    # A durable override that acts invisibly is exactly the magic this codebase
    # refuses: the replay bypasses the detector, so the user is told it happened.
    assert second.sign_override_replayed is True


@pytest.mark.integration
def test_confirm_does_not_ratify_the_sign_convention(
    db: Database, tmp_path: Path
) -> None:
    """`confirm=True` agrees with the detector; it must NOT disarm the polarity guard.

    Ratifying "yes, this IS a card" needs no guard bypass — the marker check
    re-confirms that recipe on every replay of a real card. Setting
    ``sign_ratified`` here would instead strip the protection in the dangerous
    direction: a checking statement sharing this fingerprint (same issuer, same
    columns, same page count) would silently import every paycheck as an expense.
    """
    import json as _json

    svc = ImportService(db)
    import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="01"), refresh=False, confirm=True
    )

    row = db.execute("SELECT extraction_recipe FROM app.pdf_formats").fetchone()
    assert row is not None
    stored = _json.loads(row[0])
    assert stored["sign_convention"] == "negative_is_income"
    assert stored["sign_ratified"] is False

    # The card's fingerprint-identical twin: the guard must still refuse to replay
    # the card recipe onto it, so its rows land as printed.
    result = import_answering_gate(
        svc, write_checking_statement_pdf(tmp_path), refresh=False
    )

    assert result.sign_override_replayed is False
    assert _amounts(db) == [
        Decimal("-150.00"),  # card: +150.00 charge, inverted on confirm
        Decimal("-50.00"),  # checking: as printed, NOT inverted
        Decimal("50.00"),  # card: -50.00 payment, inverted on confirm
        Decimal("150.00"),  # checking: as printed, NOT inverted
    ]


@pytest.mark.integration
def test_sign_gate_metric_records_all_three_outcomes(
    db: Database, tmp_path: Path
) -> None:
    """PDF_SIGN_GATE_TOTAL bumps proposed/confirmed/overridden at their exits.

    The gate had zero telemetry before this fix — a false-positive card
    detection was invisible in aggregate. Drives all three real transitions
    (propose, override, confirm) end-to-end rather than asserting against the
    gate's internals directly.

    ``save_format=False`` on every call keeps each import a fresh
    auto-derivation: a saved recipe would make the third call a REPLAY
    (``is_auto_derived`` false), which returns before the ``confirmed`` bump
    and would make this test's third assertion fail for the wrong reason.
    """
    from moneybin.metrics.registry import PDF_SIGN_GATE_TOTAL

    def _count(outcome: str) -> float:
        return PDF_SIGN_GATE_TOTAL.labels(outcome=outcome)._value.get()  # type: ignore[reportPrivateUsage]

    svc = ImportService(db)

    proposed_before = _count("proposed")
    overridden_before = _count("overridden")
    confirmed_before = _count("confirmed")

    with pytest.raises(ImportConfirmationRequiredError):
        svc.import_file(
            write_card_statement_pdf(tmp_path, month="01"),
            refresh=False,
            save_format=False,
        )
    assert _count("proposed") == proposed_before + 1

    svc.import_file(
        write_card_statement_pdf(tmp_path, month="02"),
        refresh=False,
        sign="negative_is_expense",
        save_format=False,
    )
    assert _count("overridden") == overridden_before + 1

    svc.import_file(
        write_card_statement_pdf(tmp_path, month="01"),
        refresh=False,
        confirm=True,
        save_format=False,
    )
    assert _count("confirmed") == confirmed_before + 1


def _saved_pdf_format(db: Database) -> tuple[dict[str, Any], str, int]:
    """Return (stored recipe, sign_convention column, version) of the sole format."""
    import json as _json

    row = db.execute(
        "SELECT extraction_recipe, sign_convention, version FROM app.pdf_formats"
    ).fetchone()
    assert row is not None
    return _json.loads(row[0]), row[1], int(row[2])


@pytest.mark.integration
def test_a_corrected_sign_override_on_a_replay_persists_and_sticks(
    db: Database, tmp_path: Path
) -> None:
    """A ratified convention must be revocable — by the only tool the user has.

    The recipe is written on first contact only, so a `sign=` on a REPLAY used to
    correct that one import and silently revert the next month. With no delete or
    edit path for saved PDF formats (`import formats delete` speaks to the tabular
    table), a wrong first-contact ratification was permanent — while the CLI note
    told the user "Re-run with --sign to change it". This makes that true.
    """
    svc = ImportService(db)

    # 1. The user asserts the WRONG convention on first contact — and it sticks:
    #    the recipe is saved, and every future statement of this layout replays it.
    import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="01"),
        refresh=False,
        sign="negative_is_income",
    )
    assert _amounts(db) == [Decimal("-150.00"), Decimal("50.00")]  # inverted

    # 2. Next statement, corrected: `sign=` on a replay of the saved format.
    second = import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="02"),
        refresh=False,
        sign="negative_is_expense",
    )
    assert second.transactions == 2
    recipe, sign_column, version = _saved_pdf_format(db)
    assert recipe["sign_convention"] == "negative_is_expense"
    assert recipe["sign_ratified"] is True
    assert version == 2  # audited + undo-reversible, not a silent overwrite
    # `import formats show` reads the column, not the recipe — a stale column
    # would report the convention the user just corrected away from.
    assert sign_column == "negative_is_expense"

    # 3. A third statement of the same format, no flags. The correction — not the
    #    original ratification — must be what replays. (Fresh directory: the raw
    #    PK includes source_file, so rows land either way and the AMOUNTS are what
    #    discriminate a corrected replay from the stale inverted one.)
    third_dir = tmp_path / "later"
    third_dir.mkdir()
    third = import_answering_gate(
        svc, write_card_statement_pdf(third_dir), refresh=False
    )

    assert third.transactions == 2
    assert third.sign_override_replayed is True
    assert _amounts(db) == [
        Decimal("-150.00"),  # statement 1, imported under the wrong ratification
        Decimal("-50.00"),  # statement 2, corrected
        Decimal("-50.00"),  # statement 3, corrected convention replayed
        Decimal("50.00"),  # statement 1
        Decimal("150.00"),  # statement 2
        Decimal("150.00"),  # statement 3
    ]


@pytest.mark.integration
def test_sign_override_typed_on_a_replay_is_not_reported_as_a_saved_replay(
    db: Database, tmp_path: Path
) -> None:
    """The note must not tell the user a SAVED override acted when they just typed one.

    The gate sets ``sign_ratified`` in the same call that accepts a `sign=`, so a
    ratified-flag check alone is true on the very invocation supplying it — and the
    user gets told the convention came from a saved override they are in fact
    providing right now.
    """
    svc = ImportService(db)
    import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="01"),
        refresh=False,
        sign="negative_is_income",
    )

    second = import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="02"),
        refresh=False,
        sign="negative_is_expense",
    )

    assert second.sign_override_replayed is False


@pytest.mark.integration
def test_repeating_the_same_sign_override_does_not_bump_the_version(
    db: Database, tmp_path: Path
) -> None:
    """A `--sign` re-typed out of habit is a no-op, not a version + audit row.

    ``bump_version`` records the prior recipe for undo; bumping to an identical
    recipe would spend a version on an event whose before_value equals its
    after_value.
    """
    svc = ImportService(db)
    import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="01"),
        refresh=False,
        sign="negative_is_expense",
    )

    import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="02"),
        refresh=False,
        sign="negative_is_expense",
    )

    _, _, version = _saved_pdf_format(db)
    assert version == 1


@pytest.mark.integration
def test_a_redundant_sign_override_does_not_disarm_the_polarity_guard(
    db: Database, tmp_path: Path
) -> None:
    """A `sign=` that AGREES with the convention in force must NOT ratify.

    Ratifying on *any* `sign=` hands out the guard bypass for free. The user
    re-states the convention their card is already importing under — nothing to
    correct, nothing to bypass — and the saved recipe comes back ratified, so the
    polarity guard stands down forever. The next CHECKING statement that
    fingerprints identically (same issuer, same columns, same page count) then
    replays the card recipe and imports every paycheck as an expense: the exact
    corruption this gate exists to prevent, re-opened by a no-op flag.

    Same reasoning as ``confirm=True``, which declines to ratify for this reason.
    Only a DISAGREEING `sign=` needs the bypass.
    """
    svc = ImportService(db)

    # 1. A genuine card, confirmed. Saved: negative_is_income, guard armed.
    import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="01"), refresh=False, confirm=True
    )

    # 2. Next month's card, with the convention already in force re-typed.
    second = import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="02"),
        refresh=False,
        sign="negative_is_income",
    )
    assert second.transactions == 2

    recipe, sign_column, version = _saved_pdf_format(db)
    assert recipe["sign_convention"] == "negative_is_income"
    assert recipe["sign_ratified"] is False  # the guard survives the no-op flag
    assert sign_column == "negative_is_income"
    assert version == 1  # nothing changed — no bump, no audit row

    # 3. The card's fingerprint-identical checking twin. The guard must still
    #    refuse to replay the card recipe onto it: its rows land as printed.
    third = import_answering_gate(
        svc, write_checking_statement_pdf(tmp_path), refresh=False
    )

    assert third.sign_override_replayed is False
    assert _amounts(db) == [
        Decimal("-150.00"),  # card 01: +150.00 charge, inverted
        Decimal("-150.00"),  # card 02: inverted by the saved recipe
        Decimal("-50.00"),  # checking: as printed, NOT inverted
        Decimal("50.00"),  # card 01: -50.00 payment, inverted
        Decimal("50.00"),  # card 02
        Decimal("150.00"),  # checking: as printed, NOT inverted
    ]


@pytest.mark.integration
def test_no_save_format_does_not_rewrite_the_saved_recipe_on_a_sign_override(
    db: Database, tmp_path: Path
) -> None:
    """`save_format=False` must suppress the replay re-persist, not just save_new.

    ``--no-save-format`` is what a user reaches for on a one-off or sensitive
    statement: it promises the import will not teach ``app.pdf_formats`` anything.
    A `sign=` on that import applies to THIS statement only — it must not flip the
    saved recipe's convention, bump its version, or leave an audit row.
    """
    svc = ImportService(db)
    import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="01"), refresh=False, confirm=True
    )

    import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="02"),
        refresh=False,
        sign="negative_is_expense",
        save_format=False,
    )

    recipe, sign_column, version = _saved_pdf_format(db)
    assert recipe["sign_convention"] == "negative_is_income"
    assert recipe["sign_ratified"] is False
    assert sign_column == "negative_is_income"
    assert version == 1
    bump = db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'pdf_format.bump_version'"
    ).fetchone()
    assert bump is not None and bump[0] == 0

    # The override still governed the import it was typed on.
    assert _amounts(db) == [
        Decimal("-150.00"),  # month 01, confirmed as a card → inverted
        Decimal("-50.00"),  # month 02, overridden → as printed
        Decimal("50.00"),  # month 01
        Decimal("150.00"),  # month 02
    ]


# ---------------------------------------------------------------------------
# Test 18-19: a confirmed card statement types the account 'credit'
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_confirmed_card_statement_types_the_account_credit(
    db: Database, tmp_path: Path
) -> None:
    """A confirmed card is a FACT about the account — keep it, don't discard it."""
    svc = ImportService(db)
    import_answering_gate(
        svc, write_card_statement_pdf(tmp_path), refresh=False, confirm=True
    )

    row = db.execute(
        "SELECT account_type FROM raw.tabular_accounts WHERE source_type = 'pdf'"
    ).fetchone()
    assert row is not None
    assert row[0] == "credit"


@pytest.mark.integration
def test_checking_statement_leaves_account_type_null(
    db: Database, tmp_path: Path
) -> None:
    """We only assert a type we actually established. No guessing."""
    svc = ImportService(db)
    import_answering_gate(svc, write_checking_statement_pdf(tmp_path), refresh=False)

    row = db.execute(
        "SELECT account_type FROM raw.tabular_accounts WHERE source_type = 'pdf'"
    ).fetchone()
    assert row is not None
    assert row[0] is None


# ---------------------------------------------------------------------------
# Test 20: transaction ids are stable across sign conventions
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_transaction_ids_are_stable_across_sign_conventions(
    db: Database, tmp_path: Path
) -> None:
    """Flipping the convention must NOT rotate ids — else a re-import double-counts.

    Ids are keyed on the RAW pre-normalization ``amount``/``debit``/``credit``
    cell values (import_service.py, ``content_key`` in ``_import_pdf_transactions``
    around line 2768-2779) — never on the sign-normalized ``amount`` column a
    convention flip changes. ``save_format=False`` on both calls keeps each
    import first-contact: the first asserts ``negative_is_expense`` via an
    explicit override, the second confirms the card detector's own
    ``negative_is_income`` proposal — two genuinely different conventions, not
    one convention replayed twice. (Without ``save_format=False`` the first
    call's ratified override would persist to ``app.pdf_formats`` and the
    second call would replay it unchanged, silently testing one convention
    against itself.)
    """
    svc = ImportService(db)
    pdf = write_card_statement_pdf(tmp_path)

    import_answering_gate(
        svc, pdf, refresh=False, sign="negative_is_expense", save_format=False
    )
    as_expense = {
        r[0]
        for r in db.execute(
            "SELECT transaction_id FROM raw.tabular_transactions"
        ).fetchall()
    }
    db.execute("DELETE FROM raw.tabular_transactions")

    import_answering_gate(svc, pdf, refresh=False, confirm=True, save_format=False)
    as_income = {
        r[0]
        for r in db.execute(
            "SELECT transaction_id FROM raw.tabular_transactions"
        ).fetchall()
    }

    assert as_expense == as_income
    assert len(as_expense) == 2  # sanity: the fixture's two rows, not an empty set


@pytest.mark.integration
def test_a_saved_recipe_that_stops_reconciling_is_repaired_and_versioned(
    db: Database, tmp_path: Path
) -> None:
    """A recipe frozen under older derivation logic is re-derived and persisted.

    The service half of self-healing replay. Routing proves the fresh recipe
    reconciles; the service must then write it back via ``bump_version`` — a
    repair that lands the rows but leaves ``app.pdf_formats`` still holding the
    broken recipe would re-break on the very next statement of this layout.
    """
    svc = ImportService(db)
    import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="01"), refresh=False, confirm=True
    )
    recipe, _, version = _saved_pdf_format(db)
    assert version == 1

    # Freeze a recipe that can no longer read the whole statement: this Amount
    # pattern requires a leading "-", so the +150.00 charge stops matching and
    # the rows come up short of the balance delta. Stands in for any derivation
    # bug fixed after a recipe was already persisted.
    name_row = db.execute("SELECT name FROM app.pdf_formats").fetchone()
    assert name_row is not None
    broken = {
        **recipe,
        "fields": [
            {**f, "pattern": r"-\$?[\d,]+\.\d{2}"} if f["name"] == "Amount" else f
            for f in recipe["fields"]
        ],
    }
    PdfFormatsRepo(db).bump_version(
        name_row[0], broken, reason="test: simulate a stale saved recipe", actor="test"
    )

    result = import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="02"), refresh=False
    )

    # The statement lands in full rather than seeding.
    assert result.transactions == 2

    healed, sign_column, healed_version = _saved_pdf_format(db)
    amount_pattern = next(
        f["pattern"] for f in healed["fields"] if f["name"] == "Amount"
    )
    assert amount_pattern != r"-\$?[\d,]+\.\d{2}"  # repaired, not left broken
    # 1 (first-contact save_new) + 1 (the corruption above) + 1 (the repair).
    assert healed_version == 3
    # The repair fixes a pattern and nothing else: polarity is untouched.
    assert healed["sign_convention"] == "negative_is_income"
    assert sign_column == "negative_is_income"


@pytest.mark.integration
def test_no_save_format_lands_the_rows_but_does_not_persist_the_repair(
    db: Database, tmp_path: Path
) -> None:
    """`save_format=False` suppresses the repair write, not the import itself.

    Same promise the flag makes everywhere else: this statement will not teach
    ``app.pdf_formats`` anything. The user still gets their rows.
    """
    svc = ImportService(db)
    import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="01"), refresh=False, confirm=True
    )
    recipe, _, _ = _saved_pdf_format(db)
    name_row = db.execute("SELECT name FROM app.pdf_formats").fetchone()
    assert name_row is not None
    broken = {
        **recipe,
        "fields": [
            {**f, "pattern": r"-\$?[\d,]+\.\d{2}"} if f["name"] == "Amount" else f
            for f in recipe["fields"]
        ],
    }
    PdfFormatsRepo(db).bump_version(
        name_row[0], broken, reason="test: simulate a stale saved recipe", actor="test"
    )

    result = import_answering_gate(
        svc,
        write_card_statement_pdf(tmp_path, month="02"),
        refresh=False,
        save_format=False,
    )

    assert result.transactions == 2  # the rows still land
    _, _, version = _saved_pdf_format(db)
    assert version == 2  # save_new + the corruption; the repair was NOT written


@pytest.mark.integration
def test_a_repair_that_un_inverts_the_ledger_is_gated(
    db: Database, tmp_path: Path
) -> None:
    """A re-derived polarity change must reach a human in the DANGEROUS direction.

    The gate's own short-circuit only proposes for `negative_is_income`, so an
    income -> expense repair would otherwise apply silently, un-inverting a
    convention a human ratified, with no prompt and no trace but a log line.
    `rederived_from_sign` is what makes the gate look.

    Uses the fixtures' matched pair: the card and checking statements share a
    layout fingerprint and differ only in the card disclosures, so the saved
    card recipe replays onto the checking twin and re-derivation reads the
    twin as negative_is_expense.
    """
    svc = ImportService(db)
    import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="01"), refresh=False, confirm=True
    )
    recipe, _, _ = _saved_pdf_format(db)
    name_row = db.execute("SELECT name FROM app.pdf_formats").fetchone()
    assert name_row is not None
    # Ratified so the polarity guard defers and the replay actually reaches
    # reconciliation; the broken Amount pattern is what makes it fail there.
    broken = {
        **recipe,
        "sign_ratified": True,
        "fields": [
            {**f, "pattern": r"-\$?[\d,]+\.\d{2}"} if f["name"] == "Amount" else f
            for f in recipe["fields"]
        ],
    }
    PdfFormatsRepo(db).bump_version(
        name_row[0], broken, reason="test: simulate a stale saved recipe", actor="test"
    )

    with pytest.raises(ImportConfirmationRequiredError) as exc:
        import_answering_gate(
            svc, write_checking_statement_pdf(tmp_path), refresh=False
        )

    assert exc.value.outcome.reason == "sign_convention"
    # The direction has to travel with the proposal. Every surface renders the
    # first-contact credit-card framing when it's absent — which for THIS
    # direction describes `--confirm` as ratifying a card convention when it
    # actually accepts the as-printed one, and prints no command that keeps the
    # convention already in force. Carrying the prior is what makes the guidance
    # answerable rather than merely present.
    proposed = exc.value.outcome.proposed
    assert isinstance(proposed, SignConventionProposal)
    assert proposed.sign_convention == "negative_is_expense"
    assert proposed.prior_sign_convention == "negative_is_income"
    # Nothing from the checking statement landed while the flip is unratified.
    assert _amounts(db) == [Decimal("-150.00"), Decimal("50.00")]


@pytest.mark.integration
def test_confirming_a_sign_flipping_repair_lets_it_land(
    db: Database, tmp_path: Path
) -> None:
    """The gate must be answerable — otherwise the statement is unimportable.

    This is the whole point of surfacing rather than refusing: before, no flag
    could authorize the repair because routing seeded and the gate ignores
    non-transaction decisions.
    """
    svc = ImportService(db)
    import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="01"), refresh=False, confirm=True
    )
    recipe, _, _ = _saved_pdf_format(db)
    name_row = db.execute("SELECT name FROM app.pdf_formats").fetchone()
    assert name_row is not None
    broken = {
        **recipe,
        "fields": [
            {**f, "pattern": r"-\$?[\d,]+\.\d{2}"} if f["name"] == "Amount" else f
            for f in recipe["fields"]
        ],
    }
    PdfFormatsRepo(db).bump_version(
        name_row[0], broken, reason="test: simulate a stale saved recipe", actor="test"
    )

    result = import_answering_gate(
        svc, write_card_statement_pdf(tmp_path, month="02"), refresh=False, confirm=True
    )

    assert result.transactions == 2


@pytest.mark.integration
def test_pdf_import_gates_account_identity_before_begin_import(
    db: Database,
    tmp_path: Path,
) -> None:
    """A PDF statement's account identity gets a pre-load confirm, like every channel.

    The gap PR #375 named and left open, and this session's primary target:
    ``_import_pdf_transactions`` called ``resolve()`` directly, so a statement
    minted a new account — or adopted an existing one on a weak
    institution+last4 signal — with nothing ever surfaced. A wrong silent *merge*
    is the expensive case: it is hard to notice and hard to undo, and it is
    exactly what the seeded twin below sets up.

    The gate belongs after the sign gate and before ``begin_import``, the
    position the sign gate already established: a confirmation is not a failed
    import, so it must not strand an ``import_log`` row in "importing" or leave
    a "failed" row for an import that never started.
    """
    _seed_chase_twin(db)
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        # Never the answering helper here: the gate itself is under test.
        svc.import_file(fake_pdf, refresh=False, confirm=True, actor_kind="human")

    outcome = exc.value.outcome
    assert outcome.reason == "account_confirmation"
    assert outcome.channel == "pdf"
    # The user binds an opaque document key; the partial account number is only
    # candidate evidence and never appears in the durable native key.
    [source_key] = [p["source_account_key"] for p in outcome.account_proposals]
    assert source_key.startswith("pdf_doc_")
    assert "1234" not in source_key
    # Nothing loaded, no batch opened, no link written, no recipe saved.
    assert _count(db, "SELECT COUNT(*) FROM raw.tabular_transactions") == 0
    assert _count(db, "SELECT COUNT(*) FROM raw.import_log") == 0
    assert _count(db, "SELECT COUNT(*) FROM app.account_links") == 0
    assert _count(db, "SELECT COUNT(*) FROM app.pdf_formats") == 0


@pytest.mark.integration
def test_partial_pdf_confirmation_reports_ledger_overlap(
    db: Database,
    tmp_path: Path,
) -> None:
    _seed_chase_twin(db)
    for transaction_id, transaction_date, amount in (
        ("existing-coffee", "2024-01-15", "-50.00"),
        ("existing-paycheck", "2024-01-21", "150.00"),
    ):
        db.execute(
            "INSERT INTO core.fct_transactions "  # noqa: S608  # test fixture
            "(transaction_id, account_id, transaction_date, amount, currency_code) "
            "VALUES (?, ?, ?, ?, ?)",
            [transaction_id, "acct_existing01", transaction_date, amount, None],
        )
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        svc.import_file(fake_pdf, refresh=False)

    [candidate] = exc.value.outcome.account_proposals[0]["candidates"]
    assert candidate.get("overlap_matched") == 2
    assert candidate.get("overlap_comparable") == 2
    assert candidate.get("overlap_window_start") == "2024-01-15"
    assert candidate.get("overlap_window_end") == "2024-01-20"


@pytest.mark.integration
def test_pdf_gate_observes_the_measured_overlap_of_the_candidate_it_surfaces(
    db: Database,
    tmp_path: Path,
) -> None:
    """The gate feeds the histogram that varies with the accounts in front of it.

    Confidence was a per-signal constant, so a histogram of it reported which
    rungs fired and never whether a proposal was any good. The overlap ratio is
    fed instead, at the one point overlap is measured. Same fixture as
    ``test_partial_pdf_confirmation_reports_ledger_overlap``: two of the
    statement's rows are already held by the twin and both match, so one
    candidate is surfaced and observed at a ratio of 2/2.
    """
    from prometheus_client import REGISTRY

    _seed_chase_twin(db)
    for transaction_id, transaction_date, amount in (
        ("existing-coffee", "2024-01-15", "-50.00"),
        ("existing-paycheck", "2024-01-21", "150.00"),
    ):
        db.execute(
            "INSERT INTO core.fct_transactions "  # noqa: S608  # test fixture
            "(transaction_id, account_id, transaction_date, amount, currency_code) "
            "VALUES (?, ?, ?, ?, ?)",
            [transaction_id, "acct_existing01", transaction_date, amount, None],
        )
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)
    before_count = (
        REGISTRY.get_sample_value("moneybin_account_link_overlap_ratio_count") or 0.0
    )
    before_sum = (
        REGISTRY.get_sample_value("moneybin_account_link_overlap_ratio_sum") or 0.0
    )

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        svc.import_file(fake_pdf, refresh=False)

    [candidate] = exc.value.outcome.account_proposals[0]["candidates"]
    assert "confidence" not in candidate
    after_count = (
        REGISTRY.get_sample_value("moneybin_account_link_overlap_ratio_count") or 0.0
    )
    after_sum = (
        REGISTRY.get_sample_value("moneybin_account_link_overlap_ratio_sum") or 0.0
    )
    assert after_count == before_count + 1
    # Exact, not approximate: the ratio is 2/2, and adding 1.0 to the running
    # sum here is the same float operation the histogram just performed.
    assert after_sum == before_sum + 1.0


@pytest.mark.integration
def test_pdf_account_metadata_populates_the_raw_account(
    db: Database,
    tmp_path: Path,
) -> None:
    doc = _make_doc(
        text_lines=[
            *_standard_text_lines(),
            "Account Name: Household Checking",
            "Account Type: Personal Checking",
            "Product Name: Total Checking",
            "Routing Number: 021000021",
            "Currency: usd",
        ],
        tables=[_standard_table()],
    )
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        result = import_answering_gate(svc, fake_pdf, refresh=False)

    assert result.transactions == 2
    row = db.execute(
        "SELECT account_name, account_label, account_type, currency "
        "FROM raw.tabular_accounts WHERE source_type = 'pdf'"
    ).fetchone()
    # account_label distinct from account_name: dim_accounts.sql's
    # tabular_accounts CTE reads account_label (never account_name) to decide
    # display_name_is_user_set, so a captured "Account Name:" line must land
    # there too -- not just on account_name -- or the next import/backfill
    # would read this genuinely person-named account as unnamed.
    assert row == (
        "Household Checking",
        "Household Checking",
        "Personal Checking",
        "USD",
    )


@pytest.mark.integration
def test_pdf_with_no_account_anchor_gates_before_minting(
    db: Database,
    tmp_path: Path,
) -> None:
    """A statement naming no account is ``identity_unknown``, not a confident mint.

    With no readable account number the source-native key identifies only these
    exact document bytes. Proceeding silently would still mint an account with
    no cross-document evidence and only a placeholder display name. No twin is
    seeded here on purpose: the point is that a proposal with an EMPTY candidate
    list still has to stop, which is the half ``identity_unknown`` exists to
    express — the tabular bare-file branch already opts in this way via
    ``fallback_keys``.
    """
    create_core_tables(db)
    doc = _anchorless_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        svc.import_file(fake_pdf, refresh=False, confirm=True, actor_kind="human")

    outcome = exc.value.outcome
    assert outcome.reason == "account_confirmation"
    assert outcome.channel == "pdf"
    [proposal] = outcome.account_proposals
    assert proposal["candidates"] == []
    assert proposal["is_new"] is True
    assert _count(db, "SELECT COUNT(*) FROM raw.tabular_transactions") == 0
    assert _count(db, "SELECT COUNT(*) FROM app.account_links") == 0


@pytest.mark.integration
def test_anchorless_pdf_surfaces_pre_document_identity_binding(
    db: Database,
    tmp_path: Path,
) -> None:
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    _seed_chase_twin(db)
    AccountLinksRepo(db).insert(
        link_id="legacy_anchorless_pdf",
        account_id="acct_existing01",
        ref_kind="source_native",
        ref_value="statement",
        source_type="pdf",
        source_origin="chase",
        decided_by="user",
        actor="system",
    )
    doc = _anchorless_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)
    db.execute(
        "INSERT INTO raw.tabular_accounts "
        "(account_id, account_name, account_number_masked, institution_name, "
        "source_file, source_type, source_origin, import_id) VALUES "
        "('statement', 'Legacy statement', NULL, 'Chase', "
        "'/legacy/location/statement.pdf', 'pdf', 'chase', 'legacy_import')"
    )
    period = "2024-01-01-2024-01-31"
    for row_number, (day, description, amount) in enumerate(
        (("2024-01-15", "Coffee Shop", "-50.00"), ("2024-01-20", "Paycheck", "150.00")),
        start=1,
    ):
        content_key = f"{period}|{day}|{amount}|0|0|{description}|statement"
        transaction_id = f"pdf_{hashlib.sha256(content_key.encode()).hexdigest()[:16]}"
        db.execute(
            "INSERT INTO raw.tabular_transactions "
            "(transaction_id, account_id, transaction_date, amount, description, "
            "source_file, source_type, source_origin, import_id, row_number) "
            "VALUES (?, 'statement', ?, ?, ?, '/legacy/location/statement.pdf', "
            "'pdf', 'chase', 'legacy_import', ?)",
            [transaction_id, day, amount, description, row_number],
        )

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        svc.import_file(fake_pdf, refresh=False, confirm=True, actor_kind="human")

    [proposal] = exc.value.outcome.account_proposals
    assert [
        (candidate["account_id"], candidate["signal"])
        for candidate in proposal["candidates"]
    ] == [("acct_existing01", "legacy_pdf_identity")]

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        result = svc.import_file(
            fake_pdf,
            refresh=False,
            account_bindings={proposal["source_account_key"]: "acct_existing01"},
        )

    assert result.transactions == 0
    assert db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_type = 'pdf'"
    ).fetchone() == (2,)


@pytest.mark.integration
def test_pinning_an_anchorless_pdf_teaches_exact_document_identity(
    db: Database,
    tmp_path: Path,
) -> None:
    """An exact re-import adopts the document digest learned by an explicit pin."""
    _seed_chase_twin(db)
    doc = _anchorless_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        svc.import_file(
            fake_pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_id="acct_existing01",
        )

    taught = db.execute(
        "SELECT ref_value FROM app.account_links "
        "WHERE ref_kind = 'source_native' AND status = 'accepted' "
        "ORDER BY ref_value"
    ).fetchall()
    # One link only: the document's own key. The pin no longer also writes a
    # self-map (acct_existing01 -> acct_existing01), which existed solely
    # because the pin used to overwrite the source-native key.
    assert [row[0] for row in taught] == [
        f"pdf_doc_{hashlib.sha256(b'%PDF-1.4 fake').hexdigest()[:16]}",
    ]

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        repeated = svc.import_file(
            fake_pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
        )

    assert repeated.accounts_created == ()


@pytest.mark.integration
def test_pdf_binding_new_mints_and_loads(
    db: Database,
    tmp_path: Path,
) -> None:
    """Answering the PDF gate with `new` mints a distinct account and loads the rows.

    The seeded twin is what raises the gate, so `new` here carries its real
    meaning: "not that existing Chase ...1234 account — keep this one separate."
    """
    _seed_chase_twin(db)
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        # Plain import_file throughout: the binding IS the subject here, so a
        # helper that supplies one would answer the question under test.
        svc.import_file(fake_pdf, refresh=False, confirm=True, actor_kind="human")
    key = exc.value.outcome.account_proposals[0]["source_account_key"]

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        result = svc.import_file(
            fake_pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={key: "new"},
        )
    assert result.transactions == 2
    row = db.execute(
        "SELECT account_id FROM app.account_links WHERE ref_kind='source_native' "
        "AND ref_value=? AND status='accepted'",
        [key],
    ).fetchone()
    assert row is not None and row[0]


@pytest.mark.integration
def test_partial_pdf_second_statement_requires_review_again(
    db: Database,
    tmp_path: Path,
) -> None:
    """A later partial-only statement stays reviewable instead of auto-linking."""
    _seed_chase_twin(db)
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)
    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as exc,
    ):
        svc.import_file(fake_pdf, refresh=False, confirm=True, actor_kind="human")
    key = exc.value.outcome.account_proposals[0]["source_account_key"]
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
        return_value=doc,
    ):
        svc.import_file(
            fake_pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={key: "new"},
        )

    # Next month: same card, different statement bytes. Last four plus issuer is
    # still partial evidence, so the new document cannot silently reuse a link.
    next_doc = _standard_doc(opening="1100.00", closing="1200.00")
    next_pdf = tmp_path / "statement_february.pdf"
    next_pdf.write_bytes(b"%PDF-1.4 fake february")
    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract",
            return_value=next_doc,
        ),
        pytest.raises(ImportConfirmationRequiredError) as next_exc,
    ):
        svc.import_file(next_pdf, refresh=False, confirm=True, actor_kind="human")
    [next_proposal] = next_exc.value.outcome.account_proposals
    assert next_proposal["source_account_key"] != key
    assert next_proposal["candidates"]


@pytest.mark.integration
def test_account_id_pin_keeps_the_statements_own_key(
    db: Database,
    tmp_path: Path,
) -> None:
    """Pinning with --account-id says which account, not what the document is called.

    The pin belongs in ``explicit_account_id`` alone. ``source_account_key``
    stays the document's own opaque key, so one accepted link points that key at
    the pinned account and ``raw.tabular_accounts.account_id`` carries the key
    rather than the canonical id.

    There is no self-map (``<id> -> <id>``) any more: that row only ever existed
    because the pin overwrote the native key, and it is what let a second pin of
    the same document fork the key and double-count the statement.
    """
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name) VALUES (?, ?)",  # noqa: S608  # test fixture
        ["acct_pinned01", "Chase Card"],
    )
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        import_answering_gate(
            svc,
            fake_pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_id="acct_pinned01",
        )

    linked = dict(
        db.execute(
            "SELECT ref_value, account_id FROM app.account_links "
            "WHERE ref_kind='source_native' AND status='accepted'"
        ).fetchall()
    )
    assert list(linked) == [key for key in linked if key.startswith("pdf_doc_")], (
        f"pin wrote a non-document source_native key: {linked}"
    )
    [document_key] = list(linked)
    assert linked[document_key] == "acct_pinned01"

    # The raw row is keyed by the document, not by the account it was pinned to.
    raw_keys = db.execute(
        "SELECT account_id FROM raw.tabular_accounts WHERE source_type = 'pdf'"
    ).fetchall()
    assert [r[0] for r in raw_keys] == [document_key], raw_keys


@pytest.mark.integration
def test_account_id_pin_refuses_a_document_already_bound_elsewhere(
    db: Database,
    tmp_path: Path,
) -> None:
    """A pin contradicting this document's accepted binding is refused before load.

    The pin used to be honoured by forking the key: the statement's own
    ``pdf_doc_`` key stayed on the first account while the rows landed under the
    canonical id, so ONE statement produced two accounts' worth of transactions
    and no per-account view could show the duplicate. Keying the raw row
    natively is what lets the gate see the conflict at all.

    Re-pointing the remembered key instead is not the alternative: that is a
    silent import-time re-point, which M1S.5 and "magic stays visible" both
    forbid. So the import refuses, names the account the document is already
    bound to, and writes nothing.
    """
    create_core_tables(db)
    for account_id, name in (
        ("acct_pinned01", "Chase Card"),
        ("acct_other01", "Other"),
    ):
        db.conn.execute(
            "INSERT INTO core.dim_accounts (account_id, display_name) VALUES (?, ?)",  # noqa: S608  # test fixture
            [account_id, name],
        )
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    # First: bind this exact document to acct_other01 by its positional ref.
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        svc.import_file(
            fake_pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"@0": "acct_other01"},
        )

    rows_before = _count(db, "SELECT COUNT(*) FROM raw.tabular_transactions")

    # Now pin a statement of the same card to a DIFFERENT account.
    other_pdf = tmp_path / "statement_pinned.pdf"
    other_pdf.write_bytes(fake_pdf.read_bytes())
    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
        ),
        pytest.raises(ValueError, match="already accepted onto 'acct_other01'"),
    ):
        svc.import_file(
            other_pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_id="acct_pinned01",
        )

    # Refused before load: no second copy of the statement, under either account.
    assert _count(db, "SELECT COUNT(*) FROM raw.tabular_transactions") == rows_before
    assert (
        _count(
            db,
            "SELECT COUNT(*) FROM raw.tabular_accounts WHERE source_file LIKE "
            "'%statement_pinned.pdf'",
        )
        == 0
    )

    # The remembered key is untouched, and no self-map was written for the pin.
    linked = dict(
        db.execute(
            "SELECT ref_value, account_id FROM app.account_links "
            "WHERE ref_kind='source_native' AND status='accepted'"
        ).fetchall()
    )
    assert "acct_pinned01" not in linked, linked
    [document_key] = list(linked)
    assert document_key.startswith("pdf_doc_")
    assert linked[document_key] == "acct_other01"


@pytest.mark.integration
def test_a_regenerated_pinned_statement_does_not_double_count(
    db: Database,
    tmp_path: Path,
) -> None:
    """Re-downloading the same statement must not import it twice.

    The pin fixes the canonical account, and ``transaction_id`` folds that
    canonical id — so the ids regenerate identically. Only the raw
    ``account_id`` moves, because a byte-different re-download yields a
    different ``pdf_doc_`` key, and staging dedups on
    ``(transaction_id, account_id)``. Both copies then clear it.
    """
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name) VALUES (?, ?)",  # noqa: S608  # test fixture
        ["acct_pinned01", "Chase Card"],
    )
    doc = _standard_doc()
    svc = ImportService(db)

    for name, body in (
        ("jan.pdf", b"%PDF-1.4 fake"),
        ("jan-again.pdf", b"%PDF-1.4 fake regenerated"),
    ):
        pdf = tmp_path / name
        pdf.write_bytes(body)
        with patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
        ):
            import_answering_gate(
                svc,
                pdf,
                refresh=False,
                confirm=True,
                actor_kind="human",
                account_id="acct_pinned01",
            )

    # Raw keeps a row per import batch; that is fine, because staging dedups on
    # (transaction_id, account_id) and collapses them. What must not happen is
    # the pair FORKING: a second source key for the same transaction_id clears
    # that dedup, and core.fct_transactions counts the statement twice.
    forked = db.execute(
        "SELECT transaction_id, COUNT(DISTINCT account_id) AS keys "
        "FROM raw.tabular_transactions WHERE source_type = 'pdf' "
        "GROUP BY transaction_id HAVING COUNT(DISTINCT account_id) > 1 "
        "ORDER BY transaction_id"
    ).fetchall()
    assert not forked, (
        f"the re-download forked the source key, so staging cannot dedup: {forked}"
    )


def _accept_pdf_link(db: Database, *, link_id: str, account_id: str, key: str) -> None:
    """Accept one ``pdf``/``document`` native key onto an account.

    Stands in for a statement this account already adopted, which is how a real
    card ends up holding several document keys: every statement carries its own
    digest, and each adoption accepts another one.
    """
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    AccountLinksRepo(db).insert(
        link_id=link_id,
        account_id=account_id,
        ref_kind="source_native",
        ref_value=key,
        source_type="pdf",
        source_origin="document",
        decided_by="user",
        actor="cli",
    )


@pytest.mark.integration
def test_a_regenerated_pinned_statement_dedups_when_the_account_holds_many_keys(
    db: Database,
    tmp_path: Path,
) -> None:
    """Several prior statements must not send the pin back to minting per document.

    An account holds one document key per statement it has adopted, so "more
    than one" is the ordinary state of any card with a history — not an edge
    case. Minting there would re-open the double count for exactly the accounts
    with the longest history, so the pin picks one remembered key by a stable
    order instead. Which one it picks does not matter for dedup; that both
    imports of the same statement pick the SAME one is the whole property.
    """
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name) VALUES (?, ?)",  # noqa: S608  # test fixture
        ["acct_pinned01", "Chase Card"],
    )
    for link_id, key in (
        ("lnk_prior_a", f"pdf_doc_{'a' * 16}"),
        ("lnk_prior_b", f"pdf_doc_{'b' * 16}"),
    ):
        _accept_pdf_link(db, link_id=link_id, account_id="acct_pinned01", key=key)
    doc = _standard_doc()
    svc = ImportService(db)

    for name, body in (
        ("feb.pdf", b"%PDF-1.4 fake"),
        ("feb-again.pdf", b"%PDF-1.4 fake regenerated"),
    ):
        pdf = tmp_path / name
        pdf.write_bytes(body)
        with patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
        ):
            import_answering_gate(
                svc,
                pdf,
                refresh=False,
                confirm=True,
                actor_kind="human",
                account_id="acct_pinned01",
            )

    forked = db.execute(
        "SELECT transaction_id, COUNT(DISTINCT account_id) AS keys "
        "FROM raw.tabular_transactions WHERE source_type = 'pdf' "
        "GROUP BY transaction_id HAVING COUNT(DISTINCT account_id) > 1 "
        "ORDER BY transaction_id"
    ).fetchall()
    assert not forked, f"the pin minted a fresh key instead of reusing one: {forked}"


@pytest.mark.integration
def test_account_id_pin_refuses_a_bound_document_even_when_the_target_has_a_key(
    db: Database,
    tmp_path: Path,
) -> None:
    """Reusing the target's key must not swallow the conflict the document carries.

    ``_refuse_contradicted_bindings`` asks whether the key on the incoming
    ``SourceAccount`` is already accepted elsewhere. Substituting the pinned
    account's own remembered key before that question is asked answers it
    trivially — the key belongs to the pin target, so nothing contradicts — and
    another account's statement loads under this one. So the pin only reuses a
    key when the document's own key is still unknown; a document that already
    named its account keeps saying so, and the refusal fires as it did before.
    """
    create_core_tables(db)
    for account_id, name in (
        ("acct_pinned01", "Chase Card"),
        ("acct_other01", "Other"),
    ):
        db.conn.execute(
            "INSERT INTO core.dim_accounts (account_id, display_name) VALUES (?, ?)",  # noqa: S608  # test fixture
            [account_id, name],
        )
    # The pin target already answers to a document key, so reuse is live.
    _accept_pdf_link(
        db,
        link_id="lnk_prior_a",
        account_id="acct_pinned01",
        key=f"pdf_doc_{'a' * 16}",
    )
    doc = _standard_doc()
    svc, fake_pdf = _service_with_fake_pdf(db, doc, tmp_path)

    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        svc.import_file(
            fake_pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_bindings={"@0": "acct_other01"},
        )

    rows_before = _count(db, "SELECT COUNT(*) FROM raw.tabular_transactions")

    other_pdf = tmp_path / "statement_pinned.pdf"
    other_pdf.write_bytes(fake_pdf.read_bytes())
    with (
        patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
        ),
        pytest.raises(ValueError, match="already accepted onto 'acct_other01'"),
    ):
        svc.import_file(
            other_pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_id="acct_pinned01",
        )

    assert _count(db, "SELECT COUNT(*) FROM raw.tabular_transactions") == rows_before


def _stamp_link(db: Database, *, link_id: str, decided_at: str) -> None:
    """Force one link's decision time, so ordering is not a race on the clock."""
    db.conn.execute(
        "UPDATE app.account_links SET decided_at = ? WHERE link_id = ?",  # noqa: S608  # test fixture
        [decided_at, link_id],
    )


@pytest.mark.integration
def test_the_pinned_key_pick_does_not_move_when_a_newer_key_is_added(
    db: Database,
    tmp_path: Path,
) -> None:
    """The reused key must be the one the account has answered to longest.

    A pick by sort order alone is only stable while the key set is; a merge, or
    a second document pinned to this account in between, inserts a key that can
    sort ahead of the one the account's existing rows already use — and the next
    import of a statement then lands somewhere new and double-counts. Ordering
    by decision time makes every later arrival lose: ``AccountLinksRepo.repoint``
    inserts a *new* accepted row for a merged ref, so even a merged-in key is
    younger than what the winner already held.
    """
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name) VALUES (?, ?)",  # noqa: S608  # test fixture
        ["acct_pinned01", "Chase Card"],
    )
    # Sort order and decision order disagree, so only one of the two rules can
    # produce `pdf_doc_zzz…` — which is the key the account has held longest.
    _accept_pdf_link(
        db, link_id="lnk_held", account_id="acct_pinned01", key=f"pdf_doc_{'z' * 16}"
    )
    _stamp_link(db, link_id="lnk_held", decided_at="2020-01-01 00:00:00")
    _accept_pdf_link(
        db, link_id="lnk_later", account_id="acct_pinned01", key=f"pdf_doc_{'a' * 16}"
    )
    _stamp_link(db, link_id="lnk_later", decided_at="2026-01-01 00:00:00")

    doc = _standard_doc()
    svc = ImportService(db)
    pdf = tmp_path / "mar.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake march")
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        import_answering_gate(
            svc,
            pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_id="acct_pinned01",
        )

    keys = [
        r[0]
        for r in db.execute(
            "SELECT DISTINCT account_id FROM raw.tabular_accounts "
            "WHERE source_type = 'pdf' AND source_file = ?",
            [str(pdf)],
        ).fetchall()
    ]
    assert keys == [f"pdf_doc_{'z' * 16}"], (
        f"picked by sort order, not by how long the account has held the key: {keys}"
    )


@pytest.mark.integration
def test_a_pinned_import_still_records_the_documents_own_digest(
    db: Database,
    tmp_path: Path,
) -> None:
    """Reusing a key must not erase what this exact file yields on its own.

    The raw row carries the reused key so the statement dedups, but the document
    digest is the only thing that identifies this file when it arrives WITHOUT a
    pin — dropped into an inbox drain, or re-imported by an agent that does not
    know to re-supply ``--account-id``. Without the digest on record that import
    finds no link, so it stops to ask or mints a second account for a statement
    already filed. So the pin teaches the digest alongside the key it borrowed.
    """
    import hashlib

    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name) VALUES (?, ?)",  # noqa: S608  # test fixture
        ["acct_pinned01", "Chase Card"],
    )
    _accept_pdf_link(
        db, link_id="lnk_held", account_id="acct_pinned01", key=f"pdf_doc_{'z' * 16}"
    )

    body = b"%PDF-1.4 fake april"
    digest_key = f"pdf_doc_{hashlib.sha256(body).hexdigest()[:16]}"
    doc = _standard_doc()
    svc = ImportService(db)
    pdf = tmp_path / "apr.pdf"
    pdf.write_bytes(body)
    with patch(
        "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
    ):
        import_answering_gate(
            svc,
            pdf,
            refresh=False,
            confirm=True,
            actor_kind="human",
            account_id="acct_pinned01",
        )

    owner = db.execute(
        "SELECT account_id FROM app.account_links WHERE status = 'accepted' "
        "AND ref_kind = 'source_native' AND source_type = 'pdf' AND ref_value = ?",
        [digest_key],
    ).fetchone()
    assert owner is not None, (
        "the document's own digest was never recorded, so an unpinned re-import "
        "of this exact file cannot recognise the account"
    )
    assert owner[0] == "acct_pinned01", owner
    # The raw row still carries the borrowed key, or the statement stops dedupping.
    raw = db.execute(
        "SELECT DISTINCT account_id FROM raw.tabular_accounts WHERE source_file = ?",
        [str(pdf)],
    ).fetchall()
    assert [r[0] for r in raw] == [f"pdf_doc_{'z' * 16}"], raw


@pytest.mark.integration
def test_a_borrowed_pin_key_survives_reimporting_the_same_regenerated_statement(
    db: Database,
    tmp_path: Path,
) -> None:
    """Teaching the borrowed document's own key makes the NEXT import stop borrowing.

    The reuse branch only fires while this document's key is still unknown. So
    teaching that key as an accepted link answers the branch's own gate: the
    third import of one regenerated statement finds its digest accepted, skips
    the substitution, and stores the rows under a key the second import did not
    use. ``transaction_id`` folds the canonical account, which the pin holds
    still, so the two copies share ids across two raw keys and staging's
    ``(transaction_id, account_id)`` dedup keeps both.
    """
    create_core_tables(db)
    db.conn.execute(
        "INSERT INTO core.dim_accounts (account_id, display_name) VALUES (?, ?)",  # noqa: S608  # test fixture
        ["acct_pinned01", "Chase Card"],
    )
    doc = _standard_doc()
    svc = ImportService(db)

    original = b"%PDF-1.4 fake"
    regenerated = b"%PDF-1.4 fake regenerated"
    for name, body in (
        ("feb.pdf", original),
        ("feb-again.pdf", regenerated),
        ("feb-again-copy.pdf", regenerated),
    ):
        pdf = tmp_path / name
        pdf.write_bytes(body)
        with patch(
            "moneybin.extractors.pdf.extractor.PDFExtractor.extract", return_value=doc
        ):
            import_answering_gate(
                svc,
                pdf,
                refresh=False,
                confirm=True,
                actor_kind="human",
                account_id="acct_pinned01",
            )

    forked = db.execute(
        "SELECT transaction_id, COUNT(DISTINCT account_id) AS keys "
        "FROM raw.tabular_transactions WHERE source_type = 'pdf' "
        "GROUP BY transaction_id HAVING COUNT(DISTINCT account_id) > 1 "
        "ORDER BY transaction_id"
    ).fetchall()
    assert not forked, f"the statement landed under two source keys: {forked}"

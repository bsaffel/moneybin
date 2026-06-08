"""Tests for ``ImportService.apply_pdf_bridge_response`` — the Phase 2b apply seam.

The driving agent returns ``{recipe, rows}``; apply re-executes the recipe
against the freshly-extracted document (the *actual*), runs the reconciliation
gate on those rows, and — on pass — persists the recipe and loads the rows.
The agent's returned rows are the *expectation*: apply verifies they match the
re-executed count and surfaces any divergence rather than trusting them
blindly (reconciliation on the re-executed rows is the authority).

These tests stub ``PDFExtractor.extract`` to return a fixed native-text
statement IR (the routing math has its own tests in ``test_routing.py``), but
exercise the REAL ``route_forced_recipe`` + reconciliation + load against a
real in-process DuckDB, because the reconciliation gate is the behavior under
test.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.metrics.registry import PDF_BRIDGE_EGRESS_TOTAL
from moneybin.services.import_service import BridgeApplyResult, ImportService
from moneybin.tables import PDF_FORMATS, TABULAR_TRANSACTIONS

# ---------------------------------------------------------------------------
# Fixtures — a reconciling Chase-style statement + the recipe the agent returns
# ---------------------------------------------------------------------------

_HEADERS = ["Date", "Description", "Amount"]
_ROW_REGION_START = "Date  Description  Amount"
_ROW_REGION_END = "Total:"


def _standard_doc(opening: str = "1000.00", closing: str = "1100.00") -> PdfDocument:
    """Chase statement IR whose rows net to closing - opening (reconciles)."""
    return PdfDocument(
        source_file="chase_may.pdf",
        text_lines=[
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
        ],
        tables=[
            PdfTable(
                page=1,
                header=_HEADERS,
                rows=[
                    ["01/15/2024", "Coffee Shop", "-50.00"],
                    ["01/20/2024", "Paycheck", "150.00"],
                ],
            )
        ],
    )


def _valid_recipe_dict() -> dict[str, Any]:
    """A Recipe the agent would propose; execute_recipe reproduces 2 rows."""
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


def _agent_rows(extra: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Rows keyed by recipe field names, as the bridge contract specifies."""
    rows = [
        {"Date": "01/15/2024", "Description": "Coffee Shop", "Amount": "-50.00"},
        {"Date": "01/20/2024", "Description": "Paycheck", "Amount": "150.00"},
    ]
    if extra:
        rows.extend(extra)
    return rows


def _bridge_response(
    *,
    recipe: dict[str, Any] | None = None,
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "recipe": recipe if recipe is not None else _valid_recipe_dict(),
        "rows": rows if rows is not None else _agent_rows(),
    }


@pytest.fixture()
def stub_extract(monkeypatch: pytest.MonkeyPatch) -> list[PdfDocument]:
    """Stub PDFExtractor.extract to return docs[0]. Caller sets docs[0]."""
    docs: list[PdfDocument] = [_standard_doc()]

    class _StubExtractor:
        def extract(self, _path: Path) -> PdfDocument:
            return docs[0]

    monkeypatch.setattr(
        "moneybin.extractors.pdf.extractor.PDFExtractor", _StubExtractor
    )
    return docs


def _pdf_path(tmp_path: Path) -> Path:
    path = tmp_path / "chase_may.pdf"
    path.write_bytes(b"%PDF-1.4\n%stub\n")
    return path


def _applied_count() -> float:
    return PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="applied")._value.get()  # type: ignore[reportPrivateUsage]


def _invalid_count() -> float:
    return PDF_BRIDGE_EGRESS_TOTAL.labels(outcome="invalid")._value.get()  # type: ignore[reportPrivateUsage]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_apply_reconciling_response_loads_transactions(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    before = _applied_count()
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    assert isinstance(result, BridgeApplyResult)
    assert result.outcome == "applied"
    assert result.import_id is not None
    assert result.rows_loaded == 2
    assert result.reject_reason is None
    assert _applied_count() == before + 1

    loaded = db.conn.execute(
        f"SELECT COUNT(*) FROM {TABULAR_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant, not user input
        "WHERE source_type = 'pdf'"
    ).fetchone()
    assert loaded is not None and loaded[0] == 2


def test_apply_persists_new_format(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    assert result.format_name is not None
    saved = db.conn.execute(
        f"SELECT COUNT(*) FROM {PDF_FORMATS.full_name} WHERE name = ?",  # noqa: S608  # TableRef constant, not user input
        [result.format_name],
    ).fetchone()
    assert saved is not None and saved[0] == 1


def test_apply_save_format_false_skips_persist(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response(), save_format=False
    )

    assert result.outcome == "applied"
    rows = db.conn.execute(
        f"SELECT COUNT(*) FROM {PDF_FORMATS.full_name}"  # noqa: S608  # TableRef constant, not user input
    ).fetchone()
    assert rows is not None and rows[0] == 0


def test_apply_writes_revertable_import_log(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    log = db.conn.execute(
        "SELECT status, source_type FROM raw.import_log WHERE import_id = ?",
        [result.import_id],
    ).fetchone()
    assert log is not None
    assert log[0] == "complete"
    assert log[1] == "pdf"


# ---------------------------------------------------------------------------
# Reconciliation gate — invalid proposals are rejected, nothing loads
# ---------------------------------------------------------------------------


def test_apply_non_reconciling_response_rejected(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    # closing far from opening so the 2 rows (net 100) can't tie out.
    stub_extract[0] = _standard_doc(opening="1000.00", closing="9999.00")

    before_invalid = _invalid_count()
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    assert result.outcome == "invalid"
    assert result.reject_reason == "reconciliation_failed"
    assert result.import_id is None
    assert result.rows_loaded == 0
    assert _invalid_count() == before_invalid + 1

    loaded = db.conn.execute(
        f"SELECT COUNT(*) FROM {TABULAR_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant, not user input
        "WHERE source_type = 'pdf'"
    ).fetchone()
    assert loaded is not None and loaded[0] == 0


def test_apply_invalid_does_not_persist_format(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    stub_extract[0] = _standard_doc(opening="1000.00", closing="9999.00")

    ImportService(db).apply_pdf_bridge_response(_pdf_path(tmp_path), _bridge_response())

    rows = db.conn.execute(
        f"SELECT COUNT(*) FROM {PDF_FORMATS.full_name}"  # noqa: S608  # TableRef constant, not user input
    ).fetchone()
    assert rows is not None and rows[0] == 0


# ---------------------------------------------------------------------------
# Divergence — agent's claimed rows vs the recipe's re-executed rows
# ---------------------------------------------------------------------------


def test_apply_no_divergence_when_agent_rows_match(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response()
    )

    assert result.rows_diverged is False
    assert result.expected_row_count == 2
    assert result.actual_row_count == 2


def test_apply_row_count_divergence_reported_but_still_loads(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    # Agent claims a 3rd row that the recipe (run against the doc) won't
    # reproduce — the recipe only sees 2 lines in the text region. The
    # reconciliation gate runs on the 2 re-executed rows (which tie out),
    # so the load proceeds, but the divergence is surfaced.
    phantom = [{"Date": "01/25/2024", "Description": "Ghost", "Amount": "-5.00"}]
    result = ImportService(db).apply_pdf_bridge_response(
        _pdf_path(tmp_path), _bridge_response(rows=_agent_rows(extra=phantom))
    )

    assert result.outcome == "applied"
    assert result.rows_diverged is True
    assert result.expected_row_count == 3
    assert result.actual_row_count == 2
    # Loaded the re-executed (actual) rows, NOT the agent's claimed 3.
    assert result.rows_loaded == 2


# ---------------------------------------------------------------------------
# Malformed input
# ---------------------------------------------------------------------------


def test_apply_malformed_response_raises_valueerror(
    db: Database, tmp_path: Path, stub_extract: list[PdfDocument]
) -> None:
    with pytest.raises(ValueError, match="recipe"):
        ImportService(db).apply_pdf_bridge_response(_pdf_path(tmp_path), {"rows": []})

"""Tests for the PDF import routing state machine (Task 9).

Each test exercises exactly one branch of the diagram in routing.py.
The ``db`` fixture from tests/moneybin/conftest.py provides a real in-process
DuckDB with ``app.pdf_formats`` already created by ``init_schemas``.

Text fixture construction
-------------------------
``capture_metadata`` looks for::

    Beginning Balance: $<amount>
    Ending Balance:    $<amount>

Row text between the header and "Total:" must produce amounts whose net sum
equals (closing - opening) within 1¢ for reconciliation to pass.

For the standard single-amount fixture:
    opening = 1000.00, closing = 1100.00  →  expected_delta = 100.00
    row amounts: -50.00 + 150.00 = 100.00  ✓

The header region starts with the column headers joined by two spaces (as
derive_recipe writes them into row_region.start_anchor), and the region ends
at "Total:".
"""

from __future__ import annotations

from typing import Any

import pytest

from moneybin.database import Database
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.extractors.pdf.recipe import Recipe
from moneybin.extractors.pdf.routing import route_forced_recipe, route_pdf_import
from moneybin.metrics.registry import (
    PDF_RECIPE_HIT_TOTAL,
    PDF_REPLAY_GUARD_FAILURE_TOTAL,
)
from moneybin.repositories.pdf_formats_repo import PdfFormatsRepo

# ---------------------------------------------------------------------------
# Document and recipe factories
# ---------------------------------------------------------------------------

#: Standard column headers used throughout these tests.
_HEADERS = ["Date", "Description", "Amount"]

#: Row region anchor for these tests.
#: _detect_start_anchor() scans doc.text_lines for the literal line whose
#: `\s{2,}` split equals the header list and returns it verbatim. The
#: fixture's text_lines include "Date  Description  Amount" so the anchor
#: lands on that string — which happens to equal "  ".join(_HEADERS) for
#: this fixture but would differ on a real PDF with proportionally-spaced
#: text (e.g. "Date     Description          Amount").
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
    """A single-page transaction table with Date/Description/Amount headers."""
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
    """Text lines that capture_metadata will parse for balances and anchors."""
    return [
        "Chase Bank Statement",
        "Account Number: 1234",
        "Statement Period: 01/01/2024",
        "To: 01/31/2024",
        f"Beginning Balance: ${opening}",
        f"Ending Balance: ${closing}",
        # Row region: start_anchor + rows + end_anchor
        _ROW_REGION_START,
        "01/15/2024  Coffee Shop  -50.00",
        "01/20/2024  Paycheck  150.00",
        _ROW_REGION_END,
    ]


def _standard_doc(
    opening: str = "1000.00",
    closing: str = "1100.00",
    rows: list[list[str]] | None = None,
) -> PdfDocument:
    """Full Chase statement doc — fingerprint matches Chase, balances set."""
    return _make_doc(
        text_lines=_standard_text_lines(opening, closing),
        tables=[_standard_table(rows)],
    )


def _valid_recipe_dict() -> dict[str, Any]:
    """A Recipe dict that model_validate accepts and execute_recipe can run.

    ``metadata_anchors`` is omitted so it deserialises to ``None``, signalling
    "no explicit anchors — fall back to DEFAULT_ANCHORS." An explicit empty
    list would mean "deliberately decline metadata capture" and route to seed
    with reason=metadata_incomplete.
    """
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


def _save_chase_format(db: Database, recipe: dict[str, Any] | None = None) -> None:
    """Insert a Chase format row into app.pdf_formats so fingerprint lookup hits."""
    repo = PdfFormatsRepo(db)
    # Fingerprint must match what compute_fingerprint(_standard_doc()) produces.
    # Chase issuer, headers preserved in original PDF column order
    # ["Date", "Description", "Amount"], page_bucket = "1".
    fingerprint = {
        "issuer": "Chase",
        "headers": ["Date", "Description", "Amount"],
        "page_bucket": "1",
    }
    repo.save_new(
        "chase_checking_pdf",
        recipe if recipe is not None else _valid_recipe_dict(),
        fingerprint=fingerprint,
        institution_name="Chase",
        document_kind="checking_statement",
        front_end="text",
        routing="transactions",
        actor="test",
    )


# ---------------------------------------------------------------------------
# Test 1: Fingerprint matches → replay succeeds → reconciles → transactions
# ---------------------------------------------------------------------------


def test_replay_hit_reconcile_pass_routes_to_transactions(db: Database) -> None:
    """Saved recipe replays; rows reconcile → outcome=transactions."""
    _save_chase_format(db)
    doc = _standard_doc()

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "transactions"
    assert decision.reason == "passed"
    assert decision.recipe is not None
    assert decision.replay_guard_failed is False
    assert len(decision.rows) > 0
    assert decision.confidence > 0.0
    assert decision.matched_format_name == "chase_checking_pdf"


# ---------------------------------------------------------------------------
# Test 2: Fingerprint matches → replay → reconciliation fails → replay guard
# ---------------------------------------------------------------------------


def test_replay_hit_reconcile_fail_sets_replay_guard(db: Database) -> None:
    """Saved recipe replays but rows don't reconcile → replay_guard_failed=True."""
    _save_chase_format(db)
    # Use opening=1000, closing=9999 → expected_delta=8999
    # But rows produce -50 + 150 = 100 → mismatch
    doc = _standard_doc(opening="1000.00", closing="9999.00")

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "replay_reconciliation_failed"
    assert decision.replay_guard_failed is True
    assert decision.matched_format_name == "chase_checking_pdf"


# ---------------------------------------------------------------------------
# Test 3: Fingerprint matches → saved recipe invalid → falls through to auto-derive
# ---------------------------------------------------------------------------


def test_replay_invalid_recipe_falls_through_to_auto_derive(db: Database) -> None:
    """Saved recipe fails model_validate; auto-derive takes over and succeeds."""
    # Store an intentionally invalid recipe (missing required fields).
    bad_recipe: dict[str, Any] = {"not_a_valid": "recipe"}
    _save_chase_format(db, recipe=bad_recipe)
    doc = _standard_doc()

    decision = route_pdf_import(doc, db)

    # Auto-derive should take over; if reconcile passes → transactions.
    # If auto-derive succeeds and reconcile fails (text alignment) → seed.
    # The key invariant: replay_guard_failed must NOT be set because it fell
    # through to auto-derive.
    assert decision.replay_guard_failed is False
    # Reason is one of the auto-derive paths (not replay_reconciliation_failed).
    assert decision.reason != "replay_reconciliation_failed"


# ---------------------------------------------------------------------------
# Test 4: No fingerprint match → auto-derive → reconcile passes → transactions
# ---------------------------------------------------------------------------


def test_no_fingerprint_match_auto_derive_reconcile_pass(db: Database) -> None:
    """No saved format; auto-derive recipe; rows reconcile → transactions."""
    # No row inserted in app.pdf_formats → fingerprint miss.
    doc = _standard_doc()

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "transactions"
    assert decision.reason == "passed"
    assert decision.replay_guard_failed is False
    assert decision.recipe is not None
    assert decision.matched_format_name is None


# ---------------------------------------------------------------------------
# Test 5: No fingerprint match → auto-derive returns None → no_transaction_table
# ---------------------------------------------------------------------------


def test_no_fingerprint_no_transaction_table(db: Database) -> None:
    """Document has no transaction-shaped table → seed, no_transaction_table."""
    doc = _make_doc(
        text_lines=["Just a header page", "No transactions here"],
        # No tables at all — derive_recipe will return None.
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "no_transaction_table"
    assert decision.recipe is None
    assert decision.confidence == 0.0
    assert decision.matched_format_name is None


# ---------------------------------------------------------------------------
# Test 6: Auto-derive → reconciliation fails (auto-derived) → reconciliation_failed
# ---------------------------------------------------------------------------


def test_auto_derive_reconcile_fail_no_replay_guard(db: Database) -> None:
    """Auto-derived recipe; rows don't reconcile → reconciliation_failed, no replay guard."""
    # Balances set so delta = 9000 but rows produce 100.
    doc = _standard_doc(opening="1000.00", closing="10000.00")

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "reconciliation_failed"
    assert decision.replay_guard_failed is False


# ---------------------------------------------------------------------------
# Test 7: Metadata incomplete (no balances) → metadata_incomplete
# ---------------------------------------------------------------------------


def test_metadata_incomplete_routes_to_seed(db: Database) -> None:
    """Document lacks balance lines → metadata_incomplete."""
    # Build a doc with a valid transaction table but no balance text lines.
    text_lines = [
        "Chase Bank Statement",
        "Account Number: 1234",
        # No Beginning/Ending Balance lines.
        _ROW_REGION_START,
        "01/15/2024  Coffee Shop  -50.00",
        "01/20/2024  Paycheck  150.00",
        _ROW_REGION_END,
    ]
    doc = _make_doc(
        text_lines=text_lines,
        tables=[_standard_table()],
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "metadata_incomplete"


# ---------------------------------------------------------------------------
# Test 8: Recipe matches zero rows → no_rows
# ---------------------------------------------------------------------------


def test_zero_rows_routes_to_seed(db: Database) -> None:
    """Table has a valid transaction header but no data rows → no_rows.

    We build a doc whose text does NOT contain the row_region content the
    recipe expects, so execute_recipe produces zero rows.
    """
    # Table has rows (so auto-derive succeeds), but the text lines contain no
    # parseable rows in the region (no lines between the start_anchor and
    # end_anchor that match the date/amount pattern).
    text_lines = [
        "Chase Bank Statement",
        "Account Number: 1234",
        "Beginning Balance: $1000.00",
        "Ending Balance: $1100.00",
        # Row region start + immediate end → zero parseable rows.
        _ROW_REGION_START,
        # Only whitespace lines between anchors.
        "   ",
        _ROW_REGION_END,
    ]
    # Table must have at least one row for auto-derive to succeed (it needs
    # sample data for format detection).  The table rows are used by
    # derive_recipe but the TEXT is what execute_recipe processes.
    doc = _make_doc(
        text_lines=text_lines,
        tables=[_standard_table()],
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "no_rows"
    assert decision.rows == []


# ---------------------------------------------------------------------------
# Test 9: Empty doc → no_transaction_table
# ---------------------------------------------------------------------------


def test_empty_doc_routes_to_seed(db: Database) -> None:
    """Document with no tables and no text → seed, no_transaction_table."""
    doc = _make_doc()

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "no_transaction_table"
    assert decision.recipe is None
    assert decision.rows == []


# ---------------------------------------------------------------------------
# Additional: RouteDecision fields are complete on transactions path
# ---------------------------------------------------------------------------


def test_transactions_decision_carries_recipe_rows_metadata(db: Database) -> None:
    """On the transactions path, recipe/rows/metadata are all populated."""
    doc = _standard_doc()

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "transactions"
    assert decision.recipe is not None
    assert len(decision.rows) >= 1
    assert decision.metadata.opening_balance is not None
    assert decision.metadata.closing_balance is not None
    assert 0.0 <= decision.confidence <= 1.0


# ---------------------------------------------------------------------------
# Additional: replay path sets replay_guard_failed=False on success
# ---------------------------------------------------------------------------


def test_replay_success_does_not_set_replay_guard(db: Database) -> None:
    """Successful replay never sets replay_guard_failed even though it's replay."""
    _save_chase_format(db)
    doc = _standard_doc()

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "transactions"
    assert decision.replay_guard_failed is False
    assert decision.matched_format_name == "chase_checking_pdf"


# ---------------------------------------------------------------------------
# Additional: metadata_incomplete on seed path preserves metadata fields
# ---------------------------------------------------------------------------


def test_metadata_incomplete_metadata_has_no_balances(db: Database) -> None:
    """On metadata_incomplete path, returned metadata has both balances None."""
    text_lines = [
        "Chase Bank Statement",
        _ROW_REGION_START,
        "01/15/2024  Coffee Shop  -50.00",
        _ROW_REGION_END,
    ]
    doc = _make_doc(
        text_lines=text_lines,
        tables=[_standard_table()],
    )

    decision = route_pdf_import(doc, db)

    assert decision.reason == "metadata_incomplete"
    assert decision.metadata.opening_balance is None
    assert decision.metadata.closing_balance is None


# ---------------------------------------------------------------------------
# low_confidence branch — under the Phase 2a binary-fill confidence model,
# this branch is structurally unreachable (non-empty rows always score 1.0;
# empty rows trip no_rows before confidence is computed). The branch remains
# as a defensive guard for Phase 2b partial-fill scoring. Test wiring via
# monkeypatch so the routing logic is locked even when the scorer can't
# produce a sub-threshold score on its own.
# ---------------------------------------------------------------------------


def test_low_confidence_routes_to_seed(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When is_high_confidence returns False, routing falls back to seed."""

    def _always_low(_conf: float) -> bool:
        return False

    monkeypatch.setattr(
        "moneybin.extractors.pdf.routing.is_high_confidence", _always_low
    )
    doc = _standard_doc()

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "low_confidence"
    assert decision.recipe is not None  # recipe was derived; just didn't score
    assert decision.rows  # rows were extracted before the score check fired
    assert decision.metadata.opening_balance is None  # metadata not captured yet


# ---------------------------------------------------------------------------
# route_forced_recipe — Phase 2b bridge apply path
# ---------------------------------------------------------------------------
# A caller-supplied (bridge-authored) recipe runs through the SAME execute →
# confidence → reconcile engine as replay, but it is NOT a replay of a saved
# recipe: matched_format_name stays None (signals first-contact save), the
# replay metrics never fire, and a reconciliation failure reports the
# non-replay reason "reconciliation_failed".


def test_forced_recipe_reconciles_routes_to_transactions(db: Database) -> None:
    """A bridge recipe that reconciles → outcome=transactions, no saved name."""
    recipe = Recipe.model_validate(_valid_recipe_dict())
    doc = _standard_doc()

    decision = route_forced_recipe(doc, recipe)

    assert decision.outcome == "transactions"
    assert decision.reason == "passed"
    assert decision.recipe is recipe
    assert decision.matched_format_name is None  # first contact, not a replay
    assert decision.replay_guard_failed is False
    assert len(decision.rows) > 0
    assert decision.fp is not None


def test_forced_recipe_does_not_emit_replay_metrics(db: Database) -> None:
    """A successful bridge apply is not a replay — replay KPIs must not move."""
    recipe = Recipe.model_validate(_valid_recipe_dict())
    doc = _standard_doc()

    hit_before = PDF_RECIPE_HIT_TOTAL.labels(outcome="replay_success")._value.get()  # type: ignore[reportPrivateUsage]
    route_forced_recipe(doc, recipe)
    hit_after = PDF_RECIPE_HIT_TOTAL.labels(outcome="replay_success")._value.get()  # type: ignore[reportPrivateUsage]

    assert hit_after == hit_before


def test_forced_recipe_reconcile_fail_is_not_replay_guard(db: Database) -> None:
    """A bridge recipe that fails reconciliation reports the non-replay reason.

    A replay failure (saved recipe drifted) is a different signal than a
    bridge proposal that doesn't tie out: the former trips the replay guard
    and its metric, the latter is just an invalid proposal. route_forced_recipe
    must report ``reconciliation_failed`` (not ``replay_reconciliation_failed``)
    and leave the replay-guard counter untouched.
    """
    recipe = Recipe.model_validate(_valid_recipe_dict())
    # opening=1000, closing=9999 → expected_delta 8999, rows net 100 → mismatch.
    doc = _standard_doc(opening="1000.00", closing="9999.00")

    guard_before = PDF_REPLAY_GUARD_FAILURE_TOTAL._value.get()  # type: ignore[reportPrivateUsage]
    decision = route_forced_recipe(doc, recipe)
    guard_after = PDF_REPLAY_GUARD_FAILURE_TOTAL._value.get()  # type: ignore[reportPrivateUsage]

    assert decision.outcome == "seed"
    assert decision.reason == "reconciliation_failed"
    assert decision.replay_guard_failed is False
    assert guard_after == guard_before


# ---------------------------------------------------------------------------
# Underivable-vs-absent transaction table (bridge escalation vs seed)
# ---------------------------------------------------------------------------


def test_positions_statement_reports_no_transaction_table(db: Database) -> None:
    """A document with no transaction table at all seeds — it is not agent-fodder.

    A brokerage positions statement (Symbol/Shares/Price/Value) is genuinely not
    transaction-shaped. Escalating it to the driving agent would be off-target,
    so it must keep reporting `no_transaction_table` and route to seed.
    """
    doc = PdfDocument(
        source_file="positions.pdf",
        tables=[],
        text_lines=[
            "Fidelity Investments",
            "Symbol    Shares    Price     Value",
            "AAPL      100       180.00    18000.00",
        ],
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "no_transaction_table"


def test_debit_credit_statement_is_underivable_not_absent(
    db: Database,
) -> None:
    r"""A debit/credit statement IS transaction-shaped — it must reach the bridge.

    "Date | Description | Withdrawals | Deposits" is the most common real bank
    layout, and deterministic derivation deliberately defers it (Phase 2b: the
    \\s{2,} split can't tell a blank debit cell from a blank credit cell). That
    makes it the single largest class of "transaction-shaped but underivable" —
    exactly what the agent bridge is for. Reporting it as `no_transaction_table`
    (excluded from escalation) would silently seed the most common statement
    there is.
    """
    doc = PdfDocument(
        source_file="wf.pdf",
        tables=[],
        text_lines=[
            "Wells Fargo",
            "Date         Description          Withdrawals    Deposits",
            "01/02/2024   COFFEE SHOP          4.50           ",
            "01/05/2024   PAYROLL DEPOSIT                     2000.00",
        ],
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "transaction_table_underivable"


def test_european_number_format_is_not_bridge_eligible(
    db: Database,
) -> None:
    """A non-US number locale must NOT escalate — the executor cannot replay it.

    The document is transaction-shaped, so it would otherwise report
    `transaction_table_underivable` and reach the bridge. But `execute_recipe`
    rejects a non-US `number_format` outright, so whatever recipe the agent
    authors provably cannot run: the user pays an LLM egress of their statement
    text and a confirmation prompt, and the import falls back to a seed anyway.
    `unsupported_number_format` is deliberately excluded from escalation.
    """
    doc = PdfDocument(
        source_file="euro.pdf",
        tables=[],
        text_lines=[
            "Euro Bank",
            "Date         Description          Amount",
            "01/02/2024   COFFEE SHOP          -4,50",
            "01/05/2024   PAYROLL DEPOSIT      2.000,00",
        ],
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "unsupported_number_format"

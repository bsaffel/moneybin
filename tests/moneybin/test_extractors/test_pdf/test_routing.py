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
from moneybin.extractors.pdf.auto_derive import derive_recipe, recipe_polarity_fits
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.extractors.pdf.metadata import StatementMetadata
from moneybin.extractors.pdf.recipe import Recipe
from moneybin.extractors.pdf.routing import route_forced_recipe, route_pdf_import
from moneybin.metrics.registry import (
    PDF_RECIPE_HIT_TOTAL,
    PDF_REPLAY_GUARD_FAILURE_TOTAL,
    PDF_SELF_HEAL_TOTAL,
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


def _recipe(sign_convention: str = "negative_is_expense") -> Recipe:
    """A valid Recipe with the given sign_convention; shape from _valid_recipe_dict."""
    return Recipe.model_validate({
        **_valid_recipe_dict(),
        "sign_convention": sign_convention,
    })


def _save_chase_format(
    db: Database,
    recipe: dict[str, Any] | None = None,
    source: str = "detected",
) -> None:
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
        source=source,
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


def test_wrapped_header_yearless_without_period_is_underivable_not_absent(
    db: Database,
) -> None:
    r"""A wrapped-header MM/DD statement with no billing period must reach the bridge.

    The columns are named only by a two-physical-line header ("Date of" /
    "Transaction ... $ Amount"), so no single line splits into a named header and
    only shape reconstruction sees the table. `derive_recipe` correctly declines
    (year-less rows with no capturable period → it won't author a recipe that
    emits wrong dates). The failure classifier must ALSO see the table by shape,
    else it reports `no_transaction_table` — excluded from bridge escalation — and
    the statement is silently seeded, the exact failure mode shape reconstruction
    was added to close.
    """
    doc = PdfDocument(
        source_file="chase_card.pdf",
        tables=[],
        text_lines=[
            "CHASE FREEDOM UNLIMITED",
            "Minimum Payment Due: $25.00",
            "ACCOUNT ACTIVITY",
            "Date of",
            "Transaction          Merchant Name or Description          $ Amount",
            "12/24   COFFEE SHOP          25.00",
            "01/15   BOOKSTORE          40.00",
            "Totals Year-to-Date",
        ],
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "transaction_table_underivable"


def test_route_yearless_drift_row_fails_extraction_to_bridge(db: Database) -> None:
    """A year-less row that can't be placed fails the whole extraction → bridge.

    The billing period is capturable, so derivation authors a year-less recipe; but
    one row's date sits far outside the cycle (an anomaly). execute_recipe raises
    YearlessDateError, and routing must fail the WHOLE extraction and seed as
    transaction_table_underivable (bridge-eligible) rather than silently drop the
    row. Drives the routing YearlessDateError handler end-to-end (first-contact).
    """
    doc = PdfDocument(
        source_file="chase_card.pdf",
        tables=[],
        text_lines=[
            "CHASE FREEDOM UNLIMITED",
            "Minimum Payment Due: $25.00",
            "Opening/Closing Date   01/01/25 - 01/31/25",
            "Date of",
            "Transaction   Description   $ Amount",
            "01/05   COFFEE SHOP   25.00",
            "01/15   BOOKSTORE   40.00",
            "06/15   ANOMALY ROW   10.00",  # ~4 months outside the billing cycle
            "Ending Balance   1234.56",
        ],
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "transaction_table_underivable"


def test_mixed_width_wrapped_header_reaches_bridge_not_seed(db: Database) -> None:
    """A wrapped-header statement with one odd-width row must reach the bridge.

    Shape reconstruction refuses to derive a recipe when the rows aren't a single
    uniform width (a description with an internal 2+-space gap gains a cell), but
    the statement IS transaction-shaped — no line names the columns, so no other
    rung recognises it. The failure classifier must still see it and report
    ``transaction_table_underivable`` (→ bridge) rather than ``no_transaction_table``
    (→ silent seed with no path to the assisted reader).
    """
    doc = PdfDocument(
        source_file="chase_card.pdf",
        tables=[],
        text_lines=[
            "CHASE FREEDOM UNLIMITED",
            "Minimum Payment Due: $25.00",
            "Date of",
            "Transaction   Merchant Name or Description   $ Amount",
            "12/24   COFFEE SHOP   25.00",
            "12/26   PAYPAL   *SOMEVENDOR   40.00",  # internal 2-space gap → 4 cells
            "01/15   BOOKSTORE   15.00",
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


def test_european_debit_credit_statement_is_not_bridge_eligible(
    db: Database,
) -> None:
    """A non-US locale must not escalate even in the deferred debit/credit layout.

    The number-format probe ran off the *strict* selector, which returns None for
    a debit/credit layout — so a European split-column statement skipped the
    locale check entirely and escalated. The bridge cannot help: `execute_recipe`
    rejects a non-US `number_format` outright. That egresses the user's statement
    text to an AI provider for a result that provably cannot be used, which is
    the precise harm the reason split exists to prevent.
    """
    doc = PdfDocument(
        source_file="euro_wf.pdf",
        tables=[
            PdfTable(
                page=1,
                header=["Date", "Description", "Withdrawals", "Deposits"],
                rows=[
                    ["01/02/2024", "COFFEE SHOP", "4,50", ""],
                    ["01/05/2024", "PAYROLL", "", "2.000,00"],
                ],
            )
        ],
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "unsupported_number_format"


# The unruled debit/credit statement — no ruling lines AND a split amount pair —
# is the layout that reconstructs as no table at all: `\s{2,}` collapses the one
# blank side, so each row splits to fewer cells than the header and the
# width-matching reconstructor rejects it on the first data row. The locale probe
# therefore has no table to read and must fall back to the raw text lines. These
# two tests pin both sides of that fallback.


def _unruled_debit_credit_doc(rows: list[str]) -> PdfDocument:
    """A split-column statement whose rows exist ONLY as whitespace-aligned text."""
    return PdfDocument(
        source_file="unruled_wf.pdf",
        tables=[],
        text_lines=[
            "Wells Fargo",
            "Date         Description     Withdrawals   Deposits",
            *rows,
        ],
    )


def test_unruled_european_debit_credit_statement_is_not_bridge_eligible(
    db: Database,
) -> None:
    """A non-US locale must not escalate even when NO table reconstructs at all.

    Probing the loose selector fixed the *ruled* debit/credit case, but an unruled
    one selects no table either — so the probe was skipped entirely and every such
    statement, US or European, escalated. That is the most common real layout in a
    non-US locale, and the bridge provably cannot help it: `execute_recipe` raises
    on any `number_format != "us"`. Escalating egresses the user's raw statement
    text to an AI provider for a result that cannot be used, then seeds anyway.
    """
    doc = _unruled_debit_credit_doc([
        "01/02/2024   COFFEE SHOP     4,50",
        "01/05/2024   PAYROLL                       2.000,00",
    ])

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "unsupported_number_format"


def test_replay_refuses_a_recipe_whose_sign_convention_does_not_fit(
    db: Database,
) -> None:
    """A negative_is_expense recipe must not replay onto an all-positive document.

    `derive_recipe` refuses to author such a recipe (its own comment: "route to
    seed rather than auto-derive a recipe that silently corrupts signs on every
    future replay") — but routing replays a saved recipe BEFORE it ever calls
    `derive_recipe`, so that guard was skipped on the replay path.

    Reconciliation does not catch it: this statement's balance delta (+200) equals
    the sum of its positive amounts, so the replayed recipe reconciles cleanly with
    every sign backwards and imports two card charges as income.

    Unreachable until now — unruled statements didn't derive, so they never saved a
    format and never matched one. Now a Chase checking statement's saved recipe
    fingerprint-matches a Chase card statement with the same columns and page count.
    """
    _save_chase_format(db)  # negative_is_expense, Chase / Date,Description,Amount / 1pg

    all_positive = [
        ["01/15/2024", "Coffee Shop", "50.00"],
        ["01/20/2024", "Grocery Mart", "150.00"],
    ]
    doc = _make_doc(
        text_lines=[
            "Chase Bank Statement",
            "Account Number: 1234",
            "Statement Period: 01/01/2024",
            "To: 01/31/2024",
            "Beginning Balance: $1000.00",
            "Ending Balance: $1200.00",
            _ROW_REGION_START,
            "01/15/2024  Coffee Shop  50.00",
            "01/20/2024  Grocery Mart  150.00",
            _ROW_REGION_END,
        ],
        tables=[_standard_table(all_positive)],
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.recipe is None
    # The saved format is disowned, not merely bypassed — a populated
    # matched_format_name tells the service "this was a replay, skip save_new".
    assert decision.matched_format_name is None


def _card_statement_doc(opening: str = "0.00", closing: str = "100.00") -> PdfDocument:
    """A credit-card statement: charges positive, one payment negative.

    Sign-wise this is indistinguishable from a checking statement — `[+150, -50]`
    against `[-50, +150]`. Only the disclosures say which it is.
    """
    return _make_doc(
        text_lines=[
            "Chase Bank Statement",
            "Account Number: 1234",
            "Statement Period: 01/01/2024",
            "To: 01/31/2024",
            "Minimum Payment Due: $25.00",
            f"Beginning Balance: ${opening}",
            f"Ending Balance: ${closing}",
            _ROW_REGION_START,
            "01/15/2024  Coffee Shop  150.00",
            "01/20/2024  Payment Thank You  -50.00",
            _ROW_REGION_END,
        ],
        tables=[
            _standard_table([
                ["01/15/2024", "Coffee Shop", "150.00"],
                ["01/20/2024", "Payment Thank You", "-50.00"],
            ])
        ],
    )


def _empty_metadata() -> StatementMetadata:
    """The StatementMetadata value route_pdf_import passes to derive_recipe (routing.py:335)."""
    return StatementMetadata(
        account_id=None,
        period_start=None,
        period_end=None,
        opening_balance=None,
        closing_balance=None,
    )


def _card_statement_doc_all_positive() -> PdfDocument:
    """A card statement with no payment row — an ordinary card month, still a card."""
    return _make_doc(
        text_lines=[
            "Chase Bank Statement",
            "Account Number: 1234",
            "Statement Period: 01/01/2024",
            "To: 01/31/2024",
            "Minimum Payment Due: $25.00",
            "Beginning Balance: $0.00",
            "Ending Balance: $150.00",
            _ROW_REGION_START,
            "01/15/2024  Coffee Shop  150.00",
            _ROW_REGION_END,
        ],
        tables=[_standard_table([["01/15/2024", "Coffee Shop", "150.00"]])],
    )


def _checking_statement_doc() -> PdfDocument:
    """An ordinary checking statement: no card markers, mixed-sign amounts."""
    return _make_doc(
        text_lines=[
            "Chase Bank Statement",
            "Account Number: 1234",
            "Statement Period: 01/01/2024",
            "To: 01/31/2024",
            "Beginning Balance: $1000.00",
            "Ending Balance: $1100.00",
            _ROW_REGION_START,
            "01/15/2024  Coffee Shop  -50.00",
            "01/20/2024  Paycheck  150.00",
            _ROW_REGION_END,
        ],
        tables=[_standard_table()],
    )


def _all_positive_no_markers_doc() -> PdfDocument:
    """No card markers, no negative amounts anywhere — genuinely ambiguous."""
    return _make_doc(
        text_lines=[
            "Chase Bank Statement",
            "Account Number: 1234",
            "Statement Period: 01/01/2024",
            "To: 01/31/2024",
            "Beginning Balance: $1000.00",
            "Ending Balance: $1200.00",
            _ROW_REGION_START,
            "01/15/2024  Coffee Shop  50.00",
            "01/20/2024  Grocery Mart  150.00",
            _ROW_REGION_END,
        ],
        tables=[
            _standard_table([
                ["01/15/2024", "Coffee Shop", "50.00"],
                ["01/20/2024", "Grocery Mart", "150.00"],
            ])
        ],
    )


def test_card_statement_derives_negative_is_income() -> None:
    """A self-declared card statement derives the inverted convention.

    #313 made this decline (safe but useless — the card would not import at all).
    The markers state the convention outright, so derive it; the SERVICE gates the
    inversion behind a confirm (see test_import_pdf_transactions.py).
    """
    doc = _card_statement_doc(opening="0.00", closing="100.00")
    recipe = derive_recipe(doc, _empty_metadata())
    assert recipe is not None
    assert recipe.sign_convention == "negative_is_income"


def test_card_statement_with_no_payment_row_still_derives() -> None:
    """All-positive amounts + card markers is NOT ambiguous — the markers decide.

    Today this declines on both #313 tells. A card month with no payment is an
    ordinary card month.
    """
    doc = _card_statement_doc_all_positive()
    recipe = derive_recipe(doc, _empty_metadata())
    assert recipe is not None
    assert recipe.sign_convention == "negative_is_income"


def test_checking_statement_still_derives_negative_is_expense() -> None:
    """The happy path stays silent — only the inversion is gated."""
    doc = _checking_statement_doc()
    recipe = derive_recipe(doc, _empty_metadata())
    assert recipe is not None
    assert recipe.sign_convention == "negative_is_expense"


def test_all_positive_without_card_markers_still_declines() -> None:
    """Genuinely ambiguous: no markers, no negatives. Never guess — escalate."""
    doc = _all_positive_no_markers_doc()
    assert derive_recipe(doc, _empty_metadata()) is None


def test_card_statement_does_not_replay_a_checking_recipe(db: Database) -> None:
    """The replay gate must apply the card check, not just the any-negative one.

    A saved negative_is_expense Chase recipe must not replay onto a Chase card
    statement sharing its fingerprint (issuer, headers, page_bucket) — that would
    import every charge as income. The gate disowns it (matched_format_name stays
    None, proving no replay happened); auto_derive then re-reads the document
    fresh and, since Task 2, proposes the correct negative_is_income recipe
    instead of declining outright.
    """
    _save_chase_format(db)  # negative_is_expense, Chase / Date,Description,Amount / 1pg

    decision = route_pdf_import(_card_statement_doc(), db)

    assert decision.outcome == "transactions"
    assert decision.recipe is not None
    assert decision.recipe.sign_convention == "negative_is_income"
    # The saved (bank) recipe was disowned, not reused — a populated
    # matched_format_name would tell the service "this was a replay."
    assert decision.matched_format_name is None


def test_card_recipe_refuses_to_replay_onto_a_non_card_document() -> None:
    """The mirror of #313. A saved card recipe must not invert a checking statement.

    Fingerprint is (issuer, headers, page_bucket) — a same-issuer checking
    statement with the same columns matches a card format's fingerprint. Without
    this guard, replay writes every paycheck as an expense.
    """
    card_recipe = _recipe(sign_convention="negative_is_income")
    checking_doc = _checking_statement_doc()
    assert recipe_polarity_fits(card_recipe, checking_doc) is False


def test_card_recipe_replays_onto_a_card_document() -> None:
    card_recipe = _recipe(sign_convention="negative_is_income")
    card_doc = _card_statement_doc(opening="0.00", closing="100.00")
    assert recipe_polarity_fits(card_recipe, card_doc) is True


def test_a_human_ratified_sign_outranks_the_marker_heuristic() -> None:
    """`sign_ratified` short-circuits the guard — in BOTH directions.

    The guard reads the document's text; ``sign_ratified`` means a human already
    read it and disagreed. Both refusals it would otherwise raise are exactly the
    ones that make an override unreplayable: a ``negative_is_expense`` recipe on a
    marker-bearing document (the false-positive card the user corrected), and a
    ``negative_is_income`` recipe on a document with no markers (a genuine card
    that prints none of the five disclosures).
    """
    bank_recipe = _recipe(sign_convention="negative_is_expense")
    card_recipe = _recipe(sign_convention="negative_is_income")
    card_doc = _card_statement_doc(opening="0.00", closing="100.00")
    checking_doc = _checking_statement_doc()

    # Both are refused while the convention is only an inference…
    assert recipe_polarity_fits(bank_recipe, card_doc) is False
    assert recipe_polarity_fits(card_recipe, checking_doc) is False

    # …and both replay once a human has asserted them.
    ratified_bank = bank_recipe.model_copy(update={"sign_ratified": True})
    ratified_card = card_recipe.model_copy(update={"sign_ratified": True})
    assert recipe_polarity_fits(ratified_bank, card_doc) is True
    assert recipe_polarity_fits(ratified_card, checking_doc) is True


def test_route_decision_carries_card_markers(db: Database) -> None:
    """The confirm gate shows the user which disclosures drove the inversion."""
    doc = _card_statement_doc(opening="0.00", closing="100.00")
    decision = route_pdf_import(doc, db)
    assert decision.outcome == "transactions"
    assert decision.recipe is not None
    assert decision.recipe.sign_convention == "negative_is_income"
    assert "minimum payment" in decision.card_markers


def test_route_decision_has_no_markers_for_checking(db: Database) -> None:
    decision = route_pdf_import(_checking_statement_doc(), db)
    assert decision.card_markers == ()


def test_euro_symbol_statement_is_recognised_as_non_us(db: Database) -> None:
    """A € amount must read as European, not as an unknown locale.

    The probe stripped only `$`, so every cell on a €-denominated statement matched
    neither number pattern, the locale came back unknown, and the document fell
    through to `transaction_table_underivable` — escalating to the bridge for a
    recipe `execute_recipe` rejects outright. Detecting the locale is what holds it
    back from that pointless egress.
    """
    doc = PdfDocument(
        source_file="euro.pdf",
        tables=[
            PdfTable(
                page=1,
                header=["Date", "Description", "Withdrawals", "Deposits"],
                rows=[
                    ["01/02/2024", "COFFEE SHOP", "-€4,50", ""],
                    ["01/05/2024", "PAYROLL", "", "€2.000,00"],
                ],
            )
        ],
    )

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "unsupported_number_format"


def test_locale_is_probed_on_the_table_derivation_actually_failed_on(
    db: Database,
) -> None:
    """On a multi-table document, the locale probe must not read the wrong table.

    Probing the *loose* table first picks whichever transaction-shaped table is
    largest — not necessarily the one `derive_recipe` gave up on. Here the main
    ledger is a small US table that fails derivation on the all-positive sign guard,
    beside a larger European debit/credit sub-table. Reading the larger one reports
    `unsupported_number_format` and seeds the statement, when the US ledger the
    deriver actually choked on is exactly what the bridge exists to read.
    """
    us_ledger_all_positive = PdfTable(
        page=1,
        header=["Date", "Description", "Amount"],
        rows=[
            ["01/15/2024", "Coffee Shop", "50.00"],
            ["01/20/2024", "Grocery Mart", "150.00"],
        ],
    )
    larger_european_sub_table = PdfTable(
        page=1,
        header=["Date", "Description", "Withdrawals", "Deposits"],
        rows=[
            ["01/02/2024", "FEE A", "1,50", ""],
            ["01/03/2024", "FEE B", "2,50", ""],
            ["01/04/2024", "FEE C", "3,50", ""],
            ["01/05/2024", "FEE D", "4,50", ""],
            ["01/06/2024", "FEE E", "5,50", ""],
        ],
    )
    doc = _make_doc(tables=[us_ledger_all_positive, larger_european_sub_table])

    decision = route_pdf_import(doc, db)

    assert decision.reason == "transaction_table_underivable"


def test_unruled_us_debit_credit_statement_still_escalates(db: Database) -> None:
    """The locale fallback must not over-correct: a US unruled split layout escalates.

    Negative control for the test above. The raw-text locale probe exists to hold
    *non-US* statements back from the bridge — it must not also hold back the US
    ones, which are precisely what the bridge was built to read.
    """
    doc = _unruled_debit_credit_doc([
        "01/02/2024   COFFEE SHOP     4.50",
        "01/05/2024   PAYROLL                       2,000.00",
    ])

    decision = route_pdf_import(doc, db)

    assert decision.reason == "transaction_table_underivable"


# ---------------------------------------------------------------------------
# Self-healing replay: a saved recipe that stops reconciling is re-derived
# ---------------------------------------------------------------------------

#: Amount pattern shipped before the sub-dollar fix. It requires at least one
#: digit before the decimal point, so a statement printing a sub-dollar fee as
#: a bare ".39" (no leading zero) silently drops that row. Pinned as a literal
#: rather than imported: the point is that a recipe PERSISTED under the old
#: logic keeps this pattern forever, so the test must not track the current
#: derivation code.
_STALE_AMOUNT_PATTERN = r"-?\$?[\d,]+\.\d{2}"


def _stale_recipe_dict(
    sign_convention: str = "negative_is_expense",
    *,
    sign_ratified: bool = False,
) -> dict[str, Any]:
    """The saved-recipe shape, pinned to the pre-fix Amount pattern."""
    recipe = _valid_recipe_dict()
    recipe["sign_convention"] = sign_convention
    recipe["sign_ratified"] = sign_ratified
    for field in recipe["fields"]:
        if field["name"] == "Amount":
            field["pattern"] = _STALE_AMOUNT_PATTERN
    return recipe


def _subdollar_doc(opening: str = "1000.00", closing: str = "1100.39") -> PdfDocument:
    """Statement whose rows include a sub-dollar fee printed as a bare ".39".

    Mirrors the real Chase card statement that motivated this path: the fee row
    prints with no leading zero, so the pre-fix Amount pattern drops it and the
    extracted rows land 39c short of the balance delta.
    Rows: -50.00 + 150.00 + 0.39 = 100.39 = closing - opening.
    """
    rows = [
        ["01/15/2024", "Coffee Shop", "-50.00"],
        ["01/20/2024", "Paycheck", "150.00"],
        ["01/22/2024", "Foreign Transaction Fee", ".39"],
    ]
    text_lines = [
        "Chase Bank Statement",
        "Account Number: 1234",
        "Statement Period: 01/01/2024",
        "To: 01/31/2024",
        f"Beginning Balance: ${opening}",
        f"Ending Balance: ${closing}",
        _ROW_REGION_START,
        "01/15/2024  Coffee Shop  -50.00",
        "01/20/2024  Paycheck  150.00",
        "01/22/2024  Foreign Transaction Fee  .39",
        _ROW_REGION_END,
    ]
    return _make_doc(text_lines=text_lines, tables=[_standard_table(rows)])


def test_replay_failure_re_derives_and_recovers_the_dropped_row(
    db: Database,
) -> None:
    """A saved recipe frozen under old derivation logic is repaired in place.

    The saved recipe's Amount pattern cannot match ".39", so the replay comes up
    39c short and reconciliation fails. Before self-healing this seeded the whole
    statement — a fix to the derivation code could never reach the recipe already
    in app.pdf_formats.
    """
    _save_chase_format(db, recipe=_stale_recipe_dict())
    doc = _subdollar_doc()

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "transactions"
    assert decision.reason == "passed"
    assert decision.rederived is True
    # The format identity survives the repair: this is the SAME saved format,
    # re-derived — not a first-contact save under a new name.
    assert decision.matched_format_name == "chase_checking_pdf"
    assert len(decision.rows) == 3


def test_self_heal_falls_back_to_seed_when_the_document_is_underivable(
    db: Database,
) -> None:
    """The fail-safe half of "recover automatically, or fail safe".

    Replay reads the text; derivation reads the tables — so the two halves are
    failed independently. The text carries a well-formed row region whose
    amounts don't add up to the balance delta (replay reconciles to 100.00
    against a stated 200.00), while the table's amount column is unparseable
    (`transaction_table_underivable`). The fingerprint still matches, so the
    saved recipe is replayed and self-heal is entered — and must then hand back
    None so the caller keeps its seed decision, rather than raising or returning
    a half-built repair.
    """
    _save_chase_format(db)
    doc = _make_doc(
        text_lines=_standard_text_lines(opening="1000.00", closing="1200.00"),
        tables=[
            PdfTable(
                page=1,
                header=_HEADERS,
                # One parseable negative keeps `recipe_polarity_fits` satisfied
                # so the replay is actually attempted — an all-unparseable
                # column would be refused by the polarity guard instead, and the
                # test would pass without ever reaching self-heal.
                rows=[
                    ["01/15/2024", "Coffee Shop", "-50.00"],
                    ["01/20/2024", "Paycheck", "n/a"],
                ],
            )
        ],
    )
    before = PDF_SELF_HEAL_TOTAL.labels(outcome="underivable")._value.get()  # type: ignore[reportPrivateUsage]

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.rederived is False
    after = PDF_SELF_HEAL_TOTAL.labels(outcome="underivable")._value.get()  # type: ignore[reportPrivateUsage]
    assert after == before + 1


@pytest.mark.parametrize("source", ["manual", "bridge"])
def test_self_heal_refuses_a_human_authored_recipe(db: Database, source: str) -> None:
    """Guard A: only machine-derived recipes are repaired automatically.

    A manual- or bridge-authored recipe encodes human intent. Silently replacing
    it with an auto-derived guess destroys that work, so it escalates as before.

    Both values are exercised because "bridge" is the one that nearly wasn't
    reachable: the service persisted every first-contact recipe as "detected"
    regardless of rung, so this guard's stated primary case — protecting an
    agent-authored, human-vetted recipe — silently could not fire.
    """
    _save_chase_format(db, recipe=_stale_recipe_dict(), source=source)
    doc = _subdollar_doc()

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "replay_reconciliation_failed"
    assert decision.rederived is False


def test_self_heal_refuses_to_change_the_sign_convention(db: Database) -> None:
    """Guard B: a repair may fix a pattern, never flip ledger polarity.

    bump_version mirrors the new recipe's sign_convention into the column every
    reader trusts, so an unguarded heal could invert every amount on the
    statement without a human ever seeing it.

    This fixture isolates the sign guard and nothing else. The saved recipe is a
    human-ratified negative_is_income; auto-derive reads this document as
    negative_is_expense (proved by
    test_replay_failure_re_derives_and_recovers_the_dropped_row, which heals the
    same document). So the re-derived recipe WOULD reconcile — the only thing
    standing between it and a silent ledger-wide inversion is the sign guard.
    Remove the guard and this test fails, which is the point.

    sign_ratified is what lets the replay reach reconciliation at all: it
    outranks recipe_polarity_fits, which would otherwise refuse a card-convention
    recipe on a document carrying no card disclosures and never get here. It also
    makes this the highest-stakes form of the case — an unguarded heal would
    silently overturn a decision a human explicitly made.
    """
    _save_chase_format(
        db, recipe=_stale_recipe_dict("negative_is_income", sign_ratified=True)
    )
    doc = _subdollar_doc()

    decision = route_pdf_import(doc, db)

    # The flip is never applied on the routing layer's own authority. It was
    # previously refused outright, which left the statement permanently
    # unimportable; it is now handed up flagged, and ImportService's sign gate
    # holds it until a human ratifies (see the service suite). Either way the
    # invariant this test exists for is the same: not silently.
    assert decision.rederived_from_sign == "negative_is_income"
    assert decision.recipe is not None
    assert decision.recipe.sign_ratified is False


def test_self_heal_keeps_the_original_seed_decision_when_re_derivation_also_fails(
    db: Database,
) -> None:
    """A re-derived recipe that still doesn't reconcile changes nothing.

    The balances here tie out to no row sum, so neither the saved recipe nor a
    fresh derivation can reconcile. The statement must seed with the original
    replay reason and guard flag intact.
    """
    _save_chase_format(db, recipe=_stale_recipe_dict())
    doc = _standard_doc(opening="1000.00", closing="9999.00")

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "replay_reconciliation_failed"
    assert decision.replay_guard_failed is True
    assert decision.rederived is False


def test_successful_replay_is_never_marked_re_derived(db: Database) -> None:
    """Negative control: a replay that reconciles must not touch the heal path."""
    _save_chase_format(db)
    doc = _standard_doc()

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "transactions"
    assert decision.rederived is False


def test_first_contact_reconciliation_failure_is_not_a_heal(db: Database) -> None:
    """Negative control: auto-derive is already fresh — there is nothing to repair."""
    doc = _standard_doc(opening="1000.00", closing="9999.00")

    decision = route_pdf_import(doc, db)

    assert decision.outcome == "seed"
    assert decision.reason == "reconciliation_failed"
    assert decision.rederived is False


def test_a_repaired_replay_is_distinguishable_from_a_seeded_one_in_metrics(
    db: Database,
) -> None:
    """The guard counter fires before the repair, so it counts triggers, not outcomes.

    Without a separate self-heal counter a fleet where every replay failure heals
    looks identical to one where every failure seeds — which is the only number
    that says whether this rung works.
    """
    _save_chase_format(db, recipe=_stale_recipe_dict())
    before = PDF_SELF_HEAL_TOTAL.labels(outcome="repaired")._value.get()  # type: ignore[reportPrivateUsage]

    route_pdf_import(_subdollar_doc(), db)

    after = PDF_SELF_HEAL_TOTAL.labels(outcome="repaired")._value.get()  # type: ignore[reportPrivateUsage]
    assert after == before + 1


def test_a_sign_changing_repair_is_counted_separately_from_a_clean_one(
    db: Database,
) -> None:
    """A repair awaiting ratification must not read as a completed repair.

    Both land rows in the same shape; only the label says one is still waiting
    on a human. Collapsing them would hide every pending polarity flip.
    """
    _save_chase_format(
        db, recipe=_stale_recipe_dict("negative_is_income", sign_ratified=True)
    )
    label = PDF_SELF_HEAL_TOTAL.labels(outcome="repaired_pending_sign")
    before = label._value.get()  # type: ignore[reportPrivateUsage]

    route_pdf_import(_subdollar_doc(), db)

    assert label._value.get() == before + 1  # type: ignore[reportPrivateUsage]


def test_a_sign_changing_repair_is_surfaced_rather_than_refused(
    db: Database,
) -> None:
    """A repair that flips polarity is a question for a human, not a dead end.

    Refusing outright left the statement permanently unimportable: the routing
    decision seeds, and ImportService._gate_pdf_sign_convention ignores
    non-transaction decisions, so no `--sign` or `--confirm` could ever
    authorize the repair. The decision now carries the fresh recipe plus the
    convention it replaced, so the service can put the flip in front of a
    person.
    """
    _save_chase_format(
        db, recipe=_stale_recipe_dict("negative_is_income", sign_ratified=True)
    )

    decision = route_pdf_import(_subdollar_doc(), db)

    assert decision.outcome == "transactions"
    assert decision.rederived is True
    assert decision.rederived_from_sign == "negative_is_income"
    assert decision.recipe is not None
    assert decision.recipe.sign_convention == "negative_is_expense"
    # The human's prior ratification does NOT carry across a polarity change —
    # they ratified the old convention, not this one.
    assert decision.recipe.sign_ratified is False


def test_a_repair_that_keeps_the_convention_carries_no_sign_change(
    db: Database,
) -> None:
    """Negative control: only a genuine flip may reach the sign gate."""
    _save_chase_format(db, recipe=_stale_recipe_dict())

    decision = route_pdf_import(_subdollar_doc(), db)

    assert decision.rederived is True
    assert decision.rederived_from_sign is None

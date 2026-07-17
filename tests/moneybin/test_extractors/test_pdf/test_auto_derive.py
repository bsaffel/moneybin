"""Tests for auto-derive: Recipe derivation from high-confidence PDF extraction."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from moneybin.extractors.pdf.auto_derive import credit_card_markers, derive_recipe
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.extractors.pdf.metadata import StatementMetadata

# ---------------------------------------------------------------------------
# Minimal metadata fixture (no balances — only structure matters for derivation)
# ---------------------------------------------------------------------------

_EMPTY_META = StatementMetadata(
    account_id=None,
    period_start=None,
    period_end=None,
    opening_balance=None,
    closing_balance=None,
)


def _make_doc(
    header: list[str],
    rows: list[list[str]],
    extra_tables: list[PdfTable] | None = None,
) -> PdfDocument:
    """Build a PdfDocument with one (or more) tables."""
    tables: list[PdfTable] = [PdfTable(page=1, header=header, rows=rows)]
    if extra_tables:
        tables.extend(extra_tables)
    return PdfDocument(source_file="stmt.pdf", tables=tables)


# ---------------------------------------------------------------------------
# Scenario 1: Date / Description / Amount  (single amount column)
# ---------------------------------------------------------------------------


def test_derive_single_amount_column_returns_recipe() -> None:
    """Standard 3-column layout yields a Recipe with negative_is_expense."""
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[
            ["01/15/2024", "Coffee Shop", "-4.50"],
            ["01/16/2024", "Paycheck", "1,500.00"],
        ],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None


def test_derive_single_amount_sign_convention() -> None:
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    assert recipe.sign_convention == "negative_is_expense"


def test_derive_single_amount_number_format() -> None:
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        # Include at least one negative so the sign-convention sanity check
        # (added for the codex P1 / claude zero-delta finding) doesn't bail.
        rows=[["01/15/2024", "Coffee Shop", "-1,234.56"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    assert recipe.number_format == "us"


def test_derive_single_amount_field_count() -> None:
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    assert len(recipe.fields) == 3


def test_derive_single_amount_date_format_detected() -> None:
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[
            ["01/15/2024", "Coffee Shop", "-4.50"],
            ["01/16/2024", "Paycheck", "1,500.00"],
        ],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    date_field = next(f for f in recipe.fields if f.cast == "date")
    assert date_field.date_format == "%m/%d/%Y"


def test_derive_single_amount_routing_is_transactions() -> None:
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    assert recipe.routing == "transactions"


def test_derive_single_amount_fields_have_correct_casts() -> None:
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    casts = {f.name: f.cast for f in recipe.fields}
    assert casts["Date"] == "date"
    assert casts["Description"] == "str"
    assert casts["Amount"] == "decimal"


def test_derive_single_amount_row_split() -> None:
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    assert recipe.row_split == r"\s{2,}"


# ---------------------------------------------------------------------------
# Scenario 2: Date / Desc / Debit / Credit  (split columns)
# ---------------------------------------------------------------------------


def test_derive_debit_credit_returns_none_phase_2a() -> None:
    r"""Debit/Credit layout is deferred to Phase 2b — auto_derive returns None.

    Rationale: \s{2,} row_split collapses blank columns positionally, so
    debit-only and credit-only rows produce identical token lists with no way
    to disambiguate without column-position hints on the recipe. Gating here
    routes the document cleanly to seed (no_transaction_table) instead of
    extracting zero rows mid-flight.
    """
    doc = _make_doc(
        header=["Date", "Desc", "Debit", "Credit"],
        rows=[
            ["01/15/2024", "Coffee Shop", "4.50", ""],
            ["01/16/2024", "Paycheck", "", "1500.00"],
        ],
    )
    assert derive_recipe(doc, _EMPTY_META) is None


def test_derive_withdraw_deposit_returns_none_phase_2a() -> None:
    """Withdraw/Deposit variant also deferred (same Phase 2b blocker)."""
    doc = _make_doc(
        header=["Date", "Description", "Withdrawals", "Deposits"],
        rows=[["01/15/2024", "Coffee Shop", "4.50", ""]],
    )
    assert derive_recipe(doc, _EMPTY_META) is None


# ---------------------------------------------------------------------------
# Scenario 3: Non-transaction layout → None
# ---------------------------------------------------------------------------


def test_non_transaction_layout_returns_none() -> None:
    """A 1099 form with Box 1/Box 2 headers is not a transaction table."""
    doc = _make_doc(
        header=["Box 1", "Box 2", "Amount"],
        rows=[["1099-INT", "Interest", "42.00"]],
    )
    result = derive_recipe(doc, _EMPTY_META)
    assert result is None


def test_no_date_column_returns_none() -> None:
    """Table without a date-like first column is not transactions."""
    doc = _make_doc(
        header=["Payer", "Recipient", "Amount"],
        rows=[["Alice", "Bob", "100.00"]],
    )
    result = derive_recipe(doc, _EMPTY_META)
    assert result is None


def test_too_few_columns_returns_none() -> None:
    """Fewer than 3 columns → not a transaction table."""
    doc = _make_doc(
        header=["Date", "Amount"],
        rows=[["01/15/2024", "-4.50"]],
    )
    result = derive_recipe(doc, _EMPTY_META)
    assert result is None


def test_no_amount_column_returns_none() -> None:
    """Date column present but no amount/debit/credit variant → None."""
    doc = _make_doc(
        header=["Date", "Description", "Category"],
        rows=[["01/15/2024", "Coffee Shop", "Food"]],
    )
    result = derive_recipe(doc, _EMPTY_META)
    assert result is None


def test_all_positive_amounts_returns_none() -> None:
    """All-positive amounts → ambiguous sign convention → seed fallback.

    Credit-card statements use positive=expense / negative=payment, the
    opposite of bank statements. auto_derive defaults to negative_is_expense,
    so a recipe built from an all-positive sample would corrupt signs on
    import. Detecting the absence of any negative in the sample is a
    cheap signal that the layout doesn't match the default convention.
    Regression for the codex P1 + claude zero-delta-month findings.
    """
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[
            # Credit-card style: charges shown as positive
            ["01/15/2024", "Coffee Shop", "4.50"],
            ["01/16/2024", "Restaurant", "32.18"],
            ["01/17/2024", "Gas Station", "55.00"],
        ],
    )
    assert derive_recipe(doc, _EMPTY_META) is None


# ---------------------------------------------------------------------------
# Scenario 4: Largest-table selection
# ---------------------------------------------------------------------------


def test_picks_largest_table() -> None:
    """Small junk table on page 1, large transaction table on page 2 → uses larger."""
    small = PdfTable(
        page=1,
        header=["Date", "Note", "Amount"],
        rows=[["01/01/2024", "Fee", "-5.00"]],
    )
    large = PdfTable(
        page=2,
        header=["Date", "Description", "Amount"],
        rows=[
            ["01/15/2024", "Coffee Shop", "-4.50"],
            ["01/16/2024", "Paycheck", "1,500.00"],
            ["01/17/2024", "Rent", "-1,200.00"],
        ],
    )
    doc = PdfDocument(source_file="stmt.pdf", tables=[small, large])
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    # Anchor is the first header of the SELECTED (largest) table.
    assert recipe.row_region.start_anchor == "Date"
    # Recipe field count matches the selected (large) table — 3 columns, not 2.
    assert len(recipe.fields) == 3


# ---------------------------------------------------------------------------
# Scenario 5: Ambiguous date format → None
# ---------------------------------------------------------------------------


def test_ambiguous_date_format_returns_none() -> None:
    """Mixed date formats that don't parse cleanly under a single format → None."""
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[
            ["01/15/2024", "Coffee Shop", "-4.50"],
            ["2024-01-16", "Paycheck", "1,500.00"],  # ISO format mixed in
        ],
    )
    result = derive_recipe(doc, _EMPTY_META)
    assert result is None


# ---------------------------------------------------------------------------
# Scenario 6: Empty tables list → None
# ---------------------------------------------------------------------------


def test_empty_document_returns_none() -> None:
    doc = PdfDocument(source_file="empty.pdf", tables=[])
    result = derive_recipe(doc, _EMPTY_META)
    assert result is None


# ---------------------------------------------------------------------------
# Metadata anchors
# ---------------------------------------------------------------------------


def test_metadata_anchors_populated() -> None:
    """Derived recipe carries non-empty metadata_anchors from DEFAULT_ANCHORS."""
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    assert recipe.metadata_anchors is not None
    assert len(recipe.metadata_anchors) > 0


def test_metadata_anchor_field_names() -> None:
    """Metadata anchors include expected field names from DEFAULT_ANCHORS."""
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    assert recipe.metadata_anchors is not None
    names = {f.name for f in recipe.metadata_anchors}
    assert "account_id" in names
    assert "period_start" in names
    assert "period_end" in names
    assert "opening_balance" in names
    assert "closing_balance" in names


# ---------------------------------------------------------------------------
# Region anchors
# ---------------------------------------------------------------------------


def test_region_start_anchor_uses_first_header_only() -> None:
    """start_anchor is just the first header word so it survives layout=True spacing.

    Regression for the claude CONSIDER finding: pdfplumber's layout=True
    emits proportional whitespace, so a multi-word anchor with a fixed
    separator never matches a real PDF. The single first-header word always
    appears at the start of the row-region in extracted text.
    """
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    assert recipe.row_region.start_anchor == "Date"


def test_region_end_anchor_is_total() -> None:
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    assert recipe.row_region.end_anchor == "Total:"


# ---------------------------------------------------------------------------
# Date format variants
# ---------------------------------------------------------------------------


def test_date_format_iso() -> None:
    """ISO date format (%Y-%m-%d) is detected correctly."""
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[
            ["2024-01-15", "Coffee Shop", "-4.50"],
            ["2024-01-16", "Paycheck", "1,500.00"],
        ],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    date_field = next(f for f in recipe.fields if f.cast == "date")
    assert date_field.date_format == "%Y-%m-%d"


def test_date_format_short_year() -> None:
    """Short year format (%m/%d/%y) is detected correctly."""
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[
            ["01/15/24", "Coffee Shop", "-4.50"],
            ["01/16/24", "Paycheck", "1,500.00"],
        ],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
    date_field = next(f for f in recipe.fields if f.cast == "date")
    assert date_field.date_format == "%m/%d/%y"


# ---------------------------------------------------------------------------
# No rows → None (empty table can't determine date/number format)
# ---------------------------------------------------------------------------


def test_transaction_table_with_no_rows_returns_none() -> None:
    """A correctly shaped header with zero data rows → not high confidence."""
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[],
    )
    result = derive_recipe(doc, _EMPTY_META)
    assert result is None


# ---------------------------------------------------------------------------
# Round-trip guard: derived recipe executes on document text
# ---------------------------------------------------------------------------


def test_round_trip_single_amount() -> None:
    """derive_recipe produces a Recipe that execute_recipe can run (no crash)."""
    from moneybin.extractors.pdf.recipe import execute_recipe

    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[
            ["01/15/2024", "Coffee Shop", "-4.50"],
            ["01/16/2024", "Paycheck", "1500.00"],
        ],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None

    doc_text = (
        "Account Summary\n"
        "Date  Description  Amount\n"
        "01/15/2024  Coffee Shop  -4.50\n"
        "01/16/2024  Paycheck  1500.00\n"
        "Total:\n"
    )
    result = execute_recipe(recipe, doc_text)
    # Two data rows should parse
    assert len(result.rows) == 2


# ---------------------------------------------------------------------------
# Unruled statements: no pdfplumber table, rows live only in text_lines
# ---------------------------------------------------------------------------


def _make_text_only_doc(text_lines: list[str]) -> PdfDocument:
    """Build a PdfDocument with NO tables — rows exist only as text lines.

    This is what a real bank statement produces: pdfplumber's extract_tables()
    keys on drawn ruling lines, and real statements are whitespace-aligned
    without them.
    """
    return PdfDocument(source_file="stmt.pdf", tables=[], text_lines=text_lines)


def test_derive_from_text_lines_when_no_ruled_table() -> None:
    """A whitespace-aligned statement with no ruled table still yields a Recipe.

    Root cause of F10: derivation read doc.tables (which requires ruling lines)
    while execution reads doc.text_lines. A real Chase statement produced
    tables=[] and derivation went blind, routing 10 real transactions to an
    opaque seed instead of core.
    """
    doc = _make_text_only_doc([
        "Chase Bank",
        "Account Number: 1234",
        "ACCOUNT ACTIVITY",
        "Date         Description          Amount",
        "01/02/2024   COFFEE SHOP          -4.50",
        "01/05/2024   PAYROLL DEPOSIT      2000.00",
        "01/09/2024   GROCERY MART         -73.21",
    ])

    recipe = derive_recipe(doc, _EMPTY_META)

    assert recipe is not None
    assert recipe.routing == "transactions"
    assert recipe.sign_convention == "negative_is_expense"


def test_derived_text_line_recipe_executes_on_the_same_document() -> None:
    r"""The recipe derived from text_lines must execute against that same text.

    Derivation and execution must agree by construction — they now share the
    \s{2,} splitter, so a recipe derived from text lines is self-consistent.
    """
    from moneybin.extractors.pdf.recipe import execute_recipe

    text_lines = [
        "Chase Bank",
        "ACCOUNT ACTIVITY",
        "Date         Description          Amount",
        "01/02/2024   COFFEE SHOP          -4.50",
        "01/05/2024   PAYROLL DEPOSIT      2000.00",
        "01/09/2024   GROCERY MART         -73.21",
    ]
    doc = _make_text_only_doc(text_lines)

    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None

    result = execute_recipe(recipe, "\n".join(text_lines))
    assert len(result.rows) == 3


def test_ruled_table_still_wins_when_present() -> None:
    """A document WITH a ruled table keeps deriving from it (no behavior change)."""
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None


def test_non_transaction_text_does_not_derive_a_recipe() -> None:
    """A positions statement (no date column) must NOT derive a transaction recipe.

    Guards the text-line fallback against over-reach: it must stay as strict as
    the ruled-table path, or investment statements would be misrouted.
    """
    doc = _make_text_only_doc([
        "Fidelity Investments",
        "Symbol    Shares    Price     Value",
        "AAPL      100       180.00    18000.00",
        "MSFT      50        350.00    17500.00",
    ])

    assert derive_recipe(doc, _EMPTY_META) is None


def test_derive_from_text_lines_across_pages_with_repeated_header() -> None:
    """A multi-page statement repeats its column header on each page.

    text_lines are flattened across pages, so the page-2 header line splits to
    the same width as a data row and was swallowed into the row set — poisoning
    date-format detection (every sample must parse) and killing derivation.
    Real statements are multi-page, so single-page-only derivation is no fix.
    """
    from moneybin.extractors.pdf.recipe import execute_recipe

    text_lines = [
        "Chase Bank",
        "Date         Description          Amount",
        "01/02/2024   COFFEE SHOP          -4.50",
        "01/05/2024   PAYROLL DEPOSIT      2000.00",
        # page 2 repeats the header
        "Date         Description          Amount",
        "01/09/2024   GROCERY MART         -73.21",
        "01/15/2024   UTILITIES            -150.00",
    ]
    doc = _make_text_only_doc(text_lines)

    recipe = derive_recipe(doc, _EMPTY_META)

    assert recipe is not None
    # Round-trip, not just `recipe is not None`: every row on both pages must
    # come back exactly once. Asserting only that a recipe derived would pass
    # even while the reconstructor double-collected page 2 (it hands the same
    # derived recipe back either way) — the duplication is only visible in the
    # rows the recipe actually yields.
    result = execute_recipe(recipe, "\n".join(text_lines))
    assert [(r["Date"], r["Amount"]) for r in result.rows] == [
        (date(2024, 1, 2), Decimal("-4.50")),
        (date(2024, 1, 5), Decimal("2000.00")),
        (date(2024, 1, 9), Decimal("-73.21")),
        (date(2024, 1, 15), Decimal("-150.00")),
    ]


def test_derive_across_pages_separated_by_a_footer() -> None:
    """Page footers must not truncate the reconstructed table to a single page.

    A real statement puts a footer ("Page 1 of 2") between pages. That line
    splits to a different width, ending the contiguous row run — so each page
    became its OWN synthesized table and only the largest survived selection.

    Here page 1 carries more rows but they are all deposits; the withdrawals sit
    on page 2. Selecting page 1 alone means `_has_any_negative_amount` sees no
    negative and derivation bails to seed — losing the whole statement — even
    though the document plainly has negative amounts.
    """
    from moneybin.extractors.pdf.recipe import execute_recipe

    text_lines = [
        "Chase Bank",
        "Date         Description          Amount",
        "01/02/2024   REFUND A             10.00",
        "01/03/2024   REFUND B             20.00",
        "01/04/2024   REFUND C             30.00",
        "Page 1 of 2",
        "Date         Description          Amount",
        "01/09/2024   GROCERY MART         -73.21",
        "Page 2 of 2",
    ]
    doc = _make_text_only_doc(text_lines)

    recipe = derive_recipe(doc, _EMPTY_META)

    assert recipe is not None
    # Round-trip: the page-2 row is the whole point — a recipe derived from
    # page 1 alone still satisfies `recipe is not None`, so only the executed
    # rows show whether page 2 survived.
    result = execute_recipe(recipe, "\n".join(text_lines))
    assert [(r["Date"], r["Amount"]) for r in result.rows] == [
        (date(2024, 1, 2), Decimal("10.00")),
        (date(2024, 1, 3), Decimal("20.00")),
        (date(2024, 1, 4), Decimal("30.00")),
        (date(2024, 1, 9), Decimal("-73.21")),
    ]


def test_page_break_header_repeat_does_not_duplicate_rows() -> None:
    """A repeated header must not have its rows collected twice.

    When no footer separates the pages, the page-1 run skipped the page-2 header
    and kept collecting — while page 2 also began its own run from that same
    header. Merging the two runs then counted every page-2 row twice. The repeat
    starts its own run regardless, so the earlier run must simply end there.
    """
    from moneybin.extractors.pdf.auto_derive import (
        _synthesize_tables_from_text,  # pyright: ignore[reportPrivateUsage]
    )

    doc = _make_text_only_doc([
        "Date         Description          Amount",
        "01/02/2024   COFFEE               -4.50",
        "Date         Description          Amount",
        "01/09/2024   GROCERY              -73.21",
    ])

    tables = _synthesize_tables_from_text(doc)

    assert len(tables) == 1
    assert tables[0].rows == [
        ["01/02/2024", "COFFEE", "-4.50"],
        ["01/09/2024", "GROCERY", "-73.21"],
    ]


def test_text_candidate_rows_stop_at_the_end_of_the_transaction_table() -> None:
    """A trailing date-led section must not be folded into the candidate's rows.

    The raw-text rung scans for date-led lines rather than a contiguous run (a
    wrapped description or page footer would end a contiguous run early, and an
    empty run reports the document as no-transaction-table — seeding exactly the
    statements this rung exists to escalate). Scanning to the end of the document
    instead of to the table's end sentinel means a "Daily Balance Summary" — its
    lines also open with a date — lands in the sample the locale probe reads.
    """
    from moneybin.extractors.pdf.auto_derive import (
        _text_transaction_candidate,  # pyright: ignore[reportPrivateUsage]
    )

    doc = _make_text_only_doc([
        "Wells Fargo",
        "Date         Description     Withdrawals   Deposits",
        "01/02/2024   COFFEE SHOP     4.50",
        "01/05/2024   PAYROLL                       2,000.00",
        "Ending Balance                             1,995.50",
        "Daily Balance Summary",
        "01/02/2024   -4.50",
        "01/05/2024   1,995.50",
    ])

    candidate = _text_transaction_candidate(doc)

    assert candidate is not None
    header, rows = candidate
    assert header == ["Date", "Description", "Withdrawals", "Deposits"]
    assert rows == [
        ["01/02/2024", "COFFEE SHOP", "4.50"],
        ["01/05/2024", "PAYROLL", "2,000.00"],
    ]


# ---------------------------------------------------------------------------
# credit_card_markers: which disclosures the confirm gate shows the user
# ---------------------------------------------------------------------------


def test_credit_card_markers_returns_matched_disclosures() -> None:
    """The detector reports WHICH disclosures matched — the confirm gate shows them."""
    doc = _make_text_only_doc([
        "CHASE SAPPHIRE",
        "Minimum Payment Due: $25.00",
        "Credit Limit: $10,000",
        "Date Description Amount",
    ])
    assert credit_card_markers(doc) == ("minimum payment", "credit limit")


def test_credit_card_markers_empty_for_checking_statement() -> None:
    doc = _make_text_only_doc(["CHASE TOTAL CHECKING", "Date Description Amount"])
    assert credit_card_markers(doc) == ()


# ---------------------------------------------------------------------------
# Real Chase credit-card layout (F10 close-out): a two-physical-line column
# header ("Date of" / "Transaction  ...  $ Amount"), section sub-headers
# interleaved among the rows, and MM/DD dates with the year only on a separate
# "Opening/Closing Date" line. Every synthetic fixture had a single-line header,
# no interstitial sections, and year-bearing dates — so the suite passed while
# every real statement seeded. These reproduce the real shape.
# ---------------------------------------------------------------------------


def test_derive_card_two_line_header_and_interleaved_sections() -> None:
    r"""A wrapped column header + section sub-headers still derive and round-trip.

    The real header spans two lines ("Date of" above "Transaction  Merchant ...
    $ Amount"), so no single line splits on \s{2,} into a date-led >=3-cell
    header; and PAYMENTS/PURCHASE/INTEREST section lines sit between the rows.
    The old reconstructor found no header and, even given one, broke its
    contiguous row run on the first section line — seeding the whole statement.
    Year-bearing dates here isolate the reconstruction from year inference.
    """
    from moneybin.extractors.pdf.recipe import execute_recipe

    text_lines = [
        "CHASE FREEDOM UNLIMITED",
        "Minimum Payment Due: $25.00",
        "Credit Limit: $10,000",
        "ACCOUNT ACTIVITY",
        "Date of",
        "Transaction          Merchant Name or Transaction Description $ Amount",
        "PAYMENTS AND OTHER CREDITS",
        "01/15/2025   PAYMENT THANK YOU          -100.00",
        "PURCHASE",
        "01/20/2025   COFFEE SHOP          25.00",
        "01/22/2025   BOOKSTORE          40.00",
        "INTEREST CHARGED",
        "01/22/2025   PURCHASE INTEREST CHARGE          12.00",
        "Totals Year-to-Date",
    ]
    doc = _make_text_only_doc(text_lines)

    recipe = derive_recipe(doc, _EMPTY_META)

    assert recipe is not None
    # Card disclosures present → the inverted convention is proposed.
    assert recipe.sign_convention == "negative_is_income"
    # Round-trip: the section headers must be skipped (not parsed as rows), and
    # every real transaction row — across all three sections — must come back.
    result = execute_recipe(recipe, "\n".join(text_lines))
    assert [(r["Date"], r["Amount"]) for r in result.rows] == [
        (date(2025, 1, 15), Decimal("-100.00")),
        (date(2025, 1, 20), Decimal("25.00")),
        (date(2025, 1, 22), Decimal("40.00")),
        (date(2025, 1, 22), Decimal("12.00")),
    ]


def test_derive_card_infers_year_from_opening_closing_date() -> None:
    """MM/DD rows resolve their year from the statement's billing period.

    Real card rows print MM/DD with no year; the year lives only on the
    "Opening/Closing Date  12/23/24 - 01/22/25" line. The cycle crosses year-end,
    so the year is per-row: 12/xx belongs to the opening year, 01/xx to the
    closing year. Replay-safe because each month carries its own period line.
    """
    from moneybin.extractors.pdf.recipe import execute_recipe

    text_lines = [
        "CHASE FREEDOM UNLIMITED",
        "Minimum Payment Due: $25.00",
        "Credit Limit: $10,000",
        "Opening/Closing Date   12/23/24 - 01/22/25",
        "ACCOUNT ACTIVITY",
        "Date of",
        "Transaction          Merchant Name or Transaction Description $ Amount",
        "PAYMENTS AND OTHER CREDITS",
        "12/26   PAYMENT THANK YOU          -100.00",
        "PURCHASE",
        "12/24   COFFEE SHOP          25.00",
        "01/15   BOOKSTORE          40.00",
        "INTEREST CHARGED",
        "01/22   PURCHASE INTEREST CHARGE          12.00",
        "Totals Year-to-Date",
    ]
    doc = _make_text_only_doc(text_lines)

    recipe = derive_recipe(doc, _EMPTY_META)

    assert recipe is not None
    result = execute_recipe(recipe, "\n".join(text_lines))
    assert [(r["Date"], r["Amount"]) for r in result.rows] == [
        (date(2024, 12, 26), Decimal("-100.00")),
        (date(2024, 12, 24), Decimal("25.00")),
        (date(2025, 1, 15), Decimal("40.00")),
        (date(2025, 1, 22), Decimal("12.00")),
    ]


def test_derive_declines_yearless_card_without_billing_period() -> None:
    """A wrapped-header MM/DD statement with no billing period must NOT derive.

    The rows carry no year, and no "Opening/Closing Date" line supplies one, so the
    executor could only guess. ``derive_recipe`` declines (returns None) rather than
    author a recipe that emits wrong dates — routing then escalates to the bridge
    (see ``test_routing``) instead of silently seeding. Pairs with
    ``test_derive_card_infers_year_from_opening_closing_date`` (identical shape WITH
    a period line) to isolate the period guard: with a period it derives, without
    one it declines — so a future refactor can't regress the decline into emitting
    wrong dates and stay green.
    """
    text_lines = [
        "CHASE FREEDOM UNLIMITED",
        "Minimum Payment Due: $25.00",
        "Credit Limit: $10,000",
        "ACCOUNT ACTIVITY",
        "Date of",
        "Transaction          Merchant Name or Transaction Description $ Amount",
        "PAYMENTS AND OTHER CREDITS",
        "12/26   PAYMENT THANK YOU          -100.00",
        "PURCHASE",
        "12/24   COFFEE SHOP          25.00",
        "Totals Year-to-Date",
    ]
    doc = _make_text_only_doc(text_lines)

    assert derive_recipe(doc, _EMPTY_META) is None


def test_derive_card_wider_than_three_cells_keeps_every_middle_column() -> None:
    """A shape-derived row with a middle column beyond date/desc/amount loses nothing.

    A wrapped card layout can split a row into more than three cells (here a
    separate post-date column sits between the transaction date and the merchant).
    The synthesized middle columns all canonicalise to the single `description`
    key; `_canonicalize_rows` must JOIN them rather than keep only the last, or the
    merchant/detail component is silently dropped while the row still reconciles
    and imports.
    """
    from moneybin.extractors.pdf.recipe import execute_recipe
    from moneybin.extractors.pdf.routing import (
        _canonicalize_rows,  # pyright: ignore[reportPrivateUsage] -- join-on-collision probe
    )

    text_lines = [
        "CHASE SAPPHIRE PREFERRED",
        "Minimum Payment Due: $25.00",
        "Opening/Closing Date   12/23/24 - 01/22/25",
        "ACCOUNT ACTIVITY",
        "Date of",
        "Transaction   Post Date   Merchant Name or Description   $ Amount",
        "12/24   12/26   COFFEE SHOP   25.00",
        "01/15   01/17   BOOKSTORE   40.00",
        "Totals Year-to-Date",
    ]
    doc = _make_text_only_doc(text_lines)

    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None

    extracted = execute_recipe(recipe, "\n".join(text_lines))
    canonical = _canonicalize_rows(recipe, extracted.rows)

    assert len(canonical) == 2
    # Both middle cells survive in the joined description — neither is dropped.
    assert canonical[0]["description"] == "12/26 COFFEE SHOP"
    assert canonical[1]["description"] == "01/17 BOOKSTORE"
    assert canonical[0]["amount"] == Decimal("25.00")

"""Tests for auto-derive: Recipe derivation from high-confidence PDF extraction."""

from __future__ import annotations

from moneybin.extractors.pdf.auto_derive import derive_recipe
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
    assert len(recipe.metadata_anchors) > 0


def test_metadata_anchor_field_names() -> None:
    """Metadata anchors include expected field names from DEFAULT_ANCHORS."""
    doc = _make_doc(
        header=["Date", "Description", "Amount"],
        rows=[["01/15/2024", "Coffee Shop", "-4.50"]],
    )
    recipe = derive_recipe(doc, _EMPTY_META)
    assert recipe is not None
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

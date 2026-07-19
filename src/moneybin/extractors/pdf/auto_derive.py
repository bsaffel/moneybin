"""Auto-derive a Recipe from a high-confidence deterministic PDF extraction.

Pure function: ``derive_recipe(doc, metadata) -> Recipe | None``.

Returns None when the document does not contain a recognisable transaction
table or when format detection is ambiguous — both signal "route to seed"
rather than "auto-derived recipe applies."

Round-trip contract
-------------------
The caller (Task 9 routing) is expected to verify that:

    execute_recipe(derived_recipe, doc_text)

reproduces the same rows the deterministic extraction found.  This module
is a pure function; it does not run that check itself.

Table-selection heuristic
-------------------------
A table is "transaction-shaped" when:
  - First column header matches a date-column pattern (case-insensitive)
  - Last column(s) contain an amount indicator (single "amount" OR
    a debit/withdraw + credit/deposit pair)
  - At least 3 columns total
  - At least 1 data row (zero rows → format detection impossible → None)

The largest matching table (most rows) is selected.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Literal

import regex as _re
from pydantic import ValidationError

from moneybin.extractors.pdf.column_names import (
    AMOUNT_NAME_RE as _AMOUNT_COL_RE,
)
from moneybin.extractors.pdf.column_names import (
    CREDIT_NAME_RE as _CREDIT_COL_RE,
)
from moneybin.extractors.pdf.column_names import (
    DATE_HEADER_RE as _DATE_COL_RE,
)
from moneybin.extractors.pdf.column_names import (
    DEBIT_NAME_RE as _DEBIT_COL_RE,
)
from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.extractors.pdf.metadata import (
    DEFAULT_ANCHORS,
    StatementMetadata,
    capture_metadata,
)
from moneybin.extractors.pdf.recipe import (
    FieldExtraction,
    Recipe,
    RegionAnchors,
    is_yearless_date_format,
)

logger = logging.getLogger(__name__)

# Column-classification regexes live in column_names (imported above) so
# both auto_derive (column → sign-convention) and routing (column → canonical
# row-dict key) stay in sync when a header synonym is added.

# ---------------------------------------------------------------------------
# Date / number format constants
# ---------------------------------------------------------------------------

_DATE_FORMATS: list[tuple[str, str]] = [
    ("%m/%d/%Y", r"\d{2}/\d{2}/\d{4}"),
    ("%Y-%m-%d", r"\d{4}-\d{2}-\d{2}"),
    ("%m/%d/%y", r"\d{2}/\d{2}/\d{2}"),
    # Year-less MM/DD: credit-card transaction rows print no year (it lives only
    # on the "Opening/Closing Date" line). Listed last so a sample that carries a
    # year matches a year-bearing format first; execute_recipe brackets the year
    # from the billing period at cast time.
    ("%m/%d", r"\d{2}/\d{2}"),
]

# "Does this cell open a transaction row?" — any date shape the deriver knows.
# Built from _DATE_FORMATS so a new supported format can't be added to one and
# forgotten in the other.
_ANY_DATE_RE = _re.compile(
    r"^(?:" + "|".join(pattern for _fmt, pattern in _DATE_FORMATS) + r")$"
)

# US / European number formats. Run via the `regex` package with a wall-clock
# timeout — the inner alternation (\d{1,3}(,\d{3})*|\d+) can backtrack
# exponentially on adversarial input like "1,2,3,4,5,6,7,8". This matches the
# security posture established in recipe.py + metadata.py for any pattern
# run against untrusted PDF cell text. (Req 9b dynamic bound)
_NUMBER_PATTERN_TIMEOUT_SEC = 0.05  # 50 ms — these patterns run per sample cell
# The integer part is OPTIONAL: real Chase statements print a sub-dollar fee as
# ".39" with no leading zero. Requiring a digit before the separator made that
# cell read as non-money, so the fee row was skipped by row-shape collection and
# dropped by execute_recipe's field match — the extracted total then missed the
# fee and failed +/-1c reconciliation, escalating the statement to seed.
# The optional group still must START with a digit, so ",.39" stays rejected.
_US_NUMBER_RE = _re.compile(r"^-?\$?(\d{1,3}(,\d{3})*|\d+)?\.\d{2}$")
_EUROPEAN_NUMBER_RE = _re.compile(r"^-?(\d{1,3}(\.\d{3})*|\d+)?,\d{2}$")


def _matches_us(sample: str) -> bool:
    try:
        return (
            _US_NUMBER_RE.match(sample, timeout=_NUMBER_PATTERN_TIMEOUT_SEC) is not None
        )
    except TimeoutError:
        return False


def _matches_european(sample: str) -> bool:
    try:
        return (
            _EUROPEAN_NUMBER_RE.match(sample, timeout=_NUMBER_PATTERN_TIMEOUT_SEC)
            is not None
        )
    except TimeoutError:
        return False


# Number of sample values to use for format detection (first N non-empty cells).
_SAMPLE_SIZE = 5

# The column separator for whitespace-aligned statement text. This is the single
# source of truth for three things that MUST agree, or a derived recipe parses
# different column boundaries than the ones it was derived from:
#   1. the recipe's `row_split`, which execute_recipe runs against document text,
#   2. the start-anchor scan, which matches a text line against a table header,
#   3. table reconstruction from text lines for unruled statements.
_ROW_SPLIT = r"\s{2,}"

# Candidate "end of transaction table" sentinels, ordered most-specific first.
# Auto-derive scans the document text AFTER the table's start_anchor and
# picks the first sentinel that appears; if none match we fall back to
# "Total:". The order matters — generic strings like "TOTAL" / "Total:"
# routinely appear inside transaction descriptions and "Year-to-Date Total:
# $5,000" lines, so we try the longer, structure-bound variants first.
_END_ANCHOR_CANDIDATES: tuple[str, ...] = (
    "Total Transactions",
    "Total New Charges",
    "Total New Activity",
    "Total Activity",
    "Total of Withdrawals",
    "Total of Deposits",
    "Total Charges",
    "Total Fees",
    "Total Payments",
    "Ending Balance",
    "ENDING BALANCE",
    "Closing Balance",
    "Totals",
    "TOTAL",
    "Total:",
)

# Cast defaults for metadata-anchor fields (field_name → cast literal).
_META_FIELD_CASTS: dict[str, Literal["str", "decimal", "date", "int"]] = {
    "account_id": "str",
    "period_start": "date",
    "period_end": "date",
    "opening_balance": "decimal",
    "closing_balance": "decimal",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def derivation_failure_reason(
    doc: PdfDocument,
) -> Literal[
    "no_transaction_table",
    "unsupported_number_format",
    "transaction_table_underivable",
]:
    """Explain why ``derive_recipe`` returned None, so routing can act on it.

    ``derive_recipe`` collapses every failure into ``None``, which is what let
    routing report them all as ``no_transaction_table`` — a reason excluded from
    bridge escalation — and silently seed statements the agent could have read.
    The three outcomes need different handling:

    - ``no_transaction_table`` — not a transaction document (a brokerage
      positions statement). Seeding is correct; the bridge would be off-target.
    - ``unsupported_number_format`` — a transaction table in a non-US number
      locale. NOT bridge-eligible: ``execute_recipe`` rejects a non-US
      ``number_format`` outright, so no recipe the agent could author would run.
      Escalating would egress the user's statement text to an LLM and prompt for
      a confirmation, then seed anyway.
    - ``transaction_table_underivable`` — a transaction table the deterministic
      rung couldn't crack (notably the deferred debit/credit split, the most
      common real bank layout). This is precisely what the bridge is for.
    """
    if not _looks_transactional_anywhere(doc):
        return "no_transaction_table"

    # Probe the locale off whatever the table is visible as — NOT off the strict
    # selector, which returns None for every debit/credit layout (no derivable
    # sign convention) and would therefore skip the locale check for exactly the
    # split-column statements the bridge exists to catch. See
    # _number_format_anywhere for why the raw-text fallback rung matters.
    number_fmt = _number_format_anywhere(doc)
    if number_fmt is not None and number_fmt != "us":
        return "unsupported_number_format"

    return "transaction_table_underivable"


def derive_recipe(doc: PdfDocument, _metadata: StatementMetadata) -> Recipe | None:
    """Derive a Recipe from *doc* if a transaction table can be identified.

    Args:
        doc: Parsed PDF document (Phase 1 IR).
        _metadata: StatementMetadata from capture_metadata.  Currently unused
            in derivation logic (metadata_anchors are frozen from
            DEFAULT_ANCHORS); the parameter is kept for forward-compatibility
            with Task 9 which passes it through.  Underscore prefix marks
            intentional non-use under ruff/pyright.

    Returns:
        A validated Recipe, or None if:
        - No transaction-shaped table is found.
        - Date format detection is ambiguous.
        - Number format detection is ambiguous.
    """
    table = _select_transaction_table(doc)
    if table is None:
        return None

    sign, amount_cols = _classify_sign_convention(table.header)
    if sign is None:
        return None  # shouldn't happen after _select_transaction_table, but be safe

    date_fmt, date_pattern = _detect_date_format(table)
    if date_fmt is None or date_pattern is None:
        return None
    if is_yearless_date_format(date_fmt) and not _period_capturable(doc):
        # MM/DD rows carry no year; without the billing period the executor can't
        # resolve one, so don't author a recipe that could only emit wrong dates.
        # Declining routes the statement to the bridge instead of miswriting it.
        return None

    number_fmt = _detect_number_format(table, amount_cols)
    if number_fmt is None:
        return None
    # execute_recipe only supports the US number format in Phase 2a.
    # Routing also catches NotImplementedError as a safety net, but
    # bailing here avoids polluting app.pdf_formats with recipes the
    # executor can't replay.
    if number_fmt != "us":
        return None

    # The sign convention is not recoverable from the amounts. A checking
    # statement (-50 groceries, +150 paycheck) and a card statement (+150
    # charges, -50 payment) have identical sign distributions AND both
    # reconcile cleanly, because reconciliation sums the raw signed amounts
    # either way. The document's own required disclosures are the only signal
    # that separates them.
    #
    # Markers present -> the statement names itself a card; derive the inverted
    # convention. The SERVICE gates this proposal behind a confirm — a wrong
    # inversion corrupts every row on this import and on every future replay,
    # so it is never applied silently (see ImportService._import_pdf).
    #
    # No markers and no negative amount anywhere -> genuinely ambiguous. Decline
    # rather than guess: derivation failing routes the statement to the bridge,
    # which can read it.
    if sign == "negative_is_expense":
        if credit_card_markers(doc):
            sign = "negative_is_income"
        elif not _has_any_negative_amount(table, amount_cols):
            return None

    fields = _build_fields(table.header, date_pattern, number_fmt)
    metadata_anchors = _build_metadata_anchors()
    # start_anchor: pick the actual document line that contains every table
    # header word in order — typically the header row itself. A bare
    # `table.header[0]` like "Date" routinely matches preamble lines such as
    # "Statement Date: 02/01/2024" before the transaction table starts, and
    # _carve_region would then carve the wrong region (often capturing
    # account summary rows that look like transactions, or nothing at all
    # when an end-anchor candidate appears in the preamble). The full
    # header line is uniquely specific to the transaction table.
    document_text = "\n".join(doc.text_lines)
    start_anchor = _detect_start_anchor(doc, table)
    # A shape-derived table (no header names it) is the winner only when it isn't
    # one of the named-header candidates. Such a recipe must anchor its end on a
    # terminal balance line, not a per-category subtotal that a card prints between
    # sections — else _carve_region truncates every later section on replay.
    shape_derived = table not in _named_transaction_tables(doc)
    end_anchor = _detect_end_anchor(
        document_text,
        start_anchor,
        _SHAPE_END_ANCHORS if shape_derived else _END_ANCHOR_CANDIDATES,
    )
    row_region = RegionAnchors(
        start_anchor=start_anchor,
        end_anchor=end_anchor,
    )

    try:
        return Recipe(
            metadata_anchors=metadata_anchors,
            row_region=row_region,
            row_split=_ROW_SPLIT,
            fields=fields,
            sign_convention=sign,
            number_format=number_fmt,
            routing="transactions",
        )
    except ValidationError as exc:
        # Phase 2a patterns are all hardcoded constants well under the
        # security bounds in Recipe._bound_patterns, so this is dead today.
        # The catch preserves the documented Recipe | None contract for
        # Phase 2b when bridge-authored patterns enter the pipeline and
        # could conceivably trip the static bounds.
        logger.warning(f"derive_recipe: Recipe validation failed — {exc}")
        return None


# ---------------------------------------------------------------------------
# Region anchor detection
# ---------------------------------------------------------------------------


def _detect_start_anchor(doc: PdfDocument, table: PdfTable) -> str:
    r"""Pick a start_anchor that uniquely identifies the transaction-table header line.

    Scans ``doc.text_lines`` for a line whose ``\s{2,}`` split is exactly
    ``table.header`` — i.e. the actual column-header row produced by
    ``layout=True`` extraction. Returns that stripped line. The full header
    line ("Date   Description   Amount") is far more specific than the bare
    first column name ("Date"): preamble lines such as "Statement Date:
    02/01/2024" routinely match a one-word anchor and cause
    ``_carve_region`` to start carving above the actual transaction table.

    Requiring exact-split-equality (not just "all tokens appear in order")
    rejects sentence-style preamble lines like "Date of Statement,
    Description: monthly, Amount due" that happen to contain the header
    tokens but aren't the header row. The ``\s{2,}`` separator matches the
    ``row_split`` the executor will use against this region, so a match
    here means the executor will see the same column boundaries.

    Falls back to ``table.header[0]`` when no scanned line splits exactly
    into the header list — the executor's full-text fallback in
    ``_carve_region`` is the same safety net used everywhere else and the
    misconfiguration is logged loudly there.
    """
    expected = list(table.header)
    for line in doc.text_lines:
        stripped = line.strip()
        if not stripped:
            continue
        cells = _re.split(_ROW_SPLIT, stripped)
        if cells == expected:
            return stripped
    # A shape-derived table has a synthesized (canonical) header that equals no
    # real line. Anchor on the wrapped header's amount line ("... $ Amount")
    # instead — the very line _synthesize_tables_from_row_shape begins collection
    # after, so the carved region and the derivation sample agree, and a preamble
    # "Amount Due" summary (not directly above the rows) is not mistaken for it.
    # Falls back to the bare first column name when no wrapped header is found.
    stripped_lines = [s for line in doc.text_lines if (s := line.strip())]
    cells_per_line = [_re.split(_ROW_SPLIT, s) for s in stripped_lines]
    header_idx = _shape_header_index(cells_per_line)
    if header_idx is not None:
        return stripped_lines[header_idx]
    return table.header[0]


def _detect_end_anchor(
    document_text: str,
    start_anchor: str,
    candidates: tuple[str, ...] = _END_ANCHOR_CANDIDATES,
) -> str:
    """Pick a transaction-table end_anchor present in *document_text*.

    Iterates *candidates* (most specific first) and returns the first one that
    appears AFTER ``start_anchor``. A leading ``start_anchor`` match constrains the
    search so a candidate string that also appears in the statement preamble (e.g.
    an issuer's tagline containing "TOTAL") doesn't get picked.

    A shape-derived recipe passes the narrow ``_SHAPE_END_ANCHORS`` (terminal
    balance sentinels only): the broad set's per-category subtotals ("Total Fees",
    "Total Payments") can sit BETWEEN a card's sections, and picking one as the
    persisted ``end_anchor`` would truncate every later section on replay.

    Falls back to ``"Total:"`` if no candidate matches — the executor's full-text
    fallback in ``_carve_region`` is the same safety net the previous hardcoded
    anchor relied on, and the misconfiguration is logged loudly there.
    """
    start_idx = document_text.find(start_anchor)
    search_from = start_idx + len(start_anchor) if start_idx != -1 else 0
    for candidate in candidates:
        if document_text.find(candidate, search_from) != -1:
            return candidate
    return "Total:"


# ---------------------------------------------------------------------------
# Table selection
# ---------------------------------------------------------------------------


def _is_transaction_shaped(table: PdfTable) -> bool:
    """Return True when the table matches the transaction-table heuristic."""
    headers = table.header
    if len(headers) < 3:
        return False
    if not table.rows:
        return False
    if not _DATE_COL_RE.match(headers[0]):
        return False
    sign, _ = _classify_sign_convention(headers)
    return sign is not None


def _synthesize_tables_from_text(doc: PdfDocument) -> list[PdfTable]:
    r"""Reconstruct candidate tables from ``doc.text_lines`` alone.

    pdfplumber's ``extract_tables()`` keys on drawn ruling lines, but real bank
    statements are typeset with whitespace-aligned columns and no rules — so
    ``doc.tables`` comes back empty and the rows survive only in ``text_lines``.
    Derivation used to read ``doc.tables`` exclusively while execution reads
    ``text_lines``, leaving the deriver blind to exactly the input the executor
    consumes: a real Chase statement extracted 0 transactions.

    Splitting on ``\s{2,}`` — the same ``row_split`` the derived recipe will
    execute with — means a table reconstructed here has the column boundaries
    the executor will see, so the recipe is self-consistent by construction.

    A header is any line splitting into >=3 cells whose first cell names a date
    column; its rows are the contiguous run of following lines that split to the
    same width. Contiguity matters: trailing summary lines that happen to share
    the column count would otherwise pollute date/number-format detection.

    ``text_lines`` are flattened across pages, and a multi-page statement repeats
    its column header at the top of each page, separated from the previous page's
    rows by a footer ("Page 1 of 3"). The footer splits to a different width and
    ends the run, so each page yields its own run. Runs sharing a header are
    therefore **merged** into one table: selecting the largest single run instead
    would derive from one page in isolation, and a page that happens to hold only
    deposits would trip the all-positive sign-convention guard and route the
    whole statement to seed.

    A repeated header line splits to the same width as a data row, so it is
    skipped rather than collected — otherwise it lands in the row set and poisons
    date-format detection, which requires every sample to parse.
    """
    runs: dict[tuple[str, ...], list[list[str]]] = {}
    cells_per_line = [
        _re.split(_ROW_SPLIT, line.strip()) if line.strip() else []
        for line in doc.text_lines
    ]

    for idx, cells in enumerate(cells_per_line):
        if len(cells) < 3 or not _DATE_COL_RE.match(cells[0]):
            continue
        width = len(cells)
        rows: list[list[str]] = []
        for row in cells_per_line[idx + 1 :]:
            if len(row) != width:
                break
            if row == cells:
                # Page-break repeat of the header. End this run rather than
                # skipping past it: the repeat starts a run of its own on the
                # outer loop, so continuing here would collect the next page's
                # rows twice — once in this run and once in that one.
                break
            rows.append(row)
        if rows:
            runs.setdefault(tuple(cells), []).extend(rows)

    return [
        PdfTable(page=1, header=list(header), rows=rows)
        for header, rows in runs.items()
    ]


# ---------------------------------------------------------------------------
# Shape-based reconstruction (wrapped / unnamed headers)
# ---------------------------------------------------------------------------


def _is_money_cell(cell: str) -> bool:
    """True when a cell parses as a money amount under either supported locale."""
    token = _strip_currency(cell)
    return _matches_us(token) or _matches_european(token)


def _is_transaction_row(cells: list[str]) -> bool:
    r"""True when a ``\s{2,}``-split line has the shape of a transaction row.

    Date-shaped first cell (any known format, including year-less MM/DD), a
    money-shaped last cell, and a description column between — the shape a row has
    regardless of whether the statement ever names its columns.
    """
    return (
        len(cells) >= 3
        and bool(_ANY_DATE_RE.match(cells[0]))
        and _is_money_cell(cells[-1])
    )


def _synthesize_header(rows: list[list[str]]) -> list[str]:
    """Canonical Date/…/Amount header for a shape-derived table, from its *rows*.

    The statement's own header wrapped or garbled beyond clean recovery, so name
    the columns canonically — the classifiers only need the first to read as a
    date column and the last as an amount column. Middle columns:

    - A column whose cells are ALL date-shaped is named "Posting Date" (only the
      first such column) so it casts as a date and canonicalises to the structured
      ``post_date`` field — a card layout's second date column — rather than being
      folded into free-text description.
    - Every other middle column gets a distinct ``Description_n`` name so
      ``execute_recipe``'s per-field row dict keeps each; they all canonicalise to
      the single ``description`` key, where ``routing._canonicalize_rows`` JOINS
      them (in field order) rather than dropping all but the last — so a row wider
      than three cells loses no merchant/detail component.
    """
    width = len(rows[0])
    if width == 3:
        return ["Date", "Description", "Amount"]
    # The primary (first-column) date pattern. A middle column is only "Posting
    # Date" if it parses under the SAME pattern — _build_fields casts every date
    # column with the primary's pattern, so a differently-formatted post-date
    # column would fail every row (→ no_rows). None when the first column isn't a
    # single recognisable date format (then no column can be a matching post date).
    primary_pattern = next(
        (
            pat
            for _fmt, pat in _DATE_FORMATS
            if all(_re.fullmatch(pat, r[0]) for r in rows)
        ),
        None,
    )
    middle: list[str] = []
    post_date_named = False
    for col in range(1, width - 1):
        col_matches_primary = primary_pattern is not None and all(
            _re.fullmatch(primary_pattern, r[col]) for r in rows
        )
        if col_matches_primary and not post_date_named:
            middle.append("Posting Date")
            post_date_named = True
        else:
            middle.append(f"Description_{col}")
    return ["Date", *middle, "Amount"]


def _shape_reconstructed_tables(doc: PdfDocument) -> list[PdfTable]:
    r"""Transaction tables recovered from data-row *shape*, or [] if a header names one.

    A wrapped multi-line header (Chase's "Date of" / "Transaction ... $ Amount")
    names no single line, so neither the ruled tables nor
    ``_synthesize_tables_from_text`` find it; the rows are still shape-unambiguous.
    The ``_has_debit_credit_header`` guard keeps a debit/credit statement (which
    DOES name its columns) from being force-fit to one synthesized Amount column
    here — it must stay deferred to the bridge.

    Single home for "reconstruct by shape, only when no debit/credit header claims
    the table," shared by the derivable selector (``_select_transaction_table``)
    and the statement detector (``_looks_transactional_anywhere``). When only the
    selector knew about it, a wrapped-header statement that failed derivation for
    any other reason (notably the year-less no-period decline) classified as
    ``no_transaction_table`` and was silently seeded — the exact failure mode shape
    reconstruction was added to close.
    """
    if _has_debit_credit_header(doc):
        return []
    return [
        t for t in _synthesize_tables_from_row_shape(doc) if _is_transaction_shaped(t)
    ]


def _has_shape_transaction_rows(doc: PdfDocument) -> bool:
    """True when unnamed date-led, money-tailed rows exist — a wrapped-header table.

    Looser than ``_shape_reconstructed_tables``: it recognises the document IS a
    transaction statement even when the rows don't form a single *derivable* table
    (mixed widths block derivation, but not the fact that it's transactional).
    Used only by ``_looks_transactional_anywhere`` so such a statement reports
    ``transaction_table_underivable`` and reaches the bridge instead of being
    silently seeded as ``no_transaction_table``. Guarded on no-debit/credit-header
    so a named split layout stays deferred, matching ``_shape_reconstructed_tables``.
    """
    if _has_debit_credit_header(doc):
        return False
    return any(
        _is_transaction_row(_re.split(_ROW_SPLIT, stripped))
        for line in doc.text_lines
        if (stripped := line.strip())
    )


def _names_debit_credit(headers: list[str]) -> bool:
    """True when *headers* name a date-led debit/credit pair (not a single amount).

    ``_is_transactional_header`` accepts a single-amount header OR a debit/credit
    pair; excluding the ones carrying an amount column leaves exactly the split
    layout.
    """
    return _is_transactional_header(headers) and not any(
        _AMOUNT_COL_RE.search(h) for h in headers
    )


def _has_debit_credit_header(doc: PdfDocument) -> bool:
    """True when a ruled or text line names a debit/credit (split) transaction table.

    This is the ONLY named layout shape reconstruction must defer to. A
    single-amount named header is already claimed upstream by
    ``_synthesize_tables_from_text`` (so it never reaches shape reconstruction); a
    debit/credit header names its columns but is deliberately not deterministically
    derivable (``_classify_sign_convention`` rejects the pair), so without this
    guard shape reconstruction would force-fit it to one synthesized Amount column.

    Scoped to debit/credit rather than ANY named header so an unrelated
    single-amount mini-table elsewhere in the document (a "Recent Payments" box)
    doesn't suppress reconstruction of the real wrapped transaction table.
    """
    if any(_names_debit_credit(t.header) for t in doc.tables):
        return True
    return any(
        _names_debit_credit(_re.split(_ROW_SPLIT, stripped))
        for line in doc.text_lines
        if (stripped := line.strip())
    )


# Only these truly-terminal balance sentinels stop shape-row collection. The
# broader _END_ANCHOR_CANDIDATES set includes per-category subtotals ("Total Fees",
# "Total Payments", "Totals") that a real card statement can print BETWEEN sections
# — using them as a hard stop would truncate every later section. A balance line is
# unambiguously the end of activity.
_SHAPE_END_ANCHORS: tuple[str, ...] = (
    "Ending Balance",
    "ENDING BALANCE",
    "Closing Balance",
)


def _opens_shape_end_anchor(cells: list[str]) -> bool:
    """True when a non-row line opens a terminal balance sentinel (end of activity)."""
    return bool(cells) and cells[0].startswith(_SHAPE_END_ANCHORS)


def _shape_header_index(cells_per_line: list[list[str]]) -> int | None:
    r"""Index of the wrapped header's amount line, or None if absent.

    An amount-naming non-row line ("... $ Amount") that is directly above the first
    transaction row (tolerating single-cell section sub-headers between them) is the
    second physical line of the wrapped header. Starting collection just after it
    excludes a preamble line that happens to be date-led + money-tailed (a
    due-date/amount summary) from both the derivation sample and the start-anchor
    scan. Distinct from a preamble "Amount Due" summary, which is NOT immediately
    followed by transaction rows. None → no wrapped header found; the shape rung
    then declines entirely (this isn't the wrapped-header layout it targets).
    """
    for i, cells in enumerate(cells_per_line):
        if _is_transaction_row(cells) or len(cells) <= 1:
            continue
        if not any(_AMOUNT_COL_RE.search(c) for c in cells):
            continue
        # An amount-naming non-row line is the wrapped header iff the next
        # substantive (non-single-cell) line is a transaction row.
        for nxt in cells_per_line[i + 1 :]:
            if len(nxt) <= 1:
                continue  # a section sub-header between header and first row
            if _is_transaction_row(nxt):
                return i
            break  # next substantive line isn't a row → not the header; keep looking
    return None


def _synthesize_tables_from_row_shape(doc: PdfDocument) -> list[PdfTable]:
    r"""Reconstruct a transaction table from data-row shape when no header names it.

    Real credit-card statements wrap the column header across two physical lines
    ("Date of" / "Transaction  Merchant ...  $ Amount"), so no single line
    ``\s{2,}``-splits into a date-led >=3-cell header and
    ``_synthesize_tables_from_text`` finds nothing. The rows themselves are
    unambiguous — a date-shaped first cell, a money-shaped last cell — so collect
    them by shape, skipping the interleaved section sub-headers ("PAYMENTS AND
    OTHER CREDITS", "PURCHASE", "INTEREST CHARGED") that split to a single cell,
    and synthesize a canonical header of the rows' width.

    Bounds that keep the sample honest and the layout unambiguous:

    - **Start after the wrapped header.** Collection begins just after the header's
      amount line (``_shape_header_index``), so a preamble line that happens to be
      date-led + money-tailed isn't sampled or mistaken for the region's first row.
    - **Stop at a balance sentinel.** Only a terminal balance line
      (``_SHAPE_END_ANCHORS``) ends collection — NOT the broad end-anchor set,
      whose per-category subtotals ("Total Fees") can sit between sections and would
      truncate every later one.
    - **Uniform width.** Every collected row must share one width. ``execute_recipe``
      splits raw text and drops any line whose cell count != the recipe's field
      count, so a lone row whose description carries an internal 2+-space gap (an
      extra cell) would be silently dropped from extraction — and if the dropped
      amounts net near zero, reconciliation still passes and those rows vanish with
      nothing surfaced. Refuse to reconstruct rather than derive a partial recipe.
    - **Single amount column.** If the rows carry money in the LAST TWO cells
      (amount + running balance, or a debit/credit pair), which trailing column is
      the transaction amount is ambiguous — defer to the bridge rather than mislabel
      a balance as Amount. (A wrapped-header debit/credit statement evades the
      named-header guard, so it is caught here by row shape.)

    Caller-guarded: only fires when no debit/credit header names the columns.
    """
    cells_per_line = [
        _re.split(_ROW_SPLIT, stripped) if (stripped := line.strip()) else []
        for line in doc.text_lines
    ]
    header_idx = _shape_header_index(cells_per_line)
    if header_idx is None:
        # No wrapped-header amount line: this isn't the wrapped-header shape this
        # rung targets, and scanning the whole document unbounded would risk
        # folding preamble/summary rows into the sample. Refuse; the statement
        # still reaches the bridge via _has_shape_transaction_rows.
        return []
    scan_from = header_idx + 1

    rows: list[list[str]] = []
    for cells in cells_per_line[scan_from:]:
        if not cells:
            continue
        if _is_transaction_row(cells):
            rows.append(cells)
        elif rows and _opens_shape_end_anchor(cells):
            break  # terminal balance sentinel — activity has ended
    if not rows:
        return []
    width = len(rows[0])
    if any(len(r) != width for r in rows):
        return []  # rows must be one uniform width — see the docstring
    if all(_is_money_cell(r[-2]) for r in rows):
        return []  # money in the last two cells → debit/credit or running balance
    return [PdfTable(page=1, header=_synthesize_header(rows), rows=rows)]


def _period_capturable(doc: PdfDocument) -> bool:
    """True when the document carries both billing-period dates.

    A year-less MM/DD statement is only derivable if the executor can resolve each
    row's year from the period; derive_recipe refuses to author a recipe that
    can't, routing to the bridge instead.
    """
    md = capture_metadata("\n".join(doc.text_lines))
    return md.period_start is not None and md.period_end is not None


def _is_transactional_header(headers: list[str]) -> bool:
    """Return True when *headers* name a transaction table, derivable or not.

    Deliberately looser than ``_is_transaction_shaped``: it accepts a
    debit/credit column pair, which ``_classify_sign_convention`` rejects
    because the deterministic executor can't disambiguate the blank side
    (deferred to Phase 2b).

    That distinction is the whole point. "Is this a statement?" and "can I derive
    a recipe for it?" are different questions, and answering the first with the
    second classes the most common real bank layout ("Withdrawals | Deposits") as
    *not a transaction document* — silently seeding it instead of escalating to
    the agent that could read it.
    """
    if len(headers) < 3 or not _DATE_COL_RE.match(headers[0]):
        return False
    return bool(_amount_like_columns(headers))


def _amount_like_columns(headers: list[str]) -> list[int]:
    """Indices of every money-bearing column — amount, or the debit/credit pair.

    Unlike ``_classify_sign_convention`` this doesn't care whether the layout is
    *derivable*; it only answers "which columns hold numbers", which is all a
    number-locale probe needs. The classifier rejects the debit/credit pair
    outright, so reusing it here would leave split-column statements unprobed.
    """
    amounts = [i for i, h in enumerate(headers) if _AMOUNT_COL_RE.search(h)]
    if amounts:
        return amounts
    # The debit/credit pair only counts as money-bearing when BOTH sides are
    # present — a lone "Debit" column is not the split layout, and treating it
    # as one would misclassify tables this predicate is meant to exclude.
    debits = [i for i, h in enumerate(headers) if _DEBIT_COL_RE.search(h)]
    credits = [i for i, h in enumerate(headers) if _CREDIT_COL_RE.search(h)]
    if debits and credits:
        return sorted(debits + credits)
    return []


def _select_loose_transaction_table(doc: PdfDocument) -> PdfTable | None:
    """Largest transaction table, derivable or not (ruled, else reconstructed)."""
    candidates = [
        t
        for t in (*doc.tables, *_synthesize_tables_from_text(doc))
        if _is_transactional_header(t.header) and t.rows
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda t: len(t.rows))


def _text_transaction_candidate(
    doc: PdfDocument,
) -> tuple[list[str], list[list[str]]] | None:
    r"""Header cells + dated rows of a statement visible *only* in the raw text.

    The last-resort reader, for the one layout no table reconstructs from: an
    unruled debit/credit statement has exactly one blank side per row, and
    ``\s{2,}`` collapses it — so its rows split to *fewer* cells than its header
    and ``_synthesize_tables_from_text``'s width match rejects them on the first
    data row. A transaction-shaped header line with dated rows beneath it is
    still unambiguous evidence of a statement, and those rows still carry the
    document's money tokens, which is all the two callers need.

    The returned rows are **ragged** — their widths differ from the header's and
    from each other. Never index them by a header column position; treat them as
    an unordered bag of cells (see ``_money_tokens``). Returning a ``PdfTable``
    would invite exactly that misuse, which is why this returns a plain pair.

    Two bounds keep a later date-led section (a "Daily Balance Summary") from
    folding its rows into the sample: the scan stops at the table's end sentinel,
    and — since a document might carry no sentinel at all — after ``_SAMPLE_SIZE``
    rows, which is every row the only consumer reads. The header, the other
    consumer, doesn't depend on the rows.

    It deliberately does NOT require rows to be *contiguous* with the header the
    way ``_synthesize_tables_from_text`` does: a wrapped description line or a
    page footer ends a contiguous run early, and a run that comes back empty
    reports the document as no-transaction-table — seeding the very statements
    this rung exists to escalate. The residue is a document with no sentinel AND
    fewer than ``_SAMPLE_SIZE`` transaction rows, where a trailing dated section
    can still reach the sample; it shares the document's locale, so it cannot
    change the answer.
    """
    lines = [line.strip() for line in doc.text_lines]
    cells_per_line = [_re.split(_ROW_SPLIT, line) if line else [] for line in lines]
    for idx, cells in enumerate(cells_per_line):
        if not _is_transactional_header(cells):
            continue
        rows: list[list[str]] = []
        for offset in range(idx + 1, len(cells_per_line)):
            if _opens_end_anchor(lines[offset]) or len(rows) == _SAMPLE_SIZE:
                break
            row = cells_per_line[offset]
            if row and _ANY_DATE_RE.match(row[0]):
                rows.append(row)
        if rows:
            return cells, rows
    return None


def _opens_end_anchor(line: str) -> bool:
    """True when *line* opens one of the document's end-of-transaction sentinels.

    Safe to test against the raw line: a transaction row starts with its date, so
    only a summary line can start with one of these.
    """
    return any(line.startswith(candidate) for candidate in _END_ANCHOR_CANDIDATES)


def _looks_transactional_anywhere(doc: PdfDocument) -> bool:
    """Return True when *doc* is a bank statement, derivable or not.

    Walks the named-header tables (ruled + text-reconstructed), then unnamed
    date-led/money-tailed rows (a wrapped/unnamed header), then the raw text lines
    — load-bearing for the most common real layout; see
    ``_text_transaction_candidate``. The shape rung here is deliberately LOOSER
    than the one ``_select_transaction_table`` uses: derivation refuses a
    mixed-width shape table, but a mixed-width statement is still a statement, so
    "is it a statement?" stays True (→ ``transaction_table_underivable`` → bridge)
    even where "can I derive it?" is False. See ``_has_shape_transaction_rows``.
    """
    return (
        _select_loose_transaction_table(doc) is not None
        or _has_shape_transaction_rows(doc)
        or _text_transaction_candidate(doc) is not None
    )


def _named_transaction_tables(doc: PdfDocument) -> list[PdfTable]:
    """Transaction-shaped tables that a header names — ruled or text-reconstructed."""
    return [
        t
        for t in (*doc.tables, *_synthesize_tables_from_text(doc))
        if _is_transaction_shaped(t)
    ]


def _select_transaction_table(doc: PdfDocument) -> PdfTable | None:
    """Return the transaction-shaped table with the MOST rows, across all rungs.

    Named tables (ruled or text-reconstructed) and the shape-reconstructed table
    (a wrapped/unnamed header) are all candidates; the real transaction table is
    the largest. Selecting by row count rather than "first non-empty rung" keeps an
    unrelated small named table — a "Recent Payments" box — from suppressing
    recovery of the real wrapped transaction table via shape reconstruction. Shape
    reconstruction stays guarded (``_shape_reconstructed_tables``) so a debit/credit
    statement is never force-fit here.
    """
    candidates = [*_named_transaction_tables(doc), *_shape_reconstructed_tables(doc)]
    if not candidates:
        return None
    return max(candidates, key=lambda t: len(t.rows))


def recipe_polarity_fits(recipe: Recipe, doc: PdfDocument) -> bool:
    """False when replaying *recipe* onto *doc* would invert the sign of every row.

    ``derive_recipe`` refuses to author a ``negative_is_expense`` recipe for a
    document whose sampled rows hold no negative amount: the convention is
    ambiguous there, and a credit-card layout (positive = expense) would import
    every charge as income. Reconciliation does not catch it — a statement whose
    balance delta equals the sum of its positive amounts reconciles cleanly with
    the signs exactly backwards.

    Routing replays a saved recipe **before** it ever calls ``derive_recipe``, so
    those guards are skipped on the replay path entirely. This re-applies them.
    Until unruled statements became derivable they never authored a saved format
    and never matched one, so the gap was unreachable; now a checking statement's
    saved recipe can fingerprint-match a same-issuer credit-card statement with
    the same columns and page count, and replay would write its charges as income.

    Note the card check does the real work: a card statement with even one payment
    row holds a negative amount, so ``_has_any_negative_amount`` alone would accept
    it. That is the whole hazard — a payment row is not evidence the document
    spends in the negative direction.

    The guard is symmetric. A ``negative_is_income`` (card) recipe replayed onto a
    document with no card disclosures is refused for exactly the same reason a
    ``negative_is_expense`` (bank) recipe is refused on a card: replay skips
    derivation entirely, so the derivation-time guards never run.

    ``sign_ratified`` is the one thing that outranks all of it — see below.
    """
    if recipe.sign_ratified:
        # A human overrode the detector with an explicit `sign=` for this format.
        # The marker scan is a text heuristic; a human's assertion about their own
        # statement beats it. Deferring is not optional: the override exists to
        # correct a FALSE POSITIVE, and a false positive is by definition a
        # document carrying card markers — so the guard below would refuse the
        # corrected recipe on the very evidence the human just overruled, forcing
        # the same override every month. (The mirror case is the same: a user who
        # declares `negative_is_income` on a card that prints none of the five
        # disclosures would be refused by the marker check.)
        #
        # Residual risk, accepted: the decision is keyed on the layout fingerprint
        # (issuer + column headers + page count), so a genuine card statement that
        # fingerprints identically to the overridden format inherits the human's
        # convention. That is the cost of a durable override, and the reason the
        # replay is surfaced to the user (ImportResult.sign_override_replayed)
        # rather than applied silently.
        return True

    markers = credit_card_markers(doc)

    if recipe.sign_convention == "negative_is_income":
        # Mirror of the guard below. A saved card recipe replayed onto a document
        # that no longer names itself a card would invert a checking statement's
        # every row — the same corruption as the bank-recipe-onto-card case,
        # pointed the other way. Fingerprint (issuer + headers + page_bucket)
        # cannot separate them; only the disclosures can.
        return bool(markers)

    if recipe.sign_convention != "negative_is_expense":
        return True
    if markers:
        return False
    table = _select_transaction_table(doc)
    if table is None:
        # No table to judge against — leave the decision to the executor and the
        # reconciliation gate rather than blocking a replay on missing evidence.
        return True
    _, amount_cols = _classify_sign_convention(table.header)
    if not amount_cols:
        return True
    return _has_any_negative_amount(table, amount_cols)


def transaction_headers(doc: PdfDocument) -> list[str] | None:
    """The transaction table's column headers, however that table is visible.

    Three rungs, most-specific first, because each finds a table the next can't:
    the *derivable* table (what ``derive_recipe`` will actually use), else the
    *loose* one (a debit/credit layout — real, just not derivable yet), else the
    raw-text candidate (an unruled debit/credit layout, which reconstructs as no
    table at all). Returns None only when the document isn't a statement.

    Exists for ``fingerprint``: characterising a statement by "the largest table"
    collapses every layout the deterministic rung can't crack onto one degenerate
    fingerprint, so two different institutions collide and replay each other's
    saved recipe against the wrong layout. The three rungs here are exactly the
    three ways ``derivation_failure_reason`` can see a table, kept in one place so
    the two can't drift.
    """
    for table in (_select_transaction_table(doc), _select_loose_transaction_table(doc)):
        if table is not None:
            return table.header
    candidate = _text_transaction_candidate(doc)
    if candidate is not None:
        header, _ = candidate
        return header
    return None


# ---------------------------------------------------------------------------
# Sign convention
# ---------------------------------------------------------------------------


def _classify_sign_convention(
    headers: list[str],
) -> tuple[
    Literal["negative_is_expense", "negative_is_income", "split_debit_credit"] | None,
    list[int],
]:
    """Classify the sign convention from column headers.

    Returns (sign_convention, amount_column_indices) or (None, []) if the
    layout is not recognisable.
    """
    # Single "amount" column (check all non-first headers, but spec says
    # "last column" — we check all to be tolerant of description-between patterns).
    amount_indices = [i for i, h in enumerate(headers) if _AMOUNT_COL_RE.search(h)]
    if amount_indices:
        return "negative_is_expense", amount_indices

    # Debit + credit pair — DEFERRED to Phase 2b. The recipe row_split
    # uses \s{2,} which collapses blank columns positionally, and bank
    # statements have exactly one blank side per row. Without positional
    # column hints (column_starts on the recipe) we can't disambiguate
    # debit-only from credit-only rows: both produce the same token list.
    # Bailing out cleanly here routes the document to seed with
    # reason="no_transaction_table" rather than silently dropping every
    # row mid-extract.
    debit_indices = [i for i, h in enumerate(headers) if _DEBIT_COL_RE.search(h)]
    credit_indices = [i for i, h in enumerate(headers) if _CREDIT_COL_RE.search(h)]
    if debit_indices and credit_indices:
        return None, []

    return None, []


# ---------------------------------------------------------------------------
# Date format detection
# ---------------------------------------------------------------------------


def _detect_date_format(table: PdfTable) -> tuple[str | None, str | None]:
    """Sample the first column of *table* and return (strptime_fmt, regex_pattern).

    Returns (None, None) if no single format parses all samples.
    """
    samples = [row[0].strip() for row in table.rows if row[0].strip()][:_SAMPLE_SIZE]
    if not samples:
        return None, None

    for fmt, pattern in _DATE_FORMATS:
        if _all_parse(samples, fmt):
            return fmt, pattern

    return None, None


def _all_parse(samples: list[str], fmt: str) -> bool:
    """Return True if every sample string parses under *fmt*."""
    for s in samples:
        try:
            datetime.strptime(s, fmt)
        except ValueError:
            return False
    return True


# ---------------------------------------------------------------------------
# Number format detection
# ---------------------------------------------------------------------------


def _detect_number_format(
    table: PdfTable,
    amount_col_indices: list[int],
) -> Literal["us", "european"] | None:
    """Sample amount columns and return the number format, or None if ambiguous."""
    samples: list[str] = []
    for row in table.rows[:_SAMPLE_SIZE]:
        for idx in amount_col_indices:
            cell = _strip_currency(row[idx])
            if cell and cell not in ("", "-"):
                samples.append(cell)

    return _number_format_from_samples(samples)


# Currency symbols that can sit on either side of an amount. Stripping only "$"
# left the symbol attached on every non-US statement, so no sample matched either
# number pattern, the locale came back unknown, and the document escalated to the
# bridge — which is precisely the egress a detected "european" was meant to
# prevent. The locale probe cares about the separators, never the currency.
_CURRENCY_CHARS = "$€£¥₹"


def _strip_currency(cell: str) -> str:
    """Strip whitespace and any currency symbol from an amount cell.

    Removes the symbol wherever it sits rather than trimming the ends: a negative
    amount writes the sign outside the symbol (``-€4,50``), so an end-trim would
    stop at the ``-`` and leave the ``€`` in place. A currency symbol never
    carries meaning inside a number, so dropping every occurrence is safe.
    """
    stripped = cell.translate({ord(sym): None for sym in _CURRENCY_CHARS})
    return stripped.strip()


def _number_format_from_samples(
    samples: list[str],
) -> Literal["us", "european"] | None:
    """Classify a bag of amount cells as one number locale, or None if ambiguous."""
    if not samples:
        # No real amount cells (only empties or "-" placeholders); can't
        # determine the format from this layout — refuse to auto-derive and let
        # the caller seed.
        return None

    # All samples must match a single format. Use the bounded helpers so a
    # pathological cell value can't ReDoS the format-detection scan.
    if all(_matches_us(s) for s in samples):
        return "us"
    if all(_matches_european(s) for s in samples):
        return "european"

    return None


def _money_tokens(rows: list[list[str]]) -> list[str]:
    """Money-shaped cells from ragged rows, where column position can't be trusted.

    ``_text_transaction_candidate``'s rows have no reliable column alignment, so
    column-indexed sampling (what ``_detect_number_format`` does) is unavailable.
    Select by *shape* instead: keep a cell only if it parses as money under one
    of the two locales. The two patterns disagree on the decimal separator and
    can never both match one token, so a document mixing them stays ambiguous and
    the caller escalates rather than guessing a locale.
    """
    samples: list[str] = []
    for row in rows[:_SAMPLE_SIZE]:
        for cell in row:
            token = _strip_currency(cell)
            if _matches_us(token) or _matches_european(token):
                samples.append(token)
    return samples


def _number_format_anywhere(doc: PdfDocument) -> Literal["us", "european"] | None:
    """The document's number locale, read from whatever the table is visible as.

    Walks the same three rungs as ``transaction_headers``, in the same order, and
    for the same reason: the locale must be read off the table ``derive_recipe``
    actually gave up on. Probing the *loose* table first would, on a document with
    more than one transaction-shaped table, read whichever is largest — so a small
    US ledger that failed derivation for an unrelated reason (ambiguous dates, the
    all-positive guard) alongside a bigger non-US sub-table would report
    ``unsupported_number_format`` and be seeded, when it should have reached the
    bridge.

    The text rung is load-bearing for the unruled debit/credit case, which
    reconstructs as no table at all. Skipping it leaves exactly those statements
    unprobed, so a European ``Withdrawals | Deposits`` statement escalates,
    egresses the user's text to an agent, and is then rejected outright by
    ``execute_recipe`` (which raises on any ``number_format != "us"``).
    """
    for table in (_select_transaction_table(doc), _select_loose_transaction_table(doc)):
        if table is not None:
            return _detect_number_format(table, _amount_like_columns(table.header))

    candidate = _text_transaction_candidate(doc)
    if candidate is None:
        return None
    _, rows = candidate
    return _number_format_from_samples(_money_tokens(rows))


# Disclosures that appear on a credit-card statement and essentially never on a
# deposit-account statement. US card issuers are required to print the payment and
# APR disclosures, so this is high-recall as well as high-precision.
#
# Matched case-insensitively against the document's text lines. A match *proposes*
# the inverted convention; the service gates that proposal behind a confirm
# (ImportService._gate_pdf_sign_convention), so a false positive costs a needless
# confirmation the user can override with `sign=`. A false negative, by contrast,
# would import a card statement's every charge as income with nothing surfaced —
# reconciliation passes either way. That asymmetry is the whole reason this list
# is scanned rather than the amounts.
_CREDIT_CARD_MARKERS: tuple[str, ...] = (
    "minimum payment",
    "credit limit",
    "available credit",
    "payment due date",
    "annual percentage rate",
)


def credit_card_markers(doc: PdfDocument) -> tuple[str, ...]:
    """The card-statement disclosures *doc* contains, in _CREDIT_CARD_MARKERS order.

    Returns the matched markers rather than a bare bool so the confirm gate can
    show the user what the inference was based on — an inversion the user cannot
    see the evidence for is exactly the kind of magic this codebase refuses.
    """
    haystack = "\n".join(doc.text_lines).lower()
    return tuple(m for m in _CREDIT_CARD_MARKERS if m in haystack)


def _has_any_negative_amount(table: PdfTable, amount_col_indices: list[int]) -> bool:
    """Return True if any amount cell in the table carries a leading minus sign.

    Used by derive_recipe to verify the negative_is_expense default is
    consistent with the document. A statement with zero negative amounts is
    almost certainly a positive=expense (credit-card) layout that Phase 2a
    doesn't yet auto-handle correctly.

    Scans the full transaction table (not just _SAMPLE_SIZE) — for a
    deposit-heavy first few rows on a real bank statement, the negatives
    might appear after row 5 and a truncated scan would falsely route the
    document to seed.
    """
    for row in table.rows:
        for idx in amount_col_indices:
            cell = row[idx].strip().lstrip("$").strip()
            if cell.startswith("-"):
                return True
    return False


# ---------------------------------------------------------------------------
# Field construction
# ---------------------------------------------------------------------------


def _build_fields(
    headers: list[str],
    date_pattern: str,
    number_format: Literal["us", "european"],
) -> list[FieldExtraction]:
    """Build one FieldExtraction per column."""
    # Integer part optional (must start with a digit when present) so a sub-dollar
    # fee printed as ".39" survives execute_recipe's fullmatch — see _US_NUMBER_RE.
    amount_pattern = (
        r"-?\$?(?:\d[\d,]*)?\.\d{2}"
        if number_format == "us"
        else r"-?(?:\d[\d.]*)?,\d{2}"
    )
    result: list[FieldExtraction] = []
    for header in headers:
        if _DATE_COL_RE.match(header):
            # Date column — find the matching format to get date_format string.
            # date_pattern was already selected; derive the corresponding strptime fmt.
            date_fmt = _pattern_to_fmt(date_pattern)
            result.append(
                FieldExtraction(
                    name=header,
                    pattern=date_pattern,
                    cast="date",
                    date_format=date_fmt,
                )
            )
        elif (
            _AMOUNT_COL_RE.search(header)
            or _DEBIT_COL_RE.search(header)
            or _CREDIT_COL_RE.search(header)
        ):
            result.append(
                FieldExtraction(
                    name=header,
                    pattern=amount_pattern,
                    cast="decimal",
                )
            )
        else:
            result.append(
                FieldExtraction(
                    name=header,
                    pattern=r".+",
                    cast="str",
                )
            )
    return result


def _pattern_to_fmt(pattern: str) -> str:
    """Map a regex date pattern back to its strptime format string."""
    for fmt, pat in _DATE_FORMATS:
        if pat == pattern:
            return fmt
    # Defensive: keeps _DATE_FORMATS and this function honest. If a new
    # date pattern lands in _DATE_FORMATS without a corresponding entry
    # here, fail loud instead of silently parsing dates as %m/%d/%Y.
    raise AssertionError(
        f"unrecognised date pattern {pattern!r} — add a row to _DATE_FORMATS "
        "covering (strptime_fmt, regex_pattern)"
    )


# ---------------------------------------------------------------------------
# Metadata anchors
# ---------------------------------------------------------------------------


def _build_metadata_anchors() -> list[FieldExtraction]:
    r"""Freeze every DEFAULT_ANCHORS pattern as its own FieldExtraction entry.

    Each field name in DEFAULT_ANCHORS carries an ordered list of alternative
    patterns (e.g. account_id has both ``"Account Number: \S+"`` and
    ``"Account ending in \d+"``). Saving only ``patterns[0]`` means a
    first-contact capture that matched the second pattern can't be reproduced
    on replay — `account_id` falls back to None and the import re-aliases by
    filename, splitting future statements for the same account into different
    `dim_accounts` rows.

    Emit one ``FieldExtraction(name=field_name, pattern=...)`` per pattern;
    routing.py groups them back into ``dict[name -> list[patterns]]`` for
    ``capture_metadata`` on replay (preserving the original list order so the
    cheapest pattern still tries first).

    Cast defaults:
        account_id       → str
        period_start/end → date
        opening/closing  → decimal
    """
    anchors: list[FieldExtraction] = []
    for field_name, patterns in DEFAULT_ANCHORS.items():
        cast = _META_FIELD_CASTS.get(field_name, "str")
        for pattern in patterns:
            anchors.append(
                FieldExtraction(
                    name=field_name,
                    pattern=pattern,
                    cast=cast,
                )
            )
    return anchors

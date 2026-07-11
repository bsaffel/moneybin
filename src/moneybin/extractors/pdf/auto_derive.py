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
from moneybin.extractors.pdf.metadata import DEFAULT_ANCHORS, StatementMetadata
from moneybin.extractors.pdf.recipe import FieldExtraction, Recipe, RegionAnchors

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
_US_NUMBER_RE = _re.compile(r"^-?\$?(\d{1,3}(,\d{3})*|\d+)\.\d{2}$")
_EUROPEAN_NUMBER_RE = _re.compile(r"^-?(\d{1,3}(\.\d{3})*|\d+),\d{2}$")


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

    number_fmt = _detect_number_format(table, amount_cols)
    if number_fmt is None:
        return None
    # execute_recipe only supports the US number format in Phase 2a.
    # Routing also catches NotImplementedError as a safety net, but
    # bailing here avoids polluting app.pdf_formats with recipes the
    # executor can't replay.
    if number_fmt != "us":
        return None

    # Sign-convention sanity check: auto_derive defaults single-amount layouts to
    # negative_is_expense, but credit-card statements use the opposite convention
    # (positive = expense, negative = payment). Two independent tells, because
    # neither catches the other's case:
    #
    # - The document names itself a card statement. This is the load-bearing one:
    #   a card statement with even one payment row has negative amounts, so the
    #   amounts alone can never rule it out (see _looks_like_credit_card_statement).
    # - No negative amounts at all. Catches a card statement that carries none of
    #   the disclosures — the convention is ambiguous, and reconciliation still
    #   passes on a flat (zero-delta) month while the import writes expenses as
    #   income.
    #
    # Decline rather than guess: derivation failing routes the statement to the
    # bridge, which can read it. A recipe with the sign convention backwards
    # corrupts every row, on this import and on every future replay.
    if sign == "negative_is_expense" and (
        _looks_like_credit_card_statement(doc)
        or not _has_any_negative_amount(table, amount_cols)
    ):
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
    end_anchor = _detect_end_anchor(document_text, start_anchor)
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
    return table.header[0]


def _detect_end_anchor(document_text: str, start_anchor: str) -> str:
    """Pick a transaction-table end_anchor present in *document_text*.

    Iterates ``_END_ANCHOR_CANDIDATES`` (most specific first) and returns
    the first candidate that appears AFTER ``start_anchor``. A leading
    ``start_anchor`` match constrains the search so a candidate string that
    also appears in the statement preamble (e.g. an issuer's tagline
    containing "TOTAL") doesn't get picked.

    Falls back to ``"Total:"`` if no candidate matches — the executor's
    full-text fallback in ``_carve_region`` is the same safety net the
    previous hardcoded anchor relied on, and the misconfiguration is
    logged loudly there.
    """
    start_idx = document_text.find(start_anchor)
    search_from = start_idx + len(start_anchor) if start_idx != -1 else 0
    for candidate in _END_ANCHOR_CANDIDATES:
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

    Checks ruled tables and reconstructed ones first, then falls back to the raw
    text lines — which is load-bearing for the most common real layout; see
    ``_text_transaction_candidate``.
    """
    return (
        _select_loose_transaction_table(doc) is not None
        or _text_transaction_candidate(doc) is not None
    )


def _select_transaction_table(doc: PdfDocument) -> PdfTable | None:
    """Return the largest *derivable* transaction-shaped table, or None.

    Ruled tables win when pdfplumber found any; tables reconstructed from
    ``text_lines`` are the fallback for unruled (real-world) statements.
    """
    candidates = [t for t in doc.tables if _is_transaction_shaped(t)]
    if not candidates:
        candidates = [
            t for t in _synthesize_tables_from_text(doc) if _is_transaction_shaped(t)
        ]
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
    """
    if recipe.sign_convention != "negative_is_expense":
        return True
    if _looks_like_credit_card_statement(doc):
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
# Matched case-insensitively against the document's text lines. A false positive
# costs an unnecessary escalation to the bridge (the statement is still captured);
# a false negative silently inverts the sign of every row. The asymmetry is the
# whole reason this list is scanned rather than the amounts.
_CREDIT_CARD_MARKERS: tuple[str, ...] = (
    "minimum payment",
    "credit limit",
    "available credit",
    "payment due date",
    "annual percentage rate",
)


def _looks_like_credit_card_statement(doc: PdfDocument) -> bool:
    """True when the document carries a credit-card statement's disclosures.

    The deterministic rung cannot infer the sign convention from the amounts. A
    checking statement (``-50`` groceries, ``+150`` paycheck) and a card statement
    (``+150`` charges, ``-50`` payment) have *identical* sign distributions — the
    information simply is not in the numbers. ``_classify_sign_convention``
    nonetheless hands back ``negative_is_expense`` for every single-amount layout,
    so without this check a card statement's charges import as **income** and its
    payment as an expense, and reconciliation ties out because it sums the raw
    signed amounts either way.

    So read the document instead of guessing at its arithmetic, and when it names
    itself a card statement, decline to derive: the bridge can read it. Declining
    is the safe direction — an unnecessary escalation is visible and recoverable,
    a silently inverted ledger is neither.
    """
    return any(
        marker in line.lower()
        for line in doc.text_lines
        for marker in _CREDIT_CARD_MARKERS
    )


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
    amount_pattern = (
        r"-?\$?[\d,]+\.\d{2}" if number_format == "us" else r"-?[\d.]+,\d{2}"
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

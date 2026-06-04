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
import re
from datetime import datetime
from typing import Literal

import regex as _re
from pydantic import ValidationError

from moneybin.extractors.pdf.ir import PdfDocument, PdfTable
from moneybin.extractors.pdf.metadata import DEFAULT_ANCHORS, StatementMetadata
from moneybin.extractors.pdf.recipe import FieldExtraction, Recipe, RegionAnchors

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column-classification regexes (compiled once)
# ---------------------------------------------------------------------------

_DATE_COL_RE = re.compile(r"^(date|trans.*date|posting.*date)$", re.IGNORECASE)
_AMOUNT_COL_RE = re.compile(r"amount", re.IGNORECASE)
_DEBIT_COL_RE = re.compile(r"debit|withdraw", re.IGNORECASE)
_CREDIT_COL_RE = re.compile(r"credit|deposit", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Date / number format constants
# ---------------------------------------------------------------------------

_DATE_FORMATS: list[tuple[str, str]] = [
    ("%m/%d/%Y", r"\d{2}/\d{2}/\d{4}"),
    ("%Y-%m-%d", r"\d{4}-\d{2}-\d{2}"),
    ("%m/%d/%y", r"\d{2}/\d{2}/\d{2}"),
]

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

    # Sign-convention sanity check: auto_derive defaults single-amount
    # layouts to negative_is_expense, but credit-card statements use the
    # opposite convention (positive = expense, negative = payment). When
    # the sampled rows contain no negative amounts the convention is
    # ambiguous — reconciliation can still pass on a flat (zero-delta)
    # month and the import would write positive expenses as income.
    # Route to seed rather than auto-derive a recipe that silently
    # corrupts signs on every future replay.
    if sign == "negative_is_expense" and not _has_any_negative_amount(
        table, amount_cols
    ):
        return None

    fields = _build_fields(table.header, date_pattern, number_fmt)
    metadata_anchors = _build_metadata_anchors()
    # start_anchor: just the first header word. layout=True extraction emits
    # proportional whitespace between columns, so a multi-word anchor with a
    # fixed separator never matches a real PDF — the first header alone is
    # always present and stable across statements with the same layout.
    row_region = RegionAnchors(
        start_anchor=table.header[0],
        end_anchor="Total:",
    )

    try:
        return Recipe(
            metadata_anchors=metadata_anchors,
            row_region=row_region,
            row_split=r"\s{2,}",
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


def _select_transaction_table(doc: PdfDocument) -> PdfTable | None:
    """Return the largest transaction-shaped table, or None."""
    candidates = [t for t in doc.tables if _is_transaction_shaped(t)]
    if not candidates:
        return None
    return max(candidates, key=lambda t: len(t.rows))


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
            cell = row[idx].strip().lstrip("$").strip()
            if cell and cell not in ("", "-"):
                samples.append(cell)

    if not samples:
        # All amount cells are empty (e.g. debit/credit with only one side per row).
        # Fall back to checking for digit-dot pattern to infer US format.
        # Collect any non-empty, non-dash cell from amount columns. The "-"
        # exclusion must mirror the first pass: a sample of only "-" placeholders
        # would fail _matches_us and force the recipe to seed.
        for row in table.rows[:_SAMPLE_SIZE]:
            for idx in amount_col_indices:
                cell = row[idx].strip().lstrip("$").strip()
                if cell and cell != "-":
                    samples.append(cell)

    if not samples:
        # Still nothing — can't determine format; refuse to auto-derive.
        return None

    # All samples must match a single format. Use the bounded helpers so a
    # pathological cell value can't ReDoS the format-detection scan.
    if all(_matches_us(s) for s in samples):
        return "us"
    if all(_matches_european(s) for s in samples):
        return "european"

    return None


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
    """Freeze DEFAULT_ANCHORS as FieldExtraction entries (first pattern per field).

    Cast defaults:
        account_id       → str
        period_start/end → date
        opening/closing  → decimal
    """
    anchors: list[FieldExtraction] = []
    for field_name, patterns in DEFAULT_ANCHORS.items():
        if not patterns:
            continue
        cast = _META_FIELD_CASTS.get(field_name, "str")
        anchors.append(
            FieldExtraction(
                name=field_name,
                pattern=patterns[0],
                cast=cast,
            )
        )
    return anchors

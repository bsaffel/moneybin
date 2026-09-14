"""Stage 2: File readers producing format-agnostic Polars DataFrames.

Each reader converts a specific file type into a Polars DataFrame with
string column names. This is the format-agnostic boundary — everything
downstream operates on DataFrames regardless of source format.
"""

import csv
import datetime
import logging
import re
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import polars as pl
import polars.selectors as cs

from moneybin.extractors.tabular.date_detection import (
    detect_date_format,
    parse_amount_str,
)
from moneybin.extractors.tabular.format_detector import (
    FormatInfo,
    _read_sample_lines,  # pyright: ignore[reportPrivateUsage]  # shared package helper
)
from moneybin.extractors.tabular.formats import DATE_TYPED_TABULAR_FIELDS

logger = logging.getLogger(__name__)


DEFAULT_TRAILING_PATTERNS: list[str] = [
    r"^(Total|Grand Total|Sum|Totals)\b",
    r"^(Export(ed)?|Generated|Downloaded|Report) (Date|On|At)\b",
    r"^(Record Count|Row Count|Number of)",
    r"^(Opening|Closing|Beginning|Ending) Balance\b",
    r"^,{3,}$",
    r"^\s*$",
]


@dataclass
class ReadResult:
    """Output of a file reader."""

    df: pl.DataFrame
    skip_rows: int = 0
    rows_skipped_trailing: int = 0
    row_count_warning: bool = False
    sheet_used: str | None = None
    has_header: bool = True
    """Whether a header row was consumed. False for a detected-headerless file
    and for schema-typed formats (parquet/feather) where column names are
    metadata, not a consumed row."""
    header_row_looks_like_data: bool = False
    """True when the row consumed as the header also parses as a transaction
    record (date + amount) — a red flag that a real data row may have been
    eaten as a header via an explicit skip_rows override (CSV or Excel).
    Auto-detection is its own safety net for both formats (it never picks a
    data-looking row as the header itself), so this stays False there — see
    ``header_position_ambiguous`` for auto-detection's own red flag. UNLIKE
    that flag, this one is NOT dismissible: the row is already gone, consumed
    as column names, and no confirmation recovers it (see
    ``header_row_consumed_recovery``'s docstring)."""
    header_position_ambiguous: bool = False
    """True when auto-detection (``_classify_header_rows``) picked a
    header-like row that has a data-like row somewhere before it. That
    earlier row might be a genuine one- or two-line balance summary (an
    intentionally skipped preamble — see ``_classify_header_rows``'s
    docstring) or real transaction data mistaken for one; detection cannot
    tell those apart. UNLIKE ``header_row_looks_like_data``, nothing is lost
    yet at this point — the rows in question are merely classified as
    preamble, not consumed as a header — so this IS dismissible: a caller
    who confirms is ratifying the detected header position, not overriding
    an unrecoverable loss. Always False for an explicit skip_rows (nothing
    to detect) and for the headerless outcome (no header was picked at
    all)."""
    header_position_ambiguous_rows: tuple[tuple[str, ...], ...] = ()
    """The actual disputed row(s) behind ``header_position_ambiguous`` — the
    earlier row(s) ``_classify_header_rows`` found reading as data, as FULL
    positional (unstripped, blanks included) cell tuples — cell ``i`` names
    the same physical column as ``header_position_ambiguous_header_cells[i]``.
    A confirm that names an inference without showing the evidence it's
    ratifying is functionally a silent action (design-principles.md, "Magic
    stays visible") — this is what lets a preview or CLI warning show the
    caller the actual row, not just the fact that one exists. Always empty
    when ``header_position_ambiguous`` is False."""
    header_position_ambiguous_header_cells: tuple[str, ...] = ()
    """The chosen header row's own FULL positional cells, from the SAME
    physical sample as ``header_position_ambiguous_rows`` — lets a caller
    resolve a disputed cell's column identity by POSITION rather than
    guessing against ``df.columns`` (a dropped blank column, or a fastexcel
    ``__UNNAMED__`` rename, would misalign a positional guess there). Always
    empty when ``header_position_ambiguous`` is False."""

    @property
    def rows_in_file(self) -> int:
        """Reconciled source-row accounting derived from the reader's fields.

        Sums preamble skipped + header (0/1) + rows loaded + trailing rows
        dropped. Equals the file's physical row count in the common case, but
        is derived from the reader's own accounting — NOT an independent
        physical line count. It can undercount the raw file where the parser
        coalesced rows the reader never saw as separate (repeated-header dedup
        in paginated exports, quoted fields containing embedded newlines). Use
        it to reconcile the reader's accounting, not as a data-loss oracle.
        """
        return (
            self.skip_rows
            + (1 if self.has_header else 0)
            + len(self.df)
            + self.rows_skipped_trailing
        )


def read_file(
    path: Path,
    info: FormatInfo,
    *,
    skip_rows: int | None = None,
    sheet: str | None = None,
    skip_trailing_patterns: list[str] | None = None,
    no_row_limit: bool = False,
    source_bytes: bytes | None = None,
    has_header: bool | None = None,
) -> ReadResult:
    """Read a file into a format-agnostic Polars DataFrame.

    Args:
        path: File path.
        info: Format detection result from Stage 1.
        skip_rows: Explicit skip rows (overrides detection).
        sheet: Excel sheet name (overrides auto-selection).
        skip_trailing_patterns: Regex patterns for trailing junk.
            None = use defaults, [] = no patterns.
        no_row_limit: If True, skip row count limits.
        source_bytes: Already materialized source object to parse.
        has_header: Persisted header decision; None runs detection.

    Returns:
        ReadResult with DataFrame and metadata.

    Raises:
        ValueError: If row count exceeds limit without override.
    """
    if info.file_type in ("csv", "tsv", "pipe", "semicolon"):
        result = _read_text(
            path,
            info,
            skip_rows=skip_rows,
            skip_trailing_patterns=skip_trailing_patterns,
            source_bytes=source_bytes,
            has_header=has_header,
        )
    elif info.file_type == "excel":
        result = _read_excel(
            path,
            info,
            skip_rows=skip_rows,
            sheet=sheet,
            source_bytes=source_bytes,
            has_header=has_header,
        )
    elif info.file_type == "parquet":
        result = _read_parquet(path, source_bytes=source_bytes)
    elif info.file_type == "feather":
        result = _read_feather(path, source_bytes=source_bytes)
    else:
        raise ValueError(f"No reader for file type: {info.file_type}")

    from moneybin.config import get_settings

    tabular_cfg = get_settings().providers.tabular
    row_count = len(result.df)
    if row_count > tabular_cfg.row_refuse_threshold and not no_row_limit:
        raise ValueError(
            f"File has {row_count:,} rows, exceeding the "
            f"{tabular_cfg.row_refuse_threshold:,} row limit. "
            f"Use --no-row-limit to override."
        )
    if row_count > tabular_cfg.row_warn_threshold:
        logger.warning(
            f"⚠️  File has {row_count:,} rows (warning threshold: "
            f"{tabular_cfg.row_warn_threshold:,}). Proceeding with import."
        )
        result.row_count_warning = True

    return result


def _read_text(
    path: Path,
    info: FormatInfo,
    *,
    skip_rows: int | None = None,
    skip_trailing_patterns: list[str] | None = None,
    source_bytes: bytes | None = None,
    has_header: bool | None = None,
) -> ReadResult:
    """Read a text-based tabular file (CSV, TSV, pipe, semicolon).

    Args:
        path: File path.
        info: Format detection result with delimiter and encoding.
        skip_rows: Explicit preamble rows to skip (overrides auto-detection).
        skip_trailing_patterns: Regex patterns for trailing junk rows.
        source_bytes: Already materialized source object to parse.
        has_header: Persisted header decision; None runs detection.

    Returns:
        ReadResult with the parsed DataFrame and metadata.
    """
    encoding = info.encoding
    delimiter = info.delimiter or ","

    # Explicit skip_rows implies a header at that row; auto-detection both
    # locates the header and decides whether the file has one at all.
    explicit_skip = skip_rows is not None
    resolved_has_header = True
    preamble_looks_like_data = False
    ambiguous_rows: tuple[tuple[str, ...], ...] = ()
    header_cells: tuple[str, ...] = ()
    if skip_rows is None:
        (
            skip_rows,
            resolved_has_header,
            preamble_looks_like_data,
            ambiguous_rows,
            header_cells,
        ) = _detect_header(
            path,
            encoding,
            delimiter,
            source_bytes=source_bytes,
        )
    elif has_header is not None:
        resolved_has_header = has_header

    # header_row_looks_like_data is defense-in-depth for the EXPLICIT skip_rows
    # path only (has_header is unconditionally True there — no safety check of
    # its own). Auto-detection (_detect_header) never selects a data-looking row
    # AS THE HEADER ITSELF, so computing it there would always be False — skip
    # the read. header_position_ambiguous (below) is auto-detection's own,
    # separately-dismissible red flag for the shape this one does not cover.
    header_row_looks_like_data = False
    if explicit_skip:
        header_row_looks_like_data = resolved_has_header and _row_looks_like_data_at(
            path,
            encoding,
            delimiter,
            skip_rows,
            source_bytes=source_bytes,
        )

    df = pl.read_csv(
        path if source_bytes is None else BytesIO(source_bytes),
        separator=delimiter,
        encoding=encoding if encoding != "utf-8-sig" else "utf8",
        skip_rows=skip_rows,
        has_header=resolved_has_header,
        infer_schema_length=0,
        truncate_ragged_lines=True,
    )

    patterns = skip_trailing_patterns
    if patterns is None:
        patterns = DEFAULT_TRAILING_PATTERNS
    rows_removed = 0
    if patterns and len(df) > 0:
        df, rows_removed = _remove_trailing_rows(df, patterns)

    if len(df) > 0:
        df = _remove_repeated_headers(df)

    return ReadResult(
        df=df,
        skip_rows=skip_rows,
        rows_skipped_trailing=rows_removed,
        has_header=resolved_has_header,
        header_row_looks_like_data=header_row_looks_like_data,
        header_position_ambiguous=preamble_looks_like_data,
        header_position_ambiguous_rows=ambiguous_rows,
        header_position_ambiguous_header_cells=header_cells,
    )


def _tokenize_csv_line(line: str, delimiter: str) -> list[str]:
    """Tokenize one sample line the way ``pl.read_csv``'s real read does.

    A bare ``line.split(delimiter)`` disagrees with the real read whenever a
    field is quoted: it leaves literal quote characters in a header cell
    (``'"Date"'`` instead of ``'Date'``, so it can never equal a value in
    ``df.columns``) and splits a quoted field containing the delimiter
    (``"Coffee, large"``) into two cells instead of one. Both defects make
    ``header_position_ambiguous_header_cells``/``_rows`` diverge from the
    columns and cells the real ``pl.read_csv`` call produces, which is what
    ``disputed_row_fields`` aligns them against — so a quoted-header or
    quoted-description file silently loses its disputed-row evidence at
    every surface even though nothing leaks. ``csv.reader`` with the same
    delimiter and polars' default quote character (``"``) tokenizes
    identically to the real read. One line at a time (never the whole
    sample at once) keeps each result aligned with its own physical row
    index — ``skip_rows``/the header row are physical indices, and a
    multi-line parse could merge lines on an unterminated quote.
    """
    try:
        return next(csv.reader([line], delimiter=delimiter))
    except csv.Error:
        # Pathological quoting the real read would also choke on (e.g. a
        # delimiter that collides with the NUL-byte guard) — fall back to
        # the naive split rather than raising out of a detection helper.
        return line.split(delimiter)


def _detect_header(
    path: Path,
    encoding: str,
    delimiter: str,
    *,
    source_bytes: bytes | None = None,
) -> tuple[int, bool, bool, tuple[tuple[str, ...], ...], tuple[str, ...]]:
    """Locate the header row, or determine the file is headerless.

    Samples the first 30 content lines, splits each on ``delimiter``, and
    delegates the header/headerless decision to ``_classify_header_rows`` —
    see that function for the two-outcome algorithm.

    Args:
        path: File path.
        encoding: File encoding.
        delimiter: Column delimiter.
        source_bytes: Already materialized source object to inspect.

    Returns:
        ``(skip_rows, has_header, preamble_looks_like_data, ambiguous_rows,
        header_cells)`` — see ``_classify_header_rows``.
    """
    enc = encoding if encoding != "utf-8-sig" else "utf-8"
    lines = [
        line.lstrip("\ufeff")
        for line in _read_sample_lines(
            path,
            enc,
            n=30,
            source_bytes=source_bytes,
        )
    ]
    # A blank line becomes an empty cell list — _classify_header_rows skips
    # any row with fewer than 2 cells, so this preserves the original
    # "if not line.strip(): continue" behavior while keeping physical row
    # indices intact (skip_rows / header_row is a physical row index).
    rows = [
        [] if not line.strip() else _tokenize_csv_line(line, delimiter)
        for line in lines
    ]
    return _classify_header_rows(rows)


def _classify_header_rows(
    rows: list[list[str]],
) -> tuple[int, bool, bool, tuple[tuple[str, ...], ...], tuple[str, ...]]:
    """Locate the header row, or determine the sampled rows are headerless.

    Shared by every tabular reader — CSV/TSV/pipe/semicolon (``_detect_header``
    splits sample lines on the delimiter) and Excel (``_read_excel`` samples
    worksheet cell values directly) — so the header/headerless decision is
    identical across formats; only how a physical row becomes a list of raw
    cell strings differs.

    Decides between two outcomes:

    - **Header present.** The first row that reads as labels (low numeric
      ratio), does *not* itself parse as a transaction, *and* is followed by
      a data row is the header. Returns ``(row_index, True)``. Scanning the
      whole window means any number of data-like preamble rows above the
      header — opening- and closing-balance summary lines such as
      ``2026-01-01,100.00`` — are skipped rather than mistaken for the first
      row of a headerless file. The follow-by-data check is what keeps a
      footer/trailer that also reads as labels (``Downloaded On,2026-04-17``,
      sitting *below* the data in a headerless file) from winning.
    - **Headerless.** When no row qualifies as a header, the first row that
      parses as a data record (date plus numeric amount) starts the data.
      Returns ``(row_index, False)`` so the reader keeps that row. This is
      the Wells Fargo case: ``Date,Amount,*,,Description`` with no header
      line, where every row leads with a date (low numeric ratio) and so
      none reads as a header.

    Args:
        rows: The first ~30 physical rows, each a list of raw (unstripped)
            cell strings. A row's position in this list IS its physical
            skip_rows/header-row index, so callers must not drop or reorder
            rows before calling this — a blank or too-short row is skipped
            internally (via the ``len(parts) < 2`` / empty-``non_empty``
            checks below), not by the caller filtering it out beforehand.

    Returns:
        ``(skip_rows, has_header, preamble_looks_like_data, ambiguous_rows,
        header_cells)`` — rows to skip before the header (or before the
        first data row when headerless), whether a header row is present, a
        red flag for the header-found outcome (whether a data-like row
        precedes the chosen header), that row's own FULL positional cells
        (empty when the flag is False), and the chosen header row's own
        FULL positional cells from the SAME sample (also empty when the
        flag is False). ``ambiguous_rows[j][i]`` and ``header_cells[i]``
        name the same physical column — neither is stripped of blank
        cells, so a caller can align a disputed cell to a column name
        without guessing against the (possibly column-dropped) final
        DataFrame. The flagged row might be a genuine one- or two-line
        balance summary (the intended preamble-skip case, see the
        docstring above) — or it might be a real transaction silently
        discarded as "preamble" because a later, unrelated row happens to
        read as labels. This classifier cannot tell those apart, so it
        reports the ambiguity — and the actual disputed cells, so a caller
        can show the evidence rather than just the fact — instead of
        picking a silent winner; callers fold the flag into
        ``ReadResult.header_position_ambiguous`` so the caller surfaces a
        confirmation instead of trusting the guess. Always ``False``/empty
        for the headerless outcome and the empty-input default — a
        genuinely headerless file only ever loses trailing rows (already
        reported via ``rows_skipped_trailing``), never leading ones.
    """
    # Two passes (see docstring): find a label row followed by data, else fall
    # back to the first data row as headerless. `non_empty` (stripped, blanks
    # dropped) only ever feeds the header/data-row SCORING heuristics below —
    # `ambiguous_rows`/`header_cells` are built from `rows[i]` (the full,
    # position-preserving physical row) so a caller can align cells by index.
    # No quote-stripping here: CSV callers already tokenize with the csv
    # module (_tokenize_csv_line), which de-quotes a field itself, and Excel
    # callers pass native cell values that were never CSV-quoted to begin
    # with — a leftover .strip('"').strip("'") would instead mangle a
    # genuine quote character an Excel cell holds as real content.
    qualifying: list[tuple[int, list[str]]] = []
    for i, parts in enumerate(rows):
        if len(parts) < 2:
            continue
        non_empty = [p.strip() for p in parts if p.strip()]
        if not non_empty:
            continue
        qualifying.append((i, non_empty))

    for idx, (i, non_empty) in enumerate(qualifying):
        if _looks_like_header(non_empty) and not _looks_like_data_row(non_empty):
            # A real header sits above the data, so a data row must follow it.
            # This rejects a footer/trailer that also reads as labels (e.g.
            # "Downloaded On,2026-04-17": a date, no amount, low numeric ratio)
            # but appears after the data in a headerless file — without the
            # follow check it would win header detection and the rows above it
            # would be skipped as preamble.
            if any(_looks_like_data_row(later) for _, later in qualifying[idx + 1 :]):
                ambiguous_rows = tuple(
                    tuple(rows[earlier_i])
                    for earlier_i, earlier in qualifying[:idx]
                    if _looks_like_data_row(earlier)
                )
                header_cells = tuple(rows[i]) if ambiguous_rows else ()
                return i, True, bool(ambiguous_rows), ambiguous_rows, header_cells
    for i, non_empty in qualifying:
        if _looks_like_data_row(non_empty):
            return i, False, False, (), ()

    return 0, True, False, (), ()


def _row_looks_like_data_at(
    path: Path,
    encoding: str,
    delimiter: str,
    row_index: int,
    *,
    source_bytes: bytes | None = None,
) -> bool:
    """Return True if the physical row at ``row_index`` parses as a transaction.

    Defense-in-depth check on the row an explicit ``skip_rows`` override is
    about to consume as a header — that path sets ``has_header=True`` with no
    safety check of its own (unlike auto-detection). A row that both is treated
    as a header AND parses as a data record is a red flag: a real transaction
    may be about to be silently dropped.

    Args:
        path: File path.
        encoding: File encoding.
        delimiter: Column delimiter.
        row_index: Zero-based physical line index to check.
        source_bytes: Already materialized source object to inspect.

    Returns:
        True when the row at ``row_index`` parses as a transaction record.
    """
    enc = encoding if encoding != "utf-8-sig" else "utf-8"
    lines = _read_sample_lines(
        path,
        enc,
        n=row_index + 1,
        source_bytes=source_bytes,
    )
    if row_index >= len(lines):
        return False
    # lstrip a leading BOM: a utf-8-sig file is decoded as utf-8 here
    # (polars-compatible), so physical line 0 may retain it.
    parts = _tokenize_csv_line(lines[row_index].lstrip("\ufeff"), delimiter)
    non_empty = [p.strip() for p in parts if p.strip()]
    return _looks_like_data_row(non_empty) if non_empty else False


def _looks_like_data_row(cells: list[str]) -> bool:
    """Return True if a row already parses as a transaction record.

    A genuine data row carries both a parseable date and a parseable amount; a
    header row carries neither (``Date``/``Amount`` are labels, not values).
    Used to detect headerless files before the first row is consumed as a
    header. Reuses ``detect_date_format`` and ``parse_amount_str`` so date and
    amount recognition stay coherent with the rest of the tabular pipeline.

    Args:
        cells: Non-empty, unquoted cell strings from one row.

    Returns:
        True when at least one cell is a date and at least one is an amount.
    """
    has_date = any(detect_date_format([c])[0] is not None for c in cells)
    has_amount = any(_is_amount(c) for c in cells)
    return has_date and has_amount


def _looks_like_header(cells: list[str]) -> bool:
    """Return True if a row reads as a header rather than a data record.

    A header carries mostly non-amount labels (``Date``, ``Amount``,
    ``Description``), so few of its cells parse as amounts.

    Args:
        cells: Non-empty, unquoted cell strings from one row.

    Returns:
        True when the row has at least two cells and fewer than half parse as
        amounts.
    """
    if len(cells) < 2:
        return False
    amount_count = sum(1 for c in cells if _is_amount(c))
    return amount_count / len(cells) < 0.5


def _is_amount(s: str) -> bool:
    """Return True if the string parses as a transaction amount.

    Reuses ``parse_amount_str`` — the importer's amount parser — so header
    detection recognizes exactly the formats the loader does: parentheses
    negatives (``(42.50)``), DR/CR suffixes, currency symbols, and thousands
    separators, not a narrower float-only subset. Probes with the ``us``
    convention (as ``column_mapper`` does); the boolean result is
    format-agnostic, since european-formatted values still parse non-None.

    Args:
        s: Cell string to test.

    Returns:
        True if the cell parses as an amount.
    """
    return parse_amount_str(s, "us") is not None


def _remove_trailing_rows(
    df: pl.DataFrame, patterns: list[str]
) -> tuple[pl.DataFrame, int]:
    """Remove trailing rows matching regex patterns, scanning from end.

    Args:
        df: Input DataFrame.
        patterns: List of regex patterns to match against row values.

    Returns:
        Tuple of (trimmed DataFrame, number of rows removed).
    """
    if len(df) == 0 or not patterns:
        return df, 0

    compiled = [re.compile(p, re.IGNORECASE) for p in patterns]
    first_col = df.columns[0]
    values = df[first_col].to_list()

    remove_from = len(values)
    for i in range(len(values) - 1, -1, -1):
        val = str(values[i]) if values[i] is not None else ""
        row_str = ",".join(
            str(df[col][i]) if df[col][i] is not None else "" for col in df.columns
        )
        if any(p.search(val) or p.search(row_str) for p in compiled):
            remove_from = i
        else:
            break

    if remove_from < len(values):
        removed = len(values) - remove_from
        return df.head(remove_from), removed
    return df, 0


def _remove_repeated_headers(df: pl.DataFrame) -> pl.DataFrame:
    """Remove rows that duplicate the header row (paginated exports).

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with repeated header rows removed.
    """
    if len(df) == 0:
        return df
    headers_lower = [c.lower() for c in df.columns]
    mask = [True] * len(df)
    first_col = df.columns[0]
    first_col_values = df[first_col].cast(pl.Utf8).to_list()
    for i, val in enumerate(first_col_values):
        if val is not None and val.lower() == headers_lower[0]:
            row_values = [
                str(df[col][i]).lower() if df[col][i] is not None else ""
                for col in df.columns
            ]
            if row_values == headers_lower:
                mask[i] = False
    return df.filter(pl.Series(mask))


def _openpyxl_sheet_access_errors() -> tuple[type[Exception], ...]:
    """The one failure tuple every openpyxl workbook-open or sheet-index site here shares.

    A container openpyxl can't open at all (legacy ``.xls``, corruption)
    raises ``InvalidFileException`` reading a path or ``zipfile.BadZipFile``
    reading in-memory bytes; a sheet name that doesn't exist in an
    otherwise-openable workbook raises a bare ``KeyError`` from
    ``Workbook.__getitem__``. Every guard in this module must degrade the
    same way for all three, so this is the one place they're named — a
    function, not a bare module constant, because it defers the
    ``openpyxl`` import: every call site here is Excel-only, but
    ``readers.py`` itself is imported for every file type, and hoisting
    this to a module-level tuple would force ``import openpyxl`` at
    ``readers.py`` import time regardless of what file the caller is
    actually reading.
    """
    from openpyxl.utils.exceptions import InvalidFileException

    return (InvalidFileException, zipfile.BadZipFile, KeyError)


def _excel_sample_rows(
    path: Path,
    sheet_name: str,
    *,
    source_bytes: bytes | None = None,
    n: int = 30,
) -> list[list[str]]:
    """Read the first ``n`` physical rows of an Excel sheet as cell strings.

    Streams via ``read_only`` rather than loading the whole sheet, mirroring
    the text readers' raw-sample cost. Cell values stand in for a delimiter
    split's fields, so ``_classify_header_rows`` runs unmodified on the result.

    Args:
        path: File path.
        sheet_name: Sheet to sample.
        source_bytes: Already materialized workbook object to inspect.
        n: Maximum number of rows to sample.

    Returns:
        Rows as lists of raw (unstripped) cell strings; ``None`` cells become
        ``""``.
    """
    import openpyxl

    wb = openpyxl.load_workbook(
        path if source_bytes is None else BytesIO(source_bytes),
        read_only=True,
        data_only=True,
    )
    try:
        ws = wb[sheet_name]
        return [
            [_excel_cell_text(v) for v in row]
            for row in ws.iter_rows(min_row=1, max_row=n, values_only=True)
        ]
    finally:
        wb.close()


# A native Excel *date* cell (openpyxl's datetime.datetime with a midnight
# time — Excel has no separate date type) is what _excel_cell_text normalizes
# for the classification sample via value.date().isoformat(). fastexcel
# stringifies that same cell as "2026-01-01 00:00:00" once pl.read_excel
# reads the column as Utf8. Matching only the midnight time keeps this to
# that one case: a non-midnight time means the cell held a real timestamp,
# not a date, so it is left alone rather than truncated.
_EXCEL_MIDNIGHT_DATETIME_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}) 00:00:00$")


def _normalize_mapped_date_cells(
    df: pl.DataFrame, columns: list[str], target_format: str | None
) -> pl.DataFrame:
    """Rewrite exactly the native-midnight cells in mapped date columns.

    Matches ``_EXCEL_MIDNIGHT_DATETIME_RE`` per cell — the shape a native
    Excel date renders as — and re-renders the captured date into
    ``target_format`` (bare ISO when ``target_format`` is ``None``, since no
    format is known yet). A cell whose captured date fails to parse
    (calendar-invalid, e.g. day 30 in February) is left byte-identical
    rather than raising. Every other cell — already-correct text, a
    non-midnight timestamp, anything not matching the pattern — passes
    through untouched. Unlike a column-wide regex collapse followed by a
    separate ISO-rescan, this never re-touches a cell the rewrite didn't
    itself produce, so a dirty or ambiguous cell that was never native
    cannot get silently reparsed under a different format.
    """
    for column in columns:
        if column not in df.columns:
            continue
        rewritten: list[str | None] = []
        for value in df[column].to_list():
            if value is None:
                rewritten.append(value)
                continue
            match = _EXCEL_MIDNIGHT_DATETIME_RE.match(value)
            if match is None:
                rewritten.append(value)
                continue
            iso_date = match.group(1)
            if target_format is None:
                rewritten.append(iso_date)
                continue
            try:
                parsed = datetime.datetime.strptime(iso_date, "%Y-%m-%d")
            except ValueError:
                rewritten.append(value)
                continue
            rewritten.append(parsed.strftime(target_format))
        df = df.with_columns(pl.Series(column, rewritten, dtype=pl.Utf8))
    return df


def mapped_date_columns(
    field_mapping: dict[str, str] | None,
) -> tuple[str | None, list[str]]:
    """Split a field mapping into (transaction_date's column, other date columns).

    The one place every ``normalize_excel_date_columns_for_detection``/
    ``_after_mapping`` caller gets this list from, so it can't drift per
    call site again.
    """
    if not field_mapping:
        return None, []
    primary = field_mapping.get(DATE_TYPED_TABULAR_FIELDS[0])
    others = [
        field_mapping[f] for f in DATE_TYPED_TABULAR_FIELDS[1:] if f in field_mapping
    ]
    return primary, others


def normalize_excel_date_columns_for_detection(
    df: pl.DataFrame, *, file_type: str, date_format: str | None
) -> pl.DataFrame:
    """Return a normalized COPY of every date-shaped column, for detection only.

    Never mutates the caller's real frame, and the result must never be
    imported or shown as a sample — it exists solely so date-shaped content
    is recognizable to ``map_columns``'s content-based discovery and to
    ``detect_date_format`` (``_DATE_FORMATS`` is date-only, so a native
    Excel cell's raw ``"<date> 00:00:00"`` text defeats both). The real
    frame stays untouched until ``normalize_excel_date_columns_after_
    mapping`` renders it, once, against the FINAL mapping and format.

    No-op for non-Excel file types. Renders EVERY string column's midnight-
    shaped cells (``_normalize_mapped_date_cells``, unscoped by mapping or
    column identity) rather than pre-selecting candidates: a column-
    selection probe (openpyxl's native-type scan, a majority-shape gate) is
    itself a source of false negatives — a blank spacer column that shifts
    physical indices, a column split between native and text rows that
    fails a strict-majority gate — and this function's only consumer is
    choosing what to render, which the per-cell rule already limits to
    genuinely midnight-shaped text. A vectorized ``str.contains`` any-check
    skips the per-cell Python loop entirely for a column with no candidate
    cell, so a wide, all-text file pays no extra cost for this scan.
    """
    if file_type != "excel":
        return df
    candidates = [
        col
        for col in df.select(cs.string()).columns
        if df[col].drop_nulls().str.contains(_EXCEL_MIDNIGHT_DATETIME_RE.pattern).any()
    ]
    if not candidates:
        return df
    return _normalize_mapped_date_cells(df, candidates, date_format)


def normalize_excel_date_columns_after_mapping(
    df: pl.DataFrame,
    *,
    file_type: str,
    field_mapping: dict[str, str] | None,
    date_format: str | None,
) -> pl.DataFrame:
    """Render the real frame's date columns, exactly once, against the FINAL mapping.

    This is the ONLY function that ever mutates the frame that gets
    imported or shown as a post-render sample. Every one of the three
    ``read_file`` callers (``import_service.py``'s ``_import_tabular``, the
    MCP ``import_preview_coarse`` tool, the CLI ``import preview`` command)
    calls this once column mapping and the effective date format are both
    fully resolved — never before. Because the real frame was never touched
    by ``normalize_excel_date_columns_for_detection`` (which only ever
    normalized a throwaway copy), every mapped column here is still in its
    original native/text shape regardless of whether the caller named it
    up front or ``map_columns`` only aliased it in later, so this one call
    is always sufficient — no second pass, no "did the first pass already
    touch this column" bookkeeping.

    Anything that reads the imported frame's date-column text — override
    validation, confirmation-gate samples, preview sample_values, a
    persisted reviewed plan — must run AFTER this call, or it sees
    pre-render native/mixed text instead of what actually gets imported.
    """
    if file_type != "excel":
        return df
    date_column, additional_date_columns = mapped_date_columns(field_mapping)
    mapped_columns = ([date_column] if date_column else []) + additional_date_columns
    if not mapped_columns:
        return df
    return _normalize_mapped_date_cells(df, mapped_columns, date_format)


def _excel_cell_text(value: object) -> str:
    """Render one sampled cell as the text the header classifier expects.

    A native Excel date cell arrives as ``datetime``, whose ``str()`` carries a
    time suffix (``2026-01-01 00:00:00``). ``_DATE_FORMATS`` is date-only, so
    ``detect_date_format`` would reject every such cell, ``_looks_like_data_row``
    would never fire, and ``_classify_header_rows`` would fall back to treating
    row 0 as a header — re-creating the eaten-first-row bug for the shape a
    spreadsheet-native export actually has. Normalize to ISO instead; only the
    classification sample is affected, never the values polars reads.
    """
    if value is None:
        return ""
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    return str(value)


# Matches the exact text pl.read_excel(infer_schema_length=0) renders for a
# native Excel date/datetime cell when calamine (not openpyxl) reads it: an
# ISO date followed by a space and a time-of-day, with or without a
# fractional-seconds tail. This is the fastexcel/calamine-sourced counterpart
# to _excel_cell_text's isinstance(value, datetime) branch: with
# infer_schema_length=0 every column is already Utf8 (str), so there is no
# datetime OBJECT left to isinstance-check — calamine has already rendered it
# to this exact shape by the time it reaches Python. Anchored full-string
# (^...$) so an ordinary description column containing a date-shaped
# substring is never partially rewritten.
_FASTEXCEL_DATETIME_TEXT = re.compile(
    r"^(\d{4}-\d{2}-\d{2}) \d{2}:\d{2}:\d{2}(\.\d+)?$"
)


def _fastexcel_probe_cell_text(value: str) -> str:
    """Strip a calamine-rendered time-of-day suffix for header classification.

    ``_classify_excel_headerless_via_fastexcel`` feeds ``_classify_header_rows``
    a raw, unheadered fastexcel/calamine read. Unlike the openpyxl-backed probe
    (``_excel_sample_rows``, which applies ``_excel_cell_text`` to typed
    ``datetime`` objects), this read has ``infer_schema_length=0`` and so
    already arrives as text — but calamine renders a native date/datetime cell
    as ``"2026-01-01 00:00:00"``, the same time-suffixed shape
    ``_excel_cell_text`` exists to strip. ``_DATE_FORMATS`` is date-only, so
    that suffix defeats ``detect_date_format``/``_looks_like_data_row`` here
    exactly as it would there, and a genuinely headerless legacy ``.xls`` with
    native-typed dates falls through to the ``(0, True, False)`` default —
    the first real transaction consumed as a header. Both probes must present
    ``_classify_header_rows`` with the identical shape for identical
    underlying data, or the two engines classify the same file differently.
    """
    match = _FASTEXCEL_DATETIME_TEXT.match(value)
    return match.group(1) if match else value


def _fastexcel_sample_rows(
    path: Path,
    sheet_name: str | None,
    *,
    n: int,
    source_bytes: bytes | None = None,
) -> list[list[str]]:
    """Read the first ``n`` physical rows of an Excel sheet via fastexcel/calamine.

    The one bounded, unheadered read both ``_classify_excel_headerless_via_
    fastexcel`` and ``_excel_row_looks_like_data_at_bounded`` use whenever
    openpyxl can't answer their question itself — legacy ``.xls`` (which
    openpyxl never supported) or a caller-supplied sheet name openpyxl
    can't find in an otherwise-openable container. ``n_rows`` caps the read
    itself (fastexcel/calamine stops parsing once it has this many rows)
    rather than materializing the whole sheet and slicing afterward — the
    only difference between the two callers is what they pass for ``n``.

    Lets ``fastexcel.FastExcelError`` propagate uncaught: each caller wraps
    its own call in the same ``except``, so the real read further down
    still gets the chance to raise the actual, clean error rather than this
    helper swallowing it into a shared fallback both callers would then
    have to distinguish again.

    Args:
        path: File path.
        sheet_name: Sheet to sample, or None to let fastexcel pick its own
            default (mirrors ``pl.read_excel``'s own default).
        n: Maximum number of rows to sample.
        source_bytes: Already materialized workbook object to inspect.

    Returns:
        Rows as lists of cell strings, with ``_fastexcel_probe_cell_text``'s
        calamine-rendered time-of-day-suffix normalization already applied
        — the same normalization ``_excel_cell_text`` applies for the
        openpyxl-backed sampler, so both probes present ``_classify_
        header_rows``/``_looks_like_data_row`` with the identical shape for
        identical underlying data.
    """
    probe_df = pl.read_excel(
        path if source_bytes is None else BytesIO(source_bytes),
        sheet_name=sheet_name,
        has_header=False,
        infer_schema_length=0,
        read_options={"header_row": None, "n_rows": n},
    )
    return [
        [_fastexcel_probe_cell_text(v) if v is not None else "" for v in row]
        for row in probe_df.iter_rows()
    ]


def _excel_row_looks_like_data_at(
    path: Path,
    sheet_name: str,
    row_index: int,
    *,
    source_bytes: bytes | None = None,
) -> bool:
    """Return True if the physical row at ``row_index`` parses as a transaction.

    Defense-in-depth check on the row an explicit Excel ``skip_rows`` override
    is about to consume as a header — mirrors ``_row_looks_like_data_at`` for
    CSV. Samples the physical cell values via ``_excel_sample_rows`` (which
    applies ``_excel_cell_text``) rather than reading back ``df.columns``:
    fastexcel's post-read stringification of a native Excel date/datetime
    header cell doesn't reliably come back in a form ``detect_date_format``
    recognizes, the same failure ``_excel_cell_text`` exists to prevent for
    the auto-detection sample.

    Args:
        path: File path.
        sheet_name: Sheet to sample.
        row_index: Zero-based physical row index to check.
        source_bytes: Already materialized workbook object to inspect.

    Returns:
        True when the row at ``row_index`` parses as a transaction record.
    """
    rows = _excel_sample_rows(
        path, sheet_name, source_bytes=source_bytes, n=row_index + 1
    )
    if row_index >= len(rows):
        return False
    # No quote-stripping: these are native openpyxl cell values, never
    # CSV-quoted text, so a leading/trailing quote character is genuine
    # cell content (see _classify_header_rows's identical rule).
    non_empty = [c.strip() for c in rows[row_index] if c.strip()]
    return _looks_like_data_row(non_empty) if non_empty else False


def _excel_row_looks_like_data_at_bounded(
    path: Path,
    sheet_name: str | None,
    row_index: int,
    *,
    source_bytes: bytes | None = None,
) -> bool:
    """Same question as ``_excel_row_looks_like_data_at``, never defaulting to False.

    The header-row safety signal must be computed for every Excel
    container, including one openpyxl can't open (legacy ``.xls``) or
    whose caller-supplied sheet name it can't find in an otherwise-openable
    workbook. Tries the openpyxl-backed sampler first (exact row semantics
    the real read will use); on ``_openpyxl_sheet_access_errors()``, falls
    back to the SAME bounded fastexcel/calamine read
    ``_classify_excel_headerless_via_fastexcel`` already uses for this
    identical "openpyxl can't answer this" gap in auto-detect
    classification — one fallback, not a second copy of it. Only when
    fastexcel ALSO can't read the container (``fastexcel.FastExcelError``)
    does this return False; the real read further down then raises the
    actual, clean error. Defaulting to False on a merely-failed SAMPLING
    attempt (rather than genuine unreadability) is exactly the regression
    this function exists to close: a stale saved ``skip_rows`` offset
    pointing at a real transaction row in a legacy ``.xls`` kept high
    confidence and silently dropped that row instead of raising
    ``header_row_consumed``.

    Args:
        path: File path.
        sheet_name: Sheet to sample, or None to let fastexcel pick its own
            default (mirrors ``pl.read_excel``'s own default) — the shape
            ``sheet_used`` already takes when openpyxl couldn't even open
            the container to pick a sheet.
        row_index: Zero-based physical row index to check.
        source_bytes: Already materialized workbook object to inspect.

    Returns:
        True when the row at ``row_index`` parses as a transaction record.
    """
    if sheet_name is not None:
        try:
            return _excel_row_looks_like_data_at(
                path, sheet_name, row_index, source_bytes=source_bytes
            )
        except _openpyxl_sheet_access_errors():
            pass  # Fall through to the fastexcel/calamine sampler below.

    import fastexcel

    try:
        rows = _fastexcel_sample_rows(
            path, sheet_name, n=row_index + 1, source_bytes=source_bytes
        )
    except fastexcel.FastExcelError:
        return False
    if row_index >= len(rows):
        return False
    non_empty = [c.strip() for c in rows[row_index] if c.strip()]
    return _looks_like_data_row(non_empty) if non_empty else False


def _classify_excel_headerless_via_fastexcel(
    path: Path,
    *,
    sheet_name: str | None,
    source_bytes: bytes | None,
) -> tuple[int, bool, bool, tuple[tuple[str, ...], ...], tuple[str, ...]]:
    """Classify header/headerless via a raw fastexcel/calamine read.

    Used whenever openpyxl can't answer this classification question itself
    — either it can't open the container at all (legacy ``.xls``, which
    openpyxl never supported), or (when called with an explicit
    ``sheet_name``) it already proved that name doesn't exist in a container
    it CAN open. fastexcel/calamine reads legacy ``.xls`` directly, so a raw
    unheadered read stands in for the openpyxl-backed ``_excel_sample_rows``
    classification and feeds the same ``_classify_header_rows`` algorithm.

    ``sheet_name=None`` lets fastexcel pick its own default sheet (mirrors
    ``pl.read_excel``'s own "neither ``sheet_id`` nor ``sheet_name`` given"
    default — sheet 1); a caller-supplied name is passed through unchanged
    so classification targets the SAME sheet the real read further down will
    use — never silently substituting a different one.

    Returns:
        ``(skip_rows, has_header, preamble_looks_like_data, ambiguous_rows,
        header_cells)`` — see ``_classify_header_rows``. Falls back to
        ``(0, True, False, (), ())`` — the historical pre-detection default —
        only when fastexcel itself can't read this container either
        (``fastexcel.FastExcelError``); the real read further down then
        gets the chance to raise the actual, clean error. A caller-supplied
        sheet name that doesn't exist raises its own ``ValueError`` from
        ``pl.read_excel`` (already classified by ``handle_cli_errors``)
        rather than ``FastExcelError``, so that error propagates out of
        this function uncaught instead of being swallowed into a misleading
        fallback.
    """
    import fastexcel

    try:
        # 30 matches _classify_header_rows's own "first ~30 physical rows"
        # contract (the same bound _excel_sample_rows already samples for
        # the openpyxl-backed classification path) — reusing it rather than
        # inventing a second bound for the identical classification task.
        sample_rows = _fastexcel_sample_rows(
            path, sheet_name, n=30, source_bytes=source_bytes
        )
        return _classify_header_rows(sample_rows)
    except fastexcel.FastExcelError:
        return 0, True, False, (), ()


def _read_excel(
    path: Path,
    info: FormatInfo,
    *,
    skip_rows: int | None = None,
    sheet: str | None = None,
    source_bytes: bytes | None = None,
    has_header: bool | None = None,
) -> ReadResult:
    """Read an Excel (.xlsx) file.

    Shares header/headerless detection with the CSV/TSV/pipe/semicolon path
    via ``_classify_header_rows`` — see that function for the algorithm.

    Args:
        path: File path.
        info: Format detection result (unused for Excel, kept for API consistency).
        skip_rows: Explicit header-row index (overrides auto-detection).
            With has_header=False, this is the start-of-data row index instead.
        sheet: Sheet name to read. If None, picks the sheet with the most rows.
        source_bytes: Already materialized workbook object to parse.
        has_header: Persisted header decision; None runs detection.

    Returns:
        ReadResult with the parsed DataFrame and sheet metadata.
    """
    import openpyxl

    sheet_used = sheet
    if sheet_used is None:
        try:
            wb = openpyxl.load_workbook(
                path if source_bytes is None else BytesIO(source_bytes),
                read_only=True,
                data_only=True,
            )
            try:
                best_sheet = wb.sheetnames[0]
                best_rows = 0
                for name in wb.sheetnames:
                    ws = wb[name]
                    row_count = ws.max_row or 0
                    if row_count > best_rows:
                        best_rows = row_count
                        best_sheet = name
                sheet_used = best_sheet
            finally:
                wb.close()
        except _openpyxl_sheet_access_errors():
            # Same openpyxl-can't-open-this-container case the two sampler
            # guards below handle (see their comments for why path vs. bytes
            # raise different exceptions) — this call is the third and last
            # unguarded openpyxl entry point in this reader. sheet_used stays
            # None: pl.read_excel's own default (sheet_id/sheet_name both
            # unset) is sheet 1, which is the same "let the component that
            # can actually parse the file decide" fallback used everywhere
            # else here, not a new policy invented for this call site.
            sheet_used = None

    # Explicit skip_rows implies a header at that row; auto-detection both
    # locates the header and decides whether the sheet has one at all — same
    # contract as _read_text.
    explicit_skip = skip_rows is not None
    resolved_has_header = True
    preamble_looks_like_data = False
    ambiguous_rows: tuple[tuple[str, ...], ...] = ()
    header_cells: tuple[str, ...] = ()
    if skip_rows is None:
        # sheet_used is None here only when the lookup above already proved
        # openpyxl can't open this container at all (legacy .xls) — asking
        # _excel_sample_rows would hit the identical failure on the
        # identical bytes before ever indexing a sheet name. Classify from
        # the reader that CAN open a legacy .xls instead (see
        # _classify_excel_headerless_via_fastexcel): a raw, unheadered
        # fastexcel/calamine read gives the same shape _classify_header_rows
        # expects, just sourced differently. Without this, a genuinely
        # headerless .xls always fell back to "row 0 is the header" with no
        # classification signal at all — MB-449 for exactly the one file
        # variant this reader's own openpyxl-fallback tests exist to cover.
        if sheet_used is None:
            (
                skip_rows,
                resolved_has_header,
                preamble_looks_like_data,
                ambiguous_rows,
                header_cells,
            ) = _classify_excel_headerless_via_fastexcel(
                path, sheet_name=None, source_bytes=source_bytes
            )
        else:
            try:
                sample_rows = _excel_sample_rows(
                    path, sheet_used, source_bytes=source_bytes
                )
                (
                    skip_rows,
                    resolved_has_header,
                    preamble_looks_like_data,
                    ambiguous_rows,
                    header_cells,
                ) = _classify_header_rows(sample_rows)
            except _openpyxl_sheet_access_errors():
                # openpyxl only ever supported .xlsx/.xlsm/.xltx/.xltm — never
                # legacy binary .xls. This sampling call is new: pre-PR,
                # supplying --sheet skipped openpyxl entirely and let
                # calamine/fastexcel (which does read legacy .xls) handle the
                # file alone. All three exceptions mean openpyxl has no
                # opinion here, for two different reasons:
                # - InvalidFileException/BadZipFile: openpyxl cannot open
                #   this CONTAINER at all — openpyxl raises one or the other
                #   depending on *how* it's asked to open the file, not on
                #   anything about the caller: given a path it runs its own
                #   extension check first and raises InvalidFileException;
                #   given bytes (BytesIO, e.g. the MCP confirm-after-preview
                #   replay path) it skips straight to `ZipFile(...)`, which
                #   raises BadZipFile for a non-zip (OLE2/legacy-.xls)
                #   container. Don't "simplify" this back to one exception —
                #   the two entry shapes genuinely fail differently.
                # - KeyError: the container opens fine, but ``sheet_used``
                #   (a caller-supplied ``--sheet``/saved-format sheet name)
                #   doesn't exist in it — openpyxl's ``Workbook.__getitem__``
                #   raises a bare KeyError, which src/moneybin/errors.py
                #   deliberately excludes from its generic LookupError
                #   classification, so left uncaught this reached the caller
                #   as a raw traceback instead of a clean error.
                # For the container case, classify via the SAME fastexcel
                # fallback the no-explicit-sheet branch above uses (scoped to
                # this sheet_used, so header/headerless detection still
                # targets the right sheet instead of losing the caller's
                # choice) rather than hardcoding skip_rows=0 (headered) —
                # a genuinely headerless legacy .xls with an explicit --sheet
                # must classify the same way the no-explicit-sheet branch does, or
                # MB-449 reopens for exactly this one variant. For the
                # sheet-name case, fastexcel independently fails to find the
                # same nonexistent sheet and raises its own clean ValueError
                # (already classified by handle_cli_errors) rather than
                # fastexcel.FastExcelError, so it propagates out of the
                # helper uncaught instead of being swallowed into a
                # misleading "headered" fallback.
                (
                    skip_rows,
                    resolved_has_header,
                    preamble_looks_like_data,
                    ambiguous_rows,
                    header_cells,
                ) = _classify_excel_headerless_via_fastexcel(
                    path, sheet_name=sheet_used, source_bytes=source_bytes
                )
    elif has_header is not None:
        resolved_has_header = has_header

    # header_row is the sheet's own absolute row index, and skip_rows here
    # means "skip this many rows, then start reading" — passing skip_rows as
    # header_row (headered) or as skip_rows with header_row=None (headerless)
    # lets fastexcel's own header-dedup and type-coercion handle the read,
    # rather than re-implementing it against a manually sliced DataFrame.
    read_options = (
        {"header_row": skip_rows}
        if resolved_has_header
        else {"header_row": None, "skip_rows": skip_rows}
    )
    df = pl.read_excel(
        path if source_bytes is None else BytesIO(source_bytes),
        sheet_name=sheet_used,
        has_header=resolved_has_header,
        infer_schema_length=0,
        read_options=read_options,
    )
    # Native-date column rendering (normalize_excel_date_columns_for_
    # detection / _after_mapping) does NOT run here. It needs Stage 3's
    # resolved column mapping and effective date format to decide
    # correctly, and neither is known during this read — see those
    # functions' docstrings. The caller (import_service.py's
    # _import_tabular) applies them after Stage 3, before/after map_columns
    # respectively.

    # header_row_looks_like_data is defense-in-depth for the EXPLICIT
    # skip_rows path only (mirrors _read_text). Auto-detection
    # (_classify_header_rows) never selects a data-looking row as the header
    # ITSELF, so this stays False there — header_position_ambiguous (below)
    # is auto-detection's own, separately-dismissible red flag. This signal
    # is computed for EVERY Excel container, including one openpyxl can't
    # open at all or whose sheet_used it couldn't find — never defaulted to
    # False just because that specific sampling attempt failed; see
    # _excel_row_looks_like_data_at_bounded for why (a stale saved skip_rows
    # offset pointing at a real transaction in a legacy .xls must still
    # raise header_row_consumed, not silently drop the row).
    header_row_looks_like_data = False
    if explicit_skip and resolved_has_header:
        header_row_looks_like_data = _excel_row_looks_like_data_at_bounded(
            path, sheet_used, skip_rows, source_bytes=source_bytes
        )

    return ReadResult(
        df=df,
        skip_rows=skip_rows,
        sheet_used=sheet_used,
        has_header=resolved_has_header,
        header_row_looks_like_data=header_row_looks_like_data,
        header_position_ambiguous=preamble_looks_like_data,
        header_position_ambiguous_rows=ambiguous_rows,
        header_position_ambiguous_header_cells=header_cells,
    )


def _read_parquet(path: Path, *, source_bytes: bytes | None = None) -> ReadResult:
    """Read a Parquet file.

    Args:
        path: File path.
        source_bytes: Already materialized Parquet object to parse.

    Returns:
        ReadResult with the parsed DataFrame.
    """
    df = pl.read_parquet(path if source_bytes is None else BytesIO(source_bytes))
    # Columnar formats carry column names in schema metadata — no data row is
    # consumed as a header, so has_header=False keeps rows_in_file == len(df).
    return ReadResult(df=df, has_header=False)


def _read_feather(path: Path, *, source_bytes: bytes | None = None) -> ReadResult:
    """Read a Feather/Arrow IPC file.

    Args:
        path: File path.
        source_bytes: Already materialized Feather object to parse.

    Returns:
        ReadResult with the parsed DataFrame.
    """
    df = pl.read_ipc(path if source_bytes is None else BytesIO(source_bytes))
    # Schema-typed like parquet — column names are metadata, not a header row.
    return ReadResult(df=df, has_header=False)

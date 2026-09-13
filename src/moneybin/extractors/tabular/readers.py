"""Stage 2: File readers producing format-agnostic Polars DataFrames.

Each reader converts a specific file type into a Polars DataFrame with
string column names. This is the format-agnostic boundary — everything
downstream operates on DataFrames regardless of source format.
"""

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
    earlier row(s) ``_classify_header_rows`` found reading as data, as raw
    (stripped, non-empty) cell strings. A confirm that names an inference
    without showing the evidence it's ratifying is functionally a silent
    action (design-principles.md, "Magic stays visible") — this is what
    lets a preview or CLI warning show the caller the actual row, not just
    the fact that one exists. Always empty when
    ``header_position_ambiguous`` is False."""
    excel_native_date_columns: frozenset[str] | None = None
    """Excel-only. Column names openpyxl reports as natively date/datetime
    typed (see ``_excel_native_date_columns``) — a property of the file,
    discoverable at read time even though whether to actually rewrite those
    columns is a Stage 3+ decision (see
    ``normalize_excel_date_columns_before_mapping``). ``None`` when openpyxl
    could not open the container at all (legacy ``.xls``); an empty
    frozenset means openpyxl opened fine but found no natively-typed date
    column. Always ``None`` for every other file type."""

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
    if skip_rows is None:
        skip_rows, resolved_has_header, preamble_looks_like_data, ambiguous_rows = (
            _detect_header(
                path,
                encoding,
                delimiter,
                source_bytes=source_bytes,
            )
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
    )


def _detect_header(
    path: Path,
    encoding: str,
    delimiter: str,
    *,
    source_bytes: bytes | None = None,
) -> tuple[int, bool, bool, tuple[tuple[str, ...], ...]]:
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
        ``(skip_rows, has_header, preamble_looks_like_data, ambiguous_rows)``
        — see ``_classify_header_rows``.
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
    rows = [[] if not line.strip() else line.split(delimiter) for line in lines]
    return _classify_header_rows(rows)


def _classify_header_rows(
    rows: list[list[str]],
) -> tuple[int, bool, bool, tuple[tuple[str, ...], ...]]:
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
        ``(skip_rows, has_header, preamble_looks_like_data, ambiguous_rows)``
        — rows to skip before the header (or before the first data row when
        headerless), whether a header row is present, a red flag for the
        header-found outcome (whether a data-like row precedes the chosen
        header), and that row's own cells (empty when the flag is False).
        The flagged row might be a genuine one- or two-line balance summary
        (the intended preamble-skip case, see the docstring above) — or it
        might be a real transaction silently discarded as "preamble" because
        a later, unrelated row happens to read as labels. This classifier
        cannot tell those apart, so it reports the ambiguity — and the
        actual disputed cells, so a caller can show the evidence rather than
        just the fact — instead of picking a silent winner; callers fold the
        flag into ``ReadResult.header_position_ambiguous`` so the caller
        surfaces a confirmation instead of trusting the guess. Always
        ``False``/empty for the headerless outcome and the empty-input
        default — a genuinely headerless file only ever loses trailing rows
        (already reported via ``rows_skipped_trailing``), never leading ones.
    """
    # Two passes (see docstring): find a label row followed by data, else fall
    # back to the first data row as headerless.
    qualifying: list[tuple[int, list[str]]] = []
    for i, parts in enumerate(rows):
        if len(parts) < 2:
            continue
        non_empty = [p.strip().strip('"').strip("'") for p in parts if p.strip()]
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
                    tuple(earlier)
                    for _, earlier in qualifying[:idx]
                    if _looks_like_data_row(earlier)
                )
                return i, True, bool(ambiguous_rows), ambiguous_rows
    for i, non_empty in qualifying:
        if _looks_like_data_row(non_empty):
            return i, False, False, ()

    return 0, True, False, ()


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
    parts = lines[row_index].lstrip("\ufeff").split(delimiter)
    non_empty = [p.strip().strip('"').strip("'") for p in parts if p.strip()]
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


def normalize_excel_date_columns(
    df: pl.DataFrame,
    *,
    columns: list[str] | None = None,
    native_date_columns: frozenset[str] | None = None,
) -> tuple[pl.DataFrame, frozenset[str]]:
    """Collapse a native-date column's rendered text to its date.

    ``_excel_cell_text`` closes this gap for the header-classification
    sample, which reads cells directly via openpyxl and sees the original
    ``datetime`` object. The real data ``pl.read_excel`` returns never goes
    through that path — fastexcel stringifies a native date cell as
    ``"2026-01-01 00:00:00"`` on its own, and every ``_DATE_FORMATS`` entry
    (date_detection.py) is date-only, so without this a spreadsheet-native
    date column never reaches ``detect_date_format`` as a recognized date and
    a correctly-headered file is refused as having none. A genuine timestamp
    column (a real, non-midnight time) does not match and is left as-is —
    that shape still isn't recognized by any ``_DATE_FORMATS`` entry, which
    is a separate, disclosed gap for ``date_detection`` to close, not this
    reader.

    Public (no leading underscore) and Excel-only: called from
    ``import_service.py``'s ``_import_tabular``, not from this module's own
    ``_read_excel``. Normalization can't run at read time and still be
    correct — Stage 3 (column mapping, including a saved format matched only
    by implicit header signature) and the caller's effective date format are
    both still unknown during the Stage 2 read, and either one can turn a
    would-be normalization into a truncation of a format the caller
    explicitly declared. The caller relocates this call to run after Stage 3
    resolves both, and *before* ``map_columns`` (whose own content-based date
    detection needs a recognized shape to identify or validate a native-date
    column at all, by name alias or by scanning unclaimed columns).

    Three selection strategies:

    - ``columns`` given (a caller already knows which columns are mapped
      date fields — see ``mapped_date_columns``): every one of them
      qualifies, full stop. A mapped date column is a date column by
      construction, so a native/text mix within it is irrelevant — unlike
      the two shape-guessing strategies below, this one needs no evidence,
      because the caller already supplied the answer. ``native_date_columns``
      is ignored in this mode.
    - ``columns`` is ``None`` and ``native_date_columns`` given (a
      frozenset — openpyxl opened the file and reported which columns are
      natively date/datetime-typed): normalizes exactly those columns.
      Deterministic — no shape inference, so a ``Description``/``Memo``
      column that happens to render like a date is never touched (it was
      never *typed* as one).
    - Both ``None`` (openpyxl could not open the container at all — legacy
      ``.xls``, the one case a native-type probe is unavailable): falls
      back to a tolerant-MAJORITY text-shape heuristic — a column qualifies
      when more than half of its non-null values match the anchored
      midnight pattern, matching the dirty-minority tolerance
      ``_parse_dates``/``_validate_date_format_override`` already apply
      elsewhere in this pipeline (a dirty prefix must not refuse a file
      whose remaining rows parse).

    In every strategy, the per-cell ``str.replace`` only ever rewrites
    values that actually match the anchored midnight pattern, so a
    qualifying column's non-matching cells (dirty minority, or genuine text
    in some other shape) pass through unchanged rather than being coerced.

    Args:
        df: DataFrame to normalize.
        columns: The caller's own known date-typed columns (see
            ``mapped_date_columns``) — every match rewrites regardless of
            shape. ``None`` (the auto-detect case, where no mapping exists
            yet) considers every qualifying string column via
            ``native_date_columns`` or the shape fallback, matching
            ``map_columns``'s own need to find the date column before it
            is known.
        native_date_columns: See above. Ignored when ``columns`` is given.

    Returns:
        The possibly-rewritten DataFrame, and the frozenset of column names
        actually rewritten (empty if none).
    """
    if columns is not None:
        date_cols = [col for col in columns if col in df.columns]
    elif native_date_columns is not None:
        candidates = df.select(cs.string()).columns
        date_cols = [col for col in candidates if col in native_date_columns]
    else:
        candidates = df.select(cs.string()).columns
        date_cols = [
            col
            for col in candidates
            if (non_null := df[col].drop_nulls()).len() > 0
            and non_null.str.contains(_EXCEL_MIDNIGHT_DATETIME_RE.pattern).sum() * 2
            > non_null.len()
        ]
    if not date_cols:
        return df, frozenset()
    normalized = df.with_columns(
        pl.col(date_cols).str.replace(_EXCEL_MIDNIGHT_DATETIME_RE.pattern, "${1}")
    )
    return normalized, frozenset(date_cols)


# A cell already collapsed to a bare date by normalize_excel_date_columns
# (or genuine text already in that shape) — the only shape
# _reformat_iso_dates_to reformats; anything else (dirty text, a real
# timestamp) passes through untouched.
_ISO_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _reformat_iso_dates_to(
    df: pl.DataFrame, columns: list[str], target_format: str
) -> pl.DataFrame:
    """Re-render every bare-ISO-shaped cell in ``columns`` into ``target_format``.

    The second half of the two-step sequence
    ``normalize_excel_date_columns_before_mapping`` runs on a mapped date
    column: collapse a native cell to ISO, then re-render it into the
    format the caller actually declared — so the declared format never has
    to change (no "flip"), and a column the collapse step never touched
    (already correct text) is left alone by this step too, since it isn't
    bare-ISO-shaped.
    """
    for column in columns:
        if column not in df.columns:
            continue
        reformatted = [
            datetime.datetime.strptime(value, "%Y-%m-%d").strftime(target_format)
            if value is not None and _ISO_DATE_ONLY_RE.match(value)
            else value
            for value in df[column].to_list()
        ]
        df = df.with_columns(pl.Series(column, reformatted, dtype=pl.Utf8))
    return df


def date_format_has_time_component(
    date_format: str | None,
    *,
    df: pl.DataFrame | None = None,
    date_column: str | None = None,
) -> bool:
    """True if the declared format expects the raw Excel timestamp shape.

    Two prior fixes each tried a representative synthetic raw shape (a bare
    probe string, then one adding ``%X`` support) and each closed exactly
    the one instance of this bug it was written for: a literal suffix, then
    a fractional-seconds suffix (``"%Y-%m-%d %H:%M:%S.%f"``) neither probe
    carries. Guessing the raw shape is the actual defect — a fifth shape
    would reopen this the same way.

    Ends the class instead: when ``date_column`` names a column present in
    ``df``, check whether ``date_format`` actually parses that column's own
    raw text via ``format_parses`` (the same dirty-tolerant, majority-rate
    check ``_validate_date_format_override`` already uses for the identical
    question at a later point in the pipeline). Any format that reads the
    real values expects that raw shape, directive or literal, fractional
    seconds or not, and the column must not be normalized out from under
    it; a format that doesn't (a plain ``"%Y-%m-%d"`` against
    ``"2026-01-01 00:00:00"``) declares a bare date and normalizing is safe.

    Falls back to the synthetic probe only when there is no column to
    check: ``date_column`` is ``None`` or absent from ``df.columns`` — the
    first-contact case where the caller supplied ``--date-format`` without
    ``--mapping transaction_date=<column>`` to say which column it governs
    (see ``normalize_excel_date_columns_before_mapping``'s auto-detect
    branch, this function's only caller — ``date_column`` is always
    ``None`` there in practice), so there is genuinely no raw text to read
    yet. ``pl.read_excel(infer_schema_length=0)`` renders a native Excel
    date/datetime cell as Python's ``str(datetime)`` — a midnight-suffixed
    timestamp (``"2026-01-01 00:00:00"``; see ``_excel_cell_text``'s
    docstring) absent a real time-of-day — so that stays a faithful
    stand-in for this one narrow case where the real column is unavailable.
    """
    if date_format is None:
        return False
    if df is not None and date_column is not None and date_column in df.columns:
        from moneybin.extractors.tabular.date_detection import format_parses

        return format_parses(df[date_column].cast(pl.Utf8).to_list(), date_format)
    try:
        datetime.datetime.strptime("2000-01-02 00:00:00", date_format)
    except ValueError:
        return False
    return True


# The tabular schema's only date-typed destination fields (raw_tabular_
# transactions.sql declares exactly these two as DATE). Single source of
# truth for mapped_date_columns below — each of the three
# normalize_excel_date_columns_before_mapping call sites must derive its
# scope from this same list, not repeat "transaction_date" and "post_date"
# by hand (that drift is the exact failure both functions' docstrings exist
# to prevent).
_DATE_TYPED_TABULAR_FIELDS: tuple[str, ...] = ("transaction_date", "post_date")


def mapped_date_columns(
    field_mapping: dict[str, str] | None,
) -> tuple[str | None, list[str]]:
    """Split a field mapping into (transaction_date's column, other date columns).

    The one place all three ``normalize_excel_date_columns_before_mapping``
    callers get this list from, so it can't drift per call site again.
    """
    if not field_mapping:
        return None, []
    primary = field_mapping.get(_DATE_TYPED_TABULAR_FIELDS[0])
    others = [
        field_mapping[f] for f in _DATE_TYPED_TABULAR_FIELDS[1:] if f in field_mapping
    ]
    return primary, others


def normalize_excel_date_columns_before_mapping(
    df: pl.DataFrame,
    *,
    file_type: str,
    date_format: str | None,
    date_column: str | None = None,
    additional_date_columns: list[str] | None = None,
    native_date_columns: frozenset[str] | None = None,
) -> tuple[pl.DataFrame, str | None]:
    """Shared normalize-then-map sequence for every ``read_file`` caller.

    There are three such callers — ``import_service.py``'s
    ``_import_tabular``, the MCP ``import_preview_coarse`` tool
    (``import_tools.py``), and the CLI ``import preview`` command
    (``import_cmd.py``) — and each needs the identical decision: whether to
    normalize at all, and if so, whether to scope it to the known date
    columns. Duplicating that decision at three call sites is exactly how one
    of them drifted and regressed (a native-date Excel column reaching
    column mapping unnormalized doesn't just fail to detect the date column —
    it gets misidentified as ``description`` while the real description
    column drops out of the mapping entirely, a worse failure than refusing
    to detect a date at all). All three now build ``date_column``/
    ``additional_date_columns`` via ``mapped_date_columns`` rather than
    naming destination fields by hand, so a third date-typed field would
    only need to be added there.

    No-op for non-Excel file types. When the mapping is still unknown (no
    ``date_column``), this is a pure auto-detect scan — see
    ``date_format_has_time_component`` for why a declared time-bearing
    format short-circuits it, and ``mapping_result.date_format`` (detected
    AFTER this call, from the now-normalized text) for what actually
    governs downstream in that case.

    Once the mapping is known, EVERY mapped date column (``date_column``
    plus ``additional_date_columns`` — ``transaction_date`` and
    ``post_date``, see ``mapped_date_columns``) normalizes the same way,
    regardless of its own native/text mix: a native cell collapses to ISO,
    then — when ``date_format`` is known — re-renders into that declared
    shape. The declared format therefore never has to change to
    accommodate a rewrite; every mapped column ends up in the ONE
    representation ``transform_dataframe`` already parses every date-typed
    field under (transforms.py). ``date_format_has_time_component`` is not
    consulted here: collapsing a native cell to ISO and then re-rendering
    it into any declared format — time-bearing or not — reproduces
    whatever raw text that format actually expects, so there is nothing
    left for a skip to protect.

    When ``date_format`` is unknown, cells stay in bare ISO — the shape
    ``map_columns``'s own detection will scan next — applied to every
    mapped column alike, so two differently-typed sibling columns can't
    desync from each other before a format is even chosen.

    Returns:
        ``(possibly-rewritten df, date_format)`` — the input format,
        unchanged.
    """
    if file_type != "excel":
        return df, date_format
    mapped_columns = ([date_column] if date_column else []) + list(
        additional_date_columns or []
    )
    if not mapped_columns:
        if date_format_has_time_component(date_format, df=df, date_column=date_column):
            return df, date_format
        normalized, _ = normalize_excel_date_columns(
            df, native_date_columns=native_date_columns
        )
        return normalized, date_format
    normalized, _ = normalize_excel_date_columns(df, columns=mapped_columns)
    if date_format is not None:
        normalized = _reformat_iso_dates_to(normalized, mapped_columns, date_format)
    return normalized, date_format


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


# Row cap for _excel_native_date_columns's typed scan. Measured on a real
# 49,999-row x 15-column .xlsx: the full-column scan cost ~8.0s of a ~14.0s
# read_file() call (>55% of total import time), and nearly all of that is
# openpyxl.load_workbook()'s own fixed per-file overhead — a max_row=30
# scan still cost ~2.7s, barely less than a max_row=2000 scan at ~2.9s,
# because the workbook-open dominates below roughly 1,000-2,000 rows. A
# full scan of all 49,999 rows cost ~7.6s, so shrinking the cap much below
# 2,000 buys almost nothing further while making the majority-tolerance
# check (see _excel_native_date_columns's docstring) fragile against a
# longer dirty run at the START of the column. 2,000 sits past that
# diminishing-returns knee while staying at ~4% of the accepted 50,000-row
# import limit, so a genuinely huge file's cost for this one step is
# capped regardless of size.
_EXCEL_NATIVE_DATE_SAMPLE_ROWS = 2000


def _excel_column_physical_indices(
    path: Path,
    sheet_name: str,
    *,
    has_header: bool,
    data_start_row: int,
    column_names: list[str],
    source_bytes: bytes | None,
) -> list[int]:
    """Map each ``column_names[i]`` to its true worksheet column index.

    ``pl.read_excel``'s ``drop_empty_cols=True`` default (used by every
    ``pl.read_excel`` call in this reader, since none override it) elides
    any column whose header AND every data cell in the read range are
    blank — confirmed empirically: an entirely-blank column between two
    populated ones disappears from ``df.columns`` while openpyxl's
    ``iter_rows()`` still reports it at its real physical position. Once one
    column is dropped, every later ``column_names[i]`` no longer equals the
    worksheet's physical column ``i`` — the assumption
    ``_excel_native_date_columns`` used to make. A blank header with
    populated data, or a named header with blank data, does NOT get
    dropped (fastexcel keeps it, auto-naming it ``__UNNAMED__N`` in the
    header-blank case) — only header-AND-data both blank triggers the drop.

    Asks fastexcel's own column metadata for the answer
    (``ColumnInfo.absolute_index`` is the worksheet's real position) rather
    than reimplementing that blank-detection rule by hand, which would
    silently drift the moment fastexcel's own rule changes. ``header_row``/
    ``skip_rows`` are passed with the exact same values ``_read_excel``'s
    real ``pl.read_excel`` call uses (translated to the low-level API), so
    this sees the identical column set fastexcel actually returned.

    Bounded by ``n_rows=_EXCEL_NATIVE_DATE_SAMPLE_ROWS``, matching the
    sibling openpyxl-based scans. Verified empirically that a capped
    ``n_rows`` does not change ``available_columns()``'s column list or any
    ``absolute_index`` — both are fixed by the header row alone.

    Returns:
        The physical worksheet column index for each entry in
        ``column_names``, in the same order. Falls back to the identity
        mapping (``range(len(column_names))`` — the pre-fix assumption) if
        the low-level read disagrees with the already-succeeded
        ``df.columns`` read on column count, or if fastexcel can't read the
        file this way at all: a mismatch means something about this file
        defeats the assumption this helper makes, and a wrong-but-plausible
        remapping is worse than the untouched heuristic.
    """
    import fastexcel

    try:
        reader = fastexcel.read_excel(path if source_bytes is None else source_bytes)
        sheet = reader.load_sheet(
            sheet_name,
            header_row=(data_start_row - 1) if has_header else None,
            skip_rows=None if has_header else data_start_row,
            n_rows=_EXCEL_NATIVE_DATE_SAMPLE_ROWS,
        )
        columns = sheet.available_columns()
    except fastexcel.FastExcelError:
        return list(range(len(column_names)))
    if len(columns) != len(column_names):
        return list(range(len(column_names)))
    return [col.absolute_index for col in columns]


def _excel_native_date_columns(
    path: Path,
    sheet_name: str,
    *,
    data_start_row: int,
    has_header: bool,
    column_names: list[str],
    source_bytes: bytes | None = None,
    sample_rows: int = _EXCEL_NATIVE_DATE_SAMPLE_ROWS,
) -> frozenset[str] | None:
    """Column names openpyxl reports as natively date/datetime-typed.

    ``pl.read_excel(infer_schema_length=0)`` reads every cell as text,
    discarding whether the source workbook typed it as a date —
    ``normalize_excel_date_columns``'s whole reason for existing is
    recovering that fact for the real read, the same way ``_excel_cell_text``
    recovers it for the header-classification sample. openpyxl's
    ``iter_rows(values_only=True)`` returns the untouched Python object (a
    ``datetime.date``/``datetime.datetime`` for a native date cell, since
    ``datetime.datetime`` is itself a ``date`` subclass), so a column
    qualifies when a STRICT MAJORITY of its non-null data-row values are one
    of those types — an exact, typed count, not a text-shape guess. This is
    the same dirty-minority tolerance ``normalize_excel_date_columns``'s own
    fallback heuristic applies (see its docstring): a "pending" placeholder
    or similar one-off in an otherwise all-native-date column must not
    disqualify the column outright, or the majority-tolerant fallback this
    function's ``None`` return triggers would be reachable only on the path
    that almost never runs (openpyxl failing to open the file at all), while
    the ordinary case — openpyxl succeeding — silently lost the tolerance.

    Scans only the first ``sample_rows`` data rows, not the whole column
    (review finding: an unconditional full-sheet ``openpyxl`` pass cost
    ~8.0s of a ~14.0s ``read_file()`` call on a real 49,999-row x 15-column
    file — see ``_EXCEL_NATIVE_DATE_SAMPLE_ROWS`` for the measurement behind
    the 2,000-row default). This changes what "majority" means: it is now a
    majority over the SAMPLE, not the whole column. Cost this pays: a
    dirty run at the very START of the column longer than ``sample_rows``
    (e.g. 2,000+ leading "pending" placeholders followed by genuine native
    dates) now reads as majority-non-date and is left unnormalized — worse
    than the whole-column scan for that one narrow shape, accepted because
    openpyxl's read-only streaming parser cannot skip ahead to sample the
    column's middle or tail without paying the same full per-row parse cost
    a whole-column scan would (confirmed empirically: capping at even 30
    rows saved almost nothing over 2,000, since the fixed
    ``load_workbook()`` cost dominates below roughly 1,000-2,000 rows — see
    the constant's own comment). A dirty MINORITY scattered anywhere within
    the sample is still tolerated exactly as before.

    Args:
        path: File path.
        sheet_name: Sheet to inspect.
        data_start_row: Physical row index (0-based) of the first DATA row —
            already offset past the header row when the sheet has one; the
            caller (headered or headerless) resolves that before calling.
        has_header: Whether the sheet has a consumed header row — needed
            (alongside ``data_start_row``) to ask fastexcel for the same
            column set it returned for the real read (see
            ``_excel_column_physical_indices``).
        column_names: The real column's names, in order, as
            ``pl.read_excel`` returned them. Not necessarily 1:1 with
            openpyxl's physical column positions — an entirely blank column
            (header AND every data cell empty) is dropped from this list by
            ``pl.read_excel``'s ``drop_empty_cols=True`` default but still
            occupies a real slot in openpyxl's ``iter_rows()`` — so this is
            resolved to physical indices via
            ``_excel_column_physical_indices`` before use, never assumed to
            equal ``range(len(column_names))``. That resolution is by
            column IDENTITY (fastexcel's own metadata), not row position,
            so it stays correct regardless of how many data rows this
            function goes on to sample.
        source_bytes: Already materialized workbook object to inspect.
        sample_rows: Maximum number of data rows to scan. Overridable for
            tests; production callers use the module default.

    Returns:
        ``None`` when openpyxl cannot open this container at all (legacy
        ``.xls``, or genuine corruption) — the caller falls back to a
        text-shape heuristic in that case, since typed inspection isn't
        available. An empty frozenset (as opposed to ``None``) means
        openpyxl opened the file fine and found no natively-typed date
        column — either the sampled rows are empty, or no column's
        non-null sampled values are majority ``datetime.date``. A column
        holding entirely non-date values (numbers, strings) always scores 0
        dates out of its non-null count and can never reach a majority, so
        it cannot qualify regardless of how few or many values it holds.
    """
    import openpyxl
    from openpyxl.utils.exceptions import InvalidFileException

    try:
        wb = openpyxl.load_workbook(
            path if source_bytes is None else BytesIO(source_bytes),
            read_only=True,
            data_only=True,
        )
    except (InvalidFileException, zipfile.BadZipFile):
        return None
    try:
        ws = wb[sheet_name]
        num_cols = len(column_names)
        physical_indices = _excel_column_physical_indices(
            path,
            sheet_name,
            has_header=has_header,
            data_start_row=data_start_row,
            column_names=column_names,
            source_bytes=source_bytes,
        )
        date_counts = [0] * num_cols
        non_null_counts = [0] * num_cols
        for row in ws.iter_rows(
            min_row=data_start_row + 1,
            max_row=data_start_row + sample_rows,
            values_only=True,
        ):
            for i in range(num_cols):
                physical_i = physical_indices[i]
                v = row[physical_i] if physical_i < len(row) else None
                if v is None:
                    continue
                non_null_counts[i] += 1
                if isinstance(v, datetime.date):
                    date_counts[i] += 1
        return frozenset(
            column_names[i]
            for i in range(num_cols)
            if non_null_counts[i] > 0 and date_counts[i] * 2 > non_null_counts[i]
        )
    finally:
        wb.close()


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
    non_empty = [c.strip().strip('"').strip("'") for c in rows[row_index] if c.strip()]
    return _looks_like_data_row(non_empty) if non_empty else False


def _classify_excel_headerless_via_fastexcel(
    path: Path,
    *,
    sheet_name: str | None,
    source_bytes: bytes | None,
) -> tuple[int, bool, bool, tuple[tuple[str, ...], ...]]:
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
        ``(skip_rows, has_header, preamble_looks_like_data, ambiguous_rows)``
        — see ``_classify_header_rows``. Falls back to
        ``(0, True, False, ())`` — the historical pre-detection default —
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
        # n_rows caps the read itself (fastexcel/calamine stops parsing
        # once it has this many rows) rather than materializing the whole
        # sheet and slicing afterward — review finding: the old
        # materialize-then-.head(30) paid for the WHOLE sheet here, then
        # _read_excel's real read parses the whole sheet again below, so a
        # large supported .xls paid for two complete parses even when
        # destined for the row-limit refusal. 30 matches
        # _classify_header_rows's own "first ~30 physical rows" contract
        # (the same bound _excel_sample_rows already samples for the
        # openpyxl-backed classification path) — reusing it rather than
        # inventing a second bound for the identical classification task.
        probe_df = pl.read_excel(
            path if source_bytes is None else BytesIO(source_bytes),
            sheet_name=sheet_name,
            has_header=False,
            infer_schema_length=0,
            read_options={"header_row": None, "n_rows": 30},
        )
        # _fastexcel_probe_cell_text strips the same calamine-rendered
        # time-of-day suffix _excel_cell_text strips for the openpyxl-backed
        # probe (see its docstring) — both probes must present
        # _classify_header_rows with the identical shape for identical
        # underlying data.
        sample_rows = [
            [_fastexcel_probe_cell_text(v) if v is not None else "" for v in row]
            for row in probe_df.iter_rows()
        ]
        return _classify_header_rows(sample_rows)
    except fastexcel.FastExcelError:
        return 0, True, False, ()


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
    from openpyxl.utils.exceptions import InvalidFileException

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
        except (InvalidFileException, zipfile.BadZipFile):
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
            skip_rows, resolved_has_header, preamble_looks_like_data, ambiguous_rows = (
                _classify_excel_headerless_via_fastexcel(
                    path, sheet_name=None, source_bytes=source_bytes
                )
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
                ) = _classify_header_rows(sample_rows)
            except (InvalidFileException, zipfile.BadZipFile, KeyError):
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
    # Native-date column normalization (normalize_excel_date_columns) does
    # NOT run here. It needs Stage 3's resolved column mapping and effective
    # date format to decide correctly, and neither is known during this
    # read — see that function's docstring. The caller (import_service.py's
    # _import_tabular) applies it after Stage 3, before map_columns. But the
    # FACT of which columns openpyxl reports as natively date/datetime-typed
    # is a property of the file, discoverable now — probe it here so the
    # caller can normalize exactly those columns instead of guessing from
    # rendered text shape. sheet_used is None only when openpyxl already
    # proved it can't open this container at all (legacy .xls) — asking
    # again would hit the identical failure.
    native_date_columns = (
        None
        if sheet_used is None
        else _excel_native_date_columns(
            path,
            sheet_used,
            # skip_rows means "header row" when resolved_has_header, but
            # "first data row" when headerless (see _read_excel's own
            # skip_rows docstring) — _excel_native_date_columns needs the
            # first DATA row either way, so add the header offset only when
            # a header is actually consumed. Passing skip_rows unconditionally
            # (as if it always named a header row) skipped the first
            # genuine data row of every headerless native-date sheet, which
            # silently emptied the candidate set on a small sheet (e.g. one
            # data row) with no header to skip at all.
            data_start_row=skip_rows + 1 if resolved_has_header else skip_rows,
            has_header=resolved_has_header,
            column_names=list(df.columns),
            source_bytes=source_bytes,
        )
    )

    # header_row_looks_like_data is defense-in-depth for the EXPLICIT
    # skip_rows path only (mirrors _read_text). Auto-detection
    # (_classify_header_rows) never selects a data-looking row as the header
    # ITSELF, so this stays False there — header_position_ambiguous (below)
    # is auto-detection's own, separately-dismissible red flag. Classifies
    # the physical sampled row, not df.columns — fastexcel's post-read column
    # naming for a native Excel date/datetime cell doesn't reliably parse as
    # a date (see _excel_row_looks_like_data_at).
    header_row_looks_like_data = False
    if explicit_skip and resolved_has_header:
        # Same reasoning as the auto-detect branch's sheet_used check above:
        # a None sheet_used already proves openpyxl can't open this file, so
        # this sampler would hit the identical failure before indexing a
        # sheet name.
        if sheet_used is None:
            header_row_looks_like_data = False
        else:
            try:
                header_row_looks_like_data = _excel_row_looks_like_data_at(
                    path, sheet_used, skip_rows, source_bytes=source_bytes
                )
            except (InvalidFileException, zipfile.BadZipFile):
                # Same fallback as the auto-detect branch above, and for the
                # same reason: a .xls-sourced saved format (file_type is
                # always "excel" — see format_detector.py's _EXTENSION_MAP —
                # never the literal "xls") with skip_rows > 0 reaches this
                # defense-in-depth sampler too, and openpyxl still can't open
                # a legacy .xls (path or bytes shape — see the auto-detect
                # branch's comment for why the two shapes raise different
                # exceptions). An unreadable sampler has no opinion; the real
                # read below still succeeds via fastexcel.
                header_row_looks_like_data = False

    return ReadResult(
        df=df,
        skip_rows=skip_rows,
        sheet_used=sheet_used,
        has_header=resolved_has_header,
        header_row_looks_like_data=header_row_looks_like_data,
        header_position_ambiguous=preamble_looks_like_data,
        header_position_ambiguous_rows=ambiguous_rows,
        excel_native_date_columns=native_date_columns,
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

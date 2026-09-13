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
    data-looking row as the header), so this stays False there."""
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
    if skip_rows is None:
        skip_rows, resolved_has_header = _detect_header(
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
    # as the header, so computing it there would always be False — skip the read.
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
    )


def _detect_header(
    path: Path,
    encoding: str,
    delimiter: str,
    *,
    source_bytes: bytes | None = None,
) -> tuple[int, bool]:
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
        ``(skip_rows, has_header)`` — rows to skip before the header (or
        before the first data row when headerless), and whether a header
        row is present.
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


def _classify_header_rows(rows: list[list[str]]) -> tuple[int, bool]:
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
        ``(skip_rows, has_header)`` — rows to skip before the header (or
        before the first data row when headerless), and whether a header
        row is present.
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
                return i, True
    for i, non_empty in qualifying:
        if _looks_like_data_row(non_empty):
            return i, False

    return 0, True


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

    Two selection strategies, chosen by whether openpyxl could type the
    file (see ``_excel_native_date_columns``):

    - ``native_date_columns`` given (a frozenset — openpyxl opened the file
      and reported which columns are natively date/datetime-typed):
      normalizes exactly those columns, intersected with ``columns`` when
      given. Deterministic — no shape inference, so a ``Description``/
      ``Memo`` column that happens to render like a date is never touched
      (it was never *typed* as one), and one dirty value in a genuinely
      native-date column never voids the rest of it (the column's identity
      came from its cell types, not from measuring its rendered text).
    - ``native_date_columns`` is ``None`` (openpyxl could not open the
      container at all — legacy ``.xls``, the one case a native-type probe
      is unavailable): falls back to a tolerant-MAJORITY text-shape
      heuristic — a column qualifies when more than half of its non-null
      values match the anchored midnight pattern, matching the
      dirty-minority tolerance ``_parse_dates``/``_validate_date_format_
      override`` already apply elsewhere in this pipeline (a dirty prefix
      must not refuse a file whose remaining rows parse). The per-cell
      ``str.replace`` only ever rewrites values that actually match the
      pattern, so a qualifying column's non-matching minority passes
      through unchanged rather than being coerced.

    Args:
        df: DataFrame to normalize.
        columns: Restrict the candidate set to these column names — pass
            the already-known ``transaction_date`` mapping (a saved/matched
            format's or a reviewed plan's) so an unrelated column is never
            touched even incidentally. ``None`` (the auto-detect case,
            where no mapping exists yet) considers every qualifying string
            column, matching ``map_columns``'s own need to find the date
            column before it is known.
        native_date_columns: See above.

    Returns:
        The possibly-rewritten DataFrame, and the frozenset of column names
        actually rewritten (empty if none) — the caller needs this to know
        whether a persisted date format must be overridden to match the
        rewrite it just caused (see
        ``normalize_excel_date_columns_before_mapping``).
    """
    if native_date_columns is not None:
        candidates = columns if columns is not None else df.select(cs.string()).columns
        date_cols = [
            col
            for col in candidates
            if col in native_date_columns and col in df.columns
        ]
    else:
        candidates = columns if columns is not None else df.select(cs.string()).columns
        date_cols = [
            col
            for col in candidates
            if col in df.columns
            and (non_null := df[col].drop_nulls()).len() > 0
            and non_null.str.contains(_EXCEL_MIDNIGHT_DATETIME_RE.pattern).sum() * 2
            > non_null.len()
        ]
    if not date_cols:
        return df, frozenset()
    normalized = df.with_columns(
        pl.col(date_cols).str.replace(_EXCEL_MIDNIGHT_DATETIME_RE.pattern, "${1}")
    )
    return normalized, frozenset(date_cols)


def date_format_has_time_component(date_format: str | None) -> bool:
    """True if a strptime format string declares a time-of-day directive.

    A declared time component (%H/%M/%S/%I/%p, or the locale-dependent %X)
    means the caller — or a saved/reviewed format — is stating what the raw
    bytes look like, so ``normalize_excel_date_columns`` must not collapse a
    column out from under a format that expects the time-bearing shape.
    """
    return date_format is not None and any(
        directive in date_format for directive in ("%H", "%M", "%S", "%I", "%p", "%X")
    )


def normalize_excel_date_columns_before_mapping(
    df: pl.DataFrame,
    *,
    file_type: str,
    date_format: str | None,
    date_column: str | None = None,
    native_date_columns: frozenset[str] | None = None,
) -> tuple[pl.DataFrame, str | None]:
    """Shared normalize-then-map sequence for every ``read_file`` caller.

    There are three such callers — ``import_service.py``'s
    ``_import_tabular``, the MCP ``import_preview_coarse`` tool
    (``import_tools.py``), and the CLI ``import preview`` command
    (``import_cmd.py``) — and each needs the identical decision: whether to
    normalize at all, and if so, whether to scope it to a single known
    column. Duplicating that decision at three call sites is exactly how one
    of them drifted and regressed (a native-date Excel column reaching
    column mapping unnormalized doesn't just fail to detect the date column —
    it gets misidentified as ``description`` while the real description
    column drops out of the mapping entirely, a worse failure than refusing
    to detect a date at all).

    No-op for non-Excel file types. Skips normalization entirely when
    ``date_format`` declares a time component — see
    ``date_format_has_time_component`` and ``normalize_excel_date_columns``
    for why. Otherwise normalizes, scoped to ``date_column`` when the caller
    already knows which column maps to ``transaction_date`` (a saved/matched
    format or a reviewed plan); ``None`` normalizes broadly, matching what
    ``map_columns``'s own content-based discovery needs when no mapping
    exists yet.

    Returns:
        ``(possibly-rewritten df, effective_date_format)``. ``date_format``
        passes through unchanged UNLESS this step actually rewrote
        ``date_column`` — a persisted/reviewed format was written for the
        column's *pre-rewrite* text, so the parser must follow the rewrite
        this step just made, not the format that no longer matches what's
        in the column. These two outcomes are mutually exclusive: a
        time-bearing ``date_format`` always skips normalization (returned
        above), so a column is never both rewritten and still expected to
        parse under its original format.
    """
    if file_type != "excel":
        return df, date_format
    if date_format_has_time_component(date_format):
        return df, date_format
    normalized, rewritten = normalize_excel_date_columns(
        df,
        columns=[date_column] if date_column else None,
        native_date_columns=native_date_columns,
    )
    effective_date_format = (
        "%Y-%m-%d" if date_column and date_column in rewritten else date_format
    )
    return normalized, effective_date_format


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
    Scans the whole data range rather than a bounded sample: this is the
    source of truth ``normalize_excel_date_columns`` rewrites from, so a
    partial scan could miscount past the sample window.

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
            equal ``range(len(column_names))``.
        source_bytes: Already materialized workbook object to inspect.

    Returns:
        ``None`` when openpyxl cannot open this container at all (legacy
        ``.xls``, or genuine corruption) — the caller falls back to a
        text-shape heuristic in that case, since typed inspection isn't
        available. An empty frozenset (as opposed to ``None``) means
        openpyxl opened the file fine and found no natively-typed date
        column — either the sheet has no data rows, or no column's
        non-null values are majority ``datetime.date``. A column holding
        entirely non-date values (numbers, strings) always scores 0 dates
        out of its non-null count and can never reach a majority, so it
        cannot qualify regardless of how few or many values it holds.
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
        for row in ws.iter_rows(min_row=data_start_row + 1, values_only=True):
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
) -> tuple[int, bool]:
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
        ``(skip_rows, has_header)``. Falls back to ``(0, True)`` — the
        historical pre-detection default — only when fastexcel itself can't
        read this container either (``fastexcel.FastExcelError``); the real
        read further down then gets the chance to raise the actual, clean
        error. A caller-supplied sheet name that doesn't exist raises its
        own ``ValueError`` from ``pl.read_excel`` (already classified by
        ``handle_cli_errors``) rather than ``FastExcelError``, so that error
        propagates out of this function uncaught instead of being swallowed
        into a misleading ``(0, True)`` fallback.
    """
    import fastexcel

    try:
        probe_df = pl.read_excel(
            path if source_bytes is None else BytesIO(source_bytes),
            sheet_name=sheet_name,
            has_header=False,
            infer_schema_length=0,
            read_options={"header_row": None},
        )
        sample_rows = [
            [v if v is not None else "" for v in row]
            for row in probe_df.head(30).iter_rows()
        ]
        return _classify_header_rows(sample_rows)
    except fastexcel.FastExcelError:
        return 0, True


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
            skip_rows, resolved_has_header = _classify_excel_headerless_via_fastexcel(
                path, sheet_name=None, source_bytes=source_bytes
            )
        else:
            try:
                sample_rows = _excel_sample_rows(
                    path, sheet_used, source_bytes=source_bytes
                )
                skip_rows, resolved_has_header = _classify_header_rows(sample_rows)
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
                skip_rows, resolved_has_header = (
                    _classify_excel_headerless_via_fastexcel(
                        path, sheet_name=sheet_used, source_bytes=source_bytes
                    )
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
    # (_classify_header_rows) never selects a data-looking row as the header,
    # so this is always False there. Classifies the physical sampled row, not
    # df.columns — fastexcel's post-read column naming for a native Excel
    # date/datetime cell doesn't reliably parse as a date (see
    # _excel_row_looks_like_data_at).
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

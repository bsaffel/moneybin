"""Stage 2: File readers producing format-agnostic Polars DataFrames.

Each reader converts a specific file type into a Polars DataFrame with
string column names. This is the format-agnostic boundary — everything
downstream operates on DataFrames regardless of source format.
"""

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from moneybin.extractors.tabular.date_detection import detect_date_format
from moneybin.extractors.tabular.format_detector import FormatInfo

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


def read_file(
    path: Path,
    info: FormatInfo,
    *,
    skip_rows: int | None = None,
    sheet: str | None = None,
    skip_trailing_patterns: list[str] | None = None,
    no_row_limit: bool = False,
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
        )
    elif info.file_type == "excel":
        result = _read_excel(path, info, skip_rows=skip_rows, sheet=sheet)
    elif info.file_type == "parquet":
        result = _read_parquet(path)
    elif info.file_type == "feather":
        result = _read_feather(path)
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
) -> ReadResult:
    """Read a text-based tabular file (CSV, TSV, pipe, semicolon).

    Args:
        path: File path.
        info: Format detection result with delimiter and encoding.
        skip_rows: Explicit preamble rows to skip (overrides auto-detection).
        skip_trailing_patterns: Regex patterns for trailing junk rows.

    Returns:
        ReadResult with the parsed DataFrame and metadata.
    """
    encoding = info.encoding
    delimiter = info.delimiter or ","

    # Explicit skip_rows implies a header at that row; auto-detection both
    # locates the header and decides whether the file has one at all.
    has_header = True
    if skip_rows is None:
        skip_rows, has_header = _detect_header(path, encoding, delimiter)

    df = pl.read_csv(
        path,
        separator=delimiter,
        encoding=encoding if encoding != "utf-8-sig" else "utf8",
        skip_rows=skip_rows,
        has_header=has_header,
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
    )


def _detect_header(path: Path, encoding: str, delimiter: str) -> tuple[int, bool]:
    """Locate the header row, or determine the file is headerless.

    Scans up to the first 30 content rows and decides between two outcomes:

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
        path: File path.
        encoding: File encoding.
        delimiter: Column delimiter.

    Returns:
        ``(skip_rows, has_header)`` — rows to skip before the header (or
        before the first data row when headerless), and whether a header
        row is present.
    """
    enc = encoding if encoding != "utf-8-sig" else "utf-8"
    lines: list[str] = []
    try:
        with open(path, encoding=enc, errors="replace") as f:  # noqa: PTH123 — standard file open
            for i, line in enumerate(f):
                if i >= 30:
                    break
                lines.append(line.rstrip("\n\r"))
    except OSError:
        return 0, True

    # Collect qualifying rows up front, then look for a real header before
    # falling back to headerless. A header reads as labels (low numeric ratio)
    # and does not itself parse as a transaction (no date+amount), so scanning
    # the whole window for one skips any number of data-like preamble rows —
    # opening/closing-balance summary lines — sitting above the header instead
    # of mistaking the first of them for a headerless table. Only when no
    # header row exists is the file genuinely headerless: the Wells Fargo case,
    # where every row leads with a date and so none reads as a header.
    qualifying: list[tuple[int, list[str]]] = []
    for i, line in enumerate(lines):
        if not line.strip():
            continue
        parts = line.split(delimiter)
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


def _looks_like_data_row(cells: list[str]) -> bool:
    """Return True if a row already parses as a transaction record.

    A genuine data row carries both a parseable date and a numeric amount; a
    header row carries neither (``Date``/``Amount`` are labels, not values).
    Used to detect headerless files before the first row is consumed as a
    header. Reuses ``detect_date_format`` so date recognition stays coherent
    with the rest of the tabular pipeline.

    Args:
        cells: Non-empty, unquoted cell strings from one row.

    Returns:
        True when at least one cell is a date and at least one is numeric.
    """
    has_date = any(detect_date_format([c])[0] is not None for c in cells)
    has_amount = any(_is_numeric(c) for c in cells)
    return has_date and has_amount


def _looks_like_header(cells: list[str]) -> bool:
    """Return True if a row reads as a header rather than a data record.

    A header carries mostly non-numeric labels (``Date``, ``Amount``,
    ``Description``), so its numeric ratio is low. Mirrors the inline header
    test in ``_detect_header`` so the peek-ahead and the main scan agree.

    Args:
        cells: Non-empty, unquoted cell strings from one row.

    Returns:
        True when the row has at least two cells and fewer than half are
        numeric.
    """
    if len(cells) < 2:
        return False
    numeric_count = sum(1 for c in cells if _is_numeric(c))
    return numeric_count / len(cells) < 0.5


def _is_numeric(s: str) -> bool:
    """Return True if the string represents a numeric value.

    Args:
        s: String to test.

    Returns:
        True if parseable as a float after stripping currency symbols.
    """
    s = s.replace(",", "").replace("$", "").replace("€", "").strip()
    try:
        float(s)
        return True
    except ValueError:
        return False


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


def _read_excel(
    path: Path,
    info: FormatInfo,
    *,
    skip_rows: int | None = None,
    sheet: str | None = None,
) -> ReadResult:
    """Read an Excel (.xlsx) file.

    Args:
        path: File path.
        info: Format detection result (unused for Excel, kept for API consistency).
        skip_rows: Rows to skip after header detection.
        sheet: Sheet name to read. If None, picks the sheet with the most rows.

    Returns:
        ReadResult with the parsed DataFrame and sheet metadata.
    """
    import openpyxl

    sheet_used = sheet
    if sheet_used is None:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
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

    df = pl.read_excel(
        path,
        sheet_name=sheet_used,
        infer_schema_length=0,
    )

    actual_skip = 0
    if skip_rows is not None and skip_rows > 0:
        df = df.slice(skip_rows)
        actual_skip = skip_rows

    return ReadResult(df=df, skip_rows=actual_skip, sheet_used=sheet_used)


def _read_parquet(path: Path) -> ReadResult:
    """Read a Parquet file.

    Args:
        path: File path.

    Returns:
        ReadResult with the parsed DataFrame.
    """
    df = pl.read_parquet(path)
    return ReadResult(df=df)


def _read_feather(path: Path) -> ReadResult:
    """Read a Feather/Arrow IPC file.

    Args:
        path: File path.

    Returns:
        ReadResult with the parsed DataFrame.
    """
    df = pl.read_ipc(path)
    return ReadResult(df=df)

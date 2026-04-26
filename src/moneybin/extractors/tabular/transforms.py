"""Stage 4: Transform and validate mapped DataFrame.

Takes the mapped DataFrame from Stage 3 (column_mapper) and transforms it
into the raw.tabular_transactions schema shape:
- Parses dates and amounts according to detected/specified formats
- Normalizes amounts to MoneyBin sign convention (negative = expense)
- Generates deterministic transaction IDs for dedup
- Preserves original values for audit
- Filters invalid rows and tracks rejection details
- Assigns 1-based row numbers for debugging
"""

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

import polars as pl

from moneybin.extractors.tabular.date_detection import parse_amount_str
from moneybin.extractors.tabular.formats import (
    NumberFormatType,
    SignConventionType,
)

logger = logging.getLogger(__name__)

# Fields the transform layer understands in field_mapping
_SIGN_FIELDS = {"amount", "debit_amount", "credit_amount"}

# Optional string fields that pass through unchanged from source to raw table.
# These are read from the DataFrame column identified by field_mapping and
# written to the output as-is (NULL when unmapped or cell is empty).
_OPTIONAL_STR_FIELDS = (
    "memo",
    "category",
    "subcategory",
    "transaction_type",
    "status",
    "check_number",
    "source_transaction_id",
    "reference_number",
    "currency",
    "member_name",
)


def _col_as_strings(df: pl.DataFrame, col: str | None, n: int) -> list[str]:
    """Extract a column from a DataFrame as a list of strings.

    Returns ``[""] * n`` if the column is absent or None.

    Args:
        df: Source DataFrame.
        col: Column name, or None.
        n: Expected length (row count).

    Returns:
        String values, one per row — None values become ``""``.
    """
    if not col or col not in df.columns:
        return [""] * n
    return [str(v) if v is not None else "" for v in df[col].to_list()]


@dataclass
class RejectionDetail:
    """Details about a rejected row."""

    row_number: int
    """1-based row number in the source file."""

    reason: str
    """Human-readable rejection reason."""


@dataclass
class TransformResult:
    """Result of Stage 4 transform and validation."""

    transactions: pl.DataFrame
    """Transformed rows ready for raw.tabular_transactions ingestion."""

    rows_rejected: int = 0
    """Count of rows that failed validation and were excluded."""

    rejection_details: list[RejectionDetail] = field(default_factory=list)
    """Per-row rejection details for error reporting."""

    balance_validated: bool = False
    """True if running balance was checked and matched."""

    sign_auto_corrected: bool = False
    """True if amounts were negated to match running balance."""


def transform_dataframe(
    *,
    df: pl.DataFrame,
    field_mapping: dict[str, str],
    date_format: str,
    sign_convention: SignConventionType,
    number_format: NumberFormatType,
    account_id: str | list[str],
    source_file: str,
    source_type: str,
    source_origin: str,
    import_id: str,
    balance_pass_threshold: float = 0.90,
    balance_tolerance_cents: int = 1,
) -> TransformResult:
    """Transform a mapped DataFrame into the raw.tabular_transactions shape.

    Args:
        df: Source DataFrame with original column names.
        field_mapping: Destination field → source column mapping from Stage 3.
        date_format: strptime format string for parsing dates.
        sign_convention: One of SignConventionType literal values.
        number_format: One of NumberFormatType literal values.
        account_id: Account identifier — single string (broadcast to all rows)
            or list of per-row IDs (for multi-account files).
        source_file: Path to the source file (for provenance).
        source_type: File format type (csv, tsv, excel, parquet, etc.).
        source_origin: Institution or source name (e.g. "chase_credit").
        import_id: Unique ID for this import run.
        balance_pass_threshold: Fraction of balance deltas that must match for
            validation to pass (default 0.90).
        balance_tolerance_cents: Maximum allowed delta mismatch in cents
            (default 1, i.e. ±$0.01).

    Returns:
        TransformResult with validated transactions DataFrame and rejection stats.
    """
    # Normalize account_id to per-row list
    account_ids: list[str] = (
        account_id if isinstance(account_id, list) else [account_id] * len(df)
    )

    # Assign 1-based row numbers before any filtering
    df = df.with_columns(pl.Series("row_number", range(1, len(df) + 1), dtype=pl.Int32))

    # Extract raw string columns by canonical field name
    date_col = field_mapping.get("transaction_date")
    desc_col = field_mapping.get("description")
    src_txn_id_col = field_mapping.get("source_transaction_id")

    # Preserve originals before parsing
    original_date_strs = _col_as_strings(df, date_col, len(df))

    # Determine original amount string — for audit, use whichever column was mapped
    if sign_convention == "split_debit_credit":
        debit_col = field_mapping.get("debit_amount")
        credit_col = field_mapping.get("credit_amount")
        # Combine debit/credit into a single original string representation
        original_amount_strs = _combine_original_debit_credit(df, debit_col, credit_col)
    else:
        original_amount_strs = _col_as_strings(df, field_mapping.get("amount"), len(df))

    # Parse amounts
    parsed_amounts, amount_rejections = _extract_amounts(
        df=df,
        field_mapping=field_mapping,
        sign_convention=sign_convention,
        number_format=number_format,
    )

    # Parse dates
    parsed_dates, date_rejections = _parse_dates(original_date_strs, date_format)

    # Build per-row rejection set
    rejected_rows: set[int] = set()
    rejection_details: list[RejectionDetail] = []

    row_numbers = df["row_number"].to_list()

    for idx, row_num in enumerate(row_numbers):
        reasons: list[str] = []
        if idx in date_rejections:
            reasons.append(date_rejections[idx])
        if idx in amount_rejections:
            reasons.append(amount_rejections[idx])
        if reasons:
            rejected_rows.add(idx)
            rejection_details.append(
                RejectionDetail(
                    row_number=row_num,
                    reason="; ".join(reasons),
                )
            )

    # Extract optional string field values (None for unmapped/null cells)
    optional_strs: dict[str, list[str | None]] = {}
    for field_name in _OPTIONAL_STR_FIELDS:
        col = field_mapping.get(field_name)
        if not col or col not in df.columns:
            optional_strs[field_name] = [None] * len(df)
        else:
            optional_strs[field_name] = [
                str(v) if v is not None else None for v in df[col].to_list()
            ]

    # Parse optional post_date (non-fatal — failures produce None, not rejections)
    post_date_col = field_mapping.get("post_date")
    parsed_post_dates: list[date | None] = [None] * len(df)
    if post_date_col and post_date_col in df.columns:
        parsed_post_dates, _ = _parse_dates(
            _col_as_strings(df, post_date_col, len(df)), date_format
        )

    # Parse optional balance column
    balance_col = field_mapping.get("balance")
    parsed_balances: list[Decimal | None] = [None] * len(df)
    if balance_col and balance_col in df.columns:
        balance_strs_for_output = _col_as_strings(df, balance_col, len(df))
        parsed_balances = [
            parse_amount_str(s, number_format) for s in balance_strs_for_output
        ]

    # Generate transaction IDs
    descriptions: list[str] = []
    if desc_col and desc_col in df.columns:
        descriptions = [str(v) if v is not None else "" for v in df[desc_col].to_list()]
    else:
        descriptions = [""] * len(df)

    transaction_ids = _generate_transaction_ids(
        df=df,
        src_txn_id_col=src_txn_id_col,
        account_ids=account_ids,
        original_date_strs=original_date_strs,
        parsed_amounts=parsed_amounts,
        descriptions=descriptions,
        source_type=source_type,
    )

    # Build output rows, skipping rejected indices
    out_transaction_ids: list[str] = []
    out_transaction_dates: list[date] = []
    out_post_dates: list[date | None] = []
    out_amounts: list[Decimal] = []
    out_descriptions: list[str] = []
    out_original_amounts: list[str] = []
    out_original_dates: list[str] = []
    out_balances: list[Decimal | None] = []
    out_row_numbers: list[int] = []
    out_account_ids: list[str] = []
    out_source_files: list[str] = []
    out_source_types: list[str] = []
    out_source_origins: list[str] = []
    out_import_ids: list[str] = []
    optional_out: dict[str, list[str | None]] = {f: [] for f in _OPTIONAL_STR_FIELDS}

    for idx in range(len(df)):
        if idx in rejected_rows:
            continue
        out_transaction_ids.append(transaction_ids[idx])
        out_transaction_dates.append(parsed_dates[idx])  # type: ignore[arg-type]  # None rows are filtered above
        out_post_dates.append(parsed_post_dates[idx])
        out_amounts.append(parsed_amounts[idx])  # type: ignore[arg-type]  # None rows are filtered above
        out_descriptions.append(descriptions[idx])
        out_original_amounts.append(original_amount_strs[idx])
        out_original_dates.append(original_date_strs[idx])
        out_balances.append(parsed_balances[idx])
        out_row_numbers.append(row_numbers[idx])
        out_account_ids.append(account_ids[idx])
        out_source_files.append(source_file)
        out_source_types.append(source_type)
        out_source_origins.append(source_origin)
        out_import_ids.append(import_id)
        for field_name in _OPTIONAL_STR_FIELDS:
            optional_out[field_name].append(optional_strs[field_name][idx])

    # Build output DataFrame — data and schema dicts are constructed in
    # column order matching raw.tabular_transactions.
    data: dict[str, object] = {
        "transaction_id": out_transaction_ids,
        "transaction_date": out_transaction_dates,
        "post_date": out_post_dates,
        "amount": out_amounts,
        "description": out_descriptions,
        "original_amount": out_original_amounts,
        "original_date_str": out_original_dates,
        "balance": out_balances,
        "row_number": out_row_numbers,
        "account_id": out_account_ids,
        "source_file": out_source_files,
        "source_type": out_source_types,
        "source_origin": out_source_origins,
        "import_id": out_import_ids,
    }
    schema = {
        "transaction_id": pl.Utf8,
        "transaction_date": pl.Date,
        "post_date": pl.Date,
        "amount": pl.Decimal(precision=18, scale=2),
        "description": pl.Utf8,
        "original_amount": pl.Utf8,
        "original_date_str": pl.Utf8,
        "balance": pl.Decimal(precision=18, scale=2),
        "row_number": pl.Int32,
        "account_id": pl.Utf8,
        "source_file": pl.Utf8,
        "source_type": pl.Utf8,
        "source_origin": pl.Utf8,
        "import_id": pl.Utf8,
    }
    for field_name in _OPTIONAL_STR_FIELDS:
        data[field_name] = optional_out[field_name]
        schema[field_name] = pl.Utf8

    transactions = pl.DataFrame(data, schema=schema)

    rows_rejected = len(rejected_rows)
    logger.info(
        f"Transform complete: {len(transactions)} accepted, {rows_rejected} rejected"
    )

    result = TransformResult(
        transactions=transactions,
        rows_rejected=rows_rejected,
        rejection_details=rejection_details,
    )

    # Running balance validation (Stage 4 optional check)
    if "balance" in field_mapping:
        balance_col = field_mapping["balance"]
        if balance_col in df.columns:
            balance_strs = [
                str(v) if v is not None else ""
                for v in df[balance_col].cast(pl.Utf8).to_list()
            ]
            result = _validate_running_balance(
                result,
                balance_strs,
                number_format,
                pass_threshold=balance_pass_threshold,
                tolerance_cents=balance_tolerance_cents,
            )

    return result


def _parse_dates(
    date_strings: list[str],
    date_format: str,
) -> tuple[list[date | None], dict[int, str]]:
    """Parse date strings using the given strptime format.

    Args:
        date_strings: Raw date strings, one per row.
        date_format: strptime format string.

    Returns:
        Tuple of (parsed dates list, rejection map {idx: reason}).
    """
    from datetime import datetime

    parsed: list[date | None] = []
    rejections: dict[int, str] = {}

    for idx, s in enumerate(date_strings):
        if not s or not s.strip():
            parsed.append(None)
            rejections[idx] = "Missing date value"
            continue
        try:
            dt = datetime.strptime(s.strip(), date_format)
            parsed.append(dt.date())
        except ValueError:
            parsed.append(None)
            rejections[idx] = f"Unparseable date: {s!r} with format {date_format!r}"

    return parsed, rejections


def _extract_amounts(
    *,
    df: pl.DataFrame,
    field_mapping: dict[str, str],
    sign_convention: SignConventionType,
    number_format: NumberFormatType,
) -> tuple[list[Decimal | None], dict[int, str]]:
    """Extract and normalize amounts from the DataFrame.

    Handles both single-amount and split debit/credit columns.

    Args:
        df: Source DataFrame.
        field_mapping: Destination field → source column mapping.
        sign_convention: One of SignConventionType literal values.
        number_format: One of NumberFormatType literal values.

    Returns:
        Tuple of (amounts list, rejection map {idx: reason}).
    """
    parsed: list[Decimal | None] = []
    rejections: dict[int, str] = {}
    n = len(df)

    if sign_convention == "split_debit_credit":
        debit_col = field_mapping.get("debit_amount")
        credit_col = field_mapping.get("credit_amount")

        debit_strs = _col_as_strings(df, debit_col, n)
        credit_strs = _col_as_strings(df, credit_col, n)

        for idx in range(n):
            debit_val = parse_amount_str(debit_strs[idx], number_format)
            credit_val = parse_amount_str(credit_strs[idx], number_format)

            if debit_val is not None and debit_val != 0:
                # Debit = expense → negative
                parsed.append(-abs(debit_val))
            elif credit_val is not None and credit_val != 0:
                # Credit = income → positive
                parsed.append(abs(credit_val))
            else:
                # Both empty/zero — treat as rejection
                parsed.append(None)
                rejections[idx] = "Both debit and credit are empty or zero"

    else:
        amount_col = field_mapping.get("amount")
        if not amount_col or amount_col not in df.columns:
            return [None] * n, dict.fromkeys(range(n), "Missing amount column")

        amount_strs = _col_as_strings(df, amount_col, n)

        for idx, s in enumerate(amount_strs):
            val = parse_amount_str(s, number_format)
            if val is None:
                parsed.append(None)
                rejections[idx] = f"Unparseable amount: {s!r}"
            elif sign_convention == "negative_is_income":
                # Source convention is inverted: flip the sign
                parsed.append(-val)
            else:
                # negative_is_expense — MoneyBin native convention, no change
                parsed.append(val)

    return parsed, rejections


def _combine_original_debit_credit(
    df: pl.DataFrame,
    debit_col: str | None,
    credit_col: str | None,
) -> list[str]:
    """Build original amount strings from split debit/credit columns.

    Produces a string like "debit:42.50" or "credit:100.00" for audit.

    Args:
        df: Source DataFrame.
        debit_col: Column name for debits (or None if absent).
        credit_col: Column name for credits (or None if absent).

    Returns:
        List of original amount strings, one per row.
    """
    n = len(df)
    debit_strs = _col_as_strings(df, debit_col, n)
    credit_strs = _col_as_strings(df, credit_col, n)

    result: list[str] = []
    for d, c in zip(debit_strs, credit_strs, strict=True):
        if d and d.strip():
            result.append(f"debit:{d}")
        elif c and c.strip():
            result.append(f"credit:{c}")
        else:
            result.append("")
    return result


def _validate_running_balance(
    result: TransformResult,
    balance_strs: list[str],
    number_format: NumberFormatType,
    *,
    pass_threshold: float = 0.90,
    tolerance_cents: int = 1,
) -> TransformResult:
    """Validate running balance consistency against transaction amounts.

    Checks that sequential balance deltas (balance[n] - balance[n-1]) match
    the corresponding transaction amounts within a ±0.01 tolerance.

    If the forward pass fails but succeeds after sign inversion, the amounts
    in result.transactions are negated (auto-correcting an inverted sign
    convention) and balance_validated is set to True.

    Args:
        result: Current TransformResult with a parsed ``transactions`` DataFrame.
        balance_strs: Raw balance strings from the source file, one per source row.
            May be longer than ``result.transactions`` if rows were rejected.
        number_format: One of NumberFormatType literal values.
        pass_threshold: Fraction of balance deltas that must match for validation
            to pass (default 0.90).
        tolerance_cents: Maximum allowed delta mismatch in cents (default 1,
            i.e. ±$0.01).

    Returns:
        Updated TransformResult with ``balance_validated`` set and, when
        auto-correction fires, negated amounts in ``transactions``.
    """
    _balance_tolerance = (Decimal(tolerance_cents) / Decimal(100)).quantize(
        Decimal("0.01")
    )
    _pass_threshold = pass_threshold

    amounts: list[Decimal] = result.transactions["amount"].to_list()
    row_numbers: list[int] = result.transactions["row_number"].to_list()
    n = len(amounts)

    if n < 2:
        # Need at least two rows to compute a delta
        return result

    # Parse balance strings once, mapped by accepted row numbers (0-indexed).
    balances: list[Decimal | None] = []
    for row_num in row_numbers:
        idx = row_num - 1
        if idx < len(balance_strs):
            balances.append(parse_amount_str(balance_strs[idx], number_format))
        else:
            balances.append(None)

    # Pre-compute valid consecutive pairs where both balances are present
    valid_pairs: list[tuple[int, Decimal]] = []
    for i in range(1, n):
        b_prev = balances[i - 1]
        b_curr = balances[i]
        if b_prev is not None and b_curr is not None:
            valid_pairs.append((i, (b_curr - b_prev).quantize(Decimal("0.01"))))

    def _pass_rate(amt_list: list[Decimal]) -> float:
        """Fraction of valid balance pairs where delta ≈ amount."""
        checks = len(valid_pairs)
        if checks == 0:
            return 1.0  # no balance data to validate
        passed = sum(
            1
            for i, delta in valid_pairs
            if abs(delta - amt_list[i]) <= _balance_tolerance
        )
        return passed / checks

    forward_rate = _pass_rate(amounts)

    if forward_rate >= _pass_threshold:
        result.balance_validated = True
        logger.info(
            f"Running balance validated: {forward_rate:.0%} of deltas match amounts"
        )
        return result

    # Try sign inversion
    inverted = [-a for a in amounts]
    inverted_rate = _pass_rate(inverted)

    if inverted_rate >= _pass_threshold:
        logger.warning(
            f"⚠️  Running balance validated after sign inversion: "
            f"{inverted_rate:.0%} pass rate — auto-correcting amounts. "
            f"Use --sign to override if this is wrong."
        )
        result.transactions = result.transactions.with_columns(
            (-pl.col("amount")).alias("amount")
        )
        result.balance_validated = True
        result.sign_auto_corrected = True
        return result

    logger.warning(
        f"Running balance inconsistent: forward {forward_rate:.0%}, "
        f"inverted {inverted_rate:.0%} — balance_validated=False"
    )
    return result


def _generate_transaction_ids(
    *,
    df: pl.DataFrame,
    src_txn_id_col: str | None,
    account_ids: list[str],
    original_date_strs: list[str],
    parsed_amounts: list[Decimal | None],
    descriptions: list[str],
    source_type: str,
) -> list[str]:
    """Generate transaction IDs using source ID if available, else content hash.

    ID strategy (per identifiers.md):
    1. Source-provided ID → "{account_id}:{source_txn_id}"
    2. Content hash → "{source_type}_{sha256_16hex}"
       Input: "{date}|{amount}|{description}|{account_id}"

    Args:
        df: Source DataFrame.
        src_txn_id_col: Column name holding source transaction IDs, or None.
        account_ids: Per-row account identifiers.
        original_date_strs: Raw date strings, one per row.
        parsed_amounts: Parsed Decimal amounts (None for invalid rows).
        descriptions: Description strings, one per row.
        source_type: File type prefix for content-hash IDs.

    Returns:
        List of transaction ID strings, one per row.
    """
    n = len(df)
    src_txn_ids: list[str | None] = [None] * n

    if src_txn_id_col and src_txn_id_col in df.columns:
        raw = df[src_txn_id_col].cast(pl.Utf8).to_list()
        src_txn_ids = [str(v) if v is not None else None for v in raw]

    ids: list[str] = []
    for idx in range(n):
        acct_id = account_ids[idx]
        src_id = src_txn_ids[idx]
        if src_id and src_id.strip():
            ids.append(f"{acct_id}:{src_id.strip()}")
        else:
            date_str = original_date_strs[idx] if idx < len(original_date_strs) else ""
            amount_str = (
                str(parsed_amounts[idx]) if parsed_amounts[idx] is not None else ""
            )
            desc_str = descriptions[idx] if idx < len(descriptions) else ""
            raw_key = f"{date_str}|{amount_str}|{desc_str}|{acct_id}"
            digest = hashlib.sha256(raw_key.encode()).hexdigest()[:16]
            ids.append(f"{source_type}_{digest}")

    return ids

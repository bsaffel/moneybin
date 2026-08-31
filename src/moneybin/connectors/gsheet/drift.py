"""Drift detection: compare current sheet headers + sample against pinned signature."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass, field

import polars as pl


@dataclass(frozen=True)
class DriftReport:
    """Immutable report of drift detection in a gsheet pull."""

    is_drift: bool
    reason: str
    missing_headers: list[str] = field(default_factory=list)
    empty_mapped_columns: list[str] = field(default_factory=list)
    new_columns: list[str] = field(default_factory=list)


_NULL_THRESHOLD = 0.5  # >50% null in sample counts as "mapped column is empty"


def duplicated_headers(headers: list[str]) -> list[str]:
    """Header texts that appear more than once, in first-seen order."""
    seen: set[str] = set()
    repeated: dict[str, None] = {}
    for header in headers:
        if header in seen:
            repeated[header] = None
        seen.add(header)
    return list(repeated)


def duplicate_mapped_header_drift(
    *,
    raw_headers: list[str],
    mapped_sources: Collection[str],
) -> DriftReport | None:
    """Drift when a header the pinned mapping reads has gained a twin.

    ``rows_to_df`` renames the second occurrence, so by the time headers reach
    ``detect_drift`` the duplicate looks like an ordinary new column — and new
    columns are explicitly not drift. The pinned mapping then keeps importing
    the first occurrence while the twin's values are dropped and the pull still
    reports success. Callers pass the raw header row, before deduplication,
    because that is the only place the duplication is still visible.

    Only for adapters that read a mapping. One that imports every column loses
    nothing to a rename and must not be pinned into drift over it.
    """
    duplicated = [h for h in duplicated_headers(raw_headers) if h in mapped_sources]
    if not duplicated:
        return None
    return DriftReport(
        is_drift=True,
        reason=(
            f"duplicated mapped headers: {duplicated}. Only the first column of "
            "each is imported. Reconnect to re-pin the mapping, or remove the "
            "duplicate column in the sheet."
        ),
    )


def detect_drift(
    *,
    pinned_signature: list[str],
    current_headers: list[str],
    sample_df: pl.DataFrame,
    mapped_columns: set[str],
) -> DriftReport:
    """Compare current sheet against pinned signature; return DriftReport.

    Drift triggers:
      1. Any pinned header is missing from current headers.
      2. A column in ``mapped_columns`` is >50% null in the sample.

    ``mapped_columns`` is the set of columns whose emptiness counts as drift —
    callers pass only the REQUIRED columns, not every mapped one, so optional
    columns that are routinely blank don't pin the connection in drift.

    Non-drift:
      - Reordered headers (set match).
      - New columns (not in pinned signature).
    """
    pinned_set = set(pinned_signature)
    current_set = set(current_headers)

    missing = [h for h in pinned_signature if h not in current_set]
    new_columns = [h for h in current_headers if h not in pinned_set]

    empty_mapped: list[str] = []
    for col in mapped_columns:
        if col not in current_set:
            continue
        if col in sample_df.columns and _null_ratio(sample_df[col]) > _NULL_THRESHOLD:
            empty_mapped.append(col)

    is_drift = bool(missing) or bool(empty_mapped)
    parts: list[str] = []
    if missing:
        parts.append(f"missing headers: {missing}")
    if empty_mapped:
        parts.append(f"empty mapped columns: {empty_mapped}")
    reason = "; ".join(parts) if parts else "no drift"

    return DriftReport(
        is_drift=is_drift,
        reason=reason,
        missing_headers=missing,
        empty_mapped_columns=empty_mapped,
        new_columns=new_columns,
    )


def _null_ratio(col: pl.Series) -> float:
    if col.len() == 0:
        return 0.0
    nulls = col.null_count()
    blanks_val = 0
    if col.dtype == pl.String:
        blanks_sum = col.cast(pl.String, strict=False).str.strip_chars().eq("").sum()  # type: ignore[reportUnknownMemberType]
        blanks_val = int(blanks_sum) if blanks_sum else 0
    return (nulls + blanks_val) / col.len()

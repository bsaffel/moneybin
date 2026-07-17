"""Stage 3: Column mapping engine.

Takes a DataFrame (headers + sample rows), produces a field mapping with
a confidence tier. This is the core intelligence of the smart importer.
"""

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import polars as pl

from moneybin.extractors.tabular.date_detection import (
    detect_date_format,
    detect_number_format,
    parse_amount_str,
)
from moneybin.extractors.tabular.field_aliases import (
    ACCOUNT_IDENTIFYING_FIELDS,
    match_header_to_field,
)
from moneybin.extractors.tabular.formats import (
    ConfidenceType,
    NumberFormatType,
    SignConventionType,
)
from moneybin.extractors.tabular.sign_convention import (
    infer_sign_convention,
)

if TYPE_CHECKING:
    from moneybin.extractors.confidence import Confidence

logger = logging.getLogger(__name__)

_SAMPLE_SIZE = 20


def _collect_samples(df: pl.DataFrame, col: str) -> list[str | None]:
    """Extract sample values from a column as strings.

    Args:
        df: Source DataFrame.
        col: Column name to sample.

    Returns:
        Up to ``_SAMPLE_SIZE`` string values (None for null cells).
    """
    vals = df[col].head(_SAMPLE_SIZE).cast(pl.Utf8).to_list()
    return [str(v) if v is not None else None for v in vals]


@dataclass
class MappingResult:
    """Result of column mapping (Stage 3 output)."""

    field_mapping: dict[str, str]
    """Destination field → source column name."""

    confidence: ConfidenceType
    """Confidence tier: high, medium, low."""

    date_format: str | None = None
    """Detected date format string."""

    number_format: NumberFormatType = "us"
    """Detected number format convention."""

    sign_convention: SignConventionType = "negative_is_expense"
    """Detected sign convention."""

    sign_needs_confirmation: bool = False
    """True if sign convention is ambiguous."""

    sign_evidence_header: str | None = None
    """Original amount header that triggered the sign-convention inference."""

    is_multi_account: bool = False
    """True if account-identifying columns were detected."""

    unmapped_columns: list[str] = field(default_factory=list)
    """Source columns with no destination field match."""

    flagged_fields: list[str] = field(default_factory=list)
    """Fields matched with low confidence (content-only)."""

    sample_values: dict[str, list[str]] = field(default_factory=dict)
    """Sample values for each mapped field."""

    score: float = 0.0
    """Normalized confidence in [0, 1]. Bands derive the `confidence` tier."""

    missing_required: tuple[str, ...] = ()
    """Required destination fields that could not be resolved at all."""

    structural_red_flag: bool = False
    """True when a structural signal outside content-matching (e.g. the
    consumed header row itself parses as a transaction) makes the mapping
    untrustworthy regardless of `score`. Forces the `low` tier so the
    propose->confirm gate engages instead of an agent self-accepting."""

    def to_confidence(self, *, t_high: float, t_med: float) -> "Confidence":
        """Return the channel-agnostic Confidence view of this result.

        `t_high` / `t_med` typically come from `ImportSettings.confidence`;
        passing them here lets callers respect runtime-tuned thresholds.
        """
        from moneybin.extractors.confidence import Confidence, resolve_tier

        return Confidence(
            score=self.score,
            tier=resolve_tier(
                self.score,
                t_high=t_high,
                t_med=t_med,
                structural_red_flag=self.structural_red_flag,
            ),
            flagged=tuple(self.flagged_fields),
            missing_required=self.missing_required,
        )


def map_columns(
    df: pl.DataFrame,
    *,
    overrides: dict[str, str] | None = None,
    t_high: float = 0.90,
    t_med: float = 0.70,
    structural_red_flag: bool = False,
) -> MappingResult:
    """Map source columns to destination fields.

    Args:
        df: Source DataFrame from Stage 2.
        overrides: Explicit field→column overrides from user.
        t_high: High-confidence threshold for the ``confidence`` tier label.
            Production callers should pass ``settings.import_.confidence.t_high``
            so the displayed tier agrees with the runtime-tuned bands the
            confirm primitive uses. Defaults match ``ConfidenceBands`` defaults.
        t_med: Medium-confidence threshold (same notes as ``t_high``).
        structural_red_flag: A structural signal from Stage 2 (e.g.
            ``ReadResult.header_row_looks_like_data``) outside content-matching
            that makes the mapping untrustworthy regardless of score. Forces
            the ``low`` tier so the propose->confirm gate engages instead of
            an agent self-accepting.

    Returns:
        MappingResult with mapping, confidence, and metadata.
    """
    mapping: dict[str, str] = {}
    claimed: set[str] = set()
    flagged: list[str] = []
    _samples: dict[str, list[str | None]] = {}

    # Apply overrides first
    if overrides:
        for dest_field, src_col in overrides.items():
            if src_col in df.columns:
                mapping[dest_field] = src_col
                claimed.add(src_col)

    # Header matching via alias table
    for col in df.columns:
        if col in claimed:
            continue
        dest = match_header_to_field(col)
        if dest and dest not in mapping:
            mapping[dest] = col
            claimed.add(col)

    # Collect sample values for mapped fields
    for dest, src in mapping.items():
        _samples[dest] = _collect_samples(df, src)

    # Content validation on date fields
    date_format = None
    if "transaction_date" in mapping:
        date_vals = _samples.get("transaction_date", [])
        date_format, _ = detect_date_format(date_vals)
        if date_format is None:
            flagged.append("transaction_date")

    # Detect number format from amount values
    amount_vals: list[str | None] = _samples.get(
        "amount", _samples.get("debit_amount", [])
    )
    number_format = detect_number_format(amount_vals) if amount_vals else "us"

    # Fallback discovery for required fields not yet mapped.
    # Ordered so specific detectors (date, amount) run before the
    # broad description detector, preventing dates from being
    # misidentified as descriptions and claimed prematurely.
    discovery_order = ("transaction_date", "amount", "description")
    for req_field in discovery_order:
        if req_field not in mapping:
            candidate = _discover_by_content(df, req_field, claimed)
            if candidate:
                mapping[req_field] = candidate
                claimed.add(candidate)
                flagged.append(req_field)
                _samples[req_field] = _collect_samples(df, candidate)
                if req_field == "transaction_date" and date_format is None:
                    date_format, _ = detect_date_format(_samples[req_field])

    # Sign convention inference
    sign_result = infer_sign_convention(
        amount_values=_samples.get("amount"),
        debit_values=_samples.get("debit_amount"),
        credit_values=_samples.get("credit_amount"),
        header_context=mapping.get("amount", ""),
    )

    # Multi-account detection
    is_multi_account = bool(set(mapping.keys()) & ACCOUNT_IDENTIFYING_FIELDS)

    # Confidence tier
    unmapped = [c for c in df.columns if c not in claimed]
    score, missing_required = _score_mapping(mapping, flagged, date_format)
    from moneybin.extractors.confidence import resolve_tier

    confidence = resolve_tier(
        score, t_high=t_high, t_med=t_med, structural_red_flag=structural_red_flag
    )

    # Convert None → "" for the public sample_values (callers don't need nulls).
    sample_values: dict[str, list[str]] = {
        k: [v if v is not None else "" for v in vs] for k, vs in _samples.items()
    }

    return MappingResult(
        field_mapping=mapping,
        confidence=confidence,
        date_format=date_format,
        number_format=number_format,
        sign_convention=sign_result.convention,
        sign_needs_confirmation=sign_result.needs_confirmation,
        sign_evidence_header=sign_result.evidence_header,
        is_multi_account=is_multi_account,
        unmapped_columns=unmapped,
        flagged_fields=flagged,
        sample_values=sample_values,
        score=score,
        missing_required=missing_required,
        structural_red_flag=structural_red_flag,
    )


def _discover_by_content(
    df: pl.DataFrame,
    target_field: str,
    claimed: set[str],
) -> str | None:
    """Discover a destination field from column content analysis.

    Args:
        df: Source DataFrame to scan.
        target_field: Destination field name to find a column for.
        claimed: Set of already-claimed column names to skip.

    Returns:
        Best-matching column name, or None if no candidate scores > 0.
    """
    candidates: list[tuple[str, float]] = []

    for col in df.columns:
        if col in claimed:
            continue
        vals: list[str | None] = df[col].head(_SAMPLE_SIZE).cast(pl.Utf8).to_list()
        clean = [v for v in vals if v is not None and v.strip()]
        if not clean:
            continue

        score = _score_column_for_field(clean, target_field)
        if score > 0:
            candidates.append((col, score))

    if candidates:
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]
    return None


def _score_column_for_field(values: list[str], field_name: str) -> float:
    """Score how well a column's content matches a target field type.

    Args:
        values: Non-empty sample string values from the column.
        field_name: Destination field name to score against.

    Returns:
        Score in [0.0, 1.0]; 0.0 means no match.
    """
    if field_name == "transaction_date":
        date_fmt, confidence = detect_date_format(values)  # type: ignore[arg-type]  # list[str] satisfies list[str | None]
        if date_fmt:
            return 0.9 if confidence == "high" else 0.6
        return 0.0

    if field_name == "amount":
        numeric_count = sum(1 for v in values if parse_amount_str(v, "us") is not None)
        ratio = numeric_count / len(values) if values else 0
        return ratio * 0.9 if ratio >= 0.8 else 0.0

    if field_name == "description":
        unique_ratio = len(set(values)) / len(values) if values else 0
        avg_len = sum(len(v) for v in values) / len(values) if values else 0
        numeric_count = sum(1 for v in values if parse_amount_str(v, "us") is not None)
        numeric_ratio = numeric_count / len(values) if values else 0
        if unique_ratio > 0.5 and avg_len > 5 and numeric_ratio < 0.3:
            return 0.7
        return 0.0

    return 0.0


def _score_mapping(
    mapping: dict[str, str],
    flagged: list[str],
    date_format: str | None,
) -> tuple[float, tuple[str, ...]]:
    """Compute a normalized confidence score + the missing-required set."""
    has_date = "transaction_date" in mapping
    has_single_amount = "amount" in mapping
    has_debit = "debit_amount" in mapping
    has_credit = "credit_amount" in mapping
    has_amount = has_single_amount or (has_debit and has_credit)
    missing: list[str] = []
    if not has_date:
        missing.append("transaction_date")
    if not has_amount:
        # When the detector found one half of a debit/credit pair, the
        # actionable fix is the missing half — not "amount". Reporting
        # "amount" here sends the user to a contradictory override
        # (single-amount layered on partial-split).
        if has_debit and not has_credit:
            missing.append("credit_amount")
        elif has_credit and not has_debit:
            missing.append("debit_amount")
        else:
            missing.append("amount")
    if "description" not in mapping:
        missing.append("description")
    if missing:
        return 0.40, tuple(missing)
    if date_format is None:
        return 0.75, ()
    if flagged:
        return 0.85, ()
    return 1.0, ()

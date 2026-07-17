"""Recipe schema + bounded executor for deterministic PDF extraction (Req 9b).

A Recipe is a JSON-serializable descriptor that tells the executor how to carve
a region from raw PDF text and parse its rows into typed fields.

Security model
--------------
Phase 2b will accept bridge-authored recipes against untrusted document text.
Two bounds are enforced here so Phase 2b can plug in without a posture change:

  a) Static bound — max pattern length (``_MAX_PATTERN_LEN``). Prevents patterns
     long enough to encode meaningful computation or payload.
  b) Dynamic bound — per-match wall-clock timeout (``_PATTERN_TIMEOUT_MS``) via
     the `regex` package. stdlib `re` has no timeout= parameter; `regex` does.
     Without this, a ReDoS-friendly pattern + adversarial document text hangs
     the process indefinitely.

Both are module constants (per security.md "constants" rule) — defined once
here and never duplicated at call sites.
"""

from __future__ import annotations

import logging
import re as _stdlib_re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

import regex as _re
from pydantic import BaseModel, Field, model_validator

from moneybin.extractors.pdf.metadata import capture_metadata

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Security constants (Req 9b)
# ---------------------------------------------------------------------------

_MAX_PATTERN_LEN = 200  # static bound, Req 9b part a
_PATTERN_TIMEOUT_MS = 100  # wall-clock bound per match (ms), Req 9b part b
_PATTERN_TIMEOUT_SEC = _PATTERN_TIMEOUT_MS / 1000.0  # regex.timeout= takes seconds

# Heuristic to detect nested unbounded quantifiers — (X+)+ family, plus
# non-capturing (?:X+)+ shapes which are equally catastrophic in Python re.
# Not a full ReDoS oracle; the dynamic timeout is the primary defence and
# this is defence-in-depth at save time. (Req 9b)
_NESTED_UNBOUNDED_RE = _stdlib_re.compile(r"\([^)]*[+*]\)[+*]")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class FieldExtraction(BaseModel):
    """One named capture field within a recipe."""

    name: str
    pattern: str  # bounded by Recipe._bound_patterns validator
    cast: Literal["str", "decimal", "date", "int"]
    date_format: str | None = None


class RegionAnchors(BaseModel):
    """Text anchors that carve the table region from document text."""

    start_anchor: str
    end_anchor: str


class Recipe(BaseModel):
    """Serializable descriptor for deterministic PDF row extraction."""

    # ``None`` means "no explicit anchors authored — fall back to
    # capture_metadata's DEFAULT_ANCHORS". An empty list means "this recipe
    # deliberately declines metadata capture" (Phase 2b bridge-authored
    # recipes for statement formats with no balance lines). The distinction
    # matters so a deliberately-empty list isn't silently overridden by
    # DEFAULT_ANCHORS during replay — see routing.route_pdf_import.
    metadata_anchors: list[FieldExtraction] | None = Field(default=None)
    row_region: RegionAnchors
    row_split: str
    fields: list[FieldExtraction]
    sign_convention: Literal[
        "negative_is_expense", "negative_is_income", "split_debit_credit"
    ]
    # True only when a human overrode the detector with an explicit `sign=` on
    # this format's import (ImportService._gate_pdf_sign_convention). It tells
    # the replay guard that the convention above is an assertion, not an
    # inference, so auto_derive.recipe_polarity_fits must not second-guess it —
    # without that, a sign= override correcting a false-positive card detection
    # can never replay (the guard refuses the corrected recipe on the very
    # markers that caused the false positive) and the user re-overrides forever.
    # NOT set by confirm=True: agreeing with the detector needs no bypass, and
    # granting one there would strip the guard in the dangerous direction. NOT
    # settable by an agent either — bridge.parse_bridge_response rejects a
    # response naming this key, because the bridge apply path skips the confirm
    # gate and persists what it is handed.
    # Additive with a default — old recipes in the app.pdf_formats JSON blob
    # deserialize as False, so no migration is required.
    sign_ratified: bool = False
    number_format: Literal["us", "european", "swiss_french", "zero_decimal"] = "us"
    routing: Literal["transactions", "seed"]

    @model_validator(mode="after")
    def _bound_patterns(self) -> Recipe:
        """Enforce static security bounds on every regex the executor will run."""
        for f in [*(self.metadata_anchors or []), *self.fields]:
            self._check_pattern(f.pattern, f"field '{f.name}'")
        # row_split is also executed against document text in execute_recipe;
        # bound it on the same terms or a pathological splitter bypasses both
        # the length cap and the nested-quantifier heuristic.
        self._check_pattern(self.row_split, "row_split")
        return self

    @staticmethod
    def _check_pattern(pattern: str, label: str) -> None:
        if len(pattern) > _MAX_PATTERN_LEN:
            raise ValueError(
                f"max_pattern_len ({_MAX_PATTERN_LEN}) exceeded for {label}"
            )
        if _has_nested_unbounded_quantifier(pattern):
            raise ValueError(
                f"nested unbounded quantifier in {label} — "
                "pattern rejected to prevent catastrophic backtracking"
            )
        # Reject patterns that won't compile, using the same `regex` engine the
        # executor runs. An agent-authored recipe with a malformed regex then
        # fails validation here — wrapped to BridgeResponseError →
        # bridge_response_invalid — instead of raising a cryptic regex.error
        # deep in execute_recipe. Only row_split + field patterns reach this
        # (the row_region anchors are matched literally via str.find, never
        # compiled, so a special-char anchor like "Balance ($)" is valid).
        try:
            _re.compile(pattern)
        except _re.error as exc:
            raise ValueError(f"invalid regex in {label}: {exc}") from exc


# ---------------------------------------------------------------------------
# Output types
# ---------------------------------------------------------------------------


@dataclass
class ExtractedRows:
    """Result of executing a recipe against document text."""

    rows: list[dict[str, Any]]
    # metadata is populated by Task 5 (metadata.py); empty dict until then.
    metadata: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _has_nested_unbounded_quantifier(pattern: str) -> bool:
    """Return True if pattern contains (X+)+ family — see Req 9b."""
    return bool(_NESTED_UNBOUNDED_RE.search(pattern))


def _carve_region(text: str, anchors: RegionAnchors) -> str:
    """Return the text between start_anchor and end_anchor (exclusive).

    If either anchor is missing the full text is returned so the executor can
    still attempt extraction (some statements omit explicit section headers).
    The fallback is logged so a misconfigured recipe is observable.
    """
    start_idx = text.find(anchors.start_anchor)
    # Search for end_anchor only AFTER start_anchor — a transaction
    # description containing the end-anchor text (e.g. "Year-to-Date
    # Total: $5,000" or a merchant named "Total Kitchen") would otherwise
    # truncate the carve region and silently drop every subsequent row.
    after_start = start_idx + len(anchors.start_anchor) if start_idx != -1 else 0
    end_idx = text.find(anchors.end_anchor, after_start)
    if start_idx == -1 or end_idx == -1 or end_idx <= start_idx:
        # The full-text fallback usually results in reconciliation failing
        # later (summary/balance rows look like transactions) and silently
        # routes to seed. Surface the cause here so operators tailing logs
        # can see when an end_anchor mismatch is the root cause — most real
        # bank PDFs use "Totals" / "TOTAL" / "Total Transactions" rather
        # than the auto-derive default of "Total:".
        logger.warning(
            f"row_region anchors not found in document "
            f"(start_anchor={anchors.start_anchor!r} found={start_idx != -1}, "
            f"end_anchor={anchors.end_anchor!r} found={end_idx != -1}); "
            f"falling back to full text — reconciliation may fail downstream"
        )
        return text
    return text[after_start:end_idx]


def _cast(field: FieldExtraction, raw: str) -> Any:
    """Cast a raw string to the declared type; raises ValueError on failure.

    For numeric casts, an empty string returns Decimal("0") / 0 so that
    split_debit_credit recipes (which use an optional amount pattern for
    Debit and Credit) yield a clean zero on the blank side of each row.
    """
    if field.cast == "str":
        return raw
    if field.cast == "decimal":
        if not raw:
            return Decimal("0")
        return Decimal(raw.replace(",", "").replace("$", ""))
    if field.cast == "int":
        if not raw:
            return 0
        return int(raw.replace(",", "").replace("$", ""))
    if field.cast == "date":
        fmt = field.date_format or "%Y-%m-%d"
        return datetime.strptime(raw, fmt).date()
    raise ValueError(f"Unknown cast type: {field.cast!r}")


def is_yearless_date_format(date_format: str | None) -> bool:
    """True when a strptime format carries no year token (e.g. ``"%m/%d"``).

    Credit-card statements print transaction dates as MM/DD; the year lives only
    on a separate billing-period line. A year-less date can't be cast on its own —
    it needs the statement period to bracket the year (``_resolve_yearless_date``).
    """
    # .lower() collapses %Y onto %y, so one substring check catches both the
    # 4-digit (%Y) and 2-digit (%y) year directives. A case-sensitive check would
    # silently stop recognising %Y and misclassify a year-bearing format as
    # year-less.
    return date_format is not None and "%y" not in date_format.lower()


def group_anchors(
    anchors: list[FieldExtraction] | None,
) -> dict[str, list[str]] | None:
    """Regroup a recipe's flat metadata_anchors into capture_metadata's dict shape.

    Recipes store one FieldExtraction per (field, pattern) alternative; capture
    wants ``{field: [pattern, ...]}``. Tri-state, preserved by both callers:
    ``None`` → capture_metadata falls back to DEFAULT_ANCHORS; an empty list →
    ``{}`` → the caller deliberately declines metadata capture; a populated list →
    the grouped dict. Shared by the executor (``_statement_period``) and the replay
    pipeline (``routing._run_recipe_pipeline``) so the transformation lives once.
    """
    if anchors is None:
        return None
    grouped: dict[str, list[str]] = {}
    for a in anchors:
        grouped.setdefault(a.name, []).append(a.pattern)
    return grouped


def _statement_period(recipe: Recipe, document_text: str) -> tuple[date, date] | None:
    """The (opening, closing) billing dates, captured only when a field needs them.

    None when no field is year-less (the period is irrelevant) or the document
    lacks both period dates — year-less rows then fail to cast and are skipped,
    and derive_recipe refuses to author such a recipe in the first place.
    """
    if not any(
        f.cast == "date" and is_yearless_date_format(f.date_format)
        for f in recipe.fields
    ):
        return None
    md = capture_metadata(document_text, group_anchors(recipe.metadata_anchors))
    if md.period_start is None or md.period_end is None:
        return None
    return (md.period_start, md.period_end)


# Posting drift can put a row a little outside the printed cycle; roughly one
# billing cycle of slack absorbs that. A year-less MM/DD landing further out has
# no correct year (an OCR garble, a misparsed line) — the resolver refuses rather
# than guess one, since reconciliation sums amounts and can't catch a wrong date.
_MAX_YEARLESS_DRIFT_DAYS = 45


def _resolve_yearless_date(
    raw: str, date_format: str, period: tuple[date, date] | None
) -> date:
    """Resolve a year-less ``MM/DD`` date to a full date via the billing period.

    The cycle can cross a year boundary (``12/23/24 - 01/22/25``), so the year is
    per-row: pick whichever of the period's two years lands the date inside the
    cycle. A date a day or two outside the printed window (posted dates drift)
    falls back to the closest year, ties going to the closing year — but only
    within ``_MAX_YEARLESS_DRIFT_DAYS`` of the cycle; a date further out is an
    anomaly with no correct year and is rejected rather than silently guessed.
    """
    if period is None:
        raise ValueError("year-less date requires a statement period")
    start, end = period
    parsed = datetime.strptime(raw, date_format)  # year defaults to 1900
    # Bracket the year with the period's own two years AND the years immediately
    # adjacent to them. The period years place a row inside the cycle; the
    # adjacent years cover a posted date that drifts just past a year boundary —
    # e.g. 12/31 on a 01/01-01/31 cycle belongs to the PRIOR year. Without them a
    # within-one-calendar-year period offers a single candidate year, so such a
    # row can only resolve inside the period's year and is stored ~a year late.
    candidate_years = sorted({start.year - 1, start.year, end.year, end.year + 1})
    candidates: list[date] = []
    for year in candidate_years:
        try:
            candidates.append(date(year, parsed.month, parsed.day))
        except ValueError:
            continue  # e.g. 02/29 in a non-leap candidate year
    if not candidates:
        raise ValueError(f"cannot place year-less date {raw!r} in period")
    within = [c for c in candidates if start <= c <= end]
    if within:
        return within[0]
    # Outside the printed window: closest to the cycle, ties → later year.
    closest = min(
        candidates,
        key=lambda c: (min(abs((c - start).days), abs((c - end).days)), -c.year),
    )
    drift = min(abs((closest - start).days), abs((closest - end).days))
    if drift > _MAX_YEARLESS_DRIFT_DAYS:
        raise ValueError(
            f"year-less date {raw!r} resolves {drift} days outside the billing "
            f"period — beyond posting drift; refusing to guess a year"
        )
    return closest


def _cast_field(
    field: FieldExtraction, raw: str, period: tuple[date, date] | None
) -> Any:
    """Cast one field, resolving a year-less date against the statement period."""
    # The explicit `is not None` is redundant with is_yearless_date_format (which
    # is False for None) but narrows date_format to str for _resolve_yearless_date.
    if (
        field.cast == "date"
        and field.date_format is not None
        and is_yearless_date_format(field.date_format)
    ):
        return _resolve_yearless_date(raw, field.date_format, period)
    return _cast(field, raw)


# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------


def execute_recipe(recipe: Recipe, document_text: str) -> ExtractedRows:
    """Run recipe against document_text and return typed rows.

    Uses `regex` (not stdlib `re`) because stdlib has no `timeout=` parameter.
    Each match operation is bounded to _PATTERN_TIMEOUT_SEC to prevent ReDoS
    when a pattern + document text combination triggers catastrophic
    backtracking. (Req 9b dynamic bound)
    """
    # Only the "us" number format is honoured by _cast today; the schema
    # accepts the others as a forward declaration. Fail loud rather than
    # silently produce wrong values when a recipe declares a different locale.
    if recipe.number_format != "us":
        raise NotImplementedError(
            f"number_format={recipe.number_format!r} not yet supported by executor"
        )
    rows: list[dict[str, Any]] = []
    region = _carve_region(document_text, recipe.row_region)
    # Year-less MM/DD date rows resolve their year from the statement's billing
    # period, extracted from the document once here (None when no field needs it).
    period = _statement_period(recipe, document_text)

    for line in region.splitlines():
        if not line.strip():
            continue
        try:
            cells = _re.split(
                recipe.row_split, line.strip(), timeout=_PATTERN_TIMEOUT_SEC
            )
        except TimeoutError:
            continue

        if len(cells) != len(recipe.fields):
            continue

        row: dict[str, Any] = {}
        failed = False
        for fld, raw in zip(recipe.fields, cells, strict=True):
            try:
                m = _re.fullmatch(
                    fld.pattern, raw.strip(), timeout=_PATTERN_TIMEOUT_SEC
                )
            except TimeoutError:
                failed = True
                break
            if m is None:
                failed = True
                break
            try:
                # group(0) == validated raw.strip(); fullmatch is the gate.
                row[fld.name] = _cast_field(fld, m.group(0), period)
            except (ValueError, OverflowError, ArithmeticError):
                failed = True
                break

        if not failed:
            rows.append(row)

    # metadata populated by Task 5 (metadata.py) — placeholder until then
    return ExtractedRows(rows=rows, metadata={})

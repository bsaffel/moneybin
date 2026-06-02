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
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

import regex as _re
from pydantic import BaseModel, Field, model_validator

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

    metadata_anchors: list[FieldExtraction] = Field(default_factory=list)
    row_region: RegionAnchors
    row_split: str
    fields: list[FieldExtraction]
    sign_convention: Literal[
        "negative_is_expense", "negative_is_income", "split_debit_credit"
    ]
    number_format: Literal["us", "european", "swiss_french", "zero_decimal"] = "us"
    routing: Literal["transactions", "seed"]

    @model_validator(mode="after")
    def _bound_patterns(self) -> Recipe:
        """Enforce static security bounds on every regex the executor will run."""
        for f in [*self.metadata_anchors, *self.fields]:
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
                row[fld.name] = _cast(fld, m.group(0))
            except (ValueError, OverflowError, ArithmeticError):
                failed = True
                break

        if not failed:
            rows.append(row)

    # metadata populated by Task 5 (metadata.py) — placeholder until then
    return ExtractedRows(rows=rows, metadata={})

"""Shared primitives for the categorization package.

Pure helpers, enum/literal types, boundary validators, regex utilities, the
Pydantic input models, the ``Merchant`` row shape, and the lightweight
DB-touching resolvers used across the matcher, applier, and assist
collaborators. Lives at the package leaves so any collaborator (and
``auto_rule_service``) can import from here without a circular dependency.
"""

from __future__ import annotations

import difflib
import hashlib
import json
import logging
import re
from decimal import Decimal
from functools import lru_cache
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.limits import (
    CATEGORY_NAME_MAX_LEN,
    IDENTIFIER_MAX_LEN,
    MERCHANT_NAME_MAX_LEN,
    MERCHANT_PATTERN_MAX_LEN,
    RULE_PRIORITY_MAX,
    RULE_PRIORITY_MIN,
)
from moneybin.tables import CATEGORIES
from moneybin.vocabulary import (
    CATEGORIZATION_MATCH_TYPES,
    CategorizationMatchType,
)

logger = logging.getLogger(__name__)

# Public match types — accepted by user-authored rules and the merchant API.
# `oneOf` is intentionally excluded: it has no pattern branch in
# `matches_pattern` and would be silently inert if exposed at a public
# boundary. System-managed exemplar merchants use `InternalMatchType` below.
MatchType = CategorizationMatchType

# Internal match types — adds `oneOf` for the exemplar accumulator. Used by
# the in-memory matcher pipeline (`_match_exemplar`, `_fetch_merchants`) and
# the exemplar-merchant creation path in `_categorize_items_inner`.
InternalMatchType = Literal["exact", "contains", "regex", "oneOf"]

_VALID_MATCH_TYPES: frozenset[MatchType] = CATEGORIZATION_MATCH_TYPES

# OP_SCORES — adopted from Actual Budget's rules/rule-utils.ts. Higher score =
# more specific match; specificity wins when multiple matchers fire on the same
# row. See docs/specs/categorization-matching-mechanics.md §Matcher algorithm.
# The SQL CASE expression in _fetch_merchants' ORDER BY is generated from this
# dict via match_shape_case_sql() so the Python dict stays the canonical
# reference and SQL cannot drift from it.
_MATCH_SHAPE_SCORES: dict[str, int] = {
    "oneOf": 10,
    "exact": 10,
    "contains": 0,
    "regex": 0,
}


def score_match_shape(match_type: str) -> int:
    """Return the specificity score for a match type.

    Higher = more specific. Used to order merchants in lookup precedence.
    Unknown types return 0 (lowest specificity) — a forward-compat default.
    """
    return _MATCH_SHAPE_SCORES.get(match_type, 0)


# Categorization source priority — single source of truth. Lower number =
# higher authority. See categorization-matching-mechanics.md §Source
# precedence. The SQL CASE expression in write_categorization is generated
# from this dict via priority_case_sql() so the Python dict stays the
# canonical reference and SQL cannot drift from it.
CategorizedBy = Literal[
    "user", "rule", "auto_rule", "migration", "ml", "provider_native", "ai"
]

SOURCE_PRIORITY: dict[str, int] = {
    "user": 1,
    "rule": 2,
    "auto_rule": 3,
    "migration": 4,
    "ml": 5,
    "provider_native": 6,
    "ai": 7,
}


def priority_case_sql(column_expr: str) -> str:
    """Render a SQL CASE expression mapping categorized_by → numeric priority.

    Used by write_categorization's ON CONFLICT DO UPDATE WHERE clause to
    compare the EXCLUDED row's priority against the existing row's. Reading
    from SOURCE_PRIORITY guarantees the SQL and Python ladders never drift.
    """
    branches = " ".join(
        f"WHEN '{src}' THEN {prio}" for src, prio in SOURCE_PRIORITY.items()
    )
    return f"CASE {column_expr} {branches} END"


def match_shape_case_sql(column_expr: str) -> str:
    """Render a SQL CASE expression mapping match_type → specificity score.

    Used by _fetch_merchants' ORDER BY to put more-specific match types first.
    Reading from _MATCH_SHAPE_SCORES guarantees the SQL and Python ladders
    never drift. ELSE 0 mirrors :func:`score_match_shape`'s forward-compat
    default for unknown types.
    """
    branches = " ".join(
        f"WHEN '{mt}' THEN {score}" for mt, score in _MATCH_SHAPE_SCORES.items()
    )
    return f"CASE {column_expr} {branches} ELSE 0 END"


def plaid_bridge_match_predicate(detailed_expr: str, primary_expr: str) -> str:
    """Render the predicate matching a Plaid transaction's PFC codes to the bridge.

    Keyed on ``core.bridge_category_source_map`` (alias ``b``). Shared by the
    apply path (``apply_plaid_categories``'s JOIN) and the
    coverage-gap stat (``plaid_unmapped``'s NOT EXISTS) so the two stay in
    lockstep — if the bridge keying evolves (a new code level, a normalized
    code column), one edit updates both instead of silently diverging what gets
    categorized from what the stat reports as unmapped. ``detailed_expr`` /
    ``primary_expr`` are SQL column expressions for the detailed and primary PFC
    codes (code constants, never user input).
    """
    return (
        f"b.source_type = 'plaid' "
        f"AND b.source_category_code IN ({detailed_expr}, {primary_expr})"
    )


def is_unselective_contains(pattern: str, match_type: str) -> bool:
    """True when a `contains` pattern is too short to discriminate.

    A 2-char `contains` rule ("TO") matches STORE, AUTO, TOTAL — one such
    rule silently relabels a large slice of the ledger. `exact` is safe at any
    length (it can only fire on a description that IS the token), so the floor
    applies to `contains` only.
    """
    if match_type != "contains":
        return False
    min_len = get_settings().categorization.auto_rule_min_contains_length
    return len(pattern) < min_len


class MatcherKey(NamedTuple):
    """Canonical identity of a rule's matcher — what "the same rule" means.

    Two rules whose keys compare equal fire on exactly the same transactions,
    so they may not disagree about the category. Built only by
    :func:`canonical_matcher_key`; every creation and activation path compares
    rules through it rather than re-deriving its own notion of sameness.

    Amount bounds are rendered at the storage grain (``DECIMAL(18,2)``) as
    strings so ``5``, ``5.0`` and ``Decimal("5.00")`` collapse to one value,
    and ``None`` (no bound) stays distinct from ``0``.
    """

    merchant_pattern: str
    match_type: str
    min_amount: str | None
    max_amount: str | None
    account_id: str | None


# app.categorization_rules stores both bounds as DECIMAL(18,2).
_AMOUNT_GRAIN = Decimal("0.01")


def _canonical_amount(value: Decimal | float | int | None) -> str | None:
    """Render an amount bound at the column's grain, or ``None`` for unbounded."""
    if value is None:
        return None
    return str(Decimal(str(value)).quantize(_AMOUNT_GRAIN))


def canonical_matcher_key(
    *,
    merchant_pattern: str,
    match_type: str,
    min_amount: Decimal | float | int | None = None,
    max_amount: Decimal | float | int | None = None,
    account_id: str | None = None,
) -> MatcherKey:
    r"""Return the canonical matcher identity for one rule.

    ``name`` and ``priority`` are metadata, not identity, so they are absent:
    two rules differing only in those fire on the same rows.

    Every normalization here mirrors ``matches_pattern`` exactly, because the
    invariant is that equal keys fire on equal rows:

    - ``.lower()``, not ``.casefold()``. ``matches_pattern`` lowercases both
      sides; casefold is stricter and maps ``ß`` onto ``ss``, so it would merge
      two patterns the matcher keeps apart.
    - The pattern is **not** stripped. ``matches_pattern`` compares the stored
      pattern as written, so ``contains "CAFE "`` matches strictly fewer
      descriptions than ``contains "CAFE"`` — they are different matchers, not
      two spellings of one. (``create_rules`` still strips at its Pydantic
      boundary; the declarative target contract does not.)
    - ``account_id`` is compared exactly, as ``match_first_rule`` compares it.

    Case folding applies to ``contains`` and ``exact`` only. ``matches_pattern``
    compiles a regex with ``re.IGNORECASE``, so a regex is case-insensitive at
    match time too — but lowercasing the *pattern* rewrites its escapes: ``\D``
    (non-digit) becomes ``\d`` (digit), silently inverting the character class
    and making two opposite rules look identical. Regex patterns are compared
    verbatim.
    """
    normalized_type = match_type.strip().casefold()
    return MatcherKey(
        merchant_pattern=merchant_pattern
        if normalized_type == "regex"
        else merchant_pattern.lower(),
        match_type=normalized_type,
        min_amount=_canonical_amount(min_amount),
        max_amount=_canonical_amount(max_amount),
        account_id=account_id,
    )


def matcher_digest(key: MatcherKey) -> str:
    """Return a stable 64-char SHA-256 over a canonical matcher key.

    The storable form of matcher identity: ``app.rule_conflicts`` keys a
    recorded conflict by this digest, and the review surface joins on it. JSON
    encoding keeps the fields unambiguous — no separator can be forged from
    inside a pattern, and ``None`` (unbounded) is distinct from any string.
    """
    return hashlib.sha256(
        json.dumps(list(key), ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def validate_match_type(match_type: str) -> MatchType:
    """Validate and narrow a match_type string at a service-boundary call site."""
    if match_type not in _VALID_MATCH_TYPES:
        raise ValueError(
            f"Invalid match_type: '{match_type}'. "
            f"Must be one of: {', '.join(sorted(_VALID_MATCH_TYPES))}"
        )
    return match_type  # type: ignore[return-value]  # validated above


def did_you_mean(
    invalid: str, valid_options: list[str], n: int = 3, cutoff: float = 0.4
) -> list[str]:
    """Return up to n closest matches from valid_options for an invalid category string.

    Matches case-insensitively so "FOOD" matches "Food & Dining", then returns
    the original-cased option so callers can feed suggestions back as-is.
    """
    lower_invalid = invalid.lower()
    lower_to_orig = {opt.lower(): opt for opt in valid_options}
    matches = difflib.get_close_matches(
        lower_invalid, list(lower_to_orig), n=n, cutoff=cutoff
    )
    return [lower_to_orig[m] for m in matches]


class CategorizationItem(BaseModel):
    """One row of input for ``CategorizationService.categorize_items``.

    Validated at every boundary (CLI, MCP). The service refuses untyped dicts.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    transaction_id: str = Field(min_length=1, max_length=IDENTIFIER_MAX_LEN)
    category: str = Field(min_length=1, max_length=CATEGORY_NAME_MAX_LEN)
    subcategory: str | None = Field(
        default=None,
        min_length=1,
        max_length=CATEGORY_NAME_MAX_LEN,
    )
    canonical_merchant_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=MERCHANT_NAME_MAX_LEN,
        description=(
            "LLM-proposed canonical merchant name; merges this row's match_text "
            "into an existing merchant's oneOf exemplar set rather than creating "
            "a new merchant per row."
        ),
    )


class CategorizationRuleInput(BaseModel):
    """One rule for ``CategorizationService.create_rules``.

    Validated at the CLI/MCP boundary by ``validate_rule_items``. The
    service refuses untyped dicts.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=MERCHANT_NAME_MAX_LEN)
    merchant_pattern: str = Field(min_length=1, max_length=MERCHANT_PATTERN_MAX_LEN)
    category: str = Field(min_length=1, max_length=CATEGORY_NAME_MAX_LEN)
    subcategory: str | None = Field(
        default=None,
        min_length=1,
        max_length=CATEGORY_NAME_MAX_LEN,
    )
    match_type: MatchType = "contains"
    min_amount: float | None = None
    max_amount: float | None = None
    account_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=IDENTIFIER_MAX_LEN,
    )
    priority: int = Field(default=100, ge=RULE_PRIORITY_MIN, le=RULE_PRIORITY_MAX)


def _validate_items[T: BaseModel](
    raw: object,
    model_cls: type[T],
    *,
    id_field: str,
    list_error_msg: str,
) -> tuple[list[T], list[dict[str, str]]]:
    """Validate raw decoded JSON dicts into typed Pydantic items + per-row errors.

    Shared by ``validate_items`` and ``validate_rule_items``: per-item
    failures contribute an ``error_details`` entry but do not abort the batch.
    The ``id_field`` is the per-row identity surfaced in error dicts so callers
    can correlate failures (e.g., ``transaction_id`` for categorize_items,
    ``name`` for rule creation).
    """
    if not isinstance(raw, list):
        raise ValueError(list_error_msg)

    items: list[T] = []
    errors: list[dict[str, str]] = []
    for index, row in enumerate(raw):  # pyright: ignore[reportUnknownArgumentType]  # raw is intentionally `object`; isinstance check below narrows the type
        if not isinstance(row, dict):
            errors.append({
                id_field: "(missing)",
                "reason": f"Row {index} is not an object",
            })
            continue
        row_dict: dict[str, object] = {
            str(k): v  # pyright: ignore[reportUnknownArgumentType]  # dict keys from untyped JSON input
            for k, v in row.items()  # pyright: ignore[reportUnknownMemberType]  # dict from untyped JSON input
        }
        try:
            items.append(model_cls.model_validate(row_dict))
        except ValidationError as e:
            id_val = row_dict.get(id_field)
            id_str = str(id_val).strip() if isinstance(id_val, str) else ""
            if not id_str:
                id_str = "(missing)"
            reason = "; ".join(
                f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}"  # pyright: ignore[reportUnknownArgumentType]  # Pydantic error loc is Sequence[int | str]
                for err in e.errors()
            )
            errors.append({id_field: id_str, "reason": reason})
    return items, errors


def validate_items(
    raw: object,
) -> tuple[list[CategorizationItem], list[dict[str, str]]]:
    """Validate a raw decoded JSON array into typed items + per-row errors.

    Per-item validation: a malformed row contributes an ``error_details`` entry
    but does not abort the batch. Callers merge ``parse_errors`` into the
    final ``CategorizationResult.error_details`` so the response envelope
    surfaces every failure together.
    """
    return _validate_items(
        raw,
        CategorizationItem,
        id_field="transaction_id",
        list_error_msg="Input must be a JSON array of categorization items",
    )


def validate_rule_items(
    raw: object,
) -> tuple[list[CategorizationRuleInput], list[dict[str, str]]]:
    """Validate raw rule dicts into typed inputs + per-row errors.

    Mirrors ``validate_items``: malformed rows contribute an
    ``error_details`` entry but do not abort the batch.
    """
    return _validate_items(
        raw,
        CategorizationRuleInput,
        id_field="name",
        list_error_msg="Input must be a JSON array of rule items",
    )


@lru_cache(maxsize=512)
def _compile_regex(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern, re.IGNORECASE)


def matches_pattern(text: str, pattern: str, match_type: str) -> bool:
    """Check if text matches a pattern using the specified match type.

    Args:
        text: Text to match against.
        pattern: Pattern to match.
        match_type: One of 'exact', 'contains', 'regex'.

    Returns:
        True if the text matches the pattern.
    """
    text_lower = text.lower()
    pattern_lower = pattern.lower()

    if match_type == "exact":
        return text_lower == pattern_lower
    elif match_type == "contains":
        return pattern_lower in text_lower
    elif match_type == "regex":
        try:
            compiled = _compile_regex(pattern)
        except re.error:
            logger.warning("Invalid regex pattern in merchant rule")
            return False
        # search() cannot raise re.error after successful compilation
        return bool(compiled.search(text))
    else:
        logger.warning(f"Unknown match_type: {match_type}")
        return False


class Merchant(NamedTuple):
    """A merchant in the user's catalog.

    Identity (``merchant_id``, ``canonical_name``), recognition rules
    (``raw_pattern`` + ``match_type`` + ``exemplars``), and the default
    category mapping (``category`` + ``subcategory``) — all attributes of
    one entity. Backed by ``app.user_merchants``; surfaced through
    ``core.dim_merchants``. Nullability mirrors the DDL: ``raw_pattern`` is
    None for exemplar-only merchants (``match_type='oneOf'``); ``category``
    and ``subcategory`` are nullable when a merchant has no default mapping.
    ``exemplars`` is the set of exact ``match_text`` values for oneOf
    set-membership lookup.

    Built by :func:`moneybin.services.categorization.matcher._fetch_merchants`
    from DuckDB rows. Tuple-compatible so legacy positional unpacking keeps
    working.
    """

    merchant_id: str
    raw_pattern: str | None
    match_type: str
    canonical_name: str
    category: str | None
    subcategory: str | None
    exemplars: list[str]

    @classmethod
    def from_row(cls, row: tuple[Any, ...]) -> Merchant:
        """Lift a raw DuckDB result tuple into a typed ``Merchant``.

        Follows the ``BalanceAssertion.from_row`` / ``BalanceObservation.from_row``
        idiom: coerce to declared types at the boundary so downstream code
        stops branching on DuckDB's dynamic row values.
        """
        return cls(
            merchant_id=str(row[0]),
            raw_pattern=str(row[1]) if row[1] is not None else None,
            match_type=str(row[2]),
            canonical_name=str(row[3]),
            category=str(row[4]) if row[4] is not None else None,
            subcategory=str(row[5]) if row[5] is not None else None,
            exemplars=list(row[6] or []),
        )


# Plaid personal_finance_category.confidence_level → numeric. UNKNOWN maps to
# None so it fails the gate; LOW is retained numerically but sits below the gate.
_PLAID_CONFIDENCE: dict[str, float] = {
    "VERY_HIGH": 0.99,
    "HIGH": 0.90,
    "MEDIUM": 0.70,
    "LOW": 0.40,
}
PLAID_MIN_CONFIDENCE: float = 0.70  # assign at MEDIUM and above


def plaid_confidence_to_numeric(level: str | None) -> float | None:
    """Convert Plaid's confidence_level enum to a numeric confidence score.

    Args:
        level: A Plaid confidence_level string (VERY_HIGH, HIGH, MEDIUM, LOW,
            UNKNOWN, or None) or None.

    Returns:
        A float confidence score, or None if level is None, unmapped, or UNKNOWN.
        UNKNOWN and unrecognized values map to None so they fail the gate.
    """
    if not level:
        return None
    return _PLAID_CONFIDENCE.get(level.upper())


def resolve_category_id(
    db: Database, category: str | None, subcategory: str | None
) -> str | None:
    """Resolve a (category, subcategory) text pair to its ``category_id``.

    Returns the matching ``category_id`` from ``core.dim_categories`` (the
    unified view over seeds + ``app.user_categories``) or ``None`` when
    no match exists. ``category=None`` short-circuits to ``None`` so
    writers with optional categories (merchants, splits) can pass through
    without a guard. Phase 1 dual-write callers must accept the ``None``
    case — orphaned text is a real state (legacy rows pre-V014 backfill,
    or text written before its target category was created).

    ``IS NOT DISTINCT FROM`` treats NULL symmetrically on the subcategory
    axis, so passing ``subcategory=None`` matches a dim row with
    ``subcategory IS NULL``.
    """
    if category is None:
        return None
    row = db.execute(
        f"SELECT category_id FROM {CATEGORIES.full_name} "  # noqa: S608  # TableRef constant
        "WHERE category = ? AND subcategory IS NOT DISTINCT FROM ?",
        [category, subcategory],
    ).fetchone()
    return row[0] if row else None

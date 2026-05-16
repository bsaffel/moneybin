"""Shared primitives for the categorization package.

Pure helpers, enum/literal types, boundary validators, regex utilities, the
Pydantic input models, and the ``Merchant`` row shape used across the matcher,
applier, and assist collaborators. Holds nothing that touches the database.
Lives at the package leaves so any collaborator (and ``auto_rule_service``)
can import from here without a circular dependency.
"""

from __future__ import annotations

import difflib
import logging
import re
import typing
from functools import lru_cache
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, ConfigDict, Field, ValidationError

logger = logging.getLogger(__name__)

# Public match types — accepted by user-authored rules and the merchant API.
# `oneOf` is intentionally excluded: it has no pattern branch in
# `matches_pattern` and would be silently inert if exposed at a public
# boundary. System-managed exemplar merchants use `InternalMatchType` below.
MatchType = Literal["exact", "contains", "regex"]

# Internal match types — adds `oneOf` for the exemplar accumulator. Used by
# the in-memory matcher pipeline (`_match_exemplar`, `_fetch_merchants`) and
# the exemplar-merchant creation path in `_categorize_items_inner`.
InternalMatchType = Literal["exact", "contains", "regex", "oneOf"]

_VALID_MATCH_TYPES: frozenset[MatchType] = frozenset(typing.get_args(MatchType))

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
CategorizedBy = Literal["user", "rule", "auto_rule", "migration", "ml", "plaid", "ai"]

SOURCE_PRIORITY: dict[str, int] = {
    "user": 1,
    "rule": 2,
    "auto_rule": 3,
    "migration": 4,
    "ml": 5,
    "plaid": 6,
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

    transaction_id: str = Field(min_length=1, max_length=64)
    category: str = Field(min_length=1, max_length=100)
    subcategory: str | None = Field(default=None, min_length=1, max_length=100)
    canonical_merchant_name: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
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

    name: str = Field(min_length=1, max_length=200)
    merchant_pattern: str = Field(min_length=1, max_length=500)
    category: str = Field(min_length=1, max_length=100)
    subcategory: str | None = Field(default=None, min_length=1, max_length=100)
    match_type: MatchType = "contains"
    min_amount: float | None = None
    max_amount: float | None = None
    account_id: str | None = Field(default=None, min_length=1, max_length=64)
    priority: int = Field(default=100, ge=0, le=10_000)


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

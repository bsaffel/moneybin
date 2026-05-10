"""Transaction categorization service.

Handles merchant normalization, rule-based categorization, merchant matching,
and taxonomy management. Designed for deterministic operations — LLM-based
auto-categorization lives in the MCP layer (auto_categorize tool).

The public API is the ``CategorizationService`` class. The companion
``AutoRuleService`` (``auto_rule_service.py``) handles the auto-rule
proposal/approval/deactivation lifecycle and depends on this module's
``find_matching_rule`` and ``normalize_description``.
"""

import difflib
import logging
import re
import typing
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from time import perf_counter
from typing import Any, Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import (
    CATEGORIZE_APPLY_POST_COMMIT_DURATION_SECONDS,
    CATEGORIZE_APPLY_POST_COMMIT_ROWS_AFFECTED,
    CATEGORIZE_BULK_DURATION_SECONDS,
    CATEGORIZE_BULK_ERRORS_TOTAL,
    CATEGORIZE_BULK_ITEMS_TOTAL,
    CATEGORIZE_MATCH_OUTCOME_TOTAL,
    CATEGORIZE_WRITE_SKIPPED_PRECEDENCE_TOTAL,
    MERCHANT_EXEMPLAR_COUNT,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services._text import (
    build_match_text,
    redact_for_llm,
)
from moneybin.tables import (
    CATEGORIES,
    CATEGORIZATION_RULES,
    CATEGORY_OVERRIDES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    TRANSACTION_CATEGORIES,
    USER_CATEGORIES,
    USER_MERCHANTS,
)

logger = logging.getLogger(__name__)

MatchType = Literal["exact", "contains", "regex", "oneOf"]
_VALID_MATCH_TYPES: frozenset[MatchType] = frozenset(typing.get_args(MatchType))

# OP_SCORES — adopted from Actual Budget's rules/rule-utils.ts. Higher score =
# more specific match; specificity wins when multiple matchers fire on the same
# row. See docs/specs/categorization-matching-mechanics.md §Matcher algorithm.
# The same scores are duplicated as inline CASE constants in _fetch_merchants'
# ORDER BY — one call site, no abstraction value yet in wrapping this as a
# DuckDB UDF (matches Task 4's rationale). Keep the two in sync.
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
# from this dict via _priority_case_sql() so the Python dict stays the
# canonical reference and SQL cannot drift from it.
CategorizedBy = Literal[
    "user", "rule", "auto_rule", "migration", "ml", "plaid", "seed", "ai"
]

_SOURCE_PRIORITY: dict[str, int] = {
    "user": 1,
    "rule": 2,
    "auto_rule": 3,
    "migration": 4,
    "ml": 5,
    "plaid": 6,
    "seed": 7,
    "ai": 8,
}


def _priority_case_sql(column_expr: str) -> str:
    """Render a SQL CASE expression mapping categorized_by → numeric priority.

    Used by write_categorization's ON CONFLICT DO UPDATE WHERE clause to
    compare the EXCLUDED row's priority against the existing row's. Reading
    from _SOURCE_PRIORITY guarantees the SQL and Python ladders never drift.
    """
    branches = " ".join(
        f"WHEN '{src}' THEN {prio}" for src, prio in _SOURCE_PRIORITY.items()
    )
    return f"CASE {column_expr} {branches} END"


def validate_match_type(match_type: str) -> MatchType:
    """Validate and narrow a match_type string at a service-boundary call site."""
    if match_type not in _VALID_MATCH_TYPES:
        raise ValueError(
            f"Invalid match_type: '{match_type}'. "
            f"Must be one of: {', '.join(sorted(_VALID_MATCH_TYPES))}"
        )
    return match_type  # type: ignore[return-value]  # validated above


def _did_you_mean(
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


@dataclass(slots=True, frozen=True)
class WriteOutcome:
    """Result of a guarded write to ``app.transaction_categories``.

    Returned by :meth:`CategorizationService.write_categorization`. The
    ``written`` flag distinguishes successful writes (insert or precedence-
    permitted update) from precedence-blocked attempts. ``skipped_reason`` is
    populated only when ``written`` is False.
    """

    written: bool
    skipped_reason: Literal["lower_priority_source"] | None = None


@dataclass(slots=True)
class CategorizationStats:
    """Typed result for categorization statistics."""

    total: int
    categorized: int
    uncategorized: int
    percent_categorized: float
    by_source: dict[str, int]

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope from this categorization stats result."""
        data: dict[str, Any] = {
            "total_transactions": self.total,
            "categorized": self.categorized,
            "uncategorized": self.uncategorized,
            "percent_categorized": self.percent_categorized,
            "by_source": self.by_source,
        }
        return build_envelope(
            data=data,
            sensitivity="low",
            actions=[
                "Use transactions_categorize_pending_list to see uncategorized transactions"
            ],
        )


@dataclass(frozen=True)
class RedactedTransaction:
    """LLM-safe view of an uncategorized transaction.

    Type-enforces the redaction contract: no full amount, no date, no account ID.
    The v2 contract (per categorization-matching-mechanics.md §Match input) adds
    memo and structural-field signals. Adding any new field requires conscious
    code review — accidental PII leakage is a compile-time impossibility enforced
    by the frozen dataclass shape.
    """

    opaque_id: str
    description_redacted: str
    memo_redacted: str
    source_type: str
    transaction_type: str | None
    check_number: str | None
    is_transfer: bool
    transfer_pair_id: str | None
    payment_channel: str | None
    amount_sign: Literal["+", "-"]


@dataclass(slots=True)
class BulkCategorizationResult:
    """Typed result for bulk categorization operations."""

    applied: int
    skipped: int
    errors: int
    error_details: list[dict[str, Any]]
    merchants_created: int = 0

    def to_envelope(self, input_count: int) -> ResponseEnvelope:
        """Build a ResponseEnvelope from this bulk categorization result."""
        return build_envelope(
            data={
                "applied": self.applied,
                "skipped": self.skipped,
                "errors": self.errors,
                "error_details": self.error_details,
                "merchants_created": self.merchants_created,
            },
            sensitivity="medium",
            total_count=input_count,
            actions=[
                "Use transactions_categorize_rules_list to review auto-created rules",
                "Use transactions_categorize_pending_list to fetch the next batch",
            ],
        )

    def merge_parse_errors(self, parse_errors: list[dict[str, Any]]) -> None:
        """Prepend boundary-validation errors and reflect them in the error count."""
        if not parse_errors:
            return
        self.error_details = parse_errors + self.error_details
        self.errors += len(parse_errors)


@dataclass(slots=True)
class RuleCreationResult:
    """Typed result for CategorizationService.create_rules."""

    created: int
    skipped: int
    error_details: list[dict[str, str]]
    rule_ids: list[str]

    def to_envelope(self, input_count: int) -> ResponseEnvelope:
        """Build a ResponseEnvelope from this rule-creation result."""
        return build_envelope(
            data={
                "created": self.created,
                "skipped": self.skipped,
                "rule_ids": self.rule_ids,
                "error_details": self.error_details,
            },
            sensitivity="low",
            total_count=input_count,
            actions=[
                "Use transactions_categorize_rules_list to review all rules",
            ],
        )

    def merge_parse_errors(self, parse_errors: list[dict[str, str]]) -> None:
        """Prepend boundary-validation errors and reflect them in the skipped count."""
        if not parse_errors:
            return
        self.error_details = parse_errors + self.error_details
        self.skipped += len(parse_errors)


class BulkCategorizationItem(BaseModel):
    """One row of input for ``CategorizationService.bulk_categorize``.

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

    Shared by ``validate_bulk_items`` and ``validate_rule_items``: per-item
    failures contribute an ``error_details`` entry but do not abort the batch.
    The ``id_field`` is the per-row identity surfaced in error dicts so callers
    can correlate failures (e.g., ``transaction_id`` for bulk_categorize,
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


def validate_bulk_items(
    raw: object,
) -> tuple[list[BulkCategorizationItem], list[dict[str, str]]]:
    """Validate a raw decoded JSON array into typed items + per-row errors.

    Per-item validation: a malformed row contributes an ``error_details`` entry
    but does not abort the batch. Callers merge ``parse_errors`` into the
    final ``BulkCategorizationResult.error_details`` so the response envelope
    surfaces every failure together.
    """
    return _validate_items(
        raw,
        BulkCategorizationItem,
        id_field="transaction_id",
        list_error_msg="Input must be a JSON array of categorization items",
    )


def validate_rule_items(
    raw: object,
) -> tuple[list[CategorizationRuleInput], list[dict[str, str]]]:
    """Validate raw rule dicts into typed inputs + per-row errors.

    Mirrors ``validate_bulk_items``: malformed rows contribute an
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


# Merchant row shape used by the in-memory matcher: (merchant_id, raw_pattern,
# match_type, canonical_name, category, subcategory, exemplars). raw_pattern is
# None for exemplar-only merchants (match_type='oneOf'); exemplars is the set
# of exact match_text values for oneOf set-membership lookup.
MerchantRow = tuple[str, str | None, str, str, str, str | None, list[str]]


def _fetch_merchants(
    db: Database,
) -> list[MerchantRow] | None:
    """Fetch all merchant mappings ordered for lookup precedence.

    Ordering:
    1. is_user DESC — user-created merchants outrank seed merchants
    2. match-type OP_SCORE DESC — oneOf/exact (10) outrank contains/regex (0).
       Inlined CASE mirrors :data:`_MATCH_SHAPE_SCORES`; keep the two in sync.
    3. created_at ASC — deterministic tie-break among same-score, same-author
       merchants (NULL for seed rows; sorts last in DuckDB).

    User outranks seed regardless of match-type granularity. A user 'contains'
    rule beats a seed 'exact' rule because user authority over curated defaults
    is the architectural commitment.

    Args:
        db: Database instance (read-only access is sufficient).

    Returns:
        List of merchant rows, or None if the table doesn't exist.
    """
    try:
        return db.execute(
            f"""
            SELECT merchant_id, raw_pattern, match_type,
                   canonical_name, category, subcategory, exemplars
            FROM {MERCHANTS.full_name}
            ORDER BY
                is_user DESC,
                CASE match_type
                    WHEN 'oneOf' THEN 10
                    WHEN 'exact' THEN 10
                    WHEN 'contains' THEN 0
                    WHEN 'regex' THEN 0
                    ELSE 0
                END DESC,
                created_at ASC
            """,  # noqa: S608  # MERCHANTS is a TableRef constant, not user input
        ).fetchall()
    except duckdb.CatalogException:
        return None


def _match_shape_label(description_present: bool, memo_present: bool) -> str:
    """Return the ``shape`` metric label for a matcher call.

    Both ``_match_exemplar`` and ``_match_text`` fire
    ``CATEGORIZE_MATCH_OUTCOME_TOTAL`` with a label describing which input
    signals were present. Extracted so the mapping lives in one place.
    """
    if description_present and memo_present:
        return "both"
    if memo_present:
        return "memo_only"
    return "description_only"


def _match_exemplar(
    match_text: str,
    merchants: list[MerchantRow],
    *,
    description_present: bool = True,
    memo_present: bool = False,
) -> dict[str, str | None] | None:
    """Match match_text against merchants' oneOf exemplar sets (set membership).

    Returns the first merchant whose exemplars contain ``match_text`` exactly.
    Iteration order is the same as ``_fetch_merchants`` (is_user DESC, oneOf
    first), so user merchants win and exact-string membership fires before
    pattern-based shapes. Records ``outcome='exemplar'`` on a hit.
    """
    shape = _match_shape_label(description_present, memo_present)

    if not match_text:
        return None

    for row in merchants:
        (
            merchant_id,
            _raw_pattern,
            match_type,
            canonical_name,
            category,
            subcategory,
            exemplars,
        ) = row
        if match_type != "oneOf":
            continue
        if exemplars and match_text in exemplars:
            CATEGORIZE_MATCH_OUTCOME_TOTAL.labels(outcome="exemplar", shape=shape).inc()
            return {
                "merchant_id": merchant_id,
                "canonical_name": canonical_name,
                "category": category,
                "subcategory": subcategory,
            }
    return None


def _match_text(
    match_text: str,
    merchants: list[MerchantRow],
    *,
    description_present: bool = True,
    memo_present: bool = False,
) -> dict[str, str | None] | None:
    r"""Match the canonical match_text against a pre-fetched merchant list.

    match_text is description + "\n" + memo (per build_match_text); the matcher
    runs against this concatenation directly, without re-normalizing.

    description_present and memo_present control the "shape" label on the
    match-outcome metric so callers can attribute matches by signal source.

    Exemplar-only merchants (match_type='oneOf' with raw_pattern=None) are
    skipped — exemplar lookup is handled by :func:`_match_exemplar`, which
    callers invoke first.
    """
    shape = _match_shape_label(description_present, memo_present)

    if not match_text:
        CATEGORIZE_MATCH_OUTCOME_TOTAL.labels(outcome="none", shape=shape).inc()
        return None

    for row in merchants:
        (
            merchant_id,
            raw_pattern,
            match_type,
            canonical_name,
            category,
            subcategory,
            _exemplars,
        ) = row
        if match_type == "oneOf" or not raw_pattern:
            # Exemplar-only merchants are handled by _match_exemplar.
            continue
        if matches_pattern(match_text, raw_pattern, match_type):
            CATEGORIZE_MATCH_OUTCOME_TOTAL.labels(
                outcome=str(match_type or "contains"), shape=shape
            ).inc()
            return {
                "merchant_id": merchant_id,
                "canonical_name": canonical_name,
                "category": category,
                "subcategory": subcategory,
            }

    CATEGORIZE_MATCH_OUTCOME_TOTAL.labels(outcome="none", shape=shape).inc()
    return None


def _match_merchants(
    match_text: str,
    merchants: list[MerchantRow],
    *,
    description_present: bool = True,
    memo_present: bool = False,
) -> dict[str, str | None] | None:
    """Resolve a merchant for ``match_text`` against the cached merchant list.

    Two-stage lookup per categorization-matching-mechanics.md §Matcher
    algorithm: oneOf exemplar membership first (most-specific match shape),
    then pattern-based (exact / contains / regex) fallback.
    """
    hit = _match_exemplar(
        match_text,
        merchants,
        description_present=description_present,
        memo_present=memo_present,
    )
    if hit is not None:
        return hit
    return _match_text(
        match_text,
        merchants,
        description_present=description_present,
        memo_present=memo_present,
    )


class CategorizationService:
    """Canonical categorization surface — merchants, rules, taxonomy, auto-rules.

    All categorization operations route through this class. The MCP tools, CLI
    commands, and import service share this single entry point so caller-visible
    behavior is consistent across surfaces.
    """

    def __init__(self, db: Database) -> None:
        """Bind the service to a database connection."""
        self._db = db

    # -- Merchant lookup / management --

    def match_merchant(
        self, description: str, memo: str | None = None
    ) -> dict[str, str | None] | None:
        """Look up a merchant by raw description (and optional memo).

        Two-stage lookup: oneOf exemplar membership first (most-specific shape),
        then pattern-based exact/contains/regex (per
        categorization-matching-mechanics.md §Matcher algorithm).

        For OFX-sourced and aggregator-style transactions, memo carries the
        wrapped merchant identity and is essential for accurate matching.
        """
        merchants = _fetch_merchants(self._db)
        if merchants is None:
            return None
        match_text = build_match_text(description, memo)
        return _match_merchants(
            match_text,
            merchants,
            description_present=bool(description and description.strip()),
            memo_present=bool(memo and memo.strip()),
        )

    def create_merchant(
        self,
        raw_pattern: str | None,
        canonical_name: str,
        *,
        match_type: MatchType = "oneOf",
        category: str | None = None,
        subcategory: str | None = None,
        created_by: str = "ai",
        exemplars: list[str] | None = None,
        reapply: bool = False,
    ) -> str:
        """Create a merchant mapping.

        Args:
            raw_pattern: Pattern to match in transaction descriptions; pass
                ``None`` for exemplar-only merchants (``match_type='oneOf'``).
            canonical_name: Clean merchant name for display.
            match_type: How to match: 'exact', 'contains', 'regex', or 'oneOf'.
                Defaults to ``'oneOf'`` — system-created merchants use the
                exemplar accumulator (categorization-matching-mechanics.md
                §Schema changes); user-authored merchants pick 'contains' or
                'regex' explicitly.
            category: Optional default category for this merchant.
            subcategory: Optional default subcategory.
            created_by: Who created the mapping ('user', 'ai', 'rule').
            exemplars: Initial exemplar set (exact match_text values) for
                oneOf merchants. Defaults to ``[]``.
            reapply: When ``True``, runs ``categorize_pending`` after the
                insert so the new merchant fans out to uncategorized rows.
                Default ``False`` — callers inside a bulk flow (e.g.,
                ``bulk_categorize``) skip this and let the enclosing snowball
                pass do the work instead.

        Returns:
            The merchant_id of the created merchant.
        """
        merchant_id = uuid.uuid4().hex[:12]
        # DuckDB binds Python lists to VARCHAR[]. An empty list keeps the
        # column default semantics intact for non-exemplar merchants.
        exemplars_param: list[str] = list(exemplars) if exemplars else []
        self._db.execute(
            f"""
            INSERT INTO {USER_MERCHANTS.full_name}
            (merchant_id, raw_pattern, match_type, canonical_name,
             category, subcategory, created_by, exemplars)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,  # noqa: S608  # USER_MERCHANTS is a TableRef constant, not user input
            [
                merchant_id,
                raw_pattern,
                match_type,
                canonical_name,
                category,
                subcategory,
                created_by,
                exemplars_param,
            ],
        )
        if exemplars_param:
            MERCHANT_EXEMPLAR_COUNT.labels(merchant_id=merchant_id).set(
                len(exemplars_param)
            )
        logger.info(f"Created user merchant {merchant_id}")
        if reapply:
            self.categorize_pending()
        return merchant_id

    def _find_merchant_by_canonical_name(self, canonical_name: str) -> str | None:
        """Return ``merchant_id`` for the user-merchant with this canonical name, or None.

        Used by the exemplar accumulator to detect whether a previously-created
        merchant should grow its exemplar set instead of spawning a duplicate.
        """
        try:
            row = self._db.execute(
                f"SELECT merchant_id FROM {USER_MERCHANTS.full_name} "
                "WHERE canonical_name = ? LIMIT 1",  # noqa: S608  # TableRef constant
                [canonical_name],
            ).fetchone()
        except duckdb.CatalogException:
            return None
        return row[0] if row else None

    def _append_exemplar(self, merchant_id: str, match_text: str) -> int:
        """Append ``match_text`` to a merchant's exemplar set; return new size.

        Idempotent via ``list_distinct``: re-appending an existing exemplar
        leaves the set unchanged. Updates the per-merchant gauge to surface
        any merchant whose set is approaching the soft-cap signal (200).
        """
        # Single round-trip: DuckDB supports RETURNING on UPDATE, so the
        # post-update size flows back without a separate SELECT.
        row = self._db.execute(
            f"""
            UPDATE {USER_MERCHANTS.full_name}
            SET exemplars = list_distinct(list_append(exemplars, ?))
            WHERE merchant_id = ?
            RETURNING len(exemplars)
            """,  # noqa: S608  # USER_MERCHANTS is a TableRef constant
            [match_text, merchant_id],
        ).fetchone()
        new_size = int(row[0]) if row and row[0] is not None else 0
        MERCHANT_EXEMPLAR_COUNT.labels(merchant_id=merchant_id).set(new_size)
        return new_size

    # -- Rule management --

    def create_rules(
        self,
        items: Sequence[CategorizationRuleInput],
        *,
        reapply: bool = False,
    ) -> RuleCreationResult:
        """Create multiple categorization rules in one call.

        Each item is INSERTed into ``app.categorization_rules`` with a fresh
        12-char UUID hex ``rule_id``, ``is_active=true``, and
        ``created_by='ai'``. Per-row insertion failures are caught so a
        single bad row does not abort the batch — they appear in
        ``error_details``.

        When ``reapply=True``, ``categorize_pending`` runs after the writes so
        the new rules fan out to uncategorized rows immediately. Source-priority
        enforcement keeps user manual edits safe regardless.
        """
        created = 0
        skipped = 0
        error_details: list[dict[str, str]] = []
        rule_ids: list[str] = []

        for item in items:
            rule_id = uuid.uuid4().hex[:12]
            try:
                self._db.execute(
                    f"""
                    INSERT INTO {CATEGORIZATION_RULES.full_name}
                    (rule_id, name, merchant_pattern, match_type,
                     min_amount, max_amount, account_id,
                     category, subcategory, priority, is_active,
                     created_by, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true,
                            'ai', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,  # noqa: S608  # TableRef constant, no user input interpolated
                    [
                        rule_id,
                        item.name,
                        item.merchant_pattern,
                        item.match_type,
                        item.min_amount,
                        item.max_amount,
                        item.account_id,
                        item.category,
                        item.subcategory,
                        item.priority,
                    ],
                )
                created += 1
                rule_ids.append(rule_id)
            except Exception:  # noqa: BLE001 — DuckDB raises untyped errors on constraint violations
                skipped += 1
                logger.exception(f"create_rules failed for rule {item.name!r}")
                error_details.append({
                    "name": item.name,
                    "reason": "Failed to create rule — check logs for details.",
                })

        if reapply and created > 0:
            self.categorize_pending()

        return RuleCreationResult(
            created=created,
            skipped=skipped,
            error_details=error_details,
            rule_ids=rule_ids,
        )

    def deactivate_rule(self, rule_id: str, *, reapply: bool = False) -> bool:
        """Soft-delete a rule by setting ``is_active=false``.

        Returns ``True`` if the rule existed (and is now inactive),
        ``False`` if no rule with that ID was found.

        When ``reapply=True`` and the rule was deactivated, runs
        ``categorize_pending`` so any rows previously covered by lower-priority
        sources have a chance to be re-evaluated. Existing higher-priority
        categorizations (user/rule) are unaffected.
        """
        row = self._db.execute(
            f"""
            UPDATE {CATEGORIZATION_RULES.full_name}
            SET is_active = false, updated_at = CURRENT_TIMESTAMP
            WHERE rule_id = ?
            RETURNING rule_id
            """,  # noqa: S608  # TableRef constant, no user input interpolated
            [rule_id],
        ).fetchone()
        deactivated = row is not None
        if reapply and deactivated:
            self.categorize_pending()
        return deactivated

    # -- Category management --

    def create_category(
        self,
        category: str,
        *,
        subcategory: str | None = None,
        description: str | None = None,
    ) -> str:
        """Create a custom user category (active by default).

        Top-level duplicate detection uses an explicit pre-check because
        DuckDB's UNIQUE constraint treats NULL as distinct. The
        check-then-insert shape is safe under MoneyBin's single-process,
        single-writer connection model — see ``database.py`` for the rationale.

        Raises:
            UserError(code="CATEGORY_ALREADY_EXISTS"): the
                ``(category, subcategory)`` pair is already present in
                ``app.user_categories``.
        """
        # DuckDB treats NULL != NULL in UNIQUE constraints, so a top-level
        # category (subcategory IS NULL) can be inserted multiple times without
        # raising ConstraintException. Guard explicitly for that case.
        if subcategory is None:
            existing = self._db.execute(
                f"""
                SELECT 1 FROM {USER_CATEGORIES.full_name}
                WHERE category = ? AND subcategory IS NULL
                LIMIT 1
                """,  # noqa: S608  # TableRef constant, no user input interpolated
                [category],
            ).fetchone()
            if existing:
                raise UserError(
                    f"Category already exists: {category}",
                    code="CATEGORY_ALREADY_EXISTS",
                )

        category_id = uuid.uuid4().hex[:12]
        try:
            self._db.execute(
                f"""
                INSERT INTO {USER_CATEGORIES.full_name}
                (category_id, category, subcategory, description,
                 is_active, created_at)
                VALUES (?, ?, ?, ?, true, CURRENT_TIMESTAMP)
                """,  # noqa: S608  # TableRef constant, no user input interpolated
                [category_id, category, subcategory, description],
            )
        except duckdb.ConstraintException:
            sub = f" / {subcategory}" if subcategory else ""
            raise UserError(
                f"Category already exists: {category}{sub}",
                code="CATEGORY_ALREADY_EXISTS",
            ) from None
        return category_id

    def toggle_category(self, category_id: str, *, is_active: bool) -> None:
        """Enable or disable a category. Existing categorizations are preserved.

        Default categories (is_default=true) write to ``app.category_overrides``;
        user-created categories update ``app.user_categories.is_active`` directly.

        Raises:
            UserError(code="CATEGORY_NOT_FOUND"): no category with this ID
                exists in either ``app.user_categories`` or the seeded defaults.
        """
        cat = self._db.execute(
            f"SELECT is_default FROM {CATEGORIES.full_name} WHERE category_id = ?",  # noqa: S608  # TableRef constant
            [category_id],
        ).fetchone()
        if not cat:
            raise UserError(
                f"Category {category_id} not found",
                code="CATEGORY_NOT_FOUND",
            )

        if cat[0]:  # default category — record/upsert the override
            self._db.execute(
                f"""
                INSERT INTO {CATEGORY_OVERRIDES.full_name}
                    (category_id, is_active, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (category_id) DO UPDATE
                    SET is_active = excluded.is_active,
                        updated_at = excluded.updated_at
                """,  # noqa: S608  # TableRef constant
                [category_id, is_active],
            )
        else:
            self._db.execute(
                f"UPDATE {USER_CATEGORIES.full_name} "  # noqa: S608  # TableRef constant
                f"SET is_active = ? WHERE category_id = ?",
                [is_active, category_id],
            )

    # -- Categorization core --

    def write_categorization(
        self,
        *,
        transaction_id: str,
        category: str,
        subcategory: str | None,
        categorized_by: str,
        merchant_id: str | None = None,
        rule_id: str | None = None,
        confidence: float | None = None,
    ) -> WriteOutcome:
        """Insert or replace a categorization, respecting source precedence.

        Single guarded write path for ``app.transaction_categories``. Lower
        numeric priority = higher authority (per
        ``categorization-matching-mechanics.md`` §Source precedence). A new
        write succeeds only if its source priority is ≤ the existing row's;
        otherwise the existing row stands and the
        ``CATEGORIZE_WRITE_SKIPPED_PRECEDENCE_TOTAL`` metric is incremented.

        Returns:
            ``WriteOutcome.written=True`` if the write took effect (insert or
            permitted update); ``False`` with ``skipped_reason='lower_priority_source'``
            if a higher-priority categorization already exists.
        """
        # The SQL CASE expression is generated from _SOURCE_PRIORITY so the
        # ladder lives in exactly one place. See _priority_case_sql.
        existing_table = TRANSACTION_CATEGORIES.full_name
        excluded_priority = _priority_case_sql("EXCLUDED.categorized_by")
        existing_priority = _priority_case_sql(f"{existing_table}.categorized_by")
        cursor = self._db.execute(
            f"""
            INSERT INTO {existing_table}
                (transaction_id, category, subcategory, categorized_at,
                 categorized_by, merchant_id, rule_id, confidence)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
            ON CONFLICT (transaction_id) DO UPDATE SET
                category = EXCLUDED.category,
                subcategory = EXCLUDED.subcategory,
                categorized_at = EXCLUDED.categorized_at,
                categorized_by = EXCLUDED.categorized_by,
                merchant_id = EXCLUDED.merchant_id,
                rule_id = EXCLUDED.rule_id,
                confidence = EXCLUDED.confidence
            WHERE {excluded_priority} <= {existing_priority}
            RETURNING transaction_id
            """,  # noqa: S608  # TRANSACTION_CATEGORIES is a TableRef constant; CASE built from _SOURCE_PRIORITY
            [
                transaction_id,
                category,
                subcategory,
                categorized_by,
                merchant_id,
                rule_id,
                confidence,
            ],
        )
        if cursor.fetchone() is not None:
            return WriteOutcome(written=True)

        # Row exists with a higher-priority source; record the skip with labels
        # for both sides of the comparison so dashboards can distinguish "ai
        # blocked by user" from "ai blocked by rule" etc.
        existing = self._db.execute(
            f"SELECT categorized_by FROM {TRANSACTION_CATEGORIES.full_name} "
            "WHERE transaction_id = ?",  # noqa: S608  # TRANSACTION_CATEGORIES is a TableRef constant
            [transaction_id],
        ).fetchone()
        CATEGORIZE_WRITE_SKIPPED_PRECEDENCE_TOTAL.labels(
            src_existing=existing[0] if existing else "unknown",
            src_attempted=categorized_by,
        ).inc()
        return WriteOutcome(written=False, skipped_reason="lower_priority_source")

    def bulk_categorize(
        self, items: Sequence[BulkCategorizationItem]
    ) -> BulkCategorizationResult:
        """Assign categories to multiple transactions with merchant auto-creation.

        For each item, looks up the transaction description, resolves or creates
        a merchant mapping, then inserts/replaces the category assignment.
        Merchant resolution is best-effort — failures do not prevent categorization.

        Read-side cost is O(1) in the number of items: one batch description
        fetch and one merchant-table fetch, regardless of input size.

        Auto-applies ``categorize_pending`` after writes commit so newly-created
        merchants and exemplars immediately fan out to remaining uncategorized
        rows (the "snowball" — categorization-matching-mechanics.md §Apply
        order, bug 4). Source-priority enforcement from ``write_categorization``
        keeps user manual edits safe.

        Args:
            items: Validated list of BulkCategorizationItem (transaction_id, category,
                optional subcategory). Validation is the caller's responsibility —
                use ``validate_bulk_items`` at the CLI/MCP boundary before calling this.

        Returns:
            BulkCategorizationResult with applied/skipped/error counts.
        """
        _start = perf_counter()
        try:
            result = self._bulk_categorize_inner(items)
            # Snowball: fan newly-created merchants/exemplars out to remaining
            # uncategorized rows. Skipped on no-op batches so we don't churn a
            # pending sweep when nothing committed.
            if result.applied > 0 or result.merchants_created > 0:
                snowball_start = perf_counter()
                try:
                    counts = self.categorize_pending()
                    CATEGORIZE_APPLY_POST_COMMIT_ROWS_AFFECTED.observe(counts["total"])
                finally:
                    CATEGORIZE_APPLY_POST_COMMIT_DURATION_SECONDS.observe(
                        perf_counter() - snowball_start
                    )
            return result
        except Exception:
            CATEGORIZE_BULK_ERRORS_TOTAL.inc()
            raise
        finally:
            CATEGORIZE_BULK_DURATION_SECONDS.observe(perf_counter() - _start)

    def _bulk_categorize_inner(
        self, items: Sequence[BulkCategorizationItem]
    ) -> BulkCategorizationResult:
        applied = 0
        skipped = 0
        errors = 0
        merchants_created = 0
        error_details: list[dict[str, Any]] = []

        if not items:
            return BulkCategorizationResult(
                applied=applied,
                skipped=skipped,
                errors=errors,
                error_details=error_details,
                merchants_created=merchants_created,
            )

        # Phase 1 — validate categories against the active taxonomy.
        # Fetch once for the whole batch so cost is O(1) in batch size.
        try:
            valid_category_set = {
                row[0]
                for row in self._db.execute(
                    f"SELECT DISTINCT category FROM {CATEGORIES.full_name} WHERE is_active"  # noqa: S608  # CATEGORIES is a TableRef constant
                ).fetchall()
            }
        except duckdb.CatalogException:
            # View not yet materialized (e.g., no seeds loaded); skip validation.
            valid_category_set = None

        if valid_category_set:
            valid_sorted = sorted(valid_category_set)
            validated_items: list[BulkCategorizationItem] = []
            for item in items:
                if item.category not in valid_category_set:
                    errors += 1
                    suggestions = _did_you_mean(item.category, valid_sorted)
                    reason = (
                        f"Invalid category {item.category!r}; "
                        f"did you mean: {', '.join(suggestions)}"
                        if suggestions
                        else f"Invalid category {item.category!r}"
                    )
                    error_details.append({
                        "transaction_id": item.transaction_id,
                        "reason": reason,
                        "error": "invalid_category",
                        "invalid_value": item.category,
                        "valid_categories": valid_sorted,
                        "did_you_mean": suggestions,
                    })
                else:
                    validated_items.append(item)
            items = validated_items

            if not items:
                CATEGORIZE_BULK_ITEMS_TOTAL.labels(outcome="error").inc(errors)
                return BulkCategorizationResult(
                    applied=applied,
                    skipped=skipped,
                    errors=errors,
                    error_details=error_details,
                    merchants_created=merchants_created,
                )

        # Phase 2 — batch-fetch txn rows (description + amount + account_id)
        txn_ids = [item.transaction_id for item in items]
        placeholders = ",".join(["?"] * len(txn_ids))
        # Lazy import keeps the module-level dependency one-way
        # (auto_rule_service → categorization_service).
        from moneybin.services.auto_rule_service import (  # noqa: PLC0415 — deferred to avoid circular import
            AutoRuleService,
            BulkRecordingContext,
            TxnRow,
        )

        txn_rows: dict[str, TxnRow] = {}
        try:
            rows = self._db.execute(
                f"""
                SELECT transaction_id, description, amount, account_id, memo
                FROM {FCT_TRANSACTIONS.full_name}
                WHERE transaction_id IN ({placeholders})
                """,  # noqa: S608 — FCT_TRANSACTIONS is a compile-time TableRef constant; values are parameterized
                txn_ids,
            ).fetchall()
            txn_rows = {
                row[0]: TxnRow(
                    description=row[1],
                    amount=float(row[2]) if row[2] is not None else None,
                    account_id=str(row[3]) if row[3] is not None else None,
                    memo=row[4],
                )
                for row in rows
            }
        except Exception:  # noqa: BLE001 — best-effort; degrades to no merchant resolution
            logger.warning("Could not batch-fetch transaction rows", exc_info=True)

        # Phase 3 — fetch merchants and active rules once for the whole batch.
        # Guard against any non-CatalogException (schema drift, binder errors, etc.)
        # so a merchant-table or rules-table failure doesn't block all category
        # writes for the batch.
        try:
            raw_merchants = _fetch_merchants(self._db)
            cached_merchants: list[tuple[Any, ...]] = (
                list(raw_merchants) if raw_merchants is not None else []
            )
        except Exception:  # noqa: BLE001 — best-effort; degrades to no merchant resolution
            logger.warning("Could not batch-fetch merchants", exc_info=True)
            cached_merchants = []
        try:
            cached_rules = self.fetch_active_rules()
        except Exception:  # noqa: BLE001 — best-effort; degrades to no rule cover checks
            logger.warning("Could not batch-fetch active rules", exc_info=True)
            cached_rules = []

        ctx = BulkRecordingContext(
            txn_rows=txn_rows,
            active_rules=cached_rules,
            merchant_mappings=cached_merchants,
        )
        auto_rule_svc = AutoRuleService(self._db)

        # Phase 4 — per-item categorization (writes only)
        for item in items:
            txn_id = item.transaction_id
            category = item.category
            subcategory = item.subcategory
            try:
                # Resolve pre-existing merchant first so auto-rule learning can
                # use the merchant's raw_pattern (e.g., "AMZN") instead of
                # falling back to the raw description. New merchants are
                # deferred until after record_categorization — creating one
                # first would let _merchant_mapping_covers short-circuit the
                # proposal before it can be tracked.
                merchant_id: str | None = None
                existing: dict[str, Any] | None = None
                description = ctx.description_for(txn_id)
                memo = ctx.memo_for(txn_id)
                match_text = build_match_text(description, memo)
                if match_text and ctx.merchant_mappings:
                    try:
                        existing = _match_merchants(
                            match_text,
                            ctx.merchant_mappings,
                            description_present=bool(
                                description and description.strip()
                            ),
                            memo_present=bool(memo and memo.strip()),
                        )
                        if existing:
                            merchant_id = existing["merchant_id"]
                    except Exception:  # noqa: BLE001 — merchant lookup is best-effort
                        logger.debug(
                            f"Could not resolve merchant for {txn_id}",
                            exc_info=True,
                        )

                try:
                    auto_rule_svc.record_categorization(
                        txn_id,
                        category,
                        subcategory=subcategory,
                        merchant_id=merchant_id,
                        context=ctx,
                    )
                except Exception:  # noqa: BLE001 — auto-rule learning is best-effort
                    logger.warning("auto-rule recording failed", exc_info=True)

                # Exemplar accumulator (categorization-matching-mechanics.md
                # §Schema changes). When no merchant matched this row, either
                # grow the exemplar set of an existing merchant with the same
                # LLM-proposed canonical_merchant_name, or create a new
                # exemplar-only merchant. System-generated merchants never
                # invent a contains pattern from the full description — that
                # over-generalized aggregator strings (bug 3).
                if merchant_id is None and match_text:
                    try:
                        canonical_name = item.canonical_merchant_name or match_text
                        existing_id = self._find_merchant_by_canonical_name(
                            canonical_name
                        )
                        if existing_id is not None:
                            self._append_exemplar(existing_id, match_text)
                            merchant_id = existing_id
                        else:
                            merchant_id = self.create_merchant(
                                None,
                                canonical_name,
                                match_type="oneOf",
                                category=category,
                                subcategory=subcategory,
                                created_by="ai",
                                exemplars=[match_text],
                            )
                            merchants_created += 1
                            # Register into context so subsequent items in this
                            # batch see the new exemplar-only merchant at the
                            # head of the merchant list (oneOf is first per
                            # _fetch_merchants ordering).
                            new_row: MerchantRow = (
                                merchant_id,
                                None,
                                "oneOf",
                                canonical_name,
                                category,
                                subcategory,
                                [match_text],
                            )
                            ctx.register_new_merchant(new_row)
                    except Exception:  # noqa: BLE001 — exemplar accumulation is best-effort; categorization proceeds without it
                        logger.debug(
                            f"Could not accumulate exemplar for {txn_id}",
                            exc_info=True,
                        )

                outcome = self.write_categorization(
                    transaction_id=txn_id,
                    category=category,
                    subcategory=subcategory,
                    categorized_by="ai",
                    merchant_id=merchant_id,
                )
                if outcome.written:
                    applied += 1
                else:
                    # Higher-priority source already categorized this row;
                    # leave it alone and surface as a skip.
                    skipped += 1
                    error_details.append({
                        "transaction_id": txn_id,
                        "reason": (
                            "Skipped: a higher-priority categorization "
                            "(user, rule, or other) already covers this transaction."
                        ),
                        "error": "lower_priority_source",
                    })
            except Exception:  # noqa: BLE001 — DuckDB raises untyped errors on constraint violations
                errors += 1
                logger.exception(f"bulk_categorize failed for transaction {txn_id!r}")
                error_details.append({
                    "transaction_id": txn_id,
                    "reason": "Failed to apply category — check logs for details.",
                })

        # Best-effort override check: deactivates auto-rules whose categories
        # have been corrected past the configured threshold. Runs once per
        # batch so cost is independent of batch size.
        if applied:
            try:
                auto_rule_svc.check_overrides()
            except Exception:  # noqa: BLE001 — override check is best-effort
                logger.debug("auto-rule override check failed", exc_info=True)

        CATEGORIZE_BULK_ITEMS_TOTAL.labels(outcome="applied").inc(applied)
        CATEGORIZE_BULK_ITEMS_TOTAL.labels(outcome="skipped").inc(skipped)
        CATEGORIZE_BULK_ITEMS_TOTAL.labels(outcome="error").inc(errors)
        return BulkCategorizationResult(
            applied=applied,
            skipped=skipped,
            errors=errors,
            error_details=error_details,
            merchants_created=merchants_created,
        )

    def apply_merchant_categories(self) -> int:
        """Apply merchant-based categories to uncategorized transactions.

        Fetches all merchants once, then matches each uncategorized transaction
        in Python — avoids a per-transaction DB query.

        Returns:
            Number of transactions categorized.
        """
        merchants = _fetch_merchants(self._db)
        if not merchants:
            return 0

        try:
            uncategorized = self._db.execute(
                f"""
                SELECT t.transaction_id, t.description, t.memo
                FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                    ON t.transaction_id = c.transaction_id
                WHERE c.transaction_id IS NULL
                    AND (
                        (t.description IS NOT NULL AND t.description != '')
                        OR (t.memo IS NOT NULL AND t.memo != '')
                    )
                """,
            ).fetchall()
        except duckdb.CatalogException:
            return 0

        if not uncategorized:
            return 0

        categorized_count = 0
        for txn_id, description, memo in uncategorized:
            match_text = build_match_text(description, memo)
            if not match_text:
                continue
            merchant = _match_merchants(
                match_text,
                merchants,
                description_present=bool(description and str(description).strip()),
                memo_present=bool(memo and str(memo).strip()),
            )
            if merchant and merchant.get("category"):
                # Merchants don't have a dedicated source-priority slot in the v1
                # ladder (user/rule/auto_rule/migration/ml/plaid/seed/ai). Recording
                # merchant matches as 'rule' preserves historical behavior; a
                # follow-up spec may introduce a dedicated 'merchant' priority
                # between auto_rule and migration.
                outcome = self.write_categorization(
                    transaction_id=txn_id,
                    category=str(merchant["category"]),
                    subcategory=merchant["subcategory"],
                    categorized_by="rule",
                    merchant_id=merchant["merchant_id"],
                    confidence=1.0,
                )
                if outcome.written:
                    categorized_count += 1

        if categorized_count:
            logger.info(
                f"Merchant matching categorized {categorized_count} transactions"
            )
        return categorized_count

    def fetch_active_rules(self) -> list[tuple[Any, ...]]:
        """Return all active rules in priority order (priority ASC, created_at ASC)."""
        try:
            return self._db.execute(
                f"""
                SELECT rule_id, merchant_pattern, match_type,
                       min_amount, max_amount, account_id,
                       category, subcategory, created_by
                FROM {CATEGORIZATION_RULES.full_name}
                WHERE is_active = true
                ORDER BY priority ASC, created_at ASC
                """
            ).fetchall()
        except duckdb.CatalogException:
            return []

    @staticmethod
    def match_first_rule(
        rules: list[tuple[Any, ...]],
        description: str,
        amount: float | None,
        account_id: str | None,
        memo: str | None = None,
    ) -> tuple[str, str, str | None, str] | None:
        """Return ``(rule_id, category, subcategory, created_by)`` for the first rule that matches.

        Evaluates the pattern against the canonical ``match_text`` only —
        ``build_match_text(description, memo)``, each side normalized via
        ``normalize_description`` per the matching spec
        (``categorization-matching-mechanics.md`` §Match input). Amount bounds
        and account filter are applied as before. Returns ``None`` when no
        rule matches.
        """
        match_text = build_match_text(description, memo)
        for rule in rules:
            (
                rule_id,
                pattern,
                match_type,
                min_amount,
                max_amount,
                rule_account_id,
                category,
                subcategory,
                created_by,
            ) = rule
            if not matches_pattern(match_text, pattern, match_type):
                continue
            if (
                min_amount is not None
                and amount is not None
                and amount < float(min_amount)
            ):
                continue
            if (
                max_amount is not None
                and amount is not None
                and amount > float(max_amount)
            ):
                continue
            if rule_account_id is not None and account_id != rule_account_id:
                continue
            return rule_id, category, subcategory, created_by
        return None

    def find_matching_rule(
        self,
        transaction_id: str,
        *,
        rules_override: list[tuple[Any, ...]] | None = None,
        txn_row_override: tuple[str, float | None, str | None, str | None]
        | None = None,
    ) -> tuple[str, str, str | None, str] | None:
        """Return the first active rule matching this transaction, or ``None``.

        Result tuple is ``(rule_id, category, subcategory, created_by)``.
        Single-transaction variant of :meth:`apply_rules`; lets callers (e.g.,
        the auto-rule proposal pipeline) ask "is this transaction already
        covered by an existing rule?" using the canonical match semantics
        instead of re-implementing them.

        The bulk path supplies pre-loaded rule rows and txn metadata via
        ``rules_override`` and ``txn_row_override`` so this function issues no
        queries during a bulk loop. Both default to ``None`` for non-bulk callers.
        ``txn_row_override`` is ``(description, amount, account_id, memo)``.
        """
        description: str
        amount: float | None
        account_id: str | None
        memo: str | None
        if txn_row_override is not None:
            description, amount, account_id, memo = txn_row_override
        else:
            try:
                txn_row = self._db.execute(
                    f"SELECT description, amount, account_id, memo "
                    f"FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
                    [transaction_id],
                ).fetchone()
            except duckdb.CatalogException:
                return None
            if not txn_row:
                return None
            # DuckDB row values are dynamically typed; normalize to the shapes
            # match_first_rule expects.
            raw_desc, raw_amt, raw_acct, raw_memo = txn_row
            description = str(raw_desc) if raw_desc else ""
            amount = float(raw_amt) if raw_amt is not None else None
            account_id = str(raw_acct) if raw_acct is not None else None
            memo = str(raw_memo) if raw_memo else None
        if not description and not memo:
            return None
        rules = (
            rules_override if rules_override is not None else self.fetch_active_rules()
        )
        if not rules:
            return None
        return self.match_first_rule(rules, description, amount, account_id, memo)

    def apply_rules(self) -> int:
        """Apply active categorization rules to uncategorized transactions.

        Runs before merchant mapping in :meth:`categorize_pending` so that
        explicit rules take priority. Rules are evaluated in priority order
        (lower number = higher priority); the first matching rule wins. Rules
        can filter by merchant pattern, amount range, and account ID.

        Provenance: when the matched rule was created by the auto-rule
        pipeline (``created_by='auto_rule'``), the resulting categorization
        is written with ``categorized_by='auto_rule'`` so downstream stats
        can identify auto-rule-driven assignments without joining through
        ``rule_id``. All other rules write ``categorized_by='rule'``.

        Returns:
            Number of transactions categorized.
        """
        rules = self.fetch_active_rules()
        if not rules:
            return 0

        try:
            uncategorized = self._db.execute(
                f"""
                SELECT t.transaction_id, t.description, t.amount, t.account_id, t.memo
                FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                    ON t.transaction_id = c.transaction_id
                WHERE c.transaction_id IS NULL
                    AND (
                        (t.description IS NOT NULL AND t.description != '')
                        OR (t.memo IS NOT NULL AND t.memo != '')
                    )
                """,
            ).fetchall()
        except duckdb.CatalogException:
            return 0

        if not uncategorized:
            return 0

        categorized_count = 0
        for txn_id, description, amount, account_id, memo in uncategorized:
            match = self.match_first_rule(
                rules,
                str(description) if description else "",
                float(amount) if amount is not None else None,
                str(account_id) if account_id is not None else None,
                str(memo) if memo else None,
            )
            if match is None:
                continue
            rule_id, category, subcategory, created_by = match
            categorized_by = "auto_rule" if created_by == "auto_rule" else "rule"
            outcome = self.write_categorization(
                transaction_id=txn_id,
                category=category,
                subcategory=subcategory,
                categorized_by=categorized_by,
                rule_id=rule_id,
                confidence=1.0,
            )
            if outcome.written:
                categorized_count += 1

        if categorized_count:
            logger.info(f"Rule engine categorized {categorized_count} transactions")
        return categorized_count

    def categorize_pending(self) -> dict[str, int]:
        """Categorize all pending (uncategorized) transactions.

        Runs current rules and merchants against pending transactions.
        Rules run first in priority order so explicit user-defined rules (which can
        filter by amount, account, and pattern) take precedence over generic merchant
        mappings. Merchant mappings apply only to transactions not matched by any rule.

        Idempotent: a second run on the same state writes nothing.

        Returns:
            Dict with counts: {'merchant': N, 'rule': N, 'total': N}.
        """
        rule_count = self.apply_rules()
        merchant_count = self.apply_merchant_categories()
        total = merchant_count + rule_count

        if total:
            logger.info(
                f"Categorized {total} pending transactions "
                f"({merchant_count} merchant, {rule_count} rule)"
            )

        return {
            "merchant": merchant_count,
            "rule": rule_count,
            "total": total,
        }

    def get_active_categories(self) -> list[dict[str, str | bool | None]]:
        """Get all active categories."""
        try:
            rows = self._db.execute(
                f"""
                SELECT category_id, category, subcategory, description,
                       is_default, plaid_detailed
                FROM {CATEGORIES.full_name}
                WHERE is_active = true
                ORDER BY category, subcategory
                """
            ).fetchall()
        except duckdb.CatalogException:
            return []

        return [
            {
                "category_id": r[0],
                "category": r[1],
                "subcategory": r[2],
                "description": r[3],
                "is_default": r[4],
                "plaid_detailed": r[5],
            }
            for r in rows
        ]

    def get_all_categories(
        self, *, include_inactive: bool
    ) -> list[dict[str, str | bool | None]]:
        """Get categories with consistent field shape including is_active.

        Active-only views can use ``get_active_categories()`` to omit
        ``is_active`` from each row; this method always includes it so the
        MCP tool surface is consumer-friendly when toggling the include flag.
        """
        where = "" if include_inactive else "WHERE is_active = true"
        try:
            rows = self._db.execute(
                f"""
                SELECT category_id, category, subcategory, description,
                       is_default, is_active, plaid_detailed
                FROM {CATEGORIES.full_name}
                {where}
                ORDER BY category, subcategory
                """  # noqa: S608  # constant clause, not user input
            ).fetchall()
        except duckdb.CatalogException:
            return []

        return [
            {
                "category_id": r[0],
                "category": r[1],
                "subcategory": r[2],
                "description": r[3],
                "is_default": r[4],
                "is_active": r[5],
                "plaid_detailed": r[6],
            }
            for r in rows
        ]

    def list_rules(self) -> list[dict[str, Any]]:
        """List all categorization rules (active and inactive) ordered by priority."""
        try:
            rows = self._db.execute(
                f"""
                SELECT rule_id, name, merchant_pattern, match_type,
                       min_amount, max_amount, account_id,
                       category, subcategory, priority, is_active
                FROM {CATEGORIZATION_RULES.full_name}
                ORDER BY priority ASC, created_at ASC
                """
            ).fetchall()
        except duckdb.CatalogException:
            return []

        return [
            {
                "rule_id": r[0],
                "name": r[1],
                "merchant_pattern": r[2],
                "match_type": r[3],
                "min_amount": r[4],
                "max_amount": r[5],
                "account_id": r[6],
                "category": r[7],
                "subcategory": r[8],
                "priority": r[9],
                "is_active": r[10],
            }
            for r in rows
        ]

    def list_merchants(self) -> list[dict[str, str | None]]:
        """List all merchant name mappings ordered by canonical name."""
        try:
            rows = self._db.execute(
                f"""
                SELECT merchant_id, raw_pattern, match_type,
                       canonical_name, category, subcategory
                FROM {MERCHANTS.full_name}
                ORDER BY canonical_name
                """
            ).fetchall()
        except duckdb.CatalogException:
            return []

        return [
            {
                "merchant_id": r[0],
                "raw_pattern": r[1],
                "match_type": r[2],
                "canonical_name": r[3],
                "category": r[4],
                "subcategory": r[5],
            }
            for r in rows
        ]

    def list_uncategorized_transactions(
        self, *, limit: int
    ) -> list[dict[str, Any]] | None:
        """List uncategorized transactions ordered by date descending.

        Returns ``None`` (rather than ``[]``) when the underlying tables don't
        exist yet — callers can distinguish "no transactions" from "no schema"
        and surface a more useful action hint.
        """
        try:
            result = self._db.execute(
                f"""
                SELECT t.transaction_id, t.transaction_date, t.amount,
                       t.description, t.memo, t.account_id
                FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                    ON t.transaction_id = c.transaction_id
                WHERE c.transaction_id IS NULL
                ORDER BY t.transaction_date DESC
                LIMIT ?
                """,
                [limit],
            )
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
        except duckdb.CatalogException:
            return None

        return [dict(zip(columns, row, strict=False)) for row in rows]

    def count_uncategorized(self) -> int:
        """Return the number of transactions without a category assignment."""
        try:
            row = self._db.execute(
                f"""
                SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                    ON t.transaction_id = c.transaction_id
                WHERE c.transaction_id IS NULL
                """  # noqa: S608  # TableRef constants, no user input interpolated
            ).fetchone()
            return int(row[0]) if row else 0
        except Exception:  # noqa: BLE001 — tables may not exist before first import
            return 0

    # -- Stats --

    def categorization_stats(self) -> dict[str, int | float]:
        """Get summary statistics about categorization coverage.

        Returns:
            Dict with total, categorized, uncategorized counts and
            breakdown by categorized_by source.
        """
        try:
            total_result = self._db.execute(
                f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name}"
            ).fetchone()
            total = total_result[0] if total_result else 0
        except duckdb.CatalogException:
            return {
                "total": 0,
                "categorized": 0,
                "uncategorized": 0,
                "pct_categorized": 0,
            }

        try:
            categorized_result = self._db.execute(
                f"SELECT COUNT(*) FROM {TRANSACTION_CATEGORIES.full_name}"
            ).fetchone()
            categorized = categorized_result[0] if categorized_result else 0
        except duckdb.CatalogException:
            categorized = 0

        uncategorized = total - categorized
        pct = round((categorized / total * 100), 1) if total > 0 else 0.0

        stats: dict[str, int | float] = {
            "total": total,
            "categorized": categorized,
            "uncategorized": uncategorized,
            "pct_categorized": pct,
        }

        # Breakdown by source
        try:
            source_rows = self._db.execute(
                f"""
                SELECT categorized_by, COUNT(*) AS cnt
                FROM {TRANSACTION_CATEGORIES.full_name}
                GROUP BY categorized_by
                ORDER BY cnt DESC
                """
            ).fetchall()
            for source, count in source_rows:
                stats[f"by_{source}"] = count
        except duckdb.CatalogException:
            pass

        return stats

    def stats(self) -> CategorizationStats:
        """Get categorization stats as a typed result.

        Wrapper around :meth:`categorization_stats` that returns a typed object.
        """
        raw = self.categorization_stats()
        by_source = {
            k.removeprefix("by_"): v
            for k, v in raw.items()
            if k.startswith("by_") and isinstance(v, int)
        }
        return CategorizationStats(
            total=int(raw["total"]),
            categorized=int(raw["categorized"]),
            uncategorized=int(raw["uncategorized"]),
            percent_categorized=float(raw["pct_categorized"]),
            by_source=by_source,
        )

    def categorize_assist(
        self,
        limit: int = 100,
        account_filter: list[str] | None = None,
        date_range: tuple[str, str] | None = None,
    ) -> list[RedactedTransaction]:
        """Return uncategorized transactions as redacted records for LLM review.

        Sensitivity: medium. Output is sent to the user's LLM via MCP or
        written to disk via the CLI bridge. The redaction contract is enforced
        by RedactedTransaction's frozen dataclass shape (v2: description + memo
        redacted; structural fields exposed unredacted).
        """
        import time

        from moneybin.metrics.registry import (
            CATEGORIZE_ASSIST_DURATION_SECONDS,
            CATEGORIZE_ASSIST_TXNS_RETURNED_TOTAL,
        )

        settings = get_settings().categorization
        effective_limit = min(limit, settings.assist_max_batch_size)

        where_clauses = ["tc.transaction_id IS NULL"]
        params: list[object] = []
        if account_filter:
            where_clauses.append(
                f"t.account_id IN ({','.join('?' * len(account_filter))})"
            )
            params.extend(account_filter)
        if date_range:
            where_clauses.append("t.transaction_date BETWEEN ? AND ?")
            params.extend(date_range)
        where_sql = " AND ".join(where_clauses)

        start = time.monotonic()
        result: list[RedactedTransaction] = []
        try:
            rows = self._db.execute(
                f"""
                SELECT t.transaction_id,
                       t.description,
                       t.memo,
                       t.source_type,
                       t.transaction_type,
                       t.check_number,
                       t.is_transfer,
                       t.transfer_pair_id,
                       t.payment_channel,
                       t.amount
                FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} tc USING (transaction_id)
                WHERE {where_sql}
                LIMIT ?
                """,  # noqa: S608  # where_sql composed from constants and parameter placeholders
                params + [effective_limit],
            ).fetchall()

            result = [
                RedactedTransaction(
                    opaque_id=row[0],
                    description_redacted=redact_for_llm(row[1] or ""),
                    memo_redacted=redact_for_llm(row[2] or ""),
                    source_type=row[3] or "",
                    transaction_type=row[4],
                    check_number=row[5],
                    is_transfer=bool(row[6]),
                    transfer_pair_id=row[7],
                    payment_channel=row[8],
                    amount_sign="-" if (row[9] is not None and row[9] < 0) else "+",
                )
                for row in rows
            ]
            return result
        finally:
            CATEGORIZE_ASSIST_DURATION_SECONDS.observe(time.monotonic() - start)
            CATEGORIZE_ASSIST_TXNS_RETURNED_TOTAL.inc(len(result))

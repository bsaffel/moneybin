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
    CATEGORIZE_BULK_DURATION_SECONDS,
    CATEGORIZE_BULK_ERRORS_TOTAL,
    CATEGORIZE_BULK_ITEMS_TOTAL,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services._text import normalize_description, redact_for_llm
from moneybin.services.audit_service import AuditService
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

MatchType = Literal["exact", "contains", "regex"]
_VALID_MATCH_TYPES: frozenset[MatchType] = frozenset(typing.get_args(MatchType))


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

    Type-enforces the redaction contract: no amount, date, or account fields.
    Adding any new field requires conscious code review — accidental PII leakage
    is a compile-time impossibility enforced by the frozen dataclass shape.
    """

    opaque_id: str  # transaction_id (opaque hash, never decoded by LLM)
    description_redacted: str  # output of redact_for_llm()
    source_type: str  # 'csv' | 'ofx' | 'plaid' | 'pdf' — helps LLM judge quality


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


def _fetch_merchants(
    db: Database,
) -> list[tuple[str, str, str, str, str, str | None]] | None:
    """Fetch all merchant mappings ordered for lookup precedence.

    Ordering:
    1. is_user DESC — user-created merchants outrank seed merchants
    2. match_type — exact > contains > regex (existing precedence)

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
                   canonical_name, category, subcategory
            FROM {MERCHANTS.full_name}
            ORDER BY
                is_user DESC,
                CASE match_type
                    WHEN 'exact' THEN 1
                    WHEN 'contains' THEN 2
                    WHEN 'regex' THEN 3
                END
            """,  # noqa: S608  # MERCHANTS is a TableRef constant, not user input
        ).fetchall()
    except duckdb.CatalogException:
        return None


def _match_description(
    description: str,
    merchants: list[tuple[str, str, str, str, str, str | None]],
) -> dict[str, str | None] | None:
    """Match a description against a pre-fetched merchant list.

    Args:
        description: Transaction description to match.
        merchants: Pre-fetched merchant rows from ``_fetch_merchants()``.

    Returns:
        Dict with merchant_id, canonical_name, category, subcategory
        if found, otherwise None.
    """
    normalized = normalize_description(description)
    if not normalized:
        return None

    for row in merchants:
        merchant_id, raw_pattern, match_type, canonical_name, category, subcategory = (
            row
        )
        if matches_pattern(description, raw_pattern, match_type) or matches_pattern(
            normalized, raw_pattern, match_type
        ):
            return {
                "merchant_id": merchant_id,
                "canonical_name": canonical_name,
                "category": category,
                "subcategory": subcategory,
            }

    return None


class CategorizationService:
    """Canonical categorization surface — merchants, rules, taxonomy, auto-rules.

    All categorization operations route through this class. The MCP tools, CLI
    commands, and import service share this single entry point so caller-visible
    behavior is consistent across surfaces.
    """

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Bind the service to a database connection.

        ``audit`` is keyword-only so existing positional callers continue
        unchanged. Used by ``set_category`` / ``clear_category`` to emit
        ``category.set`` / ``category.clear`` audit events alongside the
        ``app.transaction_categories`` mutation.
        """
        self._db = db
        self._audit = audit if audit is not None else AuditService(db)

    # -- Per-transaction category writes (Req 25–31 audit emission) --

    _CATEGORY_AUDIT_TARGET = ("app", "transaction_categories")

    def set_category(
        self,
        transaction_id: str,
        *,
        category: str,
        subcategory: str | None = None,
        categorized_by: str = "user",
        actor: str,
    ) -> None:
        """Upsert a transaction's user category and emit ``category.set`` audit.

        Captures the prior row (or NULL) as ``before`` and the new shape as
        ``after`` so the audit trail can reconstruct overwrites. Mutation +
        audit row commit atomically.
        """
        self._db.begin()
        try:
            self.set_category_in_active_txn(
                transaction_id,
                category=category,
                subcategory=subcategory,
                categorized_by=categorized_by,
                actor=actor,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def set_category_in_active_txn(
        self,
        transaction_id: str,
        *,
        category: str,
        subcategory: str | None,
        categorized_by: str,
        actor: str,
    ) -> None:
        """``set_category`` body without txn boundaries.

        Use when the caller already owns a transaction and wants to batch
        multiple category writes atomically with their own audit chain.
        """
        prior = self._fetch_category_row(transaction_id)
        self._db.conn.execute(
            f"""
            INSERT INTO {TRANSACTION_CATEGORIES.full_name}
              (transaction_id, category, subcategory,
               categorized_at, categorized_by)
            VALUES (?, ?, ?, NOW(), ?)
            ON CONFLICT (transaction_id) DO UPDATE SET
                category = EXCLUDED.category,
                subcategory = EXCLUDED.subcategory,
                categorized_at = NOW(),
                categorized_by = EXCLUDED.categorized_by
            """,  # noqa: S608  # TRANSACTION_CATEGORIES is a TableRef constant
            [transaction_id, category, subcategory, categorized_by],
        )
        after = {
            "category": category,
            "subcategory": subcategory,
            "categorized_by": categorized_by,
        }
        self._audit.record_audit_event(
            action="category.set",
            target=(*self._CATEGORY_AUDIT_TARGET, transaction_id),
            before=prior,
            after=after,
            actor=actor,
        )

    def clear_category(self, transaction_id: str, *, actor: str) -> None:
        """Delete a transaction's category row and emit ``category.clear`` audit.

        No-op (and no audit event) when no row exists.
        """
        self._db.begin()
        try:
            prior = self._fetch_category_row(transaction_id)
            if prior is None:
                self._db.commit()
                return
            self._db.conn.execute(
                f"DELETE FROM {TRANSACTION_CATEGORIES.full_name} WHERE transaction_id = ?",  # noqa: S608  # TableRef constant
                [transaction_id],
            )
            self._audit.record_audit_event(
                action="category.clear",
                target=(*self._CATEGORY_AUDIT_TARGET, transaction_id),
                before=prior,
                after=None,
                actor=actor,
            )
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def _fetch_category_row(self, transaction_id: str) -> dict[str, Any] | None:
        """Return the current category row for ``transaction_id`` as a JSON-safe dict."""
        row = self._db.conn.execute(
            f"""
            SELECT category, subcategory, categorized_by
              FROM {TRANSACTION_CATEGORIES.full_name}
             WHERE transaction_id = ?
            """,  # noqa: S608  # TableRef constant
            [transaction_id],
        ).fetchone()
        if row is None:
            return None
        return {
            "category": row[0],
            "subcategory": row[1],
            "categorized_by": row[2],
        }

    # -- Merchant lookup / management --

    def match_merchant(self, description: str) -> dict[str, str | None] | None:
        """Look up a merchant by raw description.

        Args:
            description: Transaction description to match.

        Returns:
            Dict with merchant_id, canonical_name, category, subcategory
            if found, otherwise None.
        """
        merchants = _fetch_merchants(self._db)
        if merchants is None:
            return None
        return _match_description(description, merchants)

    def create_merchant(
        self,
        raw_pattern: str,
        canonical_name: str,
        *,
        match_type: MatchType = "contains",
        category: str | None = None,
        subcategory: str | None = None,
        created_by: str = "ai",
    ) -> str:
        """Create a merchant mapping.

        Args:
            raw_pattern: Pattern to match in transaction descriptions.
            canonical_name: Clean merchant name for display.
            match_type: How to match: 'exact', 'contains', or 'regex'.
            category: Optional default category for this merchant.
            subcategory: Optional default subcategory.
            created_by: Who created the mapping ('user', 'ai', 'rule').

        Returns:
            The merchant_id of the created merchant.
        """
        merchant_id = uuid.uuid4().hex[:12]
        self._db.execute(
            f"""
            INSERT INTO {USER_MERCHANTS.full_name}
            (merchant_id, raw_pattern, match_type, canonical_name,
             category, subcategory, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,  # noqa: S608  # USER_MERCHANTS is a TableRef constant, not user input
            [
                merchant_id,
                raw_pattern,
                match_type,
                canonical_name,
                category,
                subcategory,
                created_by,
            ],
        )
        logger.info(f"Created user merchant {merchant_id}")
        return merchant_id

    # -- Rule management --

    def create_rules(
        self, items: Sequence[CategorizationRuleInput]
    ) -> RuleCreationResult:
        """Create multiple categorization rules in one call.

        Each item is INSERTed into ``app.categorization_rules`` with a fresh
        12-char UUID hex ``rule_id``, ``is_active=true``, and
        ``created_by='ai'``. Per-row insertion failures are caught so a
        single bad row does not abort the batch — they appear in
        ``error_details``.
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

        return RuleCreationResult(
            created=created,
            skipped=skipped,
            error_details=error_details,
            rule_ids=rule_ids,
        )

    def deactivate_rule(self, rule_id: str) -> bool:
        """Soft-delete a rule by setting ``is_active=false``.

        Returns ``True`` if the rule existed (and is now inactive),
        ``False`` if no rule with that ID was found.
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
        return row is not None

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

    def bulk_categorize(
        self, items: Sequence[BulkCategorizationItem]
    ) -> BulkCategorizationResult:
        """Assign categories to multiple transactions with merchant auto-creation.

        For each item, looks up the transaction description, resolves or creates
        a merchant mapping, then inserts/replaces the category assignment.
        Merchant resolution is best-effort — failures do not prevent categorization.

        Read-side cost is O(1) in the number of items: one batch description
        fetch and one merchant-table fetch, regardless of input size.

        Args:
            items: Validated list of BulkCategorizationItem (transaction_id, category,
                optional subcategory). Validation is the caller's responsibility —
                use ``validate_bulk_items`` at the CLI/MCP boundary before calling this.

        Returns:
            BulkCategorizationResult with applied/skipped/error counts.
        """
        _start = perf_counter()
        try:
            return self._bulk_categorize_inner(items)
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
                SELECT transaction_id, description, amount, account_id, source_type
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
                    source_type=str(row[4]) if row[4] is not None else None,
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
                if description and ctx.merchant_mappings:
                    try:
                        existing = _match_description(
                            description, ctx.merchant_mappings
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

                if merchant_id is None and description:
                    try:
                        normalized = normalize_description(description)
                        if normalized:
                            merchant_id = self.create_merchant(
                                normalized,
                                normalized,
                                match_type="contains",
                                category=category,
                                subcategory=subcategory,
                                created_by="ai",
                            )
                            merchants_created += 1
                            # Register into context preserving _fetch_merchants()
                            # ordering (exact → contains → regex) so subsequent
                            # items in this batch match the just-created contains
                            # rule before any pre-existing regex rule.
                            new_row = (
                                merchant_id,
                                normalized,
                                "contains",
                                normalized,
                                category,
                                subcategory,
                            )
                            ctx.register_new_merchant(new_row)
                    except Exception:  # noqa: BLE001 — merchant resolution is best-effort; categorization proceeds without it
                        logger.debug(
                            f"Could not create merchant for {txn_id}",
                            exc_info=True,
                        )

                self._db.execute(
                    f"""
                    INSERT OR REPLACE INTO {TRANSACTION_CATEGORIES.full_name}
                    (transaction_id, category, subcategory,
                     categorized_at, categorized_by, merchant_id)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'ai', ?)
                    """,
                    [txn_id, category, subcategory, merchant_id],
                )
                applied += 1
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
                SELECT t.transaction_id, t.description
                FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                    ON t.transaction_id = c.transaction_id
                WHERE c.transaction_id IS NULL
                    AND t.description IS NOT NULL
                    AND t.description != ''
                """,
            ).fetchall()
        except duckdb.CatalogException:
            return 0

        if not uncategorized:
            return 0

        rows_to_insert: list[list[object]] = []
        for txn_id, description in uncategorized:
            merchant = _match_description(description, merchants)
            if merchant and merchant.get("category"):
                rows_to_insert.append([
                    txn_id,
                    merchant["category"],
                    merchant["subcategory"],
                    merchant["merchant_id"],
                ])
        if rows_to_insert:
            self._db.executemany(
                f"""
                INSERT OR IGNORE INTO {TRANSACTION_CATEGORIES.full_name}
                (transaction_id, category, subcategory, categorized_at,
                 categorized_by, merchant_id, confidence)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'rule', ?, 1.0)
                """,
                rows_to_insert,
            )
        categorized_count = len(rows_to_insert)

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
    ) -> tuple[str, str, str | None, str] | None:
        """Return ``(rule_id, category, subcategory, created_by)`` for the first rule that matches.

        Evaluates pattern (against both raw and normalized description),
        amount bounds, and account filter. Mirrors the rule engine semantics
        so callers can ask "would any rule match this transaction?" without
        duplicating logic. Returns ``None`` when no rule matches.
        """
        normalized = normalize_description(description)
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
            if not (
                matches_pattern(description, pattern, match_type)
                or matches_pattern(normalized, pattern, match_type)
            ):
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
        txn_row_override: tuple[str, float | None, str | None] | None = None,
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
        """
        if txn_row_override is not None:
            description, amount, account_id = txn_row_override
        else:
            try:
                txn_row = self._db.execute(
                    f"SELECT description, amount, account_id "
                    f"FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
                    [transaction_id],
                ).fetchone()
            except duckdb.CatalogException:
                return None
            if not txn_row or not txn_row[0]:
                return None
            description, amount, account_id = txn_row
        if not description:
            return None
        rules = (
            rules_override if rules_override is not None else self.fetch_active_rules()
        )
        if not rules:
            return None
        return self.match_first_rule(
            rules,
            str(description),
            float(amount) if amount is not None else None,
            str(account_id) if account_id is not None else None,
        )

    def apply_rules(self) -> int:
        """Apply active categorization rules to uncategorized transactions.

        Runs before merchant mapping in :meth:`apply_deterministic` so that
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
                SELECT t.transaction_id, t.description, t.amount, t.account_id
                FROM {FCT_TRANSACTIONS.full_name} t
                LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                    ON t.transaction_id = c.transaction_id
                WHERE c.transaction_id IS NULL
                    AND t.description IS NOT NULL
                    AND t.description != ''
                """,
            ).fetchall()
        except duckdb.CatalogException:
            return 0

        if not uncategorized:
            return 0

        rows_to_insert: list[list[object]] = []
        for txn_id, description, amount, account_id in uncategorized:
            match = self.match_first_rule(
                rules,
                str(description),
                float(amount) if amount is not None else None,
                str(account_id) if account_id is not None else None,
            )
            if match is None:
                continue
            rule_id, category, subcategory, created_by = match
            categorized_by = "auto_rule" if created_by == "auto_rule" else "rule"
            rows_to_insert.append([
                txn_id,
                category,
                subcategory,
                categorized_by,
                rule_id,
            ])
        if rows_to_insert:
            self._db.executemany(
                f"""
                INSERT OR IGNORE INTO {TRANSACTION_CATEGORIES.full_name}
                (transaction_id, category, subcategory, categorized_at,
                 categorized_by, rule_id, confidence)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?, 1.0)
                """,
                rows_to_insert,
            )
        categorized_count = len(rows_to_insert)

        if categorized_count:
            logger.info(f"Rule engine categorized {categorized_count} transactions")
        return categorized_count

    def apply_deterministic(self) -> dict[str, int]:
        """Run all deterministic categorization: rules first, then merchant fallback.

        Rules run first in priority order so explicit user-defined rules (which can
        filter by amount, account, and pattern) take precedence over generic merchant
        mappings. Merchant mappings apply only to transactions not matched by any rule.

        Returns:
            Dict with counts: {'merchant': N, 'rule': N, 'total': N}.
        """
        rule_count = self.apply_rules()
        merchant_count = self.apply_merchant_categories()
        total = merchant_count + rule_count

        if total:
            logger.info(
                f"Deterministic categorization: {merchant_count} merchant, "
                f"{rule_count} rule, {total} total"
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
        by RedactedTransaction's frozen dataclass shape.
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
                SELECT t.transaction_id, t.description, t.source_type
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
                    source_type=row[2] or "",
                )
                for row in rows
            ]
            return result
        finally:
            CATEGORIZE_ASSIST_DURATION_SECONDS.observe(time.monotonic() - start)
            CATEGORIZE_ASSIST_TXNS_RETURNED_TOTAL.inc(len(result))

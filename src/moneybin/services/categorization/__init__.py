"""Transaction categorization service.

Handles merchant normalization, rule-based categorization, merchant matching,
and taxonomy management. Designed for deterministic operations — LLM-based
auto-categorization lives in the MCP layer (auto_categorize tool).

The public API is the ``CategorizationService`` class. The companion
``AutoRuleService`` (``auto_rule_service.py``) handles the auto-rule
proposal/approval/deactivation lifecycle and depends on this module's
``find_matching_rule`` and ``normalize_description``.
"""

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Literal

import duckdb

from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import (
    CATEGORIZE_APPLY_POST_COMMIT_DURATION_SECONDS,
    CATEGORIZE_APPLY_POST_COMMIT_ROWS_AFFECTED,
    CATEGORIZE_DURATION_SECONDS,
    CATEGORIZE_ERRORS_TOTAL,
    CATEGORIZE_ITEMS_TOTAL,
    CATEGORIZE_WRITE_SKIPPED_PRECEDENCE_TOTAL,
    MERCHANT_EXEMPLAR_COUNT,
)
from moneybin.metrics.registry import (
    CATEGORIZE_MATCH_OUTCOME_TOTAL as CATEGORIZE_MATCH_OUTCOME_TOTAL,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services._text import (
    build_match_inputs,
    redact_for_llm,
)
from moneybin.services.audit_service import AuditService
from moneybin.services.categorization._shared import (
    SOURCE_PRIORITY,
    CategorizationItem,
    CategorizationRuleInput,
    CategorizedBy,
    InternalMatchType,
    Merchant,
    did_you_mean,
    priority_case_sql,
)
from moneybin.services.categorization._shared import (
    MatchType as MatchType,
)
from moneybin.services.categorization._shared import (
    match_shape_case_sql as match_shape_case_sql,
)
from moneybin.services.categorization._shared import (
    matches_pattern as matches_pattern,
)
from moneybin.services.categorization._shared import (
    score_match_shape as score_match_shape,
)
from moneybin.services.categorization._shared import (
    validate_items as validate_items,
)
from moneybin.services.categorization._shared import (
    validate_match_type as validate_match_type,
)
from moneybin.services.categorization._shared import (
    validate_rule_items as validate_rule_items,
)
from moneybin.services.categorization.matcher import (
    CategorizationMatcher,
    match_merchants,
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

    transaction_id: str
    description_redacted: str
    memo_redacted: str
    source_type: str
    transaction_type: str | None
    check_number: str | None
    is_transfer: bool
    transfer_pair_id: str | None
    payment_channel: str | None
    amount_sign: Literal["+", "-", "0"]


def _amount_sign_label(amount: float | None) -> Literal["+", "-", "0"]:
    """Map a raw amount to the LLM-facing sign signal.

    ``"0"`` covers both ``NULL`` (defective import) and zero amount (balance
    adjustments, voided rows). Mapping both to ``"+"`` biases the LLM toward
    income-side categories on rows that are neither income nor expense.
    """
    if amount is None or amount == 0:
        return "0"
    return "-" if amount < 0 else "+"


@dataclass(slots=True)
class CategorizationResult:
    """Typed result for categorization operations."""

    applied: int
    skipped: int
    errors: int
    error_details: list[dict[str, Any]]
    merchants_created: int = 0

    def to_envelope(self, input_count: int) -> ResponseEnvelope:
        """Build a ResponseEnvelope from this categorization result."""
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
    existing: int
    skipped: int
    error_details: list[dict[str, str]]
    rule_ids: list[str]

    def to_envelope(self, input_count: int) -> ResponseEnvelope:
        """Build a ResponseEnvelope from this rule-creation result."""
        return build_envelope(
            data={
                "created": self.created,
                "existing": self.existing,
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
        self._matcher = CategorizationMatcher(db)

    # -- Per-transaction category writes (Req 25–31 audit emission) --

    _CATEGORY_AUDIT_TARGET = ("app", "transaction_categories")

    def set_category(
        self,
        transaction_id: str,
        *,
        category: str,
        subcategory: str | None = None,
        categorized_by: Literal["user"] = "user",
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
        categorized_by: Literal["user"] = "user",
        actor: str,
    ) -> None:
        """``set_category`` body without txn boundaries.

        Use when the caller already owns a transaction and wants to batch
        multiple category writes atomically with their own audit chain.

        ``categorized_by`` is restricted to ``"user"``. The user-manual-edit
        path is the only intended use; the direct ``INSERT ... ON CONFLICT``
        below bypasses :meth:`write_categorization`'s precedence guard, so
        admitting a lower-priority source here would let it silently overwrite
        a higher-priority existing categorization. New callers needing a
        non-user write must route through :meth:`write_categorization`.
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

    def match_merchant(
        self, description: str, memo: str | None = None
    ) -> dict[str, str | None] | None:
        """Look up a merchant by raw description (and optional memo)."""
        return self._matcher.match_merchant(description, memo)

    def create_merchant(
        self,
        raw_pattern: str | None,
        canonical_name: str,
        *,
        match_type: InternalMatchType = "oneOf",
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
                Default ``False`` — callers inside a batch flow (e.g.,
                ``categorize_items``) skip this and let the enclosing snowball
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
             category, subcategory, created_by, exemplars, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
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

    def _find_merchant_by_canonical_name(
        self,
        canonical_name: str,
        *,
        category: str,
        subcategory: str | None,
    ) -> str | None:
        """Return ``merchant_id`` for the oneOf merchant with this canonical name + category, or None.

        Used by the exemplar accumulator to detect whether a previously-created
        merchant should grow its exemplar set instead of spawning a duplicate.

        Filters on ``match_type='oneOf'`` because exemplars are only consulted
        for oneOf merchants (see :func:`_match_exemplar`); appending an
        exemplar to a contains/exact/regex merchant is dead data. Also filters
        on ``category``/``subcategory``: two ``oneOf`` merchants can legitimately
        share a canonical name (e.g. an Amazon merchant mapped to ``Shopping``
        and another mapped to ``Groceries``); merging exemplars across them
        would cross-pollinate categorization. When the canonical name collides
        with a different-category merchant, the caller creates a new oneOf
        merchant alongside it — the two coexist, each doing its own job.
        """
        try:
            if subcategory is None:
                row = self._db.execute(
                    f"SELECT merchant_id FROM {USER_MERCHANTS.full_name} "
                    "WHERE canonical_name = ? AND match_type = 'oneOf' "
                    "AND category = ? AND subcategory IS NULL "
                    "LIMIT 1",  # noqa: S608  # TableRef constant
                    [canonical_name, category],
                ).fetchone()
            else:
                row = self._db.execute(
                    f"SELECT merchant_id FROM {USER_MERCHANTS.full_name} "
                    "WHERE canonical_name = ? AND match_type = 'oneOf' "
                    "AND category = ? AND subcategory = ? "
                    "LIMIT 1",  # noqa: S608  # TableRef constant
                    [canonical_name, category, subcategory],
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
        #
        # The `updated_at` advance is gated on the exemplars set actually
        # growing — when the exemplar already exists, the SET expression is
        # a no-op and the row's per-row freshness must NOT advance, per the
        # spec's "advances iff a real input changed" contract. See
        # docs/specs/core-updated-at-convention.md.
        row = self._db.execute(
            f"""
            UPDATE {USER_MERCHANTS.full_name}
            SET exemplars = list_distinct(list_append(exemplars, ?)),
                updated_at = CASE
                    WHEN list_contains(exemplars, ?) THEN updated_at
                    ELSE CURRENT_TIMESTAMP
                END
            WHERE merchant_id = ?
            RETURNING len(exemplars)
            """,  # noqa: S608  # USER_MERCHANTS is a TableRef constant
            [match_text, match_text, merchant_id],
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
        """Create multiple categorization rules in one call (idempotent).

        For each item, looks up an active rule with the same matcher and
        output (``merchant_pattern``, ``match_type``, ``min_amount``,
        ``max_amount``, ``account_id``, ``category``, ``subcategory``);
        ``name`` and ``priority`` are treated as metadata and excluded
        from the dedup key. If found, returns the existing ``rule_id``
        and bumps the ``existing`` counter; otherwise INSERTs a fresh
        12-char UUID hex ``rule_id`` with ``is_active=true`` and
        ``created_by='ai'``.

        Same matcher with a *different* category output is currently
        treated as a new rule, not a conflict — see
        ``docs/specs/mcp-tool-surface.md`` "Rule-conflict detection
        (follow-up)" for the deferred conflict-resolution work.

        When ``reapply=True``, ``categorize_pending`` runs after the writes so
        the new rules fan out to uncategorized rows immediately. Source-priority
        enforcement keeps user manual edits safe regardless.

        Per-row insertion failures are caught so a single bad row does
        not abort the batch — they appear in ``error_details``.
        """
        created = 0
        existing = 0
        skipped = 0
        error_details: list[dict[str, str]] = []
        rule_ids: list[str] = []

        for item in items:
            try:
                # DuckDB IS NOT DISTINCT FROM treats NULL = NULL as true,
                # so optional fields (min/max_amount, account_id, subcategory)
                # match on NULL across calls.
                found = self._db.execute(
                    f"""
                    SELECT rule_id FROM {CATEGORIZATION_RULES.full_name}
                    WHERE is_active = true
                      AND merchant_pattern = ?
                      AND match_type = ?
                      AND min_amount IS NOT DISTINCT FROM ?
                      AND max_amount IS NOT DISTINCT FROM ?
                      AND account_id IS NOT DISTINCT FROM ?
                      AND category = ?
                      AND subcategory IS NOT DISTINCT FROM ?
                    LIMIT 1
                    """,  # noqa: S608  # TableRef constant, no user input interpolated
                    [
                        item.merchant_pattern,
                        item.match_type,
                        item.min_amount,
                        item.max_amount,
                        item.account_id,
                        item.category,
                        item.subcategory,
                    ],
                ).fetchone()
                if found is not None:
                    existing += 1
                    rule_ids.append(found[0])
                    continue
                rule_id = uuid.uuid4().hex[:12]
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
            existing=existing,
            skipped=skipped,
            error_details=error_details,
            rule_ids=rule_ids,
        )

    def deactivate_rule(self, rule_id: str, *, reapply: bool = False) -> bool:
        """Soft-delete a rule by setting ``is_active=false``.

        Returns ``True`` if the rule existed (and is now inactive),
        ``False`` if no rule with that ID was found.

        When ``reapply=True`` and the rule was deactivated, strips any
        categorizations the now-deactivated rule had written
        (``categorized_by IN ('rule', 'auto_rule')`` with this rule_id) so
        those rows become pending again, then runs ``categorize_pending`` to
        re-evaluate them against the remaining active matchers. Writes from
        higher-priority sources (user/migration/ml/plaid) that happen to
        share this rule_id reference are left intact.
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
            self._db.execute(
                f"""
                DELETE FROM {TRANSACTION_CATEGORIES.full_name}
                WHERE rule_id = ?
                  AND categorized_by IN ('rule', 'auto_rule')
                """,  # noqa: S608  # TableRef constant, no user input interpolated
                [rule_id],
            )
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
                 is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, true, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
                f"SET is_active = ?, updated_at = CURRENT_TIMESTAMP "
                f"WHERE category_id = ?",
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

        Raises:
            ValueError: if ``categorized_by`` is not in :data:`SOURCE_PRIORITY`.
                The generated SQL CASE has no ELSE branch (see
                :func:`priority_case_sql`), so an unknown source would silently
                resolve to NULL priority and never overwrite — fail loudly
                instead of letting a typo masquerade as a precedence skip.
        """
        if categorized_by not in SOURCE_PRIORITY:
            raise ValueError(
                f"Unknown categorized_by={categorized_by!r}; "
                f"must be one of {sorted(SOURCE_PRIORITY)}"
            )
        # The SQL CASE expression is generated from SOURCE_PRIORITY so the
        # ladder lives in exactly one place. See priority_case_sql.
        existing_table = TRANSACTION_CATEGORIES.full_name
        excluded_priority = priority_case_sql("EXCLUDED.categorized_by")
        existing_priority = priority_case_sql(f"{existing_table}.categorized_by")
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
            """,  # noqa: S608  # TRANSACTION_CATEGORIES is a TableRef constant; CASE built from SOURCE_PRIORITY
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

    def categorize_items(
        self, items: Sequence[CategorizationItem]
    ) -> CategorizationResult:
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
            items: Validated list of CategorizationItem (transaction_id, category,
                optional subcategory). Validation is the caller's responsibility —
                use ``validate_items`` at the CLI/MCP boundary before calling this.

        Returns:
            CategorizationResult with applied/skipped/error counts.
        """
        _start = perf_counter()
        try:
            result = self._categorize_items_inner(items)
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
            CATEGORIZE_ERRORS_TOTAL.inc()
            raise
        finally:
            CATEGORIZE_DURATION_SECONDS.observe(perf_counter() - _start)

    def _categorize_items_inner(
        self, items: Sequence[CategorizationItem]
    ) -> CategorizationResult:
        applied = 0
        skipped = 0
        errors = 0
        merchants_created = 0
        error_details: list[dict[str, Any]] = []

        if not items:
            return CategorizationResult(
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
            # View not yet materialized (e.g., seed categories not loaded); skip validation.
            valid_category_set = None

        if valid_category_set:
            valid_sorted = sorted(valid_category_set)
            validated_items: list[CategorizationItem] = []
            for item in items:
                if item.category not in valid_category_set:
                    errors += 1
                    suggestions = did_you_mean(item.category, valid_sorted)
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
                CATEGORIZE_ITEMS_TOTAL.labels(outcome="error").inc(errors)
                return CategorizationResult(
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
        # (auto_rule_service → categorization).
        from moneybin.services.auto_rule_service import (  # noqa: PLC0415 — deferred to avoid circular import
            AutoRuleService,
            RecordingContext,
            TxnRow,
        )

        txn_rows: dict[str, TxnRow] = {}
        try:
            rows = self._db.execute(
                f"""
                SELECT transaction_id, description, amount, account_id,
                       memo, source_type
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
                    source_type=str(row[5]) if row[5] is not None else None,
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
            raw_merchants = self._matcher.fetch_merchants()
            cached_merchants: list[Merchant] = (
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

        ctx = RecordingContext(
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
                # Resolve pre-existing merchant first (read-only) so the
                # precedence-guarded write below can attach the matched
                # merchant_id when one already exists. Side-effects
                # (auto-rule recording + exemplar accumulation) are deferred
                # until after a successful write so a rejected suggestion
                # (lower-priority source) cannot poison merchant matching or
                # auto-rule training.
                merchant_id: str | None = None
                existing: dict[str, Any] | None = None
                description = ctx.description_for(txn_id)
                memo = ctx.memo_for(txn_id)
                match_text, norm_desc, norm_memo = build_match_inputs(description, memo)
                if match_text and ctx.merchant_mappings:
                    try:
                        existing = match_merchants(
                            match_text,
                            ctx.merchant_mappings,
                            normalized_description=norm_desc,
                            normalized_memo=norm_memo,
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

                outcome = self.write_categorization(
                    transaction_id=txn_id,
                    category=category,
                    subcategory=subcategory,
                    categorized_by="ai",
                    merchant_id=merchant_id,
                )
                if not outcome.written:
                    # Higher-priority source already categorized this row;
                    # leave it alone and surface as a skip. Skip auto-rule
                    # learning and exemplar accumulation entirely — the
                    # suggestion was rejected, so mutating downstream state
                    # based on it would poison future matching.
                    skipped += 1
                    error_details.append({
                        "transaction_id": txn_id,
                        "reason": (
                            "Skipped: a higher-priority categorization "
                            "(user, rule, or other) already covers this transaction."
                        ),
                        "error": "lower_priority_source",
                    })
                    continue

                applied += 1

                # Side-effects gated on outcome.written — only fire when the
                # categorization actually landed.
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
                # grow the exemplar set of an existing oneOf merchant with
                # the same LLM-proposed canonical_merchant_name, or create a
                # new exemplar-only merchant. System-generated merchants
                # never invent a contains pattern from the full description —
                # that over-generalized aggregator strings (bug 3).
                if merchant_id is None and match_text:
                    try:
                        canonical_name = item.canonical_merchant_name or match_text
                        existing_id = self._find_merchant_by_canonical_name(
                            canonical_name,
                            category=category,
                            subcategory=subcategory,
                        )
                        if existing_id is not None:
                            self._append_exemplar(existing_id, match_text)
                        else:
                            new_merchant_id = self.create_merchant(
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
                            # CategorizationMatcher.fetch_merchants ordering).
                            new_row = Merchant(
                                merchant_id=new_merchant_id,
                                raw_pattern=None,
                                match_type="oneOf",
                                canonical_name=canonical_name,
                                category=category,
                                subcategory=subcategory,
                                exemplars=[match_text],
                            )
                            ctx.register_new_merchant(new_row)
                    except Exception:  # noqa: BLE001 — exemplar accumulation is best-effort; categorization proceeds without it
                        logger.debug(
                            f"Could not accumulate exemplar for {txn_id}",
                            exc_info=True,
                        )
            except Exception:  # noqa: BLE001 — DuckDB raises untyped errors on constraint violations
                errors += 1
                logger.exception(f"categorize_items failed for transaction {txn_id!r}")
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

        CATEGORIZE_ITEMS_TOTAL.labels(outcome="applied").inc(applied)
        CATEGORIZE_ITEMS_TOTAL.labels(outcome="skipped").inc(skipped)
        CATEGORIZE_ITEMS_TOTAL.labels(outcome="error").inc(errors)
        return CategorizationResult(
            applied=applied,
            skipped=skipped,
            errors=errors,
            error_details=error_details,
            merchants_created=merchants_created,
        )

    def apply_merchant_categories(
        self,
        *,
        uncategorized: list[tuple[Any, ...]] | None = None,
        skip_txn_ids: set[str] | None = None,
    ) -> int:
        """Apply merchant-based categories to uncategorized transactions.

        Fetches all merchants once, then matches each uncategorized transaction
        in Python — avoids a per-transaction DB query.

        ``uncategorized`` lets :meth:`categorize_pending` share a single scan
        across :meth:`apply_rules` and this method. Rows are expected in the
        ``(transaction_id, description, amount, account_id, memo)`` shape from
        :meth:`CategorizationMatcher.fetch_uncategorized_rows`; ``amount`` and
        ``account_id`` are ignored here. When omitted, the rows are fetched.

        ``skip_txn_ids`` filters rows by transaction_id. :meth:`categorize_pending`
        passes the rule pass's applied set; without that filter the merchant
        write would overwrite the rule write at the same ``'rule'`` priority
        under the ``<=`` precedence guard.

        Returns:
            Number of transactions categorized.
        """
        merchants = self._matcher.fetch_merchants()
        if not merchants:
            return 0

        if uncategorized is None:
            rows = self._matcher.fetch_uncategorized_rows()
            if rows is None:
                return 0
            uncategorized = rows

        if not uncategorized:
            return 0

        categorized_count = 0
        for txn_id, description, _amount, _account_id, memo in uncategorized:
            if skip_txn_ids is not None and txn_id in skip_txn_ids:
                continue
            match_text, norm_desc, norm_memo = build_match_inputs(description, memo)
            if not match_text:
                continue
            merchant = match_merchants(
                match_text,
                merchants,
                normalized_description=norm_desc,
                normalized_memo=norm_memo,
                description_present=bool(description and str(description).strip()),
                memo_present=bool(memo and str(memo).strip()),
            )
            if merchant and merchant.get("category"):
                # Merchants don't have a dedicated source-priority slot in the v1
                # ladder (user/rule/auto_rule/migration/ml/plaid/ai). Recording
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
        return self._matcher.fetch_active_rules()

    @staticmethod
    def match_first_rule(
        rules: list[tuple[Any, ...]],
        description: str,
        amount: float | None,
        account_id: str | None,
        memo: str | None = None,
    ) -> tuple[str, str, str | None, str] | None:
        """Return ``(rule_id, category, subcategory, created_by)`` for the first rule that matches."""
        return CategorizationMatcher.match_first_rule(
            rules, description, amount, account_id, memo
        )

    def find_matching_rule(
        self,
        transaction_id: str,
        *,
        rules_override: list[tuple[Any, ...]] | None = None,
        txn_row_override: tuple[str, float | None, str | None, str | None]
        | None = None,
    ) -> tuple[str, str, str | None, str] | None:
        """Return the first active rule matching this transaction, or ``None``."""
        return self._matcher.find_matching_rule(
            transaction_id,
            rules_override=rules_override,
            txn_row_override=txn_row_override,
        )

    def apply_rules(
        self, *, uncategorized: list[tuple[Any, ...]] | None = None
    ) -> set[str]:
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

        ``uncategorized`` lets :meth:`categorize_pending` share a single scan
        with :meth:`apply_merchant_categories`. Rows are expected in the
        ``(transaction_id, description, amount, account_id, memo)`` shape from
        :meth:`CategorizationMatcher.fetch_uncategorized_rows`. When omitted, the rows are fetched.

        Returns:
            Set of ``transaction_id``s that landed a successful write. Count
            via ``len(...)``. :meth:`categorize_pending` passes the set to
            :meth:`apply_merchant_categories` as ``skip_txn_ids`` so the
            merchant pass doesn't overwrite rule writes at the same priority.
        """
        rules = self.fetch_active_rules()
        if not rules:
            return set()

        if uncategorized is None:
            rows = self._matcher.fetch_uncategorized_rows()
            if rows is None:
                return set()
            uncategorized = rows

        if not uncategorized:
            return set()

        applied: set[str] = set()
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
            categorized_by: CategorizedBy = (
                "auto_rule" if created_by == "auto_rule" else "rule"
            )
            outcome = self.write_categorization(
                transaction_id=txn_id,
                category=category,
                subcategory=subcategory,
                categorized_by=categorized_by,
                rule_id=rule_id,
                confidence=1.0,
            )
            if outcome.written:
                applied.add(txn_id)

        if applied:
            logger.info(f"Rule engine categorized {len(applied)} transactions")
        return applied

    def categorize_pending(self) -> dict[str, int]:
        """Categorize all pending (uncategorized) transactions.

        Runs current rules and merchants against pending transactions.
        Rules run first in priority order so explicit user-defined rules (which can
        filter by amount, account, and pattern) take precedence over generic merchant
        mappings. Merchant mappings apply only to transactions not matched by any rule.

        Idempotent: a second run on the same state writes nothing.

        Fetches uncategorized rows once and shares them with both
        :meth:`apply_rules` and :meth:`apply_merchant_categories`. The set of
        rule-written ``transaction_id``s is passed as ``skip_txn_ids`` to the
        merchant pass so it doesn't overwrite the rule writes at the same
        priority.

        Returns:
            Dict with counts: {'merchant': N, 'rule': N, 'total': N}.
        """
        rows = self._matcher.fetch_uncategorized_rows()
        if not rows:
            return {"merchant": 0, "rule": 0, "total": 0}

        rule_applied = self.apply_rules(uncategorized=rows)
        merchant_count = self.apply_merchant_categories(
            uncategorized=rows, skip_txn_ids=rule_applied
        )
        rule_count = len(rule_applied)
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
                    transaction_id=row[0],
                    description_redacted=redact_for_llm(row[1] or ""),
                    memo_redacted=redact_for_llm(row[2] or ""),
                    source_type=row[3] or "",
                    transaction_type=row[4],
                    check_number=row[5],
                    is_transfer=bool(row[6]),
                    transfer_pair_id=row[7],
                    payment_channel=row[8],
                    amount_sign=_amount_sign_label(row[9]),
                )
                for row in rows
            ]
            return result
        finally:
            CATEGORIZE_ASSIST_DURATION_SECONDS.observe(time.monotonic() - start)
            CATEGORIZE_ASSIST_TXNS_RETURNED_TOTAL.inc(len(result))

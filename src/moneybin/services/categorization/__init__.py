"""Transaction categorization service.

Handles merchant normalization, rule-based categorization, merchant matching,
and taxonomy management. Designed for deterministic operations — LLM-based
auto-categorization lives in the MCP layer (auto_categorize tool).

The public API is the ``CategorizationService`` class. The companion
``AutoRuleService`` (``auto_rule_service.py``) handles the auto-rule
proposal/approval/deactivation lifecycle and depends on this module's
``find_matching_rule``. Pure text helpers (``normalize_description``,
``build_match_inputs``, ``redact_for_llm``) live in
``moneybin.services._text`` so both packages can import from there
without a circular dependency.
"""

import logging
from collections.abc import Sequence
from typing import Any, Literal

from moneybin.config import get_settings as get_settings
from moneybin.database import Database
from moneybin.errors import UserError as UserError
from moneybin.metrics.registry import (
    CATEGORIZE_APPLY_POST_COMMIT_DURATION_SECONDS as CATEGORIZE_APPLY_POST_COMMIT_DURATION_SECONDS,
)
from moneybin.metrics.registry import (
    CATEGORIZE_APPLY_POST_COMMIT_ROWS_AFFECTED as CATEGORIZE_APPLY_POST_COMMIT_ROWS_AFFECTED,
)
from moneybin.metrics.registry import (
    CATEGORIZE_DURATION_SECONDS as CATEGORIZE_DURATION_SECONDS,
)
from moneybin.metrics.registry import (
    CATEGORIZE_ERRORS_TOTAL as CATEGORIZE_ERRORS_TOTAL,
)
from moneybin.metrics.registry import (
    CATEGORIZE_ITEMS_TOTAL as CATEGORIZE_ITEMS_TOTAL,
)
from moneybin.metrics.registry import (
    CATEGORIZE_MATCH_OUTCOME_TOTAL as CATEGORIZE_MATCH_OUTCOME_TOTAL,
)
from moneybin.metrics.registry import (
    CATEGORIZE_WRITE_SKIPPED_PRECEDENCE_TOTAL as CATEGORIZE_WRITE_SKIPPED_PRECEDENCE_TOTAL,
)
from moneybin.metrics.registry import (
    MERCHANT_EXEMPLAR_COUNT as MERCHANT_EXEMPLAR_COUNT,
)
from moneybin.protocol.envelope import ResponseEnvelope as ResponseEnvelope
from moneybin.protocol.envelope import build_envelope as build_envelope
from moneybin.services._text import (
    build_match_inputs as build_match_inputs,
)
from moneybin.services._text import (
    redact_for_llm as redact_for_llm,
)
from moneybin.services.audit_service import AuditService
from moneybin.services.categorization._shared import (
    SOURCE_PRIORITY as SOURCE_PRIORITY,
)
from moneybin.services.categorization._shared import (
    CategorizationItem,
    CategorizationRuleInput,
    InternalMatchType,
)
from moneybin.services.categorization._shared import (
    CategorizedBy as CategorizedBy,
)
from moneybin.services.categorization._shared import (
    MatchType as MatchType,
)
from moneybin.services.categorization._shared import (
    Merchant as Merchant,
)
from moneybin.services.categorization._shared import (
    did_you_mean as did_you_mean,
)
from moneybin.services.categorization._shared import (
    match_shape_case_sql as match_shape_case_sql,
)
from moneybin.services.categorization._shared import (
    matches_pattern as matches_pattern,
)
from moneybin.services.categorization._shared import (
    priority_case_sql as priority_case_sql,
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
from moneybin.services.categorization.applier import (
    MatchApplier,
    RuleCreationResult,
    WriteOutcome,
)
from moneybin.services.categorization.assist import (
    AssistBridge,
    RedactedTransaction,
)
from moneybin.services.categorization.matcher import (
    CategorizationMatcher,
)
from moneybin.services.categorization.orchestrator import (
    CategorizationOrchestrator,
    CategorizationResult,
)
from moneybin.services.categorization.queries import (
    CategorizationQueries,
    CategorizationStats,
)
from moneybin.tables import (
    CATEGORIES as CATEGORIES,
)
from moneybin.tables import (
    CATEGORIZATION_RULES as CATEGORIZATION_RULES,
)
from moneybin.tables import (
    CATEGORY_OVERRIDES as CATEGORY_OVERRIDES,
)
from moneybin.tables import (
    FCT_TRANSACTIONS as FCT_TRANSACTIONS,
)
from moneybin.tables import (
    MERCHANTS as MERCHANTS,
)
from moneybin.tables import (
    TRANSACTION_CATEGORIES as TRANSACTION_CATEGORIES,
)
from moneybin.tables import (
    USER_CATEGORIES as USER_CATEGORIES,
)
from moneybin.tables import (
    USER_MERCHANTS as USER_MERCHANTS,
)

logger = logging.getLogger(__name__)


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
        self._audit = audit if audit is not None else AuditService(db)
        self._matcher = CategorizationMatcher(db)
        self._applier = MatchApplier(db, audit=self._audit)
        self._assist = AssistBridge(db)
        self._queries = CategorizationQueries(db)
        self._orchestrator = CategorizationOrchestrator(
            db, matcher=self._matcher, applier=self._applier
        )

    # -- Per-transaction category writes (audit emission via applier) --

    def set_category(
        self,
        transaction_id: str,
        *,
        category: str,
        subcategory: str | None = None,
        categorized_by: Literal["user"] = "user",
        actor: str,
    ) -> None:
        """Upsert a transaction's user category and emit ``category.set`` audit."""
        self._applier.set_category(
            transaction_id,
            category=category,
            subcategory=subcategory,
            categorized_by=categorized_by,
            actor=actor,
        )

    def set_category_in_active_txn(
        self,
        transaction_id: str,
        *,
        category: str,
        subcategory: str | None,
        categorized_by: Literal["user"] = "user",
        actor: str,
    ) -> None:
        """``set_category`` body without txn boundaries (caller owns the transaction)."""
        self._applier.set_category_in_active_txn(
            transaction_id,
            category=category,
            subcategory=subcategory,
            categorized_by=categorized_by,
            actor=actor,
        )

    def clear_category(self, transaction_id: str, *, actor: str) -> None:
        """Delete a transaction's category row and emit ``category.clear`` audit."""
        self._applier.clear_category(transaction_id, actor=actor)

    # -- Merchant lookup / management --

    def match_merchant(
        self, description: str, memo: str | None = None
    ) -> dict[str, str | None] | None:
        """Look up a merchant by raw description (and optional memo)."""
        return self._matcher.match_merchant(description, memo)

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
        """Create a merchant mapping; optionally fan out to uncategorized rows.

        Pure write delegates to ``MatchApplier.create_merchant_core``; when
        ``reapply=True``, ``categorize_pending`` runs after the insert so the
        new merchant fans out to uncategorized rows immediately. Callers
        inside a batch flow (e.g., ``categorize_items``) skip this and let the
        enclosing snowball pass do the work instead.
        """
        merchant_id = self._applier.create_merchant_core(
            raw_pattern,
            canonical_name,
            match_type=match_type,
            category=category,
            subcategory=subcategory,
            created_by=created_by,
            exemplars=exemplars,
        )
        if reapply:
            self.categorize_pending()
        return merchant_id

    # -- Rule management --

    def create_rules(
        self,
        items: Sequence[CategorizationRuleInput],
        *,
        reapply: bool = False,
    ) -> RuleCreationResult:
        """Create multiple categorization rules in one call (idempotent).

        Pure writes delegate to ``MatchApplier.create_rules_core``. When
        ``reapply=True`` and at least one rule was newly created,
        ``categorize_pending`` runs so the new rules fan out to uncategorized
        rows immediately. Source-priority enforcement keeps user manual edits
        safe regardless.
        """
        result = self._applier.create_rules_core(items)
        if reapply and result.created > 0:
            self.categorize_pending()
        return result

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
        deactivated = self._applier.deactivate_rule_core(rule_id)
        if reapply and deactivated:
            self._applier.delete_rule_categorizations(rule_id)
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
        """Create a custom user category (active by default)."""
        return self._applier.create_category(
            category, subcategory=subcategory, description=description
        )

    def toggle_category(self, category_id: str, *, is_active: bool) -> None:
        """Enable or disable a category. Existing categorizations are preserved."""
        self._applier.toggle_category(category_id, is_active=is_active)

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
        """Insert or replace a categorization, respecting source precedence."""
        return self._applier.write_categorization(
            transaction_id=transaction_id,
            category=category,
            subcategory=subcategory,
            categorized_by=categorized_by,
            merchant_id=merchant_id,
            rule_id=rule_id,
            confidence=confidence,
        )

    # -- Batch orchestration --

    def categorize_items(
        self, items: Sequence[CategorizationItem]
    ) -> CategorizationResult:
        """Assign categories to multiple transactions with merchant auto-creation."""
        return self._orchestrator.categorize_items(items)

    def apply_rules(
        self, *, uncategorized: list[tuple[Any, ...]] | None = None
    ) -> set[str]:
        """Apply active categorization rules to uncategorized transactions."""
        return self._orchestrator.apply_rules(uncategorized=uncategorized)

    def apply_merchant_categories(
        self,
        *,
        uncategorized: list[tuple[Any, ...]] | None = None,
        skip_txn_ids: set[str] | None = None,
    ) -> int:
        """Apply merchant-based categories to uncategorized transactions."""
        return self._orchestrator.apply_merchant_categories(
            uncategorized=uncategorized, skip_txn_ids=skip_txn_ids
        )

    def categorize_pending(self) -> dict[str, int]:
        """Categorize all pending (uncategorized) transactions."""
        return self._orchestrator.categorize_pending()

    # -- Taxonomy / catalog reads --

    def get_active_categories(self) -> list[dict[str, str | bool | None]]:
        """Get all active categories."""
        return self._queries.get_active_categories()

    def get_all_categories(
        self, *, include_inactive: bool
    ) -> list[dict[str, str | bool | None]]:
        """Get categories with consistent field shape including is_active."""
        return self._queries.get_all_categories(include_inactive=include_inactive)

    def list_rules(self) -> list[dict[str, Any]]:
        """List all categorization rules (active and inactive) ordered by priority."""
        return self._queries.list_rules()

    def list_merchants(self) -> list[dict[str, str | None]]:
        """List all merchant name mappings ordered by canonical name."""
        return self._queries.list_merchants()

    def list_uncategorized_transactions(
        self, *, limit: int
    ) -> list[dict[str, Any]] | None:
        """List uncategorized transactions ordered by date descending."""
        return self._queries.list_uncategorized_transactions(limit=limit)

    def count_uncategorized(self) -> int:
        """Return the number of transactions without a category assignment."""
        return self._queries.count_uncategorized()

    def categorization_stats(self) -> dict[str, int | float]:
        """Get summary statistics about categorization coverage."""
        return self._queries.categorization_stats()

    def stats(self) -> CategorizationStats:
        """Get categorization stats as a typed result."""
        return self._queries.stats()

    def categorize_assist(
        self,
        limit: int = 100,
        account_filter: list[str] | None = None,
        date_range: tuple[str, str] | None = None,
    ) -> list[RedactedTransaction]:
        """Return uncategorized transactions as redacted records for LLM review."""
        return self._assist.categorize_assist(
            limit=limit,
            account_filter=account_filter,
            date_range=date_range,
        )

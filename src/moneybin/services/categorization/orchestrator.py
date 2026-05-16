"""Batch categorization: coordinates matcher reads and applier writes.

The orchestrator owns every multi-row "drive transactions through matching
to produce category writes" flow. It composes :class:`CategorizationMatcher`
(read-only) and :class:`MatchApplier` (writes); the facade constructs all
three and exposes the orchestration methods as thin delegates.

Flow inventory:

- :meth:`categorize_items` — apply an LLM-validated batch of
  (transaction_id, category) decisions; resolves merchants, writes
  categorizations, accumulates exemplars, runs a post-commit snowball.
- :meth:`apply_rules` — sweep uncategorized rows against active
  categorization rules.
- :meth:`apply_merchant_categories` — sweep uncategorized rows against the
  merchant catalog.
- :meth:`categorize_pending` — combined snowball: scan uncategorized once,
  run rules pass, then merchants pass (rule wins on overlap).

The dense entry point is :meth:`_categorize_items_inner`. It pulls from
several layers: taxonomy validation, batch transaction read, cached
merchants + rules from the matcher, the auto-rule observation hook
(lazy-imported to keep the module dependency one-way), and the applier's
write + exemplar accumulator. Source-precedence enforcement lives in
``MatchApplier.write_categorization`` — lower-priority sources cannot
overwrite higher-priority writes, so a rejected LLM suggestion never
poisons future matching or auto-rule training.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import Any

import duckdb

from moneybin.database import Database
from moneybin.metrics.registry import (
    CATEGORIZE_APPLY_POST_COMMIT_DURATION_SECONDS,
    CATEGORIZE_APPLY_POST_COMMIT_ROWS_AFFECTED,
    CATEGORIZE_DURATION_SECONDS,
    CATEGORIZE_ERRORS_TOTAL,
    CATEGORIZE_ITEMS_TOTAL,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services._text import build_match_inputs
from moneybin.services.categorization._shared import (
    CategorizationItem,
    CategorizedBy,
    Merchant,
    did_you_mean,
)
from moneybin.services.categorization.applier import MatchApplier
from moneybin.services.categorization.matcher import (
    CategorizationMatcher,
    match_merchants,
)
from moneybin.tables import CATEGORIES, FCT_TRANSACTIONS

logger = logging.getLogger(__name__)


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


class CategorizationOrchestrator:
    """Drives transactions through matching to produce category writes.

    Composes a :class:`CategorizationMatcher` (read-only) and a
    :class:`MatchApplier` (writes). Holds no state of its own beyond the
    injected collaborators; every method is a self-contained flow.
    """

    def __init__(
        self,
        db: Database,
        *,
        matcher: CategorizationMatcher,
        applier: MatchApplier,
    ) -> None:
        """Bind the orchestrator to its collaborators."""
        self._db = db
        self._matcher = matcher
        self._applier = applier

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
        order, bug 4). Source-priority enforcement from
        ``MatchApplier.write_categorization`` keeps user manual edits safe.

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
            cached_rules = self._matcher.fetch_active_rules()
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

                outcome = self._applier.write_categorization(
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
                        existing_id = self._applier.find_merchant_by_canonical_name(
                            canonical_name,
                            category=category,
                            subcategory=subcategory,
                        )
                        if existing_id is not None:
                            self._applier.append_exemplar(existing_id, match_text)
                        else:
                            new_merchant_id = self._applier.create_merchant_core(
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
        rules = self._matcher.fetch_active_rules()
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
            match = CategorizationMatcher.match_first_rule(
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
            outcome = self._applier.write_categorization(
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
                outcome = self._applier.write_categorization(
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

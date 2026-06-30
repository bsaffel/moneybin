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
from typing import TYPE_CHECKING, Any

import duckdb

from moneybin.database import Database
from moneybin.metrics.registry import (
    CATEGORIZE_APPLY_POST_COMMIT_DURATION_SECONDS,
    CATEGORIZE_APPLY_POST_COMMIT_ROWS_AFFECTED,
    CATEGORIZE_DURATION_SECONDS,
    CATEGORIZE_ERRORS_TOTAL,
    CATEGORIZE_ITEMS_TOTAL,
    CATEGORIZE_MATCH_OUTCOME_TOTAL,
    MERCHANT_RESOLUTION_OUTCOME_TOTAL,
)
from moneybin.privacy.payloads.categorize import CategorizeCommitPayload
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
from moneybin.tables import CATEGORIES, FCT_TRANSACTIONS, INT_TRANSACTIONS_MERGED

if TYPE_CHECKING:
    from moneybin.services.merchant_resolver import MerchantResolver

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CategorizationResult:
    """Typed result for categorization operations."""

    applied: int
    skipped: int
    errors: int
    error_details: list[dict[str, Any]]
    merchants_created: int = 0

    def to_payload(self) -> CategorizeCommitPayload:
        """Return a typed payload for the MCP/CLI envelope boundary."""
        return CategorizeCommitPayload(
            applied=self.applied,
            skipped=self.skipped,
            errors=self.errors,
            merchants_created=self.merchants_created,
            error_details=list(self.error_details),
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
        # merchant_entity_id lives in prep.int_transactions__merged (Task 5
        # deliberately stops it at prep); LEFT JOIN on the gold transaction_id
        # so rung-0 entity resolution can run before name matching. The fallback
        # drops the prep join (NULL entity id) so unit/pre-transform DBs — where
        # core.fct_transactions exists without the prep layer — still load rows.
        with_entity = f"""
            SELECT t.transaction_id, t.description, t.amount, t.account_id,
                   t.memo, t.source_type, t.merchant_name, m.merchant_entity_id,
                   m.merchant_entity_source_type
            FROM {FCT_TRANSACTIONS.full_name} AS t
            LEFT JOIN {INT_TRANSACTIONS_MERGED.full_name} AS m
                ON t.transaction_id = m.transaction_id
            WHERE t.transaction_id IN ({placeholders})
        """  # noqa: S608 — table names are compile-time TableRef constants; values are parameterized
        without_entity = f"""
            SELECT t.transaction_id, t.description, t.amount, t.account_id,
                   t.memo, t.source_type, t.merchant_name,
                   NULL AS merchant_entity_id, NULL AS merchant_entity_source_type
            FROM {FCT_TRANSACTIONS.full_name} AS t
            WHERE t.transaction_id IN ({placeholders})
        """  # noqa: S608 — table names are compile-time TableRef constants; values are parameterized
        try:
            try:
                rows = self._db.execute(with_entity, txn_ids).fetchall()
            except (duckdb.CatalogException, duckdb.BinderException):
                rows = self._db.execute(without_entity, txn_ids).fetchall()
            txn_rows = {
                row[0]: TxnRow(
                    description=row[1],
                    amount=float(row[2]) if row[2] is not None else None,
                    account_id=str(row[3]) if row[3] is not None else None,
                    memo=row[4],
                    source_type=str(row[5]) if row[5] is not None else None,
                    merchant_name=str(row[6]) if row[6] is not None else None,
                    merchant_entity_id=str(row[7]) if row[7] is not None else None,
                    merchant_entity_source_type=str(row[8])
                    if row[8] is not None
                    else None,
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

        # Rung-0 merchant resolution (M1T): resolve a transaction's merchant by
        # Plaid's merchant_entity_id before name matching. Bindings, rejected, and
        # pending are loaded once for the batch; the resolver mutates the cache in
        # place as it mints/binds so later items in the same batch adopt earlier
        # mints. Lazy import mirrors auto_rule_service above — MerchantResolver
        # imports back into this package (applier), so a module-level import cycles.
        resolver, bindings, rejected, pending = self._build_merchant_resolver()

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

                # Rung-0 (M1T): resolve by Plaid merchant_entity_id. Runs for
                # every item (a Plaid row may carry an entity id with no name
                # match) and after name matching so the resolver can adopt/
                # auto-bind/propose against the name match `existing`. A rung-4
                # mint sets merchant_id, which correctly gates the exemplar
                # accumulator off for id-bearing rows below.
                # The binding/mint/propose is an entity-keyed fact deliberately
                # committed before write_categorization; a precedence skip
                # suppresses only the categorization, never the binding
                # (spec Decision 7 — precedence-safe).
                merchant_id = self._resolve_entity_merchant(
                    resolver,
                    bindings,
                    self._applier,
                    rejected=rejected,
                    pending=pending,
                    merchant_entity_id=ctx.merchant_entity_id_for(txn_id),
                    source_type=ctx.merchant_entity_source_type_for(txn_id),
                    provider_merchant_name=ctx.merchant_name_for(txn_id),
                    name_match=existing,
                    current_merchant_id=merchant_id,
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

    def _build_merchant_resolver(
        self,
    ) -> tuple[
        MerchantResolver | None,
        dict[tuple[str, str], str],
        set[tuple[str, str, str]],
        set[tuple[str, str]],
    ]:
        """Construct the rung-0 MerchantResolver and load its caches once.

        Loads the accepted bindings cache (rung 1 adopt), the rejected decisions
        set (rung 2/3 guard), and the pending decisions set (rung 2/4 guard) so
        the resolver skips auto-binding or minting for entities currently under
        user review (spec "magic stays visible" — mirrors the guard in harvest()).

        Best-effort: if the merchant-link tables are missing (pre-migration DB),
        returns ``(None, {}, set(), set())`` so categorization degrades to name
        matching only. Lazy import keeps the module dependency one-way —
        MerchantResolver imports back into this package's applier, so a
        top-level import cycles.
        """
        from moneybin.services.merchant_resolver import (  # noqa: PLC0415 — deferred to avoid circular import
            MerchantResolver,
        )

        try:
            resolver = MerchantResolver(self._db, actor="system")
            bindings = resolver.load_bindings()
            rejected = resolver.load_rejected()
            pending = resolver.load_pending()
        except Exception:  # noqa: BLE001 — best-effort; degrades to no entity resolution
            logger.warning("Could not initialize merchant resolver", exc_info=True)
            return None, {}, set(), set()
        return resolver, bindings, rejected, pending

    @staticmethod
    def _resolve_entity_merchant(
        resolver: MerchantResolver | None,
        bindings: dict[tuple[str, str], str],
        applier: MatchApplier,
        *,
        rejected: set[tuple[str, str, str]],
        pending: set[tuple[str, str]],
        merchant_entity_id: str | None,
        source_type: str | None,
        provider_merchant_name: str | None,
        name_match: dict[str, Any] | None,
        current_merchant_id: str | None,
    ) -> str | None:
        """Run rung-0 entity resolution; return the merchant_id to write.

        Returns ``current_merchant_id`` unchanged when the resolver is absent or
        produces no id (no entity id, empty source_type, or degraded). On an
        adopt/auto-bind/mint outcome, records the entity-id match metric and
        returns the resolved id.
        """
        if resolver is None or not merchant_entity_id or not source_type:
            return current_merchant_id
        try:
            res = resolver.resolve(
                merchant_entity_id=merchant_entity_id,
                source_type=source_type,
                provider_merchant_name=provider_merchant_name,
                name_match=name_match,
                bindings=bindings,
                rejected=rejected,
                pending=pending,
                applier=applier,
            )
        except Exception:  # noqa: BLE001 — entity resolution is best-effort
            logger.debug("merchant entity resolution failed", exc_info=True)
            return current_merchant_id
        if res.merchant_id is None:
            return current_merchant_id
        # Spec-mandated ladder-outcome counter (one per resolved transaction).
        MERCHANT_RESOLUTION_OUTCOME_TOTAL.labels(outcome=res.outcome).inc()
        if res.outcome in ("adopted", "auto_bound", "minted"):
            CATEGORIZE_MATCH_OUTCOME_TOTAL.labels(
                outcome="entity_id", shape="both"
            ).inc()
        return res.merchant_id

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
        with :meth:`apply_merchant_categories`. Rows come from
        :meth:`CategorizationMatcher.fetch_uncategorized_rows`; this method uses
        only the leading ``(transaction_id, description, amount, account_id,
        memo)`` columns and ignores the trailing entity-resolution columns.
        When omitted, the rows are fetched.

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
        for txn_id, description, amount, account_id, memo, *_rest in uncategorized:
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
        in Python — avoids a per-transaction DB query. An empty merchant catalog
        (``[]``) still runs entity resolution so rung-4 minting fires for novel
        provider entity ids before any merchant is authored; only a ``None``
        return from ``fetch_merchants`` (catalog table absent) exits early.

        ``uncategorized`` lets :meth:`categorize_pending` share a single scan
        across :meth:`apply_rules` and this method. Rows come from
        :meth:`CategorizationMatcher.fetch_uncategorized_rows`: the leading
        ``(transaction_id, description, amount, account_id, memo)`` columns plus
        trailing ``(merchant_entity_id, source_type, merchant_name,
        merchant_entity_source_type)`` consulted for rung-0 entity resolution.
        ``amount``, ``account_id``, and the merge-winner ``source_type`` are
        ignored here — the resolver keys on ``merchant_entity_source_type`` (the
        member that issued the entity id). When omitted, the rows are fetched.

        ``skip_txn_ids`` filters rows by transaction_id. :meth:`categorize_pending`
        passes the rule pass's applied set; without that filter the merchant
        write would overwrite the rule write at the same ``'rule'`` priority
        under the ``<=`` precedence guard.

        Entity resolution runs unconditionally for every row — an entity-bound
        transaction (spec Decision 3 rung-1) adopts its bound merchant even
        when the description text matches no merchant pattern. When the resolver
        returns a ``merchant_id`` with no name match, the category is sourced
        from that merchant's catalog default. Rung-4 minting can fire for a
        novel entity id with no existing binding and no name match; subsequent
        re-runs hit rung 1 (idempotent binding cache), so re-runs adopt rather
        than re-mint.

        Returns:
            Number of transactions categorized.
        """
        merchants = self._matcher.fetch_merchants()
        if merchants is None:
            return 0  # merchant catalog table absent — nothing to do
        # An EMPTY catalog ([]) still proceeds: entity resolution (rung-4) can mint
        # the first merchant from a provider entity id even before any merchant is
        # authored. An empty list is valid input for match_merchants (returns None).

        if uncategorized is None:
            rows = self._matcher.fetch_uncategorized_rows()
            if rows is None:
                return 0
            uncategorized = rows

        if not uncategorized:
            return 0

        # Rung-0 (M1T): consult the entity resolver before the merchant_id is
        # written so an existing entity binding wins / auto-binds. Built once
        # for the sweep; degrades to (None, {}, set(), set()) when the link
        # tables are absent.
        resolver, bindings, rejected, pending = self._build_merchant_resolver()

        # Category lookup for the entity-adoption path (no name match): when the
        # resolver returns a merchant_id without a name-matched category, read the
        # merchant's default category from this map. Built once from the
        # already-fetched merchant list — no additional query.
        merchant_cat: dict[str, tuple[str | None, str | None]] = {
            m.merchant_id: (m.category, m.subcategory) for m in merchants
        }

        categorized_count = 0
        for (
            txn_id,
            description,
            _amount,
            _account_id,
            memo,
            merchant_entity_id,
            _source_type,
            merchant_name,
            merchant_entity_source_type,
        ) in uncategorized:
            if skip_txn_ids is not None and txn_id in skip_txn_ids:
                continue
            match_text, norm_desc, norm_memo = build_match_inputs(description, memo)
            # Compute the name match but do NOT gate the resolver on it: an
            # entity-bound transaction must adopt its merchant even when the
            # description matches no pattern (spec Decision 3 rung-1 payoff).
            merchant = (
                match_merchants(
                    match_text,
                    merchants,
                    normalized_description=norm_desc,
                    normalized_memo=norm_memo,
                    description_present=bool(description and str(description).strip()),
                    memo_present=bool(memo and str(memo).strip()),
                )
                if match_text
                else None
            )
            # Merchants don't have a dedicated source-priority slot in the v1
            # ladder (user/rule/auto_rule/migration/ml/plaid/ai). Recording
            # merchant matches as 'rule' preserves historical behavior; a
            # follow-up spec may introduce a dedicated 'merchant' priority
            # between auto_rule and migration.
            categorized_by: CategorizedBy = "rule"
            # The binding/mint/propose is an entity-keyed fact deliberately committed
            # before write_categorization; a precedence skip suppresses only the
            # categorization, never the binding (spec Decision 7 — precedence-safe).
            merchant_id = self._resolve_entity_merchant(
                resolver,
                bindings,
                self._applier,
                rejected=rejected,
                pending=pending,
                merchant_entity_id=str(merchant_entity_id)
                if merchant_entity_id is not None
                else None,
                source_type=str(merchant_entity_source_type)
                if merchant_entity_source_type is not None
                else None,
                provider_merchant_name=str(merchant_name)
                if merchant_name is not None
                else None,
                name_match=merchant,
                current_merchant_id=merchant["merchant_id"] if merchant else None,
            )
            # Choose the category to write.
            # Rung-1 "skip name matching": the adopted/bound merchant's own
            # category wins over a disagreeing name match — the entity binding
            # is the identity source of truth.  Fall back to the name match's
            # category only when the resolved merchant has no default category
            # (e.g. a Plaid-minted merchant) or no entity resolution occurred.
            category: str | None = None
            subcategory: str | None = None
            if merchant_id is not None:
                # Rung-1 "skip name matching": the adopted/bound merchant's own category wins
                # over a disagreeing name match — the entity binding is the identity source of truth.
                cat, subcat = merchant_cat.get(merchant_id, (None, None))
                if cat is not None:
                    category, subcategory = cat, subcat
            if category is None and merchant and merchant.get("category"):
                # Resolved merchant has no default category (fresh plaid mint) or no entity
                # resolution occurred → fall back to the name match's category.
                category, subcategory = (
                    str(merchant["category"]),
                    merchant["subcategory"],
                )
            # app.transaction_categories.category is NOT NULL — skip the write
            # when category is None (identity captured by binding; category
            # deferred to rules / LLM / Tier-2b).
            if category is None:
                continue
            outcome = self._applier.write_categorization(
                transaction_id=txn_id,
                category=category,
                subcategory=subcategory,
                categorized_by=categorized_by,
                merchant_id=merchant_id,
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

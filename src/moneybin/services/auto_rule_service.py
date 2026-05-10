"""Auto-rule lifecycle service: pattern extraction, proposal tracking, override detection.

Observes user/AI categorizations recorded via ``CategorizationService.categorize_items``,
stages pattern → category proposals in ``app.proposed_rules``, promotes approved
proposals into active rules in ``app.categorization_rules`` with
``created_by='auto_rule'``, and deactivates rules whose categories the user has
overridden past the configured threshold.

Depends on ``CategorizationService`` for canonical rule-match semantics
(``find_matching_rule``). The dependency is one-directional — ``CategorizationService``
does not import this module at module load time.
"""

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any

import duckdb

from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.services._text import normalize_description
from moneybin.services.categorization_service import (
    CategorizationService,
    matches_pattern,
)
from moneybin.tables import (
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    PROPOSED_RULES,
    RULE_DEACTIVATIONS,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ApproveResult:
    """Result of an approve() call: counts of approved, skipped, and newly categorized transactions."""

    approved: int = 0
    skipped: int = 0
    newly_categorized: int = 0
    rule_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RejectResult:
    """Result of a reject() call: counts of rejected and skipped proposals."""

    rejected: int = 0
    skipped: int = 0


@dataclass(slots=True)
class AutoReviewResult:
    """Pending auto-rule proposals awaiting human review."""

    proposals: list[dict[str, Any]]
    total_count: int = 0


@dataclass(slots=True)
class AutoConfirmResult:
    """Aggregate counts from a confirm() call (combined approve + reject)."""

    approved: int = 0
    rejected: int = 0
    skipped: int = 0
    newly_categorized: int = 0
    rule_ids: list[str] = field(default_factory=list)


@dataclass(slots=True)
class AutoStatsResult:
    """Auto-rule health metrics."""

    active_auto_rules: int = 0
    pending_proposals: int = 0
    transactions_categorized: int = 0


@dataclass(slots=True, frozen=True)
class TxnRow:
    """Pre-loaded transaction columns needed by the batch path."""

    description: str | None
    amount: float | None
    account_id: str | None


@dataclass(slots=True)
class RecordingContext:
    """In-memory caches threaded through ``record_categorization`` during a batch loop.

    Owns the data that today's per-item helpers re-fetch from the database.
    Mutators (``register_new_merchant``) preserve the same ordering invariants
    that ``_fetch_merchants`` produces (exact → contains → regex), so cover
    checks see new merchants in their canonical match position.
    """

    txn_rows: dict[str, TxnRow]
    active_rules: list[tuple[Any, ...]]
    merchant_mappings: list[tuple[Any, ...]]
    new_merchant_count: int = field(default=0)

    def txn_row_for(self, transaction_id: str) -> TxnRow | None:
        """Return the pre-loaded TxnRow for the given transaction_id, or None."""
        return self.txn_rows.get(transaction_id)

    def description_for(self, transaction_id: str) -> str | None:
        """Return the description for the given transaction_id, or None if not loaded."""
        row = self.txn_rows.get(transaction_id)
        return row.description if row else None

    def merchant_row_for(self, merchant_id: str) -> tuple[Any, ...] | None:
        """Return the cached merchant tuple for the given merchant_id, or None."""
        for merchant in self.merchant_mappings:
            if str(merchant[0]) == merchant_id:
                return merchant
        return None

    def register_new_merchant(self, merchant_row: tuple[Any, ...]) -> None:
        """Insert at the canonical match-order position (before the first regex)."""
        insert_at = next(
            (i for i, m in enumerate(self.merchant_mappings) if m[2] == "regex"),
            len(self.merchant_mappings),
        )
        self.merchant_mappings.insert(insert_at, merchant_row)
        self.new_merchant_count += 1

    def merchant_mapping_covers(
        self, pattern: str, category: str, subcategory: str | None
    ) -> bool:
        """Mirror of ``AutoRuleService._merchant_mapping_covers`` against the cached list."""
        for merchant in self.merchant_mappings:
            _mid, raw_pattern, match_type, _canonical, m_cat, m_subcat = merchant
            if str(m_cat) != category:
                continue
            if (m_subcat if m_subcat is None else str(m_subcat)) != subcategory:
                continue
            if matches_pattern(
                pattern, str(raw_pattern), str(match_type or "contains")
            ):
                return True
        return False


class AutoRuleService:
    """Auto-rule lifecycle: observe → propose → approve/reject → deactivate.

    All public methods are independent of ``CategorizationService``'s public
    surface; callers wire them up through their own command/tool layer (CLI,
    MCP). Hooks called from ``CategorizationService.categorize_items`` use a
    lazy import on the categorization side to keep imports one-directional.
    """

    def __init__(self, db: Database) -> None:
        """Bind the service to a database connection."""
        self._db = db
        self._cat_service: CategorizationService | None = None

    @property
    def _categorization(self) -> CategorizationService:
        if self._cat_service is None:
            self._cat_service = CategorizationService(self._db)
        return self._cat_service

    # -- Observation --

    def record_categorization(
        self,
        transaction_id: str,
        category: str,
        *,
        subcategory: str | None = None,
        merchant_id: str | None = None,
        context: RecordingContext | None = None,
    ) -> str | None:
        """Record a categorization event for auto-rule learning.

        Returns the proposed_rule_id if a proposal was created or updated,
        ``None`` if the categorization was filtered out (already covered by
        an active rule, by a merchant mapping with no in-progress proposal,
        or no extractable pattern).

        ``merchant_id`` lets callers pass an already-resolved merchant so the
        merchant's ``(raw_pattern, match_type)`` is used as the proposal
        pattern. Without it, ``_extract_pattern`` looks up the merchant from
        ``transaction_categories``, but ``categorize_items`` calls this hook
        before that row exists — so omitting the parameter forces the
        description fallback for every fresh categorization.

        ``context`` supplies pre-loaded transaction rows, active rules, and
        merchant mappings for the batch path so no read queries are issued
        per-transaction. When ``None``, the existing DB-backed behavior is used.
        """
        extracted = self._extract_pattern(
            transaction_id, merchant_id=merchant_id, context=context
        )
        if not extracted:
            return None
        pattern, match_type = extracted

        if self._active_rule_covers_transaction(
            transaction_id, category, subcategory, context=context
        ):
            return None

        settings = get_settings().categorization
        threshold = settings.auto_rule_proposal_threshold
        sample_cap = settings.auto_rule_sample_txn_cap
        existing = self._find_pending_proposal(pattern, match_type)

        # Merchant coverage is only a reason to skip when there is no
        # in-progress proposal for this pattern. Otherwise tracking proposals
        # could be permanently stuck below threshold once categorize_items
        # creates the merchant mapping during the first categorization.
        if existing is None and self._merchant_mapping_covers(
            pattern, category, subcategory, context=context
        ):
            return None

        if existing is not None:
            (
                proposed_rule_id,
                existing_category,
                existing_subcategory,
                count,
                samples,
            ) = existing
            if existing_category == category and existing_subcategory == subcategory:
                already_counted = transaction_id in samples
                new_samples = samples if already_counted else samples + [transaction_id]
                # Keep the most recent samples when over capacity — appended IDs
                # are newest, so a head-slice would silently drop the new txn.
                new_samples = new_samples[-sample_cap:]
                # Only increment trigger_count for distinct transactions so
                # replays (MCP retry, re-import) cannot promote a proposal to
                # ``pending`` without genuinely new evidence.
                new_count = count if already_counted else count + 1
                new_status = "pending" if new_count >= threshold else "tracking"
                self._db.execute(
                    f"""
                    UPDATE {PROPOSED_RULES.full_name}
                    SET trigger_count = ?, sample_txn_ids = ?, status = ?
                    WHERE proposed_rule_id = ?
                    """,
                    [new_count, new_samples, new_status, proposed_rule_id],
                )
                return proposed_rule_id
            # Different category: supersede the old proposal, fall through to create a new one
            self._db.execute(
                f"UPDATE {PROPOSED_RULES.full_name} SET status = 'superseded' WHERE proposed_rule_id = ?",
                [proposed_rule_id],
            )

        proposed_rule_id = uuid.uuid4().hex[:12]
        initial_status = "pending" if threshold <= 1 else "tracking"
        self._db.execute(
            f"""
            INSERT INTO {PROPOSED_RULES.full_name}
            (proposed_rule_id, merchant_pattern, match_type, category, subcategory,
             status, trigger_count, source, sample_txn_ids)
            VALUES (?, ?, ?, ?, ?, ?, 1, 'pattern_detection', ?)
            """,
            [
                proposed_rule_id,
                pattern,
                match_type,
                category,
                subcategory,
                initial_status,
                [transaction_id],
            ],
        )
        return proposed_rule_id

    # -- Decisions --

    def review(self, *, limit: int | None = None) -> AutoReviewResult:
        """Return pending auto-rule proposals as a typed result.

        ``limit`` defaults to ``categorization.auto_rule_list_default_limit``
        when ``None``. ``total_count`` reflects the unbounded queue size so
        callers see ``has_more`` when truncation occurs.
        """
        effective_limit = self._resolve_list_limit(limit)
        proposals = self.list_pending_proposals(limit=effective_limit)
        # When the queue fits under the cap, the total is just len(proposals).
        # Skip the COUNT(*) roundtrip in the common case (typical pending queues
        # are well under 100).
        total = (
            len(proposals)
            if len(proposals) < effective_limit
            else self._count_pending_proposals()
        )
        return AutoReviewResult(proposals=proposals, total_count=total)

    @staticmethod
    def _resolve_list_limit(limit: int | None) -> int:
        """Return ``limit`` or the configured default."""
        if limit is not None:
            return limit
        return get_settings().categorization.auto_rule_list_default_limit

    def accept(
        self,
        accept: list[str] | None = None,
        reject: list[str] | None = None,
    ) -> AutoConfirmResult:
        """Accept and/or reject pending proposals; returns aggregate counts.

        IDs appearing in both lists are dropped from ``accept`` so an explicit
        reject always wins. The CLI does the same dedup before calling, but
        applying it here keeps direct service callers (MCP, scripts) safe.
        """
        approve_set = set(accept or [])
        reject_set = set(reject or [])
        approve_set -= reject_set
        a = self.approve(sorted(approve_set))
        r = self.reject(sorted(reject_set))
        return AutoConfirmResult(
            approved=a.approved,
            rejected=r.rejected,
            skipped=a.skipped + r.skipped,
            newly_categorized=a.newly_categorized,
            rule_ids=a.rule_ids,
        )

    def approve(self, proposed_rule_ids: list[str]) -> ApproveResult:
        """Promote pending proposals to active rules and immediately categorize matching transactions."""
        settings = get_settings().categorization
        result = ApproveResult()

        for pid in proposed_rule_ids:
            row = self._db.execute(
                f"""
                SELECT merchant_pattern, match_type, category, subcategory, status
                FROM {PROPOSED_RULES.full_name} WHERE proposed_rule_id = ?
                """,
                [pid],
            ).fetchone()
            if not row or row[4] != "pending":
                result.skipped += 1
                continue

            pattern, match_type, category, subcategory, _status = row
            rule_id = uuid.uuid4().hex[:12]
            # Wrap rule INSERT, proposal UPDATE, and back-fill INSERT in a single
            # transaction so a partial failure (e.g., interrupt between steps)
            # cannot leave an active rule whose source proposal is still 'pending'
            # — which would let approve() create a duplicate rule on retry.
            self._db.begin()
            try:
                self._db.execute(
                    f"""
                    INSERT INTO {CATEGORIZATION_RULES.full_name}
                    (rule_id, name, merchant_pattern, match_type, category, subcategory,
                     priority, is_active, created_by, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, true, 'auto_rule', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    [
                        rule_id,
                        f"auto: {pattern}",
                        pattern,
                        match_type,
                        category,
                        subcategory,
                        settings.auto_rule_default_priority,
                    ],
                )
                self._db.execute(
                    f"""
                    UPDATE {PROPOSED_RULES.full_name}
                    SET status = 'approved', decided_at = CURRENT_TIMESTAMP, decided_by = 'user'
                    WHERE proposed_rule_id = ?
                    """,
                    [pid],
                )
                newly = self._categorize_existing_with_rule(
                    rule_id, category, subcategory
                )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise
            result.approved += 1
            result.rule_ids.append(rule_id)
            result.newly_categorized += newly

        if result.approved:
            logger.info(
                f"Approved {result.approved} auto-rule proposal(s); "
                f"{result.newly_categorized} existing transaction(s) categorized"
            )
        return result

    def reject(self, proposed_rule_ids: list[str]) -> RejectResult:
        """Mark pending proposals as rejected. No rule is created."""
        result = RejectResult()
        for pid in proposed_rule_ids:
            row = self._db.execute(
                f"SELECT status FROM {PROPOSED_RULES.full_name} WHERE proposed_rule_id = ?",
                [pid],
            ).fetchone()
            if not row or row[0] != "pending":
                result.skipped += 1
                continue
            self._db.execute(
                f"""
                UPDATE {PROPOSED_RULES.full_name}
                SET status = 'rejected', decided_at = CURRENT_TIMESTAMP, decided_by = 'user'
                WHERE proposed_rule_id = ?
                """,
                [pid],
            )
            result.rejected += 1
        return result

    # -- Override detection --

    def check_overrides(self) -> int:
        """Deactivate auto-rules with override count >= configured threshold; return number deactivated.

        An override = a transaction whose description matches the auto-rule's pattern
        but is currently categorized by 'user' with a different category. When the
        threshold is reached we deactivate the rule, mark its source proposal superseded,
        create a new pending proposal with the most common override category, and
        append a row to ``app.rule_deactivations`` for the audit trail.
        """
        settings = get_settings().categorization
        threshold = settings.auto_rule_override_threshold
        proposal_threshold = settings.auto_rule_proposal_threshold
        sample_cap = settings.auto_rule_sample_txn_cap

        # Fast-path: skip the override scan entirely when no auto-rules exist.
        # categorize_items calls this on every batch, so a one-row probe avoids
        # an unnecessary aggregate scan in the common pre-promotion case.
        if not self._db.execute(
            f"""
            SELECT 1 FROM {CATEGORIZATION_RULES.full_name}
            WHERE created_by = 'auto_rule' AND is_active = true LIMIT 1
            """
        ).fetchone():
            return 0

        rules = self._db.execute(
            f"""
            SELECT rule_id, merchant_pattern, match_type, category, subcategory, created_at
            FROM {CATEGORIZATION_RULES.full_name}
            WHERE is_active = true AND created_by = 'auto_rule'
            """
        ).fetchall()
        deactivated = 0

        for (
            rule_id,
            pattern,
            rule_match_type,
            rule_category,
            rule_subcategory,
            rule_created_at,
        ) in rules:
            # An override is any human-driven correction recorded after the rule
            # was created whose (category, subcategory) disagrees with the rule.
            # Excludes 'rule' and 'auto_rule' (machine-applied; counting them
            # would deactivate auto-rules due to overlapping rule engine output)
            # and predates legacy categorizations via the created_at filter.
            # Pattern matching is done in Python via ``matches_pattern`` against
            # both the raw and normalized description, mirroring how the rule
            # engine itself selects rows. SQL-only matching cannot reproduce
            # ``normalize_description`` (Python regex) and got case-sensitivity
            # wrong on the regex branch.
            candidate_rows = self._db.execute(
                f"""
                SELECT c.transaction_id, t.description, c.category, c.subcategory,
                       c.categorized_at
                FROM {TRANSACTION_CATEGORIES.full_name} c
                JOIN {FCT_TRANSACTIONS.full_name} t ON c.transaction_id = t.transaction_id
                WHERE c.categorized_by IN ('user', 'ai')
                  AND c.categorized_at > ?
                  AND t.description IS NOT NULL
                  AND (
                    c.category != ?
                    OR COALESCE(c.subcategory, '') != COALESCE(?, '')
                  )
                ORDER BY c.categorized_at DESC
                """,
                [rule_created_at, rule_category, rule_subcategory],
            ).fetchall()
            buckets: dict[tuple[str, str | None], list[str]] = {}
            for txn_id, description, c_cat, c_subcat, _at in candidate_rows:
                desc = str(description)
                if not (
                    matches_pattern(desc, pattern, rule_match_type)
                    or matches_pattern(
                        normalize_description(desc), pattern, rule_match_type
                    )
                ):
                    continue
                key = (str(c_cat), c_subcat if c_subcat is None else str(c_subcat))
                buckets.setdefault(key, []).append(str(txn_id))
            total_overrides = sum(len(v) for v in buckets.values())
            if total_overrides < threshold:
                continue

            # Pick the largest bucket as the corrected category.
            (new_category, new_subcategory), winning_ids = max(
                buckets.items(), key=lambda kv: len(kv[1])
            )
            winning_count = len(winning_ids)
            sample_ids = winning_ids[:sample_cap]
            # Re-proposal trigger_count reflects the winning bucket — total
            # overrides may include unrelated minority buckets that should not
            # inflate the new proposal's count.
            new_status = (
                "pending" if winning_count >= proposal_threshold else "tracking"
            )
            new_pid = uuid.uuid4().hex[:12]
            # Wrap deactivate + supersede + re-propose + audit in a single
            # transaction so a failure between steps cannot leave the rule
            # deactivated with no replacement proposal.
            self._db.begin()
            try:
                self._db.execute(
                    f"UPDATE {CATEGORIZATION_RULES.full_name} SET is_active = false, updated_at = CURRENT_TIMESTAMP WHERE rule_id = ?",
                    [rule_id],
                )
                self._db.execute(
                    f"""
                    UPDATE {PROPOSED_RULES.full_name}
                    SET status = 'superseded'
                    WHERE LOWER(merchant_pattern) = LOWER(?) AND status = 'approved'
                    """,
                    [pattern],
                )
                self._db.execute(
                    f"""
                    INSERT INTO {PROPOSED_RULES.full_name}
                    (proposed_rule_id, merchant_pattern, match_type, category, subcategory,
                     status, trigger_count, source, sample_txn_ids)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'pattern_detection', ?)
                    """,
                    [
                        new_pid,
                        pattern,
                        rule_match_type,
                        new_category,
                        new_subcategory,
                        new_status,
                        winning_count,
                        sample_ids,
                    ],
                )
                self._db.execute(
                    f"""
                    INSERT INTO {RULE_DEACTIVATIONS.full_name}
                    (deactivation_id, rule_id, reason, override_count, new_category, new_subcategory)
                    VALUES (?, ?, 'override_threshold', ?, ?, ?)
                    """,
                    [
                        uuid.uuid4().hex[:12],
                        rule_id,
                        total_overrides,
                        new_category,
                        new_subcategory,
                    ],
                )
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise
            deactivated += 1

        if deactivated:
            logger.info(f"Deactivated {deactivated} auto-rule(s) due to user overrides")
        return deactivated

    # -- Read views --

    def list_pending_proposals(
        self, *, limit: int | None = None
    ) -> list[dict[str, object]]:
        """Return pending auto-rule proposals for human review.

        ``limit`` caps the number of rows returned. ``None`` returns all
        pending rows (legacy behavior used by callers that need the full
        set, e.g. ``--approve-all`` expansion).
        """
        limit_clause = "LIMIT ?" if limit is not None else ""
        params = [limit] if limit is not None else []
        try:
            rows = self._db.execute(
                f"""
                SELECT proposed_rule_id, merchant_pattern, match_type, category, subcategory,
                       trigger_count, sample_txn_ids
                FROM {PROPOSED_RULES.full_name}
                WHERE status = 'pending'
                ORDER BY trigger_count DESC, proposed_at ASC
                {limit_clause}
                """,
                params,
            ).fetchall()
        except duckdb.CatalogException:
            return []
        return [
            {
                "proposed_rule_id": r[0],
                "merchant_pattern": r[1],
                "match_type": r[2],
                "category": r[3],
                "subcategory": r[4],
                "trigger_count": r[5],
                "sample_txn_ids": list(r[6] or []),
            }
            for r in rows
        ]

    def list_active_rules(self, *, limit: int | None = None) -> list[dict[str, object]]:
        """Return active auto-rules (rows with created_by='auto_rule').

        ``limit`` defaults to ``categorization.auto_rule_list_default_limit``
        when ``None`` so callers (CLI, MCP) get a bounded result by default
        without each having to resolve the setting themselves. Note this
        diverges from ``list_pending_proposals(limit=None)`` which returns
        the full unbounded set; pair this call with ``count_active_rules()``
        when surfacing truncation to users.
        """
        effective_limit = self._resolve_list_limit(limit)
        try:
            rows = self._db.execute(
                f"""
                SELECT rule_id, merchant_pattern, match_type, category, subcategory, priority
                FROM {CATEGORIZATION_RULES.full_name}
                WHERE created_by = 'auto_rule' AND is_active = true
                ORDER BY priority ASC, rule_id
                LIMIT ?
                """,
                [effective_limit],
            ).fetchall()
        except duckdb.CatalogException:
            return []
        return [
            {
                "rule_id": r[0],
                "merchant_pattern": r[1],
                "match_type": r[2],
                "category": r[3],
                "subcategory": r[4],
                "priority": r[5],
            }
            for r in rows
        ]

    def _count_pending_proposals(self) -> int:
        """Return total count of pending proposals for has_more computation."""
        try:
            row = self._db.execute(
                f"SELECT COUNT(*) FROM {PROPOSED_RULES.full_name} WHERE status = 'pending'"
            ).fetchone()
        except duckdb.CatalogException:
            return 0
        return int(row[0]) if row else 0

    def count_active_rules(self) -> int:
        """Return total count of active auto-rules for has_more computation."""
        try:
            row = self._db.execute(
                f"SELECT COUNT(*) FROM {CATEGORIZATION_RULES.full_name} "
                "WHERE created_by = 'auto_rule' AND is_active = true"
            ).fetchone()
        except duckdb.CatalogException:
            return 0
        return int(row[0]) if row else 0

    def stats(self) -> AutoStatsResult:
        """Return counts of active auto-rules, pending proposals, and applied transactions."""

        def _scalar(sql: str) -> int:
            try:
                row = self._db.execute(sql).fetchone()
            except duckdb.CatalogException:
                return 0
            return int(row[0]) if row else 0

        return AutoStatsResult(
            active_auto_rules=self.count_active_rules(),
            pending_proposals=self._count_pending_proposals(),
            transactions_categorized=_scalar(
                f"SELECT COUNT(*) FROM {TRANSACTION_CATEGORIES.full_name} "
                "WHERE categorized_by = 'auto_rule'"
            ),
        )

    # -- Internals --

    def _extract_pattern(
        self,
        transaction_id: str,
        *,
        merchant_id: str | None = None,
        context: RecordingContext | None = None,
    ) -> tuple[str, str] | None:
        """Extract a (pattern, match_type) tuple for the given transaction.

        When a merchant is provided (or already linked via
        ``transaction_categories``), returns the merchant's
        ``(raw_pattern, match_type)`` — the substring that actually matches
        statement descriptions (e.g., 'AMZN'), not the canonical display name
        (e.g., 'Amazon'). Falls back to a normalized description with
        ``match_type='contains'`` when no merchant is associated.

        When ``context`` is provided, the description fallback reads from the
        pre-loaded ``TxnRow`` instead of querying ``fct_transactions``.
        """
        if merchant_id is None and context is None:
            # Batch path passes merchant_id explicitly; this lookup is only needed
            # on the single-item path where no context is provided.
            row = self._db.execute(
                f"SELECT merchant_id FROM {TRANSACTION_CATEGORIES.full_name} WHERE transaction_id = ?",
                [transaction_id],
            ).fetchone()
            merchant_id = str(row[0]) if row and row[0] else None
        if merchant_id:
            if context is not None:
                m_row = context.merchant_row_for(merchant_id)
                if m_row and m_row[1]:
                    return str(m_row[1]), str(m_row[2] or "contains")
            else:
                m = self._db.execute(
                    f"SELECT raw_pattern, match_type FROM {MERCHANTS.full_name} WHERE merchant_id = ?",
                    [merchant_id],
                ).fetchone()
                if m and m[0]:
                    return str(m[0]), str(m[1] or "contains")

        if context is not None:
            description = context.description_for(transaction_id)
        else:
            desc_row = self._db.execute(
                f"SELECT description FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
                [transaction_id],
            ).fetchone()
            description = str(desc_row[0]) if desc_row and desc_row[0] else None
        if not description:
            return None
        cleaned = normalize_description(description)
        if not cleaned:
            return None
        return cleaned, "contains"

    def _active_rule_covers_transaction(
        self,
        transaction_id: str,
        category: str,
        subcategory: str | None,
        *,
        context: RecordingContext | None = None,
    ) -> bool:
        """True when some active rule already matches this transaction with the same category.

        Delegates to ``CategorizationService.find_matching_rule`` so dedup uses
        canonical rule-engine semantics (contains/regex/exact + amount/account
        filters) instead of an exact-pattern string compare.

        When ``context`` is provided, passes pre-loaded rules and transaction
        data through the overrides so no DB reads are issued.
        """
        rules_override = context.active_rules if context is not None else None
        txn_row = context.txn_row_for(transaction_id) if context is not None else None
        txn_row_override = (
            (txn_row.description or "", txn_row.amount, txn_row.account_id)
            if txn_row is not None
            else None
        )
        match = self._categorization.find_matching_rule(
            transaction_id,
            rules_override=rules_override,
            txn_row_override=txn_row_override,
        )
        if match is None:
            return False
        _rule_id, matched_category, matched_subcategory, _created_by = match
        return matched_category == category and matched_subcategory == subcategory

    def _merchant_mapping_covers(
        self,
        pattern: str,
        category: str,
        subcategory: str | None,
        *,
        context: RecordingContext | None = None,
    ) -> bool:
        """True when a merchant mapping already produces this (category, subcategory) for this pattern.

        Evaluates each merchant's ``raw_pattern`` + ``match_type`` against the
        candidate pattern in Python, mirroring how the merchant matcher itself
        resolves descriptions. SQL substring comparison would miss ``exact``
        and ``regex`` merchants and ignored ``subcategory`` differences.

        When ``context`` is provided, delegates to
        ``RecordingContext.merchant_mapping_covers`` against the cached
        merchant list so no DB read is issued.
        """
        if context is not None:
            return context.merchant_mapping_covers(pattern, category, subcategory)
        try:
            rows = self._db.execute(
                f"""
                SELECT raw_pattern, match_type, category, subcategory
                FROM {MERCHANTS.full_name}
                """
            ).fetchall()
        except duckdb.CatalogException:
            return False
        for raw_pattern, m_type, m_cat, m_subcat in rows:
            if str(m_cat) != category:
                continue
            if (m_subcat if m_subcat is None else str(m_subcat)) != subcategory:
                continue
            if matches_pattern(pattern, str(raw_pattern), str(m_type or "contains")):
                return True
        return False

    def _find_pending_proposal(
        self, pattern: str, match_type: str
    ) -> tuple[str, str, str | None, int, list[str]] | None:
        """Find a pending or tracking proposal with the same (pattern, match_type)."""
        row = self._db.execute(
            f"""
            SELECT proposed_rule_id, category, subcategory, trigger_count, sample_txn_ids
            FROM {PROPOSED_RULES.full_name}
            WHERE LOWER(merchant_pattern) = LOWER(?)
              AND match_type = ?
              AND status IN ('pending', 'tracking')
            ORDER BY proposed_at DESC LIMIT 1
            """,
            [pattern, match_type],
        ).fetchone()
        if not row:
            return None
        return row[0], row[1], row[2], int(row[3]), list(row[4] or [])

    def _categorize_existing_with_rule(
        self,
        rule_id: str,
        category: str,
        subcategory: str | None,
    ) -> int:
        """Run the new rule against currently-uncategorized matching transactions. Returns count categorized.

        Defers per-transaction matching to ``CategorizationService.match_first_rule``
        against the full active-rule set so that a higher-priority user rule wins
        if both would match — without this, the auto-rule could permanently claim
        a transaction that should have been caught by the higher-priority rule
        (subsequent ``apply_rules`` runs use ``INSERT OR IGNORE`` and won't
        override). Only assigns when the priority winner is this rule.
        """
        active_rules = self._categorization.fetch_active_rules()
        scan_cap = get_settings().categorization.auto_rule_backfill_scan_cap
        rows = self._db.execute(
            f"""
            SELECT t.transaction_id, t.description, t.amount, t.account_id
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c ON t.transaction_id = c.transaction_id
            WHERE c.transaction_id IS NULL
              AND t.description IS NOT NULL
            ORDER BY t.transaction_id
            LIMIT ?
            """,
            [scan_cap],
        ).fetchall()
        matched_ids: list[str] = []
        for txn_id, description, amount, account_id in rows:
            winner = CategorizationService.match_first_rule(
                active_rules,
                str(description),
                float(amount) if amount is not None else None,
                str(account_id) if account_id is not None else None,
            )
            if winner is None:
                continue
            if winner[0] == rule_id:
                matched_ids.append(str(txn_id))
        if not matched_ids:
            return 0
        self._db.executemany(
            f"""
            INSERT OR IGNORE INTO {TRANSACTION_CATEGORIES.full_name}
            (transaction_id, category, subcategory, categorized_at, categorized_by, rule_id, confidence)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'auto_rule', ?, 1.0)
            """,
            [[mid, category, subcategory, rule_id] for mid in matched_ids],
        )
        return len(matched_ids)

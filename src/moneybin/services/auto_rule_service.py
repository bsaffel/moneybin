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
from dataclasses import dataclass, field
from typing import Any

import duckdb

from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.metrics.registry import (
    AUTO_RULE_BROAD_PENDING,
    AUTO_RULE_PATTERN_DOWNGRADED_TOTAL,
)
from moneybin.repositories.categorization_rules_repo import CategorizationRulesRepo
from moneybin.repositories.proposed_rules_repo import ProposedRulesRepo
from moneybin.services._text import build_match_inputs, normalize_description
from moneybin.services.audit_service import AuditService
from moneybin.services.categorization import (
    CategorizationService,
    Merchant,
    matches_pattern,
)
from moneybin.services.categorization._shared import resolve_category_id
from moneybin.tables import (
    CATEGORIZATION_RULES,
    FCT_TRANSACTIONS,
    MERCHANTS,
    PROPOSED_RULES,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)


def _pattern_hits(
    pattern: str, match_type: str, match_text: str, norm_desc: str, norm_memo: str
) -> bool:
    """Mirror the live matcher's field selection for a single row.

    ``build_match_inputs``' contract: ``contains`` and unanchored ``regex``
    patterns test the concatenated ``match_text`` (they may span the
    description/memo boundary); ``exact`` tests the individual normalized fields,
    whose semantics break once memo is appended. Diverging from this would make
    the reviewer's blast-radius number disagree with what the rule actually does.
    """
    if match_type == "exact":
        return matches_pattern(norm_desc, pattern, "exact") or matches_pattern(
            norm_memo, pattern, "exact"
        )
    return matches_pattern(match_text, pattern, match_type)


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
    memo: str | None = None
    source_type: str | None = None
    merchant_entity_id: str | None = None
    merchant_entity_source_type: str | None = None
    merchant_name: str | None = None


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
    merchant_mappings: list[Merchant]
    new_merchant_count: int = field(default=0)

    def txn_row_for(self, transaction_id: str) -> TxnRow | None:
        """Return the pre-loaded TxnRow for the given transaction_id, or None."""
        return self.txn_rows.get(transaction_id)

    def description_for(self, transaction_id: str) -> str | None:
        """Return the description for the given transaction_id, or None if not loaded."""
        row = self.txn_rows.get(transaction_id)
        return row.description if row else None

    def memo_for(self, transaction_id: str) -> str | None:
        """Return the memo for the given transaction_id, or None if not loaded."""
        row = self.txn_rows.get(transaction_id)
        return row.memo if row else None

    def merchant_entity_id_for(self, transaction_id: str) -> str | None:
        """Return the provider merchant_entity_id, or None if not loaded/non-Plaid."""
        row = self.txn_rows.get(transaction_id)
        return row.merchant_entity_id if row else None

    def merchant_entity_source_type_for(self, transaction_id: str) -> str | None:
        """Return the source_type of the member that issued merchant_entity_id, or None.

        This is the entity-paired source_type — the binding key for merchant
        resolution — NOT the merge-winner ``canonical_source_type``.
        """
        row = self.txn_rows.get(transaction_id)
        return row.merchant_entity_source_type if row else None

    def merchant_name_for(self, transaction_id: str) -> str | None:
        """Return the provider merchant_name for the given transaction_id, or None."""
        row = self.txn_rows.get(transaction_id)
        return row.merchant_name if row else None

    def merchant_row_for(self, merchant_id: str) -> Merchant | None:
        """Return the cached merchant row for the given merchant_id, or None."""
        for merchant in self.merchant_mappings:
            if merchant.merchant_id == merchant_id:
                return merchant
        return None

    def register_new_merchant(self, merchant_row: Merchant) -> None:
        """Insert at the canonical match-order position (before the first regex).

        Preserves the ordering from ``_fetch_merchants``
        (oneOf → exact → contains → regex). Exemplar-only merchants are
        inserted at the front so they fire before pattern-based shapes for
        subsequent items in the same bulk batch.
        """
        if merchant_row.match_type == "oneOf":
            insert_at = 0
        else:
            insert_at = next(
                (
                    i
                    for i, m in enumerate(self.merchant_mappings)
                    if m.match_type == "regex"
                ),
                len(self.merchant_mappings),
            )
        self.merchant_mappings.insert(insert_at, merchant_row)
        self.new_merchant_count += 1

    def merchant_mapping_covers(
        self, pattern: str, category: str, subcategory: str | None
    ) -> bool:
        """Mirror of ``AutoRuleService._merchant_mapping_covers`` against the cached list."""
        for merchant in self.merchant_mappings:
            if merchant.category != category:
                continue
            if merchant.subcategory != subcategory:
                continue
            if merchant.match_type == "oneOf" or merchant.raw_pattern is None:
                # Exemplar-only merchants cover exact match_text values rather
                # than patterns; pattern-coverage doesn't apply.
                continue
            if matches_pattern(
                pattern, merchant.raw_pattern, merchant.match_type or "contains"
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

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Bind the service to a database connection.

        ``audit`` is keyword-only so existing positional callers are unchanged.
        The rules + proposed-rules repos share it so every lifecycle mutation
        emits a paired audit row (Invariant 10).
        """
        self._db = db
        self._cat_service: CategorizationService | None = None
        self._audit = audit if audit is not None else AuditService(db)
        self._rules = CategorizationRulesRepo(db, audit=self._audit)
        self._proposed = ProposedRulesRepo(db, audit=self._audit)

    @property
    def _categorization(self) -> CategorizationService:
        if self._cat_service is None:
            # Forward the shared AuditService so backfill categorizations in
            # approve() route through the injected audit, not a fresh instance
            # (keeps the AutoRuleService(db, audit=...) injection contract whole).
            self._cat_service = CategorizationService(self._db, audit=self._audit)
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
        # Manual-source exemption: per transaction-curation spec Req 7, user
        # categorizations on manual rows do not seed auto-rule training. The
        # check happens here so neither the proposal-creation path nor the
        # increment path observes manual rows.
        if self._is_manual_source(transaction_id, context=context):
            return None
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

        # Threads the supersede audit id onto the new insert when a
        # different-category proposal replaces an existing one (Req 5 cascade);
        # stays None for a fresh proposal.
        supersede_parent: str | None = None
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
                # Re-resolve to heal rows whose FK was NULL at V014 backfill
                # time (target category created after the proposal first
                # landed) — without this, later FK-based cascades miss them.
                category_id = resolve_category_id(self._db, category, subcategory)
                self._proposed.reinforce(
                    proposed_rule_id,
                    trigger_count=new_count,
                    sample_txn_ids=new_samples,
                    status=new_status,
                    category_id=category_id,
                    actor="auto_rule_service",
                )
                return proposed_rule_id
            # Different category: supersede the old proposal, then create a new
            # one below — one user action, so the new insert threads the
            # supersede's audit id (Req 5 cascade).
            supersede_parent = self._proposed.supersede(
                proposed_rule_id, actor="auto_rule_service"
            ).audit_id

        initial_status = "pending" if threshold <= 1 else "tracking"
        category_id = resolve_category_id(self._db, category, subcategory)
        event = self._proposed.insert(
            merchant_pattern=pattern,
            match_type=match_type,
            category=category,
            subcategory=subcategory,
            category_id=category_id,
            status=initial_status,
            sample_txn_ids=[transaction_id],
            actor="auto_rule_service",
            parent_audit_id=supersede_parent,
        )
        return event.target_id

    # -- Decisions --

    def _estimate_match_counts(self, proposals: list[dict[str, Any]]) -> dict[str, int]:
        """Return ``{proposed_rule_id: estimated_match_count}``.

        The blast radius a reviewer sees must be the blast radius the rule will
        have, so this uses the matcher's own predicate (``matches_pattern`` over
        ``build_match_inputs``) rather than an approximation.
        ``normalize_description`` is a Python regex chain with no faithful SQL
        equivalent, so the text is normalized here rather than in the query.

        ONE scan, normalized once, then an in-memory pass per proposal. Do NOT
        rewrite this as a per-proposal query against ``core.fct_transactions``:
        that view is the full merge/dedup/categorization pipeline, and
        re-evaluating it once per item is exactly what made ``system_doctor``
        hang for >73s (F14).
        """
        if not proposals:
            return {}
        try:
            rows = self._db.execute(
                f"SELECT description, memo FROM {FCT_TRANSACTIONS.full_name}"  # noqa: S608  # TableRef constant
            ).fetchall()
        except duckdb.CatalogException:
            # Pre-first-import: no fact table, so nothing can match.
            return {str(p["proposed_rule_id"]): 0 for p in proposals}

        match_inputs = [build_match_inputs(r[0], r[1]) for r in rows]
        counts: dict[str, int] = {}
        for p in proposals:
            pid = str(p["proposed_rule_id"])
            pattern = str(p.get("merchant_pattern") or "")
            match_type = str(p.get("match_type") or "contains")
            if not pattern:
                counts[pid] = 0
                continue
            counts[pid] = sum(
                1
                for match_text, norm_desc, norm_memo in match_inputs
                if _pattern_hits(pattern, match_type, match_text, norm_desc, norm_memo)
            )
        return counts

    @staticmethod
    def _is_broad(estimated_match_count: int, trigger_count: int) -> bool:
        """Return True when a proposal's blast radius outruns its evidence.

        Two conditions, both required. The floor keeps the guard from crying wolf
        on small rules; the ratio scales the bar with the amount of evidence
        behind the proposal, so a pattern earns a wider reach as the user
        confirms it more often.
        """
        settings = get_settings().categorization
        if estimated_match_count < settings.auto_rule_broad_match_min:
            return False
        return estimated_match_count > settings.auto_rule_broad_match_factor * max(
            trigger_count, 1
        )

    def review(self, *, limit: int | None = None) -> AutoReviewResult:
        """Return pending auto-rule proposals, each carrying its blast radius.

        ``limit`` defaults to ``categorization.auto_rule_list_default_limit``
        when ``None``. ``total_count`` reflects the unbounded queue size so
        callers see ``has_more`` when truncation occurs.

        Every proposal carries ``estimated_match_count`` (how many transactions
        the rule would actually hit) and ``is_broad``. A broad proposal cannot be
        accepted without an explicit ``allow_broad`` override — see
        :meth:`approve`.
        """
        effective_limit = self._resolve_list_limit(limit)
        # Re-annotated Any: list_pending_proposals declares dict[str, object],
        # but this loop reads trigger_count back out with int(); Any lets the
        # per-field int()/str() coercions below type-check like they already
        # do for the same dicts once they cross into AutoReviewResult.
        proposals: list[dict[str, Any]] = self.list_pending_proposals(
            limit=effective_limit
        )
        counts = self._estimate_match_counts(proposals)
        broad_count = 0
        for p in proposals:
            pid = str(p["proposed_rule_id"])
            estimated = counts.get(pid, 0)
            broad = self._is_broad(estimated, int(p.get("trigger_count", 0) or 0))
            p["estimated_match_count"] = estimated
            p["is_broad"] = broad
            if broad:
                broad_count += 1
        AUTO_RULE_BROAD_PENDING.set(broad_count)
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
        *,
        actor: str = "system",
    ) -> AutoConfirmResult:
        """Accept and/or reject pending proposals; returns aggregate counts.

        IDs appearing in both lists are dropped from ``accept`` so an explicit
        reject always wins. The CLI does the same dedup before calling, but
        applying it here keeps direct service callers (MCP, scripts) safe.
        ``actor`` is threaded onto the audit rows (CLI/MCP pass their surface).
        """
        approve_set = set(accept or [])
        reject_set = set(reject or [])
        approve_set -= reject_set
        a = self.approve(sorted(approve_set), actor=actor)
        r = self.reject(sorted(reject_set), actor=actor)
        return AutoConfirmResult(
            approved=a.approved,
            rejected=r.rejected,
            skipped=a.skipped + r.skipped,
            newly_categorized=a.newly_categorized,
            rule_ids=a.rule_ids,
        )

    def approve(
        self, proposed_rule_ids: list[str], *, actor: str = "system"
    ) -> ApproveResult:
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
            # Wrap rule INSERT, proposal UPDATE, and back-fill in a single
            # transaction so a partial failure (e.g., interrupt between steps)
            # cannot leave an active rule whose source proposal is still
            # 'pending' — which would let approve() create a duplicate rule on
            # retry. The two repo writes join this outer txn (in_outer_txn=True;
            # DuckDB has no nested txns), and the proposal approve threads the
            # rule-insert's audit id so the promotion is one chain (Req 5).
            # Phase 1 dual-write: re-resolve the FK from the text snapshot at
            # write time (the proposed_rule's own category_id may have been NULL
            # during V014 backfill if the target category didn't yet exist).
            category_id = resolve_category_id(self._db, category, subcategory)
            self._db.begin()
            try:
                rule_event = self._rules.insert(
                    name=f"auto: {pattern}",
                    merchant_pattern=pattern,
                    match_type=match_type,
                    min_amount=None,
                    max_amount=None,
                    account_id=None,
                    category=category,
                    subcategory=subcategory,
                    category_id=category_id,
                    priority=settings.auto_rule_default_priority,
                    created_by="auto_rule",
                    actor=actor,
                    in_outer_txn=True,
                )
                rule_id = rule_event.target_id
                if rule_id is None:  # pragma: no cover — insert always sets the id
                    raise RuntimeError("CategorizationRulesRepo.insert returned no id")
                # Link rule_id onto the proposal (FK-keyed; check_overrides
                # supersedes via rule_id), sharing the rule insert's audit id.
                self._proposed.mark_approved(
                    pid,
                    rule_id=rule_id,
                    actor=actor,
                    parent_audit_id=rule_event.audit_id,
                    in_outer_txn=True,
                )
                newly = self._categorize_existing_with_rule(
                    rule_id, category, subcategory
                )
                self._db.commit()
            except BaseException:
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

    def reject(
        self, proposed_rule_ids: list[str], *, actor: str = "system"
    ) -> RejectResult:
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
            self._proposed.mark_rejected(pid, actor=actor)
            result.rejected += 1
        return result

    # -- Override detection --

    def check_overrides(self) -> int:
        """Deactivate auto-rules with override count >= configured threshold; return number deactivated.

        An override = a transaction whose canonical match_text (description +
        memo, normalized) matches the auto-rule's pattern but is currently
        categorized by 'user' with a different category. When the threshold is
        reached, deactivate the rule via ``CategorizationRulesRepo`` (which
        emits a ``categorization_rule.deactivate`` audit row with the override
        forensics in ``context``).
        """
        settings = get_settings().categorization
        threshold = settings.auto_rule_override_threshold
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
            # Pattern matching uses ``matches_pattern`` against the canonical
            # ``match_text`` (description + memo, normalized) — same input as
            # every other matcher per the spec's parity principle. SQL-only
            # matching cannot reproduce ``normalize_description``.
            # Manual-source exemption: per transaction-curation spec Req 7,
            # user categorizations on manual rows do not feed auto-rule
            # training. Manual rows are user-curated by definition; using
            # them as override evidence would conflate authoring intent with
            # rule-deactivation signal.
            candidate_rows = self._db.execute(
                f"""
                SELECT c.transaction_id, t.description, t.memo, c.category,
                       c.subcategory, c.categorized_at
                FROM {TRANSACTION_CATEGORIES.full_name} c
                JOIN {FCT_TRANSACTIONS.full_name} t ON c.transaction_id = t.transaction_id
                WHERE c.categorized_by IN ('user', 'ai')
                  AND c.categorized_at > ?
                  AND (
                    (t.description IS NOT NULL AND t.description != '')
                    OR (t.memo IS NOT NULL AND t.memo != '')
                  )
                  AND t.source_type != 'manual'
                  AND (
                    c.category != ?
                    OR COALESCE(c.subcategory, '') != COALESCE(?, '')
                  )
                ORDER BY c.categorized_at DESC
                """,
                [rule_created_at, rule_category, rule_subcategory],
            ).fetchall()
            override_count = 0
            sample_ids: list[str] = []
            for txn_id, description, memo, _c_cat, _c_subcat, _at in candidate_rows:
                match_text, norm_desc, norm_memo = build_match_inputs(
                    str(description) if description else None,
                    str(memo) if memo else None,
                )
                candidates = [t for t in (match_text, norm_desc, norm_memo) if t]
                if not any(
                    matches_pattern(c, pattern, rule_match_type) for c in candidates
                ):
                    continue
                override_count += 1
                if len(sample_ids) < sample_cap:
                    sample_ids.append(str(txn_id))
            if override_count < threshold:
                continue

            # Deactivate via the repo so the write + paired audit row commit
            # atomically with a full before/after row (Req 4). The override
            # forensics live in `context` (the deactivation reason +
            # evidence), keeping the audit action taxonomy-conformant
            # (categorization_rule.deactivate) for both manual and override
            # paths.
            # REVISIT: re-proposal (insert a new pattern_detection rule shaped
            # by the winning override bucket) is intentionally omitted — the
            # replacement-rule heuristic lacks production signal.
            self._rules.deactivate(
                rule_id,
                actor="auto_rule_service",
                context={
                    "reason": "override_threshold",
                    "override_count": override_count,
                    "sample_ids": sample_ids,
                },
            )
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

    def _is_manual_source(
        self,
        transaction_id: str,
        *,
        context: RecordingContext | None = None,
    ) -> bool:
        """Return True if the transaction's source_type is 'manual'.

        Used to enforce the auto-rule training exemption for manual rows
        (transaction-curation spec Req 7). When a bulk context is provided,
        ``source_type`` comes from the cached ``TxnRow`` so the bulk path
        issues no extra queries; otherwise we read it directly.
        """
        if context is not None:
            row = context.txn_row_for(transaction_id)
            return bool(row) and row.source_type == "manual"
        db_row = self._db.execute(
            f"SELECT source_type FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
            [transaction_id],
        ).fetchone()
        return bool(db_row) and db_row[0] == "manual"

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
                if m_row and m_row.raw_pattern:
                    return m_row.raw_pattern, m_row.match_type or "contains"
            else:
                m = self._db.execute(
                    f"SELECT raw_pattern, match_type FROM {MERCHANTS.full_name} WHERE merchant_id = ?",
                    [merchant_id],
                ).fetchone()
                if m and m[0]:
                    return str(m[0]), str(m[1] or "contains")

        if context is not None:
            description = context.description_for(transaction_id)
            memo = context.memo_for(transaction_id)
        else:
            desc_row = self._db.execute(
                f"SELECT description, memo FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
                [transaction_id],
            ).fetchone()
            description = str(desc_row[0]) if desc_row and desc_row[0] else None
            memo = str(desc_row[1]) if desc_row and desc_row[1] else None
        # Description is the preferred pattern source — it carries the merchant
        # identity for most sources. Memo is the fallback for OFX-style rows
        # where the merchant name is wrapped into the memo field.
        cleaned_desc = normalize_description(description) if description else ""
        if cleaned_desc:
            return cleaned_desc, self._invented_match_type(cleaned_desc)
        cleaned_memo = normalize_description(memo) if memo else ""
        if cleaned_memo:
            return cleaned_memo, self._invented_match_type(cleaned_memo)
        return None

    @staticmethod
    def _invented_match_type(pattern: str) -> str:
        """Return the match type for a pattern the machine invented.

        Only the description/memo fallback reaches here. A merchant's
        ``raw_pattern`` is user-authored and returns above, untouched — this
        guards the inference, not the human.

        ``normalize_description`` can reduce a description to a 1-2 character
        token (a truncated "TRANSFER TO ..." becomes "TO"). As a ``contains``
        rule that matches STORE, AUTO, TOTAL; one accepted proposal then
        relabels those rows Internal Transfer on the next categorize_run, and a
        Transfer label drops them out of spend reports entirely. Below the floor
        we propose ``exact`` instead: the user's evidence is kept, but the rule
        can only fire on a description that IS the token.
        """
        min_len = get_settings().categorization.auto_rule_min_contains_length
        if len(pattern) < min_len:
            AUTO_RULE_PATTERN_DOWNGRADED_TOTAL.inc()
            return "exact"
        return "contains"

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
            (
                txn_row.description or "",
                txn_row.amount,
                txn_row.account_id,
                txn_row.memo,
            )
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
            if raw_pattern is None or m_type == "oneOf":
                # Exemplar-only merchants don't participate in pattern coverage.
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
            SELECT t.transaction_id, t.description, t.amount, t.account_id, t.memo
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c ON t.transaction_id = c.transaction_id
            WHERE c.transaction_id IS NULL
              AND (t.description IS NOT NULL OR t.memo IS NOT NULL)
            ORDER BY t.transaction_id
            LIMIT ?
            """,
            [scan_cap],
        ).fetchall()
        # Route every write through write_categorization so the source-priority
        # guard (categorization-matching-mechanics.md §Source precedence) fires
        # on the auto-rule backfill path; a direct INSERT would let auto_rule
        # silently overwrite a higher-priority existing categorization.
        applied = 0
        for txn_id, description, amount, account_id, memo in rows:
            winner = CategorizationService.match_first_rule(
                active_rules,
                str(description) if description else "",
                float(amount) if amount is not None else None,
                str(account_id) if account_id is not None else None,
                str(memo) if memo else None,
            )
            if winner is None:
                continue
            if winner[0] != rule_id:
                continue
            outcome = self._categorization.write_categorization(
                transaction_id=str(txn_id),
                category=category,
                subcategory=subcategory,
                categorized_by="auto_rule",
                rule_id=rule_id,
                confidence=1.0,
                # Runs inside approve()'s open transaction (DuckDB has no nested
                # txns), so the repo joins it rather than opening its own.
                in_outer_txn=True,
            )
            if outcome.written:
                applied += 1
        return applied

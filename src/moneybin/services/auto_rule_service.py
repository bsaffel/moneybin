"""Auto-rule lifecycle service: pattern extraction, proposal tracking, override detection.

Observes user/AI categorizations recorded via ``CategorizationService.bulk_categorize``,
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

import duckdb

from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.services.categorization_service import (
    CategorizationService,
    normalize_description,
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


class AutoRuleService:
    """Auto-rule lifecycle: observe → propose → approve/reject → deactivate.

    All public methods are independent of ``CategorizationService``'s public
    surface; callers wire them up through their own command/tool layer (CLI,
    MCP). Hooks called from ``CategorizationService.bulk_categorize`` use a
    lazy import on the categorization side to keep imports one-directional.
    """

    def __init__(self, db: Database) -> None:
        """Bind the service to a database connection."""
        self._db = db

    # -- Observation --

    def record_categorization(
        self,
        transaction_id: str,
        category: str,
        *,
        subcategory: str | None = None,
    ) -> str | None:
        """Record a categorization event for auto-rule learning.

        Returns the proposed_rule_id if a proposal was created or updated,
        ``None`` if the categorization was filtered out (already covered by
        an active rule, by a merchant mapping with no in-progress proposal,
        or no extractable pattern).
        """
        extracted = self._extract_pattern(transaction_id)
        if not extracted:
            return None
        pattern, match_type = extracted

        if self._active_rule_covers_transaction(transaction_id, category, subcategory):
            return None

        settings = get_settings().categorization
        threshold = settings.auto_rule_proposal_threshold
        sample_cap = settings.auto_rule_sample_txn_cap
        existing = self._find_pending_proposal(pattern)

        # Merchant coverage is only a reason to skip when there is no
        # in-progress proposal for this pattern. Otherwise tracking proposals
        # could be permanently stuck below threshold once bulk_categorize
        # creates the merchant mapping during the first categorization.
        if existing is None and self._merchant_mapping_covers(pattern, category):
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
                new_samples = (
                    samples + [transaction_id]
                    if transaction_id not in samples
                    else samples
                )
                # Keep the most recent samples when over capacity — appended IDs
                # are newest, so a head-slice would silently drop the new txn.
                new_samples = new_samples[-sample_cap:]
                new_count = count + 1
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

    def confirm(
        self,
        approve: list[str] | None = None,
        reject: list[str] | None = None,
    ) -> dict[str, object]:
        """Approve and/or reject pending proposals; returns aggregate counts."""
        a = self.approve(approve or [])
        r = self.reject(reject or [])
        return {
            "approved": a.approved,
            "newly_categorized": a.newly_categorized,
            "rule_ids": a.rule_ids,
            "rejected": r.rejected,
            "skipped": a.skipped + r.skipped,
        }

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
                    rule_id, pattern, match_type, category, subcategory
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
        # bulk_categorize calls this on every batch, so a one-row probe avoids
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
            # Description match honors the rule's own match_type so override
            # counting matches how the rule engine actually selects rows.
            match_sql = self._description_match_sql(rule_match_type)
            rows = self._db.execute(
                f"""
                SELECT c.category, c.subcategory, COUNT(*) AS n
                FROM {TRANSACTION_CATEGORIES.full_name} c
                JOIN {FCT_TRANSACTIONS.full_name} t ON c.transaction_id = t.transaction_id
                WHERE c.categorized_by IN ('user', 'ai')
                  AND c.categorized_at > ?
                  AND (
                    c.category != ?
                    OR COALESCE(c.subcategory, '') != COALESCE(?, '')
                  )
                  AND {match_sql}
                GROUP BY c.category, c.subcategory
                ORDER BY n DESC
                """,
                [rule_created_at, rule_category, rule_subcategory, pattern],
            ).fetchall()
            total_overrides = sum(r[2] for r in rows)
            if total_overrides < threshold:
                continue

            new_category = rows[0][0]
            new_subcategory = rows[0][1]
            sample_rows = self._db.execute(
                f"""
                SELECT c.transaction_id
                FROM {TRANSACTION_CATEGORIES.full_name} c
                JOIN {FCT_TRANSACTIONS.full_name} t ON c.transaction_id = t.transaction_id
                WHERE c.categorized_by IN ('user', 'ai')
                  AND c.categorized_at > ?
                  AND c.category = ?
                  AND COALESCE(c.subcategory, '') = COALESCE(?, '')
                  AND {match_sql}
                ORDER BY c.categorized_at DESC
                LIMIT ?
                """,
                [
                    rule_created_at,
                    new_category,
                    new_subcategory,
                    pattern,
                    sample_cap,
                ],
            ).fetchall()
            sample_ids = [r[0] for r in sample_rows]
            new_status = (
                "pending" if total_overrides >= proposal_threshold else "tracking"
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
                        total_overrides,
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

    def list_pending_proposals(self) -> list[dict[str, object]]:
        """Return all pending auto-rule proposals for human review."""
        try:
            rows = self._db.execute(
                f"""
                SELECT proposed_rule_id, merchant_pattern, match_type, category, subcategory,
                       trigger_count, sample_txn_ids
                FROM {PROPOSED_RULES.full_name}
                WHERE status = 'pending'
                ORDER BY trigger_count DESC, proposed_at ASC
                """
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

    def list_active_rules(self) -> list[dict[str, object]]:
        """Return active auto-rules (rows with created_by='auto_rule')."""
        try:
            rows = self._db.execute(
                f"""
                SELECT rule_id, merchant_pattern, match_type, category, subcategory, priority
                FROM {CATEGORIZATION_RULES.full_name}
                WHERE created_by = 'auto_rule' AND is_active = true
                ORDER BY priority ASC, rule_id
                """
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

    def stats(self) -> dict[str, int]:
        """Return counts of active auto-rules, pending proposals, and applied transactions."""

        def _scalar(sql: str) -> int:
            try:
                row = self._db.execute(sql).fetchone()
            except duckdb.CatalogException:
                return 0
            return int(row[0]) if row else 0

        active = _scalar(
            f"SELECT COUNT(*) FROM {CATEGORIZATION_RULES.full_name} "
            "WHERE created_by = 'auto_rule' AND is_active = true"
        )
        pending = _scalar(
            f"SELECT COUNT(*) FROM {PROPOSED_RULES.full_name} WHERE status = 'pending'"
        )
        applied = _scalar(
            f"SELECT COUNT(*) FROM {TRANSACTION_CATEGORIES.full_name} "
            "WHERE categorized_by = 'auto_rule'"
        )
        return {
            "active_auto_rules": active,
            "pending_proposals": pending,
            "transactions_categorized": applied,
        }

    # -- Internals --

    def _extract_pattern(self, transaction_id: str) -> tuple[str, str] | None:
        """Extract a (pattern, match_type) tuple for the given transaction.

        When the transaction has a resolved merchant_id, returns the merchant's
        ``(raw_pattern, match_type)`` — the substring that actually matches statement
        descriptions (e.g., 'AMZN'), not the canonical display name (e.g., 'Amazon').
        Falls back to a normalized description with ``match_type='contains'`` when
        no merchant is associated.
        """
        row = self._db.execute(
            f"SELECT merchant_id FROM {TRANSACTION_CATEGORIES.full_name} WHERE transaction_id = ?",
            [transaction_id],
        ).fetchone()
        merchant_id = row[0] if row else None
        if merchant_id:
            m = self._db.execute(
                f"SELECT raw_pattern, match_type FROM {MERCHANTS.full_name} WHERE merchant_id = ?",
                [merchant_id],
            ).fetchone()
            if m and m[0]:
                return str(m[0]), str(m[1] or "contains")

        desc_row = self._db.execute(
            f"SELECT description FROM {FCT_TRANSACTIONS.full_name} WHERE transaction_id = ?",
            [transaction_id],
        ).fetchone()
        if not desc_row or not desc_row[0]:
            return None
        cleaned = normalize_description(str(desc_row[0]))
        if not cleaned:
            return None
        return cleaned, "contains"

    def _active_rule_covers_transaction(
        self,
        transaction_id: str,
        category: str,
        subcategory: str | None,
    ) -> bool:
        """True when some active rule already matches this transaction with the same category.

        Delegates to ``CategorizationService.find_matching_rule`` so dedup uses
        canonical rule-engine semantics (contains/regex/exact + amount/account
        filters) instead of an exact-pattern string compare.
        """
        match = CategorizationService(self._db).find_matching_rule(transaction_id)
        if match is None:
            return False
        _rule_id, matched_category, matched_subcategory, _created_by = match
        return matched_category == category and matched_subcategory == subcategory

    def _merchant_mapping_covers(self, pattern: str, category: str) -> bool:
        """True when a merchant mapping already produces this category for this pattern.

        Compares ``raw_pattern`` (the field actually used to match descriptions)
        rather than ``canonical_name`` (a display label). For description-derived
        patterns we additionally check whether the pattern contains the merchant's
        raw_pattern as a substring.
        """
        try:
            row = self._db.execute(
                f"""
                SELECT 1 FROM {MERCHANTS.full_name}
                WHERE category = ?
                  AND (
                    LOWER(raw_pattern) = LOWER(?)
                    OR POSITION(LOWER(raw_pattern) IN LOWER(?)) > 0
                  )
                LIMIT 1
                """,
                [category, pattern, pattern],
            ).fetchone()
        except duckdb.CatalogException:
            return False
        return row is not None

    def _find_pending_proposal(
        self, pattern: str
    ) -> tuple[str, str, str | None, int, list[str]] | None:
        row = self._db.execute(
            f"""
            SELECT proposed_rule_id, category, subcategory, trigger_count, sample_txn_ids
            FROM {PROPOSED_RULES.full_name}
            WHERE LOWER(merchant_pattern) = LOWER(?) AND status IN ('pending', 'tracking')
            ORDER BY proposed_at DESC LIMIT 1
            """,
            [pattern],
        ).fetchone()
        if not row:
            return None
        return row[0], row[1], row[2], int(row[3]), list(row[4] or [])

    @staticmethod
    def _description_match_sql(match_type: str) -> str:
        """SQL fragment matching ``?`` against ``t.description`` per match_type."""
        if match_type == "exact":
            return "LOWER(t.description) = LOWER(?)"
        if match_type == "regex":
            return "regexp_matches(t.description, ?)"
        return "POSITION(LOWER(?) IN LOWER(t.description)) > 0"

    def _categorize_existing_with_rule(
        self,
        rule_id: str,
        pattern: str,
        match_type: str,
        category: str,
        subcategory: str | None,
    ) -> int:
        """Run the new rule against currently-uncategorized matching transactions. Returns count categorized."""
        match_sql = self._description_match_sql(match_type)
        rows = self._db.execute(
            f"""
            SELECT t.transaction_id
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c ON t.transaction_id = c.transaction_id
            WHERE c.transaction_id IS NULL
              AND t.description IS NOT NULL
              AND {match_sql}
            """,
            [pattern],
        ).fetchall()
        if not rows:
            return 0
        self._db.executemany(
            f"""
            INSERT OR IGNORE INTO {TRANSACTION_CATEGORIES.full_name}
            (transaction_id, category, subcategory, categorized_at, categorized_by, rule_id, confidence)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'auto_rule', ?, 1.0)
            """,
            [[r[0], category, subcategory, rule_id] for r in rows],
        )
        return len(rows)

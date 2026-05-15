"""Transaction matching orchestrator.

Runs Tier 2b (within-source overlap) then Tier 3 (cross-source) matching.
Each tier: blocking -> scoring -> 1:1 assignment -> persist decisions.
"""

import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching import UNIONED_TABLE
from moneybin.matching.assignment import assign_greedy
from moneybin.matching.persistence import (
    MatchStatus,
    MatchTier,
    MatchType,
    create_match_decision,
    get_rejected_pairs,
)
from moneybin.matching.scoring import (
    CandidatePair,
    get_candidates_cross_source,
    get_candidates_within_source,
)
from moneybin.matching.transfer import (
    get_candidates_transfers,
)
from moneybin.metrics.registry import (
    DEDUP_MATCH_CONFIDENCE,
    DEDUP_MATCHES_TOTAL,
    DEDUP_PAIRS_SCORED,
    DEDUP_REVIEW_PENDING,
    TRANSFER_MATCH_CONFIDENCE,
    TRANSFER_MATCHES_PROPOSED,
    TRANSFER_PAIRS_SCORED,
)

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Summary of a matching run."""

    auto_merged: int = 0
    pending_review: int = 0
    pending_transfers: int = 0

    @property
    def has_matches(self) -> bool:
        """True if any matches (auto-merged or pending) were found."""
        return (
            self.auto_merged > 0
            or self.pending_review > 0
            or self.pending_transfers > 0
        )

    @property
    def has_pending(self) -> bool:
        """True if any matches are awaiting user review."""
        return self.pending_review > 0 or self.pending_transfers > 0

    def summary(self) -> str:
        """Return a human-readable summary of the matching run."""
        parts: list[str] = []
        if self.auto_merged:
            parts.append(f"{self.auto_merged} auto-merged")
        if self.pending_review:
            parts.append(f"{self.pending_review} pending review")
        if self.pending_transfers:
            parts.append(f"{self.pending_transfers} potential transfers")
        if not parts:
            return "No new matches found"
        return ", ".join(parts)


@dataclass
class _DedupDecisions:
    """Both projections derived from a single active-dedup-decisions query."""

    matched_ids: set[tuple[str, str]]
    secondary_ids: set[tuple[str, str, str]]


class TransactionMatcher:
    """Orchestrates transaction matching across tiers."""

    def __init__(
        self,
        db: Database,
        settings: MatchingSettings,
        *,
        table: str = UNIONED_TABLE,
    ) -> None:
        """Initialize the matcher with a database connection, settings, and source table."""
        self._db = db
        self._settings = settings
        self._table = table

    def run(self, *, auto_accept_transfers: bool = False) -> MatchResult:
        """Run Tier 2b then Tier 3 matching.

        ``auto_accept_transfers`` writes transfer matches as ``accepted`` instead
        of ``pending``, simulating automated human review. Used by the scenario
        runner so transfer evaluations can read from ``core.bridge_transfers``
        without an interactive review step.
        """
        result = MatchResult()
        rejected = get_rejected_pairs(self._db)

        # Fetch pre-existing dedup decisions before tier 2b so the candidate
        # functions can exclude already-matched pairs. The candidate functions
        # filter excluded_ids in Python (not SQL), so this call must precede any
        # tier that uses already_matched. A second call after tiers picks up
        # decisions created in this run for the secondary-ids computation.
        already_matched = self._fetch_active_dedup_decisions().matched_ids

        # Tier 2b: within-source overlap (high-confidence only)
        tier_2b_matched = self._run_tier(
            tier="2b",
            candidates_fn=lambda excluded: get_candidates_within_source(
                self._db,
                table=self._table,
                date_window_days=self._settings.date_window_days,
                excluded_ids=excluded,
                rejected_pairs=rejected,
            ),
            excluded_ids=already_matched,
            result=result,
        )
        already_matched.update(tier_2b_matched)

        # Tier 3: cross-source
        tier_3_matched = self._run_tier(
            tier="3",
            candidates_fn=lambda excluded: get_candidates_cross_source(
                self._db,
                table=self._table,
                date_window_days=self._settings.date_window_days,
                excluded_ids=excluded,
                rejected_pairs=rejected,
            ),
            excluded_ids=already_matched,
            result=result,
        )
        already_matched.update(tier_3_matched)

        # Tier 4: transfer detection (runs after dedup).
        # Exclude transactions in active transfers AND the non-primary side of
        # each dedup group. Without this, duplicate source rows (e.g., csv_chk1
        # and ofx_chk1 both deduped) can each form separate transfer proposals
        # that resolve to the same merged transaction pair in bridge_transfers.
        # Re-query after tiers so decisions created in this run are included.
        transfer_excluded = self._get_transfer_matched_ids()
        transfer_excluded |= self._fetch_active_dedup_decisions().secondary_ids
        rejected_transfer = get_rejected_pairs(self._db, match_type="transfer")

        self._run_transfer_tier(
            excluded_ids=transfer_excluded,
            rejected_pairs=rejected_transfer,
            result=result,
            auto_accept=auto_accept_transfers,
        )

        return result

    def _classify_pair(
        self, pair: CandidatePair, tier: MatchTier
    ) -> tuple[MatchStatus, str] | None:
        """Return (status, decided_by) if pair should be persisted, None to skip.

        The tier-3-only review-threshold branch lives exclusively here so it can
        be tested without a database fixture.
        """
        if pair.confidence_score >= self._settings.high_confidence_threshold:
            return ("accepted", "auto")
        if tier == "3" and pair.confidence_score >= self._settings.review_threshold:
            return ("pending", "auto")
        return None

    def _persist_dedup_match(
        self,
        pair: CandidatePair,
        tier: MatchTier,
        status: MatchStatus,
        decided_by: str,
    ) -> None:
        """Write one dedup match decision to the database."""
        match_id = uuid.uuid4().hex[:12]
        create_match_decision(
            self._db,
            match_id=match_id,
            source_transaction_id_a=pair.source_transaction_id_a,
            source_type_a=pair.source_type_a,
            source_origin_a=pair.source_origin_a,
            source_transaction_id_b=pair.source_transaction_id_b,
            source_type_b=pair.source_type_b,
            source_origin_b=pair.source_origin_b,
            account_id=pair.account_id,
            confidence_score=pair.confidence_score,
            match_signals={
                "date_distance": pair.date_distance_days,
                "description_similarity": round(pair.description_similarity, 4),
            },
            match_tier=tier,
            match_status=status,
            decided_by=decided_by,
            match_reason=(
                f"Amount match, {pair.date_distance_days}d apart, "
                f"desc similarity {pair.description_similarity:.2f}"
            ),
        )

    def _run_tier(
        self,
        *,
        tier: MatchTier,
        candidates_fn: Callable[[set[tuple[str, str]]], list[CandidatePair]],
        excluded_ids: set[tuple[str, str]],
        result: MatchResult,
    ) -> set[tuple[str, str]]:
        """Run blocking -> scoring -> assignment -> persist for one tier."""
        candidates: list[CandidatePair] = candidates_fn(excluded_ids)
        DEDUP_PAIRS_SCORED.inc(len(candidates))

        if not candidates:
            return set()

        assigned = assign_greedy(candidates)
        newly_matched: set[tuple[str, str]] = set()
        tier_merged = 0
        tier_pending = 0

        for pair in assigned:
            DEDUP_MATCH_CONFIDENCE.observe(pair.confidence_score)

            classification = self._classify_pair(pair, tier)
            if classification is None:
                continue
            status, decided_by = classification

            if status == "accepted":
                result.auto_merged += 1
                tier_merged += 1
                DEDUP_MATCHES_TOTAL.labels(match_tier=tier, decided_by="auto").inc()
            else:
                result.pending_review += 1
                tier_pending += 1
                DEDUP_REVIEW_PENDING.inc()

            self._persist_dedup_match(pair, tier, status, decided_by)
            newly_matched.add((pair.source_transaction_id_a, pair.account_id))
            newly_matched.add((pair.source_transaction_id_b, pair.account_id))

        if tier_merged or tier_pending:
            logger.info(
                f"Tier {tier}: {tier_merged} auto-merged, {tier_pending} pending review"
            )

        return newly_matched

    def _fetch_active_dedup_decisions(self) -> _DedupDecisions:
        """Query active/pending dedup decisions and derive both exclusion sets.

        Returns matched_ids (both sides of every pair) and secondary_ids (the
        lower-priority side of each pair, used to prevent duplicated transfer
        proposals for deduped source rows).
        """
        rows = self._db.execute(
            """
            SELECT source_transaction_id_a, source_type_a,
                   source_transaction_id_b, source_type_b,
                   account_id
            FROM app.match_decisions
            WHERE match_status IN ('accepted', 'pending')
              AND reversed_at IS NULL
              AND match_type = 'dedup'
            """
        ).fetchall()
        priority = self._settings.source_priority
        max_pri = len(priority)
        priority_index = {src: i for i, src in enumerate(priority)}
        matched: set[tuple[str, str]] = set()
        secondary: set[tuple[str, str, str]] = set()
        for row in rows:
            stid_a, st_a, stid_b, st_b, acct = row
            matched.add((stid_a, acct))
            matched.add((stid_b, acct))
            pri_a = priority_index.get(st_a, max_pri)
            pri_b = priority_index.get(st_b, max_pri)
            if pri_a <= pri_b:
                secondary.add((stid_b, st_b, acct))
            else:
                secondary.add((stid_a, st_a, acct))
        return _DedupDecisions(matched_ids=matched, secondary_ids=secondary)

    def _get_transfer_matched_ids(self) -> set[tuple[str, str, str]]:
        """Get (source_transaction_id, source_type, account_id) in active/pending transfers.

        Includes source_type in the key to avoid false collisions when
        account-scoped IDs repeat across different source types.
        """
        rows = self._db.execute(
            """
            SELECT source_transaction_id_a, source_type_a, account_id,
                   source_transaction_id_b, source_type_b, account_id_b
            FROM app.match_decisions
            WHERE match_status IN ('accepted', 'pending')
              AND reversed_at IS NULL
              AND match_type = 'transfer'
            """
        ).fetchall()
        ids: set[tuple[str, str, str]] = set()
        for row in rows:
            ids.add((row[0], row[1], row[2]))
            ids.add((row[3], row[4], row[5]))
        return ids

    def _run_transfer_tier(
        self,
        *,
        excluded_ids: set[tuple[str, str, str]],
        rejected_pairs: list[dict[str, Any]],
        result: MatchResult,
        auto_accept: bool = False,
    ) -> None:
        """Run transfer detection (Tier 4): blocking -> scoring -> assignment -> persist."""
        candidates = get_candidates_transfers(
            self._db,
            table=self._table,
            date_window_days=self._settings.date_window_days,
            excluded_ids=excluded_ids,
            rejected_pairs=rejected_pairs,
            signal_weights=self._settings.transfer_signal_weights,
        )
        TRANSFER_PAIRS_SCORED.inc(len(candidates))

        if not candidates:
            return

        assigned = assign_greedy(candidates)
        tier_pending = 0
        transfer_match_type: MatchType = "transfer"
        transfer_status: MatchStatus = "accepted" if auto_accept else "pending"

        for pair in assigned:
            TRANSFER_MATCH_CONFIDENCE.observe(pair.confidence_score)

            if pair.confidence_score < self._settings.transfer_review_threshold:
                logger.debug(
                    f"Transfer below threshold ({pair.confidence_score:.2f}): "
                    f"{pair.account_id_a[:8]} -> {pair.account_id_b[:8]}"
                )
                continue

            match_id = uuid.uuid4().hex[:12]
            create_match_decision(
                self._db,
                match_id=match_id,
                source_transaction_id_a=pair.source_transaction_id_a,
                source_type_a=pair.source_type_a,
                source_origin_a=pair.source_origin_a,
                source_transaction_id_b=pair.source_transaction_id_b,
                source_type_b=pair.source_type_b,
                source_origin_b=pair.source_origin_b,
                account_id=pair.account_id_a,
                account_id_b=pair.account_id_b,
                confidence_score=pair.confidence_score,
                match_signals={
                    "date_distance": round(pair.date_distance_score, 4),
                    "keyword": round(pair.keyword_score, 4),
                    "roundness": round(pair.amount_roundness_score, 4),
                    "pair_frequency": round(pair.pair_frequency_score, 4),
                },
                match_type=transfer_match_type,
                match_tier=None,
                match_status=transfer_status,
                decided_by="auto",
                match_reason=(
                    f"Transfer: {pair.account_id_a[:8]} -> {pair.account_id_b[:8]}, "
                    f"{pair.date_distance_days}d apart"
                ),
            )

            if auto_accept:
                result.auto_merged += 1
            else:
                result.pending_transfers += 1
            tier_pending += 1
            TRANSFER_MATCHES_PROPOSED.inc()

        if tier_pending:
            verb = "auto-accepted" if auto_accept else "potential"
            logger.info(f"Tier 4: {tier_pending} {verb} transfers found")

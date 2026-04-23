"""Transaction matching orchestrator.

Runs Tier 2b (within-source overlap) then Tier 3 (cross-source) matching.
Each tier: blocking -> scoring -> 1:1 assignment -> persist decisions.
"""

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.assignment import assign_greedy
from moneybin.matching.persistence import (
    create_match_decision,
    get_active_matches,
    get_rejected_pairs,
)
from moneybin.matching.scoring import (
    CandidatePair,
    get_candidates_cross_source,
    get_candidates_within_source,
)
from moneybin.metrics.registry import (
    DEDUP_MATCH_CONFIDENCE,
    DEDUP_MATCHES_TOTAL,
    DEDUP_PAIRS_SCORED,
    DEDUP_REVIEW_PENDING,
)

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Summary of a matching run."""

    auto_merged: int = 0
    pending_review: int = 0

    def summary(self) -> str:
        """Return a human-readable summary of the matching run."""
        parts = []
        if self.auto_merged:
            parts.append(f"{self.auto_merged} auto-merged")
        if self.pending_review:
            parts.append(f"{self.pending_review} pending review")
        if not parts:
            return "No new matches found"
        return ", ".join(parts)


class TransactionMatcher:
    """Orchestrates transaction matching across tiers."""

    def __init__(
        self,
        db: Database,
        settings: MatchingSettings,
        *,
        table: str = "prep.int_transactions__unioned",
    ) -> None:
        """Initialize the matcher with a database connection, settings, and source table."""
        self._db = db
        self._settings = settings
        self._table = table

    def run(self) -> MatchResult:
        """Run Tier 2b then Tier 3 matching."""
        result = MatchResult()
        rejected = get_rejected_pairs(self._db)

        already_matched = self._get_already_matched_ids()

        # Tier 2b: within-source overlap (high-confidence only)
        # get_candidates_within_source does not accept excluded_ids
        tier_2b_matched = self._run_tier(
            tier="2b",
            candidates_fn=lambda excluded: get_candidates_within_source(
                self._db,
                table=self._table,
                date_window_days=self._settings.date_window_days,
                rejected_pairs=rejected,
            ),
            excluded_ids=already_matched,
            result=result,
        )
        already_matched.update(tier_2b_matched)

        # Tier 3: cross-source
        self._run_tier(
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

        return result

    def _run_tier(
        self,
        *,
        tier: str,
        candidates_fn: Any,
        excluded_ids: set[str],
        result: MatchResult,
    ) -> set[str]:
        """Run blocking -> scoring -> assignment -> persist for one tier."""
        candidates: list[CandidatePair] = candidates_fn(excluded_ids)
        DEDUP_PAIRS_SCORED.inc(len(candidates))

        if not candidates:
            return set()

        assigned = assign_greedy(candidates)
        newly_matched: set[str] = set()

        for pair in assigned:
            DEDUP_MATCH_CONFIDENCE.observe(pair.confidence_score)

            if pair.confidence_score >= self._settings.high_confidence_threshold:
                status = "accepted"
                decided_by = "auto"
                result.auto_merged += 1
                DEDUP_MATCHES_TOTAL.labels(match_tier=tier, decided_by="auto").inc()
            elif (
                tier == "3" and pair.confidence_score >= self._settings.review_threshold
            ):
                status = "pending"
                decided_by = "auto"
                result.pending_review += 1
                DEDUP_REVIEW_PENDING.inc()
            else:
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

            newly_matched.add(pair.source_transaction_id_a)
            newly_matched.add(pair.source_transaction_id_b)

        if assigned:
            logger.info(
                f"Tier {tier}: {result.auto_merged} auto-merged, "
                f"{result.pending_review} pending review"
            )

        return newly_matched

    def _get_already_matched_ids(self) -> set[str]:
        """Get source_transaction_ids that are already in active matches."""
        active = get_active_matches(self._db)
        ids: set[str] = set()
        for m in active:
            ids.add(m["source_transaction_id_a"])
            ids.add(m["source_transaction_id_b"])
        return ids

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
from moneybin.matching.assignment import (
    NodeKey,
    assign_components,
    assign_greedy,
    connected_components,
)
from moneybin.matching.persistence import (
    MatchStatus,
    MatchTier,
    MatchType,
    get_active_dedup_edges,
    get_rejected_pairs,
)
from moneybin.matching.reconciliation import (
    ReconciliationError,
    retire_transfers_invalidated_by_dedup,
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
from moneybin.tables import MATCH_DECISIONS

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Summary of a matching run."""

    auto_merged: int = 0
    pending_review: int = 0
    pending_transfers: int = 0
    accepted_transfers: int = 0
    # Accepted transfers this run reversed because a dedup component now holds
    # both of their legs. Not part of has_matches: nothing was *found*, and a
    # caller reporting "no new matches" is still right. It is reported rather
    # than left in the log because the user accepted those transfers.
    transfers_retired: int = 0

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

    @property
    def has_durable_writes(self) -> bool:
        """True if the run has already committed something to the ledger.

        Distinct from ``has_matches``, which answers "did it find anything" and
        deliberately excludes retirements. This one answers "is there state
        behind it now" — the question that decides whether a failed run owes the
        caller a report.
        """
        return self.has_matches or self.transfers_retired > 0

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


class MatchRunError(Exception):
    """A run that raised after it had already committed part of its work.

    The matcher opens no transaction around the run: each dedup tier persists one
    decision per pair, and the reconciliation commits each reversal as it goes, so
    everything written before the failing step is durable. Without this carrier the
    partial result dies with the exception and the caller reports zero — naming
    merges the user can see in the ledger as never made, and a transfer the user
    accepted as untouched when it has in fact been undone.

    It also keeps a *late* ``CatalogException`` from being read as the first-load
    "views not built yet" precondition: that classification claims nothing was
    examined, which stops being true the moment a tier writes a decision. Only a
    run that committed something is wrapped, so a genuinely empty first load still
    reaches that branch as its own exception.
    """

    def __init__(self, cause: BaseException, *, partial: MatchResult) -> None:
        """Wrap ``cause``, carrying the work that had already committed."""
        super().__init__(str(cause))
        # Restatement can become the exception chain's cause; retain both failures.
        self.cause = cause
        self.partial = partial
        self.restatement_error: BaseException | None = None


@dataclass
class _DedupDecisions:
    """Projections derived from a single active-dedup-decisions query."""

    active_edges: list[tuple[NodeKey, NodeKey]]
    secondary_ids: set[tuple[str, str, str]]


class TransactionMatcher:
    """Orchestrates transaction matching across tiers."""

    def __init__(
        self,
        db: Database,
        settings: MatchingSettings,
        *,
        table: str = UNIONED_TABLE,
        actor: str = "system",
    ) -> None:
        """Initialize the matcher with a database connection, settings, and source table.

        ``actor`` is the audit actor for the match decisions this run writes
        (``"system"`` for automated runs; surfaces pass ``"cli"``/``"mcp"``).
        Every decision is also ``decided_by="auto"`` — the matcher proposes, the
        user disposes.
        """
        # Deferred import: engine is loaded via services.__init__ →
        # matching_service → engine, and the repo's base → services.audit_service
        # chain re-enters that path; a module-top import here would cycle.
        from moneybin.repositories.match_decisions_repo import (  # noqa: PLC0415
            MatchDecisionsRepo,
        )

        self._db = db
        self._settings = settings
        self._table = table
        self._actor = actor
        self._decisions = MatchDecisionsRepo(db)

    def run(self, *, auto_accept_transfers: bool = False) -> MatchResult:
        """Run Tier 2b then Tier 3 matching.

        ``auto_accept_transfers`` writes transfer matches as ``accepted`` instead
        of ``pending``, simulating automated human review. Used by the scenario
        runner so transfer evaluations can read from ``core.bridge_transfers``
        without an interactive review step.
        """
        result = MatchResult()
        rejected = get_rejected_pairs(self._db)

        # Both dedup tiers feed one growing union-find seeded from existing active
        # edges, instead of excluding earlier-tier rows. A row matched in Tier 2b
        # can still gain a Tier-3 cross-source edge when it connects a new
        # component, so a 3rd+ copy is no longer left edge-less. The candidate
        # functions pass excluded_ids=None — union-find, not row-exclusion, is now
        # what prevents redundant edges within a component.
        seed = self._fetch_active_dedup_decisions()
        component_edges: list[tuple[NodeKey, NodeKey]] = list(seed.active_edges)

        # Everything that writes runs under this guard, starting at Tier 2b —
        # the run's first durable write. The matcher opens no transaction, so a
        # tier's decisions and the reconciliation's reversals are committed by
        # the time any later step can fail, and an exception that discarded
        # `result` would take the only record of them with it. Starting the
        # guard lower (at the reconciliation, or after it) leaves the same
        # partial state behind with nothing left to report it. The reads above
        # stay outside: they precede every write, so a failure there genuinely
        # has nothing to carry.
        try:
            # Tier 2b: within-source overlap (high-confidence only)
            tier_2b_edges = self._run_tier(
                tier="2b",
                candidates_fn=lambda: get_candidates_within_source(
                    self._db,
                    table=self._table,
                    date_window_days=self._settings.date_window_days,
                    excluded_ids=None,
                    rejected_pairs=rejected,
                ),
                seed_edges=component_edges,
                result=result,
            )
            component_edges.extend(tier_2b_edges)

            # Tier 3: cross-source. Passing high_confidence_threshold enables the
            # agreement floor: a pair whose descriptions agree scores at/above the
            # threshold at any gap inside the window, regardless of similarity
            # (assign_components' source_file guard keeps N true duplicates 1:1).
            tier_3_edges = self._run_tier(
                tier="3",
                candidates_fn=lambda: get_candidates_cross_source(
                    self._db,
                    table=self._table,
                    date_window_days=self._settings.date_window_days,
                    excluded_ids=None,
                    rejected_pairs=rejected,
                    high_confidence_threshold=self._settings.high_confidence_threshold,
                ),
                seed_edges=component_edges,
                result=result,
            )
            component_edges.extend(tier_3_edges)

            # Between the tiers: dedup is settled, transfer detection has not
            # run. This is the only point where both are true, and it is what
            # the reconciliation needs from either side — the components it
            # reads include the edges this run just wrote, and the transfers it
            # reverses are gone before the exclusion set below is built, so a
            # leg it frees is a Tier 4 candidate in this same run rather than
            # the next one.
            result.transfers_retired = retire_transfers_invalidated_by_dedup(
                self._db, decisions=self._decisions, actor=self._actor
            )

            # Tier 4: transfer detection (runs after dedup).
            # Exclude transactions in active transfers AND the non-primary side
            # of each dedup group. Without this, duplicate source rows (e.g.,
            # csv_chk1 and ofx_chk1 both deduped) can each form separate transfer
            # proposals that resolve to the same merged transaction pair in
            # bridge_transfers. Re-query after tiers so decisions created in this
            # run are included.
            transfer_excluded = self._get_transfer_matched_ids()
            transfer_excluded |= self._fetch_active_dedup_decisions().secondary_ids
            rejected_transfer = get_rejected_pairs(self._db, match_type="transfer")

            self._run_transfer_tier(
                excluded_ids=transfer_excluded,
                rejected_pairs=rejected_transfer,
                result=result,
                auto_accept=auto_accept_transfers,
            )
        except ReconciliationError as exc:
            # Its own count, not `result`'s: the assignment above never ran.
            # Wrapped unconditionally, unlike the branch below — this exception
            # exists only because reversals committed, and it is never the
            # first-load precondition that branch has to leave alone.
            result.transfers_retired = exc.transfers_retired
            raise MatchRunError(exc, partial=result) from exc
        except Exception as exc:
            if not result.has_durable_writes:
                # Nothing committed, so there is no partial outcome to carry —
                # and wrapping here would hide the bare CatalogException that
                # `refresh()` reads as "views not built yet" on a first load.
                # The carrier is earned by a durable write, not by failing.
                raise
            raise MatchRunError(exc, partial=result) from exc

        return result

    def _classify_pair(
        self, pair: CandidatePair, tier: MatchTier
    ) -> tuple[MatchStatus, str] | None:
        """Return (status, decided_by) if pair should be persisted, None to skip.

        The tier-3-only review-threshold branch lives exclusively here so it can
        be tested without a database fixture.
        """
        # Agreement, not the score, is what earns a silent merge. The score
        # cannot carry that gate on its own: the weighted formula peaks at
        # date_distance_days=0, where its date term is 1.0, so a disagreeing
        # pair landing on the same day clears the threshold on closeness alone
        # — two distinct transactions at one merchant differing only in a
        # trailing reference number score ~0.97. Agreement is a separate
        # signal precisely because similarity is not evidence of identity.
        if (
            pair.confidence_score >= self._settings.high_confidence_threshold
            and pair.descriptions_agree
        ):
            return ("accepted", "auto")
        # Every cross-source pair that survives assignment is reviewable. It has
        # already cleared same-account, exact-amount, in-window blocking and won a
        # 1:1 assignment, which is enough evidence to ask about even when the
        # descriptions disagree. Dropping it instead would be its own silent
        # action: the duplicate stays in the ledger and nobody is ever told.
        # assign_components bounds this, so the queue cannot run away.
        if tier == "3":
            return ("pending", "auto")
        return None

    def _persist_dedup_match(
        self,
        pair: CandidatePair,
        tier: MatchTier,
        status: MatchStatus,
        decided_by: str,
    ) -> None:
        """Write one dedup match decision to the database (audited)."""
        match_id = uuid.uuid4().hex[:12]
        self._decisions.insert(
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
                "descriptions_agree": pair.descriptions_agree,
            },
            match_tier=tier,
            match_status=status,
            decided_by=decided_by,
            match_reason=(
                f"Amount match, {pair.date_distance_days}d apart, "
                f"desc similarity {pair.description_similarity:.2f}"
                + (", descriptions agree" if pair.descriptions_agree else "")
            ),
            actor=self._actor,
        )

    def _run_tier(
        self,
        *,
        tier: MatchTier,
        candidates_fn: Callable[[], list[CandidatePair]],
        seed_edges: list[tuple[NodeKey, NodeKey]],
        result: MatchResult,
    ) -> list[tuple[NodeKey, NodeKey]]:
        """Run blocking -> scoring -> assignment -> persist for one tier.

        Returns the newly-added component edges as (node_a, node_b) NodeKey pairs.
        """
        candidates: list[CandidatePair] = candidates_fn()
        DEDUP_PAIRS_SCORED.inc(len(candidates))

        # Classify before assignment, so an edge that cannot be persisted never
        # consumes one. assign_components orders by score alone and the
        # cardinality guard lets a row claim only one partner per physical source,
        # so on Tier 2b a higher-scoring *disagreeing* pair would union the
        # component first, push the lower-scoring agreeing pair out on the guard,
        # and then be dropped itself by the agreement gate — leaving the real
        # duplicate unmatched and counted twice, with nothing written and no review
        # queue on that tier to notice. Tier 3 classifies every pair, so this is a
        # no-op there. Filtering here also keeps newly_added consistent with what
        # is persisted: every assigned edge now has a classification.
        classified = [(pair, self._classify_pair(pair, tier)) for pair in candidates]
        mergeable = {
            pair: classification
            for pair, classification in classified
            if classification is not None
        }

        if not mergeable:
            return []

        assigned = assign_components(list(mergeable), seed_edges=seed_edges)
        newly_added: list[tuple[NodeKey, NodeKey]] = []
        tier_merged = 0
        tier_pending = 0

        for pair in assigned:
            DEDUP_MATCH_CONFIDENCE.observe(pair.confidence_score)
            status, decided_by = mergeable[pair]

            # Persist first, count second — the order Tier 4 already uses.
            # `result` now rides out on MatchRunError when a later pair raises,
            # so its counters have to name what committed rather than what the
            # loop reached. A clean run completes the loop and totals the same
            # either way; a crashing one differs by exactly the failed pair.
            self._persist_dedup_match(pair, tier, status, decided_by)

            if status == "accepted":
                result.auto_merged += 1
                tier_merged += 1
                DEDUP_MATCHES_TOTAL.labels(match_tier=tier, decided_by="auto").inc()
            else:
                result.pending_review += 1
                tier_pending += 1
                DEDUP_REVIEW_PENDING.inc()

            newly_added.append((
                (pair.source_type_a, pair.source_transaction_id_a, pair.account_id),
                (pair.source_type_b, pair.source_transaction_id_b, pair.account_id),
            ))

        if tier_merged or tier_pending:
            # Info, not debug: MatchResult.summary() reports run-wide totals, so
            # this is the only readable record of the per-tier split — debug
            # would drop it from the log file, not just the console. The
            # console suppression is the denylist's job.
            logger.info(
                f"Tier {tier}: {tier_merged} auto-merged, {tier_pending} pending review"
            )

        return newly_added

    def _fetch_active_dedup_decisions(self) -> _DedupDecisions:
        """Derive both projections from the active+pending dedup edge set.

        Returns active_edges (one (node_a, node_b) full-triple pair per decision,
        used to seed the union-find so new copies attach to existing components)
        and secondary_ids (all non-primary component members, excluded from
        transfer detection so deduped source rows don't form duplicate transfer
        proposals).
        """
        priority = self._settings.source_priority
        max_pri = len(priority)
        priority_index = {src: i for i, src in enumerate(priority)}
        edges: list[tuple[NodeKey, NodeKey]] = [
            (
                (e["source_type_a"], e["source_transaction_id_a"], e["account_id"]),
                (e["source_type_b"], e["source_transaction_id_b"], e["account_id"]),
            )
            for e in get_active_dedup_edges(self._db, statuses=("accepted", "pending"))
        ]

        # Exclude every non-primary member of each component from transfer
        # matching — not just the lower-priority side of each individual edge.
        # Pairwise exclusion misses members that are the higher-priority side of
        # their own edge but are still non-primary within the broader component
        # (e.g. a "V" topology: manual–ofx and parquet–ofx share one component;
        # pairwise keeps parquet eligible because parquet > ofx, but manual is
        # primary).
        secondary: set[tuple[str, str, str]] = set()
        for members in connected_components(edges):
            # node is (source_type, source_transaction_id, account_id)
            primary = min(members, key=lambda n: priority_index.get(n[0], max_pri))
            for n in members:
                if n != primary:
                    secondary.add((n[1], n[0], n[2]))  # (stid, source_type, account_id)
        return _DedupDecisions(active_edges=edges, secondary_ids=secondary)

    def _get_transfer_matched_ids(self) -> set[tuple[str, str, str]]:
        """Get (source_transaction_id, source_type, account_id) in active/pending transfers.

        Includes source_type in the key to avoid false collisions when
        account-scoped IDs repeat across different source types.
        """
        rows = self._db.execute(
            f"""
            SELECT source_transaction_id_a, source_type_a, account_id,
                   source_transaction_id_b, source_type_b, account_id_b
            FROM {MATCH_DECISIONS.full_name}
            WHERE match_status IN ('accepted', 'pending')
              AND reversed_at IS NULL
              AND match_type = 'transfer'
            """  # noqa: S608 — TableRef constant; no interpolated values
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
            self._decisions.insert(
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
                },
                match_type=transfer_match_type,
                match_tier=None,
                match_status=transfer_status,
                decided_by="auto",
                match_reason=(
                    f"Transfer: {pair.account_id_a[:8]} -> {pair.account_id_b[:8]}, "
                    f"{pair.date_distance_days}d apart"
                ),
                actor=self._actor,
            )

            if auto_accept:
                result.auto_merged += 1
                result.accepted_transfers += 1
            else:
                result.pending_transfers += 1
            tier_pending += 1
            TRANSFER_MATCHES_PROPOSED.inc()

        if tier_pending:
            verb = "auto-accepted" if auto_accept else "potential"
            # See the tier-summary site above: keep in the file, denylisted
            # from the console. summary() does not distinguish auto-accepted
            # from potential transfers.
            logger.info(f"Tier 4: {tier_pending} {verb} transfers found")

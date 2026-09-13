"""Transactional review-only investment planning and durable inspection."""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.event_planning import evaluate_plan
from moneybin.metrics.registry import (
    investment_match_duration_seconds,
    investment_match_proposals_total,
)
from moneybin.repositories.investment_match_decisions_repo import (
    InvestmentMatchDecisionsRepo,
)
from moneybin.tables import (
    INVESTMENT_EVENT_EVIDENCE,
    INVESTMENT_EVENT_HEADERS,
    INVESTMENT_EVENT_LEGS,
    TableRef,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InvestmentPlanResult:
    """Committed planner dispositions, never accepted ledger changes."""

    proposed: int
    stale: int
    suppressed: int
    pending_unique: int
    pending_competing: int
    unchanged: int = 0


class InvestmentMatchingService:
    """Compose the pure planner and the only investment Proposal write owner."""

    def __init__(self, db: Database) -> None:
        """Bind planning and inspection to the caller's database."""
        self._db = db
        self._repo = InvestmentMatchDecisionsRepo(db)

    def _facts(
        self, table: TableRef, locked_components: Sequence[Mapping[str, Any]] = ()
    ) -> list[dict[str, Any]]:
        evidence = INVESTMENT_EVENT_EVIDENCE.full_name
        legs = INVESTMENT_EVENT_LEGS.full_name
        parameters: list[Any] = []
        reserved = ""
        for component in locked_components:
            for row in component["reserved_rows"]:
                reserved += " OR (source_type = ? AND source_origin = ? AND native_reference = ?)"
                parameters.extend(row)
        candidate_events = f"""
            SELECT left_source_event_key AS source_event_key FROM {evidence} WHERE is_candidate
            UNION SELECT right_source_event_key FROM {evidence} WHERE is_candidate
            UNION SELECT source_event_key FROM {legs} WHERE FALSE {reserved}
        """  # fixed TableRefs and parameterized reserved-row identities
        predicate = (
            "is_candidate"
            if table == INVESTMENT_EVENT_EVIDENCE
            else f"source_event_key IN ({candidate_events})"
        )
        cursor = self._db.execute(
            f"SELECT * FROM {table.full_name} WHERE {predicate}",  # fixed private TableRefs and predicates
            [] if table == INVESTMENT_EVENT_EVIDENCE else parameters,
        )
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def run(
        self,
        *,
        actor: str = "system",
        locked_components: Sequence[Mapping[str, Any]] | None = None,
    ) -> InvestmentPlanResult:
        """Plan against one transaction snapshot and commit only pending/stale rows."""
        started = perf_counter()
        dispositions: list[tuple[str, str]] = []
        self._db.begin()
        try:
            existing = self._repo.all_rows()
            active = {
                row["proposal_id"]: row
                for row in existing
                if row["status"] == "accepted"
                or (row["status"] == "stale" and row["accepted_at"] is not None)
            }
            supplied = {row["decision_id"]: row for row in locked_components or ()}
            complete = len(supplied) == len(locked_components or ()) and set(
                supplied
            ) == set(active)
            for decision_id, row in active.items():
                component = supplied.get(decision_id, {})
                expected_rows = {
                    (leg["source_type"], leg["source_origin"], leg["native_reference"])
                    for leg in row["legs"]
                }
                complete = (
                    complete
                    and bool(expected_rows)
                    and (
                        set(component.get("members", ())) == set(row["members"])
                        and {tuple(item) for item in component.get("reserved_rows", ())}
                        == expected_rows
                    )
                )
            if not complete:
                raise UserError(
                    "Active investment membership is required for replacement planning.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            authoritative_components = [
                {**component, "status": active[decision_id]["status"]}
                for decision_id, component in supplied.items()
            ]
            plan = evaluate_plan(
                self._facts(INVESTMENT_EVENT_HEADERS, authoritative_components),
                self._facts(INVESTMENT_EVENT_LEGS, authoritative_components),
                self._facts(INVESTMENT_EVENT_EVIDENCE),
                decisions=existing,
                locked_components=authoritative_components,
            )
            dispositions.extend((band, "suppressed") for band in plan.suppressed_bands)
            proposals = plan.proposals
            desired = {
                (p.relationship_fingerprint, p.candidate_graph_fingerprint)
                for p in proposals
            }
            stale = 0
            for row in existing:
                key = (
                    row["relationship_fingerprint"],
                    row["candidate_graph_fingerprint"],
                )
                if row["status"] == "pending" and key not in desired:
                    stale += self._repo.mark_stale(
                        row["proposal_id"], actor=actor, in_outer_txn=True
                    )
                    dispositions.append((row["confidence_band"], "stale"))
            pending = {
                (row["relationship_fingerprint"], row["candidate_graph_fingerprint"])
                for row in existing
                if row["status"] == "pending"
            }
            accepted = {
                row["relationship_fingerprint"]
                for row in existing
                if row["status"] == "accepted"
            }
            proposed = unchanged = 0
            for proposal in proposals:
                key = (
                    proposal.relationship_fingerprint,
                    proposal.candidate_graph_fingerprint,
                )
                if key in pending or proposal.relationship_fingerprint in accepted:
                    unchanged += 1
                    continue
                self._repo.insert_pending(proposal, actor=actor, in_outer_txn=True)
                proposed += 1
                dispositions.append((
                    proposal.confidence_band,
                    "pending_competing" if proposal.is_competing else "pending_unique",
                ))
            result = InvestmentPlanResult(
                proposed,
                stale,
                len(plan.suppressed_relationships),
                sum(not p.is_competing for p in proposals),
                sum(p.is_competing for p in proposals),
                unchanged,
            )
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        try:
            for band, outcome in dispositions:
                investment_match_proposals_total.labels(
                    band=band, outcome=outcome
                ).inc()
            investment_match_duration_seconds.labels(operation="plan").observe(
                perf_counter() - started
            )
        except Exception:
            logger.warning("Investment planning telemetry could not be recorded")
        return result

    def pending(self) -> list[dict[str, Any]]:
        """Read persisted pending evidence without rerunning assignment."""
        return [row for row in self._repo.all_rows() if row["status"] == "pending"]

    def history(self) -> list[dict[str, Any]]:
        """Read historical lifecycle rows with their original review evidence."""
        return [row for row in self._repo.all_rows() if row["status"] != "pending"]

"""Transactional review-only investment planning and durable inspection."""

import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from time import perf_counter
from typing import Any, Literal

from moneybin import error_codes
from moneybin.database import Database, has_column
from moneybin.errors import UserError
from moneybin.investments.event_choices import validate_selection
from moneybin.investments.event_planning import PlanningResult, Proposal, evaluate_plan
from moneybin.metrics.registry import (
    INVESTMENT_MATCH_DECISIONS_TOTAL,
    INVESTMENT_MATCH_DURATION_SECONDS,
    INVESTMENT_MATCH_PROPOSALS_TOTAL,
)
from moneybin.repositories.investment_match_decisions_repo import (
    InvestmentMatchDecisionsRepo,
)
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import (
    ACCOUNT_SETTINGS,
    INVESTMENT_EVENT_EVIDENCE,
    INVESTMENT_EVENT_HEADERS,
    INVESTMENT_EVENT_LEGS,
    TableRef,
)

logger = logging.getLogger(__name__)


def supports_investment_rejection_undo(actions: Iterable[str]) -> bool:
    """Only rejection operations and their dependent staleness have an inverse."""
    selected = set(actions)
    return "investment_match.reject" in selected and selected <= {
        "investment_match.reject",
        "investment_match.stale",
    }


def record_committed_investment_decisions(
    auto_eligible: Sequence[bool], *, duration_seconds: float
) -> None:
    """Publish bounded first-decision telemetry only after its transaction commits."""
    if not auto_eligible:
        return
    try:
        for eligible in auto_eligible:
            INVESTMENT_MATCH_DECISIONS_TOTAL.labels(
                decision="rejected", auto_eligible=str(eligible).lower()
            ).inc()
        INVESTMENT_MATCH_DURATION_SECONDS.labels(operation="decision").observe(
            duration_seconds
        )
    except Exception:
        logger.warning("Investment decision telemetry could not be recorded")


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
        """Candidate-scoped planner facts, minus accounts with a source chosen.

        A chosen account's ledger has one source, so a proposal there is noise;
        existing pending proposals go stale under the existing rules. Reserved
        rows of accepted decisions are kept so locked components stay whole.
        The saved setting is read, not the built dim, because matching runs
        before the transform.
        """
        evidence = INVESTMENT_EVENT_EVIDENCE.full_name
        legs = INVESTMENT_EVENT_LEGS.full_name
        parameters: list[Any] = []
        reserved = ""
        for component in locked_components:
            for row in component["reserved_rows"]:
                reserved += " OR (source_type = ? AND source_origin = ? AND native_reference = ?)"
                parameters.extend(row)
        candidate = "is_candidate"
        if has_column(self._db, ACCOUNT_SETTINGS, "investment_source_type"):
            chosen = f"""
                SELECT source_event_key FROM {legs}
                WHERE account_id IN (
                    SELECT account_id FROM {ACCOUNT_SETTINGS.full_name}
                    WHERE investment_source_type IS NOT NULL
                )
            """  # fixed TableRefs
            candidate += (
                f" AND left_source_event_key NOT IN ({chosen})"
                f" AND right_source_event_key NOT IN ({chosen})"
            )
        candidate_events = f"""
            SELECT left_source_event_key AS source_event_key FROM {evidence} WHERE {candidate}
            UNION SELECT right_source_event_key FROM {evidence} WHERE {candidate}
            UNION SELECT source_event_key FROM {legs} WHERE FALSE {reserved}
        """  # fixed TableRefs and parameterized reserved-row identities
        predicate = (
            candidate
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
                INVESTMENT_MATCH_PROPOSALS_TOTAL.labels(
                    band=band, outcome=outcome
                ).inc()
            INVESTMENT_MATCH_DURATION_SECONDS.labels(operation="plan").observe(
                perf_counter() - started
            )
        except Exception:
            logger.warning("Investment planning telemetry could not be recorded")
        return result

    def pending(self) -> list[dict[str, Any]]:
        """Read persisted pending evidence without rerunning assignment."""
        return [row for row in self._repo.all_rows() if row["status"] == "pending"]

    def _decision_plan(self, existing: list[dict[str, Any]]) -> PlanningResult:
        if any(
            row["status"] == "accepted" or row["accepted_at"] is not None
            for row in existing
        ):
            raise UserError(
                "Active investment membership is unavailable for decisions.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        return evaluate_plan(
            self._facts(INVESTMENT_EVENT_HEADERS),
            self._facts(INVESTMENT_EVENT_LEGS),
            self._facts(INVESTMENT_EVENT_EVIDENCE),
            decisions=existing,
        )

    def prepare_decision(
        self,
        decision_id: str,
        *,
        decision: Literal["accept", "reject"],
        field_choices: Sequence[Mapping[str, str]] = (),
    ) -> Proposal:
        """Recompute current revisions, dependencies, constraints and assignment."""
        if decision == "reject" and field_choices:
            raise UserError(
                "Reject forbids field choices.", code=error_codes.MUTATION_INVALID_INPUT
            )
        existing = self._repo.all_rows()
        row = next((r for r in existing if r["proposal_id"] == decision_id), None)
        if row is None:
            raise UserError(
                "No investment Proposal exists for this id.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        current = self._decision_plan(existing)
        proposal = next(
            (
                p
                for p in current.proposals
                if p.relationship_fingerprint == row["relationship_fingerprint"]
                and p.candidate_graph_fingerprint == row["candidate_graph_fingerprint"]
            ),
            None,
        )
        if row["status"] != "pending" or proposal is None:
            raise UserError(
                "Investment Proposal is stale; run investment matching again.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        if decision == "accept":
            validate_selection(proposal.legs, proposal.field_choices, field_choices)
            raise UserError(
                "Investment match acceptance is unavailable until atomic Golden materialization exists.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        return proposal

    def invalidate_pending(self, *, actor: str, parent_audit_id: str) -> None:
        """Stale changed assignment space in the caller's decision transaction."""
        existing = self._repo.all_rows()
        desired = {
            (p.relationship_fingerprint, p.candidate_graph_fingerprint)
            for p in self._decision_plan(existing).proposals
        }
        for row in existing:
            if (
                row["status"] == "pending"
                and (
                    row["relationship_fingerprint"],
                    row["candidate_graph_fingerprint"],
                )
                not in desired
            ):
                self._repo.mark_stale(
                    row["proposal_id"],
                    actor=actor,
                    in_outer_txn=True,
                    parent_audit_id=parent_audit_id,
                )

    def decide(
        self,
        decision_id: str,
        *,
        decision: Literal["accept", "reject"],
        actor: str,
        field_choices: Sequence[Mapping[str, str]] = (),
    ) -> None:
        """Commit a whole-Proposal rejection and affected stale rows atomically."""
        started = perf_counter()
        self._db.begin()
        try:
            proposal = self.prepare_decision(
                decision_id, decision=decision, field_choices=field_choices
            )
            event = self._repo.reject(decision_id, actor=actor, in_outer_txn=True)
            self.invalidate_pending(actor=actor, parent_audit_id=event.audit_id)
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        record_committed_investment_decisions(
            [proposal.auto_eligible], duration_seconds=perf_counter() - started
        )

    def undo_rejections(self, events: Sequence[AuditEvent], *, actor: str) -> None:
        """Undo constraints as an operation; never restore historical row images."""
        rejections = [
            event for event in events if event.action == "investment_match.reject"
        ]
        if not supports_investment_rejection_undo(event.action for event in events):
            raise UserError(
                "Only investment rejection operations have an available undo path.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        inverses = [
            self._repo.undo_rejection(event, actor=actor)
            for event in reversed(rejections)
        ]
        self.invalidate_pending(actor=actor, parent_audit_id=inverses[-1].audit_id)

    def history(self) -> list[dict[str, Any]]:
        """Read historical lifecycle rows with their original review evidence."""
        return [row for row in self._repo.all_rows() if row["status"] != "pending"]

    def count_pending(self) -> int:
        """Count pending Proposals without loading legs or evidence."""
        return self._repo.count(pending=True)

    def count_history(self) -> int:
        """Count historical (non-pending) Proposals without loading legs or evidence."""
        return self._repo.count(pending=False)

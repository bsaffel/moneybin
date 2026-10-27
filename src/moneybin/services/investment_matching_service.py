"""Transactional review-only investment planning and durable inspection."""

import logging
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

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

if TYPE_CHECKING:
    from moneybin.services.investment_membership_service import MembershipTransition

logger = logging.getLogger(__name__)


def supports_investment_rejection_undo(actions: Iterable[str]) -> bool:
    """Only rejection operations and their dependent staleness have an inverse."""
    selected = set(actions)
    return "investment_match.reject" in selected and selected <= {
        "investment_match.reject",
        "investment_match.stale",
    }


def record_committed_investment_decisions(
    decisions: Sequence[tuple[Literal["accepted", "rejected"], bool]],
    *,
    duration_seconds: float,
) -> None:
    """Publish bounded first-decision telemetry only after its transaction commits."""
    if not decisions:
        return
    try:
        for status, eligible in decisions:
            INVESTMENT_MATCH_DECISIONS_TOTAL.labels(
                decision=status, auto_eligible=str(eligible).lower()
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
            from moneybin.services.investment_membership_service import (
                InvestmentMembershipService,
            )

            membership = InvestmentMembershipService(self._db)
            membership.invalidate_accepted(actor=actor)
            existing = self._repo.all_rows()
            active = {
                row["proposal_id"]: row
                for row in existing
                if row["status"] == "accepted"
                or (row["status"] == "stale" and row["accepted_at"] is not None)
            }
            authoritative_components = membership.locked_components(existing)
            if locked_components is not None:
                supplied = {r["decision_id"]: r for r in locked_components}
                if (
                    len(supplied) != len(locked_components)
                    or set(supplied) != set(active)
                    or any(
                        set(supplied[c["decision_id"]].get("members", ()))
                        != set(c["members"])
                        or {
                            tuple(r)
                            for r in supplied[c["decision_id"]].get("reserved_rows", ())
                        }
                        != set(c["reserved_rows"])
                        for c in authoritative_components
                    )
                ):
                    raise UserError(
                        "Supplied investment membership is incomplete.",
                        code=error_codes.MUTATION_INVALID_INPUT,
                    )
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
        from moneybin.services.investment_membership_service import (
            InvestmentMembershipService,
        )

        locked = InvestmentMembershipService(self._db).locked_components(existing)
        return evaluate_plan(
            self._facts(INVESTMENT_EVENT_HEADERS, locked),
            self._facts(INVESTMENT_EVENT_LEGS, locked),
            self._facts(INVESTMENT_EVENT_EVIDENCE),
            decisions=existing,
            locked_components=locked,
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
            self.prepare_acceptance(decision_id, proposal, field_choices=field_choices)
        return proposal

    def decision_dependencies(self) -> dict[str, Any]:
        """Bind complete membership, choices and saved curation during confirmation."""
        from moneybin.investments.event_projection import read_rows
        from moneybin.repositories.investment_event_members_repo import (
            InvestmentEventMembersRepo,
        )
        from moneybin.repositories.investment_match_field_resolutions_repo import (
            InvestmentMatchFieldResolutionsRepo,
        )
        from moneybin.tables import LOT_SELECTIONS

        return {
            "membership": InvestmentEventMembersRepo(self._db).active_members(),
            "resolutions": InvestmentMatchFieldResolutionsRepo(
                self._db
            ).active_resolutions(),
            "selections": read_rows(
                self._db,
                f"SELECT * FROM {LOT_SELECTIONS.full_name} ORDER BY investment_transaction_id, lot_id",
            ),
        }  # TableRef constant

    def prepare_acceptance(
        self,
        decision_id: str,
        proposal: Proposal,
        *,
        field_choices: Sequence[Mapping[str, str]] = (),
        prior: "MembershipTransition | None" = None,
    ) -> "MembershipTransition":
        """Build the whole proposed ledger without publishing any App rows."""
        from moneybin.repositories.investment_event_members_repo import (
            InvestmentEventMembersRepo,
        )
        from moneybin.repositories.investment_match_field_resolutions_repo import (
            InvestmentMatchFieldResolutionsRepo,
        )
        from moneybin.services.investment_membership_service import (
            InvestmentMembershipService,
        )

        membership = InvestmentMembershipService(self._db)
        repo = InvestmentEventMembersRepo(self._db)
        active = prior.proposed if prior is not None else repo.active_members()

        def identity(row: Mapping[str, Any]) -> tuple[str, str, str]:
            return row["source_type"], row["source_origin"], row["native_reference"]

        selected = {identity(leg) for leg in proposal.legs}
        affected = [
            r
            for r in active
            if identity(r) in selected
            or r["decision_id"] in proposal.supersedes_decision_ids
        ]
        if not affected or not selected <= {identity(r) for r in affected}:
            raise UserError(
                "Complete registered investment membership is required; run a full refresh.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        selected_fields = validate_selection(
            proposal.legs, proposal.field_choices, field_choices
        )
        roles = {r["leg_role"] for r in selected_fields}
        candidate_groups = {r["event_group_id"] for r in affected}
        history = repo.history()
        group_id = min(
            candidate_groups,
            key=lambda group: (
                min(r["created_at"] for r in history if r["event_group_id"] == group),
                group,
            ),
        )
        role_ids: dict[str, str] = {}
        for role in roles:
            candidates = [
                r
                for r in affected
                if r["semantic_leg_role"] == role and r["event_group_id"] == group_id
            ]
            role_ids[role] = (
                min(
                    candidates,
                    key=lambda r: (r["created_at"], r["investment_transaction_id"]),
                )["investment_transaction_id"]
                if candidates
                else uuid4().hex[:12]
            )
        proposed = [
            r
            for r in active
            if r["member_kind"] == "source_event" and r not in affected
        ]
        proposed += [
            membership.source_member(
                dict(leg),
                event_group_id=group_id,
                investment_transaction_id=role_ids[leg["leg_role"]],
                decision_id=decision_id,
            )
            for leg in proposal.legs
        ]
        mapping = {
            r["investment_transaction_id"]: role_ids[r["semantic_leg_role"]]
            for r in affected
            if r["semantic_leg_role"] in role_ids
        }
        resolutions = [
            r
            for r in (
                prior.resolutions
                if prior is not None
                else InvestmentMatchFieldResolutionsRepo(self._db).active_resolutions()
            )
            if r["decision_id"] not in proposal.supersedes_decision_ids
        ]
        for selection in field_choices:
            conflict = next(
                c
                for c in proposal.field_choices
                if c["conflict_id"] == selection["conflict_id"]
            )
            choice = next(
                c
                for c in conflict["choices"]
                if c["choice_id"] == selection["choice_id"]
            )
            resolutions.append({
                "resolution_id": uuid4().hex[:12],
                "decision_id": decision_id,
                "investment_transaction_id": role_ids[conflict["leg_role"]],
                "field_name": conflict["field"],
                **{
                    key: choice[key]
                    for key in (
                        "source_type",
                        "source_origin",
                        "native_reference",
                        "observation_version",
                    )
                },
                "created_at": datetime.now(UTC),
                "retired_at": None,
            })
        return membership.build_membership_transition(
            proposed,
            resolutions=resolutions,
            semantic_id_map={
                **(prior.semantic_id_map if prior is not None else {}),
                **mapping,
            },
        )

    def preflight_batch(
        self,
        decisions: Sequence[
            tuple[
                str, Literal["accept", "reject"], Proposal, Sequence[Mapping[str, str]]
            ]
        ],
    ) -> None:
        """Prove the combined ledger before any member of a batch writes."""
        accepted = [item for item in decisions if item[1] == "accept"]
        claimed: set[tuple[str, str, str]] = set()
        prior: MembershipTransition | None = None
        for decision_id, _decision, proposal, choices in accepted:
            identities = {
                (leg["source_type"], leg["source_origin"], leg["native_reference"])
                for leg in proposal.legs
            }
            if claimed & identities or any(
                set(rejected.members) <= set(proposal.members)
                for _id, decision, rejected, _choices in decisions
                if decision == "reject"
            ):
                raise UserError(
                    "Investment batch contains conflicting complete decisions.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            claimed.update(identities)
            prior = self.prepare_acceptance(
                decision_id, proposal, field_choices=choices, prior=prior
            )

    def apply_in_active_txn(
        self,
        decision_id: str,
        *,
        decision: Literal["accept", "reject"],
        actor: str,
        field_choices: Sequence[Mapping[str, str]] = (),
        prepared: Proposal | None = None,
    ) -> AuditEvent:
        """Apply one verified whole-event decision in the batch owner's transaction."""
        proposal = prepared or self.prepare_decision(
            decision_id, decision=decision, field_choices=field_choices
        )
        if decision == "reject":
            return self._repo.reject(decision_id, actor=actor, in_outer_txn=True)
        from moneybin.services.investment_membership_service import (
            InvestmentMembershipService,
        )

        transition = self.prepare_acceptance(
            decision_id, proposal, field_choices=field_choices
        )
        root = self._repo.accept(
            decision_id,
            actor=actor,
            in_outer_txn=True,
            context={
                "prior_membership_ids": [
                    r["membership_id"] for r in transition.current
                ],
                "new_membership_ids": [r["membership_id"] for r in transition.proposed],
                "supersedes_decision_ids": list(proposal.supersedes_decision_ids),
            },
        )
        for prior_id in proposal.supersedes_decision_ids:
            self._repo.supersede(
                prior_id, actor=actor, parent_audit_id=root.audit_id, in_outer_txn=True
            )
        InvestmentMembershipService(self._db).publish_transition(
            transition, actor=actor, parent_audit_id=root.audit_id
        )
        return root

    def invalidate_pending(
        self, *, actor: str, parent_audit_id: str | None = None
    ) -> None:
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
            event = self.apply_in_active_txn(
                decision_id, decision=decision, actor=actor, field_choices=field_choices
            )
            self.invalidate_pending(actor=actor, parent_audit_id=event.audit_id)
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        record_committed_investment_decisions(
            [
                (
                    "accepted" if decision == "accept" else "rejected",
                    proposal.auto_eligible,
                )
            ],
            duration_seconds=perf_counter() - started,
        )

        if decision == "accept":
            from moneybin.services.restate import restate_after_write
            from moneybin.tables import (
                INVESTMENT_EVENT_MEMBERS,
                INVESTMENT_MATCH_FIELD_RESOLUTIONS,
            )

            restate_after_write(
                self._db,
                [INVESTMENT_EVENT_MEMBERS, INVESTMENT_MATCH_FIELD_RESOLUTIONS],
                committed_change="investment decision",
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

"""Audited investment Proposal lifecycle and human rejection transitions."""

import json
from dataclasses import asdict
from typing import Any
from uuid import uuid4

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.investments.event_planning import Proposal
from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import INVESTMENT_MATCH_DECISIONS

_COLUMNS = (
    "proposal_id",
    "algorithm_version",
    "relationship_fingerprint",
    "candidate_graph_fingerprint",
    "confidence_band",
    "status",
    "auto_eligible",
    "is_competing",
    "proposal",
    "actor",
    "created_at",
    "updated_at",
    "decided_at",
    "accepted_at",
)


class InvestmentMatchDecisionsRepo(BaseRepo):
    """Keep planner evidence and human decisions in one audited lifecycle."""

    repository = "investment_match_decisions"
    table_ref = INVESTMENT_MATCH_DECISIONS
    pk_columns = ("proposal_id",)

    def _fetch_row(self, proposal_id: str) -> dict[str, Any] | None:
        return self._fetch_one(self.table_ref, _COLUMNS, "proposal_id", proposal_id)

    def all_rows(self) -> list[dict[str, Any]]:
        """Read durable lifecycle and its complete Proposal payload."""
        cursor = self._db.execute(
            f"""SELECT * FROM {INVESTMENT_MATCH_DECISIONS.full_name}
                ORDER BY created_at, proposal_id"""  # noqa: S608  # fixed TableRef
        )
        columns = [column[0] for column in cursor.description]
        result: list[dict[str, Any]] = []
        for row in cursor.fetchall():
            record = dict(zip(columns, row, strict=True))
            payload = json.loads(record.pop("proposal"))
            result.append({**payload, **record})
        return result

    def count(self, *, pending: bool) -> int:
        """Count Proposals by lifecycle bucket without touching the payload.

        ``all_rows`` selects and JSON-decodes every row's complete legs and
        evidence to answer a question a bare ``COUNT(*)`` answers directly —
        a counts-only caller (``reviews(kind="summary")``) must not pay that
        cost. Mirrors ``InvestmentMatchingService.pending``/``history``'s own
        split: pending is exactly ``status = 'pending'``, history is
        everything else.
        """
        comparison = "=" if pending else "!="
        cursor = self._db.execute(
            f"""SELECT COUNT(*) FROM {INVESTMENT_MATCH_DECISIONS.full_name}
                WHERE status {comparison} ?""",  # noqa: S608  # fixed TableRef, fixed operator; bound value
            ["pending"],
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0

    def insert_pending(
        self, proposal: Proposal, *, actor: str, in_outer_txn: bool = False
    ) -> str:
        """Create one pending Proposal and its audit inside the same transaction."""
        proposal_id = uuid4().hex[:12]
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""INSERT INTO {INVESTMENT_MATCH_DECISIONS.full_name}
                    (proposal_id, algorithm_version, relationship_fingerprint,
                     candidate_graph_fingerprint, confidence_band, status,
                     auto_eligible, is_competing, proposal, actor)
                    VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)""",  # noqa: S608  # fixed TableRef; bound values
                # fixed TableRef and parameterized values
                [
                    proposal_id,
                    proposal.algorithm_version,
                    proposal.relationship_fingerprint,
                    proposal.candidate_graph_fingerprint,
                    proposal.confidence_band,
                    proposal.auto_eligible,
                    proposal.is_competing,
                    json.dumps(asdict(proposal), default=str, sort_keys=True),
                    actor,
                ],
            )
            self._emit_audit(
                action="investment_match.propose",
                target=(*self._audit_target, proposal_id),
                before=None,
                after=self._serialize_for_audit(self._fetch_row(proposal_id)),
                actor=actor,
            )
        return proposal_id

    def mark_stale(
        self,
        proposal_id: str,
        *,
        actor: str,
        in_outer_txn: bool = False,
        parent_audit_id: str | None = None,
    ) -> bool:
        """Invalidate pending evidence without recording a human decision."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(proposal_id)
            if before is None or before["status"] not in {"pending", "accepted"}:
                return False
            self._db.execute(
                f"""UPDATE {INVESTMENT_MATCH_DECISIONS.full_name}
                    SET status = 'stale', updated_at = CURRENT_TIMESTAMP
                    WHERE proposal_id = ? AND status IN ('pending', 'accepted')""",  # noqa: S608  # fixed TableRef; bound value
                # fixed TableRef and parameterized value
                [proposal_id],
            )
            self._emit_audit(
                action="investment_match.stale",
                target=(*self._audit_target, proposal_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(self._fetch_row(proposal_id)),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
        return True

    def reject(
        self, proposal_id: str, *, actor: str, in_outer_txn: bool = False
    ) -> AuditEvent:
        """Record the first human decision without changing its immutable evidence."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(proposal_id)
            if before is None or before["status"] != "pending":
                raise UserError(
                    "Investment Proposal is stale or no longer pending.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            self._db.execute(
                f"""UPDATE {INVESTMENT_MATCH_DECISIONS.full_name}
                    SET status = 'rejected', actor = ?,
                        decided_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE proposal_id = ?""",  # noqa: S608  # fixed TableRef; bound values
                [actor, proposal_id],
            )
            return self._emit_audit(
                action="investment_match.reject",
                target=(*self._audit_target, proposal_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(self._fetch_row(proposal_id)),
                actor=actor,
            )

    def accept(
        self,
        proposal_id: str,
        *,
        actor: str,
        context: dict[str, Any],
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Publish ratification and its complete prior topology audit reference."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(proposal_id)
            if before is None or before["status"] != "pending":
                raise UserError(
                    "Investment Proposal is no longer pending.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            self._db.execute(
                f"""UPDATE {INVESTMENT_MATCH_DECISIONS.full_name}
                SET status = 'accepted', actor = ?, decided_at = CURRENT_TIMESTAMP,
                    accepted_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE proposal_id = ?""",  # noqa: S608  # TableRef, bound values
                [actor, proposal_id],
            )
            return self._emit_audit(
                action="investment_match.accept",
                target=(*self._audit_target, proposal_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(self._fetch_row(proposal_id)),
                actor=actor,
                context=context,
            )

    def supersede(
        self,
        proposal_id: str,
        *,
        actor: str,
        parent_audit_id: str,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Retire one complete ratified component under its replacement decision."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(proposal_id)
            if (
                before is None
                or before["status"] not in {"accepted", "stale"}
                or before["accepted_at"] is None
            ):
                raise UserError(
                    "Prior investment Match is no longer active.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            self._db.execute(
                f"""UPDATE {INVESTMENT_MATCH_DECISIONS.full_name}
                SET status = 'reversed', actor = ?, updated_at = CURRENT_TIMESTAMP
                WHERE proposal_id = ?""",  # noqa: S608  # TableRef, bound values
                [actor, proposal_id],
            )
            return self._emit_audit(
                action="investment_match.supersede",
                target=(*self._audit_target, proposal_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(self._fetch_row(proposal_id)),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def restore_lifecycle(
        self,
        proposal_id: str,
        *,
        status: str,
        actor: str,
        parent_audit_id: str | None = None,
        undoes_operation_id: str | None = None,
    ) -> AuditEvent:
        """Publish a domain-validated reversal or restored ratified component."""
        if status not in {"accepted", "stale", "reversed"}:
            raise ValueError("Invalid restored investment lifecycle")
        before = self._fetch_row(proposal_id)
        if before is None or before["accepted_at"] is None:
            raise UserError(
                "Ratified investment topology is incomplete.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        self._db.execute(
            f"""UPDATE {INVESTMENT_MATCH_DECISIONS.full_name}
                SET status = ?, actor = ?, updated_at = CURRENT_TIMESTAMP
                WHERE proposal_id = ?""",  # noqa: S608  # fixed TableRef, bound lifecycle
            [status, actor, proposal_id],
        )
        return self._emit_audit(
            action="investment_match.reverse"
            if status == "reversed"
            else "investment_match.restore",
            target=(*self._audit_target, proposal_id),
            before=self._serialize_for_audit(before),
            after=self._serialize_for_audit(self._fetch_row(proposal_id)),
            actor=actor,
            parent_audit_id=parent_audit_id,
            is_undo=undoes_operation_id is not None,
            undoes_operation_id=undoes_operation_id,
        )

    def undo_rejection(self, event: AuditEvent, *, actor: str) -> AuditEvent:
        """Remove a negative constraint inside the domain undo transaction."""
        before = self._fetch_row(str(event.target_id))
        if (
            event.action != "investment_match.reject"
            or before is None
            or before["status"] != "rejected"
            or self._serialize_for_audit(before) != event.after_value
        ):
            raise UserError(
                "Investment rejection no longer matches its audit evidence.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        self._db.execute(
            f"""UPDATE {INVESTMENT_MATCH_DECISIONS.full_name}
                SET status = 'stale', actor = ?, updated_at = CURRENT_TIMESTAMP
                WHERE proposal_id = ?""",  # noqa: S608  # fixed TableRef; bound values
            [actor, event.target_id],
        )
        return self._emit_audit(
            action="investment_match.reject.undo",
            target=(*self._audit_target, event.target_id),
            before=self._serialize_for_audit(before),
            after=self._serialize_for_audit(self._fetch_row(str(event.target_id))),
            actor=actor,
            is_undo=True,
            undoes_operation_id=event.operation_id,
        )

    def undo_event(
        self, event: AuditEvent, *, actor: str, in_outer_txn: bool = False
    ) -> AuditEvent | None:
        """Never expose generic row-image undo as an investment decision."""
        raise UserError(
            "Investment matching is review-only; undo is unavailable.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )

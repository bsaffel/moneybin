"""Audited planner-owned pending and stale investment Proposal transitions."""

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
    """Persist immutable review evidence without human-decision methods."""

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
        self, proposal_id: str, *, actor: str, in_outer_txn: bool = False
    ) -> bool:
        """Invalidate pending evidence without recording a human decision."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(proposal_id)
            if before is None or before["status"] != "pending":
                return False
            self._db.execute(
                f"""UPDATE {INVESTMENT_MATCH_DECISIONS.full_name}
                    SET status = 'stale', updated_at = CURRENT_TIMESTAMP
                    WHERE proposal_id = ? AND status = 'pending'""",  # noqa: S608  # fixed TableRef; bound value
                # fixed TableRef and parameterized value
                [proposal_id],
            )
            self._emit_audit(
                action="investment_match.stale",
                target=(*self._audit_target, proposal_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(self._fetch_row(proposal_id)),
                actor=actor,
            )
        return True

    def undo_event(
        self, event: AuditEvent, *, actor: str, in_outer_txn: bool = False
    ) -> AuditEvent | None:
        """Never expose generic row-image undo as an investment decision."""
        raise UserError(
            "Investment matching is review-only; undo is unavailable.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )

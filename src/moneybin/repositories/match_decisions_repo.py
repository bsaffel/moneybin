"""Audited writes to ``app.match_decisions`` (same-record dedup + transfer matches).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. The matcher
(``matching/engine.py``) and ``MatchingService`` compose this instead of issuing
raw mutation SQL — match decisions are user-affecting state (``moneybin matches
confirm``/``undo`` exist), so they are routed under Invariant 10 (Resolved Design
Decision §1) even though they're written outside ``services/`` (RDD §5).

``decided_by``/``reversed_by`` are *domain* columns (``auto``/``user``/``system``)
distinct from the audit ``actor`` (the surface: ``cli``/``mcp``/``system``); the
caller supplies both.
"""

from __future__ import annotations

import json
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import MATCH_DECISIONS

_MATCH_DECISIONS_COLUMNS = (
    "match_id",
    "source_transaction_id_a",
    "source_type_a",
    "source_origin_a",
    "source_transaction_id_b",
    "source_type_b",
    "source_origin_b",
    "account_id",
    "confidence_score",
    "match_signals",
    "match_type",
    "match_tier",
    "account_id_b",
    "match_status",
    "match_reason",
    "decided_by",
    "decided_at",
    "reversed_at",
    "reversed_by",
)


class MatchDecisionsRepo(BaseRepo):
    """Audited CRUD over ``app.match_decisions``."""

    repository = "match_decisions"

    _AUDIT_TARGET = (MATCH_DECISIONS.schema, MATCH_DECISIONS.name)

    def _fetch_row(self, match_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            MATCH_DECISIONS, _MATCH_DECISIONS_COLUMNS, "match_id", match_id
        )

    def insert(
        self,
        *,
        match_id: str,
        source_transaction_id_a: str,
        source_type_a: str,
        source_origin_a: str,
        source_transaction_id_b: str,
        source_type_b: str,
        source_origin_b: str,
        account_id: str,
        confidence_score: float,
        match_signals: dict[str, Any],
        match_status: str,
        decided_by: str,
        match_tier: str | None = None,
        match_reason: str | None = None,
        match_type: str = "dedup",
        account_id_b: str | None = None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a new match decision + audit. ``target_id`` is ``match_id``.

        ``decided_at`` is stamped ``CURRENT_TIMESTAMP``; ``match_signals`` is
        stored as JSON. The caller supplies ``match_id`` (a fresh truncated UUID).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {MATCH_DECISIONS.full_name} (
                    match_id, source_transaction_id_a, source_type_a,
                    source_origin_a, source_transaction_id_b, source_type_b,
                    source_origin_b, account_id, confidence_score, match_signals,
                    match_type, match_tier, account_id_b, match_status,
                    match_reason, decided_by, decided_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    CURRENT_TIMESTAMP
                )
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    match_id,
                    source_transaction_id_a,
                    source_type_a,
                    source_origin_a,
                    source_transaction_id_b,
                    source_type_b,
                    source_origin_b,
                    account_id,
                    confidence_score,
                    json.dumps(match_signals),
                    match_type,
                    match_tier,
                    account_id_b,
                    match_status,
                    match_reason,
                    decided_by,
                ],
            )
            after = self._fetch_row(match_id)
            return self._emit_audit(
                action="match_decision.insert",
                target=(*self._AUDIT_TARGET, match_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def update_status(
        self,
        match_id: str,
        *,
        status: str,
        decided_by: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Transition a decision's status (e.g. pending → accepted/rejected).

        Re-stamps ``decided_at``/``decided_by``; captures full before/after.
        Raises ``ValueError`` when no match with this id exists.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self._fetch_row(match_id), "match_id", match_id)
            self._db.execute(
                f"""
                UPDATE {MATCH_DECISIONS.full_name}
                SET match_status = ?, decided_by = ?, decided_at = CURRENT_TIMESTAMP
                WHERE match_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [status, decided_by, match_id],
            )
            after = self._fetch_row(match_id)
            return self._emit_audit(
                action="match_decision.update_status",
                target=(*self._AUDIT_TARGET, match_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def reverse(
        self,
        match_id: str,
        *,
        reversed_by: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Reverse a decision (sets ``reversed_at``/``reversed_by``, status reversed).

        Captures the full prior row in ``before``. Raises ``ValueError`` when no
        match with this id exists.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self._fetch_row(match_id), "match_id", match_id)
            self._db.execute(
                f"""
                UPDATE {MATCH_DECISIONS.full_name}
                SET reversed_at = CURRENT_TIMESTAMP, reversed_by = ?,
                    match_status = 'reversed'
                WHERE match_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [reversed_by, match_id],
            )
            after = self._fetch_row(match_id)
            return self._emit_audit(
                action="match_decision.reverse",
                target=(*self._AUDIT_TARGET, match_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

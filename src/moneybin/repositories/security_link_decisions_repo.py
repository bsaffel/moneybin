"""Audited writes to ``app.security_link_decisions`` (M1G.4 fuzzy-match review queue).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through this repo, which pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. The ``SecurityResolver``
(Task 9) writes ``pending`` proposals here; the review surfaces (Task 12) accept
/ reject / reverse them.

``decided_by`` is the *domain* column (``auto``/``user``) distinct from the audit
``actor`` (the surface: ``cli``/``mcp``/``system``); the caller supplies both.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, ClassVar

import duckdb

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import SECURITY_LINK_DECISIONS, TableRef

_SECURITY_LINK_DECISIONS_COLUMNS = (
    "decision_id",
    "ref_kind",
    "ref_value",
    "source_type",
    "provider_ticker",
    "provider_name",
    "candidate_security_id",
    "confidence_score",
    "match_signals",
    "status",
    "decided_by",
    "match_reason",
    "decided_at",
    "reversed_at",
    "reversed_by",
)

# Columns stored as JSON-encoded text. Reads decode them so the audit
# before/after payload carries nested JSON, not a doubly-encoded string
# (AuditService json.dumps the whole payload). Writes json.dumps once.
_JSON_COLUMNS = frozenset({"match_signals"})

# Pre-quoted column list for multi-row SELECT (security.md: identifiers quoted).
_COLS = ", ".join(f'"{c}"' for c in _SECURITY_LINK_DECISIONS_COLUMNS)


def _decode_row(row: tuple[Any, ...]) -> dict[str, Any]:
    """Map a fetched row to a column → value dict, decoding JSON columns."""
    out: dict[str, Any] = {}
    for col, val in zip(_SECURITY_LINK_DECISIONS_COLUMNS, row, strict=True):
        if col in _JSON_COLUMNS and isinstance(val, str):
            out[col] = json.loads(val)
        else:
            out[col] = val
    return out


class SecurityLinkDecisionsRepo(BaseRepo):
    """Audited CRUD over ``app.security_link_decisions``."""

    repository: ClassVar[str] = "security_link_decisions"
    table_ref: ClassVar[TableRef] = SECURITY_LINK_DECISIONS
    pk_columns: ClassVar[tuple[str, ...]] = ("decision_id",)

    def _fetch_row(self, decision_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            SECURITY_LINK_DECISIONS,
            _SECURITY_LINK_DECISIONS_COLUMNS,
            "decision_id",
            decision_id,
            decode=_decode_row,
        )

    def fetch_by_id(self, decision_id: str) -> dict[str, Any] | None:
        """Read one decoded decision row by id, or None when absent. Read-only.

        Returns None when the table does not yet exist (``CatalogException``
        guard), matching ``count_pending``/``list_pending``/``history`` so a
        fresh DB yields a clean not-found rather than a raw catalog error.
        """
        try:
            return self._fetch_row(decision_id)
        except duckdb.CatalogException:
            return None

    def insert(
        self,
        *,
        ref_kind: str,
        ref_value: str,
        source_type: str,
        candidate_security_id: str,
        actor: str,
        provider_ticker: str | None = None,
        provider_name: str | None = None,
        confidence_score: float | None = None,
        match_signals: dict[str, Any] | None = None,
        match_reason: str | None = None,
        status: str = "pending",
        decided_by: str = "auto",
        decision_id: str | None = None,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a security-link decision + paired audit. ``target_id`` is ``decision_id``.

        Mints a 12-hex ``decision_id`` (Strategy 3, ``identifiers.md``) when
        ``decision_id`` is ``None``. ``decided_at`` is stamped
        ``CURRENT_TIMESTAMP``; ``match_signals`` is stored as JSON (``NULL``
        when omitted, not the literal string ``"null"``).
        """
        resolved_id = decision_id if decision_id is not None else uuid.uuid4().hex[:12]
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {SECURITY_LINK_DECISIONS.full_name} (
                    decision_id, ref_kind, ref_value, source_type,
                    provider_ticker, provider_name, candidate_security_id,
                    confidence_score, match_signals, status, decided_by,
                    match_reason, decided_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    resolved_id,
                    ref_kind,
                    ref_value,
                    source_type,
                    provider_ticker,
                    provider_name,
                    candidate_security_id,
                    confidence_score,
                    json.dumps(match_signals) if match_signals is not None else None,
                    status,
                    decided_by,
                    match_reason,
                ],
            )
            after = self._fetch_row(resolved_id)
            return self._emit_audit(
                action="security_link_decision.insert",
                target=(*self._audit_target, resolved_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def update_status(
        self,
        decision_id: str,
        *,
        status: str,
        decided_by: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Transition a pending decision to accepted or rejected.

        Raises ``ValueError`` when no decision with this id exists, when the
        current status is not ``pending``, or when ``status`` is not
        ``accepted``/``rejected``. A decision transitions through this method
        exactly once; ``reverse()`` is the only path off a terminal
        (accepted/rejected) state — this repo refuses to merge silently, so an
        already-decided row never re-decides itself.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(decision_id), "decision_id", decision_id
            )
            if before["status"] != "pending" or status not in (
                "accepted",
                "rejected",
            ):
                raise ValueError(
                    "security_link_decisions.update_status: cannot transition "
                    f"decision {decision_id} from {before['status']!r} to "
                    f"{status!r}; only pending -> accepted/rejected is allowed"
                )
            self._db.execute(
                f"""
                UPDATE {SECURITY_LINK_DECISIONS.full_name}
                SET status = ?, decided_by = ?, decided_at = CURRENT_TIMESTAMP
                WHERE decision_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [status, decided_by, decision_id],
            )
            after = self._fetch_row(decision_id)
            return self._emit_audit(
                action="security_link_decision.update_status",
                target=(*self._audit_target, decision_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def reverse(
        self,
        decision_id: str,
        *,
        reversed_by: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Reverse a decision (sets ``reversed_at``/``reversed_by``, status reversed).

        Captures the full prior row in ``before``. Raises ``ValueError`` when no
        decision with this id exists, when it is already reversed — re-reversing
        would overwrite the original reversal's audit trail
        (``reversed_at``/``reversed_by``) — or when it is still ``pending``: a
        pending row has no accept/reject decision yet to undo, so reversing it
        would silently dequeue a review item with no decision ever recorded.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(decision_id), "decision_id", decision_id
            )
            if before["status"] not in ("accepted", "rejected"):
                raise ValueError(
                    "security_link_decisions.reverse: cannot reverse decision "
                    f"{decision_id} with status {before['status']!r}; only "
                    "accepted/rejected decisions can be reversed"
                )
            self._db.execute(
                f"""
                UPDATE {SECURITY_LINK_DECISIONS.full_name}
                SET reversed_at = CURRENT_TIMESTAMP, reversed_by = ?,
                    status = 'reversed'
                WHERE decision_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [reversed_by, decision_id],
            )
            after = self._fetch_row(decision_id)
            return self._emit_audit(
                action="security_link_decision.reverse",
                target=(*self._audit_target, decision_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def list_pending(self) -> list[dict[str, Any]]:
        """Return all pending, non-reversed decisions ordered for review grouping.

        Returns ``status='pending'`` rows with ``reversed_at IS NULL``, decoded
        via ``_decode_row``, ordered ``ref_value, decision_id`` so callers can
        group proposals by the ambiguous provider ref under review (the
        security-link analog of ``AccountLinkDecisionsRepo.list_pending``'s
        ``provisional_account_id`` grouping). Returns an empty list when the
        table does not yet exist (``CatalogException`` guard), matching
        ``count_pending``/``history``. Read-only — no audit emitted.
        """
        try:
            rows = self._db.execute(
                f"SELECT {_COLS} FROM {SECURITY_LINK_DECISIONS.full_name} "  # noqa: S608  # constant column list + TableRef
                "WHERE status = 'pending' AND reversed_at IS NULL "
                "ORDER BY ref_value, decision_id",
            ).fetchall()
        except duckdb.CatalogException:
            return []
        return [_decode_row(r) for r in rows]

    def list_rejected(self) -> list[dict[str, Any]]:
        """Return all rejected, non-reversed decisions (the never-re-propose set).

        The ``SecurityResolver`` reads this as a batch cache: a
        ``(ref_kind, ref_value, candidate_security_id)`` pairing the user
        rejected is never proposed again — re-proposing it every sync would
        mean the review queue never drains. A ``reversed`` decision is NOT in
        this set (``reversed_at IS NULL``), so a reversal re-opens the
        proposal. Returns an empty list when the table does not yet exist
        (``CatalogException`` guard). Read-only — no audit emitted.
        """
        try:
            rows = self._db.execute(
                f"SELECT {_COLS} FROM {SECURITY_LINK_DECISIONS.full_name} "  # noqa: S608  # constant column list + TableRef
                "WHERE status = 'rejected' AND reversed_at IS NULL "
                "ORDER BY ref_value, decision_id",
            ).fetchall()
        except duckdb.CatalogException:
            return []
        return [_decode_row(r) for r in rows]

    def count_pending(self) -> int:
        """Pending-decision count for the review sweep (fresh DB -> 0)."""
        try:
            row = self._db.execute(
                f"SELECT COUNT(*) FROM {SECURITY_LINK_DECISIONS.full_name} "  # noqa: S608  # TableRef constant
                "WHERE status = 'pending'"
            ).fetchone()
        except duckdb.CatalogException:
            return 0
        return int(row[0]) if row else 0

    def history(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """All decisions (any status) newest-first by ``decided_at``. Read-only.

        Returns an empty list when the table does not yet exist
        (``CatalogException`` guard). A negative ``limit`` is clamped to 0 —
        DuckDB rejects a negative LIMIT (``BinderException``). No audit emitted.
        """
        limit = max(limit, 0)
        try:
            rows = self._db.execute(
                f"SELECT {_COLS} FROM {SECURITY_LINK_DECISIONS.full_name} "  # noqa: S608  # constant column list + TableRef + parameterized limit
                "ORDER BY decided_at DESC NULLS LAST, decision_id DESC LIMIT ?",
                [limit],
            ).fetchall()
        except duckdb.CatalogException:
            return []
        return [_decode_row(r) for r in rows]

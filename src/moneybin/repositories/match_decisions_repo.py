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
from dataclasses import dataclass
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


@dataclass(frozen=True)
class RepointAccountResult:
    """What :meth:`MatchDecisionsRepo.repoint_account` did.

    ``accepted_transfers_retired`` is separate from ``events`` because it is
    the only part a *user* has to be told about: every other effect of a
    re-key is bookkeeping, while reversing an accepted transfer undoes a
    decision they made. Counting it here rather than at the call site keeps
    the collapse predicate in one place — the caller cannot re-derive which
    rows collapsed without copying it.
    """

    events: tuple[AuditEvent, ...]
    accepted_transfers_retired: int


# Columns stored as JSON-encoded text. Reads decode them to Python objects so the
# audit ``before``/``after`` payload carries nested JSON, not a doubly-encoded
# string (``AuditService`` json.dumps the whole payload). Writes json.dumps once.
_JSON_COLUMNS = frozenset({"match_signals"})


def _decode_row(row: tuple[Any, ...]) -> dict[str, Any]:
    """Map a fetched row to a column → value dict, decoding JSON columns."""
    out: dict[str, Any] = {}
    for col, val in zip(_MATCH_DECISIONS_COLUMNS, row, strict=True):
        if col in _JSON_COLUMNS and isinstance(val, str):
            out[col] = json.loads(val)
        else:
            out[col] = val
    return out


class MatchDecisionsRepo(BaseRepo):
    """Audited CRUD over ``app.match_decisions``."""

    repository = "match_decisions"

    table_ref = MATCH_DECISIONS
    pk_columns = ("match_id",)

    def _fetch_row(self, match_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            MATCH_DECISIONS,
            _MATCH_DECISIONS_COLUMNS,
            "match_id",
            match_id,
            decode=_decode_row,
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
                target=(*self._audit_target, match_id),
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
                target=(*self._audit_target, match_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def repoint_account(
        self,
        *,
        from_account_id: str,
        to_account_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> RepointAccountResult:
        """Re-key decisions naming a merged-away account onto the survivor.

        An account merge re-points ``app.account_links``, but a decision row
        stores the ``account_id`` it was made under. Left behind, that row no
        longer describes any live pair: ``get_rejected_pairs`` keys its tuple on
        ``account_id`` and ``_fetch_active_dedup_decisions`` builds its
        ``NodeKey`` from it, while the same transactions now sit under the
        survivor. The rejection therefore stops matching itself, and a matcher
        run that clears the confidence threshold can auto-accept a pair the user
        explicitly said were not duplicates.

        Both columns move. ``account_id`` is the shared account on a dedup row
        and side A on a transfer; ``account_id_b`` is the transfer's second
        account and is NULL for dedup. Either can name the account that just
        went away.

        A transfer whose *other* leg is already the survivor is retired rather
        than re-keyed: both endpoints collapse onto one account, and a transfer
        between an account and itself is not a thing that can be true. Accepted
        and rejected rows are reversed; pending ones are rejected, since a
        pending row has no decision to undo.

        One audit per row, like every other method here — the undo engine
        replays rows individually, and a single event covering N updates could
        not restore them one at a time. Returns the events in mutation order,
        alongside a count of the *accepted* transfers the collapse retired —
        the caller owes the user a disclosure for those and nothing else here.
        Re-keying is idempotent: a second call finds no rows and returns empty.
        """
        if from_account_id == to_account_id:
            raise ValueError(
                "match_decisions.repoint_account: from_account_id and "
                f"to_account_id are both {to_account_id!r}"
            )
        with self._transaction(in_outer_txn=in_outer_txn):
            rows = self._db.execute(
                f"""
                SELECT match_id FROM {MATCH_DECISIONS.full_name}
                WHERE account_id = ? OR account_id_b = ?
                ORDER BY match_id
                """,  # noqa: S608  # TableRef + parameterized values
                [from_account_id, from_account_id],
            ).fetchall()
            events: list[AuditEvent] = []
            accepted_transfers_retired = 0
            for (match_id,) in rows:
                before = self._require(self._fetch_row(match_id), "match_id", match_id)
                # A transfer whose other leg is already the survivor has both
                # endpoints collapsing onto one account. Re-keying it would
                # write account_id == account_id_b: an accepted row then
                # materializes in core.bridge_transfers as a transfer from an
                # account to itself, and a pending one sits in the queue as a
                # proposal nobody can action. A transfer cannot survive its two
                # sides becoming one account, so retire it instead.
                other = (
                    before["account_id_b"]
                    if before["account_id"] == from_account_id
                    else before["account_id"]
                )
                collapsed = (
                    before["match_type"] == "transfer" and other == to_account_id
                )
                self._db.execute(
                    f"""
                    UPDATE {MATCH_DECISIONS.full_name}
                    SET account_id = CASE WHEN account_id = ? THEN ? ELSE account_id END,
                        account_id_b = CASE
                            WHEN account_id_b = ? THEN ? ELSE account_id_b
                        END
                    WHERE match_id = ?
                    """,  # noqa: S608  # TableRef + parameterized values
                    [
                        from_account_id,
                        to_account_id,
                        from_account_id,
                        to_account_id,
                        match_id,
                    ],
                )
                after = self._fetch_row(match_id)
                events.append(
                    self._emit_audit(
                        action="match_decision.repoint_account",
                        target=(*self._audit_target, match_id),
                        before=self._serialize_for_audit(before),
                        after=self._serialize_for_audit(after),
                        actor=actor,
                        parent_audit_id=parent_audit_id,
                    )
                )
                # Retirement follows the re-key rather than replacing it. The
                # row must still name a live account: transform drops the
                # provisional from core.dim_accounts moments later, and the
                # app_match_decisions_account_fk invariant checks every row
                # regardless of status, so a retired row left pointing at the
                # dead account fails it.
                if not collapsed:
                    continue
                status = before["match_status"]
                if status == "accepted":
                    # Only this branch is disclosable. A rejected row being
                    # reversed removes nothing from the ledger, and a pending
                    # one was never the user's decision to undo.
                    accepted_transfers_retired += 1
                if status in ("accepted", "rejected"):
                    events.append(
                        self.reverse(
                            match_id,
                            reversed_by="system",
                            actor=actor,
                            parent_audit_id=parent_audit_id,
                            in_outer_txn=True,
                        )
                    )
                elif status == "pending":
                    events.append(
                        self.update_status(
                            match_id,
                            status="rejected",
                            decided_by="system",
                            actor=actor,
                            parent_audit_id=parent_audit_id,
                            in_outer_txn=True,
                        )
                    )
                # The fourth status, `reversed`, needs nothing further and is
                # named here because two branches covering three of four reads
                # like an omission. The re-key above already ran unconditionally,
                # so the row still points at a live account; there is no standing
                # decision left to undo, and re-reversing it would overwrite the
                # original reversal's audit trail.
            return RepointAccountResult(
                events=tuple(events),
                accepted_transfers_retired=accepted_transfers_retired,
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
        match with this id exists, when it is already reversed — re-reversing
        would overwrite the original reversal's audit trail
        (``reversed_at``/``reversed_by``) — or when it is still ``pending``: a
        pending row has no accept/reject decision yet to undo, so reversing it
        would silently dequeue a review item with no decision ever recorded.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self._fetch_row(match_id), "match_id", match_id)
            if before["match_status"] not in ("accepted", "rejected"):
                raise ValueError(
                    "match_decisions.reverse: cannot reverse match "
                    f"{match_id} with status {before['match_status']!r}; only "
                    "accepted/rejected decisions can be reversed"
                )
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
                target=(*self._audit_target, match_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def accept_pending(
        self,
        *,
        match_type: str | None = None,
        decided_by: str,
        actor: str,
        in_outer_txn: bool = False,
    ) -> list[str]:
        """Accept every pending, non-reversed match (optionally filtered by type).

        Bulk acceptance is the per-row ``update_status`` applied inside one
        transaction, so each acceptance emits its own paired ``app.audit_log``
        row (Invariant 10) and the batch is all-or-nothing. ``match_type`` is a
        code-supplied filter (validated by the caller); ``decided_by`` is the
        domain column, ``actor`` the audit surface.

        Returns the ids it flipped rather than their count: a caller that also
        runs the transfer reconciliation needs to know *which* rows were its
        own, because the reconciliation can reverse one of them inside the same
        transaction. Re-deriving that set from the filter would duplicate this
        predicate and drift from it.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            where = "WHERE match_status = 'pending' AND reversed_at IS NULL"
            params: list[object] = []
            if match_type is not None:
                where += " AND match_type = ?"
                params.append(match_type)
            rows = self._db.execute(
                f"SELECT match_id FROM {MATCH_DECISIONS.full_name} "  # noqa: S608  # TableRef + literal WHERE; value parameterized
                f"{where} ORDER BY match_id",
                params,
            ).fetchall()
            for (match_id,) in rows:
                self.update_status(
                    match_id,
                    status="accepted",
                    decided_by=decided_by,
                    actor=actor,
                    in_outer_txn=True,
                )
            return [match_id for (match_id,) in rows]

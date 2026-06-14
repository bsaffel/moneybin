"""Audited writes to ``app.account_link_decisions`` (M1S merge-proposal queue).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through this repo, which pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. The ``AccountResolver``
(M1S.2) writes ``pending`` proposals here; the review surfaces (M1S.5) accept /
reject / reverse them.

``decided_by`` is the *domain* column (``auto``/``user``) distinct from the audit
``actor`` (the surface: ``cli``/``mcp``/``system``); the caller supplies both.
"""

from __future__ import annotations

import json
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import ACCOUNT_LINK_DECISIONS

_ACCOUNT_LINK_DECISIONS_COLUMNS = (
    "decision_id",
    "provisional_account_id",
    "candidate_account_id",
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


def _decode_row(row: tuple[Any, ...]) -> dict[str, Any]:
    """Map a fetched row to a column → value dict, decoding JSON columns."""
    out: dict[str, Any] = {}
    for col, val in zip(_ACCOUNT_LINK_DECISIONS_COLUMNS, row, strict=True):
        if col in _JSON_COLUMNS and isinstance(val, str):
            out[col] = json.loads(val)
        else:
            out[col] = val
    return out


class AccountLinkDecisionsRepo(BaseRepo):
    """Audited CRUD over ``app.account_link_decisions``."""

    repository = "account_link_decisions"

    table_ref = ACCOUNT_LINK_DECISIONS
    pk_columns = ("decision_id",)

    def _fetch_row(self, decision_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            ACCOUNT_LINK_DECISIONS,
            _ACCOUNT_LINK_DECISIONS_COLUMNS,
            "decision_id",
            decision_id,
            decode=_decode_row,
        )

    def insert(
        self,
        *,
        decision_id: str,
        provisional_account_id: str,
        candidate_account_id: str,
        confidence_score: float | None,
        match_signals: dict[str, Any],
        decided_by: str,
        actor: str,
        status: str = "pending",
        match_reason: str | None = None,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a merge-proposal decision + paired audit. ``target_id`` is ``decision_id``.

        ``decided_at`` is stamped ``CURRENT_TIMESTAMP``; ``match_signals`` is stored
        as JSON. The caller supplies ``decision_id`` (a fresh truncated UUID).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {ACCOUNT_LINK_DECISIONS.full_name} (
                    decision_id, provisional_account_id, candidate_account_id,
                    confidence_score, match_signals, status, decided_by,
                    match_reason, decided_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    decision_id,
                    provisional_account_id,
                    candidate_account_id,
                    confidence_score,
                    json.dumps(match_signals),
                    status,
                    decided_by,
                    match_reason,
                ],
            )
            after = self._fetch_row(decision_id)
            return self._emit_audit(
                action="account_link_decision.insert",
                target=(*self._audit_target, decision_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

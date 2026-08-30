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

import uuid
from collections.abc import Callable
from typing import Any, ClassVar

from moneybin.database import Database
from moneybin.repositories.base import LinkDecisionsRepoBase
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


def _refresh_security_link_pending_gauge(db: Database) -> None:
    """Avoid a repository-to-service import cycle until the gauge is needed."""
    from moneybin.services.security_resolver import (  # noqa: PLC0415 — repo→service import must stay lazy
        refresh_security_link_pending_gauge,
    )

    refresh_security_link_pending_gauge(db)


class SecurityLinkDecisionsRepo(LinkDecisionsRepoBase):
    """Audited CRUD over ``app.security_link_decisions``."""

    repository: ClassVar[str] = "security_link_decisions"
    table_ref: ClassVar[TableRef] = SECURITY_LINK_DECISIONS
    pk_columns: ClassVar[tuple[str, ...]] = ("decision_id",)
    columns: ClassVar[tuple[str, ...]] = _SECURITY_LINK_DECISIONS_COLUMNS
    json_columns: ClassVar[frozenset[str]] = frozenset({"match_signals"})
    action_prefix: ClassVar[str] = "security_link_decision"
    pending_order_by: ClassVar[tuple[str, ...]] = ("ref_value", "decision_id")
    pending_gauge_hook: ClassVar[Callable[[Database], None] | None] = (
        _refresh_security_link_pending_gauge
    )

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
        return self._insert_decision(
            values={
                "decision_id": resolved_id,
                "ref_kind": ref_kind,
                "ref_value": ref_value,
                "source_type": source_type,
                "provider_ticker": provider_ticker,
                "provider_name": provider_name,
                "candidate_security_id": candidate_security_id,
                "confidence_score": confidence_score,
                "match_signals": match_signals,
                "status": status,
                "decided_by": decided_by,
                "match_reason": match_reason,
            },
            actor=actor,
            parent_audit_id=parent_audit_id,
            in_outer_txn=in_outer_txn,
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
        before = self._require(self._fetch_row(decision_id), "decision_id", decision_id)
        if before["status"] != "pending" or status not in ("accepted", "rejected"):
            raise ValueError(
                "security_link_decisions.update_status: cannot transition "
                f"decision {decision_id} from {before['status']!r} to "
                f"{status!r}; only pending -> accepted/rejected is allowed"
            )
        return self._update_status(
            decision_id,
            status=status,
            decided_by=decided_by,
            actor=actor,
            before=before,
            parent_audit_id=parent_audit_id,
            in_outer_txn=in_outer_txn,
        )

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
        return self._list_with_status(
            status="rejected", order_by=("ref_value", "decision_id")
        )

    def count_pending(self) -> int:
        """Pending-decision count for the review sweep (fresh DB -> 0)."""
        return self._count_pending()

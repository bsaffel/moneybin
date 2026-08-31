"""Audited writes to ``app.merchant_link_decisions`` (M1T fuzzy-match review queue).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through this repo, which pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. The ``MerchantResolver``
(M1T) writes ``pending`` proposals here; the review surfaces accept / reject /
reverse them.

``decided_by`` is the *domain* column (``auto``/``user``) distinct from the audit
``actor`` (the surface: ``cli``/``mcp``/``system``); the caller supplies both.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, ClassVar

from moneybin.database import Database
from moneybin.repositories.base import LinkDecisionsRepoBase
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import MERCHANT_LINK_DECISIONS, TableRef

_MERCHANT_LINK_DECISIONS_COLUMNS = (
    "decision_id",
    "ref_kind",
    "ref_value",
    "source_type",
    "provider_merchant_name",
    "candidate_merchant_id",
    "confidence_score",
    "match_signals",
    "status",
    "decided_by",
    "match_reason",
    "decided_at",
    "reversed_at",
    "reversed_by",
)


def _refresh_merchant_link_pending_gauge(db: Database) -> None:
    """Avoid a repository-to-service import cycle until the gauge is needed."""
    from moneybin.services.merchant_resolver import (  # noqa: PLC0415 — repo→service import must stay lazy
        refresh_merchant_link_pending_gauge,
    )

    refresh_merchant_link_pending_gauge(db)


class MerchantLinkDecisionsRepo(LinkDecisionsRepoBase):
    """Audited CRUD over ``app.merchant_link_decisions``."""

    repository: ClassVar[str] = "merchant_link_decisions"
    table_ref: ClassVar[TableRef] = MERCHANT_LINK_DECISIONS
    pk_columns: ClassVar[tuple[str, ...]] = ("decision_id",)
    columns: ClassVar[tuple[str, ...]] = _MERCHANT_LINK_DECISIONS_COLUMNS
    json_columns: ClassVar[frozenset[str]] = frozenset({"match_signals"})
    action_prefix: ClassVar[str] = "merchant_link_decision"
    pending_order_by: ClassVar[tuple[str, ...]] = ("decided_at", "decision_id")
    pending_gauge_hook: ClassVar[Callable[[Database], None] | None] = (
        _refresh_merchant_link_pending_gauge
    )

    def insert(
        self,
        *,
        decision_id: str,
        ref_kind: str,
        ref_value: str,
        source_type: str,
        provider_merchant_name: str | None = None,
        candidate_merchant_id: str,
        confidence_score: float | None,
        match_signals: dict[str, Any],
        decided_by: str,
        actor: str,
        status: str = "pending",
        match_reason: str | None = None,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a merchant-link decision + paired audit. ``target_id`` is ``decision_id``.

        ``decided_at`` is stamped ``CURRENT_TIMESTAMP``; ``match_signals`` is stored
        as JSON. The caller supplies ``decision_id`` (a fresh truncated UUID).
        """
        return self._insert_decision(
            values={
                "decision_id": decision_id,
                "ref_kind": ref_kind,
                "ref_value": ref_value,
                "source_type": source_type,
                "provider_merchant_name": provider_merchant_name,
                "candidate_merchant_id": candidate_merchant_id,
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
        """Transition a decision's status (e.g. pending → accepted/rejected).

        Re-stamps ``decided_at``/``decided_by``; captures full before/after.
        Raises ``ValueError`` when no decision with this id exists.
        """
        return self._update_status(
            decision_id,
            status=status,
            decided_by=decided_by,
            actor=actor,
            parent_audit_id=parent_audit_id,
            in_outer_txn=in_outer_txn,
        )

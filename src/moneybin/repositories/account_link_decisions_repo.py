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

from collections.abc import Callable
from typing import Any, ClassVar

from moneybin.database import Database
from moneybin.repositories.base import LinkDecisionsRepoBase
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import ACCOUNT_LINK_DECISIONS, TableRef

_ACCOUNT_LINK_DECISIONS_COLUMNS = (
    "decision_id",
    "provisional_account_id",
    "candidate_account_id",
    "confidence_score",
    "match_signals",
    "status",
    "decided_by",
    "match_reason",
    "provisional_display_name",
    "candidate_display_name",
    "decided_at",
    "reversed_at",
    "reversed_by",
)

# The frozen display names arrived in V051, and `get_database(read_only=True)`
# skips migrations — so the first read after an upgrade can land on a table that
# never got them. The same list with literal NULLs in their place, positions
# preserved so `_decode_row`'s strict zip still lines up.
_V051_COLUMNS = frozenset({"provisional_display_name", "candidate_display_name"})
_PRE_V051_COLS = ", ".join(
    f'NULL AS "{c}"' if c in _V051_COLUMNS else f'"{c}"'
    for c in _ACCOUNT_LINK_DECISIONS_COLUMNS
)


def _refresh_account_link_pending_gauge(db: Database) -> None:
    """Avoid a repository-to-service import cycle until the gauge is needed."""
    from moneybin.services.account_resolver import (  # noqa: PLC0415 — repo→service import must stay lazy
        refresh_account_link_pending_gauge,
    )

    refresh_account_link_pending_gauge(db)


class AccountLinkDecisionsRepo(LinkDecisionsRepoBase):
    """Audited CRUD over ``app.account_link_decisions``."""

    repository: ClassVar[str] = "account_link_decisions"
    table_ref: ClassVar[TableRef] = ACCOUNT_LINK_DECISIONS
    pk_columns: ClassVar[tuple[str, ...]] = ("decision_id",)
    columns: ClassVar[tuple[str, ...]] = _ACCOUNT_LINK_DECISIONS_COLUMNS
    json_columns: ClassVar[frozenset[str]] = frozenset({"match_signals"})
    action_prefix: ClassVar[str] = "account_link_decision"
    pending_order_by: ClassVar[tuple[str, ...]] = (
        "provisional_account_id",
        "decision_id",
    )
    pending_gauge_hook: ClassVar[Callable[[Database], None] | None] = (
        _refresh_account_link_pending_gauge
    )

    # Resolved on first read, then cached for the connection's lifetime.
    _has_v051_columns: bool | None = None

    def _columns_sql(self) -> str:
        """SELECT column list, degrading to NULLs on a pre-V051 table.

        A read-only open skips migrations, so an upgraded database whose first
        operation is a read still has the old shape. NULL is the honest answer
        and the one callers already handle: it means "never frozen", which
        sends them back to resolving the name live. The sibling
        ``CatalogException`` guards cannot cover this — DuckDB raises
        ``BinderException`` for a missing column and reserves
        ``CatalogException`` for a missing table. Mirrors
        ``AuditService._undo_columns_sql``.
        """
        if self._has_v051_columns is None:
            row = self._db.conn.execute(
                "SELECT 1 FROM information_schema.columns "
                "WHERE table_schema = 'app' "
                "AND table_name = 'account_link_decisions' "
                "AND column_name = 'provisional_display_name'"
            ).fetchone()
            self._has_v051_columns = row is not None
        return super()._columns_sql() if self._has_v051_columns else _PRE_V051_COLS

    def _fetch_row(self, decision_id: str) -> dict[str, Any] | None:
        # Not BaseRepo._fetch_one: it quotes every column as an identifier, and
        # the pre-V051 projection carries `NULL AS ...` expressions instead.
        row = self._db.execute(
            f"SELECT {self._columns_sql()} FROM {ACCOUNT_LINK_DECISIONS.full_name} "  # noqa: S608  # constant column list + TableRef
            'WHERE "decision_id" = ?',
            [decision_id],
        ).fetchone()
        return None if row is None else self._decode_row(row)

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
        return self._insert_decision(
            values={
                "decision_id": decision_id,
                "provisional_account_id": provisional_account_id,
                "candidate_account_id": candidate_account_id,
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
        provisional_display_name: str,
        candidate_display_name: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Transition a decision's status (e.g. pending → accepted/rejected).

        Re-stamps ``decided_at``/``decided_by``; captures full before/after.
        Raises ``ValueError`` when no decision with this id exists.

        Both display names are required rather than optional because this is the
        last moment either one can be read. Accepting re-points every accepted
        link off the provisional account, so the next transform drops it from
        ``core.dim_accounts`` and the raw fallback loses its join — a caller that
        forgot to pass them would leave the record of an irreversible merge as
        two opaque ids, and nothing would fail at the time. An empty string is a
        legitimate value (nothing named the account); omission is not a choice
        the signature offers.
        """
        return self._update_status(
            decision_id,
            status=status,
            decided_by=decided_by,
            actor=actor,
            extra_values={
                "provisional_display_name": provisional_display_name,
                "candidate_display_name": candidate_display_name,
            },
            parent_audit_id=parent_audit_id,
            in_outer_txn=in_outer_txn,
        )

"""AccountLinksService — review-queue facade for ``app.account_link_decisions``.

Mirrors :mod:`moneybin.services.matching_service`: a thin service that composes
two Invariant-10 repos (``AccountLinksRepo`` and ``AccountLinkDecisionsRepo``)
and coordinates multi-write atomic operations using the same
``db.begin() / db.commit() / db.rollback()`` pattern.

``actor`` is the audit surface (``cli``/``mcp``); ``decided_by`` is the domain
column (``user``/``system``/``auto``). The caller supplies both.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import duckdb

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_resolution_types import (
    PendingLinkCandidate,
    PendingLinkGroup,
)
from moneybin.tables import ACCOUNT_LINK_DECISIONS, ACCOUNT_LINKS, DIM_ACCOUNTS

logger = logging.getLogger(__name__)

# Column order matches app_account_link_decisions.sql (and the repo's constant).
_DECISION_COLUMNS = (
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
_DECISION_COLS = ", ".join(f'"{c}"' for c in _DECISION_COLUMNS)


def _signal_from_decoded(match_signals: Any) -> str:
    """Extract the 'signal' key from an already-decoded match_signals dict.

    ``list_pending()`` returns ``match_signals`` as a decoded ``dict``; the
    ``Any`` annotation covers the raw DB read path too (used by ``history``).
    Uses try/except to avoid pyright's ``dict[Unknown, Unknown]`` narrowing
    issue after ``isinstance`` checks on ``Any``-typed inputs.
    """
    try:
        return str(match_signals["signal"])
    except (KeyError, TypeError):
        return ""


def _decode_decision_row(row: tuple[Any, ...]) -> dict[str, Any]:
    """Map a raw DB row to a dict, decoding the JSON ``match_signals`` column."""
    out: dict[str, Any] = {}
    for col, val in zip(_DECISION_COLUMNS, row, strict=True):
        if col == "match_signals" and isinstance(val, str):
            out[col] = json.loads(val)
        else:
            out[col] = val
    return out


def _resolve_display_name(db: Database, account_id: str) -> str:
    """Return ``display_name`` from ``core.dim_accounts``; empty string when absent.

    Guards ``duckdb.CatalogException`` so callers work before the core layer is
    materialized (e.g. during initial import before a SQLMesh run).
    """
    try:
        row = db.execute(
            f"SELECT display_name FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ?",  # noqa: S608  # TableRef constant + parameterized value
            [account_id],
        ).fetchone()
        return (row[0] or "") if row else ""
    except duckdb.CatalogException:
        return ""


class AccountLinksService:
    """Review-queue facade over ``app.account_link_decisions`` + ``app.account_links``.

    Composes ``AccountLinkDecisionsRepo`` and ``AccountLinksRepo`` for all
    mutations (Invariant 10). Multi-step atomic operations use
    ``db.begin()`` / ``db.commit()`` / ``db.rollback()`` with each repo method
    called via ``in_outer_txn=True`` — the same pattern as
    :class:`~moneybin.services.matching_service.MatchingService.set_status`.
    """

    def __init__(self, db: Database, *, actor: str = "cli") -> None:
        """Initialize with a Database and the audit surface actor."""
        self._db = db
        self._actor = actor
        self._links = AccountLinksRepo(db)
        self._decisions = AccountLinkDecisionsRepo(db)

    # ------------------------------------------------------------------
    # Read-only methods
    # ------------------------------------------------------------------

    def count_pending(self) -> int:
        """Number of DISTINCT provisional accounts with pending, non-reversed decisions.

        The review *unit* is the provisional account, not the raw decision row —
        one provisional with two candidate proposals counts as one item, not two.
        Returns 0 when the table does not yet exist.
        """
        try:
            row = self._db.execute(
                f"""
                SELECT COUNT(DISTINCT provisional_account_id)
                FROM {ACCOUNT_LINK_DECISIONS.full_name}
                WHERE status = 'pending' AND reversed_at IS NULL
                """,  # noqa: S608  # TableRef constant, no user values
            ).fetchone()
            return int(row[0]) if row else 0
        except duckdb.CatalogException:
            return 0

    def pending(self) -> list[PendingLinkGroup]:
        """Return pending decisions grouped by provisional account.

        Reads ``AccountLinkDecisionsRepo.list_pending()`` (already ordered
        ``provisional_account_id, decision_id``) and groups into
        ``PendingLinkGroup`` structs. Display names are resolved from
        ``core.dim_accounts``; empty string when the row is absent or the
        table is not yet materialized (``CatalogException`` guard).
        Read-only — no audit emitted.
        """
        rows = self._decisions.list_pending()
        if not rows:
            return []

        # list_pending() orders by provisional_account_id, decision_id — group in order.
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            pid = row["provisional_account_id"]
            groups.setdefault(pid, []).append(row)

        result: list[PendingLinkGroup] = []
        for provisional_id, decisions in groups.items():
            prov_display = _resolve_display_name(self._db, provisional_id)
            candidates = tuple(
                PendingLinkCandidate(
                    decision_id=d["decision_id"],
                    candidate_account_id=d["candidate_account_id"],
                    candidate_display_name=_resolve_display_name(
                        self._db, d["candidate_account_id"]
                    ),
                    confidence=d["confidence_score"],
                    # match_signals is already decoded by list_pending(); cast for pyright.
                    signal=_signal_from_decoded(d["match_signals"]),
                )
                for d in decisions
            )
            result.append(
                PendingLinkGroup(
                    provisional_account_id=provisional_id,
                    provisional_display_name=prov_display,
                    candidates=candidates,
                )
            )
        return result

    def history(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """All decisions (any status) newest-first by ``decided_at``. Read-only.

        Mirrors ``MatchingService.get_log``. Returns an empty list when the
        table does not yet exist (``CatalogException`` guard).
        """
        try:
            rows = self._db.execute(
                f"""
                SELECT {_DECISION_COLS}
                FROM {ACCOUNT_LINK_DECISIONS.full_name}
                ORDER BY decided_at DESC NULLS LAST
                LIMIT ?
                """,  # noqa: S608  # constant column list + TableRef + parameterized limit
                [limit],
            ).fetchall()
        except duckdb.CatalogException:
            return []
        return [_decode_decision_row(r) for r in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_decision(self, decision_id: str) -> dict[str, Any] | None:
        """Read one decision row by id. Returns None when not found."""
        row = self._db.execute(
            f"SELECT {_DECISION_COLS} FROM {ACCOUNT_LINK_DECISIONS.full_name} WHERE decision_id = ?",  # noqa: S608  # constant columns + TableRef + parameterized pk
            [decision_id],
        ).fetchone()
        return _decode_decision_row(row) if row else None

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def set(  # noqa: A003  # mirrors the existing set_status verb shape; "set" is the surface verb
        self,
        decision_id: str,
        *,
        target_account_id: str | None,
        decided_by: str = "user",
    ) -> None:
        """Accept (merge) or standalone-reject a pending link decision atomically.

        ``target_account_id`` is not None (accept → merge):
          1. Validates the arg matches the decision's own ``candidate_account_id``.
          2. Re-points every accepted ``source_native`` link for the provisional
             account onto ``target_account_id`` via ``AccountLinksRepo.repoint``.
          3. Marks the named decision ``accepted``.
          4. Auto-rejects every other pending, non-reversed decision for the
             same provisional account.

        ``target_account_id`` is None (standalone):
          Rejects every pending, non-reversed decision for the provisional
          account. No repoint — the provisional stays its own canonical account.

        All writes run inside one ``db.begin()`` / ``db.commit()`` transaction
        (matching :meth:`MatchingService.set_status`). Each repo method is
        called with ``in_outer_txn=True`` so it joins the enclosing transaction
        instead of opening a nested one.

        Raises ``UserError`` when:
        - ``decision_id`` is not found (MUTATION_NOT_FOUND).
        - The decision is not in ``pending`` status (MUTATION_CONSTRAINT_VIOLATION).
        - ``target_account_id`` does not match the decision's ``candidate_account_id``
          (MUTATION_INVALID_INPUT).
        """
        self._db.begin()
        try:
            decision = self._fetch_decision(decision_id)
            if decision is None:
                raise UserError(
                    f"No account-link decision found for id {decision_id!r}.",
                    code=error_codes.MUTATION_NOT_FOUND,
                )
            if decision["status"] != "pending":
                raise UserError(
                    f"Decision {decision_id!r} is {decision['status']!r}, not pending.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )

            provisional_id = decision["provisional_account_id"]

            if target_account_id is not None:
                # Accept path: validate confirming arg then repoint + accept + auto-reject.
                if target_account_id != decision["candidate_account_id"]:
                    raise UserError(
                        "target_account_id does not match the candidate named in "
                        f"decision {decision_id!r}; pass the decision's own "
                        "candidate_account_id as a confirming safety check.",
                        code=error_codes.MUTATION_INVALID_INPUT,
                    )
                # Re-point all accepted source_native links for the provisional.
                links = self._db.execute(
                    f"""
                    SELECT link_id FROM {ACCOUNT_LINKS.full_name}
                    WHERE account_id = ?
                      AND ref_kind = 'source_native'
                      AND status = 'accepted'
                    """,  # noqa: S608  # TableRef constant + parameterized values
                    [provisional_id],
                ).fetchall()
                for (link_id,) in links:
                    self._links.repoint(
                        link_id=link_id,
                        new_account_id=target_account_id,
                        decided_by=decided_by,
                        actor=self._actor,
                        in_outer_txn=True,
                    )
                # Accept the named decision.
                self._decisions.update_status(
                    decision_id,
                    status="accepted",
                    decided_by=decided_by,
                    actor=self._actor,
                    in_outer_txn=True,
                )
                # Auto-reject sibling pending decisions on the same provisional.
                sibling_rows = self._db.execute(
                    f"""
                    SELECT decision_id FROM {ACCOUNT_LINK_DECISIONS.full_name}
                    WHERE provisional_account_id = ?
                      AND decision_id != ?
                      AND status = 'pending'
                      AND reversed_at IS NULL
                    """,  # noqa: S608  # TableRef constant + parameterized values
                    [provisional_id, decision_id],
                ).fetchall()
                for (sid,) in sibling_rows:
                    self._decisions.update_status(
                        sid,
                        status="rejected",
                        decided_by=decided_by,
                        actor=self._actor,
                        in_outer_txn=True,
                    )
            else:
                # Standalone path: reject every pending decision for this provisional.
                pending_rows = self._db.execute(
                    f"""
                    SELECT decision_id FROM {ACCOUNT_LINK_DECISIONS.full_name}
                    WHERE provisional_account_id = ?
                      AND status = 'pending'
                      AND reversed_at IS NULL
                    """,  # noqa: S608  # TableRef constant + parameterized values
                    [provisional_id],
                ).fetchall()
                for (did,) in pending_rows:
                    self._decisions.update_status(
                        did,
                        status="rejected",
                        decided_by=decided_by,
                        actor=self._actor,
                        in_outer_txn=True,
                    )

            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise

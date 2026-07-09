"""Audited writes to ``app.lot_selections`` (specific-identification overrides).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo``. Records, for one disposal, which lots to
draw from and how much — an override of the default FIFO fallback (Phase A
DDL, ``app_lot_selections.sql``). Composite PK
``(investment_transaction_id, lot_id)``.

``set_for_disposal`` is a **declarative replace**: the entire selection set for
a disposal is captured, deleted, and re-inserted from ``selections`` in one
transaction, then audited as a SINGLE ``lot_selections.set`` row (not one row
per lot) — the before/after images are collection-shaped
(``{"investment_transaction_id", "selections": [...]}``), not row-shaped. An
empty ``selections`` list clears all overrides for that disposal, falling back
to FIFO. This repo does no validation (existence of the disposal/lots, or that
selected quantities are sane) — that is a service-layer concern (Task 14).

Because the audit capture is collection-shaped rather than keyed by
``pk_columns``, the generic :meth:`BaseRepo.undo_event` cannot reverse a
``lot_selections.set`` event — ``_require_capture`` raises ``UserError``
(no ``lot_id`` key to locate a row by). This is an accepted, documented
degradation, not a bug.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import LOT_SELECTIONS


def _selections_payload(
    investment_transaction_id: str, rows: list[tuple[str, Decimal]]
) -> dict[str, Any]:
    """Build the collection-shaped, JSON-safe before/after audit payload.

    Sorted by ``lot_id`` for a deterministic payload across replace calls;
    quantity is ``str(Decimal)`` matching ``_serialize_for_audit``'s convention
    (not routed through it — that helper doesn't recurse into nested lists).
    """
    return {
        "investment_transaction_id": investment_transaction_id,
        "selections": [
            {"lot_id": lot_id, "quantity": str(quantity)}
            for lot_id, quantity in sorted(rows, key=lambda r: r[0])
        ],
    }


class LotSelectionsRepo(BaseRepo):
    """Audited declarative set/clear over ``app.lot_selections``."""

    repository = "lot_selections"

    table_ref = LOT_SELECTIONS
    pk_columns = ("investment_transaction_id", "lot_id")

    def _current_selections(
        self, investment_transaction_id: str
    ) -> list[tuple[str, Decimal]]:
        rows = self._db.execute(
            "SELECT lot_id, quantity FROM "  # noqa: S608  # TableRef + parameterized values
            f"{LOT_SELECTIONS.full_name} "
            "WHERE investment_transaction_id = ? ORDER BY lot_id",
            [investment_transaction_id],
        ).fetchall()
        return [(lot_id, Decimal(quantity)) for lot_id, quantity in rows]

    def set_for_disposal(
        self,
        *,
        investment_transaction_id: str,
        selections: list[tuple[str, Decimal]],
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Replace the whole lot-selection set for one disposal + one audit row.

        DELETEs every existing ``(investment_transaction_id, lot_id)`` row for
        this disposal, then INSERTs one row per ``(lot_id, quantity)`` in
        ``selections``. An empty list clears all overrides. Emits exactly ONE
        ``lot_selections.set`` audit row for the whole set, not one per lot.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._current_selections(investment_transaction_id)
            self._db.execute(
                "DELETE FROM "  # noqa: S608  # TableRef + parameterized values
                f"{LOT_SELECTIONS.full_name} WHERE investment_transaction_id = ?",
                [investment_transaction_id],
            )
            for lot_id, quantity in selections:
                self._db.execute(
                    "INSERT INTO "  # noqa: S608  # TableRef + parameterized values
                    f"{LOT_SELECTIONS.full_name} "
                    "(investment_transaction_id, lot_id, quantity) VALUES (?, ?, ?)",
                    [investment_transaction_id, lot_id, quantity],
                )
            after = self._current_selections(investment_transaction_id)
            return self._emit_audit(
                action="lot_selections.set",
                target=(*self._audit_target, investment_transaction_id),
                before=_selections_payload(investment_transaction_id, before),
                after=_selections_payload(investment_transaction_id, after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def list_for_disposal(
        self, investment_transaction_id: str
    ) -> list[tuple[str, Decimal]]:
        """Return the current ``(lot_id, quantity)`` selections, ordered by ``lot_id``."""
        return self._current_selections(investment_transaction_id)

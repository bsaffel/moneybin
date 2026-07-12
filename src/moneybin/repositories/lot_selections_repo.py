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
``lot_selections.set`` event — ``_require_capture`` would refuse (no ``lot_id``
key to locate a row by). This repo overrides ``undo_event`` instead: since
``set_for_disposal`` is already a whole-set replace and the audit row's
``before_value["selections"]`` is the complete prior list, undoing is exact —
replay that list through the same DELETE+INSERT replace path
(:meth:`_replace_selections`). One code path covers both directions: undoing a
replace restores the prior (non-empty) set, and undoing the very first ``set``
for a disposal restores an empty set, i.e. clears it back to FIFO — the
"prior state" is just an empty list in that case, not a special case.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from moneybin import error_codes
from moneybin.errors import UserError
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

    def _replace_selections(
        self, investment_transaction_id: str, selections: list[tuple[str, Decimal]]
    ) -> None:
        """DELETE + re-INSERT the whole selection set for one disposal.

        The shared whole-set-replace mechanics. Used by both
        :meth:`set_for_disposal` (forward) and :meth:`undo_event` (restore) —
        undo of a whole-set replace is itself a whole-set replace, just with
        the prior list instead of the caller's new one.
        """
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
            self._replace_selections(investment_transaction_id, selections)
            after = self._current_selections(investment_transaction_id)
            return self._emit_audit(
                action="lot_selections.set",
                target=(*self._audit_target, investment_transaction_id),
                before=_selections_payload(investment_transaction_id, before),
                after=_selections_payload(investment_transaction_id, after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def undo_event(
        self,
        event: AuditEvent,
        *,
        actor: str,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Restore the prior lot-selection set for one disposal.

        ``set_for_disposal`` never partially updates a disposal's set — it
        replaces the whole thing — and its audit capture stores the complete
        prior list in ``before_value["selections"]``. That makes undo exact:
        replay the prior list through :meth:`_replace_selections`, the same
        DELETE+INSERT path the forward ``set`` used. No row-level
        reconstruction needed, and no branch between "restore a prior set" and
        "clear the first set" — the latter is simply the case where the prior
        list is empty.
        """
        before = event.before_value
        after = event.after_value
        if before == after:
            return None
        if before is None or after is None:
            # set_for_disposal always captures both images (collection-shaped,
            # never null) — this guards a malformed/foreign audit row rather
            # than a real forward-write outcome.
            raise UserError(
                f"Cannot undo {event.action!r}: its audit row is missing a "
                f"before/after image on {event.target_table} — not reversible.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        disposal_id = str(before["investment_transaction_id"])
        prior_selections = [
            (str(row["lot_id"]), Decimal(str(row["quantity"])))
            for row in before["selections"]
        ]
        with self._transaction(in_outer_txn=in_outer_txn):
            self._replace_selections(disposal_id, prior_selections)
            restored = self._current_selections(disposal_id)
            return self._emit_audit(
                action=f"{event.action}.undo",
                target=(event.target_schema, event.target_table, disposal_id),
                before=after,
                after=_selections_payload(disposal_id, restored),
                actor=actor,
                is_undo=True,
                undoes_operation_id=event.operation_id,
            )

    def list_for_disposal(
        self, investment_transaction_id: str
    ) -> list[tuple[str, Decimal]]:
        """Return the current ``(lot_id, quantity)`` selections, ordered by ``lot_id``."""
        return self._current_selections(investment_transaction_id)

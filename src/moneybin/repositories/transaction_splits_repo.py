"""Audited writes to ``app.transaction_splits`` (allocations of one parent txn).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` (REC-PR3 repo-ification). PK ``split_id``.

Full-row audit (Req 4). ``clear`` emits one ``split.remove`` per deleted row
(DN3) rather than a single ``split.clear`` summary, so the undo consumer can
reinsert each split individually. ``category_id`` FK resolution stays in the
service (``resolve_category_id``); the repo receives the resolved value.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import TRANSACTION_SPLITS

_SPLITS_COLUMNS = (
    "split_id",
    "transaction_id",
    "amount",
    "category",
    "subcategory",
    "category_id",
    "note",
    "ord",
    "created_at",
    "created_by",
)


class TransactionSplitsRepo(BaseRepo):
    """Audited insert/delete/clear over ``app.transaction_splits``."""

    repository = "transaction_splits"

    _AUDIT_TARGET = (TRANSACTION_SPLITS.schema, TRANSACTION_SPLITS.name)

    def _fetch_row(self, split_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            TRANSACTION_SPLITS, _SPLITS_COLUMNS, "split_id", split_id
        )

    def insert(
        self,
        *,
        split_id: str,
        transaction_id: str,
        amount: Decimal,
        category: str | None,
        subcategory: str | None,
        category_id: str | None,
        note: str | None,
        ord: int,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert one split + ``split.add`` audit (before=None, after=full row).

        ``split_id``, ``ord`` and ``category_id`` are computed by the caller
        (the service generates the id, the next ord, and resolves the FK).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {TRANSACTION_SPLITS.full_name}
                    (split_id, transaction_id, amount, category, subcategory,
                     category_id, note, ord, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    split_id,
                    transaction_id,
                    amount,
                    category,
                    subcategory,
                    category_id,
                    note,
                    ord,
                    actor,
                ],
            )
            after = self._fetch_row(split_id)
            return self._emit_audit(
                action="split.add",
                target=(*self._AUDIT_TARGET, transaction_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def delete(
        self,
        *,
        split_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Delete one split + ``split.remove`` audit (full before row, after=None).

        Raises ``LookupError`` if ``split_id`` is unknown.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(split_id)
            if before is None:
                raise LookupError(f"split_id={split_id} not found")
            self._db.execute(
                f"DELETE FROM {TRANSACTION_SPLITS.full_name} WHERE split_id = ?",  # noqa: S608  # TableRef + parameterized value
                [split_id],
            )
            return self._emit_audit(
                action="split.remove",
                target=(*self._AUDIT_TARGET, before["transaction_id"]),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def clear(
        self,
        *,
        transaction_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> list[AuditEvent]:
        """Delete all of a transaction's splits, one ``split.remove`` per row.

        Returns the per-row audit events (empty when the parent had no splits).
        Per-row capture (DN3) keeps each split individually undoable.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            ids = [
                r[0]
                for r in self._db.execute(
                    f"SELECT split_id FROM {TRANSACTION_SPLITS.full_name} "  # noqa: S608  # TableRef + parameterized value
                    f"WHERE transaction_id = ? ORDER BY ord, split_id",
                    [transaction_id],
                ).fetchall()
            ]
            events: list[AuditEvent] = []
            for split_id in ids:
                events.append(
                    self.delete(
                        split_id=split_id,
                        actor=actor,
                        parent_audit_id=parent_audit_id,
                        in_outer_txn=True,
                    )
                )
            return events

"""Audited writes to ``app.transaction_splits`` (allocations of one parent txn).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` (REC-PR3 repo-ification). PK ``split_id``.

Full-row audit (Req 4). ``clear`` emits one ``split.remove`` per deleted row
(DN3) rather than a single ``split.clear`` summary, so the undo consumer can
reinsert each split individually. ``category_id`` FK resolution stays in the
service (``resolve_category_id``); the repo receives the resolved value.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import TRANSACTION_SPLITS

logger = logging.getLogger(__name__)

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

    table_ref = TRANSACTION_SPLITS
    pk_columns = ("split_id",)

    def _fetch_row(self, split_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            TRANSACTION_SPLITS, _SPLITS_COLUMNS, "split_id", split_id
        )

    def get(self, split_id: str) -> dict[str, Any] | None:
        """Return one split row by id, or None if not found."""
        return self._fetch_row(split_id)

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
                target=(*self._audit_target, split_id),
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
                target=(*self._audit_target, split_id),
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

    def delete_by_category(
        self,
        category_id: str,
        *,
        actor: str,
        in_outer_txn: bool = False,
    ) -> list[AuditEvent]:
        """Delete every split using one category, with per-row audit."""
        with self._transaction(in_outer_txn=in_outer_txn):
            split_ids = [
                str(row[0])
                for row in self._db.execute(
                    f"SELECT split_id FROM {TRANSACTION_SPLITS.full_name} "  # noqa: S608  # TableRef + parameterized value
                    "WHERE category_id = ? ORDER BY split_id",
                    [category_id],
                ).fetchall()
            ]
            return [
                self.delete(
                    split_id=split_id,
                    actor=actor,
                    in_outer_txn=True,
                )
                for split_id in split_ids
            ]

    def _has_splits(self, transaction_id: str) -> bool:
        """Whether any split currently allocates ``transaction_id``."""
        row = self._db.execute(
            f"SELECT COUNT(*) FROM {TRANSACTION_SPLITS.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE transaction_id = ?",
            [transaction_id],
        ).fetchone()
        return bool(row is not None and row[0])

    def repoint_transaction(
        self,
        *,
        old_transaction_id: str,
        new_transaction_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> tuple[AuditEvent, ...]:
        """Move every split off a superseded ``transaction_id`` onto its successor.

        Called when a dedup merge or a Plaid pending→posted transition re-keys the
        canonical transaction (ADR-015). ``split_id`` is the primary key, so no
        two split *rows* ever collide — but the allocations do.
        ``core.fct_transaction_lines`` drops the whole-transaction line as soon as
        a transaction has any split, so a survivor holding the union of two
        complete allocations publishes double the real amount to every
        spending-by-category report.

        **When the destination already has splits, nothing moves.** The
        superseded allocation is left exactly where it is and no event is
        emitted: merging the two double-counts money, and deleting either side
        destroys a curation the user entered. Those splits then sit on an id
        absent from ``core.fct_transactions``, where only the doctor's
        ``app_transaction_splits_fk`` invariant can see them.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            split_ids = [
                str(row[0])
                for row in self._db.execute(
                    f"SELECT split_id FROM {TRANSACTION_SPLITS.full_name} "  # noqa: S608  # TableRef + parameterized value
                    f"WHERE transaction_id = ? ORDER BY split_id",
                    [old_transaction_id],
                ).fetchall()
            ]
            if split_ids and self._has_splits(new_transaction_id):
                logger.debug(
                    f"Split repoint refused: {new_transaction_id} already carries "
                    f"splits, so the allocation on {old_transaction_id} stays put"
                )
                return ()
            events: list[AuditEvent] = []
            for split_id in split_ids:
                before = self._require(self._fetch_row(split_id), "split_id", split_id)
                self._db.execute(
                    f"UPDATE {TRANSACTION_SPLITS.full_name} "  # noqa: S608  # TableRef + parameterized values
                    f"SET transaction_id = ? WHERE split_id = ?",
                    [new_transaction_id, split_id],
                )
                events.append(
                    self._emit_audit(
                        action="split.repoint_transaction",
                        target=(*self._audit_target, split_id),
                        before=self._serialize_for_audit(before),
                        after=self._serialize_for_audit(self._fetch_row(split_id)),
                        actor=actor,
                        parent_audit_id=parent_audit_id,
                    )
                )
            return tuple(events)

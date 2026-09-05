"""Audited writes to ``app.transaction_tags`` (slug-flavored labels per transaction).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` (REC-PR3 repo-ification). Composite PK
``(transaction_id, tag)``.

These are **primitives**: ``add``/``remove`` mutate exactly one (transaction, tag)
row and audit it with the full row (Req 4). The service keeps the idempotency
orchestration (skip re-adding an existing tag, skip removing an absent one) — the
repo emits audit only for real mutations (DN2: no ``noop`` audit rows). ``remove``
on an absent tag raises ``LookupError``; the service guarantees existence first.
"""

from __future__ import annotations

from typing import Any

from moneybin.repositories.base import BaseRepo, quote_ident
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import TRANSACTION_TAGS

_TAGS_COLUMNS = (
    "transaction_id",
    "tag",
    "applied_at",
    "applied_by",
)


class TransactionTagsRepo(BaseRepo):
    """Audited single-tag add/remove/rename over ``app.transaction_tags``."""

    repository = "transaction_tags"

    table_ref = TRANSACTION_TAGS
    pk_columns = ("transaction_id", "tag")

    def _row_target_id(self, row: dict[str, Any]) -> str:
        """Composite ``target_id`` for a tag row: ``transaction_id:tag``.

        Mirrors the forward mutations' ``f"{transaction_id}:{tag}"`` so undo rows
        scope to the same row — critical for rename, where the undo lands on the
        old tag key.
        """
        return f"{row['transaction_id']}:{row['tag']}"

    def _fetch_tag(self, transaction_id: str, tag: str) -> dict[str, Any] | None:
        """Read one (transaction_id, tag) row as a full dict, or ``None``.

        The base ``_fetch_one`` keys on a single column; tags use a composite
        PK, so this reads on both. Columns are code constants, quoted defensively.
        """
        cols = ", ".join(quote_ident(c) for c in _TAGS_COLUMNS)
        row = self._db.execute(
            f"SELECT {cols} FROM {TRANSACTION_TAGS.full_name} "  # noqa: S608  # TableRef + quoted constant columns
            f"WHERE transaction_id = ? AND tag = ?",
            [transaction_id, tag],
        ).fetchone()
        if row is None:
            return None
        return dict(zip(_TAGS_COLUMNS, row, strict=True))

    def add(
        self,
        *,
        transaction_id: str,
        tag: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert one tag + ``tag.add`` audit (before=None, after=full row).

        The caller (service) guarantees the tag is not already present.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {TRANSACTION_TAGS.full_name}
                    (transaction_id, tag, applied_by)
                VALUES (?, ?, ?)
                """,  # noqa: S608  # TableRef + parameterized values
                [transaction_id, tag, actor],
            )
            after = self._fetch_tag(transaction_id, tag)
            return self._emit_audit(
                action="tag.add",
                target=(*self._audit_target, f"{transaction_id}:{tag}"),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def remove(
        self,
        *,
        transaction_id: str,
        tag: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Delete one tag + ``tag.remove`` audit (full before row, after=None).

        Raises ``LookupError`` if the (transaction_id, tag) row is absent.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_tag(transaction_id, tag)
            if before is None:
                raise LookupError(
                    f"tag={tag!r} not found on transaction_id={transaction_id}"
                )
            self._db.execute(
                f"DELETE FROM {TRANSACTION_TAGS.full_name} "  # noqa: S608  # TableRef + parameterized values
                f"WHERE transaction_id = ? AND tag = ?",
                [transaction_id, tag],
            )
            return self._emit_audit(
                action="tag.remove",
                target=(*self._audit_target, f"{transaction_id}:{tag}"),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def rename_row(
        self,
        *,
        transaction_id: str,
        old_tag: str,
        new_tag: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Rename one transaction's tag + ``tag.rename_row`` audit (full rows).

        Used per-row by the service's global ``rename_tag``; ``parent_audit_id``
        chains each row event back to the parent ``tag.rename``. Raises
        ``LookupError`` if the (transaction_id, old_tag) row is absent.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_tag(transaction_id, old_tag)
            if before is None:
                raise LookupError(
                    f"tag={old_tag!r} not found on transaction_id={transaction_id}"
                )
            self._db.execute(
                f"UPDATE {TRANSACTION_TAGS.full_name} SET tag = ? "  # noqa: S608  # TableRef + parameterized values
                f"WHERE transaction_id = ? AND tag = ?",
                [new_tag, transaction_id, old_tag],
            )
            after = self._fetch_tag(transaction_id, new_tag)
            return self._emit_audit(
                action="tag.rename_row",
                # Row identity is the (current) renamed tag — the after-image PK
                # the undo locates by, so cascade scoping matches the live row.
                target=(*self._audit_target, f"{transaction_id}:{new_tag}"),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def repoint_transaction(
        self,
        *,
        old_transaction_id: str,
        new_transaction_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> tuple[AuditEvent, ...]:
        """Move every tag off a superseded ``transaction_id`` onto its successor.

        Called when a dedup merge or a Plaid pending→posted transition re-keys the
        canonical transaction (ADR-015). ``transaction_id`` is half the composite
        PK, so a tag the destination already carries would collide: that row is
        deleted rather than moved. Nothing is lost — the surviving row is the same
        (transaction, tag) pair.

        Either way the *old* key is audited: the collision as a delete, the move
        as a delete on the old key paired with an insert on the new one. Both
        keys naming the change is what keeps the move visible to
        ``UndoService._cascade_blockers``, which joins on exact ``target_id``.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            tags = [
                str(row[0])
                for row in self._db.execute(
                    f"SELECT tag FROM {TRANSACTION_TAGS.full_name} "  # noqa: S608  # TableRef + parameterized value
                    f"WHERE transaction_id = ? ORDER BY tag",
                    [old_transaction_id],
                ).fetchall()
            ]
            events: list[AuditEvent] = []
            for tag in tags:
                before = self._fetch_tag(old_transaction_id, tag)
                if before is None:  # pragma: no cover — read inside the same txn
                    continue
                if self._fetch_tag(new_transaction_id, tag) is None:
                    self._db.execute(
                        f"UPDATE {TRANSACTION_TAGS.full_name} SET transaction_id = ? "  # noqa: S608  # TableRef + parameterized values
                        f"WHERE transaction_id = ? AND tag = ?",
                        [new_transaction_id, old_transaction_id, tag],
                    )
                    # Two row-grain events, not one: ``transaction_id`` is half
                    # the primary key, so the move vacates one row identity and
                    # creates another. An event naming only the new key hides
                    # the move from the old key's side, and undo replays an
                    # operation in reverse write order — so the arrival is
                    # reversed before the departure is restored.
                    changes = [
                        (f"{old_transaction_id}:{tag}", before, None),
                        (
                            f"{new_transaction_id}:{tag}",
                            None,
                            self._fetch_tag(new_transaction_id, tag),
                        ),
                    ]
                else:
                    self._db.execute(
                        f"DELETE FROM {TRANSACTION_TAGS.full_name} "  # noqa: S608  # TableRef + parameterized values
                        f"WHERE transaction_id = ? AND tag = ?",
                        [old_transaction_id, tag],
                    )
                    changes = [(f"{old_transaction_id}:{tag}", before, None)]
                events.extend(
                    self._emit_audit(
                        action="tag.repoint_transaction",
                        target=(*self._audit_target, target_id),
                        before=self._serialize_for_audit(row_before),
                        after=self._serialize_for_audit(row_after),
                        actor=actor,
                        parent_audit_id=parent_audit_id,
                    )
                    for target_id, row_before, row_after in changes
                )
            return tuple(events)

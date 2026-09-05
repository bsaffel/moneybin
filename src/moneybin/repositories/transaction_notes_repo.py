"""Audited writes to ``app.transaction_notes`` (multi-note threads per transaction).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``TransactionService``
composes this instead of issuing raw notes SQL (REC-PR3 repo-ification).

Audit rows capture the **full** note row in ``before``/``after`` (Req 4), not the
partial dicts the service emitted previously — so the undo consumer can
reconstruct a note from its audit row alone. Missing-note ``edit``/``delete``
raise ``LookupError`` (not the base ``_require`` ``ValueError``) to preserve the
service's pre-existing not-found contract.
"""

from __future__ import annotations

from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import TRANSACTION_NOTES

_NOTES_COLUMNS = (
    "note_id",
    "transaction_id",
    "text",
    "author",
    "created_at",
)


class TransactionNotesRepo(BaseRepo):
    """Audited add/edit/delete over ``app.transaction_notes`` (keyed by ``note_id``)."""

    repository = "transaction_notes"

    table_ref = TRANSACTION_NOTES
    pk_columns = ("note_id",)

    def _fetch_row(self, note_id: str) -> dict[str, Any] | None:
        return self._fetch_one(TRANSACTION_NOTES, _NOTES_COLUMNS, "note_id", note_id)

    def add(
        self,
        *,
        transaction_id: str,
        note_id: str,
        text: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert one note + ``note.add`` audit (before=None, after=full row).

        ``note_id`` is supplied by the caller (the service generates the
        truncated UUID4) so the write is deterministic and testable.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            self._db.execute(
                f"""
                INSERT INTO {TRANSACTION_NOTES.full_name}
                    (note_id, transaction_id, text, author)
                VALUES (?, ?, ?, ?)
                """,  # noqa: S608  # TableRef + parameterized values
                [note_id, transaction_id, text, actor],
            )
            after = self._fetch_row(note_id)
            return self._emit_audit(
                action="note.add",
                target=(*self._audit_target, note_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def edit(
        self,
        *,
        note_id: str,
        text: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Update note text + ``note.edit`` audit (full prior/new rows).

        Raises ``LookupError`` if ``note_id`` is unknown.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(note_id)
            if before is None:
                raise LookupError(f"note_id={note_id} not found")
            self._db.execute(
                f"UPDATE {TRANSACTION_NOTES.full_name} SET text = ? WHERE note_id = ?",  # noqa: S608  # TableRef + parameterized values
                [text, note_id],
            )
            after = self._fetch_row(note_id)
            return self._emit_audit(
                action="note.edit",
                target=(*self._audit_target, note_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def delete(
        self,
        *,
        note_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Delete one note + ``note.delete`` audit (full before row, after=None).

        Raises ``LookupError`` if ``note_id`` is unknown.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(note_id)
            if before is None:
                raise LookupError(f"note_id={note_id} not found")
            self._db.execute(
                f"DELETE FROM {TRANSACTION_NOTES.full_name} WHERE note_id = ?",  # noqa: S608  # TableRef + parameterized value
                [note_id],
            )
            return self._emit_audit(
                action="note.delete",
                target=(*self._audit_target, note_id),
                before=self._serialize_for_audit(before),
                after=None,
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
        """Move every note off a superseded ``transaction_id`` onto its successor.

        Called when a dedup merge or a Plaid pending→posted transition re-keys the
        canonical transaction (ADR-015); without it the notes reference an id that
        no longer exists in ``core.fct_transactions``. ``note_id`` is the primary
        key, so a note never collides with one already on the destination.

        One audit per note, like every other method here — the undo engine replays
        rows individually, and a single event covering N updates could not restore
        them one at a time.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            note_ids = [
                str(row[0])
                for row in self._db.execute(
                    f"SELECT note_id FROM {TRANSACTION_NOTES.full_name} "  # noqa: S608  # TableRef + parameterized value
                    f"WHERE transaction_id = ? ORDER BY note_id",
                    [old_transaction_id],
                ).fetchall()
            ]
            events: list[AuditEvent] = []
            for note_id in note_ids:
                before = self._require(self._fetch_row(note_id), "note_id", note_id)
                self._db.execute(
                    f"UPDATE {TRANSACTION_NOTES.full_name} "  # noqa: S608  # TableRef + parameterized values
                    f"SET transaction_id = ? WHERE note_id = ?",
                    [new_transaction_id, note_id],
                )
                events.append(
                    self._emit_audit(
                        action="note.repoint_transaction",
                        target=(*self._audit_target, note_id),
                        before=self._serialize_for_audit(before),
                        after=self._serialize_for_audit(self._fetch_row(note_id)),
                        actor=actor,
                        parent_audit_id=parent_audit_id,
                    )
                )
            return tuple(events)

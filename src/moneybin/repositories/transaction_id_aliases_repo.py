"""Audited writes to ``app.transaction_id_aliases`` (M1S old_id -> new_id map).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), the mutation flows
through this repo, which pairs the write with an ``app.audit_log`` row inside the
same DuckDB transaction. The map is **append-only**: each ``old_transaction_id``
forwards to exactly one ``new_transaction_id`` (enforced by the PK and the guard
below). The transform-layer re-key (M1S.3) and future id-changing merges seed it.

NOTE (M1S.3): the backfill writes one alias per re-keyed transaction. If that
audit volume proves heavy, M1S.3 may switch the backfill to a bulk path; the repo
is internal, so that is a two-way door.
"""

from __future__ import annotations

from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import TRANSACTION_ID_ALIASES

_TRANSACTION_ID_ALIASES_COLUMNS = (
    "old_transaction_id",
    "new_transaction_id",
    "created_at",
)


class TransactionIdAliasesRepo(BaseRepo):
    """Audited, append-only writes to ``app.transaction_id_aliases``."""

    repository = "transaction_id_aliases"

    table_ref = TRANSACTION_ID_ALIASES
    pk_columns = ("old_transaction_id",)

    def undo_event(
        self,
        event: AuditEvent,
        *,
        actor: str,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Refuse to undo — the alias map is append-only.

        ``BaseRepo.undo_event`` reverses an INSERT with a DELETE; for this table
        that removes a forward pointer and orphans every reference that resolves
        ``old_transaction_id`` through it (the "never an orphan" contract). Aliases
        are derived from merges, so the merge — not the alias row — is the undoable
        unit. Raising here keeps ``undo_dispatch`` from silently orphaning a ref.
        """
        raise ValueError(
            "transaction_id_aliases is append-only; alias rows are not "
            "individually undoable (deleting one would orphan references that "
            "forward through it)"
        )

    def _fetch_row(self, old_transaction_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            TRANSACTION_ID_ALIASES,
            _TRANSACTION_ID_ALIASES_COLUMNS,
            "old_transaction_id",
            old_transaction_id,
        )

    def insert(
        self,
        *,
        old_transaction_id: str,
        new_transaction_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Append one old -> new forwarding alias + paired audit.

        Append-only: raises ``ValueError`` if ``old_transaction_id`` already
        forwards (a clean error ahead of the raw PK-constraint violation), so a
        conflicting re-alias can't silently overwrite an existing pointer.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            if self._fetch_row(old_transaction_id) is not None:
                raise ValueError(
                    f"transaction_id_aliases: {old_transaction_id} is already "
                    "aliased (append-only)"
                )
            self._db.execute(
                f"""
                INSERT INTO {TRANSACTION_ID_ALIASES.full_name} (
                    old_transaction_id, new_transaction_id, created_at
                ) VALUES (?, ?, CURRENT_TIMESTAMP)
                """,  # noqa: S608  # TableRef + parameterized values
                [old_transaction_id, new_transaction_id],
            )
            after = self._fetch_row(old_transaction_id)
            return self._emit_audit(
                action="transaction_id_alias.insert",
                target=(*self._audit_target, old_transaction_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

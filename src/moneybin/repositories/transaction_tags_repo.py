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

    _AUDIT_TARGET = (TRANSACTION_TAGS.schema, TRANSACTION_TAGS.name)

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
                target=(*self._AUDIT_TARGET, transaction_id),
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
                target=(*self._AUDIT_TARGET, transaction_id),
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
                target=(*self._AUDIT_TARGET, transaction_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

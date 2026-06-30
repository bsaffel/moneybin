"""MerchantLinksRepo — provider-id -> canonical merchant binding writes (M1T).

Mirrors AccountLinksRepo: every mutation pairs an app.audit_log row (Invariant 10).
Provider-neutral strong-ref uniqueness: (source_type, ref_kind, ref_value) is unique
among accepted rows. N:1 — one merchant_id may hold many provider ids.
"""

from __future__ import annotations

import uuid
from typing import Any, ClassVar

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import MERCHANT_LINKS, TableRef

_MERCHANT_LINKS_COLUMNS = (
    "link_id",
    "merchant_id",
    "ref_kind",
    "ref_value",
    "source_type",
    "status",
    "decided_by",
    "decided_at",
    "reversed_at",
    "reversed_by",
)


class MerchantLinksRepo(BaseRepo):
    """Audited CRUD over ``app.merchant_links``."""

    repository: ClassVar[str] = "merchant_links"
    table_ref: ClassVar[TableRef] = MERCHANT_LINKS
    pk_columns: ClassVar[tuple[str, ...]] = ("link_id",)

    def _fetch_row(self, link_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            MERCHANT_LINKS, _MERCHANT_LINKS_COLUMNS, "link_id", link_id
        )

    def _guard_uniqueness(
        self, *, ref_kind: str, ref_value: str, source_type: str
    ) -> None:
        existing = self._db.execute(
            f"SELECT 1 FROM {MERCHANT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE status = 'accepted' AND source_type = ? AND ref_kind = ? "
            "AND ref_value = ? LIMIT 1",
            [source_type, ref_kind, ref_value],
        ).fetchone()
        if existing is not None:
            raise ValueError(
                "merchant_links: an accepted binding already exists for this "
                f"(source_type, ref_kind, ref_value); source_type={source_type!r}, "
                f"ref_kind={ref_kind!r}"
            )

    def _insert_row(self, row: dict[str, Any]) -> None:
        """Re-validate uniqueness before an undo re-inserts an accepted row.

        BaseRepo.undo_event re-inserts a captured row through this hook when it
        undoes a DELETE (i.e. the undo-the-undo of an insert). The app-layer
        _guard_uniqueness only runs on the insert path, so without this the
        undo could restore a second accepted mapping for a provider id already
        held by another row.
        """
        if row.get("status") == "accepted":
            self._guard_uniqueness(
                ref_kind=row["ref_kind"],
                ref_value=row["ref_value"],
                source_type=row["source_type"],
            )
        super()._insert_row(row)

    def insert(
        self,
        *,
        link_id: str,
        merchant_id: str,
        ref_kind: str,
        ref_value: str,
        source_type: str,
        decided_by: str,
        actor: str,
        status: str = "accepted",
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a new merchant-link binding + paired audit. ``target_id`` is ``link_id``."""
        with self._transaction(in_outer_txn=in_outer_txn):
            if status == "accepted":
                self._guard_uniqueness(
                    ref_kind=ref_kind, ref_value=ref_value, source_type=source_type
                )
            self._db.execute(
                f"""
                INSERT INTO {MERCHANT_LINKS.full_name} (
                    link_id, merchant_id, ref_kind, ref_value, source_type,
                    status, decided_by, decided_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    link_id,
                    merchant_id,
                    ref_kind,
                    ref_value,
                    source_type,
                    status,
                    decided_by,
                ],
            )
            after = self._fetch_row(link_id)
            return self._emit_audit(
                action="merchant_link.insert",
                target=(*self._audit_target, link_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def repoint(
        self,
        *,
        link_id: str,
        new_merchant_id: str,
        decided_by: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Re-point an accepted link onto a different canonical merchant_id (merge primitive).

        Reverses the existing row, inserts a new accepted row for ``new_merchant_id``,
        and emits a paired audit for the old row's reversal. Raises ``ValueError`` when
        ``link_id`` is not found, already points to ``new_merchant_id``, or is not accepted.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self._fetch_row(link_id), "link_id", link_id)
            if before["merchant_id"] == new_merchant_id:
                raise ValueError(
                    f"merchant_links repoint: link {link_id!r} already points to {new_merchant_id!r}"
                )
            if before["status"] != "accepted":
                raise ValueError(
                    f"merchant_links repoint: link {link_id!r} status={before['status']!r}; need accepted"
                )
            self._db.execute(
                f"UPDATE {MERCHANT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
                "SET reversed_at = CURRENT_TIMESTAMP, reversed_by = ?, status = 'reversed' WHERE link_id = ?",
                [decided_by, link_id],
            )
            after_reversal = self._fetch_row(link_id)
            self.insert(
                link_id=uuid.uuid4().hex[:12],
                merchant_id=new_merchant_id,
                ref_kind=before["ref_kind"],
                ref_value=before["ref_value"],
                source_type=before["source_type"],
                decided_by=decided_by,
                actor=actor,
                status="accepted",
                parent_audit_id=parent_audit_id,
                in_outer_txn=True,
            )
            return self._emit_audit(
                action="merchant_link.repoint",
                target=(*self._audit_target, link_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after_reversal),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def lookup(
        self, source_type: str, ref_value: str, *, ref_kind: str = "merchant_entity_id"
    ) -> str | None:
        """Return the accepted ``merchant_id`` for a provider ref, or ``None``."""
        row = self._db.execute(
            f"SELECT merchant_id FROM {MERCHANT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE status = 'accepted' AND source_type = ? AND ref_kind = ? AND ref_value = ? LIMIT 1",
            [source_type, ref_kind, ref_value],
        ).fetchone()
        return str(row[0]) if row else None

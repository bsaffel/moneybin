"""SecurityLinksRepo — provider-id -> canonical security binding writes (M1G.4).

Mirrors MerchantLinksRepo: every mutation pairs an app.audit_log row (Invariant
10). Provider-neutral strong-ref uniqueness: (source_type, ref_kind, ref_value)
is unique among accepted rows. N:1 — one security_id may hold many provider
refs (Plaid security_id churn on corporate actions re-binds to the same
canonical security).
"""

from __future__ import annotations

import uuid
from typing import Any, ClassVar

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import SECURITY_LINKS, TableRef

_SECURITY_LINKS_COLUMNS = (
    "link_id",
    "security_id",
    "ref_kind",
    "ref_value",
    "source_type",
    "status",
    "decided_by",
    "decided_at",
    "reversed_at",
    "reversed_by",
)


class SecurityLinksRepo(BaseRepo):
    """Audited CRUD over ``app.security_links``."""

    repository: ClassVar[str] = "security_links"
    table_ref: ClassVar[TableRef] = SECURITY_LINKS
    pk_columns: ClassVar[tuple[str, ...]] = ("link_id",)

    def _fetch_row(self, link_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            SECURITY_LINKS, _SECURITY_LINKS_COLUMNS, "link_id", link_id
        )

    def _guard_uniqueness(
        self, *, ref_kind: str, ref_value: str, source_type: str
    ) -> None:
        existing = self._db.execute(
            f"SELECT 1 FROM {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE status = 'accepted' AND source_type = ? AND ref_kind = ? "
            "AND ref_value = ? LIMIT 1",
            [source_type, ref_kind, ref_value],
        ).fetchone()
        if existing is not None:
            raise ValueError(
                "security_links: an accepted binding already exists for this "
                f"(source_type, ref_kind, ref_value); source_type={source_type!r}, "
                f"ref_kind={ref_kind!r}"
            )

    def _insert_row(self, row: dict[str, Any]) -> None:
        """Re-validate uniqueness before an undo re-inserts an accepted row.

        BaseRepo.undo_event re-inserts a captured row through this hook when it
        undoes a DELETE (i.e. the undo-the-undo of an insert). The app-layer
        _guard_uniqueness only runs on the insert path, so without this the
        undo could restore a second accepted mapping for a provider ref already
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
        security_id: str,
        ref_kind: str,
        ref_value: str,
        source_type: str,
        decided_by: str,
        actor: str,
        link_id: str | None = None,
        status: str = "accepted",
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a new security-link binding + paired audit. ``target_id`` is ``link_id``.

        Mints a 12-hex ``link_id`` (Strategy 3, ``identifiers.md``) when
        ``link_id`` is ``None``.
        """
        resolved_id = link_id if link_id is not None else uuid.uuid4().hex[:12]
        with self._transaction(in_outer_txn=in_outer_txn):
            if status == "accepted":
                self._guard_uniqueness(
                    ref_kind=ref_kind, ref_value=ref_value, source_type=source_type
                )
            self._db.execute(
                f"""
                INSERT INTO {SECURITY_LINKS.full_name} (
                    link_id, security_id, ref_kind, ref_value, source_type,
                    status, decided_by, decided_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    resolved_id,
                    security_id,
                    ref_kind,
                    ref_value,
                    source_type,
                    status,
                    decided_by,
                ],
            )
            after = self._fetch_row(resolved_id)
            return self._emit_audit(
                action="security_link.insert",
                target=(*self._audit_target, resolved_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def repoint(
        self,
        *,
        link_id: str,
        new_security_id: str,
        decided_by: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Re-point an accepted link onto a different canonical security_id (merge primitive).

        Reverses the existing row, inserts a new accepted row for ``new_security_id``,
        and emits a paired audit for the old row's reversal. Raises ``ValueError`` when
        ``link_id`` is not found, already points to ``new_security_id``, or is not accepted.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self._fetch_row(link_id), "link_id", link_id)
            if before["security_id"] == new_security_id:
                raise ValueError(
                    f"security_links repoint: link {link_id!r} already points to {new_security_id!r}"
                )
            if before["status"] != "accepted":
                raise ValueError(
                    f"security_links repoint: link {link_id!r} status={before['status']!r}; need accepted"
                )
            self._db.execute(
                f"UPDATE {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
                "SET reversed_at = CURRENT_TIMESTAMP, reversed_by = ?, status = 'reversed' WHERE link_id = ?",
                [decided_by, link_id],
            )
            after_reversal = self._fetch_row(link_id)
            self.insert(
                link_id=uuid.uuid4().hex[:12],
                security_id=new_security_id,
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
                action="security_link.repoint",
                target=(*self._audit_target, link_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after_reversal),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def reverse(
        self,
        *,
        link_id: str,
        reversed_by: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Reverse an accepted binding, freeing its ref for a new binding.

        Raises ``ValueError`` when no link with this id exists, or when it is
        already reversed — re-reversing would overwrite the original
        reversal's audit trail (``reversed_at``/``reversed_by``).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self._fetch_row(link_id), "link_id", link_id)
            if before["status"] == "reversed":
                raise ValueError(f"security_links: link {link_id} already reversed")
            self._db.execute(
                f"""
                UPDATE {SECURITY_LINKS.full_name}
                SET reversed_at = CURRENT_TIMESTAMP, reversed_by = ?,
                    status = 'reversed'
                WHERE link_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [reversed_by, link_id],
            )
            after = self._fetch_row(link_id)
            return self._emit_audit(
                action="security_link.reverse",
                target=(*self._audit_target, link_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

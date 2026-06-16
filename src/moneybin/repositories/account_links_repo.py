"""Audited writes to ``app.account_links`` (M1S native-ref -> canonical mapping).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through this repo, which pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. The ``AccountResolver``
(M1S.2) composes this instead of issuing raw mutation SQL — account links are
user-affecting state (``accounts links set``/``undo`` exist), so they are routed
under Invariant 10.

``decided_by``/``reversed_by`` are *domain* columns (``auto``/``user``/``system``)
distinct from the audit ``actor`` (the surface: ``cli``/``mcp``/``system``); the
caller supplies both.
"""

from __future__ import annotations

import uuid
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import ACCOUNT_LINKS

_ACCOUNT_LINKS_COLUMNS = (
    "link_id",
    "account_id",
    "ref_kind",
    "ref_value",
    "source_type",
    "source_origin",
    "status",
    "decided_by",
    "decided_at",
    "reversed_at",
    "reversed_by",
)


class AccountLinksRepo(BaseRepo):
    """Audited CRUD over ``app.account_links``."""

    repository = "account_links"

    table_ref = ACCOUNT_LINKS
    pk_columns = ("link_id",)

    def _fetch_row(self, link_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            ACCOUNT_LINKS, _ACCOUNT_LINKS_COLUMNS, "link_id", link_id
        )

    def _guard_uniqueness(
        self,
        *,
        ref_kind: str,
        ref_value: str,
        source_type: str,
        source_origin: str,
    ) -> None:
        """Enforce the finding-#3 uniqueness invariants among ``accepted`` rows.

        DuckDB has no partial/filtered unique index, so these are application-layer
        guards (consistent with the repo-enforced-invariant pattern). Error
        messages deliberately omit ``ref_value`` — it can be a full account number
        (``security.md``: no PII in errors/logs).
        """
        if ref_kind == "source_native":
            existing = self._db.execute(
                f"SELECT 1 FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
                "WHERE status = 'accepted' AND ref_kind = 'source_native' "
                "AND source_type = ? AND source_origin = ? AND ref_value = ? LIMIT 1",
                [source_type, source_origin, ref_value],
            ).fetchone()
            if existing is not None:
                raise ValueError(
                    "account_links: an accepted source_native mapping already "
                    "exists for this (source_type, source_origin, ref_value); "
                    f"source_type={source_type!r}, source_origin={source_origin!r}"
                )
        elif ref_kind in ("full_number", "persistent_token"):
            existing = self._db.execute(
                f"SELECT 1 FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized values
                "WHERE status = 'accepted' AND ref_kind = ? AND ref_value = ? LIMIT 1",
                [ref_kind, ref_value],
            ).fetchone()
            if existing is not None:
                raise ValueError(
                    f"account_links: an accepted {ref_kind} strong-ref already "
                    "exists for this ref_value"
                )

    def _insert_row(self, row: dict[str, Any]) -> None:
        """Re-validate the uniqueness guard before an undo re-inserts an accepted row.

        ``BaseRepo.undo_event`` re-inserts a captured row through this hook when it
        undoes a DELETE (i.e. the undo-the-undo of an insert). The app-layer
        ``_guard_uniqueness`` only runs on the ``insert`` path, so without this the
        undo could restore a second accepted mapping for a native ref already held
        by another row — making the staging translation JOIN non-1:1.
        """
        if row.get("status") == "accepted":
            self._guard_uniqueness(
                ref_kind=row["ref_kind"],
                ref_value=row["ref_value"],
                source_type=row["source_type"],
                source_origin=row["source_origin"],
            )
        super()._insert_row(row)

    def insert(
        self,
        *,
        link_id: str,
        account_id: str,
        ref_kind: str,
        ref_value: str,
        source_type: str,
        source_origin: str,
        decided_by: str,
        actor: str,
        status: str = "accepted",
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert a new account-link mapping + paired audit. ``target_id`` is ``link_id``.

        ``decided_at`` is stamped ``CURRENT_TIMESTAMP``. The caller supplies
        ``link_id`` (a fresh truncated UUID).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            if status == "accepted":
                self._guard_uniqueness(
                    ref_kind=ref_kind,
                    ref_value=ref_value,
                    source_type=source_type,
                    source_origin=source_origin,
                )
            self._db.execute(
                f"""
                INSERT INTO {ACCOUNT_LINKS.full_name} (
                    link_id, account_id, ref_kind, ref_value, source_type,
                    source_origin, status, decided_by, decided_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    link_id,
                    account_id,
                    ref_kind,
                    ref_value,
                    source_type,
                    source_origin,
                    status,
                    decided_by,
                ],
            )
            after = self._fetch_row(link_id)
            return self._emit_audit(
                action="account_link.insert",
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
        new_account_id: str,
        decided_by: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Re-point an accepted link onto a different canonical account_id (merge primitive).

        Within one transaction:
        1. Reverses the existing row (status='reversed') so ``_guard_uniqueness``
           no longer sees it as accepted.
        2. Inserts a new accepted row for ``new_account_id`` with the same ref
           coordinates — the guard now passes because the old row is reversed.
        3. Emits a paired audit for the OLD row's reversal (action
           ``"account_link.repoint"``). The nested ``insert`` emits its own audit
           for the new row (Invariant 10 — each mutation audited independently).

        Raises ``ValueError`` when ``link_id`` is not found, already points to
        ``new_account_id`` (caller bug), or is not in ``accepted`` status.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(self._fetch_row(link_id), "link_id", link_id)
            if before["account_id"] == new_account_id:
                raise ValueError(
                    f"account_links repoint: link {link_id!r} already points to "
                    f"account_id={new_account_id!r}"
                )
            if before["status"] != "accepted":
                raise ValueError(
                    f"account_links repoint: link {link_id!r} has status="
                    f"{before['status']!r}; can only re-point an accepted link"
                )
            # Reverse the old row first so _guard_uniqueness won't block the insert.
            self._db.execute(
                f"""
                UPDATE {ACCOUNT_LINKS.full_name}
                SET reversed_at = CURRENT_TIMESTAMP, reversed_by = ?,
                    status = 'reversed'
                WHERE link_id = ?
                """,  # noqa: S608  # TableRef + parameterized values
                [decided_by, link_id],
            )
            after_reversal = self._fetch_row(link_id)
            # Insert a new accepted row for new_account_id (same ref coordinates).
            # Uses in_outer_txn=True to join the enclosing transaction; the insert
            # emits its own audit row (Invariant 10).
            new_link_id = uuid.uuid4().hex[:12]
            self.insert(
                link_id=new_link_id,
                account_id=new_account_id,
                ref_kind=before["ref_kind"],
                ref_value=before["ref_value"],
                source_type=before["source_type"],
                source_origin=before["source_origin"],
                decided_by=decided_by,
                actor=actor,
                status="accepted",
                parent_audit_id=parent_audit_id,
                in_outer_txn=True,
            )
            # Audit the OLD row's reversal as the repoint action.
            return self._emit_audit(
                action="account_link.repoint",
                target=(*self._audit_target, link_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after_reversal),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

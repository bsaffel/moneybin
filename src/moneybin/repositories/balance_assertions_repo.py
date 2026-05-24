"""Audited writes to ``app.balance_assertions`` (user-entered balance anchors).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``BalanceService``
composes this instead of raw SQL; reads stay in the service.

The table has a **composite** primary key ``(account_id, assertion_date)``, but
``app.audit_log.target_id`` is a single string. The repo maps the pair to a
composite ``target_id`` of ``"{account_id}|{assertion_date ISO}"``; the doctor's
audit-coverage check projects the matching expression via its ``pk_expr`` hook.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from moneybin.repositories.base import BaseRepo, quote_ident
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import BALANCE_ASSERTIONS

_BALANCE_ASSERTIONS_COLUMNS = (
    "account_id",
    "assertion_date",
    "balance",
    "notes",
    "created_at",
    "updated_at",
)


def _target_id(account_id: str, assertion_date: date) -> str:
    """Composite ``target_id`` for the ``(account_id, assertion_date)`` PK.

    Must match the doctor coverage check's ``pk_expr``
    (``account_id || '|' || CAST(assertion_date AS VARCHAR)``); DuckDB casts a
    ``DATE`` to the same ``YYYY-MM-DD`` string ``date.isoformat()`` produces.
    """
    return f"{account_id}|{assertion_date.isoformat()}"


class BalanceAssertionsRepo(BaseRepo):
    """Audited CRUD over ``app.balance_assertions`` (one row per account+date)."""

    repository = "balance_assertions"

    _AUDIT_TARGET = (BALANCE_ASSERTIONS.schema, BALANCE_ASSERTIONS.name)

    def _fetch_row(
        self, account_id: str, assertion_date: date
    ) -> dict[str, Any] | None:
        """Read one row by the composite PK as a ``column → value`` dict, or ``None``.

        ``BaseRepo._fetch_one`` keys on a single column; this table's PK is
        composite, so the read is spelled out here (identifiers quoted per
        ``.claude/rules/security.md``).
        """
        cols = ", ".join(quote_ident(c) for c in _BALANCE_ASSERTIONS_COLUMNS)
        row = self._db.execute(
            f"SELECT {cols} FROM {BALANCE_ASSERTIONS.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted identifiers
            f"WHERE account_id = ? AND assertion_date = ?",
            [account_id, assertion_date],
        ).fetchone()
        if row is None:
            return None
        return dict(zip(_BALANCE_ASSERTIONS_COLUMNS, row, strict=True))

    def set(
        self,
        account_id: str,
        assertion_date: date,
        *,
        balance: Decimal,
        notes: str | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert-or-update one balance assertion + audit (``balance_assertion.set``).

        Captures the full prior row (or ``None`` on insert) as ``before`` and the
        full resulting row as ``after``. ``created_at`` is preserved on update;
        ``updated_at`` refreshes via ``NOW()`` (DuckDB parses ``CURRENT_TIMESTAMP``
        as an identifier inside ``DO UPDATE``).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(account_id, assertion_date)
            self._db.execute(
                f"""
                INSERT INTO {BALANCE_ASSERTIONS.full_name}
                    (account_id, assertion_date, balance, notes)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (account_id, assertion_date) DO UPDATE SET
                    balance = excluded.balance,
                    notes = excluded.notes,
                    updated_at = NOW()
                """,  # noqa: S608  # TableRef + parameterized values
                [account_id, assertion_date, balance, notes],
            )
            after = self._fetch_row(account_id, assertion_date)
            return self._emit_audit(
                action="balance_assertion.set",
                target=(*self._AUDIT_TARGET, _target_id(account_id, assertion_date)),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def delete(
        self,
        account_id: str,
        assertion_date: date,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Delete one assertion; ``None`` when there's nothing to delete (silent no-op)."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(account_id, assertion_date)
            if before is None:
                return None
            self._db.execute(
                f"DELETE FROM {BALANCE_ASSERTIONS.full_name} "  # noqa: S608  # TableRef + parameterized values
                f"WHERE account_id = ? AND assertion_date = ?",
                [account_id, assertion_date],
            )
            return self._emit_audit(
                action="balance_assertion.delete",
                target=(*self._AUDIT_TARGET, _target_id(account_id, assertion_date)),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

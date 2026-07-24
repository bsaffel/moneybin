"""Audited writes to ``app.security_price_overrides`` (user price marks).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``PriceService``
composes this instead of raw SQL; reads stay in the service.

The table has a **composite** primary key ``(security_id, price_date,
quote_currency)``, but ``app.audit_log.target_id`` is a single string. The repo
maps the triple to a composite ``target_id`` of
``"{security_id}|{price_date ISO}|{quote_currency}"``; the doctor's
audit-coverage check projects the matching expression via its ``pk_expr`` hook.

``delete`` is not CRUD symmetry. Source precedence ranks an override above every
provider row for its own date, and ``set`` can only replace the value while
keeping ``source = 'override'`` provenance — so without ``delete`` a mark is
unreachable once written and its date can never return to provider-derived
valuation. ``surface-design.md`` requires the paired ``_delete`` for this
mutation shape for the same reason.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from moneybin.repositories.base import BaseRepo, quote_ident
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import SECURITY_PRICE_OVERRIDES

_OVERRIDE_COLUMNS = (
    "security_id",
    "price_date",
    "quote_currency",
    "close",
    "note",
    "created_at",
    "updated_at",
)


def _target_id(security_id: str, price_date: date, quote_currency: str) -> str:
    """Composite ``target_id`` for the three-column PK.

    Must match the doctor coverage check's ``pk_expr``
    (``security_id || '|' || CAST(price_date AS VARCHAR) || '|' ||
    quote_currency``); DuckDB casts a ``DATE`` to the same ``YYYY-MM-DD`` string
    ``date.isoformat()`` produces.
    """
    return f"{security_id}|{price_date.isoformat()}|{quote_currency}"


class SecurityPriceRepo(BaseRepo):
    """Audited CRUD over ``app.security_price_overrides`` (one mark per security+date+currency)."""

    repository = "security_price_overrides"

    table_ref = SECURITY_PRICE_OVERRIDES
    pk_columns = ("security_id", "price_date", "quote_currency")

    def _fetch_row(
        self, security_id: str, price_date: date, quote_currency: str
    ) -> dict[str, Any] | None:
        """Read one mark by the composite PK as a ``column → value`` dict, or ``None``.

        ``BaseRepo._fetch_one`` keys on a single column; this table's PK is
        composite, so the read is spelled out here (identifiers quoted per
        ``.claude/rules/security.md``).
        """
        cols = ", ".join(quote_ident(c) for c in _OVERRIDE_COLUMNS)
        row = self._db.execute(
            f"SELECT {cols} FROM {SECURITY_PRICE_OVERRIDES.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted identifiers
            f"WHERE security_id = ? AND price_date = ? AND quote_currency = ?",
            [security_id, price_date, quote_currency],
        ).fetchone()
        if row is None:
            return None
        return dict(zip(_OVERRIDE_COLUMNS, row, strict=True))

    def set(
        self,
        security_id: str,
        price_date: date,
        quote_currency: str,
        *,
        close: Decimal,
        note: str | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert-or-update one price mark + audit (``security_price_override.set``).

        Captures the full prior row (or ``None`` on insert) as ``before`` and the
        full resulting row as ``after``. ``created_at`` is preserved on update so
        a correction does not erase when the mark was first authored;
        ``updated_at`` refreshes via ``NOW()`` (DuckDB parses
        ``CURRENT_TIMESTAMP`` as an identifier inside ``DO UPDATE``).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(security_id, price_date, quote_currency)
            self._db.execute(
                f"""
                INSERT INTO {SECURITY_PRICE_OVERRIDES.full_name}
                    (security_id, price_date, quote_currency, close, note)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (security_id, price_date, quote_currency) DO UPDATE SET
                    close = excluded.close,
                    note = excluded.note,
                    updated_at = NOW()
                """,  # noqa: S608  # TableRef + parameterized values
                [security_id, price_date, quote_currency, close, note],
            )
            after = self._fetch_row(security_id, price_date, quote_currency)
            return self._emit_audit(
                action="security_price_override.set",
                target=(
                    *self._audit_target,
                    _target_id(security_id, price_date, quote_currency),
                ),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def delete(
        self,
        security_id: str,
        price_date: date,
        quote_currency: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Delete one mark, returning that date to provider-derived valuation.

        ``None`` when there's nothing to delete (silent no-op), matching the
        ``BalanceAssertionsRepo`` precedent for a composite-PK delete.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(security_id, price_date, quote_currency)
            if before is None:
                return None
            self._db.execute(
                f"DELETE FROM {SECURITY_PRICE_OVERRIDES.full_name} "  # noqa: S608  # TableRef + parameterized values
                f"WHERE security_id = ? AND price_date = ? AND quote_currency = ?",
                [security_id, price_date, quote_currency],
            )
            return self._emit_audit(
                action="security_price_override.delete",
                target=(
                    *self._audit_target,
                    _target_id(security_id, price_date, quote_currency),
                ),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

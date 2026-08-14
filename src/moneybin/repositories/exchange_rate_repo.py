"""Audited writes to ``app.exchange_rate_overrides`` (user rate corrections).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``CurrencyService``
composes this instead of raw SQL; reads stay in the service.

The table has a **composite** primary key ``(from_currency, to_currency,
rate_date)``, but ``app.audit_log.target_id`` is a single string. The repo maps
the triple to a composite ``target_id`` of
``"{from_currency}|{to_currency}|{rate_date ISO}"``; the doctor's audit-coverage
check projects the matching expression via its ``pk_expr`` hook.

``delete`` is not CRUD symmetry. An override outranks the cached provider rate
for its own pair and date, and ``set`` can only replace the value — so without
``delete`` a correction is unreachable once written and that date can never
return to the rate the provider published. ``surface-design.md`` requires the
paired ``_delete`` for this mutation shape for the same reason.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from moneybin.repositories.base import BaseRepo, quote_ident
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import EXCHANGE_RATE_OVERRIDES

_OVERRIDE_COLUMNS = (
    "from_currency",
    "to_currency",
    "rate_date",
    "rate",
    "note",
    "created_at",
    "updated_at",
)


def _target_id_parts(
    from_currency: str, to_currency: str, rate_date: date | str
) -> str:
    """Composite ``target_id`` for the three-column PK.

    Must match the doctor coverage check's ``pk_expr`` (``from_currency || '|'
    || to_currency || '|' || CAST(rate_date AS VARCHAR)``); DuckDB casts a
    ``DATE`` to the same ``YYYY-MM-DD`` string ``date.isoformat()`` produces.

    ``rate_date`` accepts both forms deliberately: forward mutations pass a
    ``date``, while ``_row_target_id`` receives whatever the row carries — an ISO
    string once the row has round-tripped through a JSON audit payload. Both must
    yield the same id, or an undo scopes to a target its own mutation never used.
    """
    day = rate_date.isoformat() if isinstance(rate_date, date) else rate_date
    return f"{from_currency}|{to_currency}|{day}"


def _target_id(from_currency: str, to_currency: str, rate_date: date) -> str:
    """Composite ``target_id`` for a forward mutation's known-typed arguments."""
    return _target_id_parts(from_currency, to_currency, rate_date)


class ExchangeRateOverridesRepo(BaseRepo):
    """Audited CRUD over ``app.exchange_rate_overrides`` (one rate per pair+date)."""

    repository = "exchange_rate_overrides"

    table_ref = EXCHANGE_RATE_OVERRIDES
    pk_columns = ("from_currency", "to_currency", "rate_date")

    def _row_target_id(self, row: dict[str, Any]) -> str:
        """Mirror the forward mutations' composite audit target.

        Without this the base implementation keys an undo row on
        ``from_currency`` alone — a value shared by every override out of that
        currency — while ``set``/``delete`` emit the full triple. Undo rows
        landing under a different ``target_id`` than the mutations they reverse
        defeats ``UndoService``'s "block, don't cascade" guard, which matches
        later operations on ``(target_schema, target_table, target_id)``: it
        finds no blocker and reinstates a stale rate over a correction the user
        made afterwards.

        ``rate_date`` arrives as a ``date`` from the live row and as an ISO
        string from a JSON-decoded audit payload; both must produce the same id,
        so the value is stringified rather than re-formatted.
        """
        return _target_id_parts(
            str(row["from_currency"]),
            str(row["to_currency"]),
            row["rate_date"],
        )

    def _fetch_row(
        self, from_currency: str, to_currency: str, rate_date: date
    ) -> dict[str, Any] | None:
        """Read one override by the composite PK as a ``column → value`` dict, or ``None``.

        ``BaseRepo._fetch_one`` keys on a single column; this table's PK is
        composite, so the read is spelled out here (identifiers quoted per
        ``.claude/rules/security.md``).
        """
        cols = ", ".join(quote_ident(c) for c in _OVERRIDE_COLUMNS)
        row = self._db.execute(
            f"SELECT {cols} FROM {EXCHANGE_RATE_OVERRIDES.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted identifiers
            f"WHERE from_currency = ? AND to_currency = ? AND rate_date = ?",
            [from_currency, to_currency, rate_date],
        ).fetchone()
        if row is None:
            return None
        return dict(zip(_OVERRIDE_COLUMNS, row, strict=True))

    def set(
        self,
        from_currency: str,
        to_currency: str,
        rate_date: date,
        *,
        rate: Decimal,
        note: str | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert-or-update one rate correction + audit (``exchange_rate_override.set``).

        Captures the full prior row (or ``None`` on insert) as ``before`` and the
        full resulting row as ``after``. ``created_at`` is preserved on update so
        a correction does not erase when the override was first authored;
        ``updated_at`` refreshes via ``NOW()`` (DuckDB parses
        ``CURRENT_TIMESTAMP`` as an identifier inside ``DO UPDATE``).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(from_currency, to_currency, rate_date)
            self._db.execute(
                f"""
                INSERT INTO {EXCHANGE_RATE_OVERRIDES.full_name}
                    (from_currency, to_currency, rate_date, rate, note)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (from_currency, to_currency, rate_date) DO UPDATE SET
                    rate = excluded.rate,
                    note = excluded.note,
                    updated_at = NOW()
                """,  # noqa: S608  # TableRef + parameterized values
                [from_currency, to_currency, rate_date, rate, note],
            )
            after = self._fetch_row(from_currency, to_currency, rate_date)
            return self._emit_audit(
                action="exchange_rate_override.set",
                target=(
                    *self._audit_target,
                    _target_id(from_currency, to_currency, rate_date),
                ),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def delete(
        self,
        from_currency: str,
        to_currency: str,
        rate_date: date,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Delete one override, returning that date to the provider's published rate.

        ``None`` when there's nothing to delete (silent no-op), matching the
        ``BalanceAssertionsRepo`` precedent for a composite-PK delete.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(from_currency, to_currency, rate_date)
            if before is None:
                return None
            self._db.execute(
                f"DELETE FROM {EXCHANGE_RATE_OVERRIDES.full_name} "  # noqa: S608  # TableRef + parameterized values
                f"WHERE from_currency = ? AND to_currency = ? AND rate_date = ?",
                [from_currency, to_currency, rate_date],
            )
            return self._emit_audit(
                action="exchange_rate_override.delete",
                target=(
                    *self._audit_target,
                    _target_id(from_currency, to_currency, rate_date),
                ),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

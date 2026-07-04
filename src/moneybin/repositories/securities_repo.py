"""Audited writes to ``app.securities`` (manually-maintained security catalog).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. A future service
layer composes this instead of raw SQL; partial-field editing (updating only
some columns of an existing security) is a service-layer concern, not this
repo's — ``upsert`` always writes the full row.
"""

from __future__ import annotations

import uuid
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import SECURITIES

_SECURITIES_COLUMNS = (
    "security_id",
    "name",
    "security_type",
    "ticker",
    "exchange",
    "cusip",
    "isin",
    "figi",
    "coingecko_id",
    "is_cash_equivalent",
    "cost_basis_method",
    "currency_code",
    "created_at",
    "updated_at",
)


class SecuritiesRepo(BaseRepo):
    """Audited upsert over ``app.securities`` (manually-maintained security catalog)."""

    repository = "securities"

    table_ref = SECURITIES
    pk_columns = ("security_id",)

    def _fetch_row(self, security_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            SECURITIES, _SECURITIES_COLUMNS, "security_id", security_id
        )

    def upsert(
        self,
        *,
        security_id: str | None,
        name: str,
        security_type: str,
        ticker: str | None = None,
        exchange: str | None = None,
        cusip: str | None = None,
        isin: str | None = None,
        figi: str | None = None,
        coingecko_id: str | None = None,
        is_cash_equivalent: bool | None = None,
        cost_basis_method: str | None = None,
        currency_code: str = "USD",
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert-or-update one security + audit (``securities.upsert``).

        Mints a 12-hex ``security_id`` (Strategy 3, ``identifiers.md``) when
        ``security_id`` is ``None`` — a brand-new catalog entry. A non-``None``
        id targets that row for a full-row update. Captures the full prior row
        (or ``None`` on insert) as ``before`` and the full resulting row as
        ``after``. ``NOW()`` (not ``CURRENT_TIMESTAMP``) refreshes
        ``updated_at`` in the ``DO UPDATE`` clause: DuckDB parses
        ``CURRENT_TIMESTAMP`` as an identifier in that position, not a call.

        Returns the :class:`AuditEvent`; the resulting ``security_id`` (minted
        or caller-supplied) is its ``target_id`` — coherent with the sibling
        mint-on-insert repos (e.g. ``UserMerchantsRepo.insert``).
        """
        resolved_id = security_id if security_id is not None else uuid.uuid4().hex[:12]
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(resolved_id)
            self._db.execute(
                f"""
                INSERT INTO {SECURITIES.full_name} (
                    security_id, name, security_type, ticker, exchange,
                    cusip, isin, figi, coingecko_id, is_cash_equivalent,
                    cost_basis_method, currency_code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (security_id) DO UPDATE SET
                    name               = excluded.name,
                    security_type      = excluded.security_type,
                    ticker             = excluded.ticker,
                    exchange           = excluded.exchange,
                    cusip              = excluded.cusip,
                    isin               = excluded.isin,
                    figi               = excluded.figi,
                    coingecko_id       = excluded.coingecko_id,
                    is_cash_equivalent = excluded.is_cash_equivalent,
                    cost_basis_method  = excluded.cost_basis_method,
                    currency_code      = excluded.currency_code,
                    updated_at         = NOW()
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    resolved_id,
                    name,
                    security_type,
                    ticker,
                    exchange,
                    cusip,
                    isin,
                    figi,
                    coingecko_id,
                    is_cash_equivalent,
                    cost_basis_method,
                    currency_code,
                ],
            )
            after = self._fetch_row(resolved_id)
            return self._emit_audit(
                action="securities.upsert",
                target=(*self._audit_target, resolved_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

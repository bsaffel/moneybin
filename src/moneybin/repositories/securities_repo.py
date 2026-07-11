"""Audited writes to ``app.securities`` (security catalog with provenance).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. A future service
layer composes this instead of raw SQL.

``created_by`` (``'user'`` or ``'plaid'``) distinguishes user-authored catalog
rows from provider-minted ones. ``upsert`` always writes the full row and sets
``created_by`` on INSERT only — never on the ``ON CONFLICT`` update, so
provenance is immutable after mint. ``refresh_provider_attributes`` is the one
deliberate partial-field update: it refreshes name/type/ticker on a
``created_by='plaid'`` row only, a no-op (returns ``None``) on any other row.
``delete`` removes a ``created_by='plaid'`` row only (the merge-accept path);
user-authored rows are never deletable through this repo.
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
    "created_by",
    "created_at",
    "updated_at",
)


class SecuritiesRepo(BaseRepo):
    """Audited mint/refresh/delete over ``app.securities`` (security catalog)."""

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
        created_by: str = "user",
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

        ``created_by`` (default ``"user"``) sets provenance on INSERT only —
        deliberately absent from the ``DO UPDATE SET`` list, so a conflicting
        upsert (e.g. the resolver re-touching a row) can never flip an
        existing row's provenance. Provenance is immutable after mint.

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
                    cost_basis_method, currency_code, created_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    created_by,
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

    def refresh_provider_attributes(
        self,
        security_id: str,
        *,
        name: str,
        security_type: str,
        ticker: str | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Update provider-refreshable attributes on a plaid-minted row.

        Only ``name``/``security_type``/``ticker`` are refreshable — every
        other column (exchange, cusip, isin, figi, coingecko_id,
        is_cash_equivalent, cost_basis_method, currency_code, created_by) is
        left untouched by the ``UPDATE``'s explicit column list.

        Returns ``None`` (no write, no audit) for user-authored or missing
        rows — the resolver's "never touch created_by='user'" rule is
        enforced HERE — and also when ``name``/``security_type``/``ticker``
        already match the stored row. Without that second check, ``UPDATE``
        always bumps ``updated_at``, so ``before != after`` unconditionally
        and a daily resolver sync would accrue one no-op ``securities.refresh``
        audit row per security per day, forever.

        The ``WHERE created_by = 'plaid'`` clause is defense-in-depth: the
        Python check above already enforces provenance from the fetched
        ``before`` row, but the clause keeps the invariant true at the SQL
        layer even if a future refactor moves or fast-paths that fetch.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(security_id)
            if before is None or before.get("created_by") != "plaid":
                return None
            if (
                before["name"] == name
                and before["security_type"] == security_type
                and before["ticker"] == ticker
            ):
                return None
            self._db.execute(
                f"""
                UPDATE {SECURITIES.full_name}
                SET name = ?, security_type = ?, ticker = ?, updated_at = NOW()
                WHERE security_id = ? AND created_by = 'plaid'
                """,  # noqa: S608  # TableRef + parameterized values
                [name, security_type, ticker, security_id],
            )
            after = self._fetch_row(security_id)
            return self._emit_audit(
                action="securities.refresh",
                target=(*self._audit_target, security_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def delete(
        self,
        security_id: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Delete a provider-minted catalog row (merge-accept path only).

        Raises ``ValueError`` for a ``created_by='user'`` row — user-authored
        catalog entries are never deletable through this path.

        No cascade: ``app.security_links.security_id`` and
        ``app.security_link_decisions.candidate_security_id`` reference this
        table with no FK (DuckDB). The caller MUST repoint dependent
        ``app.security_links`` rows and migrate lot selections to another
        security BEFORE calling — otherwise those rows are left orphaned.

        The ``AND created_by = 'plaid'`` clause is defense-in-depth: the
        Python check above already enforces provenance from the fetched
        ``before`` row, but the clause keeps the invariant true at the SQL
        layer even if a future refactor moves or fast-paths that fetch.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(security_id), "security_id", security_id
            )
            if before.get("created_by") != "plaid":
                raise ValueError(
                    f"securities.delete: {security_id} is user-authored; only "
                    "provider-minted (created_by='plaid') rows are deletable"
                )
            self._db.execute(
                f"""
                DELETE FROM {SECURITIES.full_name}
                WHERE security_id = ? AND created_by = 'plaid'
                """,  # noqa: S608  # TableRef + parameterized value
                [security_id],
            )
            return self._emit_audit(
                action="securities.delete",
                target=(*self._audit_target, security_id),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

"""Audited writes to ``app.account_settings`` (per-account user settings).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``AccountService``
composes this instead of raw SQL; reads (``load``) stay in the service.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import ACCOUNT_SETTINGS

_ACCOUNT_SETTINGS_COLUMNS = (
    "account_id",
    "display_name",
    "official_name",
    "last_four",
    "account_subtype",
    "holder_category",
    "iso_currency_code",
    "credit_limit",
    "archived",
    "include_in_net_worth",
    "updated_at",
)


class AccountSettingsRepo(BaseRepo):
    """Audited CRUD over ``app.account_settings`` (one row per account)."""

    repository = "account_settings"

    table_ref = ACCOUNT_SETTINGS
    pk_columns = ("account_id",)

    def _fetch_row(self, account_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            ACCOUNT_SETTINGS, _ACCOUNT_SETTINGS_COLUMNS, "account_id", account_id
        )

    def set(
        self,
        *,
        account_id: str,
        display_name: str | None,
        official_name: str | None,
        last_four: str | None,
        account_subtype: str | None,
        holder_category: str | None,
        iso_currency_code: str | None,
        credit_limit: Decimal | None,
        archived: bool,
        include_in_net_worth: bool,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Insert-or-update one account's settings + audit (``account_settings.set``).

        Captures the full prior row (or ``None`` on insert) as ``before`` and the
        full resulting row as ``after``. ``NOW()`` (not ``CURRENT_TIMESTAMP``)
        refreshes ``updated_at`` in the ``DO UPDATE`` clause: DuckDB parses
        ``CURRENT_TIMESTAMP`` as an identifier in that position, not a call.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(account_id)
            self._db.execute(
                f"""
                INSERT INTO {ACCOUNT_SETTINGS.full_name} (
                    account_id, display_name, official_name, last_four,
                    account_subtype, holder_category, iso_currency_code,
                    credit_limit, archived, include_in_net_worth
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (account_id) DO UPDATE SET
                    display_name         = excluded.display_name,
                    official_name        = excluded.official_name,
                    last_four            = excluded.last_four,
                    account_subtype      = excluded.account_subtype,
                    holder_category      = excluded.holder_category,
                    iso_currency_code    = excluded.iso_currency_code,
                    credit_limit         = excluded.credit_limit,
                    archived             = excluded.archived,
                    include_in_net_worth = excluded.include_in_net_worth,
                    updated_at           = NOW()
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    account_id,
                    display_name,
                    official_name,
                    last_four,
                    account_subtype,
                    holder_category,
                    iso_currency_code,
                    credit_limit,
                    archived,
                    include_in_net_worth,
                ],
            )
            after = self._fetch_row(account_id)
            return self._emit_audit(
                action="account_settings.set",
                target=(*self._audit_target, account_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def delete(
        self,
        account_id: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Delete one account's settings; ``None`` when there's nothing to delete."""
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row(account_id)
            if before is None:
                return None
            self._db.execute(
                f"DELETE FROM {ACCOUNT_SETTINGS.full_name} "  # noqa: S608  # TableRef + parameterized value
                f"WHERE account_id = ?",
                [account_id],
            )
            return self._emit_audit(
                action="account_settings.delete",
                target=(*self._audit_target, account_id),
                before=self._serialize_for_audit(before),
                after=None,
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

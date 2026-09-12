"""Audited writes to ``app.account_settings`` (per-account user settings).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``AccountService``
composes this instead of raw SQL; reads (``load``) stay in the service.
"""

from __future__ import annotations

from datetime import date
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
    "currency_code",
    "credit_limit",
    "archived",
    "archived_at",
    "include_in_net_worth",
    "default_cost_basis_method",
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
        currency_code: str | None,
        credit_limit: Decimal | None,
        archived: bool,
        archived_at: date | None,
        include_in_net_worth: bool,
        default_cost_basis_method: str | None,
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
                    account_subtype, holder_category, currency_code,
                    credit_limit, archived, archived_at, include_in_net_worth,
                    default_cost_basis_method
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (account_id) DO UPDATE SET
                    display_name         = excluded.display_name,
                    official_name        = excluded.official_name,
                    last_four            = excluded.last_four,
                    account_subtype      = excluded.account_subtype,
                    holder_category      = excluded.holder_category,
                    currency_code        = excluded.currency_code,
                    credit_limit         = excluded.credit_limit,
                    archived             = excluded.archived,
                    archived_at          = excluded.archived_at,
                    include_in_net_worth = excluded.include_in_net_worth,
                    default_cost_basis_method = excluded.default_cost_basis_method,
                    updated_at           = NOW()
                """,  # noqa: S608  # TableRef + parameterized values
                [
                    account_id,
                    display_name,
                    official_name,
                    last_four,
                    account_subtype,
                    holder_category,
                    currency_code,
                    credit_limit,
                    archived,
                    archived_at,
                    include_in_net_worth,
                    default_cost_basis_method,
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

    def _restore_row(self, *, before: dict[str, Any], locate: dict[str, Any]) -> None:
        """Restore, deriving ``archived_at`` when a legacy capture omits it.

        A pre-V060 ``account_settings.set`` audit row was captured before
        ``archived_at`` existed, so neither its ``before`` nor ``after`` image
        carries the key -- ``BaseRepo._restore_row`` only sets columns present
        in ``before``, so undoing one of these rows would leave ``archived_at``
        at whatever it currently holds. Restoring ``archived=TRUE`` from such a
        row could then leave ``archived=TRUE`` with ``archived_at`` stale or
        NULL, breaking the "NULL while active" invariant. This is not a guess
        about history: the undo performs the archive/unarchive transition
        *right now*, so deriving from today's date is the same rule
        ``AccountService.settings_update`` already applies to every live
        FALSE->TRUE call. A post-V060 capture always carries the key and takes
        the base-class path unchanged.
        """
        super()._restore_row(before=before, locate=locate)
        if "archived_at" in before:
            return
        where, where_params = self._pk_where(locate)
        if before.get("archived") is True:
            self._db.execute(
                f"UPDATE {self.table_ref.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted pk; values parameterized
                f"SET archived_at = CURRENT_DATE WHERE {where}",
                where_params,
            )
        else:
            self._db.execute(
                f"UPDATE {self.table_ref.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted pk; values parameterized
                f"SET archived_at = NULL WHERE {where}",
                where_params,
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

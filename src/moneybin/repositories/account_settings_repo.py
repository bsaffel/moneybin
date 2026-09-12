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

    def _insert_row(self, row: dict[str, Any]) -> None:
        """Normalize a legacy archived capture before an undo re-inserts it.

        ``BaseRepo.undo_event`` re-inserts a captured row through this hook
        whenever it undoes a DELETE-shaped event -- including the
        undo-of-undo of a pre-V060 account's first settings write. That
        write's ``before_value`` is NULL, so undoing it *deletes* the row
        (the ``before is None`` branch of ``undo_event``), and undoing that
        generated undo lands here rather than in :meth:`_restore_row`. The
        deleted row's captured image is the original pre-V060
        ``account_settings.set`` payload, which never carried ``archived_at``
        -- reinserting it unmodified stands the row back up as
        ``archived=True, archived_at=NULL``, which the date-scoped net-worth
        predicate reads as "archived at every date," losing the date V060
        backfilled onto the (now-deleted) live row. Mutating ``row`` in place
        -- not a copy -- matters for the same reason :meth:`_restore_row`
        mutates both its images: ``undo_event`` reuses this same dict as the
        ``after_value`` of the audit row it emits for this undo, so leaving
        it unmodified would misreport the row this call actually produced
        and repeat the corruption on the next undo-of-this-undo.
        """
        if row.get("archived") is True and "archived_at" not in row:
            row["archived_at"] = date.today().isoformat()
        super()._insert_row(row)

    def _restore_row(self, *, before: dict[str, Any], locate: dict[str, Any]) -> None:
        """Restore, deriving ``archived_at`` when a legacy capture omits it.

        A pre-V060 ``account_settings.set`` audit row was captured before
        ``archived_at`` existed, so neither its ``before`` nor ``after`` image
        carries the key -- ``BaseRepo._restore_row`` only sets columns present
        in ``before``, so undoing one of these rows would leave ``archived_at``
        at whatever it currently holds. Deriving is only correct when this undo
        actually flips ``archived`` -- comparing ``before`` to ``locate`` (the
        event's captured after-image) is how we tell an archive/unarchive undo
        from a legacy row whose ``archived`` was unchanged (some other field
        moved); stamping either image would corrupt a real ``archived_at`` with
        a guessed date.

        The two images need DIFFERENT derivations, not a shared one -- ``before``
        becomes the row's new live state, so a transition TO ``archived=True``
        is happening right now and today's date is the same rule
        ``AccountService.settings_update`` already applies to every live
        FALSE->TRUE call; a transition to ``archived=False`` clears it, per
        ``archived_at``'s own contract (NULL while active). ``locate`` is not a
        live transition at all -- it is the ORIGINAL forward event's captured
        post-mutation image, from whenever that historical mutation actually
        happened, so today's date would misdate history rather than recover it.
        There is no way to recover that image's real date (that is exactly why
        the key is missing), so it always normalizes to NULL: certain when
        ``locate.archived`` is False (active implies NULL), and the documented
        "no guess beats a documented gap" convention V060's own backfill applies
        when ``locate.archived`` is True with no evidence.

        ``_require_capture`` only checks KEY PRESENCE (`set(required) -
        set(image)`), never the value, so a present ``None`` satisfies it
        exactly as well as a present date -- giving both correctness and the
        no-crash property at once. Mutating both dicts in place matters because
        ``BaseRepo.undo_event`` emits its own audit row as
        ``before=locate, after=before`` (the caller's ``after`` argument *is*
        this ``locate`` dict, passed by reference): leaving either key absent
        makes a later undo-of-this-undo hit ``_require_capture`` and fail with a
        misleading "not reversible" error. A post-V060 capture always carries
        the key and takes the base-class path unchanged.
        """
        super()._restore_row(before=before, locate=locate)
        if "archived_at" in before:
            return
        if before.get("archived") == locate.get("archived"):
            return
        where, where_params = self._pk_where(locate)
        derived_at = date.today() if before.get("archived") is True else None
        self._db.execute(
            f"UPDATE {self.table_ref.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted pk; values parameterized
            f"SET archived_at = ? WHERE {where}",
            [derived_at, *where_params],
        )
        before["archived_at"] = (
            derived_at.isoformat() if derived_at is not None else None
        )
        # locate is a historical snapshot, never "now" -- no real date to
        # recover, so it always normalizes to NULL rather than guessing.
        locate["archived_at"] = None

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

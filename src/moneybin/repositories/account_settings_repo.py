"""Audited writes to ``app.account_settings`` (per-account user settings).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. ``AccountService``
composes this instead of raw SQL; reads (``load``) stay in the service.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from moneybin import error_codes
from moneybin.errors import UserError
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
    "investment_source_type",
    "investment_source_type_changed_at",
    "updated_at",
)


class AccountSettingsRepo(BaseRepo):
    """Audited CRUD over ``app.account_settings`` (one row per account)."""

    repository = "account_settings"

    table_ref = ACCOUNT_SETTINGS
    pk_columns = ("account_id",)

    def _live_columns(self) -> frozenset[str]:
        """The settings columns the live catalog has.

        A profile opened with ``no_auto_upgrade=True`` skips migrations, so a
        column a later migration added may be absent. One probe feeds
        ``_fetch_row`` and ``set()``, so reads and writes agree on the set.
        """
        rows = self._db.execute(
            """
            SELECT column_name FROM duckdb_columns()
            WHERE schema_name = ? AND table_name = ?
            """,
            [ACCOUNT_SETTINGS.schema, ACCOUNT_SETTINGS.name],
        ).fetchall()
        live = {str(r[0]) for r in rows}
        return frozenset(c for c in _ACCOUNT_SETTINGS_COLUMNS if c in live)

    def _archived_at_supported(self) -> bool:
        """True when the live ``app.account_settings`` catalog has ``archived_at``.

        A profile opened with ``no_auto_upgrade=True`` (config.py's documented
        operator mode) skips V063 -- the migration that added this column --
        forever: ``Database.__init__`` calls ``init_schemas()``
        (``CREATE TABLE IF NOT EXISTS``, a no-op on an existing table)
        unconditionally, before deciding whether to run pending migrations at
        all. One probe per repo operation feeds ``_fetch_row``, the ``set()``
        INSERT/ON CONFLICT column lists, and the undo/restore paths below --
        one named concept ("archived_at is optional in this catalog until
        V063 applies"), not independent probes that could drift apart.

        **The invariant every caller of this probe must uphold:** the guard
        must be invisible on a migrated catalog, and truthful on an
        unmigrated one. On a catalog that HAS ``archived_at``, nothing this
        probe gates may change what any caller observes -- every branch it
        selects must be a pure no-op relative to the un-guarded code path. On
        a catalog that LACKS it, every surface a caller can read -- the
        value a service method returns, the MCP response envelope built from
        it, and the ``before``/``after`` images an undo re-derives -- must
        report exactly what was persisted, which is ``None``. No surface may
        ever report a value the write path silently dropped, and no branch
        gated by this probe may overwrite a value that a migrated catalog
        actually holds.
        """
        return "archived_at" in self._live_columns()

    def _fetch_row(
        self, account_id: str, *, live: frozenset[str] | None = None
    ) -> dict[str, Any] | None:
        if live is None:
            live = self._live_columns()
        columns = tuple(c for c in _ACCOUNT_SETTINGS_COLUMNS if c in live)
        return self._fetch_one(ACCOUNT_SETTINGS, columns, "account_id", account_id)

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
        investment_source_type: str | None,
        investment_source_type_changed_at: datetime | None,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
        context: dict[str, Any] | None = None,
    ) -> AuditEvent:
        """Insert-or-update one account's settings + audit (``account_settings.set``).

        Captures the full prior row (or ``None`` on insert) as ``before`` and the
        full resulting row as ``after``. ``NOW()`` (not ``CURRENT_TIMESTAMP``)
        refreshes ``updated_at`` in the ``DO UPDATE`` clause: DuckDB parses
        ``CURRENT_TIMESTAMP`` as an identifier in that position, not a call.

        The INSERT/ON CONFLICT column list drops every column the live catalog
        lacks (``archived_at`` pre-V063, the investment-source pair pre-V068,
        ``no_auto_upgrade=True`` -- see ``_live_columns``): there is no column
        to write the caller's value into, so it is silently not persisted
        rather than raising a raw ``duckdb.BinderException``. A caller that
        needs the value persisted (``AccountService`` for a source choice)
        checks the column itself and refuses first.

        ``context`` rides the audit row's ``context_json`` (caller-intent the
        full-row snapshot cannot carry).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            live = self._live_columns()
            before = self._fetch_row(account_id, live=live)

            values_by_column: dict[str, Any] = {
                "account_id": account_id,
                "display_name": display_name,
                "official_name": official_name,
                "last_four": last_four,
                "account_subtype": account_subtype,
                "holder_category": holder_category,
                "currency_code": currency_code,
                "credit_limit": credit_limit,
                "archived": archived,
                "archived_at": archived_at,
                "include_in_net_worth": include_in_net_worth,
                "default_cost_basis_method": default_cost_basis_method,
                "investment_source_type": investment_source_type,
                "investment_source_type_changed_at": investment_source_type_changed_at,
            }
            columns = [c for c in values_by_column if c in live]
            col_sql = ", ".join(columns)
            placeholders = ", ".join("?" for _ in columns)
            update_sql = ", ".join(
                f"{c} = excluded.{c}" for c in columns if c != "account_id"
            )
            self._db.execute(
                f"""
                INSERT INTO {ACCOUNT_SETTINGS.full_name} ({col_sql})
                VALUES ({placeholders})
                ON CONFLICT (account_id) DO UPDATE SET
                    {update_sql},
                    updated_at = NOW()
                """,  # noqa: S608  # TableRef + allowlisted literal column names + parameterized values
                [values_by_column[c] for c in columns],
            )
            after = self._fetch_row(account_id, live=live)
            return self._emit_audit(
                action="account_settings.set",
                target=(*self._audit_target, account_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
                context=context,
            )

    def _delete_by_pk(self, row: dict[str, Any]) -> None:
        """Preserve the live ``archived_at`` in the capture before deleting it.

        ``BaseRepo.undo_event``'s ``before is None`` branch reaches here when
        undoing an account's FIRST settings write -- a pre-V063 archive whose
        own captured ``after`` image has no ``archived_at`` key at all (the
        column didn't exist when it was captured), even though V063's
        backfill (or any later write) may since have given the LIVE row a
        real, recoverable date. ``undo_event`` reuses this same ``row`` dict
        object -- by reference, not a copy -- as the ``before_value`` of the
        undo event it emits, so mutating it here is what lets a later
        undo-of-this-undo (redo) reach :meth:`_insert_row` with the key
        already present, so it restores the real date instead of guessing
        ``date.today()`` (:meth:`_insert_row`'s fallback remains correct for
        the case it was written for: a legacy capture reversed before this
        fix shipped, or one where the live row genuinely never got a
        backfilled date).

        Read BEFORE the DELETE below removes the row -- there is nothing left
        to read once it commits. Guarded by ``_archived_at_supported()`` per
        that method's invariant: on a pre-V063, ``no_auto_upgrade=True``
        catalog there is no ``archived_at`` column to read, and the SELECT
        below would raise.

        Refuses outright when the row being deleted holds an investment source
        choice: the delete would drop ``investment_source_type_changed_at``
        with the row, so ledger rows that re-enter would report their old
        ``created_at`` and rewind an incremental reader's watermark.
        """
        if row.get("investment_source_type") is not None:
            raise UserError(
                "Undoing this would delete the account's settings together with "
                "its investment source choice.",
                code=error_codes.RECOVERY_NO_PATH,
                hint=(
                    "Clear the choice instead: moneybin accounts set <account> "
                    "--clear-investment-source-type"
                ),
            )
        if (
            row.get("archived") is True
            and "archived_at" not in row
            and self._archived_at_supported()
        ):
            where, where_params = self._pk_where(row)
            live_row = self._db.execute(
                f"SELECT archived_at FROM {self.table_ref.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted pk
                f"WHERE {where}",
                where_params,
            ).fetchone()
            row["archived_at"] = (
                live_row[0].isoformat()
                if live_row is not None and live_row[0] is not None
                else None
            )
        super()._delete_by_pk(row)

    def _insert_row(self, row: dict[str, Any]) -> None:
        """Normalize a legacy archived capture before an undo re-inserts it.

        ``BaseRepo.undo_event`` re-inserts a captured row through this hook
        whenever it undoes a DELETE-shaped event -- including the
        undo-of-undo of a pre-V063 account's first settings write. That
        write's ``before_value`` is NULL, so undoing it *deletes* the row
        (the ``before is None`` branch of ``undo_event``), and undoing that
        generated undo lands here rather than in :meth:`_restore_row`.
        :meth:`_delete_by_pk` now copies a real, live ``archived_at`` into
        this same capture at delete time whenever one exists, so this
        fallback fires only when no such date was ever recoverable there --
        a legacy capture reversed before that fix shipped, or a row that
        genuinely never got a backfilled date. Reinserting the row unmodified
        in that case would stand it back up as ``archived=True,
        archived_at=NULL``, which the date-scoped net-worth predicate reads
        as "archived at every date." Mutating ``row`` in place -- not a copy
        -- matters for the same reason :meth:`_restore_row` mutates both its
        images: ``undo_event`` reuses this same dict as the ``after_value``
        of the audit row it emits for this undo, so leaving it unmodified
        would misreport the row this call actually produced and repeat the
        corruption on the next undo-of-this-undo.

        Guarded by ``_archived_at_supported()``: on a pre-V063,
        ``no_auto_upgrade=True`` catalog there is no ``archived_at`` column to
        backfill into, and adding the key here would make the generic
        ``BaseRepo._insert_row`` (which inserts every key ``row`` holds) try
        to write a column that does not exist.

        A re-inserted row that holds a source choice also gets a fresh
        ``investment_source_type_changed_at``: its ledger rows re-enter the
        ledger now, so the stored time must not be older than that.
        """
        if (
            row.get("archived") is True
            and "archived_at" not in row
            and self._archived_at_supported()
        ):
            row["archived_at"] = date.today().isoformat()
        if (
            row.get("investment_source_type") is not None
            and "investment_source_type_changed_at" in self._live_columns()
        ):
            row["investment_source_type_changed_at"] = self._now().isoformat()
        super()._insert_row(row)

    def _now(self) -> datetime:
        return self._db.execute("SELECT NOW()::TIMESTAMP").fetchone()[0]  # type: ignore[index]  # NOW() always returns a row

    def _stamp_source_change(
        self, *, before: dict[str, Any], locate: dict[str, Any]
    ) -> None:
        """Advance the change time when an undo moves ``investment_source_type``.

        Undoing the setting changes the ledger's rows just as setting it did, so
        the restored change time must move forward, never back to the old value
        ``before`` carries. Mutates ``before`` in place: ``undo_event`` emits its
        audit row from these same dicts (same reason as the archived_at paths).
        """
        if "investment_source_type_changed_at" not in self._live_columns():
            return
        if before.get("investment_source_type") == locate.get("investment_source_type"):
            return
        changed_at = self._now()
        where, where_params = self._pk_where(locate)
        self._db.execute(
            f"UPDATE {self.table_ref.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted pk; values parameterized
            f"SET investment_source_type_changed_at = ? WHERE {where}",
            [changed_at, *where_params],
        )
        before["investment_source_type_changed_at"] = changed_at.isoformat()

    def _restore_row(self, *, before: dict[str, Any], locate: dict[str, Any]) -> None:
        """Restore, deriving ``archived_at`` when a legacy capture omits it.

        A pre-V063 ``account_settings.set`` audit row was captured before
        ``archived_at`` existed, so neither its ``before`` nor ``after`` image
        carries the key -- ``BaseRepo._restore_row`` only sets columns present
        in ``before``, so undoing one of these rows would leave ``archived_at``
        at whatever it currently holds. GUESSING a date is only correct when
        this undo actually flips ``archived`` -- comparing ``before`` to
        ``locate`` (the event's captured after-image) is how we tell an
        archive/unarchive undo from a legacy row whose ``archived`` was
        unchanged (some other field moved); stamping either image with a
        guessed date in that second case would corrupt a real ``archived_at``.
        That second case still backfills both images with the row's actual
        LIVE value (never a guess) rather than leaving the key absent --
        omitting it would leave the post-V063 audit row ``undo_event`` emits
        from these same dicts with an incomplete row capture, on a catalog
        that fully supports the column.

        The two images need DIFFERENT derivations, not a shared one -- ``before``
        becomes the row's new live state, so a transition TO ``archived=True``
        is happening right now and today's date is the same rule
        ``AccountService.settings_update`` already applies to every live
        FALSE->TRUE call; a transition to ``archived=False`` clears it, per
        ``archived_at``'s own contract (NULL while active). ``locate`` is not a
        live transition at all -- it is the ORIGINAL forward event's captured
        post-mutation image, from whenever that historical mutation actually
        happened, so today's date would misdate history rather than recover it.
        The legacy capture itself carries no evidence of that real date (that
        is exactly why the key is missing from it) -- but the LIVE row, right
        now, might: V063's own backfill (or any post-V063 write) may have
        already given this row a real, recoverable ``archived_at`` before this
        undo ever ran. Per this method's invariant (``_archived_at_supported``'s
        docstring), the guard must be a no-op on a migrated catalog -- clobbering
        that live value with a guessed ``NULL`` would violate exactly that.
        So ``locate["archived_at"]`` is read from the live row IMMEDIATELY
        BEFORE the ``UPDATE`` below overwrites it, not hardcoded: certain NULL
        only when the live catalog genuinely holds nothing there (no V063
        backfill happened, or ``locate.archived`` was already False), and the
        real value otherwise -- so an undo-of-this-undo (redo) can recover it.

        ``_require_capture`` only checks KEY PRESENCE (`set(required) -
        set(image)`), never the value, so a present ``None`` satisfies it
        exactly as well as a present date -- giving both correctness and the
        no-crash property at once. Mutating both dicts in place matters because
        ``BaseRepo.undo_event`` emits its own audit row as
        ``before=locate, after=before`` (the caller's ``after`` argument *is*
        this ``locate`` dict, passed by reference): leaving either key absent
        makes a later undo-of-this-undo hit ``_require_capture`` and fail with a
        misleading "not reversible" error. A post-V063 capture always carries
        the key and takes the base-class path unchanged.
        """
        super()._restore_row(before=before, locate=locate)
        self._stamp_source_change(before=before, locate=locate)
        if "archived_at" in before:
            return
        if not self._archived_at_supported():
            # Pre-V063, no_auto_upgrade=True: the live catalog has no
            # archived_at column to derive a value into. Nothing to backfill,
            # and leaving the key out of both images matches what a guarded
            # _fetch_row would capture on this same catalog.
            return
        where, where_params = self._pk_where(locate)
        # Read the row's CURRENT archived_at before the transition branch's
        # UPDATE below can overwrite it -- on a migrated catalog this can be
        # a genuine, recoverable date (V063's backfill, or any later write),
        # and the guard must never destroy that (see the docstring above).
        # Only when the live catalog holds nothing here does this degrade to
        # the same "no guess beats a documented gap" NULL used elsewhere.
        live_row = self._db.execute(
            f"SELECT archived_at FROM {self.table_ref.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted pk
            f"WHERE {where}",
            where_params,
        ).fetchone()
        live_archived_at = live_row[0] if live_row is not None else None
        if before.get("archived") == locate.get("archived"):
            # Not an archive/unarchive undo -- some OTHER field moved on a
            # legacy capture, so the base class's UPDATE above never touched
            # archived_at (it isn't a key in `before`). The live value read
            # above already describes both post-restore images equally --
            # record it on both rather than leaving the key absent, so the
            # audit event undo_event emits from these dicts (before=after,
            # after=before, swapped) captures the full row instead of
            # omitting it. Leaving it out here is what let a post-V063
            # generated audit row silently regress to a partial capture even
            # though the catalog fully supports it -- system_audit could not
            # then tell a persisted NULL from a genuinely missing legacy
            # field.
            value = (
                live_archived_at.isoformat() if live_archived_at is not None else None
            )
            before["archived_at"] = value
            locate["archived_at"] = value
            return
        derived_at = date.today() if before.get("archived") is True else None
        self._db.execute(
            f"UPDATE {self.table_ref.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted pk; values parameterized
            f"SET archived_at = ? WHERE {where}",
            [derived_at, *where_params],
        )
        before["archived_at"] = (
            derived_at.isoformat() if derived_at is not None else None
        )
        locate["archived_at"] = (
            live_archived_at.isoformat() if live_archived_at is not None else None
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

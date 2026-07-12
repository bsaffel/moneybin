"""Audited security repoint over ``raw.manual_investment_transactions`` (M1G.4).

``raw.manual_investment_transactions`` is **user-entered state that happens to
live in ``raw``** (mirroring ``raw.manual_transactions``), not provider-owned
raw. ``investments record`` resolves the account and security interactively at
entry and stores the RESOLVED ``security_id``; ``stg_manual__investment_
transactions`` then carries that id verbatim, with no link-table indirection.

So when a security merge deletes a provisional catalog row, nothing else moves
these rows off it: the link repoint only moves provider refs. This repo owns the
one mutation the merge needs — re-point ``security_id`` onto the surviving
security — and pairs it with an ``app.audit_log`` row like any protected-table
repo. That audit is what keeps ``SecurityLinksService.accept_merge`` reversible
as a single operation: a merge that repointed the ledger but could not
un-repoint it would not be undoable, and a partial undo of a merge is a defect.

Row CREATION stays in ``InvestmentService`` (a plain ``raw`` write, outside
Invariant 10's ``app.*`` surface). Only this post-hoc repoint is audited — the
asymmetry is deliberate: the merge is the one write to this table that has to
participate in an undoable cascade.
"""

from __future__ import annotations

from typing import Any, ClassVar

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import MANUAL_INVESTMENT_TRANSACTIONS, TableRef

_MANUAL_INVESTMENT_COLUMNS = (
    "source_transaction_id",
    "source_type",
    "source_origin",
    "import_id",
    "account_id",
    "security_id",
    "security_ref",
    "type",
    "subtype",
    "event_group_id",
    "trade_date",
    "settlement_date",
    "original_acquisition_date",
    "quantity",
    "price",
    "amount",
    "fees",
    "currency_code",
    "description",
    "created_at",
    "created_by",
    "investment_transaction_id",
)


class ManualInvestmentTransactionsRepo(BaseRepo):
    """Audited ``security_id`` repoint over ``raw.manual_investment_transactions``."""

    repository: ClassVar[str] = "manual_investment_transactions"
    table_ref: ClassVar[TableRef] = MANUAL_INVESTMENT_TRANSACTIONS
    pk_columns: ClassVar[tuple[str, ...]] = ("source_transaction_id",)

    def _fetch_row(self, source_transaction_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            MANUAL_INVESTMENT_TRANSACTIONS,
            _MANUAL_INVESTMENT_COLUMNS,
            "source_transaction_id",
            source_transaction_id,
        )

    def list_ids_for_security(self, security_id: str) -> list[str]:
        """``source_transaction_id`` of every manual event on ``security_id``. Read-only."""
        rows = self._db.execute(
            "SELECT source_transaction_id FROM "  # noqa: S608  # TableRef + parameterized value
            f"{MANUAL_INVESTMENT_TRANSACTIONS.full_name} "
            "WHERE security_id = ? ORDER BY source_transaction_id",
            [security_id],
        ).fetchall()
        return [str(row[0]) for row in rows]

    def repoint_security(
        self,
        *,
        source_transaction_id: str,
        new_security_id: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Re-point one manual event onto ``new_security_id`` + paired audit.

        ``investment_transaction_id`` (the predicted gold key) hashes
        ``source_transaction_id`` and ``account_id``, not ``security_id``, so it
        is unaffected — the disposal keys in ``app.lot_selections`` stay valid
        across the repoint. The lots those selections point at DO re-key
        (``lot_id`` hashes ``security_id``); migrating them is the caller's job
        (``SecurityLinksService._plan_lot_selections``).

        ``security_ref`` — what the user originally typed — is deliberately left
        alone: it is the audit trail of the resolution, not a live reference.

        Raises ``ValueError`` when the row is absent, or when it already carries
        ``new_security_id`` (a no-op repoint would emit a ``before == after``
        audit row that the undo engine skips, so the caller must not ask for one).
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._require(
                self._fetch_row(source_transaction_id),
                "source_transaction_id",
                source_transaction_id,
            )
            if before["security_id"] == new_security_id:
                raise ValueError(
                    f"manual_investment_transactions repoint: "
                    f"{source_transaction_id!r} already carries "
                    f"security_id={new_security_id!r}"
                )
            self._db.execute(
                "UPDATE "  # noqa: S608  # TableRef + parameterized values
                f"{MANUAL_INVESTMENT_TRANSACTIONS.full_name} "
                "SET security_id = ? WHERE source_transaction_id = ?",
                [new_security_id, source_transaction_id],
            )
            after = self._fetch_row(source_transaction_id)
            return self._emit_audit(
                action="manual_investment.repoint_security",
                target=(*self._audit_target, source_transaction_id),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

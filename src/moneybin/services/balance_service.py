"""Balance service.

Per-account balance queries, history, reconciliation, and assertion CRUD.
Backs both CLI (moneybin accounts balance ...) and MCP (accounts_balance_*).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from moneybin.database import Database
from moneybin.privacy.payloads.balances import (
    BalanceAssertionListPayload,
    BalanceAssertionPayload,
    BalanceAssertionRow,
    BalanceObservationListPayload,
    BalanceObservationRow,
)
from moneybin.services.account_service import assert_account_exists
from moneybin.services.audit_service import AuditService
from moneybin.tables import BALANCE_ASSERTIONS, FCT_BALANCES_DAILY

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BalanceAssertionSnapshot:
    """Exact persisted assertion state used for pre-mutation verification."""

    account_id: str
    assertion_date: date
    balance: Decimal
    notes: str | None
    created_at: str
    updated_at: str


def _observation_row_from_db(row: tuple[object, ...]) -> BalanceObservationRow:
    """Construct a BalanceObservationRow from a SELECT result tuple.

    Columns: account_id, balance_date, balance, is_observed, observation_source,
    reconciliation_delta.
    """
    return BalanceObservationRow(
        account_id=row[0],  # type: ignore[arg-type]
        balance_date=row[1],  # type: ignore[arg-type]
        balance=row[2],  # type: ignore[arg-type]
        is_observed=row[3],  # type: ignore[arg-type]
        observation_source=row[4],  # type: ignore[arg-type]
        reconciliation_delta=row[5],  # type: ignore[arg-type]
    )


def _assertion_row_from_db(row: tuple[object, ...]) -> BalanceAssertionRow:
    """Construct a BalanceAssertionRow from a SELECT result tuple.

    Columns: account_id, assertion_date, balance, notes, created_at.
    """
    return BalanceAssertionRow(
        account_id=row[0],  # type: ignore[arg-type]
        assertion_date=row[1],  # type: ignore[arg-type]
        balance=row[2],  # type: ignore[arg-type]
        notes=row[3],  # type: ignore[arg-type]
        created_at=str(row[4]),
    )


def _assertion_snapshot_from_db(
    row: tuple[object, ...],
) -> BalanceAssertionSnapshot:
    """Construct the exact persisted snapshot from a SELECT result tuple."""
    return BalanceAssertionSnapshot(
        account_id=row[0],  # type: ignore[arg-type]
        assertion_date=row[1],  # type: ignore[arg-type]
        balance=row[2],  # type: ignore[arg-type]
        notes=row[3],  # type: ignore[arg-type]
        created_at=str(row[4]),
        updated_at=str(row[5]),
    )


class BalanceService:
    """Balance queries, history, reconciliation, and assertion CRUD."""

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Initialize with an open Database connection.

        Composes :class:`BalanceAssertionsRepo` for audited
        ``app.balance_assertions`` writes (Invariant 10), sharing this service's
        ``AuditService``.
        """
        # Deferred to break a circular import — see the matching note in
        # account_service.__init__. `services/__init__` eagerly re-exports this
        # module, so a repo-first import order (e.g. a repo test) would hit a
        # partially-initialized module on a module-level repo import here.
        from moneybin.repositories.balance_assertions_repo import (  # noqa: PLC0415
            BalanceAssertionsRepo,
        )

        self._db = db
        self._audit = audit if audit is not None else AuditService(db)
        self._assertions_repo = BalanceAssertionsRepo(db, audit=self._audit)

    def _assert_account_exists(self, account_id: str) -> None:
        """Raise UserError if account_id is not in core.dim_accounts."""
        assert_account_exists(self._db, account_id)

    # --- Assertion CRUD ---

    def assert_balance(
        self,
        account_id: str,
        assertion_date: date,
        balance: Decimal,
        notes: str | None = None,
        *,
        actor: str,
    ) -> BalanceAssertionPayload:
        """Insert or update a balance assertion (audited via ``BalanceAssertionsRepo``).

        On the INSERT path, created_at and updated_at are both populated from
        the column DEFAULT. On the UPDATE path, created_at is preserved (it
        records first entry) and updated_at is refreshed so `core.fct_balances`
        per-row freshness reflects the edit, per the core-updated-at-convention
        spec.
        """
        self._assert_account_exists(account_id)
        self._assertions_repo.set(
            account_id, assertion_date, balance=balance, notes=notes, actor=actor
        )
        logger.info(f"Asserted balance for account {account_id} on {assertion_date}")
        return BalanceAssertionPayload(
            assertion=self._load_assertion(account_id, assertion_date)
        )

    def delete_assertion(
        self,
        account_id: str,
        assertion_date: date,
        *,
        actor: str,
        verify: Callable[[BalanceAssertionSnapshot], None] | None = None,
    ) -> bool:
        """Delete the assertion for (account_id, assertion_date).

        Deliberately does NOT validate ``account_id`` against ``dim_accounts``
        (unlike ``assert_balance``): delete is idempotent best-effort, so an
        unknown ``account_id`` is a silent no-op, just like a known account with
        no assertion on that date. This forgiving-delete contract is locked by
        ``test_e2e_mutating.py::...test_balance_delete_nonexistent_is_noop``.

        When ``verify`` is supplied, it receives the live assertion after the
        transaction begins and immediately before the repository delete. A
        raised exception rolls back without a mutation or audit row.
        """
        self._db.begin()
        try:
            assertion = self._find_assertion_snapshot(account_id, assertion_date)
            if assertion is not None and verify is not None:
                verify(assertion)
            deleted = (
                self._assertions_repo.delete(
                    account_id,
                    assertion_date,
                    actor=actor,
                    in_outer_txn=True,
                )
                if assertion is not None
                else None
            )
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise

        if deleted is None:
            logger.info(
                f"No balance assertion to delete for account {account_id} "
                f"on {assertion_date}"
            )
            return False
        else:
            logger.info(
                f"Deleted balance assertion for account {account_id} on {assertion_date}"
            )
            return True

    def list_assertions(
        self, account_id: str | None = None
    ) -> BalanceAssertionListPayload:
        """List assertions; optionally filter to a single account."""
        sql = f"""
            SELECT account_id, assertion_date, balance, notes, created_at
            FROM {BALANCE_ASSERTIONS.full_name}
        """
        params: list[object] = []
        if account_id is not None:
            sql += " WHERE account_id = ?"
            params.append(account_id)
        sql += " ORDER BY account_id, assertion_date DESC"
        return BalanceAssertionListPayload(
            assertions=[
                _assertion_row_from_db(row)
                for row in self._db.execute(sql, params).fetchall()
            ]
        )

    def get_assertion(
        self, account_id: str, assertion_date: date
    ) -> BalanceAssertionRow | None:
        """Return one exact assertion, or ``None`` when absent."""
        return self._find_assertion(account_id, assertion_date)

    def get_assertion_snapshot(
        self, account_id: str, assertion_date: date
    ) -> BalanceAssertionSnapshot | None:
        """Return every persisted field used to verify a mutation target."""
        return self._find_assertion_snapshot(account_id, assertion_date)

    def _load_assertion(
        self, account_id: str, assertion_date: date
    ) -> BalanceAssertionRow:
        assertion = self._find_assertion(account_id, assertion_date)
        if assertion is None:
            # No interpolated account_id: it is ACCOUNT_IDENTIFIER (CRITICAL)
            # and must not reach application logs (no-PII-in-logs rule). This is
            # a should-never-happen invariant guard right after an upsert.
            raise RuntimeError("assertion not found immediately after upsert")
        return assertion

    def _find_assertion(
        self, account_id: str, assertion_date: date
    ) -> BalanceAssertionRow | None:
        """Return one exact assertion, or ``None`` when absent."""
        row = self._db.execute(
            f"""
            SELECT account_id, assertion_date, balance, notes, created_at
            FROM {BALANCE_ASSERTIONS.full_name}
            WHERE account_id = ? AND assertion_date = ?
            """,
            [account_id, assertion_date],
        ).fetchone()
        if row is None:
            return None
        return _assertion_row_from_db(row)

    def _find_assertion_snapshot(
        self, account_id: str, assertion_date: date
    ) -> BalanceAssertionSnapshot | None:
        """Return the exact persisted row, including mutable metadata."""
        row = self._db.execute(
            f"""
            SELECT
                account_id, assertion_date, balance, notes, created_at, updated_at
            FROM {BALANCE_ASSERTIONS.full_name}
            WHERE account_id = ? AND assertion_date = ?
            """,
            [account_id, assertion_date],
        ).fetchone()
        if row is None:
            return None
        return _assertion_snapshot_from_db(row)

    # --- Reads ---

    def current_balances(
        self,
        account_ids: list[str] | None = None,
        as_of_date: date | None = None,
    ) -> BalanceObservationListPayload:
        """Most recent balance per account; optionally as-of a date."""
        params: list[object] = []
        where_parts: list[str] = []
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            where_parts.append(f"account_id IN ({placeholders})")
            params.extend(account_ids)
        if as_of_date is not None:
            where_parts.append("balance_date <= ?")
            params.append(as_of_date)
        where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
        sql = f"""
            WITH ranked AS (
                SELECT
                    account_id, balance_date, balance,
                    is_observed, observation_source, reconciliation_delta,
                    ROW_NUMBER() OVER (
                        PARTITION BY account_id ORDER BY balance_date DESC
                    ) AS _rn
                FROM {FCT_BALANCES_DAILY.full_name}
                {where_sql}
            )
            SELECT account_id, balance_date, balance,
                   is_observed, observation_source, reconciliation_delta
            FROM ranked WHERE _rn = 1
            ORDER BY account_id
        """  # noqa: S608  # placeholders parameterized via params list above
        return BalanceObservationListPayload(
            observations=[
                _observation_row_from_db(row)
                for row in self._db.execute(sql, params).fetchall()
            ]
        )

    def history(
        self,
        account_id: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> BalanceObservationListPayload:
        """Per-account balance time series."""
        sql = f"""
            SELECT account_id, balance_date, balance,
                   is_observed, observation_source, reconciliation_delta
            FROM {FCT_BALANCES_DAILY.full_name}
            WHERE account_id = ?
        """
        params: list[object] = [account_id]
        if from_date is not None:
            sql += " AND balance_date >= ?"
            params.append(from_date)
        if to_date is not None:
            sql += " AND balance_date <= ?"
            params.append(to_date)
        sql += " ORDER BY balance_date"
        return BalanceObservationListPayload(
            observations=[
                _observation_row_from_db(row)
                for row in self._db.execute(sql, params).fetchall()
            ]
        )

    def reconcile(
        self,
        account_ids: list[str] | None = None,
        threshold: Decimal = Decimal("0.01"),
    ) -> BalanceObservationListPayload:
        """Days with abs(reconciliation_delta) > threshold."""
        params: list[object] = [threshold]
        where = ""
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            where = f" AND account_id IN ({placeholders})"
            params.extend(account_ids)
        sql = f"""
            SELECT account_id, balance_date, balance,
                   is_observed, observation_source, reconciliation_delta
            FROM {FCT_BALANCES_DAILY.full_name}
            WHERE reconciliation_delta IS NOT NULL
              AND ABS(reconciliation_delta) > ? {where}
            ORDER BY account_id, balance_date DESC
        """  # noqa: S608  # placeholders parameterized
        return BalanceObservationListPayload(
            observations=[
                _observation_row_from_db(row)
                for row in self._db.execute(sql, params).fetchall()
            ]
        )

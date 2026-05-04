"""Balance service.

Per-account balance queries, history, reconciliation, and assertion CRUD.
Backs both CLI (moneybin accounts balance ...) and MCP (accounts_balance_*).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Literal

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import BALANCE_ASSERTIONS, DIM_ACCOUNTS, FCT_BALANCES_DAILY

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BalanceAssertion:
    """User-entered balance anchor."""

    account_id: str
    assertion_date: date
    balance: Decimal
    notes: str | None
    created_at: str  # TIMESTAMP from DuckDB rendered as string for transport

    def to_dict(self) -> dict[str, Any]:
        """Serialize for JSON transport."""
        return {
            "account_id": self.account_id,
            "assertion_date": self.assertion_date.isoformat(),
            "balance": self.balance,
            "notes": self.notes,
            "created_at": self.created_at,
        }

    @classmethod
    def from_row(cls, row: tuple[object, ...]) -> BalanceAssertion:
        """Construct from a SELECT (account_id, assertion_date, balance, notes, created_at) row."""
        return cls(
            account_id=row[0],  # type: ignore[arg-type]
            assertion_date=row[1],  # type: ignore[arg-type]
            balance=row[2],  # type: ignore[arg-type]
            notes=row[3],  # type: ignore[arg-type]
            created_at=str(row[4]),
        )


@dataclass(frozen=True, slots=True)
class BalanceObservation:
    """Daily balance observation from core.fct_balances_daily."""

    account_id: str
    balance_date: date
    balance: Decimal
    is_observed: bool
    observation_source: str | None
    reconciliation_delta: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        """Serialize for JSON transport."""
        return {
            "account_id": self.account_id,
            "balance_date": self.balance_date.isoformat(),
            "balance": self.balance,
            "is_observed": self.is_observed,
            "observation_source": self.observation_source,
            "reconciliation_delta": self.reconciliation_delta,
        }

    @classmethod
    def from_row(cls, row: tuple[object, ...]) -> BalanceObservation:
        """Construct from a SELECT (account_id, balance_date, balance, is_observed, observation_source, reconciliation_delta) row."""
        return cls(
            account_id=row[0],  # type: ignore[arg-type]
            balance_date=row[1],  # type: ignore[arg-type]
            balance=row[2],  # type: ignore[arg-type]
            is_observed=row[3],  # type: ignore[arg-type]
            observation_source=row[4],  # type: ignore[arg-type]
            reconciliation_delta=row[5],  # type: ignore[arg-type]
        )


@dataclass(slots=True)
class BalanceObservationListResult:
    """Result of a balance-observation query."""

    observations: list[BalanceObservation]
    sensitivity: Literal["low", "medium", "high"] = "medium"

    def to_envelope(self) -> ResponseEnvelope:
        """Wrap observations in the standard MCP response envelope."""
        return build_envelope(
            data=[o.to_dict() for o in self.observations],
            sensitivity=self.sensitivity,
        )


@dataclass(slots=True)
class BalanceAssertionListResult:
    """Result of a balance-assertion query."""

    assertions: list[BalanceAssertion]
    sensitivity: Literal["low", "medium", "high"] = "medium"

    def to_envelope(self) -> ResponseEnvelope:
        """Wrap assertions in the standard MCP response envelope."""
        return build_envelope(
            data=[a.to_dict() for a in self.assertions],
            sensitivity=self.sensitivity,
        )


class BalanceService:
    """Balance queries, history, reconciliation, and assertion CRUD."""

    def __init__(self, db: Database) -> None:
        """Initialize with an open Database connection."""
        self._db = db

    def _assert_account_exists(self, account_id: str) -> None:
        """Raise UserError if account_id is not in core.dim_accounts."""
        row = self._db.execute(
            f"SELECT 1 FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ? LIMIT 1",
            [account_id],
        ).fetchone()
        if row is None:
            raise UserError(f"Account not found: {account_id}", code="not_found")

    # --- Assertion CRUD ---

    def assert_balance(
        self,
        account_id: str,
        assertion_date: date,
        balance: Decimal,
        notes: str | None = None,
    ) -> BalanceAssertion:
        """Insert or update a balance assertion.

        Uses NOW() instead of CURRENT_TIMESTAMP — DuckDB treats CURRENT_TIMESTAMP
        as an identifier (not a function call) inside ON CONFLICT DO UPDATE clauses.
        On the INSERT path, created_at is populated from the column DEFAULT.
        """
        self._assert_account_exists(account_id)
        self._db.execute(
            f"""
            INSERT INTO {BALANCE_ASSERTIONS.full_name}
                (account_id, assertion_date, balance, notes)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (account_id, assertion_date) DO UPDATE SET
                balance = excluded.balance,
                notes = excluded.notes,
                created_at = NOW()
            """,
            [account_id, assertion_date, balance, notes],
        )
        logger.info(f"Asserted balance for account {account_id} on {assertion_date}")
        return self._load_assertion(account_id, assertion_date)

    def delete_assertion(self, account_id: str, assertion_date: date) -> None:
        """Delete the assertion for (account_id, assertion_date). Silent no-op if absent."""
        self._db.execute(
            f"DELETE FROM {BALANCE_ASSERTIONS.full_name} "
            "WHERE account_id = ? AND assertion_date = ?",
            [account_id, assertion_date],
        )
        logger.info(
            f"Deleted balance assertion for account {account_id} on {assertion_date}"
        )

    def list_assertions(self, account_id: str | None = None) -> list[BalanceAssertion]:
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
        return [
            BalanceAssertion.from_row(row)
            for row in self._db.execute(sql, params).fetchall()
        ]

    def _load_assertion(
        self, account_id: str, assertion_date: date
    ) -> BalanceAssertion:
        row = self._db.execute(
            f"""
            SELECT account_id, assertion_date, balance, notes, created_at
            FROM {BALANCE_ASSERTIONS.full_name}
            WHERE account_id = ? AND assertion_date = ?
            """,
            [account_id, assertion_date],
        ).fetchone()
        if row is None:
            raise RuntimeError(
                f"assertion not found after upsert: {account_id} {assertion_date}"
            )
        return BalanceAssertion.from_row(row)

    # --- Reads ---

    def current_balances(
        self,
        account_ids: list[str] | None = None,
        as_of_date: date | None = None,
    ) -> list[BalanceObservation]:
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
        return [
            BalanceObservation.from_row(row)
            for row in self._db.execute(sql, params).fetchall()
        ]

    def history(
        self,
        account_id: str,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> list[BalanceObservation]:
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
        return [
            BalanceObservation.from_row(row)
            for row in self._db.execute(sql, params).fetchall()
        ]

    def reconcile(
        self,
        account_ids: list[str] | None = None,
        threshold: Decimal = Decimal("0.01"),
    ) -> list[BalanceObservation]:
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
        return [
            BalanceObservation.from_row(row)
            for row in self._db.execute(sql, params).fetchall()
        ]

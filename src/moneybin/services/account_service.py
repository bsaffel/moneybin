# src/moneybin/services/account_service.py
"""Account and balance service.

Business logic for account listing and balance retrieval. Consumed by
both MCP tools and CLI commands.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from moneybin.database import Database
from moneybin.mcp.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import DIM_ACCOUNTS, OFX_BALANCES

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Account:
    """Single account record."""

    account_id: str
    account_type: str
    institution_name: str
    source_type: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        return {
            "account_id": self.account_id,
            "account_type": self.account_type,
            "institution_name": self.institution_name,
            "source_type": self.source_type,
        }


@dataclass(slots=True)
class AccountListResult:
    """Result of account listing query."""

    accounts: list[Account]

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[a.to_dict() for a in self.accounts],
            sensitivity="low",
            actions=[
                "Use accounts.balances for current balances",
                "Use spending.summary with account_id to filter by account",
            ],
        )


@dataclass(frozen=True, slots=True)
class AccountBalance:
    """Balance snapshot for a single account."""

    account_id: str
    institution_name: str | None
    account_type: str | None
    ledger_balance: float
    available_balance: float | None
    as_of_date: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        d: dict[str, Any] = {
            "account_id": self.account_id,
            "institution_name": self.institution_name,
            "account_type": self.account_type,
            "ledger_balance": self.ledger_balance,
            "available_balance": self.available_balance,
            "as_of_date": self.as_of_date,
        }
        return d


@dataclass(slots=True)
class BalanceListResult:
    """Result of balance query."""

    balances: list[AccountBalance]

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[b.to_dict() for b in self.balances],
            sensitivity="medium",
            actions=[
                "Use spending.summary for spending trends",
                "Use transactions.search with account_id for recent activity",
            ],
        )


class AccountService:
    """Account and balance operations.

    All methods return typed dataclasses with a ``to_envelope()`` method.
    """

    def __init__(self, db: Database) -> None:
        """Initialize AccountService with an open Database connection."""
        self._db = db

    def list_accounts(self) -> AccountListResult:
        """List all accounts.

        Returns:
            AccountListResult with all accounts ordered by institution
            and type.
        """
        sql = f"""
            SELECT
                account_id,
                account_type,
                institution_name,
                source_type
            FROM {DIM_ACCOUNTS.full_name}
            ORDER BY institution_name, account_type
        """

        result = self._db.execute(sql)
        rows = result.fetchall()

        accounts = [
            Account(
                account_id=str(row[0]),
                account_type=str(row[1]),
                institution_name=str(row[2]),
                source_type=str(row[3]),
            )
            for row in rows
        ]

        logger.info(f"Listed {len(accounts)} accounts")
        return AccountListResult(accounts=accounts)

    def balances(self, account_id: str | None = None) -> BalanceListResult:
        """Get latest balance for each account.

        Uses ROW_NUMBER() to pick the most recent balance snapshot per
        account, then LEFT JOINs dim_accounts for institution info.

        Args:
            account_id: Filter to a specific account.

        Returns:
            BalanceListResult with latest balances.
        """
        conditions: list[str] = ["b.rn = 1"]
        params: list[object] = []

        if account_id:
            conditions.append("b.account_id = ?")
            params.append(account_id)

        where = "WHERE " + " AND ".join(conditions)

        sql = f"""
            WITH latest AS (
                SELECT
                    account_id,
                    ledger_balance,
                    available_balance,
                    ledger_balance_date,
                    ROW_NUMBER() OVER (
                        PARTITION BY account_id
                        ORDER BY ledger_balance_date DESC
                    ) AS rn
                FROM {OFX_BALANCES.full_name}
            )
            SELECT
                b.account_id,
                a.institution_name,
                a.account_type,
                b.ledger_balance,
                b.available_balance,
                b.ledger_balance_date
            FROM latest b
            LEFT JOIN {DIM_ACCOUNTS.full_name} a
                ON b.account_id = a.account_id
            {where}
            ORDER BY a.institution_name, a.account_type
        """

        result = self._db.execute(sql, params)
        rows = result.fetchall()

        balances = [
            AccountBalance(
                account_id=str(row[0]),
                institution_name=str(row[1]) if row[1] else None,
                account_type=str(row[2]) if row[2] else None,
                ledger_balance=float(row[3]),
                available_balance=float(row[4]) if row[4] is not None else None,
                as_of_date=str(row[5]),
            )
            for row in rows
        ]

        logger.info(f"Retrieved balances for {len(balances)} accounts")
        return BalanceListResult(balances=balances)

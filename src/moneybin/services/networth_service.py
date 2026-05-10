"""Net worth service.

Cross-account daily aggregation reads from reports.net_worth (which already
filters by include_in_net_worth and archived). History supports daily/weekly/
monthly intervals with period-over-period change.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.tables import DIM_ACCOUNTS, FCT_BALANCES_DAILY, REPORTS_NET_WORTH

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class NetWorthSnapshot:
    """Net worth at a point in time, with optional per-account breakdown."""

    balance_date: date
    net_worth: Decimal
    total_assets: Decimal
    total_liabilities: Decimal
    account_count: int
    per_account: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self, *, include_per_account: bool = True) -> dict[str, Any]:
        """Serialize snapshot for JSON / envelope transport."""
        payload: dict[str, Any] = {
            "balance_date": self.balance_date.isoformat(),
            "net_worth": self.net_worth,
            "total_assets": self.total_assets,
            "total_liabilities": self.total_liabilities,
            "account_count": self.account_count,
        }
        if include_per_account:
            payload["per_account"] = self.per_account
        return payload


class NetworthService:
    """Net worth queries: current snapshot + history."""

    _VALID_INTERVALS = {"daily", "weekly", "monthly"}

    def __init__(self, db: Database) -> None:
        """Initialize with an open Database connection."""
        self._db = db

    def current(
        self,
        as_of_date: date | None = None,
        account_ids: list[str] | None = None,
    ) -> NetWorthSnapshot:
        """Latest net worth snapshot, optionally as-of a date.

        Returns a zero-snapshot if no reports.net_worth rows exist.
        """
        as_of_clause = ""
        params: list[object] = []
        if as_of_date is not None:
            as_of_clause = "WHERE balance_date <= ?"
            params.append(as_of_date)
        sql = f"""
            SELECT balance_date, net_worth, total_assets, total_liabilities, account_count
            FROM {REPORTS_NET_WORTH.full_name}
            {as_of_clause}
            ORDER BY balance_date DESC
            LIMIT 1
        """  # noqa: S608  # parameterized via params
        row = self._db.execute(sql, params).fetchone()
        if row is None:
            return NetWorthSnapshot(
                balance_date=date.today(),
                net_worth=Decimal("0"),
                total_assets=Decimal("0"),
                total_liabilities=Decimal("0"),
                account_count=0,
                per_account=[],
            )
        per_account = self._per_account_breakdown(row[0], account_ids)
        return NetWorthSnapshot(
            balance_date=row[0],
            net_worth=row[1],
            total_assets=row[2],
            total_liabilities=row[3],
            account_count=row[4],
            per_account=per_account,
        )

    def _per_account_breakdown(
        self, on_date: date, account_ids: list[str] | None
    ) -> list[dict[str, Any]]:
        """Per-account balances on a date, joining dim for include/archive filtering."""
        params: list[object] = [on_date]
        where = ""
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            where = f" AND d.account_id IN ({placeholders})"
            params.extend(account_ids)
        sql = f"""
            SELECT a.account_id, a.display_name, d.balance, d.observation_source
            FROM {FCT_BALANCES_DAILY.full_name} AS d
            INNER JOIN {DIM_ACCOUNTS.full_name} AS a ON d.account_id = a.account_id
            WHERE d.balance_date = ? AND a.include_in_net_worth AND NOT a.archived {where}
            ORDER BY a.display_name
        """  # noqa: S608  # parameterized
        return [
            {
                "account_id": row[0],
                "display_name": row[1],
                "balance": row[2],
                "observation_source": row[3],
            }
            for row in self._db.execute(sql, params).fetchall()
        ]

    def history(
        self,
        from_date: date,
        to_date: date,
        interval: str = "monthly",
    ) -> list[dict[str, Any]]:
        """Period-bucketed time series with period-over-period change."""
        if interval not in self._VALID_INTERVALS:
            raise ValueError(
                f"interval must be one of {sorted(self._VALID_INTERVALS)}, got {interval!r}"
            )
        bucket_expr = {
            "daily": "balance_date",
            "weekly": "DATE_TRUNC('week', balance_date)",
            "monthly": "DATE_TRUNC('month', balance_date)",
        }[interval]
        sql = f"""
            WITH bucketed AS (
                SELECT
                    {bucket_expr} AS period,
                    LAST(net_worth ORDER BY balance_date) AS end_net_worth
                FROM {REPORTS_NET_WORTH.full_name}
                WHERE balance_date BETWEEN ? AND ?
                GROUP BY 1
            ),
            with_change AS (
                SELECT
                    period, end_net_worth,
                    LAG(end_net_worth) OVER (ORDER BY period) AS prev,
                    end_net_worth - LAG(end_net_worth) OVER (ORDER BY period) AS change_abs
                FROM bucketed
            )
            SELECT
                period, end_net_worth, change_abs,
                CASE WHEN prev IS NULL OR prev = 0 THEN NULL
                     ELSE change_abs / prev END AS change_pct
            FROM with_change ORDER BY period
        """  # noqa: S608  # bucket_expr from allowlist; values parameterized
        rows = self._db.execute(sql, [from_date, to_date]).fetchall()
        return [
            {
                "period": row[0].isoformat() if row[0] else None,
                "net_worth": row[1],
                "change_abs": row[2],
                "change_pct": row[3],
            }
            for row in rows
        ]

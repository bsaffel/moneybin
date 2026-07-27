"""Net worth service.

Cross-account daily aggregation reads from reports.net_worth (which already
filters by include_in_net_worth and archived). History supports daily/weekly/
monthly intervals with period-over-period change.
"""

from __future__ import annotations

import logging
from datetime import date

from moneybin.database import Database
from moneybin.privacy.payloads.networth import (
    NetWorthAccountRow,
    NetWorthCurrencySegment,
    NetWorthHistoryPayload,
    NetWorthHistoryPoint,
    NetWorthSnapshotPayload,
)
from moneybin.tables import DIM_ACCOUNTS, FCT_BALANCES_DAILY, REPORTS_NET_WORTH

logger = logging.getLogger(__name__)


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
    ) -> NetWorthSnapshotPayload:
        """Latest net worth snapshot, optionally as-of a date.

        Returns explicit null position fields when no row exists on/before the date.
        """
        as_of_clause = ""
        params: list[object] = []
        if as_of_date is not None:
            as_of_clause = "WHERE balance_date <= ?"
            params.append(as_of_date)
        # reports.net_worth is one row per (balance_date, currency_code). Resolve
        # the latest date first, then take every currency reporting on it — a
        # bare LIMIT 1 would return one arbitrary currency's total as if it were
        # the whole position.
        sql = f"""
            WITH latest AS (
                SELECT MAX(balance_date) AS balance_date
                FROM {REPORTS_NET_WORTH.full_name}
                {as_of_clause}
            )
            SELECT n.balance_date, n.currency_code, n.net_worth,
                   n.total_assets, n.total_liabilities, n.account_count
            FROM {REPORTS_NET_WORTH.full_name} AS n
            INNER JOIN latest AS l ON n.balance_date = l.balance_date
            ORDER BY n.currency_code
        """  # noqa: S608  # parameterized via params
        rows = self._db.execute(sql, params).fetchall()
        if not rows:
            return NetWorthSnapshotPayload(
                balance_date=None,
                currency_code=None,
                net_worth=None,
                total_assets=None,
                total_liabilities=None,
                account_count=0,
                per_currency=[],
                per_account=[],
            )
        segments = [
            NetWorthCurrencySegment(
                currency_code=row[1],
                net_worth=row[2],
                total_assets=row[3],
                total_liabilities=row[4],
                account_count=row[5],
            )
            for row in rows
        ]
        balance_date = rows[0][0]
        per_account = self._per_account_breakdown(balance_date, account_ids)
        single = segments[0] if len(segments) == 1 else None
        return NetWorthSnapshotPayload(
            balance_date=balance_date,
            currency_code=single.currency_code if single else None,
            net_worth=single.net_worth if single else None,
            total_assets=single.total_assets if single else None,
            total_liabilities=single.total_liabilities if single else None,
            account_count=sum(segment.account_count for segment in segments),
            per_currency=segments,
            per_account=per_account,
        )

    def _per_account_breakdown(
        self, on_date: date, account_ids: list[str] | None
    ) -> list[NetWorthAccountRow]:
        """Per-account balances on a date, joining dim for include/archive filtering."""
        params: list[object] = [on_date]
        where = ""
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            where = f" AND d.account_id IN ({placeholders})"
            params.extend(account_ids)
        sql = f"""
            SELECT a.account_id, a.display_name, d.balance, d.observation_source,
                   d.currency_code
            FROM {FCT_BALANCES_DAILY.full_name} AS d
            INNER JOIN {DIM_ACCOUNTS.full_name} AS a ON d.account_id = a.account_id
            WHERE d.balance_date = ? AND a.include_in_net_worth AND NOT a.archived {where}
            ORDER BY a.display_name
        """  # noqa: S608  # parameterized
        return [
            NetWorthAccountRow(
                account_id=row[0],
                display_name=row[1],
                balance=row[2],
                observation_source=row[3],
                currency_code=row[4],
            )
            for row in self._db.execute(sql, params).fetchall()
        ]

    def history(
        self,
        from_date: date,
        to_date: date,
        interval: str = "monthly",
    ) -> NetWorthHistoryPayload:
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
        # Each currency is its own series: bucketing and the period-over-period
        # LAG both key on currency_code, so a change is never the difference
        # between two currencies' positions (multi-currency.md Requirement 5).
        #
        # Ordering on each currency's own period index rather than on `period`
        # is what keeps that true of a *capped* response. `core:networth_history`
        # is a registered report, so `reports(..., limit=N)` truncates these rows
        # with a prefix; sorting `period, currency_code` filled the last period
        # with the lexicographically-first currencies and cut the rest, making a
        # currency's series look like it ended early instead of like it was
        # truncated. Interleaving by rank ends every currency's series at the
        # same point.
        sql = f"""
            WITH bucketed AS (
                SELECT
                    {bucket_expr} AS period,
                    currency_code,
                    LAST(net_worth ORDER BY balance_date) AS end_net_worth
                FROM {REPORTS_NET_WORTH.full_name}
                WHERE balance_date BETWEEN ? AND ?
                GROUP BY 1, 2
            ),
            with_change AS (
                SELECT
                    period, currency_code, end_net_worth,
                    LAG(end_net_worth) OVER (PARTITION BY currency_code ORDER BY period) AS prev,
                    end_net_worth - LAG(end_net_worth) OVER (PARTITION BY currency_code ORDER BY period) AS change_abs,
                    ROW_NUMBER() OVER (PARTITION BY currency_code ORDER BY period) AS rank_in_currency
                FROM bucketed
            )
            SELECT
                period, currency_code, end_net_worth, change_abs,
                CASE WHEN prev IS NULL OR prev = 0 THEN NULL
                     ELSE change_abs / prev END AS change_pct
            FROM with_change ORDER BY rank_in_currency, currency_code
        """  # noqa: S608  # bucket_expr from allowlist; values parameterized
        rows = self._db.execute(sql, [from_date, to_date]).fetchall()
        points = [
            NetWorthHistoryPoint(
                period=row[0].isoformat() if row[0] else None,
                currency_code=row[1],
                net_worth=row[2],
                change_abs=row[3],
                change_pct=row[4],
            )
            for row in rows
        ]
        return NetWorthHistoryPayload(points=points)
